import os

from aws_cdk import (
    Stack,
    Tags,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_lambda as lambda_,
)
from aws_cdk.aws_lambda_event_sources import DynamoEventSource
from constructs import Construct

LAMBDAS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "lambdas")


class InfrastructureStack(Stack):
    """
    Core infrastructure for the event-driven order decisioning pipeline:

    - DynamoDB "orders" table: source of truth for incoming orders.
      Streams are enabled so Lambda 1 can react to new/updated orders.
    - DynamoDB "decisions" table: full audit trail of every decision made
      by Lambda 1 (one row per order per decision timestamp).
    - SNS topic for orders that are auto-approved (downstream fulfilment
      systems can subscribe to this).
    - SQS queue (+ DLQ) for orders that need manual review.
    - SQS queue for declined orders (e.g. for later reporting/retry logic).

    Provisioned (not on-demand) billing is used throughout, sized to 5 RCU /
    5 WCU per table, to stay within the AWS Free Tier.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------------------------------------------------
        # DynamoDB: orders table
        # ------------------------------------------------------------------
        self.orders_table = dynamodb.Table(
            self,
            "OrdersTable",
            table_name="orders",
            partition_key=dynamodb.Attribute(
                name="order_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
            read_capacity=5,
            write_capacity=5,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ------------------------------------------------------------------
        # DynamoDB: decisions table (audit trail)
        # ------------------------------------------------------------------
        self.decisions_table = dynamodb.Table(
            self,
            "DecisionsTable",
            table_name="decisions",
            partition_key=dynamodb.Attribute(
                name="order_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="decision_timestamp", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
            read_capacity=5,
            write_capacity=5,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ------------------------------------------------------------------
        # SNS: topic for auto-approved orders
        # ------------------------------------------------------------------
        self.approved_orders_topic = sns.Topic(
            self,
            "ApprovedOrdersTopic",
            topic_name="approved-orders",
            display_name="Auto-approved orders",
        )

        # ------------------------------------------------------------------
        # SQS: manual review queue, with a dead-letter queue
        # ------------------------------------------------------------------
        self.manual_review_dlq = sqs.Queue(
            self,
            "ManualReviewDLQ",
            queue_name="manual-review-queue-dlq",
            retention_period=Duration.days(14),
        )

        self.manual_review_queue = sqs.Queue(
            self,
            "ManualReviewQueue",
            queue_name="manual-review-queue",
            visibility_timeout=Duration.seconds(60),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=self.manual_review_dlq,
            ),
        )

        # ------------------------------------------------------------------
        # SQS: declined orders queue
        # ------------------------------------------------------------------
        self.declined_orders_queue = sqs.Queue(
            self,
            "DeclinedOrdersQueue",
            queue_name="declined-orders-queue",
            visibility_timeout=Duration.seconds(60),
        )

        # ------------------------------------------------------------------
        # Lambda 1: decision engine
        # Triggered by the orders stream; only fires for records whose
        # NEW image has status == PENDING.
        # ------------------------------------------------------------------
        self.decision_engine_lambda = lambda_.Function(
            self,
            "DecisionEngineLambda",
            function_name="decision-engine",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="decision_engine.handler",
            code=lambda_.Code.from_asset(LAMBDAS_DIR),
            timeout=Duration.seconds(30),
            environment={
                "ORDERS_TABLE_NAME": self.orders_table.table_name,
                "DECISIONS_TABLE_NAME": self.decisions_table.table_name,
            },
        )

        self.decision_engine_lambda.add_event_source(
            DynamoEventSource(
                self.orders_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=10,
                filters=[
                    lambda_.FilterCriteria.filter(
                        {
                            "dynamodb": {
                                "NewImage": {
                                    "status": {
                                        "S": lambda_.FilterRule.is_equal("PENDING")
                                    }
                                }
                            }
                        }
                    )
                ],
            )
        )

        # decision_engine reads/writes the orders table (update_item) and
        # only writes to the decisions table (put_item audit records).
        self.orders_table.grant_read_write_data(self.decision_engine_lambda)
        self.decisions_table.grant_write_data(self.decision_engine_lambda)

        # ------------------------------------------------------------------
        # Lambda 2: action router
        # Triggered by the orders stream; only fires for records whose
        # NEW image has status == DECIDED.
        # ------------------------------------------------------------------
        self.action_router_lambda = lambda_.Function(
            self,
            "ActionRouterLambda",
            function_name="action-router",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="action_router.handler",
            code=lambda_.Code.from_asset(LAMBDAS_DIR),
            timeout=Duration.seconds(30),
            environment={
                "ORDERS_TABLE_NAME": self.orders_table.table_name,
                "DECISIONS_TABLE_NAME": self.decisions_table.table_name,
                "SNS_TOPIC_ARN": self.approved_orders_topic.topic_arn,
                "MANUAL_REVIEW_QUEUE_URL": self.manual_review_queue.queue_url,
                "DECLINED_QUEUE_URL": self.declined_orders_queue.queue_url,
            },
        )

        self.action_router_lambda.add_event_source(
            DynamoEventSource(
                self.orders_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=10,
                filters=[
                    lambda_.FilterCriteria.filter(
                        {
                            "dynamodb": {
                                "NewImage": {
                                    "status": {
                                        "S": lambda_.FilterRule.is_equal("DECIDED")
                                    }
                                }
                            }
                        }
                    )
                ],
            )
        )

        # action_router reads/writes the orders table, and both reads
        # (queries the latest reasoning for MANUAL_REVIEW) and writes
        # (RETRY_ALTERNATIVE_PAYMENT audit records) the decisions table.
        self.orders_table.grant_read_write_data(self.action_router_lambda)
        self.decisions_table.grant_read_write_data(self.action_router_lambda)
        self.approved_orders_topic.grant_publish(self.action_router_lambda)
        self.manual_review_queue.grant_send_messages(self.action_router_lambda)
        self.declined_orders_queue.grant_send_messages(self.action_router_lambda)

        # ------------------------------------------------------------------
        # Tag every resource in this stack for cost tracking / attribution
        # ------------------------------------------------------------------
        Tags.of(self).add("project", "growth-assessment")
