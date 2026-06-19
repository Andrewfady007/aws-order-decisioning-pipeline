from aws_cdk import (
    Stack,
    Tags,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_sns as sns,
    aws_sqs as sqs,
)
from constructs import Construct


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
        # Tag every resource in this stack for cost tracking / attribution
        # ------------------------------------------------------------------
        Tags.of(self).add("project", "growth-assessment")
