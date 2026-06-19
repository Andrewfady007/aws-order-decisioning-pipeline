"""
Lambda 2: Action Router

Triggered by DynamoDB Streams on the "orders" table. Only processes
records whose NEW image has status == DECIDED (i.e. records that
decision_engine.py has just finished deciding); all other stream events
(INSERT of a PENDING order, or later status transitions made by this
Lambda itself) are ignored.

Downstream action per decision:
    AUTO_APPROVE   -> publish to SNS "approved-orders", orders.status = FULFILLED
    MANUAL_REVIEW  -> send to SQS "manual-review-queue", orders.status = QUEUED_FOR_REVIEW
    DECLINE        -> send to SQS "declined-orders-queue", write a RETRY_ALTERNATIVE_PAYMENT
                      audit record, orders.status = CLOSED
"""

import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
sqs = boto3.client("sqs")

ORDERS_TABLE_NAME = os.environ.get("ORDERS_TABLE_NAME", "orders")
DECISIONS_TABLE_NAME = os.environ.get("DECISIONS_TABLE_NAME", "decisions")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
MANUAL_REVIEW_QUEUE_URL = os.environ["MANUAL_REVIEW_QUEUE_URL"]
DECLINED_QUEUE_URL = os.environ["DECLINED_QUEUE_URL"]

orders_table = dynamodb.Table(ORDERS_TABLE_NAME)
decisions_table = dynamodb.Table(DECISIONS_TABLE_NAME)

deserializer = TypeDeserializer()

AUTO_APPROVE = "AUTO_APPROVE"
MANUAL_REVIEW = "MANUAL_REVIEW"
DECLINE = "DECLINE"

STATUS_DECIDED = "DECIDED"


def deserialize_dynamodb_image(image: dict) -> dict:
    """Convert a raw DynamoDB Streams record image (low-level AttributeValue
    format) into a plain Python dict."""
    return {k: deserializer.deserialize(v) for k, v in image.items()}


def to_decimal(value) -> Decimal:
    """DynamoDB requires Decimal (not float) for numeric attributes."""
    return Decimal(str(value))


def json_default(value):
    """Allow Decimal values (from DynamoDB) to be serialized into SNS/SQS
    message bodies as plain numbers."""
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value)} is not JSON serializable")


def get_latest_reasoning(order_id: str) -> str:
    """Look up the reasoning string for an order's most recent decision.

    decision_engine.py writes reasoning only to the decisions table, not
    onto the order record itself, so we query it back here by order_id
    (partition key), sorted descending on decision_timestamp (sort key).
    """
    response = decisions_table.query(
        KeyConditionExpression="order_id = :order_id",
        ExpressionAttributeValues={":order_id": order_id},
        ScanIndexForward=False,
        Limit=1,
    )
    items = response.get("Items", [])
    return items[0]["reasoning"] if items else ""


def log_action(order_id: str, decision: str, action: str, details: dict) -> None:
    """Emit a single structured JSON log line per downstream action."""
    logger.info(
        json.dumps(
            {
                "event": "order_action",
                "order_id": order_id,
                "decision": decision,
                "action": action,
                **details,
            }
        )
    )


def handle_auto_approve(order: dict) -> None:
    order_id = order["order_id"]
    message = {
        "order_id": order_id,
        "basket_value": float(order["basket_value"]),
        "customer_id": order["customer_id"],
    }

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"Order {order_id} approved",
        Message=json.dumps(message, default=json_default),
    )

    orders_table.update_item(
        Key={"order_id": order_id},
        UpdateExpression="SET #status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": "FULFILLED"},
    )

    log_action(order_id, AUTO_APPROVE, "fulfil", {"sns_message": message})


def handle_manual_review(order: dict) -> None:
    order_id = order["order_id"]
    reasoning = get_latest_reasoning(order_id)
    message = {
        "order_id": order_id,
        "basket_value": float(order["basket_value"]),
        "fraud_score": float(order["fraud_score"]),
        "reasoning": reasoning,
    }

    sqs.send_message(
        QueueUrl=MANUAL_REVIEW_QUEUE_URL,
        MessageBody=json.dumps(message, default=json_default),
    )

    orders_table.update_item(
        Key={"order_id": order_id},
        UpdateExpression="SET #status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": "QUEUED_FOR_REVIEW"},
    )

    log_action(order_id, MANUAL_REVIEW, "queue", {"sqs_message": message})


def handle_decline(order: dict) -> None:
    order_id = order["order_id"]
    message = {
        "order_id": order_id,
        "payment_method": order["payment_method"],
        "fraud_score": float(order["fraud_score"]),
    }

    sqs.send_message(
        QueueUrl=DECLINED_QUEUE_URL,
        MessageBody=json.dumps(message, default=json_default),
    )

    retry_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    decisions_table.put_item(
        Item={
            "order_id": order_id,
            "decision_timestamp": retry_timestamp,
            "decision": DECLINE,
            "fraud_score": to_decimal(order["fraud_score"]),
            "basket_value": to_decimal(order["basket_value"]),
            "payment_method": order["payment_method"],
            "customer_tenure_days": int(order["customer_tenure_days"]),
            "action": "RETRY_ALTERNATIVE_PAYMENT",
            "reasoning": "declined order routed for retry with an alternative payment method",
        }
    )

    orders_table.update_item(
        Key={"order_id": order_id},
        UpdateExpression="SET #status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": "CLOSED"},
    )

    log_action(order_id, DECLINE, "retry", {"sqs_message": message})


ACTION_HANDLERS = {
    AUTO_APPROVE: handle_auto_approve,
    MANUAL_REVIEW: handle_manual_review,
    DECLINE: handle_decline,
}


def handler(event, context):
    """DynamoDB Streams entry point. Processes only records whose NEW image
    has status == DECIDED; everything else is ignored."""
    processed = 0
    skipped = 0

    for record in event.get("Records", []):
        if record.get("eventName") not in ("INSERT", "MODIFY"):
            continue

        new_image = record.get("dynamodb", {}).get("NewImage")
        if not new_image:
            continue

        order = deserialize_dynamodb_image(new_image)

        if order.get("status") != STATUS_DECIDED:
            skipped += 1
            continue

        decision = order.get("decision")
        action_handler = ACTION_HANDLERS.get(decision)
        if action_handler is None:
            logger.warning(
                json.dumps(
                    {
                        "event": "unknown_decision",
                        "order_id": order.get("order_id"),
                        "decision": decision,
                    }
                )
            )
            skipped += 1
            continue

        action_handler(order)
        processed += 1

    logger.info(
        json.dumps(
            {
                "event": "batch_summary",
                "records_processed": processed,
                "records_skipped": skipped,
            }
        )
    )

    return {"processed": processed, "skipped": skipped}
