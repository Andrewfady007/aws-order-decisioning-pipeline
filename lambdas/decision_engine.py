"""
Lambda 1: Decision Engine

Triggered by DynamoDB Streams on the "orders" table. For every new/modified
order record, applies the fraud/risk decisioning rules below (checked in
order, first match wins), writes the decision back onto the order, and
appends a full audit record to the "decisions" table.

Decision rules (evaluated in this exact order):
    1. fraud_score > 0.7                                         -> DECLINE
    2. 0.4 <= fraud_score <= 0.7                                  -> MANUAL_REVIEW
    3. basket_value > 500                                         -> MANUAL_REVIEW
    4. payment_method == prepaid_card AND customer_tenure_days < 30 -> MANUAL_REVIEW
    5. customer_tenure_days < 7 AND basket_value > 200            -> MANUAL_REVIEW
    6. otherwise                                                  -> AUTO_APPROVE
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

ORDERS_TABLE_NAME = os.environ.get("ORDERS_TABLE_NAME", "orders")
DECISIONS_TABLE_NAME = os.environ.get("DECISIONS_TABLE_NAME", "decisions")

orders_table = dynamodb.Table(ORDERS_TABLE_NAME)
decisions_table = dynamodb.Table(DECISIONS_TABLE_NAME)

deserializer = TypeDeserializer()

# Decision outcomes
DECLINE = "DECLINE"
MANUAL_REVIEW = "MANUAL_REVIEW"
AUTO_APPROVE = "AUTO_APPROVE"

FRAUD_DECLINE_THRESHOLD = 0.7
FRAUD_REVIEW_LOW = 0.4
FRAUD_REVIEW_HIGH = 0.7
HIGH_BASKET_VALUE_THRESHOLD = 500
NEW_CUSTOMER_PREPAID_TENURE_DAYS = 30
VERY_NEW_CUSTOMER_TENURE_DAYS = 7
VERY_NEW_CUSTOMER_BASKET_THRESHOLD = 200


def deserialize_dynamodb_image(image: dict) -> dict:
    """Convert a raw DynamoDB Streams record image (low-level AttributeValue
    format) into a plain Python dict."""
    return {k: deserializer.deserialize(v) for k, v in image.items()}


def decide(order: dict) -> tuple[str, str]:
    """Apply the decisioning rules to a single order.

    Returns a (decision, reasoning) tuple. Rules are checked in order;
    the first matching rule wins.
    """
    fraud_score = float(order["fraud_score"])
    basket_value = float(order["basket_value"])
    payment_method = order["payment_method"]
    customer_tenure_days = int(order["customer_tenure_days"])

    if fraud_score > FRAUD_DECLINE_THRESHOLD:
        return (
            DECLINE,
            f"fraud_score {fraud_score:.4f} exceeds decline threshold "
            f"of {FRAUD_DECLINE_THRESHOLD}",
        )

    if FRAUD_REVIEW_LOW <= fraud_score <= FRAUD_REVIEW_HIGH:
        return (
            MANUAL_REVIEW,
            f"fraud_score {fraud_score:.4f} falls within the manual review "
            f"band [{FRAUD_REVIEW_LOW}, {FRAUD_REVIEW_HIGH}]",
        )

    if basket_value > HIGH_BASKET_VALUE_THRESHOLD:
        return (
            MANUAL_REVIEW,
            f"basket_value {basket_value:.2f} exceeds high-value threshold "
            f"of {HIGH_BASKET_VALUE_THRESHOLD}",
        )

    if (
        payment_method == "prepaid_card"
        and customer_tenure_days < NEW_CUSTOMER_PREPAID_TENURE_DAYS
    ):
        return (
            MANUAL_REVIEW,
            f"payment_method is prepaid_card and customer_tenure_days "
            f"{customer_tenure_days} is below {NEW_CUSTOMER_PREPAID_TENURE_DAYS} "
            f"(new customer using prepaid card)",
        )

    if (
        customer_tenure_days < VERY_NEW_CUSTOMER_TENURE_DAYS
        and basket_value > VERY_NEW_CUSTOMER_BASKET_THRESHOLD
    ):
        return (
            MANUAL_REVIEW,
            f"customer_tenure_days {customer_tenure_days} is below "
            f"{VERY_NEW_CUSTOMER_TENURE_DAYS} and basket_value {basket_value:.2f} "
            f"exceeds {VERY_NEW_CUSTOMER_BASKET_THRESHOLD} "
            f"(very new customer placing a large order)",
        )

    return (
        AUTO_APPROVE,
        "no fraud, value, payment-method, or tenure risk signals triggered; "
        "order meets auto-approval criteria",
    )


def to_decimal(value) -> Decimal:
    """DynamoDB requires Decimal (not float) for numeric attributes."""
    return Decimal(str(value))


def persist_decision(order: dict, decision: str, reasoning: str) -> dict:
    """Update the order's status in the orders table and append a full
    audit record to the decisions table. Returns the audit record written."""
    decision_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    order_id = order["order_id"]

    orders_table.update_item(
        Key={"order_id": order_id},
        UpdateExpression="SET #status = :status, decision = :decision",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "DECIDED",
            ":decision": decision,
        },
    )

    audit_record = {
        "order_id": order_id,
        "decision_timestamp": decision_timestamp,
        "decision": decision,
        "fraud_score": to_decimal(order["fraud_score"]),
        "basket_value": to_decimal(order["basket_value"]),
        "payment_method": order["payment_method"],
        "customer_tenure_days": int(order["customer_tenure_days"]),
        "reasoning": reasoning,
    }
    decisions_table.put_item(Item=audit_record)

    return audit_record


def log_decision(order_id: str, decision: str, reasoning: str, order: dict) -> None:
    """Emit a single structured JSON log line per decision, so CloudWatch
    Logs Insights can filter/aggregate on these fields directly."""
    logger.info(
        json.dumps(
            {
                "event": "order_decision",
                "order_id": order_id,
                "decision": decision,
                "reasoning": reasoning,
                "fraud_score": float(order["fraud_score"]),
                "basket_value": float(order["basket_value"]),
                "payment_method": order["payment_method"],
                "customer_tenure_days": int(order["customer_tenure_days"]),
            }
        )
    )


def handler(event, context):
    """DynamoDB Streams entry point. Processes each INSERT/MODIFY record,
    skipping orders that aren't in PENDING status (e.g. already decided)."""
    processed = 0
    skipped = 0

    for record in event.get("Records", []):
        if record.get("eventName") not in ("INSERT", "MODIFY"):
            continue

        new_image = record.get("dynamodb", {}).get("NewImage")
        if not new_image:
            continue

        order = deserialize_dynamodb_image(new_image)

        if order.get("status") != "PENDING":
            skipped += 1
            continue

        decision, reasoning = decide(order)
        persist_decision(order, decision, reasoning)
        log_decision(order["order_id"], decision, reasoning, order)
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
