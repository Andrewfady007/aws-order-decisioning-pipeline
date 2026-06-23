"""
End-to-end smoke test for the order decisioning pipeline.

Inserts a handful of synthetic test orders directly into the "orders"
table - one engineered to trigger each decision branch (AUTO_APPROVE,
MANUAL_REVIEW, DECLINE) - then polls until the live pipeline (decision-engine
-> action-router, both running as real Lambdas triggered by DynamoDB
Streams) has processed them, and verifies:

  - the order's final status matches what that decision should produce
  - a matching audit record exists in the "decisions" table with the
    correct decision and a non-empty reasoning string
  - DECLINE orders additionally get a RETRY_ALTERNATIVE_PAYMENT audit record

Prints PASS if every check succeeds, otherwise FAIL with details.
"""

import sys
import time
import uuid
from decimal import Decimal

import boto3

ORDERS_TABLE_NAME = "orders"
DECISIONS_TABLE_NAME = "decisions"
POLL_INTERVAL_SECONDS = 2
POLL_TIMEOUT_SECONDS = 60

dynamodb = boto3.resource("dynamodb")
orders_table = dynamodb.Table(ORDERS_TABLE_NAME)
decisions_table = dynamodb.Table(DECISIONS_TABLE_NAME)


def make_test_order_id() -> str:
    return f"ORD-TEST-{uuid.uuid4().hex[:8]}"


# Each case is engineered to land squarely in one decision branch, per the
# rules in lambdas/decision_engine.py.
TEST_CASES = [
    {
        "name": "auto_approve",
        "expected_decision": "AUTO_APPROVE",
        "expected_status": "FULFILLED",
        "order": {
            "customer_tenure_days": Decimal("900"),
            "basket_value": Decimal("75.00"),
            "payment_method": "credit_card",
            "sku_count": Decimal("3"),
            "fraud_score": Decimal("0.10"),
        },
    },
    {
        "name": "manual_review",
        "expected_decision": "MANUAL_REVIEW",
        "expected_status": "QUEUED_FOR_REVIEW",
        "order": {
            "customer_tenure_days": Decimal("900"),
            "basket_value": Decimal("650.00"),  # > 500 -> MANUAL_REVIEW
            "payment_method": "credit_card",
            "sku_count": Decimal("4"),
            "fraud_score": Decimal("0.10"),
        },
    },
    {
        "name": "decline",
        "expected_decision": "DECLINE",
        "expected_status": "CLOSED",
        "order": {
            "customer_tenure_days": Decimal("900"),
            "basket_value": Decimal("75.00"),
            "payment_method": "credit_card",
            "sku_count": Decimal("3"),
            "fraud_score": Decimal("0.85"),  # > 0.7 -> DECLINE
        },
    },
]


def insert_test_order(order_id: str, customer_id: str, fields: dict) -> None:
    orders_table.put_item(
        Item={
            "order_id": order_id,
            "customer_id": customer_id,
            "status": "PENDING",
            "created_at": "2026-01-01T00:00:00Z",
            **fields,
        }
    )


def poll_for_status(order_id: str, timeout: int) -> str | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = orders_table.get_item(Key={"order_id": order_id})
        item = response.get("Item")
        if item and item.get("status") not in ("PENDING", "DECIDED"):
            return item["status"]
        time.sleep(POLL_INTERVAL_SECONDS)
    return None


def get_decision_records(order_id: str) -> list[dict]:
    response = decisions_table.query(
        KeyConditionExpression="order_id = :order_id",
        ExpressionAttributeValues={":order_id": order_id},
    )
    return response.get("Items", [])


def run_case(case: dict) -> tuple[bool, str]:
    order_id = make_test_order_id()
    customer_id = "CUST-TEST"
    insert_test_order(order_id, customer_id, case["order"])

    final_status = poll_for_status(order_id, POLL_TIMEOUT_SECONDS)
    if final_status is None:
        return False, f"[{case['name']}] {order_id}: timed out waiting for a final status"

    if final_status != case["expected_status"]:
        return False, (
            f"[{case['name']}] {order_id}: expected status "
            f"{case['expected_status']!r}, got {final_status!r}"
        )

    decision_records = get_decision_records(order_id)
    matching = [r for r in decision_records if r.get("decision") == case["expected_decision"]]
    if not matching:
        return False, (
            f"[{case['name']}] {order_id}: no decisions-table record found with "
            f"decision={case['expected_decision']!r} (found: {decision_records})"
        )

    if not matching[0].get("reasoning"):
        return False, f"[{case['name']}] {order_id}: decision record missing reasoning string"

    if case["expected_decision"] == "DECLINE":
        retry_records = [r for r in decision_records if r.get("action") == "RETRY_ALTERNATIVE_PAYMENT"]
        if not retry_records:
            return False, (
                f"[{case['name']}] {order_id}: expected a RETRY_ALTERNATIVE_PAYMENT "
                f"audit record, found none"
            )

    return True, f"[{case['name']}] {order_id}: OK (status={final_status}, decision={case['expected_decision']})"


def main() -> int:
    print("Running end-to-end pipeline smoke test...\n")

    all_passed = True
    for case in TEST_CASES:
        passed, message = run_case(case)
        print(("  PASS  " if passed else "  FAIL  ") + message)
        all_passed = all_passed and passed

    print()
    if all_passed:
        print("PASS")
        return 0
    else:
        print("FAIL")
        return 1


if __name__ == "__main__":
    sys.exit(main())
