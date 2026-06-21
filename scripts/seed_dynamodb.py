"""
Upload data/orders.csv into the DynamoDB "orders" table.

Writes in batches of 25 (the DynamoDB batch_write_item limit), throttled to
roughly 5 writes/second so we stay within the table's provisioned 5 WCU
(matching the Free Tier-sized capacity set up in the CDK stack). For large
bulk loads, temporarily bump the table's write capacity and pass a higher
WRITES_PER_SECOND value to avoid a multi-hour run. Any items
that DynamoDB returns as unprocessed (e.g. due to throttling) are retried
once after the batch completes.
"""

import argparse
import csv
import time
from decimal import Decimal
from pathlib import Path

import boto3

TABLE_NAME = "orders"
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "orders.csv"
BATCH_SIZE = 25
WRITES_PER_SECOND = 5  # matches orders table's steady-state provisioned 5 WCU
PROGRESS_INTERVAL = 1000

NUMERIC_FIELDS = {"customer_tenure_days", "basket_value", "sku_count", "fraud_score"}

dynamodb = boto3.resource("dynamodb")


def read_orders(csv_path: Path) -> list[dict]:
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        orders = []
        for row in reader:
            for field in NUMERIC_FIELDS:
                row[field] = Decimal(row[field])
            orders.append(row)
        return orders


def chunk(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def write_batch(table_name: str, items: list[dict]) -> list[dict]:
    """Write one batch via batch_write_item. Returns any unprocessed items."""
    request_items = {
        table_name: [{"PutRequest": {"Item": item}} for item in items]
    }
    response = dynamodb.meta.client.batch_write_item(RequestItems=request_items)
    unprocessed = response.get("UnprocessedItems", {}).get(table_name, [])
    return [entry["PutRequest"]["Item"] for entry in unprocessed]


def seed(orders: list[dict]) -> None:
    total = len(orders)
    uploaded = 0
    failed_after_retry = []

    seconds_per_batch = BATCH_SIZE / WRITES_PER_SECOND

    for batch in chunk(orders, BATCH_SIZE):
        batch_start = time.monotonic()

        try:
            unprocessed = write_batch(TABLE_NAME, batch)
        except Exception as exc:
            print(f"  Batch failed with error: {exc}. Retrying once...")
            unprocessed = batch

        if unprocessed:
            time.sleep(1)  # brief backoff before retrying
            try:
                unprocessed = write_batch(TABLE_NAME, unprocessed)
            except Exception as exc:
                print(f"  Retry failed with error: {exc}")
            if unprocessed:
                failed_after_retry.extend(unprocessed)

        uploaded += len(batch) - len(unprocessed)

        if uploaded % PROGRESS_INTERVAL == 0 or uploaded == total:
            print(f"  Uploaded {uploaded:,} / {total:,} records...")

        # Throttle so this batch + retry don't exceed ~5 writes/sec overall.
        elapsed = time.monotonic() - batch_start
        sleep_time = seconds_per_batch - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    print(f"\nDone. Total records uploaded: {uploaded:,} / {total:,}")
    if failed_after_retry:
        print(f"Failures (after retry): {len(failed_after_retry)}")
        for item in failed_after_retry[:10]:
            print(f"  - {item.get('order_id')}")
        if len(failed_after_retry) > 10:
            print(f"  ... and {len(failed_after_retry) - 10} more")
    else:
        print("Failures: none")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only upload the first N orders (useful for a quick test run).",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Skip the first N orders (useful for resuming an interrupted run).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Reading orders from {CSV_PATH}...")
    orders = read_orders(CSV_PATH)
    if args.skip:
        orders = orders[args.skip :]
        print(f"Skipping first {args.skip:,} orders (resume mode).")
    if args.limit is not None:
        orders = orders[: args.limit]

    print(f"Loaded {len(orders):,} orders. Uploading to '{TABLE_NAME}' table...\n")
    seed(orders)


if __name__ == "__main__":
    main()
