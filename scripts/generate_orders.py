"""
Generate a realistic synthetic dataset of customer orders for the order
decisioning pipeline, including a small share of fraud-shaped anomalies.

Output: data/orders.csv (100,000 rows)
"""

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

NUM_ORDERS = 100_000
NUM_CUSTOMERS = 5_000
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "orders.csv"

# Population mix for fraud_score bands.
PCT_NORMAL = 0.96
PCT_SUSPICIOUS = 0.02
PCT_FRAUD = 0.02

PAYMENT_METHODS_NORMAL = ["credit_card", "debit_card", "prepaid_card"]
PAYMENT_WEIGHTS_NORMAL = [0.60, 0.25, 0.15]

FIELDNAMES = [
    "order_id",
    "customer_id",
    "customer_tenure_days",
    "basket_value",
    "payment_method",
    "sku_count",
    "fraud_score",
    "status",
    "created_at",
]


def random_timestamp_within_last_year() -> str:
    """Return an ISO-8601 timestamp uniformly spread over the last 12 months."""
    now = datetime.now(timezone.utc)
    days_ago = random.uniform(0, 365)
    ts = now - timedelta(days=days_ago)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def normal_order() -> dict:
    """A typical, low-risk order. fraud_score stays below 0.3."""
    payment_method = random.choices(
        PAYMENT_METHODS_NORMAL, weights=PAYMENT_WEIGHTS_NORMAL, k=1
    )[0]

    # Most baskets cluster between $20-$200, with a long tail up to $1500.
    if random.random() < 0.85:
        basket_value = round(random.uniform(20, 200), 2)
    else:
        basket_value = round(random.uniform(5, 1500), 2)

    return {
        "customer_tenure_days": random.randint(1, 1825),
        "basket_value": basket_value,
        "payment_method": payment_method,
        "sku_count": random.randint(1, 10),
        "fraud_score": round(random.uniform(0.0, 0.3), 4),
    }


def suspicious_order() -> dict:
    """Mid-risk order: elevated fraud_score, otherwise fairly normal shape."""
    payment_method = random.choices(
        PAYMENT_METHODS_NORMAL, weights=PAYMENT_WEIGHTS_NORMAL, k=1
    )[0]

    return {
        "customer_tenure_days": random.randint(1, 1825),
        "basket_value": round(random.uniform(20, 400), 2),
        "payment_method": payment_method,
        "sku_count": random.randint(1, 10),
        "fraud_score": round(random.uniform(0.4, 0.69), 4),
    }


def fraud_shaped_order() -> dict:
    """High-risk order: high fraud_score, skewed toward prepaid cards,
    high basket values, and new/thin customer tenure - the classic
    fraud-attack profile (new account, expensive goods, prepaid funding)."""
    payment_method = random.choices(
        ["prepaid_card", "credit_card", "debit_card"],
        weights=[0.70, 0.20, 0.10],
        k=1,
    )[0]

    return {
        # Fraud rings disproportionately use freshly created accounts.
        "customer_tenure_days": random.randint(1, 60),
        "basket_value": round(random.uniform(500, 1500), 2),
        "payment_method": payment_method,
        "sku_count": random.randint(1, 10),
        "fraud_score": round(random.uniform(0.7, 1.0), 4),
    }


def generate_orders(num_orders: int = NUM_ORDERS) -> list[dict]:
    num_suspicious = int(num_orders * PCT_SUSPICIOUS)
    num_fraud = int(num_orders * PCT_FRAUD)
    num_normal = num_orders - num_suspicious - num_fraud

    builders = (
        [normal_order] * num_normal
        + [suspicious_order] * num_suspicious
        + [fraud_shaped_order] * num_fraud
    )
    random.shuffle(builders)

    orders = []
    for i, builder in enumerate(builders, start=1):
        order = builder()
        order["order_id"] = f"ORD-{i:06d}"
        order["customer_id"] = f"CUST-{random.randint(1, NUM_CUSTOMERS):04d}"
        order["status"] = "PENDING"
        order["created_at"] = random_timestamp_within_last_year()
        orders.append(order)

    return orders


def write_csv(orders: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(orders)


def print_summary(orders: list[dict]) -> None:
    total = len(orders)

    counts_by_payment: dict[str, int] = {}
    fraud_score_sum = 0.0
    for order in orders:
        method = order["payment_method"]
        counts_by_payment[method] = counts_by_payment.get(method, 0) + 1
        fraud_score_sum += order["fraud_score"]

    print(f"\nGenerated {total:,} orders -> {OUTPUT_PATH}")
    print("\nCount by payment method:")
    for method, count in sorted(counts_by_payment.items(), key=lambda kv: -kv[1]):
        pct = 100 * count / total
        print(f"  {method:<14} {count:>7,}  ({pct:5.1f}%)")

    avg_fraud_score = fraud_score_sum / total
    print(f"\nAverage fraud score: {avg_fraud_score:.4f}")

    high_risk = sum(1 for o in orders if o["fraud_score"] >= 0.7)
    mid_risk = sum(1 for o in orders if 0.4 <= o["fraud_score"] < 0.7)
    print(f"  Orders with fraud_score >= 0.7 (fraud-shaped): {high_risk:,} ({100 * high_risk / total:.2f}%)")
    print(f"  Orders with fraud_score 0.4-0.69 (suspicious): {mid_risk:,} ({100 * mid_risk / total:.2f}%)")


def main() -> None:
    random.seed(42)  # reproducible dataset across runs
    orders = generate_orders(NUM_ORDERS)
    write_csv(orders, OUTPUT_PATH)
    print_summary(orders)


if __name__ == "__main__":
    main()
