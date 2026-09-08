import csv
import sys
from collections import Counter
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DATA = REPOSITORY_ROOT / "source_data"
MAX_EXAMPLES = 5

DATASETS = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "inventory",
    "returns",
    "promotions",
    "customer_events",
)

VALID_EVENT_TYPES = {
    "PAGE_VIEW",
    "PRODUCT_VIEW",
    "SEARCH",
    "ADD_TO_CART",
    "REMOVE_FROM_CART",
    "CHECKOUT_STARTED",
    "PURCHASE",
}
PRODUCT_REQUIRED_EVENTS = {"PRODUCT_VIEW", "ADD_TO_CART", "REMOVE_FROM_CART"}
VALID_DEVICE_TYPES = {"MOBILE", "DESKTOP", "TABLET"}
VALID_TRAFFIC_SOURCES = {
    "DIRECT",
    "ORGANIC_SEARCH",
    "PAID_SEARCH",
    "SOCIAL",
    "EMAIL",
    "REFERRAL",
}


class ValidationReport:
    def __init__(self) -> None:
        self.failures: dict[str, list[dict[str, object]]] = {
            dataset: [] for dataset in DATASETS
        }
        self.relationships: dict[str, bool] = {}

    def check_records(
        self,
        dataset: str,
        rule: str,
        bad_records: Iterable[object],
    ) -> int:
        count = 0
        examples = []
        for record in bad_records:
            count += 1
            if len(examples) < MAX_EXAMPLES:
                examples.append(record)

        if count:
            self.failures[dataset].append(
                {"rule": rule, "count": count, "examples": examples}
            )
        return count

    def check_count(
        self,
        dataset: str,
        actual: int,
        expected: int,
    ) -> None:
        if actual != expected:
            self.failures[dataset].append(
                {
                    "rule": f"row count must equal {expected}",
                    "count": abs(actual - expected),
                    "examples": [{"expected": expected, "actual": actual}],
                }
            )

    def check_approximate_count(
        self,
        dataset: str,
        actual: int,
        minimum: int,
        maximum: int,
    ) -> None:
        if not minimum <= actual <= maximum:
            self.failures[dataset].append(
                {
                    "rule": f"row count must be between {minimum} and {maximum}",
                    "count": min(abs(actual - minimum), abs(actual - maximum)),
                    "examples": [{"minimum": minimum, "maximum": maximum, "actual": actual}],
                }
            )

    def check_unique(self, dataset: str, rows: list[dict], field: str) -> None:
        seen = set()

        def duplicates() -> Iterable[dict]:
            for row in rows:
                value = row[field]
                if value in seen:
                    yield {field: value}
                else:
                    seen.add(value)

        self.check_records(dataset, f"{field} must be unique", duplicates())

    def print_report(self) -> bool:
        for dataset in DATASETS:
            for failure in self.failures[dataset]:
                print(f"\nDATASET: {dataset}")
                print(f"FAILED RULE: {failure['rule']}")
                print(f"BAD RECORD COUNT: {failure['count']}")
                print("EXAMPLES:")
                for example in failure["examples"]:
                    print(f"  {example}")

        print("\nSOURCE DATA VALIDATION SUMMARY\n")
        for dataset in DATASETS:
            status = "PASS" if not self.failures[dataset] else "FAIL"
            print(f"{dataset:.<28} {status}")

        print("\nRELATIONSHIP CHECKS\n")
        for relationship, passed in self.relationships.items():
            print(f"{relationship:.<40} {'PASS' if passed else 'FAIL'}")

        passed = not any(self.failures.values()) and all(self.relationships.values())
        print(f"\nOVERALL RESULT: {'PASS' if passed else 'FAIL'}")
        return passed


def read_csv(filename: str) -> list[dict[str, str]]:
    with (SOURCE_DATA / filename).open(newline="", encoding="utf-8") as source_file:
        return list(csv.DictReader(source_file))


def read_parquet(filename: str) -> tuple[pa.Table, list[dict]]:
    table = pq.read_table(SOURCE_DATA / filename)

    # Windows may not have an IANA timezone database. Removing only the display
    # timezone preserves the underlying UTC milliseconds used for validation.
    if filename == "customer_events.parquet":
        timestamp_index = table.schema.get_field_index("event_timestamp")
        validation_timestamps = table["event_timestamp"].cast(pa.timestamp("ms"))
        python_table = table.set_column(
            timestamp_index,
            "event_timestamp",
            validation_timestamps,
        )
        return table, python_table.to_pylist()

    return table, table.to_pylist()


def validate() -> bool:
    report = ValidationReport()
    today = date.today()

    customers = read_csv("customers.csv")
    _, products = read_parquet("products.parquet")
    orders = read_csv("orders.csv")
    _, order_items = read_parquet("order_items.parquet")
    payments = read_csv("payments.csv")
    _, shipments = read_parquet("shipments.parquet")
    inventory = read_csv("inventory.csv")
    returns = read_csv("returns.csv")
    _, promotions = read_parquet("promotions.parquet")
    customer_events_table, customer_events = read_parquet(
        "customer_events.parquet"
    )

    customer_ids = {row["customer_id"] for row in customers}
    products_by_id = {row["product_id"]: row for row in products}
    orders_by_id = {row["order_id"]: row for row in orders}
    order_items_by_id = {row["order_item_id"]: row for row in order_items}
    delivered_shipments = {
        row["order_id"]: row
        for row in shipments
        if row["shipment_status"] == "DELIVERED"
    }

    report.check_count("customers", len(customers), 10_000)
    report.check_unique("customers", customers, "customer_id")
    report.check_records(
        "customers",
        "customer_id must not be null or blank",
        (row for row in customers if not row["customer_id"].strip()),
    )

    report.check_count("products", len(products), 5_000)
    report.check_unique("products", products, "product_id")
    report.check_records(
        "products",
        "unit_price must be greater than zero",
        (row for row in products if row["unit_price"] <= 0),
    )

    report.check_count("orders", len(orders), 50_000)
    report.check_unique("orders", orders, "order_id")
    invalid_order_customers = report.check_records(
        "orders",
        "customer_id must exist in customers",
        (row for row in orders if row["customer_id"] not in customer_ids),
    )
    report.check_records(
        "orders",
        "order_total must be greater than zero",
        (row for row in orders if Decimal(row["order_total"]) <= 0),
    )

    report.check_count("order_items", len(order_items), 100_000)
    report.check_unique("order_items", order_items, "order_item_id")
    invalid_item_orders = report.check_records(
        "order_items",
        "order_id must exist in orders",
        (row for row in order_items if row["order_id"] not in orders_by_id),
    )
    invalid_item_products = report.check_records(
        "order_items",
        "product_id must exist in products",
        (row for row in order_items if row["product_id"] not in products_by_id),
    )
    report.check_records(
        "order_items",
        "quantity must be greater than zero",
        (row for row in order_items if row["quantity"] <= 0),
    )
    report.check_records(
        "order_items",
        "unit_price must be greater than zero",
        (row for row in order_items if row["unit_price"] <= 0),
    )
    report.check_records(
        "order_items",
        "discount_amount must be nonnegative",
        (row for row in order_items if row["discount_amount"] < 0),
    )
    report.check_records(
        "order_items",
        "line_amount must equal quantity * unit_price - discount_amount",
        (
            row
            for row in order_items
            if row["line_amount"]
            != row["quantity"] * row["unit_price"] - row["discount_amount"]
        ),
    )

    report.check_approximate_count("payments", len(payments), 45_000, 55_000)
    report.check_unique("payments", payments, "payment_id")
    invalid_payment_orders = report.check_records(
        "payments",
        "order_id must exist in orders",
        (row for row in payments if row["order_id"] not in orders_by_id),
    )
    report.check_records(
        "payments",
        "payment_date must be on or after order_date",
        (
            row
            for row in payments
            if row["order_id"] in orders_by_id
            and date.fromisoformat(row["payment_date"])
            < date.fromisoformat(orders_by_id[row["order_id"]]["order_date"])
        ),
    )
    report.check_records(
        "payments",
        "payment_amount must be nonnegative",
        (row for row in payments if Decimal(row["payment_amount"]) < 0),
    )
    report.check_records(
        "payments",
        "SUCCESS payment_amount must match order_total",
        (
            row
            for row in payments
            if row["payment_status"] == "SUCCESS"
            and row["order_id"] in orders_by_id
            and Decimal(row["payment_amount"])
            != Decimal(orders_by_id[row["order_id"]]["order_total"])
        ),
    )

    report.check_approximate_count("shipments", len(shipments), 40_000, 50_000)
    report.check_unique("shipments", shipments, "shipment_id")
    invalid_shipment_orders = report.check_records(
        "shipments",
        "order_id must exist in orders",
        (row for row in shipments if row["order_id"] not in orders_by_id),
    )
    report.check_records(
        "shipments",
        "CANCELLED orders must not have shipments",
        (
            row
            for row in shipments
            if row["order_id"] in orders_by_id
            and orders_by_id[row["order_id"]]["order_status"] == "CANCELLED"
        ),
    )
    report.check_records(
        "shipments",
        "shipment_date must be on or after order_date",
        (
            row
            for row in shipments
            if row["order_id"] in orders_by_id
            and row["shipment_date"]
            < date.fromisoformat(orders_by_id[row["order_id"]]["order_date"])
        ),
    )
    report.check_records(
        "shipments",
        "expected_delivery_date must be on or after shipment_date",
        (
            row
            for row in shipments
            if row["expected_delivery_date"] < row["shipment_date"]
        ),
    )
    report.check_records(
        "shipments",
        "actual_delivery_date must be on or after shipment_date when present",
        (
            row
            for row in shipments
            if row["actual_delivery_date"] is not None
            and row["actual_delivery_date"] < row["shipment_date"]
        ),
    )
    report.check_records(
        "shipments",
        "DELIVERED shipments must have actual_delivery_date",
        (
            row
            for row in shipments
            if row["shipment_status"] == "DELIVERED"
            and row["actual_delivery_date"] is None
        ),
    )

    report.check_count("inventory", len(inventory), 20_000)
    report.check_unique("inventory", inventory, "inventory_id")
    invalid_inventory_products = report.check_records(
        "inventory",
        "product_id must exist in products",
        (row for row in inventory if row["product_id"] not in products_by_id),
    )
    seen_inventory_pairs = set()

    def duplicate_inventory_pairs() -> Iterable[dict]:
        for row in inventory:
            pair = (row["product_id"], row["warehouse_id"])
            if pair in seen_inventory_pairs:
                yield {"product_id": pair[0], "warehouse_id": pair[1]}
            else:
                seen_inventory_pairs.add(pair)

    report.check_records(
        "inventory",
        "(product_id, warehouse_id) must be unique",
        duplicate_inventory_pairs(),
    )
    report.check_records(
        "inventory",
        "stock_quantity must be nonnegative",
        (row for row in inventory if int(row["stock_quantity"]) < 0),
    )
    report.check_records(
        "inventory",
        "reorder_level must be nonnegative",
        (row for row in inventory if int(row["reorder_level"]) < 0),
    )
    warehouse_cities: dict[str, str] = {}
    inconsistent_warehouses = []
    for row in inventory:
        existing_city = warehouse_cities.setdefault(
            row["warehouse_id"], row["warehouse_city"]
        )
        if existing_city != row["warehouse_city"]:
            inconsistent_warehouses.append(row)
    report.check_records(
        "inventory",
        "warehouse_id must map consistently to one warehouse_city",
        inconsistent_warehouses,
    )

    report.check_approximate_count("returns", len(returns), 4_500, 5_500)
    report.check_unique("returns", returns, "return_id")
    invalid_return_items = report.check_records(
        "returns",
        "order_item_id must exist in order_items",
        (row for row in returns if row["order_item_id"] not in order_items_by_id),
    )
    report.check_records(
        "returns",
        "order_id and product_id must match the referenced order_item",
        (
            row
            for row in returns
            if row["order_item_id"] in order_items_by_id
            and (
                row["order_id"]
                != order_items_by_id[row["order_item_id"]]["order_id"]
                or row["product_id"]
                != order_items_by_id[row["order_item_id"]]["product_id"]
            )
        ),
    )
    report.check_records(
        "returns",
        "returned order must have a DELIVERED shipment",
        (row for row in returns if row["order_id"] not in delivered_shipments),
    )
    report.check_records(
        "returns",
        "return_date must be on or after actual_delivery_date",
        (
            row
            for row in returns
            if row["order_id"] in delivered_shipments
            and date.fromisoformat(row["return_date"])
            < delivered_shipments[row["order_id"]]["actual_delivery_date"]
        ),
    )
    report.check_records(
        "returns",
        "return_quantity must be positive and not exceed purchased quantity",
        (
            row
            for row in returns
            if row["order_item_id"] in order_items_by_id
            and (
                int(row["return_quantity"]) <= 0
                or int(row["return_quantity"])
                > order_items_by_id[row["order_item_id"]]["quantity"]
            )
        ),
    )
    report.check_records(
        "returns",
        "refund_amount must be nonnegative",
        (row for row in returns if Decimal(row["refund_amount"]) < 0),
    )
    report.check_records(
        "returns",
        "REJECTED refund_amount must equal zero",
        (
            row
            for row in returns
            if row["return_status"] == "REJECTED"
            and Decimal(row["refund_amount"]) != 0
        ),
    )
    currency_unit = Decimal("0.01")
    report.check_records(
        "returns",
        "APPROVED/REFUNDED refund must follow proportional line amount rule",
        (
            row
            for row in returns
            if row["return_status"] in {"APPROVED", "REFUNDED"}
            and row["order_item_id"] in order_items_by_id
            and Decimal(row["refund_amount"])
            != (
                order_items_by_id[row["order_item_id"]]["line_amount"]
                * int(row["return_quantity"])
                / order_items_by_id[row["order_item_id"]]["quantity"]
            ).quantize(currency_unit, rounding=ROUND_HALF_UP)
        ),
    )

    report.check_count("promotions", len(promotions), 2_000)
    report.check_unique("promotions", promotions, "promotion_id")
    invalid_promotion_products = report.check_records(
        "promotions",
        "product_id must exist in products",
        (row for row in promotions if row["product_id"] not in products_by_id),
    )
    report.check_records(
        "promotions",
        "discount_type must be PERCENTAGE or FLAT",
        (
            row
            for row in promotions
            if row["discount_type"] not in {"PERCENTAGE", "FLAT"}
        ),
    )
    report.check_records(
        "promotions",
        "PERCENTAGE discount must be greater than 0 and at most 50",
        (
            row
            for row in promotions
            if row["discount_type"] == "PERCENTAGE"
            and not 0 < row["discount_value"] <= 50
        ),
    )
    report.check_records(
        "promotions",
        "FLAT discount must be positive and less than product unit_price",
        (
            row
            for row in promotions
            if row["discount_type"] == "FLAT"
            and row["product_id"] in products_by_id
            and not 0
            < row["discount_value"]
            < products_by_id[row["product_id"]]["unit_price"]
        ),
    )
    report.check_records(
        "promotions",
        "end_date must be on or after start_date",
        (row for row in promotions if row["end_date"] < row["start_date"]),
    )

    def expected_promotion_status(row: dict) -> str:
        if today < row["start_date"]:
            return "SCHEDULED"
        if today <= row["end_date"]:
            return "ACTIVE"
        return "EXPIRED"

    report.check_records(
        "promotions",
        "promotion_status must match start_date and end_date",
        (
            row
            for row in promotions
            if row["promotion_status"] != expected_promotion_status(row)
        ),
    )

    report.check_count("customer_events", len(customer_events), 200_000)
    report.check_unique("customer_events", customer_events, "event_id")
    invalid_event_customers = report.check_records(
        "customer_events",
        "customer_id must exist in customers",
        (
            row
            for row in customer_events
            if row["customer_id"] not in customer_ids
        ),
    )
    invalid_required_event_products = report.check_records(
        "customer_events",
        "product-related events must reference an existing product_id",
        (
            row
            for row in customer_events
            if row["event_type"] in PRODUCT_REQUIRED_EVENTS
            and row["product_id"] not in products_by_id
        ),
    )
    invalid_nonnull_event_products = report.check_records(
        "customer_events",
        "non-null product_id must exist in products",
        (
            row
            for row in customer_events
            if row["product_id"] is not None
            and row["product_id"] not in products_by_id
        ),
    )
    report.check_records(
        "customer_events",
        "event_timestamp must not be in the future",
        (
            row
            for row in customer_events
            if row["event_timestamp"] > datetime.utcnow()
        ),
    )
    report.check_records(
        "customer_events",
        "event_type must be valid",
        (
            row
            for row in customer_events
            if row["event_type"] not in VALID_EVENT_TYPES
        ),
    )
    report.check_records(
        "customer_events",
        "device_type must be valid",
        (
            row
            for row in customer_events
            if row["device_type"] not in VALID_DEVICE_TYPES
        ),
    )
    report.check_records(
        "customer_events",
        "traffic_source must be valid",
        (
            row
            for row in customer_events
            if row["traffic_source"] not in VALID_TRAFFIC_SOURCES
        ),
    )
    session_customers: dict[str, set[str]] = {}
    session_counts: Counter[str] = Counter()
    for row in customer_events:
        session_customers.setdefault(row["session_id"], set()).add(
            row["customer_id"]
        )
        session_counts[row["session_id"]] += 1
    report.check_records(
        "customer_events",
        "each session_id must belong to exactly one customer",
        (
            {"session_id": session_id, "customer_ids": sorted(customers)}
            for session_id, customers in session_customers.items()
            if len(customers) > 1
        ),
    )
    multi_event_session_count = sum(count > 1 for count in session_counts.values())
    if multi_event_session_count == 0:
        report.failures["customer_events"].append(
            {
                "rule": "at least one session must contain multiple events",
                "count": 1,
                "examples": [],
            }
        )

    print(f"Unique customer-event sessions: {len(session_counts)}")
    print(f"Sessions containing multiple events: {multi_event_session_count}")
    print(f"Customer-event timestamp schema: {customer_events_table.schema.field('event_timestamp').type}")

    report.relationships = {
        "customers -> orders": invalid_order_customers == 0,
        "orders -> order_items": invalid_item_orders == 0,
        "order_items -> products": invalid_item_products == 0,
        "orders -> payments": invalid_payment_orders == 0,
        "orders -> shipments": invalid_shipment_orders == 0,
        "order_items -> returns": invalid_return_items == 0,
        "products -> inventory": invalid_inventory_products == 0,
        "products -> promotions": invalid_promotion_products == 0,
        "customers -> customer_events": invalid_event_customers == 0,
        "products -> customer_events": (
            invalid_required_event_products == 0
            and invalid_nonnull_event_products == 0
        ),
    }

    return report.print_report()


if __name__ == "__main__":
    try:
        validation_passed = validate()
    except (FileNotFoundError, KeyError, ValueError, pa.ArrowException) as error:
        print(f"SOURCE DATA VALIDATION ERROR: {error}")
        sys.exit(1)

    sys.exit(0 if validation_passed else 1)
