import argparse
import csv
import random
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from data_generator.schemas import (
    CUSTOMER_COLUMNS,
    CUSTOMER_EVENT_COLUMNS,
    INVENTORY_COLUMNS,
    ORDER_COLUMNS,
    ORDER_ITEM_COLUMNS,
    PAYMENT_COLUMNS,
    PRODUCT_COLUMNS,
    PROMOTION_COLUMNS,
    RETURN_COLUMNS,
    SHIPMENT_COLUMNS,
)


CUSTOMER_COUNT = 10_000
PRODUCT_COUNT = 5_000
ORDER_COUNT = 50_000
ORDER_ITEM_COUNT = 100_000
PAYMENT_COUNT = 50_000
INVENTORY_COUNT = 20_000
RETURN_COUNT = 5_000
PROMOTION_COUNT = 2_000
CUSTOMER_EVENT_COUNT = 200_000
CUSTOMER_EVENT_SESSION_COUNT = 50_000
CUSTOMER_OUTPUT_PATH = Path("source_data/customers.csv")
PRODUCT_OUTPUT_PATH = Path("source_data/products.parquet")
ORDER_OUTPUT_PATH = Path("source_data/orders.csv")
ORDER_ITEM_OUTPUT_PATH = Path("source_data/order_items.parquet")
PAYMENT_OUTPUT_PATH = Path("source_data/payments.csv")
SHIPMENT_OUTPUT_PATH = Path("source_data/shipments.parquet")
INVENTORY_OUTPUT_PATH = Path("source_data/inventory.csv")
RETURN_OUTPUT_PATH = Path("source_data/returns.csv")
PROMOTION_OUTPUT_PATH = Path("source_data/promotions.parquet")
CUSTOMER_EVENT_OUTPUT_PATH = Path("source_data/customer_events.parquet")
RANDOM_SEED = 42

ORDER_STATUSES = ("PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED")
PAYMENT_METHODS = (
    "UPI",
    "CREDIT_CARD",
    "DEBIT_CARD",
    "NET_BANKING",
    "WALLET",
    "COD",
)
SHIPPING_PROVIDERS = (
    "Delhivery",
    "Blue Dart",
    "DTDC",
    "Ecom Express",
    "India Post",
)
OPEN_SHIPMENT_STATUSES = ("SHIPPED", "IN_TRANSIT", "DELAYED")
WAREHOUSES = {
    "WH001": "Mumbai",
    "WH002": "Delhi",
    "WH003": "Bengaluru",
    "WH004": "Hyderabad",
    "WH005": "Chennai",
    "WH006": "Kolkata",
    "WH007": "Pune",
    "WH008": "Ahmedabad",
}
RETURN_REASONS = (
    "DAMAGED",
    "WRONG_ITEM",
    "DEFECTIVE",
    "SIZE_ISSUE",
    "NOT_AS_DESCRIBED",
    "CUSTOMER_CHANGED_MIND",
)
RETURN_STATUSES = ("REQUESTED", "APPROVED", "PICKED_UP", "REFUNDED", "REJECTED")
PROMOTION_NAMES = (
    "Festival Savings",
    "Weekend Special",
    "Season End Sale",
    "Member Exclusive",
    "Flash Deal",
    "New Arrival Offer",
)
DEVICE_TYPES = ("MOBILE", "DESKTOP", "TABLET")
TRAFFIC_SOURCES = (
    "DIRECT",
    "ORGANIC_SEARCH",
    "PAID_SEARCH",
    "SOCIAL",
    "EMAIL",
    "REFERRAL",
)

FIRST_NAMES = (
    "Aarav",
    "Aditi",
    "Aditya",
    "Ananya",
    "Arjun",
    "Diya",
    "Ishaan",
    "Kavya",
    "Meera",
    "Neha",
    "Nikhil",
    "Priya",
    "Rahul",
    "Riya",
    "Rohan",
    "Saanvi",
    "Siddharth",
    "Sneha",
    "Vikram",
    "Vivaan",
)

LAST_NAMES = (
    "Agarwal",
    "Bose",
    "Chatterjee",
    "Desai",
    "Gupta",
    "Iyer",
    "Jain",
    "Joshi",
    "Kapoor",
    "Khan",
    "Kulkarni",
    "Mehta",
    "Menon",
    "Nair",
    "Patel",
    "Rao",
    "Reddy",
    "Shah",
    "Sharma",
    "Singh",
)

CITY_STATE_PAIRS = (
    ("Ahmedabad", "Gujarat"),
    ("Bengaluru", "Karnataka"),
    ("Bhopal", "Madhya Pradesh"),
    ("Bhubaneswar", "Odisha"),
    ("Chandigarh", "Chandigarh"),
    ("Chennai", "Tamil Nadu"),
    ("Gurugram", "Haryana"),
    ("Hyderabad", "Telangana"),
    ("Jaipur", "Rajasthan"),
    ("Kochi", "Kerala"),
    ("Kolkata", "West Bengal"),
    ("Lucknow", "Uttar Pradesh"),
    ("Mumbai", "Maharashtra"),
    ("New Delhi", "Delhi"),
    ("Patna", "Bihar"),
    ("Pune", "Maharashtra"),
)

PRODUCT_CATALOG = {
    "Electronics": {
        "brands": ("Boat", "Dell", "HP", "Lenovo", "Samsung", "Sony"),
        "items": (
            "Bluetooth Speaker",
            "Laptop",
            "Smartphone",
            "Smartwatch",
            "Wireless Earbuds",
        ),
        "price_range": (1_299_00, 89_999_00),
    },
    "Clothing": {
        "brands": ("Allen Solly", "Biba", "Levi's", "Manyavar", "Puma", "Wrogn"),
        "items": ("Casual Shirt", "Cotton Kurta", "Denim Jeans", "Jacket", "T-Shirt"),
        "price_range": (499_00, 7_999_00),
    },
    "Home & Kitchen": {
        "brands": ("Borosil", "Havells", "Milton", "Philips", "Prestige", "Usha"),
        "items": ("Cookware Set", "Electric Kettle", "Mixer Grinder", "Storage Set", "Table Fan"),
        "price_range": (399_00, 14_999_00),
    },
    "Beauty": {
        "brands": ("Biotique", "Lakme", "Mamaearth", "Maybelline", "Nivea", "Plum"),
        "items": ("Face Wash", "Hair Serum", "Moisturizer", "Shampoo", "Sunscreen"),
        "price_range": (149_00, 2_999_00),
    },
    "Sports": {
        "brands": ("Adidas", "Cosco", "Decathlon", "Nivia", "Puma", "Yonex"),
        "items": ("Badminton Racquet", "Cricket Bat", "Football", "Running Shoes", "Yoga Mat"),
        "price_range": (299_00, 12_999_00),
    },
    "Books": {
        "brands": ("HarperCollins", "Jaico", "Penguin", "Rupa", "Scholastic", "Westland"),
        "items": ("Business Book", "Children's Book", "Cookbook", "Fiction Novel", "Study Guide"),
        "price_range": (99_00, 1_999_00),
    },
}

PRODUCT_SCHEMA = pa.schema(
    [
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("product_name", pa.string(), nullable=False),
        pa.field("category", pa.string(), nullable=False),
        pa.field("brand", pa.string(), nullable=False),
        pa.field("unit_price", pa.decimal128(10, 2), nullable=False),
        pa.field("supplier_id", pa.string(), nullable=False),
        pa.field("created_date", pa.date32(), nullable=False),
        pa.field("product_status", pa.string(), nullable=False),
    ]
)

ORDER_ITEM_SCHEMA = pa.schema(
    [
        pa.field("order_item_id", pa.string(), nullable=False),
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("quantity", pa.int16(), nullable=False),
        pa.field("unit_price", pa.decimal128(10, 2), nullable=False),
        pa.field("discount_amount", pa.decimal128(12, 2), nullable=False),
        pa.field("line_amount", pa.decimal128(12, 2), nullable=False),
    ]
)

SHIPMENT_SCHEMA = pa.schema(
    [
        pa.field("shipment_id", pa.string(), nullable=False),
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("shipment_date", pa.date32(), nullable=False),
        pa.field("expected_delivery_date", pa.date32(), nullable=False),
        pa.field("actual_delivery_date", pa.date32(), nullable=True),
        pa.field("shipping_provider", pa.string(), nullable=False),
        pa.field("tracking_id", pa.string(), nullable=False),
        pa.field("shipment_status", pa.string(), nullable=False),
    ]
)

PROMOTION_SCHEMA = pa.schema(
    [
        pa.field("promotion_id", pa.string(), nullable=False),
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("promotion_name", pa.string(), nullable=False),
        pa.field("discount_type", pa.string(), nullable=False),
        pa.field("discount_value", pa.decimal128(10, 2), nullable=False),
        pa.field("start_date", pa.date32(), nullable=False),
        pa.field("end_date", pa.date32(), nullable=False),
        pa.field("promotion_status", pa.string(), nullable=False),
    ]
)

CUSTOMER_EVENT_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("customer_id", pa.string(), nullable=False),
        pa.field("product_id", pa.string(), nullable=True),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("event_timestamp", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("session_id", pa.string(), nullable=False),
        pa.field("device_type", pa.string(), nullable=False),
        pa.field("traffic_source", pa.string(), nullable=False),
    ]
)


def generate_phone_number(rng: random.Random, used_numbers: set[str]) -> str:
    while True:
        number = f"+91{rng.choice('6789')}{rng.randint(0, 999_999_999):09d}"
        if number not in used_numbers:
            used_numbers.add(number)
            return number


def generate_signup_date(rng: random.Random) -> str:
    end_date = date.today()
    start_date = end_date - timedelta(days=8 * 365)
    signup_date = start_date + timedelta(days=rng.randint(0, (end_date - start_date).days))
    return signup_date.isoformat()


def generate_customers(
    row_count: int = CUSTOMER_COUNT,
    output_path: Path = CUSTOMER_OUTPUT_PATH,
    seed: int = RANDOM_SEED,
) -> Path:
    rng = random.Random(seed)
    used_phone_numbers: set[str] = set()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CUSTOMER_COLUMNS)
        writer.writeheader()

        for sequence_number in range(1, row_count + 1):
            first_name = rng.choice(FIRST_NAMES)
            last_name = rng.choice(LAST_NAMES)
            city, state = rng.choice(CITY_STATE_PAIRS)
            customer_id = f"CUST{sequence_number:06d}"

            writer.writerow(
                {
                    "customer_id": customer_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": (
                        f"{first_name}.{last_name}.{sequence_number:06d}@example.com".lower()
                    ),
                    "phone": generate_phone_number(rng, used_phone_numbers),
                    "city": city,
                    "state": state,
                    "country": "India",
                    "signup_date": generate_signup_date(rng),
                    "customer_status": rng.choices(
                        ("ACTIVE", "INACTIVE"), weights=(85, 15), k=1
                    )[0],
                }
            )

    return output_path


def generate_products(
    row_count: int = PRODUCT_COUNT,
    output_path: Path = PRODUCT_OUTPUT_PATH,
    seed: int = RANDOM_SEED,
) -> Path:
    rng = random.Random(seed)
    categories = tuple(PRODUCT_CATALOG)
    end_date = date.today()
    start_date = end_date - timedelta(days=6 * 365)
    products = []

    for sequence_number in range(1, row_count + 1):
        category = rng.choice(categories)
        catalog = PRODUCT_CATALOG[category]
        brand = rng.choice(catalog["brands"])
        item = rng.choice(catalog["items"])
        minimum_price, maximum_price = catalog["price_range"]

        products.append(
            {
                "product_id": f"PROD{sequence_number:06d}",
                "product_name": f"{brand} {item} {sequence_number:04d}",
                "category": category,
                "brand": brand,
                "unit_price": Decimal(rng.randint(minimum_price, maximum_price)) / 100,
                "supplier_id": f"SUP{rng.randint(1, 250):04d}",
                "created_date": start_date
                + timedelta(days=rng.randint(0, (end_date - start_date).days)),
                "product_status": rng.choices(
                    ("ACTIVE", "INACTIVE"), weights=(90, 10), k=1
                )[0],
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(products, schema=PRODUCT_SCHEMA)
    if table.column_names != list(PRODUCT_COLUMNS):
        raise ValueError("Product output columns do not match the configured schema")
    pq.write_table(table, output_path, compression="snappy")
    return output_path


def generate_orders(
    row_count: int = ORDER_COUNT,
    output_path: Path = ORDER_OUTPUT_PATH,
    customers_path: Path = CUSTOMER_OUTPUT_PATH,
    seed: int = RANDOM_SEED + 2,
) -> Path:
    rng = random.Random(seed)

    with customers_path.open(newline="", encoding="utf-8") as customers_file:
        customers = list(csv.DictReader(customers_file))

    if not customers:
        raise ValueError(f"No customer records found in {customers_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    today = date.today()

    with output_path.open("w", newline="", encoding="utf-8") as orders_file:
        writer = csv.DictWriter(orders_file, fieldnames=ORDER_COLUMNS)
        writer.writeheader()

        for sequence_number in range(1, row_count + 1):
            customer = rng.choice(customers)
            signup_date = date.fromisoformat(customer["signup_date"])
            order_date = signup_date + timedelta(
                days=rng.randint(0, (today - signup_date).days)
            )
            order_total = Decimal(rng.randint(19_900, 1_500_000)) / 100

            writer.writerow(
                {
                    "order_id": f"ORD{sequence_number:06d}",
                    "customer_id": customer["customer_id"],
                    "order_date": order_date.isoformat(),
                    "order_status": rng.choices(
                        ORDER_STATUSES,
                        weights=(5, 10, 15, 60, 10),
                        k=1,
                    )[0],
                    "shipping_city": customer["city"],
                    "shipping_state": customer["state"],
                    "order_total": f"{order_total:.2f}",
                    "currency": "INR",
                }
            )

    return output_path


def generate_order_items(
    row_count: int = ORDER_ITEM_COUNT,
    output_path: Path = ORDER_ITEM_OUTPUT_PATH,
    orders_path: Path = ORDER_OUTPUT_PATH,
    products_path: Path = PRODUCT_OUTPUT_PATH,
    seed: int = RANDOM_SEED + 3,
) -> Path:
    rng = random.Random(seed)

    with orders_path.open(newline="", encoding="utf-8") as orders_file:
        order_ids = [row["order_id"] for row in csv.DictReader(orders_file)]

    product_table = pq.read_table(products_path, columns=["product_id", "unit_price"])
    products = product_table.to_pylist()

    if not order_ids:
        raise ValueError(f"No order records found in {orders_path}")
    if not products:
        raise ValueError(f"No product records found in {products_path}")

    currency_unit = Decimal("0.01")
    discount_rates = (Decimal("0"), Decimal("0.05"), Decimal("0.10"), Decimal("0.15"), Decimal("0.20"))
    order_items = []

    for sequence_number in range(1, row_count + 1):
        order_id = order_ids[(sequence_number - 1) % len(order_ids)]
        product = rng.choice(products)
        quantity = rng.randint(1, 5)
        gross_amount = product["unit_price"] * quantity
        discount_amount = (gross_amount * rng.choice(discount_rates)).quantize(
            currency_unit,
            rounding=ROUND_HALF_UP,
        )

        order_items.append(
            {
                "order_item_id": f"OITEM{sequence_number:06d}",
                "order_id": order_id,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["unit_price"],
                "discount_amount": discount_amount,
                "line_amount": gross_amount - discount_amount,
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(order_items, schema=ORDER_ITEM_SCHEMA)
    if table.column_names != list(ORDER_ITEM_COLUMNS):
        raise ValueError("Order-item output columns do not match the configured schema")
    pq.write_table(table, output_path, compression="snappy")
    return output_path


def generate_payments(
    output_path: Path = PAYMENT_OUTPUT_PATH,
    orders_path: Path = ORDER_OUTPUT_PATH,
    seed: int = RANDOM_SEED + 4,
) -> Path:
    rng = random.Random(seed)

    with orders_path.open(newline="", encoding="utf-8") as orders_file:
        orders = list(csv.DictReader(orders_file))

    if not orders:
        raise ValueError(f"No order records found in {orders_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    today = date.today()

    with output_path.open("w", newline="", encoding="utf-8") as payments_file:
        writer = csv.DictWriter(payments_file, fieldnames=PAYMENT_COLUMNS)
        writer.writeheader()

        for sequence_number, order in enumerate(orders, start=1):
            order_date = date.fromisoformat(order["order_date"])
            payment_date = order_date + timedelta(
                days=rng.randint(0, min(2, (today - order_date).days))
            )

            if order["order_status"] == "CANCELLED":
                payment_status = "FAILED"
            elif order["order_status"] == "PENDING":
                payment_status = "PENDING"
            else:
                payment_status = rng.choices(
                    ("SUCCESS", "FAILED", "PENDING"),
                    weights=(94, 3, 3),
                    k=1,
                )[0]

            payment_amount = order["order_total"] if payment_status == "SUCCESS" else "0.00"
            writer.writerow(
                {
                    "payment_id": f"PAY{sequence_number:06d}",
                    "order_id": order["order_id"],
                    "payment_date": payment_date.isoformat(),
                    "payment_method": rng.choice(PAYMENT_METHODS),
                    "payment_status": payment_status,
                    "payment_amount": payment_amount,
                    "transaction_id": f"TXN{sequence_number:08d}",
                }
            )

    return output_path


def generate_shipments(
    output_path: Path = SHIPMENT_OUTPUT_PATH,
    orders_path: Path = ORDER_OUTPUT_PATH,
    seed: int = RANDOM_SEED + 5,
) -> Path:
    rng = random.Random(seed)

    with orders_path.open(newline="", encoding="utf-8") as orders_file:
        orders = [
            order
            for order in csv.DictReader(orders_file)
            if order["order_status"] != "CANCELLED"
        ]

    if not orders:
        raise ValueError(f"No eligible order records found in {orders_path}")

    today = date.today()
    shipments = []

    for sequence_number, order in enumerate(orders, start=1):
        order_date = date.fromisoformat(order["order_date"])
        shipment_date = order_date + timedelta(
            days=rng.randint(0, min(3, (today - order_date).days))
        )
        expected_delivery_date = shipment_date + timedelta(days=rng.randint(2, 7))

        if order["order_status"] == "DELIVERED":
            shipment_status = "DELIVERED"
            actual_delivery_date = shipment_date + timedelta(
                days=rng.randint(0, min(8, (today - shipment_date).days))
            )
        else:
            shipment_status = rng.choice(OPEN_SHIPMENT_STATUSES)
            actual_delivery_date = None

        shipments.append(
            {
                "shipment_id": f"SHIP{sequence_number:06d}",
                "order_id": order["order_id"],
                "shipment_date": shipment_date,
                "expected_delivery_date": expected_delivery_date,
                "actual_delivery_date": actual_delivery_date,
                "shipping_provider": rng.choice(SHIPPING_PROVIDERS),
                "tracking_id": f"TRK{sequence_number:010d}",
                "shipment_status": shipment_status,
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(shipments, schema=SHIPMENT_SCHEMA)
    if table.column_names != list(SHIPMENT_COLUMNS):
        raise ValueError("Shipment output columns do not match the configured schema")
    pq.write_table(table, output_path, compression="snappy")
    return output_path


def generate_inventory(
    output_path: Path = INVENTORY_OUTPUT_PATH,
    products_path: Path = PRODUCT_OUTPUT_PATH,
    seed: int = RANDOM_SEED + 6,
) -> Path:
    rng = random.Random(seed)
    product_ids = pq.read_table(products_path, columns=["product_id"])[
        "product_id"
    ].to_pylist()

    if not product_ids:
        raise ValueError(f"No product records found in {products_path}")

    warehouse_ids = tuple(WAREHOUSES)
    inventory_records = []
    sequence_number = 1
    today = date.today()

    for product_id in product_ids:
        for warehouse_id in rng.sample(warehouse_ids, k=4):
            inventory_records.append(
                {
                    "inventory_id": f"INV{sequence_number:06d}",
                    "product_id": product_id,
                    "warehouse_id": warehouse_id,
                    "warehouse_city": WAREHOUSES[warehouse_id],
                    "stock_quantity": rng.randint(0, 1_000),
                    "reorder_level": rng.randint(0, 100),
                    "last_updated": (
                        today - timedelta(days=rng.randint(0, 365))
                    ).isoformat(),
                }
            )
            sequence_number += 1

    if len(inventory_records) != INVENTORY_COUNT:
        raise ValueError(
            f"Expected {INVENTORY_COUNT} inventory rows, "
            f"generated {len(inventory_records)}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as inventory_file:
        writer = csv.DictWriter(inventory_file, fieldnames=INVENTORY_COLUMNS)
        writer.writeheader()
        writer.writerows(inventory_records)

    return output_path


def generate_returns(
    row_count: int = RETURN_COUNT,
    output_path: Path = RETURN_OUTPUT_PATH,
    order_items_path: Path = ORDER_ITEM_OUTPUT_PATH,
    shipments_path: Path = SHIPMENT_OUTPUT_PATH,
    seed: int = RANDOM_SEED + 7,
) -> Path:
    rng = random.Random(seed)
    delivered_shipments = {
        row["order_id"]: row["actual_delivery_date"]
        for row in pq.read_table(
            shipments_path,
            filters=[("shipment_status", "=", "DELIVERED")],
            columns=["order_id", "actual_delivery_date"],
        ).to_pylist()
    }
    eligible_items = [
        row
        for row in pq.read_table(order_items_path).to_pylist()
        if row["order_id"] in delivered_shipments
    ]

    if len(eligible_items) < row_count:
        raise ValueError(
            f"Only {len(eligible_items)} delivered order items are available; "
            f"cannot generate {row_count} returns"
        )

    today = date.today()
    currency_unit = Decimal("0.01")
    selected_items = rng.sample(eligible_items, k=row_count)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as returns_file:
        writer = csv.DictWriter(returns_file, fieldnames=RETURN_COLUMNS)
        writer.writeheader()

        for sequence_number, item in enumerate(selected_items, start=1):
            delivery_date = delivered_shipments[item["order_id"]]
            return_date = delivery_date + timedelta(
                days=rng.randint(0, min(30, (today - delivery_date).days))
            )
            return_quantity = rng.randint(1, item["quantity"])
            return_status = rng.choice(RETURN_STATUSES)

            if return_status in ("APPROVED", "REFUNDED"):
                refund_amount = (
                    item["line_amount"] * return_quantity / item["quantity"]
                ).quantize(currency_unit, rounding=ROUND_HALF_UP)
            else:
                refund_amount = Decimal("0.00")

            writer.writerow(
                {
                    "return_id": f"RET{sequence_number:06d}",
                    "order_item_id": item["order_item_id"],
                    "order_id": item["order_id"],
                    "product_id": item["product_id"],
                    "return_date": return_date.isoformat(),
                    "return_quantity": return_quantity,
                    "return_reason": rng.choice(RETURN_REASONS),
                    "return_status": return_status,
                    "refund_amount": f"{refund_amount:.2f}",
                }
            )

    return output_path


def generate_promotions(
    row_count: int = PROMOTION_COUNT,
    output_path: Path = PROMOTION_OUTPUT_PATH,
    products_path: Path = PRODUCT_OUTPUT_PATH,
    seed: int = RANDOM_SEED + 8,
) -> Path:
    rng = random.Random(seed)
    products = pq.read_table(
        products_path,
        columns=["product_id", "unit_price"],
    ).to_pylist()

    if not products:
        raise ValueError(f"No product records found in {products_path}")

    today = date.today()
    promotions = []

    for sequence_number in range(1, row_count + 1):
        product = rng.choice(products)
        discount_type = rng.choice(("PERCENTAGE", "FLAT"))

        if discount_type == "PERCENTAGE":
            discount_value = Decimal(rng.randint(1, 50)).quantize(Decimal("0.01"))
        else:
            maximum_discount_cents = int(product["unit_price"] * 100) - 1
            discount_value = Decimal(rng.randint(1, maximum_discount_cents)) / 100

        lifecycle = rng.choice(("SCHEDULED", "ACTIVE", "EXPIRED"))
        if lifecycle == "SCHEDULED":
            start_date = today + timedelta(days=rng.randint(1, 90))
            end_date = start_date + timedelta(days=rng.randint(3, 30))
        elif lifecycle == "ACTIVE":
            start_date = today - timedelta(days=rng.randint(0, 15))
            end_date = today + timedelta(days=rng.randint(0, 30))
        else:
            end_date = today - timedelta(days=rng.randint(1, 365))
            start_date = end_date - timedelta(days=rng.randint(3, 30))

        if today < start_date:
            promotion_status = "SCHEDULED"
        elif today <= end_date:
            promotion_status = "ACTIVE"
        else:
            promotion_status = "EXPIRED"

        promotions.append(
            {
                "promotion_id": f"PROMO{sequence_number:06d}",
                "product_id": product["product_id"],
                "promotion_name": f"{rng.choice(PROMOTION_NAMES)} {sequence_number:04d}",
                "discount_type": discount_type,
                "discount_value": discount_value,
                "start_date": start_date,
                "end_date": end_date,
                "promotion_status": promotion_status,
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(promotions, schema=PROMOTION_SCHEMA)
    if table.column_names != list(PROMOTION_COLUMNS):
        raise ValueError("Promotion output columns do not match the configured schema")
    pq.write_table(table, output_path, compression="snappy")
    return output_path


def generate_customer_events(
    output_path: Path = CUSTOMER_EVENT_OUTPUT_PATH,
    customers_path: Path = CUSTOMER_OUTPUT_PATH,
    products_path: Path = PRODUCT_OUTPUT_PATH,
    seed: int = RANDOM_SEED + 9,
) -> Path:
    rng = random.Random(seed)

    with customers_path.open(newline="", encoding="utf-8") as customers_file:
        customers = [
            {
                "customer_id": row["customer_id"],
                "signup_date": date.fromisoformat(row["signup_date"]),
            }
            for row in csv.DictReader(customers_file)
        ]

    product_ids = pq.read_table(products_path, columns=["product_id"])[
        "product_id"
    ].to_pylist()
    if not customers:
        raise ValueError(f"No customer records found in {customers_path}")
    if not product_ids:
        raise ValueError(f"No product records found in {products_path}")

    events_per_session = CUSTOMER_EVENT_COUNT // CUSTOMER_EVENT_SESSION_COUNT
    if events_per_session * CUSTOMER_EVENT_SESSION_COUNT != CUSTOMER_EVENT_COUNT:
        raise ValueError("Customer-event count must divide evenly across sessions")

    now = datetime.now(timezone.utc)
    historical_start = now - timedelta(days=2 * 365)
    latest_session_start = now - timedelta(minutes=30)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    event_batch = []
    event_number = 1

    with pq.ParquetWriter(
        output_path,
        CUSTOMER_EVENT_SCHEMA,
        compression="snappy",
    ) as parquet_writer:
        for session_number in range(1, CUSTOMER_EVENT_SESSION_COUNT + 1):
            customer = rng.choice(customers)
            customer_start = datetime.combine(
                customer["signup_date"],
                time.min,
                tzinfo=timezone.utc,
            )
            earliest_session_start = max(historical_start, customer_start)
            available_seconds = int(
                (latest_session_start - earliest_session_start).total_seconds()
            )
            session_start = earliest_session_start + timedelta(
                seconds=rng.randint(0, max(0, available_seconds))
            )
            session_id = f"SESSION{session_number:07d}"
            session_product_id = rng.choice(product_ids)
            device_type = rng.choices(DEVICE_TYPES, weights=(65, 28, 7), k=1)[0]
            traffic_source = rng.choice(TRAFFIC_SOURCES)
            event_types = (
                rng.choice(("PAGE_VIEW", "SEARCH")),
                "PRODUCT_VIEW",
                rng.choice(("PRODUCT_VIEW", "ADD_TO_CART")),
                rng.choice(("REMOVE_FROM_CART", "CHECKOUT_STARTED", "PURCHASE")),
            )
            event_timestamp = session_start

            for event_type in event_types:
                if event_type in ("PAGE_VIEW", "SEARCH"):
                    product_id = None
                elif event_type in ("CHECKOUT_STARTED", "PURCHASE"):
                    product_id = session_product_id if rng.random() < 0.8 else None
                else:
                    product_id = session_product_id

                event_batch.append(
                    {
                        "event_id": f"EVT{event_number:07d}",
                        "customer_id": customer["customer_id"],
                        "product_id": product_id,
                        "event_type": event_type,
                        "event_timestamp": event_timestamp,
                        "session_id": session_id,
                        "device_type": device_type,
                        "traffic_source": traffic_source,
                    }
                )
                event_number += 1
                event_timestamp += timedelta(seconds=rng.randint(5, 300))

                if len(event_batch) == 10_000:
                    parquet_writer.write_table(
                        pa.Table.from_pylist(event_batch, schema=CUSTOMER_EVENT_SCHEMA)
                    )
                    event_batch.clear()

        if event_batch:
            parquet_writer.write_table(
                pa.Table.from_pylist(event_batch, schema=CUSTOMER_EVENT_SCHEMA)
            )

    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate clean e-commerce source data")
    parser.add_argument(
        "dataset",
        nargs="?",
        choices=(
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
        ),
        default="customers",
    )
    args = parser.parse_args()

    if args.dataset == "products":
        output_path = generate_products()
        print(f"Generated {PRODUCT_COUNT:,} products at {output_path}")
    elif args.dataset == "orders":
        output_path = generate_orders()
        print(f"Generated {ORDER_COUNT:,} orders at {output_path}")
    elif args.dataset == "order_items":
        output_path = generate_order_items()
        print(f"Generated {ORDER_ITEM_COUNT:,} order items at {output_path}")
    elif args.dataset == "payments":
        output_path = generate_payments()
        print(f"Generated {PAYMENT_COUNT:,} payments at {output_path}")
    elif args.dataset == "shipments":
        output_path = generate_shipments()
        print(f"Generated shipments at {output_path}")
    elif args.dataset == "inventory":
        output_path = generate_inventory()
        print(f"Generated {INVENTORY_COUNT:,} inventory records at {output_path}")
    elif args.dataset == "returns":
        output_path = generate_returns()
        print(f"Generated {RETURN_COUNT:,} returns at {output_path}")
    elif args.dataset == "promotions":
        output_path = generate_promotions()
        print(f"Generated {PROMOTION_COUNT:,} promotions at {output_path}")
    elif args.dataset == "customer_events":
        output_path = generate_customer_events()
        print(f"Generated {CUSTOMER_EVENT_COUNT:,} customer events at {output_path}")
    else:
        output_path = generate_customers()
        print(f"Generated {CUSTOMER_COUNT:,} customers at {output_path}")


if __name__ == "__main__":
    main()
