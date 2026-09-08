CUSTOMER_COLUMNS = (
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "state",
    "country",
    "signup_date",
    "customer_status",
)

PRODUCT_COLUMNS = (
    "product_id",
    "product_name",
    "category",
    "brand",
    "unit_price",
    "supplier_id",
    "created_date",
    "product_status",
)

ORDER_COLUMNS = (
    "order_id",
    "customer_id",
    "order_date",
    "order_status",
    "shipping_city",
    "shipping_state",
    "order_total",
    "currency",
)

ORDER_ITEM_COLUMNS = (
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_amount",
    "line_amount",
)

PAYMENT_COLUMNS = (
    "payment_id",
    "order_id",
    "payment_date",
    "payment_method",
    "payment_status",
    "payment_amount",
    "transaction_id",
)

SHIPMENT_COLUMNS = (
    "shipment_id",
    "order_id",
    "shipment_date",
    "expected_delivery_date",
    "actual_delivery_date",
    "shipping_provider",
    "tracking_id",
    "shipment_status",
)

INVENTORY_COLUMNS = (
    "inventory_id",
    "product_id",
    "warehouse_id",
    "warehouse_city",
    "stock_quantity",
    "reorder_level",
    "last_updated",
)

RETURN_COLUMNS = (
    "return_id",
    "order_item_id",
    "order_id",
    "product_id",
    "return_date",
    "return_quantity",
    "return_reason",
    "return_status",
    "refund_amount",
)

PROMOTION_COLUMNS = (
    "promotion_id",
    "product_id",
    "promotion_name",
    "discount_type",
    "discount_value",
    "start_date",
    "end_date",
    "promotion_status",
)

CUSTOMER_EVENT_COLUMNS = (
    "event_id",
    "customer_id",
    "product_id",
    "event_type",
    "event_timestamp",
    "session_id",
    "device_type",
    "traffic_source",
)
