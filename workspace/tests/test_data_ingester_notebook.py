
from decimal import Decimal
import pytest


@pytest.fixture(scope="module")
def test_data(spark):
    customers = spark.createDataFrame(
        [
            ("C1", "Alice", "alice@example.com", "123",
             "Address", "Consumer", "USA", "NYC",
             "NY", "10001", "East")
        ],
        [
            "customer_id", "customer_name", "email", "phone",
            "address", "segment", "country", "city",
            "state", "postal_code", "region"
        ]
    )

    products = spark.createDataFrame(
        [
            ("P1", "Furniture", "Chairs", "Office Chair",
             "NY", Decimal("81.882"))
        ],
        [
            "product_id", "category", "sub_category",
            "product_name", "product_state", "price_per_product"
        ]
    )

    orders = spark.createDataFrame(
        [
            (1, "O1", "21/8/2016", "25/8/2016",
             "Standard Class", "C1", "P1", 2,
             Decimal("163.76"), Decimal("0.1"), Decimal("20.126")),

            (2, "O2", "23/9/2017", "29/9/2017",
             "Standard Class", "UNKNOWN", "UNKNOWN", 1,
             Decimal("100.00"), Decimal("0.0"), Decimal("10.125"))
        ],
        [
            "Row ID", "Order ID", "Order Date", "Ship Date",
            "Ship Mode", "Customer ID", "Product ID", "Quantity",
            "Price", "Discount", "Profit"
        ]
    )

    return customers, products, orders


# 1. Test order date parsing
def test_order_date_parsing(test_data):
    _, _, orders = test_data

    result = transform_orders(orders)

    date_value = result.filter("row_id = 1").first()["order_date"]

    assert str(date_value) == "2016-08-21"


# 2. Test master table joins and profit rounding
def test_master_table_join_and_profit_rounding(test_data):
    customers, products, orders = test_data

    transformed = transform_orders(orders)

    result = create_master_table(
        transformed, customers, products
    )

    row = result.filter("row_id = 1").first()

    assert row["customer_name"] == "Alice"
    assert row["category"] == "Furniture"
    assert row["profit"] == Decimal("20.13")


# 3. Test unmatched records are retained
def test_unmatched_records_retained(test_data):
    customers, products, orders = test_data

    result = create_master_table(
        transform_orders(orders), customers, products
    )

    assert result.count() == 2

    unmatched = result.filter("row_id = 2").first()

    assert unmatched["customer_name"] is None
    assert unmatched["category"] is None


# 4. Test profit aggregation
def test_profit_aggregation(test_data):
    customers, products, orders = test_data

    master = create_master_table(
        transform_orders(orders), customers, products
    )

    result = create_profit_aggregation(master)

    assert result.count() == 2

    row = result.filter("order_year = 2016").first()

    assert row["total_profit"] == Decimal("20.13")


# 5. Test duplicate customer IDs
def test_duplicate_customer_ids(spark):
    duplicate_customers = spark.createDataFrame(
        [("C1",), ("C1",)],
        ["customer_id"]
    )

    duplicates = duplicate_keys(
        duplicate_customers, "customer_id"
    )

    assert duplicates.count() == 1


# 6. Test invalid order dates
def test_invalid_order_date(test_data):
    _, _, orders = test_data

    invalid_orders = spark.createDataFrame(
        [
            (3, "O3", "invalid-date", "25/8/2016",
             "Standard Class", "C1", "P1", 1,
             Decimal("10.00"), Decimal("0.0"), Decimal("2.00"))
        ],
        orders.schema
    )

    result = transform_orders(invalid_orders)

    assert result.first()["order_date"] is None


# 7. Test empty orders
def test_empty_orders(test_data):
    _, _, orders = test_data

    result = transform_orders(orders.limit(0))

    assert result.count() == 0
