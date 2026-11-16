from pyspark.sql import functions as F
from pyspark.sql import types as t
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

CATALOG = "main"
SCHEMA = "sales"

CUSTOMER_PATH = "/mnt/sales/customers.csv"
ORDERS_PATH = "/mnt/sales/orders.json"
PRODUCT_PATH = "/mnt/sales/products.csv"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

customers_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(CUSTOMER_PATH)
)

orders_raw = (
    spark.read
    .option("multiLine", True)
    .json(ORDERS_PATH)
)

products_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(PRODUCT_PATH)
)
#Create raw tables for each source dataset
customers_raw.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.raw_customers"
)
#Create raw tables for each source dataset
orders_raw.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.raw_orders"
)
#Create raw tables for each source dataset
products_raw.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.raw_products"
)

def transform_customers(df):
    return (
        df.select(
            F.trim(F.col("Customer ID")).alias("customer_id"),
            F.trim(F.col("Customer Name")).alias("customer_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.trim(F.col("phone")).alias("phone"),
            F.trim(F.col("address")).alias("address"),
            F.trim(F.col("Segment")).alias("segment"),
            F.trim(F.col("Country")).alias("country"),
            F.trim(F.col("City")).alias("city"),
            F.trim(F.col("State")).alias("state"),
            F.trim(F.col("Postal Code")).alias("postal_code"),
            F.trim(F.col("Region")).alias("region")
        )
        .filter(
            F.col("customer_id").isNotNull() &
            (F.col("customer_id") != "")
        )
    )
    
    
def transform_products(df):
    return (
        df.select(
            F.trim(F.col("Product ID")).alias("product_id"),
            F.trim(F.col("Category")).alias("category"),
            F.trim(F.col("Sub-Category")).alias("sub_category"),
            F.trim(F.col("Product Name")).alias("product_name"),
            F.trim(F.col("State")).alias("product_state"),
            F.col("Price per product")
             .cast(t.DecimalType(18, 3))
             .alias("price_per_product")
        )
        .filter(
            F.col("product_id").isNotNull() &
            (F.col("product_id") != "")
        )
    )
    
    
def transform_orders(df):
    return (
        df.select(
            F.col("Row ID").cast("long").alias("row_id"),
            F.trim(F.col("Order ID")).alias("order_id"),
            F.to_date(F.col("Order Date"), "d/M/yyyy").alias("order_date"),
            F.to_date(F.col("Ship Date"), "d/M/yyyy").alias("ship_date"),
            F.trim(F.col("Ship Mode")).alias("ship_mode"),
            F.trim(F.col("Customer ID")).alias("customer_id"),
            F.trim(F.col("Product ID")).alias("product_id"),
            F.col("Quantity").cast("int").alias("quantity"),
            F.col("Price").cast(t.DecimalType(18, 2)).alias("price"),
            F.col("Discount").cast(t.DecimalType(8, 4)).alias("discount"),
            F.col("Profit").cast(t.DecimalType(18, 2)).alias("profit")
        )
    )
    
# Create an enriched table for customers and products 
# Create an enriched table which has order information  Profit rounded to 2 decimal places Customer name and country Product category and sub category 
 def create_master_table(orders, customers, products):

    return (
        orders.alias("o")
        .join(
            customers.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left"
        )
        .join(
            products.alias("p"),
            F.col("o.product_id") == F.col("p.product_id"),
            "left"
        )
        .select(
            F.col("o.row_id"),
            F.col("o.order_id"),
            F.col("o.order_date"),
            F.year("o.order_date").alias("order_year"),
            F.col("o.ship_date"),
            F.col("o.ship_mode"),
            F.col("o.customer_id"),
            F.col("c.customer_name"),
            F.col("c.email"),
            F.col("c.segment"),
            F.col("c.country"),
            F.col("c.city"),
            F.col("c.state"),
            F.col("c.region"),
            F.col("o.product_id"),
            F.col("p.product_name"),
            F.col("p.category"),
            F.col("p.sub_category"),
            F.col("p.price_per_product"),
            F.col("o.quantity"),
            F.col("o.price"),
            F.col("o.discount"),
            F.round(F.col("o.profit"), 2).alias("profit")
        )
    )
    
customers = transform_customers(customers_raw)
products = transform_products(products_raw)
orders = transform_orders(orders_raw)

master_df = create_master_table(orders, customers, products)

master_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.master_sales"
)



# Create an aggregate table that shows profit by  Year Product Category Product Sub Category Customer
def create_profit_aggregation(master):
    return (
        master
        .groupBy(
            "order_year",
            "category",
            "sub_category",
            "customer_id",
            "customer_name"
        )
        .agg(
            F.round(F.sum("profit"), 2).alias("total_profit")
        )
    )

profit_aggregation = create_profit_aggregation(master_df)

profit_aggregation.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.profit_aggregation"
)

# Using SQL output the following aggregates
# Profit by Year
profit_by_year = spark.sql("""SELECT
    order_year,
    ROUND(SUM(profit), 2) AS total_profit
FROM master_df
GROUP BY order_year;""")
# Profit by Year + Product Category
profit_by_year_category = spark.sql(""" SELECT
    order_year,
    category,
    ROUND(SUM(profit), 2) AS total_profit
FROM master_df
GROUP BY
    order_year,
    category;""")
# Profit by Customer
profit_by_customer = spark.sql(""" SELECT
    customer_id,
    customer_name,
    ROUND(SUM(profit), 2) AS total_profit
FROM master_df
GROUP BY
    customer_id,
    customer_name;""")
#Profit by Customer + Year
profit_by_customer_year =  spark.sql(""" SELECT
    customer_id,
    customer_name,
    order_year,
    ROUND(SUM(profit), 2) AS total_profit
FROM master_df
GROUP BY
    customer_id,
    customer_name,
    order_year;""")

outputs = {
    "profit_by_year": profit_by_year,
    "profit_by_year_category": profit_by_year_category,
    "profit_by_customer": profit_by_customer,
    "profit_by_customer_year": profit_by_customer_year
}

for table_name, df in outputs.items():
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{CATALOG}.{SCHEMA}.{table_name}"
    )