from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum as _sum, avg, max as _max,
    month, year
)

spark = SparkSession.builder \
    .appName("Sales Reports Generator") \
    .config("spark.jars", "jars/postgresql-42.7.5.jar,jars/clickhouse-jdbc-0.8.6-shaded.jar") \
    .getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/postgres"
pg_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

ch_url = "jdbc:clickhouse://clickhouse:8123/default"
ch_props = {
    "user": "default",
    "password": "",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

def read_clickhouse(table: str):
    return spark.read.jdbc(ch_url, table, properties=ch_props)

def write_clickhouse(df, table: str):
    df.write.mode("overwrite").jdbc(ch_url, table, properties=ch_props)

sales = spark.read.jdbc(pg_url, "sales_fact", properties=pg_props)
products = spark.read.jdbc(pg_url, "dim_product", properties=pg_props)
customers = spark.read.jdbc(pg_url, "dim_customer", properties=pg_props)
stores = spark.read.jdbc(pg_url, "dim_store", properties=pg_props)
suppliers = spark.read.jdbc(pg_url, "dim_supplier", properties=pg_props)
dates = spark.read.jdbc(pg_url, "dim_date", properties=pg_props)

def build_mart_product_sales():
    return (
        sales.join(products, "product_id")
        .groupBy("product_id", "product_name", "product_category")
        .agg(
            _sum("total_price").alias("total_revenue"),
            _sum("quantity").alias("total_quantity"),
            avg("product_rating").alias("avg_rating"),
            _max("product_reviews").alias("review_count")
        )
    )

def build_mart_customer_sales():
    return (
        sales.join(customers, "customer_id")
        .groupBy("customer_id", "first_name", "last_name", "country")
        .agg(
            _sum("total_price").alias("total_spent"),
            _sum("quantity").alias("total_quantity"),
            avg("total_price").alias("avg_order_value")
        )
    )

def build_mart_monthly_sales():
    return (
        sales.join(dates, sales.date_id == dates.date_id)
        .groupBy(
            year("date_value").alias("year"),
            month("date_value").alias("month")
        )
        .agg(
            _sum("total_price").alias("monthly_revenue"),
            _sum("quantity").alias("monthly_quantity"),
            avg("total_price").alias("avg_order_value")
        )
    )

def build_mart_store_sales():
    return (
        sales.join(stores, "store_id")
        .groupBy("store_id", "name", "city", "country")
        .agg(
            _sum("total_price").alias("total_revenue"),
            _sum("quantity").alias("total_quantity"),
            avg("total_price").alias("avg_order_value")
        )
    )

def build_mart_supplier_sales():
    return (
        sales.join(suppliers, "supplier_id")
        .groupBy("supplier_id", "name", "country")
        .agg(
            _sum("total_price").alias("total_revenue"),
            _sum("quantity").alias("total_quantity")
        )
    )

def build_mart_product_quality():
    return (
        products.join(sales, "product_id")
        .groupBy("product_id", "name")
        .agg(
            avg("rating").alias("avg_rating"),
            _max("reviews").alias("review_count"),
            _sum("quantity").alias("sales_volume")
        )
    )

write_clickhouse(build_mart_product_sales(), "mart_product_sales")
write_clickhouse(build_mart_customer_sales(), "mart_customer_sales")
write_clickhouse(build_mart_monthly_sales(), "mart_monthly_sales")
write_clickhouse(build_mart_store_sales(), "mart_store_sales")
write_clickhouse(build_mart_supplier_sales(), "mart_supplier_sales")
write_clickhouse(build_mart_product_quality(), "mart_product_quality")

spark.stop()
