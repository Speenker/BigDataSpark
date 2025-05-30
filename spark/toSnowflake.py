from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, monotonically_increasing_id, \
    year, month, dayofmonth, date_format, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.types import DateType

postgres_url = "jdbc:postgresql://localhost:7379/postgres"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("Full Star Schema ETL") \
    .config("spark.jars", "jars/postgresql-42.7.5.jar") \
    .getOrCreate()

mock_df = spark.read.jdbc(postgres_url, "mock_data", properties=db_properties)
mock_df = mock_df.withColumn("sale_date", col("sale_date").cast(DateType()))
mock_df = mock_df.withColumn("product_release_date", col("product_release_date").cast(DateType()))
mock_df = mock_df.withColumn("product_expiry_date", col("product_expiry_date").cast(DateType()))

def create_dim(df, cols, id_name):
    deduped = df.select(cols).dropDuplicates()
    window = Window.orderBy(monotonically_increasing_id())
    return deduped.withColumn(id_name, row_number().over(window))

dim_customer = create_dim(mock_df, [
    "customer_first_name", "customer_last_name", "customer_age",
    "customer_email", "customer_country", "customer_postal_code",
    "customer_pet_type", "customer_pet_name", "customer_pet_breed"
], "customer_id")

dim_seller = create_dim(mock_df, [
    "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code"
], "seller_id")

dim_product = create_dim(mock_df, [
    "product_name", "product_category", "product_weight", "product_color", "product_size",
    "product_brand", "product_material", "product_description",
    "product_rating", "product_reviews", "product_release_date", "product_expiry_date"
], "product_id")

dim_store = create_dim(mock_df, [
    "store_name", "store_location", "store_city", "store_state",
    "store_country", "store_phone", "store_email"
], "store_id")

dim_supplier = create_dim(mock_df, [
    "supplier_name", "supplier_contact", "supplier_email",
    "supplier_phone", "supplier_address", "supplier_city", "supplier_country"
], "supplier_id")

dim_pet_category = create_dim(mock_df.select("pet_category"), ["pet_category"], "pet_category_id")

date_df = mock_df.select("sale_date").dropDuplicates().withColumnRenamed("sale_date", "date_value")
dim_date = date_df \
    .withColumn("year", year("date_value")) \
    .withColumn("month", month("date_value")) \
    .withColumn("day", dayofmonth("date_value")) \
    .withColumn("weekday", date_format("date_value", "E")) \
    .withColumn("date_id", row_number().over(Window.orderBy("date_value")))

dim_customer.write.jdbc(postgres_url, "dim_customer", mode="overwrite", properties=db_properties)
dim_seller.write.jdbc(postgres_url, "dim_seller", mode="overwrite", properties=db_properties)
dim_product.write.jdbc(postgres_url, "dim_product", mode="overwrite", properties=db_properties)
dim_store.write.jdbc(postgres_url, "dim_store", mode="overwrite", properties=db_properties)
dim_supplier.write.jdbc(postgres_url, "dim_supplier", mode="overwrite", properties=db_properties)
dim_pet_category.write.jdbc(postgres_url, "dim_pet_category", mode="overwrite", properties=db_properties)
dim_date.write.jdbc(postgres_url, "dim_date", mode="overwrite", properties=db_properties)

fact_df = mock_df \
    .join(dim_customer,
          (mock_df.customer_first_name == dim_customer.customer_first_name) &
          (mock_df.customer_last_name == dim_customer.customer_last_name) &
          (mock_df.customer_email == dim_customer.customer_email), "inner") \
    .join(dim_seller,
          (mock_df.seller_first_name == dim_seller.seller_first_name) &
          (mock_df.seller_last_name == dim_seller.seller_last_name) &
          (mock_df.seller_email == dim_seller.seller_email), "inner") \
    .join(dim_product,
          (mock_df.product_name == dim_product.product_name) &
          (mock_df.product_brand == dim_product.product_brand) &
          (mock_df.product_weight == dim_product.product_weight) &
          (mock_df.product_color == dim_product.product_color) &
          (mock_df.product_size == dim_product.product_size) &
          (mock_df.product_material == dim_product.product_material), "inner") \
    .join(dim_store,
          (mock_df.store_name == dim_store.store_name) &
          (mock_df.store_location == dim_store.store_location) &
          (mock_df.store_city == dim_store.store_city), "inner") \
    .join(dim_supplier,
          (mock_df.supplier_name == dim_supplier.supplier_name) &
          (mock_df.supplier_email == dim_supplier.supplier_email), "inner") \
    .join(dim_pet_category,
          mock_df.pet_category == dim_pet_category.pet_category, "inner") \
    .join(dim_date,
          mock_df.sale_date == dim_date.date_value, "inner")

sales_fact = fact_df.select(
    "customer_id", "seller_id", "product_id", "store_id", "supplier_id",
    "pet_category_id", "date_id",
    col("sale_quantity").alias("quantity"),
    col("sale_total_price").alias("total_price")
)

sales_fact.write.jdbc(postgres_url, "sales_fact", mode="overwrite", properties=db_properties)

print(" Все таблицы измерений и таблица фактов успешно созданы и загружены.")

spark.stop()
