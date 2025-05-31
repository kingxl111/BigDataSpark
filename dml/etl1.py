from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import psycopg2

def main():
    spark = SparkSession.builder \
        .appName("Lab1 Snowflake ETL") \
        .config("spark.jars", "/app/postgresql-jdbc.jar") \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://postgres:5432/snowflake"
    connection_properties = {
        "user": "snowflake",
        "password": "snowflake",
        "driver": "org.postgresql.Driver"
    }
    
    def clear_table(table_name):
        try:
            conn = psycopg2.connect(
                dbname="snowflake",
                user="snowflake",
                password="snowflake",
                host="postgres",
                port="5432"
            )
            cursor = conn.cursor()
            
            cursor.execute("SET session_replication_role = replica;")
            
            cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
            
            cursor.execute("SET session_replication_role = DEFAULT;")
            
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Cleared table: {table_name}")
        except Exception as e:
            print(f"Error clearing table {table_name}: {str(e)}")
            raise

    tables_to_clear = [
        "lab1.fact_sales",
        "lab1.dim_customer",
        "lab1.dim_seller",
        "lab1.dim_product",
        "lab1.dim_store",
        "lab1.dim_supplier"
    ]
    
    for table in tables_to_clear:
        clear_table(table)

    mock_data = spark.read.jdbc(
        url=jdbc_url,
        table="staging.mock_data",
        properties=connection_properties
    )

    dim_customer = mock_data.select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code")
    ).distinct().filter("email IS NOT NULL")

    dim_seller = mock_data.select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code")
    ).distinct().filter("email IS NOT NULL")

    dim_product = mock_data.select(
        col("product_name").alias("name"),
        col("product_category").alias("category"),
        col("product_price").alias("price"),
        col("product_weight").alias("weight"),
        col("product_color").alias("color"),
        col("product_size").alias("size"),
        col("product_brand").alias("brand"),
        col("product_material").alias("material"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("product_release_date").alias("release_date"),
        col("product_expiry_date").alias("expiry_date")
    ).distinct().filter("name IS NOT NULL")

    dim_store = mock_data.select(
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email")
    ).distinct().filter("name IS NOT NULL")

    dim_supplier = mock_data.select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("supplier_country").alias("country")
    ).distinct().filter("name IS NOT NULL")

    dim_customer.write \
        .jdbc(url=jdbc_url, table="lab1.dim_customer", mode="append", properties=connection_properties)
    
    dim_seller.write \
        .jdbc(url=jdbc_url, table="lab1.dim_seller", mode="append", properties=connection_properties)
    
    dim_product.write \
        .jdbc(url=jdbc_url, table="lab1.dim_product", mode="append", properties=connection_properties)
    
    dim_store.write \
        .jdbc(url=jdbc_url, table="lab1.dim_store", mode="append", properties=connection_properties)
    
    dim_supplier.write \
        .jdbc(url=jdbc_url, table="lab1.dim_supplier", mode="append", properties=connection_properties)

    dim_customer_df = spark.read.jdbc(jdbc_url, "lab1.dim_customer", properties=connection_properties)
    dim_seller_df = spark.read.jdbc(jdbc_url, "lab1.dim_seller", properties=connection_properties)
    dim_product_df = spark.read.jdbc(jdbc_url, "lab1.dim_product", properties=connection_properties)
    dim_store_df = spark.read.jdbc(jdbc_url, "lab1.dim_store", properties=connection_properties)
    dim_supplier_df = spark.read.jdbc(jdbc_url, "lab1.dim_supplier", properties=connection_properties)

    fact_sales = mock_data \
        .join(dim_customer_df, mock_data.customer_email == dim_customer_df.email) \
        .join(dim_seller_df, mock_data.seller_email == dim_seller_df.email) \
        .join(dim_product_df, mock_data.product_name == dim_product_df.name) \
        .join(dim_store_df, mock_data.store_name == dim_store_df.name) \
        .join(dim_supplier_df, mock_data.supplier_name == dim_supplier_df.name) \
        .select(
            col("sale_date"),
            col("customer_key"),
            col("seller_key"),
            col("product_key"),
            col("store_key"),
            col("supplier_key"),
            col("sale_quantity").alias("quantity"),
            col("sale_total_price").alias("total_price")
        ) \
        .filter("sale_date IS NOT NULL AND total_price IS NOT NULL AND quantity > 0")

    fact_sales.write \
        .jdbc(url=jdbc_url, table="lab1.fact_sales", mode="append", properties=connection_properties)

    spark.stop()

if __name__ == "__main__":
    main()