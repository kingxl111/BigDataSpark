from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("Lab1 Snowflake ETL") \
        .config("spark.jars", "/app/postgresql-jdbc.jar") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")
    logger.info("Spark session initialized")

    jdbc_url = "jdbc:postgresql://postgres:5432/snowflake"
    connection_properties = {
        "user": "snowflake",
        "password": "snowflake",
        "driver": "org.postgresql.Driver"
    }
    
    def clear_table(table_name):
        logger.info(f"Clearing table: {table_name}")
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
            logger.info(f"Successfully cleared table: {table_name}")
        except Exception as e:
            logger.error(f"Error clearing table {table_name}: {str(e)}")
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

    logger.info("Loading data from staging.mock_data")
    mock_data = spark.read.jdbc(
        url=jdbc_url,
        table="staging.mock_data",
        properties=connection_properties
    )
    
    mock_data.cache()
    mock_data_count = mock_data.count()
    logger.info(f"Loaded mock_data: {mock_data_count} rows")

    def load_dimension(df, table_name, distinct_query, id_column=None):
        logger.info(f"Processing dimension: {table_name}")
        
        distinct_df = df.selectExpr(*distinct_query).distinct()
        
        if id_column:
            distinct_df = distinct_df.filter(col(id_column).isNotNull())
        else:
            first_col = distinct_df.columns[0]
            distinct_df = distinct_df.filter(col(first_col).isNotNull())
        
        count = distinct_df.count()
        if count == 0:
            logger.warning(f"WARNING: No distinct rows found for {table_name}!")
        else:
            logger.info(f"Distinct rows for {table_name}: {count}")
        
        distinct_df.write.jdbc(
            url=jdbc_url,
            table=f"lab1.{table_name}",
            mode="append",
            properties=connection_properties
        )
        logger.info(f"Written {count} rows to {table_name}")
        return distinct_df

    dim_customer = load_dimension(
        mock_data,
        "dim_customer",
        distinct_query=[
            "customer_first_name as first_name",
            "customer_last_name as last_name",
            "customer_age as age",
            "customer_email as email",
            "customer_country as country",
            "customer_postal_code as postal_code"
        ],
        id_column="email"
    )
    
    dim_seller = load_dimension(
        mock_data,
        "dim_seller",
        distinct_query=[
            "seller_first_name as first_name",
            "seller_last_name as last_name",
            "seller_email as email",
            "seller_country as country",
            "seller_postal_code as postal_code"
        ],
        id_column="email"
    )
    
    dim_product = load_dimension(
        mock_data,
        "dim_product",
        distinct_query=[
            "product_name as name",
            "product_category as category",
            "product_price as price",
            "product_weight as weight",
            "product_color as color",
            "product_size as size",
            "product_brand as brand",
            "product_material as material",
            "product_description as description",
            "product_rating as rating",
            "product_reviews as reviews",
            "product_release_date as release_date",
            "product_expiry_date as expiry_date"
        ],
        id_column="name"
    )
    
    dim_store = load_dimension(
        mock_data,
        "dim_store",
        distinct_query=[
            "store_name as name",
            "store_location as location",
            "store_city as city",
            "store_state as state",
            "store_country as country",
            "store_phone as phone",
            "store_email as email"
        ],
        id_column="name"
    )
    
    dim_supplier = load_dimension(
        mock_data,
        "dim_supplier",
        distinct_query=[
            "supplier_name as name",
            "supplier_contact as contact",
            "supplier_email as email",
            "supplier_phone as phone",
            "supplier_address as address",
            "supplier_city as city",
            "supplier_country as country"
        ],
        id_column="name"
    )

    def check_dim_size(table_name):
        logger.info(f"Checking dimension: {table_name}")
        df = spark.read.jdbc(jdbc_url, f"lab1.{table_name}", properties=connection_properties)
        count = df.count()
        if count == 0:
            logger.warning(f"WARNING: Dimension table {table_name} is empty!")
        else:
            logger.info(f"Rows in {table_name}: {count}")
        return df.cache()
    
    dim_customer_df = check_dim_size("dim_customer")
    dim_seller_df = check_dim_size("dim_seller")
    dim_product_df = check_dim_size("dim_product")
    dim_store_df = check_dim_size("dim_store")
    dim_supplier_df = check_dim_size("dim_supplier")

    if dim_customer_df.isEmpty() or dim_seller_df.isEmpty() or dim_product_df.isEmpty() or dim_store_df.isEmpty() or dim_supplier_df.isEmpty():
        logger.error("CRITICAL: One or more dimension tables are empty! Aborting fact table creation.")
        spark.stop()
        return

    logger.info("Building fact_sales...")
    
    mock_alias = mock_data.alias("mock")
    
    customer_join = mock_alias.join(
        dim_customer_df.alias("cust"), 
        col("mock.customer_email") == col("cust.email"),
        "inner"
    )
    customer_count = customer_join.count()
    logger.info(f"After customer join: {customer_count} rows")
    
    seller_join = customer_join.join(
        dim_seller_df.alias("sell"), 
        col("mock.seller_email") == col("sell.email"),
        "inner"
    )
    seller_count = seller_join.count()
    logger.info(f"After seller join: {seller_count} rows")
    
    product_join = seller_join.join(
        dim_product_df.alias("prod"), 
        (col("mock.product_name") == col("prod.name")) & 
        (col("mock.product_brand") == col("prod.brand")),
        "inner"
    )
    product_count = product_join.count()
    logger.info(f"After product join: {product_count} rows")
    
    store_join = product_join.join(
        dim_store_df.alias("store"), 
        (col("mock.store_name") == col("store.name")) & 
        (col("mock.store_location") == col("store.location")),
        "inner"
    )
    store_count = store_join.count()
    logger.info(f"After store join: {store_count} rows")
    
    supplier_join = store_join.join(
        dim_supplier_df.alias("supp"), 
        col("mock.supplier_name") == col("supp.name"),
        "inner"
    )
    supplier_count = supplier_join.count()
    logger.info(f"After supplier join: {supplier_count} rows")
    
    filtered_fact = supplier_join.filter(
        col("sale_date").isNotNull() & 
        col("sale_total_price").isNotNull() & 
        (col("sale_quantity") > 0))
    filtered_count = filtered_fact.count()
    logger.info(f"After filtering: {filtered_count} rows")
    
    fact_sales = filtered_fact.select(
        col("sale_date").alias("sale_date"),
        col("cust.customer_key").alias("customer_key"),
        col("sell.seller_key").alias("seller_key"),
        col("prod.product_key").alias("product_key"),
        col("store.store_key").alias("store_key"),
        col("supp.supplier_key").alias("supplier_key"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price")
    )
    
    fact_count = fact_sales.count()
    logger.info(f"Prepared fact_sales with {fact_count} rows")
    
    if fact_count > 0:
        fact_sales.write.jdbc(
            url=jdbc_url,
            table="lab1.fact_sales",
            mode="append",
            properties=connection_properties
        )
        logger.info("Successfully written fact_sales")
        
        written_fact = spark.read.jdbc(
            url=jdbc_url,
            table="lab1.fact_sales",
            properties=connection_properties
        )
        written_count = written_fact.count()
        logger.info(f"Validation: {written_count} rows in fact_sales table")
        
        if written_count != fact_count:
            logger.warning(f"WARNING: Row count mismatch! Expected: {fact_count}, Actual: {written_count}")
    else:
        logger.error("ERROR: fact_sales is empty! Skipping write.")
        
        supplier_join.write.mode("overwrite").csv("/tmp/debug_fact_preparation")
        logger.info("Debug data saved to /tmp/debug_fact_preparation")

    spark.stop()
    logger.info("ETL completed successfully")

if __name__ == "__main__":
    main()
