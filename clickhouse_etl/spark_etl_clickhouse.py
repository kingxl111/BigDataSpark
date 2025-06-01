from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging, time
import os
from py4j.java_gateway import java_import

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def execute_jdbc_query(spark, url, query, user, password, driver):
    """Выполняет SQL-запрос через JDBC соединение"""
    try:
        logger.info(f"Executing JDBC query: {query}")
        java_import(spark._jvm, "java.sql.DriverManager")
        spark._jvm.Class.forName(driver)
        
        connection = spark._jvm.DriverManager.getConnection(url, user, password)
        statement = connection.createStatement()
        result = statement.execute(query)
        logger.info(f"Query executed successfully: {result}")
        return True
    except Exception as e:
        logger.error(f"Error executing JDBC query: {str(e)}")
        raise
    finally:
        try:
            if connection:
                connection.close()
        except:
            pass

def wait_for_data(spark, url, table, properties, timeout=600, interval=10):
    """Ожидает появления данных в таблице"""
    logger.info(f"Waiting for data in {table} (timeout: {timeout}s)...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            df = spark.read.jdbc(url, table, properties=properties)
            count = df.count()
            if count > 0:
                logger.info(f"Data found in {table}: {count} rows")
                return True
            logger.info(f"No data in {table} yet. Retrying in {interval}s...")
        except Exception as e:
            logger.warning(f"Error checking {table}: {str(e)}")
        
        time.sleep(interval)
    
    logger.error(f"Timeout waiting for data in {table} after {timeout} seconds")
    return False

def main():
    spark = SparkSession.builder \
        .appName("ClickHouse Marts ETL") \
        .config("spark.jars", "file:///app/jars/postgresql-42.7.3.jar,file:///app/jars/clickhouse-jdbc-0.4.6.jar") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")
    logger.info("Spark session initialized")

    
    try:
        logger.info("Listing files in /app/jars/")
        jar_files = os.listdir("/app/jars")
        logger.info(f"JAR files in /app/jars/: {jar_files}")
        
        logger.info("Listing files in /opt/bitnami/spark/jars/")
        spark_jar_files = os.listdir("/opt/bitnami/spark/jars")
        logger.info(f"JAR files in Spark jars: {spark_jar_files}")
    except Exception as e:
        logger.error(f"Failed to list files: {str(e)}")

    pg_url = "jdbc:postgresql://postgres:5432/snowflake"
    pg_properties = {
        "user": "snowflake",
        "password": "snowflake",
        "driver": "org.postgresql.Driver"
    }
    
    ch_url = "jdbc:clickhouse://clickhouse:8123"
    ch_db_url = "jdbc:clickhouse://clickhouse:8123/analytics"
    ch_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }
    
    try:
        execute_jdbc_query(
            spark,
            ch_url,
            "CREATE DATABASE IF NOT EXISTS analytics",
            "admin",
            "admin",
            "com.clickhouse.jdbc.ClickHouseDriver"
        )
        logger.info("Database 'analytics' created or exists")
    except Exception as e:
        logger.error(f"Failed to create database: {str(e)}")
        spark.stop()
        return

    logger.info("Reading data from PostgreSQL...")
    if not wait_for_data(spark, pg_url, "lab1.fact_sales", pg_properties):
        logger.error("Aborting ETL due to missing data in fact_sales")
        spark.stop()
        return
    
    try:
        
        fact_sales = spark.read.jdbc(pg_url, "lab1.fact_sales", properties=pg_properties)
        dim_product = spark.read.jdbc(pg_url, "lab1.dim_product", properties=pg_properties)
        dim_customer = spark.read.jdbc(pg_url, "lab1.dim_customer", properties=pg_properties)
        dim_store = spark.read.jdbc(pg_url, "lab1.dim_store", properties=pg_properties)
        dim_supplier = spark.read.jdbc(pg_url, "lab1.dim_supplier", properties=pg_properties)
        
        logger.info(f"Loaded fact_sales: {fact_sales.count()} rows")
        logger.info(f"Loaded dim_product: {dim_product.count()} rows")
    except Exception as e:
        logger.error(f"Error reading from PostgreSQL: {str(e)}")
        spark.stop()
        return


    ddl_statements = {
        "product_sales_mart": """
            CREATE TABLE IF NOT EXISTS analytics.product_sales_mart (
                product_key Int32,
                product_name String,
                category String,
                total_sold Int64,
                total_revenue Decimal(18,2),
                avg_rating Decimal(3,2),
                total_reviews Int64
            ) ENGINE = MergeTree()
            ORDER BY (category, product_key)
        """,
        
        "customer_sales_mart": """
            CREATE TABLE IF NOT EXISTS analytics.customer_sales_mart (
                customer_key Int32,
                country String,
                total_spent Decimal(18,2),
                avg_order_value Decimal(18,2),
                order_count Int64
            ) ENGINE = MergeTree()
            ORDER BY (country, customer_key)
        """,
        
        "time_sales_mart": """
            CREATE TABLE IF NOT EXISTS analytics.time_sales_mart (
                year Int32,
                quarter Int32,
                month Int32,
                total_sales Decimal(18,2),
                avg_order_size Decimal(18,2),
                order_count Int64
            ) ENGINE = MergeTree()
            ORDER BY (year, quarter, month)
        """,
        
        "store_sales_mart": """
            CREATE TABLE IF NOT EXISTS analytics.store_sales_mart (
                store_key Int32,
                name String,
                city String,
                country String,
                total_sales Decimal(18,2),
                order_count Int64
            ) ENGINE = MergeTree()
            ORDER BY (country, city, name)
        """,
        
        "supplier_sales_mart": """
            CREATE TABLE IF NOT EXISTS analytics.supplier_sales_mart (
                supplier_key Int32,
                name String,
                country String,
                total_sales Decimal(18,2),
                avg_product_price Decimal(18,2)
            ) ENGINE = MergeTree()
            ORDER BY (country, name)
        """,
        
        "product_quality_mart": """
            CREATE TABLE IF NOT EXISTS analytics.product_quality_mart (
                product_key Int32,
                product_name String,
                avg_rating Decimal(3,2),
                total_reviews Int64,
                total_sold Int64
            ) ENGINE = MergeTree()
            ORDER BY (avg_rating, total_sold)
        """
    }
    
    for table_name, ddl in ddl_statements.items():
        try:
            execute_jdbc_query(
                spark,
                ch_db_url,
                ddl,
                "admin",
                "admin",
                "com.clickhouse.jdbc.ClickHouseDriver"
            )
            logger.info(f"Table created: {table_name}")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {str(e)}")

    try:
        logger.info("Calculating product_sales_mart...")
        product_sales = fact_sales.join(dim_product, "product_key") \
            .groupBy("product_key", "name", "category") \
            .agg(
                F.sum("quantity").alias("total_sold"),
                F.sum("total_price").alias("total_revenue"),
                F.avg("rating").alias("avg_rating"),
                F.sum("reviews").alias("total_reviews")
            ) \
            .withColumnRenamed("name", "product_name")
        
        product_sales.write \
            .format("jdbc") \
            .option("url", ch_db_url) \
            .option("dbtable", "product_sales_mart") \
            .option("user", "admin") \
            .option("password", "admin") \
            .mode("append") \
            .save()
        logger.info("product_sales_mart written to ClickHouse")
    except Exception as e:
        logger.error(f"Failed to write product_sales_mart: {str(e)}")

    try:
        logger.info("Calculating customer_sales_mart...")
        customer_sales = fact_sales.join(dim_customer, "customer_key") \
            .groupBy("customer_key", "country") \
            .agg(
                F.sum("total_price").alias("total_spent"),
                F.avg("total_price").alias("avg_order_value"),
                F.count("*").alias("order_count")
            )
        
        customer_sales.write \
            .format("jdbc") \
            .option("url", ch_db_url) \
            .option("dbtable", "customer_sales_mart") \
            .option("user", "admin") \
            .option("password", "admin") \
            .mode("append") \
            .save()
        logger.info("customer_sales_mart written to ClickHouse")
    except Exception as e:
        logger.error(f"Failed to write customer_sales_mart: {str(e)}")
    
    try:
        logger.info("Calculating time_sales_mart...")
        time_sales = fact_sales.withColumn("year", F.year("sale_date")) \
            .withColumn("quarter", F.quarter("sale_date")) \
            .withColumn("month", F.month("sale_date")) \
            .groupBy("year", "quarter", "month") \
            .agg(
                F.sum("total_price").alias("total_sales"),
                F.avg("quantity").alias("avg_order_size"),
                F.count("*").alias("order_count")
            )
        
        time_sales.write \
            .format("jdbc") \
            .option("url", ch_db_url) \
            .option("dbtable", "time_sales_mart") \
            .option("user", "admin") \
            .option("password", "admin") \
            .mode("append") \
            .save()
        logger.info("time_sales_mart written to ClickHouse")
    except Exception as e:
        logger.error(f"Failed to write time_sales_mart: {str(e)}")
    
    try:
        logger.info("Calculating store_sales_mart...")
        store_sales = fact_sales.join(dim_store, "store_key") \
            .groupBy("store_key", "name", "city", "country") \
            .agg(
                F.sum("total_price").alias("total_sales"),
                F.count("*").alias("order_count")
            )
        
        store_sales.write \
            .format("jdbc") \
            .option("url", ch_db_url) \
            .option("dbtable", "store_sales_mart") \
            .option("user", "admin") \
            .option("password", "admin") \
            .mode("append") \
            .save()
        logger.info("store_sales_mart written to ClickHouse")
    except Exception as e:
        logger.error(f"Failed to write store_sales_mart: {str(e)}")

    try:
        logger.info("Calculating supplier_sales_mart...")
        supplier_sales = fact_sales.join(dim_supplier, "supplier_key") \
            .join(dim_product, "product_key") \
            .groupBy("supplier_key", "name", "country") \
            .agg(
                F.sum("total_price").alias("total_sales"),
                F.avg("price").alias("avg_product_price")
            )
        
        supplier_sales.write \
            .format("jdbc") \
            .option("url", ch_db_url) \
            .option("dbtable", "supplier_sales_mart") \
            .option("user", "admin") \
            .option("password", "admin") \
            .mode("append") \
            .save()
        logger.info("supplier_sales_mart written to ClickHouse")
    except Exception as e:
        logger.error(f"Failed to write supplier_sales_mart: {str(e)}")
    
    try:
        logger.info("Calculating product_quality_mart...")
        product_quality = fact_sales.join(dim_product, "product_key") \
            .groupBy("product_key", "name") \
            .agg(
                F.avg("rating").alias("avg_rating"),
                F.sum("reviews").alias("total_reviews"),
                F.sum("quantity").alias("total_sold")
            ) \
            .withColumnRenamed("name", "product_name")
        
        product_quality.write \
            .format("jdbc") \
            .option("url", ch_db_url) \
            .option("dbtable", "product_quality_mart") \
            .option("user", "admin") \
            .option("password", "admin") \
            .mode("append") \
            .save()
        logger.info("product_quality_mart written to ClickHouse")
    except Exception as e:
        logger.error(f"Failed to write product_quality_mart: {str(e)}")
    
    spark.stop()
    logger.info("ETL to ClickHouse completed successfully")

if __name__ == "__main__":
    main()