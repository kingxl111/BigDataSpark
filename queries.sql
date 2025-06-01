-- Active: 1745169676464@@127.0.0.1@5432
SELECT
    product_name,
    total_sold,
    total_revenue,
    avg_rating,
    total_reviews
FROM product_sales_mart
ORDER BY total_sold DESC
LIMIT 10




SELECT
    customer_key,
    total_spent,
    order_count,
    total_spent / order_count AS avg_order_value
FROM customer_sales_mart
ORDER BY total_spent DESC
LIMIT 10




SELECT
    year,
    quarter,
    SUM(total_sales) AS total_sales,
    AVG(avg_order_size) AS avg_order_size
FROM time_sales_mart
GROUP BY year, quarter
ORDER BY year DESC, quarter DESC



SELECT
    store_name,
    city,
    country,
    SUM(total_sales) AS total_sales,
    SUM(order_count) AS order_count
FROM store_sales_mart
GROUP BY store_name, city, country
ORDER BY total_sales DESC
LIMIT 5



SELECT
    supplier_name,
    country,
    SUM(total_sales) AS total_sales,
    AVG(avg_product_price) AS avg_product_price
FROM supplier_sales_mart
GROUP BY supplier_name, country
ORDER BY total_sales DESC
LIMIT 5



SELECT
    product_name,
    avg_rating,
    total_reviews,
    total_sold
FROM product_quality_mart
ORDER BY avg_rating DESC, total_sold DESC
LIMIT 10
