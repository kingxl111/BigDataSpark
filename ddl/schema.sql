-- Create schema and staging
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS lab1;

-- Staging table
CREATE TABLE IF NOT EXISTS staging.mock_data (
  id                     INTEGER,
  customer_first_name    TEXT,
  customer_last_name     TEXT,
  customer_age           INTEGER,
  customer_email         TEXT,
  customer_country       TEXT,
  customer_postal_code   TEXT,
  customer_pet_type      TEXT,
  customer_pet_name      TEXT,
  customer_pet_breed     TEXT,
  seller_first_name      TEXT,
  seller_last_name       TEXT,
  seller_email           TEXT,
  seller_country         TEXT,
  seller_postal_code     TEXT,
  product_name           TEXT,
  product_category       TEXT,
  product_price          NUMERIC(10,2),
  product_quantity       INTEGER,
  sale_date              DATE,
  sale_customer_id       INTEGER,
  sale_seller_id         INTEGER,
  sale_product_id        INTEGER,
  sale_quantity          INTEGER,
  sale_total_price       NUMERIC(12,2),
  store_name             TEXT,
  store_location         TEXT,
  store_city             TEXT,
  store_state            TEXT,
  store_country          TEXT,
  store_phone            TEXT,
  store_email            TEXT,
  pet_category           TEXT,
  product_weight         NUMERIC(8,2),
  product_color          TEXT,
  product_size           TEXT,
  product_brand          TEXT,
  product_material       TEXT,
  product_description    TEXT,
  product_rating         NUMERIC(3,2),
  product_reviews        INTEGER,
  product_release_date   DATE,
  product_expiry_date    DATE,
  supplier_name          TEXT,
  supplier_contact       TEXT,
  supplier_email         TEXT,
  supplier_phone         TEXT,
  supplier_address       TEXT,
  supplier_city          TEXT,
  supplier_country       TEXT
);

-- Dimension tables
CREATE TABLE IF NOT EXISTS lab1.dim_customer (
  customer_key  SERIAL PRIMARY KEY,
  first_name    TEXT,
  last_name     TEXT,
  age           INTEGER,
  email         TEXT,
  country       TEXT,
  postal_code   TEXT
);

CREATE TABLE IF NOT EXISTS lab1.dim_seller (
  seller_key    SERIAL PRIMARY KEY,
  first_name    TEXT,
  last_name     TEXT,
  email         TEXT,
  country       TEXT,
  postal_code   TEXT
);

CREATE TABLE IF NOT EXISTS lab1.dim_product (
  product_key   SERIAL PRIMARY KEY,
  name          TEXT,
  category      TEXT,
  price         NUMERIC(10,2),
  weight        NUMERIC(8,2),
  color         TEXT,
  size          TEXT,
  brand         TEXT,
  material      TEXT,
  description   TEXT,
  rating        NUMERIC(3,2),
  reviews       INTEGER,
  release_date  DATE,
  expiry_date   DATE
);

CREATE TABLE IF NOT EXISTS lab1.dim_store (
  store_key     SERIAL PRIMARY KEY,
  name          TEXT,
  location      TEXT,
  city          TEXT,
  state         TEXT,
  country       TEXT,
  phone         TEXT,
  email         TEXT
);

CREATE TABLE IF NOT EXISTS lab1.dim_supplier (
  supplier_key  SERIAL PRIMARY KEY,
  name          TEXT,
  contact       TEXT,
  email         TEXT,
  phone         TEXT,
  address       TEXT,
  city          TEXT,
  country       TEXT
);

CREATE TABLE IF NOT EXISTS lab1.fact_sales (
  sale_key        SERIAL PRIMARY KEY,
  sale_date       DATE,
  customer_key    INTEGER REFERENCES lab1.dim_customer(customer_key),
  seller_key      INTEGER REFERENCES lab1.dim_seller(seller_key),
  product_key     INTEGER REFERENCES lab1.dim_product(product_key),
  store_key       INTEGER REFERENCES lab1.dim_store(store_key),
  supplier_key    INTEGER REFERENCES lab1.dim_supplier(supplier_key),
  quantity        INTEGER,
  total_price     NUMERIC(12,2)
);