-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Silver — clean / type / de-duplicate
-- MAGIC
-- MAGIC One table per source: standardise casing (`initcap` / `lower` / `trim`), cast types,
-- MAGIC drop duplicates with `row_number() OVER (...)`, and add light enrichment
-- MAGIC (customer `age` / `age_group` / `income_bracket`, `days_in_month`, `snapshot_month_id`).

-- COMMAND ----------

USE CATALOG elecmart;
USE SCHEMA silver;

-- COMMAND ----------

-- MAGIC %md ### silver_dim_brand

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_dim_brand AS
WITH source_data AS (
  SELECT brand_id, brand_name,
         row_number() OVER (PARTITION BY brand_name ORDER BY brand_id) AS rn
  FROM elecmart.bronze.dim_brand
)
SELECT brand_id, brand_name FROM source_data WHERE rn = 1 ORDER BY brand_id;

-- COMMAND ----------

-- MAGIC %md ### silver_dim_campaign

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_dim_campaign AS
SELECT
  CAST(campaign_id AS INT)                    AS campaign_id,
  campaign_name,
  lower(campaign_channel)                     AS campaign_channel,
  promo_id,
  CAST(campaign_start_date AS TIMESTAMP_NTZ)  AS campaign_start_date,
  CAST(campaign_start_date_id AS INT)         AS campaign_start_date_id,
  CAST(campaign_end_date AS TIMESTAMP_NTZ)    AS campaign_end_date,
  CAST(campaign_end_date_id AS INT)           AS campaign_end_date_id
FROM elecmart.bronze.dim_campaign;

-- COMMAND ----------

-- MAGIC %md ### silver_dim_category / silver_dim_subcategory

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_dim_category AS
WITH deduplicate AS (
  SELECT category_id, initcap(category_name) AS category_name,
         row_number() OVER (PARTITION BY category_id, category_name ORDER BY category_id) AS rn
  FROM elecmart.bronze.dim_category
)
SELECT category_id, category_name FROM deduplicate WHERE rn = 1;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_dim_subcategory AS
SELECT subcategory_id, initcap(subcategory_name) AS subcategory_name, category_id
FROM elecmart.bronze.dim_subcategory;

-- COMMAND ----------

-- MAGIC %md ### silver_dim_customer  (age, age_group, income_bracket enrichment)

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_dim_customer AS
WITH source AS (
  SELECT
    customer_id,
    lower(email_address)                                     AS email_address,
    initcap(first_name)                                      AS first_name,
    initcap(last_name)                                       AS last_name,
    lower(gender)                                            AS gender,
    initcap(customer_persona)                                AS customer_persona,
    CAST(birth_date AS DATE)                                 AS birth_date,
    CAST(birth_year AS INT)                                  AS birth_year,
    date_diff(YEAR, CAST(birth_date AS DATE), current_date()) AS age,
    CAST(location_id AS INT)                                 AS location_id,
    CAST(signup_date AS DATE)                                AS signup_date,
    CAST(signup_date_id AS INT)                              AS signup_date_id,
    CASE
      WHEN lower(signup_channel) = 'mobile app' THEN 'mobile'
      WHEN lower(signup_channel) = 'online'     THEN 'web'
      WHEN lower(signup_channel) = 'in-store'   THEN 'store'
      ELSE lower(signup_channel)
    END                                                      AS signup_channel,
    lower(loyalty_status)                                    AS loyalty_status,
    CAST(estimated_annual_income AS DECIMAL(10,2))           AS estimated_annual_income,
    email_opt_in,
    sms_opt_in
  FROM elecmart.bronze.dim_customer
),
deduplicate AS (
  SELECT *, row_number() OVER (PARTITION BY email_address ORDER BY signup_date) AS rn FROM source
),
deduplicated AS (
  SELECT customer_id, email_address, first_name, last_name, gender, customer_persona,
         birth_date, birth_year, age, location_id, signup_date, signup_date_id,
         signup_channel, loyalty_status, estimated_annual_income, email_opt_in, sms_opt_in
  FROM deduplicate WHERE rn = 1
)
SELECT
  customer_id, email_address, first_name, last_name, gender, customer_persona,
  birth_date, birth_year, age,
  CASE
    WHEN age BETWEEN 18 AND 24 THEN '18-24'
    WHEN age BETWEEN 25 AND 34 THEN '25-34'
    WHEN age BETWEEN 35 AND 44 THEN '35-44'
    WHEN age BETWEEN 45 AND 54 THEN '45-54'
    ELSE '55+'
  END AS age_group,
  location_id, signup_date, signup_date_id, signup_channel, loyalty_status,
  CASE
    WHEN estimated_annual_income < 50000 THEN 'Low Income'
    WHEN estimated_annual_income BETWEEN 50000 AND 100000 THEN 'Middle Income'
    ELSE 'High Income'
  END AS income_bracket,
  email_opt_in, sms_opt_in
FROM deduplicated;

-- COMMAND ----------

-- MAGIC %md ### silver_dim_date

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_dim_date AS
SELECT *, day(last_day(`date`)) AS days_in_month
FROM elecmart.bronze.dim_date;

-- COMMAND ----------

-- MAGIC %md ### silver_dim_location

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_dim_location AS
SELECT location_id,
       initcap(country)        AS country,
       initcap(state_province) AS state_province,
       initcap(city)           AS city,
       initcap(location_type)  AS location_type
FROM elecmart.bronze.dim_location;

-- COMMAND ----------

-- MAGIC %md ### silver_dim_product  (drops rows where unit_cost >= unit_price or NULLs)

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_dim_product AS
WITH source AS (
  SELECT product_id,
         initcap(product_name)            AS product_name,
         CAST(category_id AS INT)         AS category_id,
         CAST(subcategory_id AS INT)      AS subcategory_id,
         CAST(brand_id AS INT)            AS brand_id,
         CAST(unit_cost AS DECIMAL(10,2)) AS unit_cost,
         CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
         CAST(warranty_years AS INT)      AS warranty_years,
         initcap(product_segment)         AS product_segment
  FROM elecmart.bronze.dim_product
),
deduplicate AS (
  SELECT *, row_number() OVER (PARTITION BY product_id, product_name ORDER BY product_id) AS rn
  FROM source
),
check_nulls AS (
  SELECT * FROM deduplicate
  WHERE rn = 1
    AND product_name IS NOT NULL
    AND unit_cost < unit_price
    AND unit_cost IS NOT NULL
    AND unit_price IS NOT NULL
)
SELECT product_id, product_name, category_id, subcategory_id, brand_id,
       unit_cost, unit_price, warranty_years, product_segment
FROM check_nulls;

-- COMMAND ----------

-- MAGIC %md ### silver_dim_promotion

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_dim_promotion AS
WITH source AS (
  SELECT CAST(promo_id AS INT)                    AS promo_id,
         initcap(promo_name)                      AS promo_name,
         initcap(promo_type)                      AS promo_type,
         initcap(discount_type)                   AS discount_type,
         CAST(discount_value AS DECIMAL(10,2))    AS discount_value,
         CAST(promo_start_date AS TIMESTAMP_NTZ)  AS promo_start_date,
         CAST(promo_start_date_id AS INT)         AS promo_start_date_id,
         CAST(promo_end_date AS TIMESTAMP_NTZ)    AS promo_end_date,
         CAST(promo_end_date_id AS INT)           AS promo_end_date_id,
         CAST(promo_duration AS INT)              AS promo_duration,
         CAST(is_active AS BOOLEAN)               AS is_active,
         row_number() OVER (PARTITION BY promo_id ORDER BY promo_start_date) AS rn
  FROM elecmart.bronze.dim_promotion
),
check_quality AS (
  SELECT * FROM source WHERE rn = 1 AND promo_start_date IS NOT NULL
)
SELECT promo_id, promo_name, promo_type, discount_type, discount_value,
       promo_start_date, promo_start_date_id, promo_end_date, promo_end_date_id, is_active
FROM check_quality;

-- COMMAND ----------

-- MAGIC %md ### silver_dim_store

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_dim_store AS
SELECT CAST(store_id AS INT)           AS store_id,
       initcap(store_name)             AS store_name,
       CAST(location_id AS INT)        AS location_id,
       initcap(store_type)            AS store_type,
       CAST(store_size AS INT)         AS store_size,
       CAST(opening_date AS DATE)      AS opening_date,
       CAST(opening_date_id AS INT)    AS opening_date_id,
       CAST(foot_traffic_index AS INT) AS foot_traffic_index
FROM elecmart.bronze.dim_store;

-- COMMAND ----------

-- MAGIC %md ### silver_fact_clickstream

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_fact_clickstream AS
SELECT CAST(session_id AS INT)                    AS session_id,
       CAST(customer_id AS INT)                   AS customer_id,
       CAST(session_start_time AS TIMESTAMP_NTZ)  AS session_start_time,
       CAST(session_start_date_id AS INT)         AS session_start_date_id,
       CAST(session_end_time AS TIMESTAMP_NTZ)    AS session_end_time,
       CAST(session_end_date_id AS INT)           AS session_end_date_id,
       trim(device_type)                          AS device_type,
       CAST(number_of_pages_viewed AS INT)        AS number_of_pages_viewed,
       CAST(product_page_visited_flag AS BOOLEAN) AS product_page_visited_flag,
       CAST(added_to_cart_flag AS BOOLEAN)        AS added_to_cart_flag,
       CAST(purchased_flag AS BOOLEAN)            AS purchased_flag,
       traffic_source,
       CAST(linked_to_a_campaign_flag AS BOOLEAN) AS linked_to_a_campaign_flag,
       CAST(campaign_id AS INT)                   AS campaign_id
FROM elecmart.bronze.fact_clickstream;

-- COMMAND ----------

-- MAGIC %md ### silver_fact_inventory

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_fact_inventory AS
SELECT CAST(inventory_id AS INT)   AS inventory_id,
       CAST(product_id AS INT)     AS product_id,
       CAST(store_id AS INT)       AS store_id,
       CAST(snapshot_month AS DATE) AS snapshot_month,
       CAST(date_format(CAST(snapshot_month AS DATE), 'yyyyMM') AS INT) AS snapshot_month_id,
       CAST(starting_stock AS INT) AS starting_stock,
       CAST(received_stock AS INT) AS received_stock,
       CAST(sold_units AS INT)     AS sold_units,
       CAST(closing_stock AS INT)  AS closing_stock,
       CAST(backorder_flag AS BOOLEAN) AS backorder_flag,
       CAST(shrinkage_loss AS INT) AS shrinkage_loss
FROM elecmart.bronze.inventory;

-- COMMAND ----------

-- MAGIC %md ### silver_fact_sale

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_fact_sale AS
SELECT CAST(sale_id AS INT)                         AS sale_id,
       CAST(transaction_id AS INT)                  AS transaction_id,
       CAST(session_id AS INT)                      AS session_id,
       CAST(transaction_timestamp AS TIMESTAMP_NTZ) AS transaction_timestamp,
       CAST(transaction_date_id AS INT)             AS transaction_date_id,
       CAST(product_id AS INT)                      AS product_id,
       CAST(quantity AS INT)                        AS quantity,
       CAST(unit_cost AS DECIMAL(10,2))             AS unit_cost,
       CAST(unit_price AS DECIMAL(10,2))            AS unit_price,
       CAST(line_cost AS DECIMAL(10,2))             AS line_cost,
       CAST(line_total AS DECIMAL(10,2))            AS line_total
FROM elecmart.bronze.fact_sale;

-- COMMAND ----------

-- MAGIC %md ### silver_fact_transaction

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_fact_transaction AS
SELECT CAST(transaction_id AS INT)                        AS transaction_id,
       CAST(transaction_timestamp AS TIMESTAMP_NTZ)       AS transaction_timestamp,
       CAST(transaction_date_id AS INT)                   AS transaction_date_id,
       CAST(customer_id AS INT)                           AS customer_id,
       CAST(store_id AS INT)                              AS store_id,
       CASE WHEN sales_channel = 'In-Store' THEN 'store' ELSE lower(sales_channel) END AS sales_channel,
       CAST(session_id AS INT)                            AS session_id,
       CAST(promo_id AS INT)                              AS promo_id,
       CAST(campaign_id AS INT)                           AS campaign_id,
       CAST(transaction_subtotal AS DECIMAL(10,2))        AS transaction_subtotal,
       CAST(transaction_discount_applied AS DECIMAL(10,2)) AS transaction_discount_applied,
       CAST(transaction_total AS DECIMAL(10,2))           AS transaction_total,
       CAST(transaction_cost AS DECIMAL(10,2))            AS transaction_cost,
       CAST(items_count AS INT)                           AS items_count,
       payment_type,
       transaction_status
FROM elecmart.bronze.fact_transaction;