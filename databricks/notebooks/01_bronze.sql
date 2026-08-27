-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 · Bronze — load raw Parquet into Delta
-- MAGIC
-- MAGIC `read_files()` infers schema and reads every part-file for each source.
-- MAGIC Assumes the 14 raw files are uploaded **flat** into the volume root:
-- MAGIC
-- MAGIC ```
-- MAGIC /Volumes/elecmart/bronze/landing/
-- MAGIC   dim_brand.parquet      dim_campaign.parquet   dim_category.parquet
-- MAGIC   dim_customer.parquet   dim_date.parquet       dim_location.parquet
-- MAGIC   dim_product.parquet    dim_promotion.parquet  dim_store.parquet
-- MAGIC   dim_subcategory.parquet
-- MAGIC   fact_transaction.parquet  fact_sale.parquet
-- MAGIC   fact_clickstream.parquet  inventory.parquet
-- MAGIC ```
-- MAGIC
-- MAGIC `read_files()` also accepts a folder or glob — if a source is split into part-files,
-- MAGIC point it at `.../landing/fact_clickstream/` instead of the `.parquet` file.
-- MAGIC Re-runnable: `CREATE OR REPLACE TABLE` rebuilds each table.

-- COMMAND ----------

USE CATALOG elecmart;
USE SCHEMA bronze;

-- COMMAND ----------
-- MAGIC %md ### Dimensions

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_customer AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/dim_customer.parquet', format => 'parquet');

CREATE OR REPLACE TABLE dim_product AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/dim_product.parquet', format => 'parquet');

CREATE OR REPLACE TABLE dim_store AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/dim_store.parquet', format => 'parquet');

CREATE OR REPLACE TABLE dim_promotion AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/dim_promotion.parquet', format => 'parquet');

CREATE OR REPLACE TABLE dim_campaign AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/dim_campaign.parquet', format => 'parquet');

CREATE OR REPLACE TABLE dim_category AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/dim_category.parquet', format => 'parquet');

CREATE OR REPLACE TABLE dim_subcategory AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/dim_subcategory.parquet', format => 'parquet');

-- dim_brand.parquet ships with TitleCase headers (Brand_ID, Brand_Name) — normalise here.
CREATE OR REPLACE TABLE dim_brand AS
SELECT `Brand_ID` AS brand_id, `Brand_Name` AS brand_name
FROM read_files('/Volumes/elecmart/bronze/landing/dim_brand.parquet', format => 'parquet');

CREATE OR REPLACE TABLE dim_location AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/dim_location.parquet', format => 'parquet');

CREATE OR REPLACE TABLE dim_date AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/dim_date.parquet', format => 'parquet');

-- COMMAND ----------
-- MAGIC %md ### Facts  (fact_clickstream ≈ 14.5 M rows / 351 MB — a couple of minutes on 2X-Small serverless)

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_transaction AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/fact_transaction.parquet', format => 'parquet');

CREATE OR REPLACE TABLE fact_sale AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/fact_sale.parquet', format => 'parquet');

CREATE OR REPLACE TABLE fact_clickstream AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/fact_clickstream.parquet', format => 'parquet');

CREATE OR REPLACE TABLE inventory AS
SELECT * FROM read_files('/Volumes/elecmart/bronze/landing/inventory.parquet', format => 'parquet');

-- COMMAND ----------
-- MAGIC %md ### Row-count check  (expected: customer 150k · product 470 · txn 900k · sale ~1.8M · clickstream 14.5M · inventory 846k)

-- COMMAND ----------

SELECT 'dim_customer'      AS table_name, count(*) AS rows FROM dim_customer
UNION ALL SELECT 'dim_product',      count(*) FROM dim_product
UNION ALL SELECT 'dim_store',        count(*) FROM dim_store
UNION ALL SELECT 'dim_promotion',    count(*) FROM dim_promotion
UNION ALL SELECT 'dim_campaign',     count(*) FROM dim_campaign
UNION ALL SELECT 'dim_category',     count(*) FROM dim_category
UNION ALL SELECT 'dim_subcategory',  count(*) FROM dim_subcategory
UNION ALL SELECT 'dim_brand',        count(*) FROM dim_brand
UNION ALL SELECT 'dim_location',     count(*) FROM dim_location
UNION ALL SELECT 'dim_date',         count(*) FROM dim_date
UNION ALL SELECT 'fact_transaction', count(*) FROM fact_transaction
UNION ALL SELECT 'fact_sale',        count(*) FROM fact_sale
UNION ALL SELECT 'fact_clickstream', count(*) FROM fact_clickstream
UNION ALL SELECT 'inventory',        count(*) FROM inventory
ORDER BY table_name;
