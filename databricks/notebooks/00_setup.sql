-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup — catalog, schemas, landing volume
-- MAGIC
-- MAGIC Elecmart Retail Analytics lakehouse.
-- MAGIC
-- MAGIC **Run this once.** Works on Databricks **Free Edition** (serverless SQL warehouse or serverless notebook).
-- MAGIC
-- MAGIC What it creates:
-- MAGIC * catalog `elecmart`
-- MAGIC * schemas `bronze`, `silver`, `gold`, `_test_failures`
-- MAGIC * managed volume `elecmart.bronze.landing` — you upload the raw files here
-- MAGIC
-- MAGIC > If your workspace does not allow creating a catalog, replace every `elecmart.`
-- MAGIC > with `workspace.` (find & replace) in all 6 notebooks and skip the CREATE CATALOG line.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS elecmart
  COMMENT 'Elecmart retail analytics lakehouse';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS elecmart.bronze         COMMENT 'Raw ingested files, 1:1 with source';
CREATE SCHEMA IF NOT EXISTS elecmart.silver         COMMENT 'Cleaned, typed, de-duplicated';
CREATE SCHEMA IF NOT EXISTS elecmart.gold           COMMENT 'Star schema + analytics-ready BI marts';
CREATE SCHEMA IF NOT EXISTS elecmart._test_failures COMMENT 'Stored rows from failed data-quality checks';

-- COMMAND ----------

-- Managed volume: governed folder to upload the raw extract into.
CREATE VOLUME IF NOT EXISTS elecmart.bronze.landing
  COMMENT 'Upload the 14 source files/folders here, one sub-folder per table';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Upload the data (≈420 MB — well within Free Edition)
-- MAGIC
-- MAGIC In the Databricks UI: **Catalog ▸ elecmart ▸ bronze ▸ landing ▸ Upload to volume**, then
-- MAGIC drag in all **14 `.parquet` files** flat (no sub-folders):
-- MAGIC
-- MAGIC ```
-- MAGIC /Volumes/elecmart/bronze/landing/
-- MAGIC   dim_brand.parquet     dim_campaign.parquet   dim_category.parquet
-- MAGIC   dim_customer.parquet  dim_date.parquet       dim_location.parquet
-- MAGIC   dim_product.parquet   dim_promotion.parquet  dim_store.parquet
-- MAGIC   dim_subcategory.parquet
-- MAGIC   fact_transaction.parquet  fact_sale.parquet
-- MAGIC   fact_clickstream.parquet  inventory.parquet
-- MAGIC ```
-- MAGIC
-- MAGIC `01_bronze` reads each file by name. If a source is split into part-files, put them in a
-- MAGIC sub-folder and point `read_files()` at the folder instead.

-- COMMAND ----------

LIST '/Volumes/elecmart/bronze/landing/';
