-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Gold — conformed star schema
-- MAGIC
-- MAGIC 6 dimensions + 4 facts, built from Silver. Dimensions denormalise their lookup tables
-- MAGIC (customer+location, product+brand+category, store+location); `gold_fact_sale` allocates the
-- MAGIC transaction-level discount down to each line.
-- MAGIC
-- MAGIC The last cell adds **informational PK/FK constraints** — not enforced, but they let
-- MAGIC AI/BI Dashboards and Genie auto-discover the joins.

-- COMMAND ----------

USE CATALOG elecmart;
USE SCHEMA gold;

-- COMMAND ----------

-- MAGIC %md ## Dimensions

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_dim_date AS
SELECT *,
       CAST(date_format(`date`, 'yyyyMM') AS INT) AS month_id,
       year || '-Q' || quarter                   AS year_quarter
FROM elecmart.silver.silver_dim_date;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_dim_customer AS
SELECT c.customer_id, c.email_address, c.first_name, c.last_name, c.gender,
       c.customer_persona, c.age_group,
       l.country, l.state_province, l.city, l.location_type,
       c.signup_date, c.signup_date_id, c.signup_channel, c.loyalty_status,
       c.income_bracket, c.email_opt_in, c.sms_opt_in
FROM elecmart.silver.silver_dim_customer c
INNER JOIN elecmart.silver.silver_dim_location l
  ON c.location_id = l.location_id;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_dim_product AS
SELECT dp.product_id, dp.product_name,
       dp.brand_id,       db.brand_name        AS brand_name,
       dp.category_id,    dc.category_name     AS category_name,
       dp.subcategory_id, dsc.subcategory_name AS subcategory_name,
       dp.unit_cost, dp.unit_price, dp.warranty_years
FROM elecmart.silver.silver_dim_product dp
INNER JOIN elecmart.silver.silver_dim_brand       db  ON dp.brand_id = db.brand_id
INNER JOIN elecmart.silver.silver_dim_category    dc  ON dp.category_id = dc.category_id
INNER JOIN elecmart.silver.silver_dim_subcategory dsc ON dp.subcategory_id = dsc.subcategory_id;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_dim_store AS
SELECT s.store_id, s.store_name, s.store_type, s.store_size,
       s.opening_date, s.opening_date_id, s.foot_traffic_index,
       l.country, l.state_province, l.city, l.location_type
FROM elecmart.silver.silver_dim_store s
INNER JOIN elecmart.silver.silver_dim_location l
  ON s.location_id = l.location_id;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_dim_campaign  AS SELECT * FROM elecmart.silver.silver_dim_campaign;
CREATE OR REPLACE TABLE gold_dim_promotion AS SELECT * FROM elecmart.silver.silver_dim_promotion;

-- COMMAND ----------

-- MAGIC %md ## Facts

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_fact_transaction AS SELECT * FROM elecmart.silver.silver_fact_transaction;
CREATE OR REPLACE TABLE gold_fact_inventory   AS SELECT * FROM elecmart.silver.silver_fact_inventory;

-- gold_fact_clickstream is a pure pass-through of 14.5M rows — keep it a VIEW so Free Edition
-- doesn't pay to store a second copy. (Change to a TABLE if you want an FK on it.)
CREATE OR REPLACE VIEW gold_fact_clickstream AS SELECT * FROM elecmart.silver.silver_fact_clickstream;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### gold_fact_sale — allocates the transaction-level discount down to each line
-- MAGIC `allocated_line_discount = line_total / transaction_subtotal * transaction_discount_applied`

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_fact_sale AS
WITH calculate_line_discount AS (
  SELECT
    fs.sale_id, fs.transaction_id, fs.session_id, fs.transaction_timestamp,
    fs.transaction_date_id, fs.product_id,
    ft.store_id, ft.customer_id, ft.sales_channel, ft.campaign_id, ft.promo_id,
    fs.quantity, fs.unit_cost, fs.unit_price, fs.line_cost, fs.line_total,
    round(fs.line_total / nullif(ft.transaction_subtotal, 0)
          * coalesce(ft.transaction_discount_applied, 0), 2) AS allocated_line_discount,
    ft.transaction_status AS transaction_status
  FROM elecmart.silver.silver_fact_sale fs
  JOIN elecmart.silver.silver_fact_transaction ft
    ON fs.transaction_id = ft.transaction_id
)
SELECT *,
       round(line_total - allocated_line_discount, 2)             AS net_line_revenue,
       round(line_total - allocated_line_discount - line_cost, 2) AS net_line_profit
FROM calculate_line_discount;

-- COMMAND ----------

-- MAGIC %md ## Informational constraints (optional but recommended)
-- MAGIC Not enforced by Databricks — they document the model and feed Genie / AI-BI join discovery.
-- MAGIC If a `SET NOT NULL` fails, some key has NULLs; fix the data or skip that line.

-- COMMAND ----------

ALTER TABLE gold_dim_date     ALTER COLUMN date_id     SET NOT NULL;
ALTER TABLE gold_dim_customer ALTER COLUMN customer_id SET NOT NULL;
ALTER TABLE gold_dim_product  ALTER COLUMN product_id  SET NOT NULL;
ALTER TABLE gold_dim_store    ALTER COLUMN store_id    SET NOT NULL;
ALTER TABLE gold_dim_campaign ALTER COLUMN campaign_id SET NOT NULL;
ALTER TABLE gold_dim_promotion ALTER COLUMN promo_id   SET NOT NULL;

ALTER TABLE gold_dim_date     ADD CONSTRAINT pk_gold_dim_date     PRIMARY KEY (date_id);
ALTER TABLE gold_dim_customer ADD CONSTRAINT pk_gold_dim_customer PRIMARY KEY (customer_id);
ALTER TABLE gold_dim_product  ADD CONSTRAINT pk_gold_dim_product  PRIMARY KEY (product_id);
ALTER TABLE gold_dim_store    ADD CONSTRAINT pk_gold_dim_store    PRIMARY KEY (store_id);
ALTER TABLE gold_dim_campaign ADD CONSTRAINT pk_gold_dim_campaign PRIMARY KEY (campaign_id);
ALTER TABLE gold_dim_promotion ADD CONSTRAINT pk_gold_dim_promotion PRIMARY KEY (promo_id);

ALTER TABLE gold_fact_sale ADD CONSTRAINT fk_sale_product
  FOREIGN KEY (product_id) REFERENCES gold_dim_product;
ALTER TABLE gold_fact_sale ADD CONSTRAINT fk_sale_customer
  FOREIGN KEY (customer_id) REFERENCES gold_dim_customer;
ALTER TABLE gold_fact_sale ADD CONSTRAINT fk_sale_store
  FOREIGN KEY (store_id) REFERENCES gold_dim_store;
ALTER TABLE gold_fact_sale ADD CONSTRAINT fk_sale_date
  FOREIGN KEY (transaction_date_id) REFERENCES gold_dim_date;

ALTER TABLE gold_fact_transaction ADD CONSTRAINT fk_txn_customer
  FOREIGN KEY (customer_id) REFERENCES gold_dim_customer;
ALTER TABLE gold_fact_transaction ADD CONSTRAINT fk_txn_store
  FOREIGN KEY (store_id) REFERENCES gold_dim_store;
ALTER TABLE gold_fact_transaction ADD CONSTRAINT fk_txn_date
  FOREIGN KEY (transaction_date_id) REFERENCES gold_dim_date;

-- (gold_fact_clickstream is a VIEW — no constraints; Genie still infers the join by column name)

ALTER TABLE gold_fact_inventory ADD CONSTRAINT fk_inv_product
  FOREIGN KEY (product_id) REFERENCES gold_dim_product;
ALTER TABLE gold_fact_inventory ADD CONSTRAINT fk_inv_store
  FOREIGN KEY (store_id) REFERENCES gold_dim_store;