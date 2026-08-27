-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Data quality checks
-- MAGIC
-- MAGIC Uniqueness / not-null on keys, fact→dim referential integrity, accepted-value domains,
-- MAGIC and custom business rules. Every check returns the count of **offending** rows — `0` = pass.
-- MAGIC The final cell calls `raise_error()` so a Workflow task fails loudly on any violation.

-- COMMAND ----------

USE CATALOG elecmart;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dq_results AS

-- ---- uniqueness / not-null on primary keys ----
SELECT 'silver_fact_transaction.transaction_id not null' AS check_name,
       count(*) AS failing_rows
FROM silver.silver_fact_transaction WHERE transaction_id IS NULL
UNION ALL
SELECT 'silver_fact_transaction.transaction_id unique',
       count(*) FROM (
         SELECT transaction_id FROM silver.silver_fact_transaction
         GROUP BY transaction_id HAVING count(*) > 1)
UNION ALL
SELECT 'silver_fact_sale.sale_id unique',
       count(*) FROM (
         SELECT sale_id FROM silver.silver_fact_sale
         GROUP BY sale_id HAVING count(*) > 1)
UNION ALL
SELECT 'silver_fact_clickstream.session_id unique',
       count(*) FROM (
         SELECT session_id FROM silver.silver_fact_clickstream
         GROUP BY session_id HAVING count(*) > 1)
UNION ALL
SELECT 'silver_dim_customer.customer_id unique',
       count(*) FROM (
         SELECT customer_id FROM silver.silver_dim_customer
         GROUP BY customer_id HAVING count(*) > 1)
UNION ALL
SELECT 'silver_dim_product.product_id unique',
       count(*) FROM (
         SELECT product_id FROM silver.silver_dim_product
         GROUP BY product_id HAVING count(*) > 1)

-- ---- referential integrity (fact → dim) ----
UNION ALL
SELECT 'fact_transaction.customer_id → dim_customer',
       count(*) FROM silver.silver_fact_transaction f
       LEFT JOIN silver.silver_dim_customer d ON f.customer_id = d.customer_id
       WHERE f.customer_id IS NOT NULL AND d.customer_id IS NULL
UNION ALL
SELECT 'fact_transaction.store_id → dim_store',
       count(*) FROM silver.silver_fact_transaction f
       LEFT JOIN silver.silver_dim_store d ON f.store_id = d.store_id
       WHERE f.store_id IS NOT NULL AND d.store_id IS NULL
UNION ALL
SELECT 'fact_transaction.transaction_date_id → dim_date',
       count(*) FROM silver.silver_fact_transaction f
       LEFT JOIN silver.silver_dim_date d ON f.transaction_date_id = d.date_id
       WHERE f.transaction_date_id IS NOT NULL AND d.date_id IS NULL
UNION ALL
SELECT 'fact_sale.product_id → dim_product',
       count(*) FROM silver.silver_fact_sale f
       LEFT JOIN silver.silver_dim_product d ON f.product_id = d.product_id
       WHERE f.product_id IS NOT NULL AND d.product_id IS NULL

-- ---- accepted values ----
UNION ALL
SELECT 'fact_transaction.sales_channel in (web,mobile,store)',
       count(*) FROM silver.silver_fact_transaction
       WHERE sales_channel NOT IN ('web','mobile','store')
UNION ALL
SELECT 'fact_transaction.transaction_status in (Completed,Returned)',
       count(*) FROM silver.silver_fact_transaction
       WHERE transaction_status NOT IN ('Completed','Returned')
UNION ALL
SELECT 'fact_transaction.payment_type accepted values',
       count(*) FROM silver.silver_fact_transaction
       WHERE payment_type NOT IN
       ('Credit Card','Debit Card','Cash','Gift Card','Apple Pay','Google Pay','PayPal')

-- ---- custom business rules (dbt tests/) ----
UNION ALL
SELECT 'dim_date: day within 1..days_in_month',
       count(*) FROM silver.silver_dim_date WHERE day < 1 OR day > days_in_month
UNION ALL
SELECT 'dim_product: unit_cost < unit_price and not null',
       count(*) FROM silver.silver_dim_product
       WHERE unit_cost >= unit_price OR unit_cost IS NULL OR unit_price IS NULL
UNION ALL
SELECT 'clickstream: campaign_id set ⇒ traffic_source = Campaign',
       count(*) FROM silver.silver_fact_clickstream
       WHERE campaign_id IS NOT NULL AND traffic_source != 'Campaign'
UNION ALL
SELECT 'clickstream: funnel order (no purchase without cart/view)',
       count(*) FROM silver.silver_fact_clickstream
       WHERE (product_page_visited_flag = false AND added_to_cart_flag = true  AND purchased_flag = true)
          OR (product_page_visited_flag = false AND added_to_cart_flag = false AND purchased_flag = true)
UNION ALL
SELECT 'gold_fact_transaction: transaction_total >= 0',
       count(*) FROM gold.gold_fact_transaction WHERE transaction_total < 0
UNION ALL
SELECT 'inventory: no negative closing stock',
       count(*) FROM silver.silver_fact_inventory WHERE closing_stock < 0;

-- COMMAND ----------

SELECT check_name,
       failing_rows,
       CASE WHEN failing_rows = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM dq_results
ORDER BY failing_rows DESC, check_name;

-- COMMAND ----------
-- MAGIC %md ### Guard — fails the job if any check has offending rows

-- COMMAND ----------

SELECT CASE
         WHEN sum(failing_rows) = 0
           THEN 'All data quality checks passed'
         ELSE raise_error(
                'Data quality FAILED for ' || count_if(failing_rows > 0) ||
                ' check(s); total offending rows = ' || sum(failing_rows))
       END AS result
FROM dq_results;
