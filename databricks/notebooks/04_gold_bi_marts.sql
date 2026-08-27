-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Gold BI marts — analytics-ready views for the dashboard
-- MAGIC
-- MAGIC Views (cheap, always fresh) implementing the business metric definitions.
-- MAGIC The AI/BI dashboard (`dashboards/elecmart_retail_analytics.lvdash.json`)
-- MAGIC reads only from these `bi_*` views, so chart logic never drifts from the metric spec.

-- COMMAND ----------

USE CATALOG elecmart;
USE SCHEMA gold;

-- COMMAND ----------

-- MAGIC %md ## 1 · Executive KPI summary (single row → counters)

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_kpi_summary AS
WITH s AS (
  SELECT
    sum(net_line_revenue)                                                          AS gross_revenue,
    sum(CASE WHEN transaction_status = 'Completed' THEN net_line_revenue END)       AS net_revenue,
    sum(CASE WHEN transaction_status = 'Completed' THEN line_cost END)              AS cogs,
    sum(CASE WHEN transaction_status = 'Returned'  THEN net_line_revenue END)       AS returned_revenue,
    count(DISTINCT CASE WHEN transaction_status = 'Completed' THEN transaction_id END) AS total_transactions,
    count(DISTINCT CASE WHEN transaction_status = 'Returned'  THEN transaction_id END) AS returned_transactions,
    count(DISTINCT transaction_id)                                                 AS total_orders,
    sum(CASE WHEN transaction_status = 'Completed' THEN quantity END)              AS units_sold
  FROM gold_fact_sale
)
SELECT
  round(net_revenue, 2)                                        AS net_revenue,
  round(gross_revenue, 2)                                      AS gross_revenue,
  round(net_revenue - cogs, 2)                                 AS gross_profit,
  round(100.0 * (net_revenue - cogs) / nullif(net_revenue, 0), 2) AS gross_margin_pct,
  total_transactions,
  returned_transactions,
  total_orders,
  units_sold,
  round(net_revenue / nullif(total_transactions, 0), 2)        AS aov,
  round(100.0 * returned_revenue / nullif(gross_revenue, 0), 2) AS return_rate_pct
FROM s;

-- COMMAND ----------

-- MAGIC %md ## 2 · Revenue & profit by month (time series)

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_revenue_by_month AS
SELECT
  CAST(date_trunc('MONTH', transaction_timestamp) AS DATE)                   AS month_start,
  round(sum(CASE WHEN transaction_status = 'Completed' THEN net_line_revenue END), 2) AS net_revenue,
  round(sum(CASE WHEN transaction_status = 'Completed' THEN net_line_revenue - line_cost END), 2) AS gross_profit,
  round(100.0
        * sum(CASE WHEN transaction_status = 'Completed' THEN net_line_revenue - line_cost END)
        / nullif(sum(CASE WHEN transaction_status = 'Completed' THEN net_line_revenue END), 0), 2) AS gross_margin_pct,
  count(DISTINCT CASE WHEN transaction_status = 'Completed' THEN transaction_id END) AS orders,
  sum(CASE WHEN transaction_status = 'Completed' THEN quantity END)          AS units_sold
FROM gold_fact_sale
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md ## 3 · Sales by category / channel / product

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_sales_by_category AS
SELECT p.category_name,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END), 2)          AS net_revenue,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue - s.line_cost END), 2) AS gross_profit,
       sum(CASE WHEN s.transaction_status = 'Completed' THEN s.quantity END)                            AS units_sold
FROM gold_fact_sale s
JOIN gold_dim_product p ON s.product_id = p.product_id
GROUP BY p.category_name
ORDER BY net_revenue DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_sales_by_channel AS
SELECT s.sales_channel,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END), 2) AS net_revenue,
       count(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END)  AS orders,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END)
             / nullif(count(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END), 0), 2) AS aov
FROM gold_fact_sale s
GROUP BY s.sales_channel
ORDER BY net_revenue DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_top_products AS
SELECT p.product_name, p.category_name, p.brand_name,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END), 2) AS net_revenue,
       sum(CASE WHEN s.transaction_status = 'Completed' THEN s.quantity END)                   AS units_sold
FROM gold_fact_sale s
JOIN gold_dim_product p ON s.product_id = p.product_id
GROUP BY p.product_name, p.category_name, p.brand_name
ORDER BY net_revenue DESC;

-- COMMAND ----------

-- MAGIC %md ## 4 · Store performance

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_sales_by_store AS
SELECT st.store_id, st.store_name, st.city, st.state_province, st.store_type,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END), 2)              AS net_revenue,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue - s.line_cost END), 2) AS gross_profit,
       round(100.0
             * sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue - s.line_cost END)
             / nullif(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END), 0), 2)  AS gross_margin_pct,
       count(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END)               AS orders,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END)
             / nullif(count(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END), 0), 2) AS aov
FROM gold_fact_sale s
JOIN gold_dim_store st ON s.store_id = st.store_id
GROUP BY st.store_id, st.store_name, st.city, st.state_province, st.store_type
ORDER BY net_revenue DESC;

-- COMMAND ----------

-- MAGIC %md ## 5 · Customer analytics

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_customer_summary AS
WITH tx AS (
  SELECT customer_id,
         count(DISTINCT transaction_id) AS orders,
         round(sum(transaction_total), 2) AS lifetime_revenue,
         min(transaction_timestamp) AS first_order_ts,
         max(transaction_timestamp) AS last_order_ts
  FROM gold_fact_transaction
  WHERE transaction_status = 'Completed' AND customer_id IS NOT NULL
  GROUP BY customer_id
)
SELECT c.customer_id, c.loyalty_status, c.income_bracket, c.age_group,
       c.customer_persona, c.signup_channel, c.country, c.state_province, c.city,
       tx.orders, tx.lifetime_revenue, tx.first_order_ts, tx.last_order_ts,
       date_diff(DAY, CAST(tx.last_order_ts AS DATE), current_date()) AS days_since_last_order,
       tx.orders > 1 AS is_repeat_customer
FROM gold_dim_customer c
JOIN tx ON c.customer_id = tx.customer_id;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_customer_loyalty_mix AS
SELECT loyalty_status,
       count(*)                              AS active_customers,
       sum(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) AS repeat_customers,
       round(100.0 * sum(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) / nullif(count(*), 0), 2) AS repeat_rate_pct,
       round(sum(lifetime_revenue), 2)       AS net_revenue,
       round(avg(lifetime_revenue), 2)       AS avg_revenue_per_customer,
       round(avg(orders), 2)                 AS avg_orders_per_customer
FROM bi_customer_summary
GROUP BY loyalty_status
ORDER BY net_revenue DESC;

-- COMMAND ----------

-- MAGIC %md ## 6 · Clickstream funnel

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_clickstream_funnel_by_month AS
SELECT
  CAST(date_trunc('MONTH', session_start_time) AS DATE)                         AS month_start,
  count(DISTINCT session_id)                                                    AS sessions,
  count(DISTINCT CASE WHEN product_page_visited_flag THEN session_id END)       AS product_view_sessions,
  count(DISTINCT CASE WHEN added_to_cart_flag THEN session_id END)              AS add_to_cart_sessions,
  count(DISTINCT CASE WHEN purchased_flag THEN session_id END)                  AS purchase_sessions,
  round(100.0 * count(DISTINCT CASE WHEN purchased_flag THEN session_id END)
        / nullif(count(DISTINCT session_id), 0), 2)                             AS conversion_rate_pct,
  round(100.0 * count(DISTINCT CASE WHEN added_to_cart_flag THEN session_id END)
        / nullif(count(DISTINCT CASE WHEN product_page_visited_flag THEN session_id END), 0), 2) AS add_to_cart_rate_pct,
  round(100.0 * count(DISTINCT CASE WHEN number_of_pages_viewed < 2 THEN session_id END)
        / nullif(count(DISTINCT session_id), 0), 2)                             AS bounce_rate_pct,
  round(100.0 * (count(DISTINCT CASE WHEN added_to_cart_flag THEN session_id END)
                 - count(DISTINCT CASE WHEN purchased_flag THEN session_id END))
        / nullif(count(DISTINCT CASE WHEN added_to_cart_flag THEN session_id END), 0), 2) AS cart_abandonment_pct
FROM gold_fact_clickstream
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_sessions_by_device AS
SELECT device_type,
       count(DISTINCT session_id) AS sessions,
       count(DISTINCT CASE WHEN purchased_flag THEN session_id END) AS purchase_sessions,
       round(100.0 * count(DISTINCT CASE WHEN purchased_flag THEN session_id END)
             / nullif(count(DISTINCT session_id), 0), 2) AS conversion_rate_pct
FROM gold_fact_clickstream
GROUP BY device_type
ORDER BY sessions DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_sessions_by_traffic_source AS
SELECT traffic_source,
       count(DISTINCT session_id) AS sessions,
       count(DISTINCT CASE WHEN purchased_flag THEN session_id END) AS purchase_sessions,
       round(100.0 * count(DISTINCT CASE WHEN purchased_flag THEN session_id END)
             / nullif(count(DISTINCT session_id), 0), 2) AS conversion_rate_pct
FROM gold_fact_clickstream
GROUP BY traffic_source
ORDER BY sessions DESC;

-- COMMAND ----------

-- MAGIC %md ## 7 · Campaign performance

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_campaign_performance AS
WITH cs AS (
  SELECT campaign_id,
         count(DISTINCT session_id) AS sessions,
         count(DISTINCT CASE WHEN purchased_flag THEN session_id END) AS purchase_sessions
  FROM gold_fact_clickstream
  WHERE campaign_id IS NOT NULL
  GROUP BY campaign_id
),
tx AS (
  SELECT campaign_id,
         count(DISTINCT transaction_id) AS attributed_orders,
         round(sum(CASE WHEN transaction_status = 'Completed' THEN transaction_total END), 2) AS attributed_revenue
  FROM gold_fact_transaction
  WHERE campaign_id IS NOT NULL
  GROUP BY campaign_id
)
SELECT c.campaign_id, c.campaign_name, c.campaign_channel,
       coalesce(cs.sessions, 0)            AS sessions,
       coalesce(tx.attributed_orders, 0)   AS attributed_orders,
       coalesce(tx.attributed_revenue, 0)  AS attributed_revenue,
       round(100.0 * cs.purchase_sessions / nullif(cs.sessions, 0), 2) AS conversion_rate_pct
FROM gold_dim_campaign c
LEFT JOIN cs ON c.campaign_id = cs.campaign_id
LEFT JOIN tx ON c.campaign_id = tx.campaign_id
ORDER BY attributed_revenue DESC;

-- COMMAND ----------

-- MAGIC %md ## 8 · Inventory health

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_inventory_by_category AS
SELECT p.category_name,
       round(sum(i.sold_units) / nullif(avg((i.starting_stock + i.closing_stock) / 2.0), 0), 2) AS inventory_turnover,
       sum(i.sold_units)      AS total_units_sold,
       sum(i.received_stock)  AS total_units_received,
       sum(i.shrinkage_loss)  AS total_shrinkage_loss,
       round(100.0 * avg(CASE WHEN i.backorder_flag THEN 1.0 ELSE 0.0 END), 2) AS backorder_rate_pct
FROM gold_fact_inventory i
JOIN gold_dim_product p ON i.product_id = p.product_id
GROUP BY p.category_name
ORDER BY inventory_turnover DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_inventory_by_month AS
SELECT snapshot_month,
       round(sum(sold_units) / nullif(avg((starting_stock + closing_stock) / 2.0), 0), 2) AS inventory_turnover,
       sum(sold_units)     AS sold_units,
       sum(received_stock) AS received_stock,
       sum(shrinkage_loss) AS shrinkage_loss,
       round(100.0 * avg(CASE WHEN backorder_flag THEN 1.0 ELSE 0.0 END), 2) AS backorder_rate_pct
FROM gold_fact_inventory
GROUP BY snapshot_month
ORDER BY snapshot_month;

-- COMMAND ----------

-- MAGIC %md ## 9 · Extra marts — daily grain, funnel stages, brand / region / payment / weekday / returns / promo / retention

-- COMMAND ----------

-- Daily grain — feeds fine-grained trends and a dashboard date-range filter.
CREATE OR REPLACE VIEW bi_daily_sales AS
SELECT
  CAST(transaction_timestamp AS DATE)                                                        AS sale_date,
  round(sum(CASE WHEN transaction_status = 'Completed' THEN net_line_revenue END), 2)        AS net_revenue,
  round(sum(CASE WHEN transaction_status = 'Completed' THEN net_line_profit END), 2)         AS gross_profit,
  count(DISTINCT CASE WHEN transaction_status = 'Completed' THEN transaction_id END)         AS orders,
  sum(CASE WHEN transaction_status = 'Completed' THEN quantity END)                          AS units_sold,
  round(sum(CASE WHEN transaction_status = 'Completed' THEN net_line_revenue END)
        / nullif(count(DISTINCT CASE WHEN transaction_status = 'Completed' THEN transaction_id END), 0), 2) AS aov
FROM gold_fact_sale
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- Whole-period funnel as 4 stage rows → clean funnel / bar chart.
CREATE OR REPLACE VIEW bi_funnel_overall AS
WITH s AS (
  SELECT
    count(DISTINCT session_id)                                              AS c_sessions,
    count(DISTINCT CASE WHEN product_page_visited_flag THEN session_id END) AS c_views,
    count(DISTINCT CASE WHEN added_to_cart_flag THEN session_id END)        AS c_cart,
    count(DISTINCT CASE WHEN purchased_flag THEN session_id END)            AS c_purch
  FROM gold_fact_clickstream
)
            SELECT 1 AS step, 'Sessions'      AS stage, c_sessions AS sessions, CAST(100.00 AS DECIMAL(5,2)) AS pct_of_sessions FROM s
UNION ALL SELECT 2, 'Product views', c_views, round(100.0 * c_views  / nullif(c_sessions, 0), 2) FROM s
UNION ALL SELECT 3, 'Add to cart',   c_cart,  round(100.0 * c_cart   / nullif(c_sessions, 0), 2) FROM s
UNION ALL SELECT 4, 'Purchases',     c_purch, round(100.0 * c_purch  / nullif(c_sessions, 0), 2) FROM s
ORDER BY step;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_sales_by_brand AS
SELECT p.brand_name,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END), 2) AS net_revenue,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_profit END), 2)  AS gross_profit,
       sum(CASE WHEN s.transaction_status = 'Completed' THEN s.quantity END)                   AS units_sold
FROM gold_fact_sale s
JOIN gold_dim_product p ON s.product_id = p.product_id
GROUP BY p.brand_name
ORDER BY net_revenue DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_sales_by_subcategory AS
SELECT p.category_name, p.subcategory_name,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END), 2) AS net_revenue,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_profit END), 2)  AS gross_profit,
       sum(CASE WHEN s.transaction_status = 'Completed' THEN s.quantity END)                   AS units_sold
FROM gold_fact_sale s
JOIN gold_dim_product p ON s.product_id = p.product_id
GROUP BY p.category_name, p.subcategory_name
ORDER BY net_revenue DESC;

-- COMMAND ----------

-- Region via the selling store's location.
CREATE OR REPLACE VIEW bi_sales_by_region AS
SELECT st.country, st.state_province, st.city,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END), 2) AS net_revenue,
       count(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END)  AS orders
FROM gold_fact_sale s
JOIN gold_dim_store st ON s.store_id = st.store_id
GROUP BY st.country, st.state_province, st.city
ORDER BY net_revenue DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_sales_by_payment_type AS
SELECT payment_type,
       count(DISTINCT CASE WHEN transaction_status = 'Completed' THEN transaction_id END)  AS orders,
       round(sum(CASE WHEN transaction_status = 'Completed' THEN transaction_total END), 2) AS net_revenue,
       round(sum(CASE WHEN transaction_status = 'Completed' THEN transaction_total END)
             / nullif(count(DISTINCT CASE WHEN transaction_status = 'Completed' THEN transaction_id END), 0), 2) AS aov
FROM gold_fact_transaction
GROUP BY payment_type
ORDER BY net_revenue DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_sales_by_weekday AS
SELECT d.day_of_week, d.day_name,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END), 2) AS net_revenue,
       count(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END)  AS orders,
       round(sum(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue END)
             / nullif(count(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END), 0), 2) AS aov
FROM gold_fact_sale s
JOIN gold_dim_date d ON s.transaction_date_id = d.date_id
GROUP BY d.day_of_week, d.day_name
ORDER BY d.day_of_week;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_returns_by_category AS
SELECT p.category_name,
       round(sum(CASE WHEN s.transaction_status = 'Returned' THEN s.net_line_revenue END), 2)  AS returned_revenue,
       count(DISTINCT CASE WHEN s.transaction_status = 'Returned' THEN s.transaction_id END)   AS returned_orders,
       round(100.0 * sum(CASE WHEN s.transaction_status = 'Returned' THEN s.net_line_revenue END)
             / nullif(sum(s.net_line_revenue), 0), 2)                                          AS return_rate_pct
FROM gold_fact_sale s
JOIN gold_dim_product p ON s.product_id = p.product_id
GROUP BY p.category_name
ORDER BY returned_revenue DESC;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_promotion_performance AS
SELECT pr.promo_id, pr.promo_name, pr.promo_type, pr.discount_type,
       count(DISTINCT CASE WHEN t.transaction_status = 'Completed' THEN t.transaction_id END)         AS promo_orders,
       round(sum(CASE WHEN t.transaction_status = 'Completed' THEN t.transaction_total END), 2)       AS promo_revenue,
       round(sum(CASE WHEN t.transaction_status = 'Completed' THEN t.transaction_discount_applied END), 2) AS discount_given,
       round(sum(CASE WHEN t.transaction_status = 'Completed' THEN t.transaction_total END)
             / nullif(count(DISTINCT CASE WHEN t.transaction_status = 'Completed' THEN t.transaction_id END), 0), 2) AS avg_revenue_per_promo_txn
FROM gold_dim_promotion pr
JOIN gold_fact_transaction t ON t.promo_id = pr.promo_id
GROUP BY pr.promo_id, pr.promo_name, pr.promo_type, pr.discount_type
ORDER BY promo_revenue DESC;

-- COMMAND ----------

-- New vs returning: a customer is "new" in the month of their first completed order.
CREATE OR REPLACE VIEW bi_customer_new_vs_returning_by_month AS
WITH orders AS (
  SELECT customer_id, transaction_total,
         CAST(date_trunc('MONTH', transaction_timestamp) AS DATE) AS month_start
  FROM gold_fact_transaction
  WHERE transaction_status = 'Completed' AND customer_id IS NOT NULL
),
first_month AS (
  SELECT customer_id, min(month_start) AS acq_month FROM orders GROUP BY customer_id
)
SELECT o.month_start,
       count(DISTINCT CASE WHEN o.month_start = f.acq_month THEN o.customer_id END) AS new_customers,
       count(DISTINCT CASE WHEN o.month_start > f.acq_month THEN o.customer_id END) AS returning_customers,
       round(sum(CASE WHEN o.month_start = f.acq_month THEN o.transaction_total END), 2) AS new_customer_revenue,
       round(sum(CASE WHEN o.month_start > f.acq_month THEN o.transaction_total END), 2) AS returning_customer_revenue
FROM orders o
JOIN first_month f ON o.customer_id = f.customer_id
GROUP BY o.month_start
ORDER BY o.month_start;

-- COMMAND ----------

CREATE OR REPLACE VIEW bi_inventory_by_store AS
SELECT st.store_name, st.city, st.state_province,
       round(sum(i.sold_units) / nullif(avg((i.starting_stock + i.closing_stock) / 2.0), 0), 2) AS inventory_turnover,
       sum(i.sold_units)     AS total_units_sold,
       sum(i.shrinkage_loss) AS total_shrinkage_loss,
       round(100.0 * avg(CASE WHEN i.backorder_flag THEN 1.0 ELSE 0.0 END), 2) AS backorder_rate_pct
FROM gold_fact_inventory i
JOIN gold_dim_store st ON i.store_id = st.store_id
GROUP BY st.store_name, st.city, st.state_province
ORDER BY inventory_turnover DESC;

-- COMMAND ----------

-- MAGIC %md ## Smoke test — every view returns rows

-- COMMAND ----------

SELECT 'bi_kpi_summary' v, count(*) n FROM bi_kpi_summary
UNION ALL SELECT 'bi_revenue_by_month',            count(*) FROM bi_revenue_by_month
UNION ALL SELECT 'bi_sales_by_category',           count(*) FROM bi_sales_by_category
UNION ALL SELECT 'bi_sales_by_channel',            count(*) FROM bi_sales_by_channel
UNION ALL SELECT 'bi_top_products',                count(*) FROM bi_top_products
UNION ALL SELECT 'bi_sales_by_store',              count(*) FROM bi_sales_by_store
UNION ALL SELECT 'bi_customer_summary',            count(*) FROM bi_customer_summary
UNION ALL SELECT 'bi_customer_loyalty_mix',        count(*) FROM bi_customer_loyalty_mix
UNION ALL SELECT 'bi_clickstream_funnel_by_month', count(*) FROM bi_clickstream_funnel_by_month
UNION ALL SELECT 'bi_sessions_by_device',          count(*) FROM bi_sessions_by_device
UNION ALL SELECT 'bi_sessions_by_traffic_source',  count(*) FROM bi_sessions_by_traffic_source
UNION ALL SELECT 'bi_campaign_performance',        count(*) FROM bi_campaign_performance
UNION ALL SELECT 'bi_inventory_by_category',       count(*) FROM bi_inventory_by_category
UNION ALL SELECT 'bi_inventory_by_month',          count(*) FROM bi_inventory_by_month
UNION ALL SELECT 'bi_daily_sales',                 count(*) FROM bi_daily_sales
UNION ALL SELECT 'bi_funnel_overall',              count(*) FROM bi_funnel_overall
UNION ALL SELECT 'bi_sales_by_brand',              count(*) FROM bi_sales_by_brand
UNION ALL SELECT 'bi_sales_by_subcategory',        count(*) FROM bi_sales_by_subcategory
UNION ALL SELECT 'bi_sales_by_region',             count(*) FROM bi_sales_by_region
UNION ALL SELECT 'bi_sales_by_payment_type',       count(*) FROM bi_sales_by_payment_type
UNION ALL SELECT 'bi_sales_by_weekday',            count(*) FROM bi_sales_by_weekday
UNION ALL SELECT 'bi_returns_by_category',         count(*) FROM bi_returns_by_category
UNION ALL SELECT 'bi_promotion_performance',       count(*) FROM bi_promotion_performance
UNION ALL SELECT 'bi_customer_new_vs_returning_by_month', count(*) FROM bi_customer_new_vs_returning_by_month
UNION ALL SELECT 'bi_inventory_by_store',          count(*) FROM bi_inventory_by_store;