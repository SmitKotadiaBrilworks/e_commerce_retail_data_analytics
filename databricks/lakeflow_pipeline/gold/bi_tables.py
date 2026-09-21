from pyspark import pipelines as dp


# ---------------------------------------------------------------------------
# Sales BI tables
# ---------------------------------------------------------------------------

@dp.materialized_view(name="elecmart.gold.bi_kpi_summary")
def bi_kpi_summary():
    return spark.sql("""
        WITH sales_kpi AS (
            SELECT
                SUM(CASE WHEN transaction_status = 'Completed' THEN net_line_revenue ELSE 0 END) AS net_revenue,
                SUM(line_total) AS gross_revenue,
                SUM(CASE WHEN transaction_status = 'Completed' THEN net_line_profit ELSE 0 END) AS gross_profit,
                SUM(CASE WHEN transaction_status = 'Completed' THEN quantity ELSE 0 END) AS units_sold
            FROM elecmart.gold.gold_fact_sale
        ),
        txn_kpi AS (
            SELECT
                COUNT(CASE WHEN transaction_status = 'Completed' THEN 1 END) AS total_transactions,
                COUNT(CASE WHEN transaction_status = 'Returned' THEN 1 END) AS returned_transactions,
                COUNT(*) AS total_orders
            FROM elecmart.gold.gold_fact_transaction
        )
        SELECT
            s.net_revenue, s.gross_revenue, s.gross_profit,
            CASE WHEN s.net_revenue > 0 THEN s.gross_profit / s.net_revenue * 100 ELSE 0 END AS gross_margin_pct,
            t.total_transactions, t.returned_transactions, t.total_orders,
            s.units_sold,
            CASE WHEN t.total_transactions > 0 THEN s.net_revenue / t.total_transactions ELSE 0 END AS aov,
            CASE WHEN t.total_orders > 0 THEN t.returned_transactions * 100.0 / t.total_orders ELSE 0 END AS return_rate_pct
        FROM sales_kpi s CROSS JOIN txn_kpi t
    """)


@dp.materialized_view(name="elecmart.gold.bi_daily_sales")
def bi_daily_sales():
    return spark.sql("""
        SELECT
            d.date AS sale_date,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_profit ELSE 0 END) AS gross_profit,
            COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END) AS orders,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.quantity ELSE 0 END) AS units_sold,
            CASE WHEN COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END) > 0
                THEN SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) /
                     COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END)
                ELSE 0 END AS aov
        FROM elecmart.gold.gold_fact_sale s
        LEFT JOIN elecmart.gold.gold_dim_date d ON s.transaction_date_id = d.date_id
        GROUP BY d.date
    """)


@dp.materialized_view(name="elecmart.gold.bi_revenue_by_month")
def bi_revenue_by_month():
    return spark.sql("""
        SELECT
            TRUNC(d.date, 'MM') AS month_start,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_profit ELSE 0 END) AS gross_profit,
            CASE WHEN SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) > 0
                THEN SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_profit ELSE 0 END) /
                     SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) * 100
                ELSE 0 END AS gross_margin_pct,
            COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END) AS orders,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.quantity ELSE 0 END) AS units_sold
        FROM elecmart.gold.gold_fact_sale s
        LEFT JOIN elecmart.gold.gold_dim_date d ON s.transaction_date_id = d.date_id
        GROUP BY TRUNC(d.date, 'MM')
    """)


@dp.materialized_view(name="elecmart.gold.bi_sales_by_store")
def bi_sales_by_store():
    return spark.sql("""
        SELECT st.store_id, st.store_name, st.city, st.state_province, st.store_type,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_profit ELSE 0 END) AS gross_profit,
            CASE WHEN SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) > 0
                THEN SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_profit ELSE 0 END) /
                     SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) * 100
                ELSE 0 END AS gross_margin_pct,
            COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END) AS orders,
            CASE WHEN COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END) > 0
                THEN SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) /
                     COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END)
                ELSE 0 END AS aov
        FROM elecmart.gold.gold_fact_sale s
        INNER JOIN elecmart.gold.gold_dim_store st ON s.store_id = st.store_id
        GROUP BY st.store_id, st.store_name, st.city, st.state_province, st.store_type
    """)


@dp.materialized_view(name="elecmart.gold.bi_sales_by_brand")
def bi_sales_by_brand():
    return spark.sql("""
        SELECT p.brand_name,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_profit ELSE 0 END) AS gross_profit,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.quantity ELSE 0 END) AS units_sold
        FROM elecmart.gold.gold_fact_sale s
        LEFT JOIN elecmart.gold.gold_dim_product p ON s.product_id = p.product_id
        GROUP BY p.brand_name
    """)


@dp.materialized_view(name="elecmart.gold.bi_sales_by_category")
def bi_sales_by_category():
    return spark.sql("""
        SELECT p.category_name,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_profit ELSE 0 END) AS gross_profit,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.quantity ELSE 0 END) AS units_sold
        FROM elecmart.gold.gold_fact_sale s
        LEFT JOIN elecmart.gold.gold_dim_product p ON s.product_id = p.product_id
        GROUP BY p.category_name
    """)


@dp.materialized_view(name="elecmart.gold.bi_sales_by_subcategory")
def bi_sales_by_subcategory():
    return spark.sql("""
        SELECT p.category_name, p.subcategory_name,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_profit ELSE 0 END) AS gross_profit,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.quantity ELSE 0 END) AS units_sold
        FROM elecmart.gold.gold_fact_sale s
        LEFT JOIN elecmart.gold.gold_dim_product p ON s.product_id = p.product_id
        GROUP BY p.category_name, p.subcategory_name
    """)


@dp.materialized_view(name="elecmart.gold.bi_sales_by_channel")
def bi_sales_by_channel():
    return spark.sql("""
        SELECT s.sales_channel,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END) AS orders,
            CASE WHEN COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END) > 0
                THEN SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) /
                     COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END)
                ELSE 0 END AS aov
        FROM elecmart.gold.gold_fact_sale s
        GROUP BY s.sales_channel
    """)


@dp.materialized_view(name="elecmart.gold.bi_sales_by_payment_type")
def bi_sales_by_payment_type():
    return spark.sql("""
        SELECT t.payment_type,
            COUNT(DISTINCT CASE WHEN t.transaction_status = 'Completed' THEN t.transaction_id END) AS orders,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            CASE WHEN COUNT(DISTINCT CASE WHEN t.transaction_status = 'Completed' THEN t.transaction_id END) > 0
                THEN SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) /
                     COUNT(DISTINCT CASE WHEN t.transaction_status = 'Completed' THEN t.transaction_id END)
                ELSE 0 END AS aov
        FROM elecmart.gold.gold_fact_transaction t
        INNER JOIN elecmart.gold.gold_fact_sale s ON t.transaction_id = s.transaction_id
        GROUP BY t.payment_type
    """)


@dp.materialized_view(name="elecmart.gold.bi_sales_by_region")
def bi_sales_by_region():
    return spark.sql("""
        SELECT st.country, st.state_province, st.city,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END) AS orders
        FROM elecmart.gold.gold_fact_sale s
        INNER JOIN elecmart.gold.gold_dim_store st ON s.store_id = st.store_id
        GROUP BY st.country, st.state_province, st.city
    """)


@dp.materialized_view(name="elecmart.gold.bi_sales_by_weekday")
def bi_sales_by_weekday():
    return spark.sql("""
        SELECT d.day_of_week, d.day_name,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END) AS orders,
            CASE WHEN COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END) > 0
                THEN SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) /
                     COUNT(DISTINCT CASE WHEN s.transaction_status = 'Completed' THEN s.transaction_id END)
                ELSE 0 END AS aov
        FROM elecmart.gold.gold_fact_sale s
        LEFT JOIN elecmart.gold.gold_dim_date d ON s.transaction_date_id = d.date_id
        GROUP BY d.day_of_week, d.day_name
    """)


@dp.materialized_view(name="elecmart.gold.bi_top_products")
def bi_top_products():
    return spark.sql("""
        SELECT p.product_name, p.category_name, p.brand_name,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS net_revenue,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.quantity ELSE 0 END) AS units_sold
        FROM elecmart.gold.gold_fact_sale s
        LEFT JOIN elecmart.gold.gold_dim_product p ON s.product_id = p.product_id
        GROUP BY p.product_name, p.category_name, p.brand_name
    """)


# ---------------------------------------------------------------------------
# Customer BI tables
# ---------------------------------------------------------------------------

@dp.materialized_view(name="elecmart.gold.bi_customer_summary")
def bi_customer_summary():
    return spark.sql("""
        SELECT c.customer_id, c.loyalty_status, c.income_bracket, c.age_group,
            c.customer_persona, c.signup_channel, c.country, c.state_province, c.city,
            COUNT(DISTINCT t.transaction_id) AS orders,
            COALESCE(SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END), 0) AS lifetime_revenue,
            MIN(t.transaction_timestamp) AS first_order_ts,
            MAX(t.transaction_timestamp) AS last_order_ts,
            DATEDIFF(CURRENT_DATE(), MAX(t.transaction_timestamp)) AS days_since_last_order,
            CASE WHEN COUNT(DISTINCT t.transaction_id) > 1 THEN true ELSE false END AS is_repeat_customer
        FROM elecmart.gold.gold_dim_customer c
        INNER JOIN elecmart.gold.gold_fact_transaction t ON c.customer_id = t.customer_id
        LEFT JOIN elecmart.gold.gold_fact_sale s ON t.transaction_id = s.transaction_id
        GROUP BY c.customer_id, c.loyalty_status, c.income_bracket, c.age_group,
                 c.customer_persona, c.signup_channel, c.country, c.state_province, c.city
    """)


@dp.materialized_view(name="elecmart.gold.bi_customer_loyalty_mix")
def bi_customer_loyalty_mix():
    return spark.sql("""
        WITH customer_orders AS (
            SELECT c.loyalty_status, c.customer_id,
                COUNT(DISTINCT t.transaction_id) AS orders,
                SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS revenue
            FROM elecmart.gold.gold_dim_customer c
            INNER JOIN elecmart.gold.gold_fact_transaction t ON c.customer_id = t.customer_id
            LEFT JOIN elecmart.gold.gold_fact_sale s ON t.transaction_id = s.transaction_id
            GROUP BY c.loyalty_status, c.customer_id
        )
        SELECT loyalty_status,
            COUNT(*) AS active_customers,
            SUM(CASE WHEN orders > 1 THEN 1 ELSE 0 END) AS repeat_customers,
            CASE WHEN COUNT(*) > 0 THEN SUM(CASE WHEN orders > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) ELSE 0 END AS repeat_rate_pct,
            SUM(revenue) AS net_revenue,
            CASE WHEN COUNT(*) > 0 THEN SUM(revenue) / COUNT(*) ELSE 0 END AS avg_revenue_per_customer,
            CASE WHEN COUNT(*) > 0 THEN SUM(orders) * 1.0 / COUNT(*) ELSE 0 END AS avg_orders_per_customer
        FROM customer_orders
        GROUP BY loyalty_status
    """)


@dp.materialized_view(name="elecmart.gold.bi_customer_new_vs_returning_by_month")
def bi_customer_new_vs_returning_by_month():
    return spark.sql("""
        WITH first_orders AS (
            SELECT customer_id, TRUNC(MIN(transaction_timestamp), 'MM') AS first_order_month
            FROM elecmart.gold.gold_fact_transaction
            GROUP BY customer_id
        ),
        monthly_txn AS (
            SELECT TRUNC(t.transaction_timestamp, 'MM') AS month_start, t.customer_id, t.transaction_id
            FROM elecmart.gold.gold_fact_transaction t
        )
        SELECT m.month_start,
            COUNT(DISTINCT CASE WHEN f.first_order_month = m.month_start THEN m.customer_id END) AS new_customers,
            COUNT(DISTINCT CASE WHEN f.first_order_month < m.month_start THEN m.customer_id END) AS returning_customers,
            SUM(CASE WHEN f.first_order_month = m.month_start AND s.transaction_status = 'Completed'
                THEN s.net_line_revenue ELSE 0 END) AS new_customer_revenue,
            SUM(CASE WHEN f.first_order_month < m.month_start AND s.transaction_status = 'Completed'
                THEN s.net_line_revenue ELSE 0 END) AS returning_customer_revenue
        FROM monthly_txn m
        JOIN first_orders f ON m.customer_id = f.customer_id
        LEFT JOIN elecmart.gold.gold_fact_sale s ON m.transaction_id = s.transaction_id
        GROUP BY m.month_start
    """)


# ---------------------------------------------------------------------------
# Campaign & Promotion BI tables
# ---------------------------------------------------------------------------

@dp.materialized_view(name="elecmart.gold.bi_campaign_performance")
def bi_campaign_performance():
    return spark.sql("""
        WITH campaign_sessions AS (
            SELECT campaign_id, COUNT(DISTINCT session_id) AS sessions
            FROM elecmart.gold.gold_fact_clickstream
            WHERE campaign_id IS NOT NULL
            GROUP BY campaign_id
        ),
        campaign_sales AS (
            SELECT campaign_id,
                COUNT(DISTINCT CASE WHEN transaction_status = 'Completed' THEN transaction_id END) AS attributed_orders,
                SUM(CASE WHEN transaction_status = 'Completed' THEN net_line_revenue ELSE 0 END) AS attributed_revenue
            FROM elecmart.gold.gold_fact_sale
            WHERE campaign_id IS NOT NULL
            GROUP BY campaign_id
        )
        SELECT c.campaign_id, c.campaign_name, c.campaign_channel,
            COALESCE(cs.sessions, 0) AS sessions,
            COALESCE(sa.attributed_orders, 0) AS attributed_orders,
            COALESCE(sa.attributed_revenue, 0) AS attributed_revenue,
            CASE WHEN COALESCE(cs.sessions, 0) > 0
                THEN COALESCE(sa.attributed_orders, 0) * 100.0 / cs.sessions
                ELSE 0 END AS conversion_rate_pct
        FROM elecmart.gold.gold_dim_campaign c
        LEFT JOIN campaign_sessions cs ON c.campaign_id = cs.campaign_id
        LEFT JOIN campaign_sales sa ON c.campaign_id = sa.campaign_id
    """)


@dp.materialized_view(name="elecmart.gold.bi_promotion_performance")
def bi_promotion_performance():
    return spark.sql("""
        SELECT p.promo_id, p.promo_name, p.promo_type, p.discount_type,
            COUNT(DISTINCT t.transaction_id) AS promo_orders,
            SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) AS promo_revenue,
            SUM(t.transaction_discount_applied) AS discount_given,
            CASE WHEN COUNT(DISTINCT t.transaction_id) > 0
                THEN SUM(CASE WHEN s.transaction_status = 'Completed' THEN s.net_line_revenue ELSE 0 END) /
                     COUNT(DISTINCT t.transaction_id)
                ELSE 0 END AS avg_revenue_per_promo_txn
        FROM elecmart.gold.gold_dim_promotion p
        INNER JOIN elecmart.gold.gold_fact_transaction t ON p.promo_id = t.promo_id
        LEFT JOIN elecmart.gold.gold_fact_sale s ON t.transaction_id = s.transaction_id
        GROUP BY p.promo_id, p.promo_name, p.promo_type, p.discount_type
    """)


@dp.materialized_view(name="elecmart.gold.bi_returns_by_category")
def bi_returns_by_category():
    return spark.sql("""
        SELECT p.category_name,
            SUM(CASE WHEN s.transaction_status = 'Returned' THEN s.net_line_revenue ELSE 0 END) AS returned_revenue,
            COUNT(DISTINCT CASE WHEN s.transaction_status = 'Returned' THEN s.transaction_id END) AS returned_orders,
            CASE WHEN COUNT(DISTINCT s.transaction_id) > 0
                THEN COUNT(DISTINCT CASE WHEN s.transaction_status = 'Returned' THEN s.transaction_id END) * 100.0 /
                     COUNT(DISTINCT s.transaction_id)
                ELSE 0 END AS return_rate_pct
        FROM elecmart.gold.gold_fact_sale s
        LEFT JOIN elecmart.gold.gold_dim_product p ON s.product_id = p.product_id
        GROUP BY p.category_name
    """)


# ---------------------------------------------------------------------------
# Clickstream BI tables
# ---------------------------------------------------------------------------

@dp.materialized_view(name="elecmart.gold.bi_clickstream_funnel_by_month")
def bi_clickstream_funnel_by_month():
    return spark.sql("""
        SELECT TRUNC(cs.session_start_time, 'MM') AS month_start,
            COUNT(DISTINCT cs.session_id) AS sessions,
            COUNT(DISTINCT CASE WHEN cs.product_page_visited_flag THEN cs.session_id END) AS product_view_sessions,
            COUNT(DISTINCT CASE WHEN cs.added_to_cart_flag THEN cs.session_id END) AS add_to_cart_sessions,
            COUNT(DISTINCT CASE WHEN cs.purchased_flag THEN cs.session_id END) AS purchase_sessions,
            CASE WHEN COUNT(DISTINCT cs.session_id) > 0
                THEN COUNT(DISTINCT CASE WHEN cs.purchased_flag THEN cs.session_id END) * 100.0 / COUNT(DISTINCT cs.session_id)
                ELSE 0 END AS conversion_rate_pct,
            CASE WHEN COUNT(DISTINCT cs.session_id) > 0
                THEN COUNT(DISTINCT CASE WHEN cs.added_to_cart_flag THEN cs.session_id END) * 100.0 / COUNT(DISTINCT cs.session_id)
                ELSE 0 END AS add_to_cart_rate_pct,
            CASE WHEN COUNT(DISTINCT cs.session_id) > 0
                THEN (COUNT(DISTINCT cs.session_id) - COUNT(DISTINCT CASE WHEN cs.product_page_visited_flag THEN cs.session_id END)) * 100.0 / COUNT(DISTINCT cs.session_id)
                ELSE 0 END AS bounce_rate_pct,
            CASE WHEN COUNT(DISTINCT CASE WHEN cs.added_to_cart_flag THEN cs.session_id END) > 0
                THEN (COUNT(DISTINCT CASE WHEN cs.added_to_cart_flag THEN cs.session_id END) - COUNT(DISTINCT CASE WHEN cs.purchased_flag THEN cs.session_id END)) * 100.0 /
                     COUNT(DISTINCT CASE WHEN cs.added_to_cart_flag THEN cs.session_id END)
                ELSE 0 END AS cart_abandonment_pct
        FROM elecmart.gold.gold_fact_clickstream cs
        GROUP BY TRUNC(cs.session_start_time, 'MM')
    """)


@dp.materialized_view(name="elecmart.gold.bi_funnel_overall")
def bi_funnel_overall():
    return spark.sql("""
        WITH funnel AS (
            SELECT
                COUNT(DISTINCT session_id) AS total_sessions,
                COUNT(DISTINCT CASE WHEN product_page_visited_flag THEN session_id END) AS product_view_sessions,
                COUNT(DISTINCT CASE WHEN added_to_cart_flag THEN session_id END) AS add_to_cart_sessions,
                COUNT(DISTINCT CASE WHEN purchased_flag THEN session_id END) AS purchase_sessions
            FROM elecmart.gold.gold_fact_clickstream
        )
        SELECT 1 AS step, 'Session' AS stage, total_sessions AS sessions,
            CASE WHEN total_sessions > 0 THEN total_sessions * 100.0 / total_sessions ELSE 0 END AS pct_of_sessions
        FROM funnel
        UNION ALL
        SELECT 2, 'Product View', product_view_sessions,
            CASE WHEN total_sessions > 0 THEN product_view_sessions * 100.0 / total_sessions ELSE 0 END
        FROM funnel
        UNION ALL
        SELECT 3, 'Add to Cart', add_to_cart_sessions,
            CASE WHEN total_sessions > 0 THEN add_to_cart_sessions * 100.0 / total_sessions ELSE 0 END
        FROM funnel
        UNION ALL
        SELECT 4, 'Purchase', purchase_sessions,
            CASE WHEN total_sessions > 0 THEN purchase_sessions * 100.0 / total_sessions ELSE 0 END
        FROM funnel
    """)


@dp.materialized_view(name="elecmart.gold.bi_sessions_by_device")
def bi_sessions_by_device():
    return spark.sql("""
        SELECT device_type,
            COUNT(DISTINCT session_id) AS sessions,
            COUNT(DISTINCT CASE WHEN purchased_flag THEN session_id END) AS purchase_sessions,
            CASE WHEN COUNT(DISTINCT session_id) > 0
                THEN COUNT(DISTINCT CASE WHEN purchased_flag THEN session_id END) * 100.0 / COUNT(DISTINCT session_id)
                ELSE 0 END AS conversion_rate_pct
        FROM elecmart.gold.gold_fact_clickstream
        GROUP BY device_type
    """)


@dp.materialized_view(name="elecmart.gold.bi_sessions_by_traffic_source")
def bi_sessions_by_traffic_source():
    return spark.sql("""
        SELECT traffic_source,
            COUNT(DISTINCT session_id) AS sessions,
            COUNT(DISTINCT CASE WHEN purchased_flag THEN session_id END) AS purchase_sessions,
            CASE WHEN COUNT(DISTINCT session_id) > 0
                THEN COUNT(DISTINCT CASE WHEN purchased_flag THEN session_id END) * 100.0 / COUNT(DISTINCT session_id)
                ELSE 0 END AS conversion_rate_pct
        FROM elecmart.gold.gold_fact_clickstream
        GROUP BY traffic_source
    """)


# ---------------------------------------------------------------------------
# Inventory BI tables
# ---------------------------------------------------------------------------

@dp.materialized_view(name="elecmart.gold.bi_inventory_by_category")
def bi_inventory_by_category():
    return spark.sql("""
        SELECT p.category_name,
            CASE WHEN AVG(i.closing_stock) > 0 THEN SUM(i.sold_units) / AVG(i.closing_stock) ELSE 0 END AS inventory_turnover,
            SUM(i.sold_units) AS total_units_sold,
            SUM(i.received_stock) AS total_units_received,
            SUM(i.shrinkage_loss) AS total_shrinkage_loss,
            CASE WHEN COUNT(*) > 0 THEN SUM(CASE WHEN i.backorder_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*) ELSE 0 END AS backorder_rate_pct
        FROM elecmart.gold.gold_fact_inventory i
        LEFT JOIN elecmart.gold.gold_dim_product p ON i.product_id = p.product_id
        GROUP BY p.category_name
    """)


@dp.materialized_view(name="elecmart.gold.bi_inventory_by_month")
def bi_inventory_by_month():
    return spark.sql("""
        SELECT i.snapshot_month,
            CASE WHEN AVG(i.closing_stock) > 0 THEN SUM(i.sold_units) / AVG(i.closing_stock) ELSE 0 END AS inventory_turnover,
            SUM(i.sold_units) AS sold_units,
            SUM(i.received_stock) AS received_stock,
            SUM(i.shrinkage_loss) AS shrinkage_loss,
            CASE WHEN COUNT(*) > 0 THEN SUM(CASE WHEN i.backorder_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*) ELSE 0 END AS backorder_rate_pct
        FROM elecmart.gold.gold_fact_inventory i
        GROUP BY i.snapshot_month
    """)


@dp.materialized_view(name="elecmart.gold.bi_inventory_by_store")
def bi_inventory_by_store():
    return spark.sql("""
        SELECT st.store_name, st.city, st.state_province,
            CASE WHEN AVG(i.closing_stock) > 0 THEN SUM(i.sold_units) / AVG(i.closing_stock) ELSE 0 END AS inventory_turnover,
            SUM(i.sold_units) AS total_units_sold,
            SUM(i.shrinkage_loss) AS total_shrinkage_loss,
            CASE WHEN COUNT(*) > 0 THEN SUM(CASE WHEN i.backorder_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*) ELSE 0 END AS backorder_rate_pct
        FROM elecmart.gold.gold_fact_inventory i
        INNER JOIN elecmart.gold.gold_dim_store st ON i.store_id = st.store_id
        GROUP BY st.store_name, st.city, st.state_province
    """)
