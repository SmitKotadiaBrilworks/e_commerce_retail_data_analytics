from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ============================================================================
# Gold Layer — Fact Tables
# Enriches silver facts with joins and computed financial columns
# Published to elecmart.gold schema
# ============================================================================


@dp.materialized_view(name="elecmart.gold.gold_fact_transaction", comment="Gold fact: transaction (direct copy from silver)")
@dp.expect_or_fail("valid_transaction_id", "transaction_id IS NOT NULL")
def gold_fact_transaction():
    return spark.read.table("silver_fact_transaction")


@dp.materialized_view(name="elecmart.gold.gold_fact_inventory", comment="Gold fact: inventory (direct copy from silver)")
@dp.expect_or_fail("valid_inventory_id", "inventory_id IS NOT NULL")
def gold_fact_inventory():
    return spark.read.table("silver_fact_inventory")


@dp.materialized_view(name="elecmart.gold.gold_fact_sale", comment="Gold fact: sale enriched with transaction details, allocated discount, net revenue and profit")
@dp.expect_or_fail("valid_sale_id", "sale_id IS NOT NULL")
@dp.expect("valid_quantity", "quantity > 0")
@dp.expect("valid_line_total", "line_total >= 0")
def gold_fact_sale():
    sale = spark.read.table("silver_fact_sale").alias("s")
    txn = spark.read.table("silver_fact_transaction").alias("t")

    return (
        sale.join(txn, F.col("s.transaction_id") == F.col("t.transaction_id"), "left")
        .select(
            F.col("s.sale_id"),
            F.col("s.transaction_id"),
            F.col("s.session_id"),
            F.col("s.transaction_timestamp"),
            F.col("s.transaction_date_id"),
            F.col("s.product_id"),
            F.col("s.quantity"),
            F.col("s.unit_cost"),
            F.col("s.unit_price"),
            F.col("s.line_cost"),
            F.col("s.line_total"),
            F.col("t.store_id"),
            F.col("t.customer_id"),
            F.col("t.sales_channel"),
            F.col("t.campaign_id"),
            F.col("t.promo_id"),
            F.col("t.transaction_status"),
            F.col("t.transaction_subtotal"),
            F.col("t.transaction_discount_applied"),
        )
        .withColumn(
            "allocated_line_discount",
            F.when(
                F.col("transaction_subtotal").isNull() | (F.col("transaction_subtotal") == 0),
                F.lit(None).cast("decimal(24,2)"),
            ).otherwise(
                (F.col("line_total") * (F.col("transaction_discount_applied") / F.col("transaction_subtotal"))).cast("decimal(24,2)"),
            ),
        )
        .withColumn(
            "net_line_revenue",
            (F.col("line_total") - F.coalesce(F.col("allocated_line_discount"), F.lit(0))).cast("decimal(26,2)"),
        )
        .withColumn(
            "net_line_profit",
            (F.col("net_line_revenue") - F.col("line_cost")).cast("decimal(27,2)"),
        )
        .drop("transaction_subtotal", "transaction_discount_applied")
    )


@dp.materialized_view(name="elecmart.gold.gold_fact_clickstream", comment="Gold fact: clickstream (direct view from silver)")
@dp.expect_or_fail("valid_session_id", "session_id IS NOT NULL")
def gold_fact_clickstream():
    return spark.read.table("silver_fact_clickstream")
