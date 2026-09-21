from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ============================================================================
# Silver Layer — Fact Tables
# Cleans bronze fact data: drops _rescued_data, removes redundant columns,
# derives snapshot_month_id, enforces data quality
# ============================================================================


@dp.materialized_view(name="silver_fact_sale", comment="Silver fact: sales (drops _rescued_data and aov_category)")
@dp.expect_or_fail("valid_sale_id", "sale_id IS NOT NULL")
@dp.expect("valid_quantity", "quantity > 0")
@dp.expect("valid_line_total", "line_total >= 0")
def silver_fact_sale():
    return spark.read.table("elecmart.bronze.fact_sale").drop("_rescued_data", "aov_category")


@dp.materialized_view(name="silver_fact_transaction", comment="Silver fact: transactions (drops _rescued_data)")
@dp.expect_or_fail("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("valid_transaction_total", "transaction_total >= 0")
def silver_fact_transaction():
    return spark.read.table("elecmart.bronze.fact_transaction").drop("_rescued_data")


@dp.materialized_view(name="silver_fact_clickstream", comment="Silver fact: clickstream (drops _rescued_data and aov_category)")
@dp.expect_or_fail("valid_session_id", "session_id IS NOT NULL")
def silver_fact_clickstream():
    return spark.read.table("elecmart.bronze.fact_clickstream").drop("_rescued_data", "aov_category")


@dp.materialized_view(name="silver_fact_inventory", comment="Silver fact: inventory with derived snapshot_month_id")
@dp.expect_or_fail("valid_inventory_id", "inventory_id IS NOT NULL")
@dp.expect("valid_closing_stock", "closing_stock >= 0")
def silver_fact_inventory():
    return (
        spark.read.table("elecmart.bronze.inventory")
        .drop("_rescued_data")
        .withColumn(
            "snapshot_month_id",
            (F.year("snapshot_month") * 100 + F.month("snapshot_month")).cast("int"),
        )
    )
