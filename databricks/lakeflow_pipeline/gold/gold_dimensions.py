from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ============================================================================
# Gold Layer — Dimension Tables
# Enriches silver dimensions with joins and computed columns
# Published to elecmart.gold schema
# ============================================================================


@dp.materialized_view(name="elecmart.gold.gold_dim_date", comment="Gold dimension: date with month_id and year_quarter")
@dp.expect_or_fail("valid_date_id", "date_id IS NOT NULL")
def gold_dim_date():
    return (
        spark.read.table("silver_dim_date")
        .drop("_rescued_data")
        .withColumn("month_id", (F.col("year") * 100 + F.col("month")).cast("int"))
        .withColumn("year_quarter", F.concat(F.col("year"), F.lit("Q"), F.col("quarter")))
    )


@dp.materialized_view(name="elecmart.gold.gold_dim_product", comment="Gold dimension: product enriched with brand, category, subcategory names")
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL")
def gold_dim_product():
    product = spark.read.table("silver_dim_product")
    brand = spark.read.table("silver_dim_brand")
    category = spark.read.table("silver_dim_category")
    subcategory = spark.read.table("silver_dim_subcategory").drop("category_id")

    return (
        product
        .join(brand, "brand_id", "left")
        .join(category, "category_id", "left")
        .join(subcategory, "subcategory_id", "left")
        .select(
            "product_id", "product_name", "brand_id", "brand_name",
            "category_id", "category_name", "subcategory_id", "subcategory_name",
            "unit_cost", "unit_price", "warranty_years",
        )
    )


@dp.materialized_view(name="elecmart.gold.gold_dim_customer", comment="Gold dimension: customer enriched with location details")
@dp.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
def gold_dim_customer():
    customer = spark.read.table("silver_dim_customer")
    location = spark.read.table("silver_dim_location")

    return (
        customer
        .join(location, "location_id", "left")
        .select(
            "customer_id", "email_address", "first_name", "last_name",
            "gender", "customer_persona", "age_group",
            "country", "state_province", "city", "location_type",
            "signup_date", "signup_date_id", "signup_channel",
            "loyalty_status", "income_bracket", "email_opt_in", "sms_opt_in",
        )
    )


@dp.materialized_view(name="elecmart.gold.gold_dim_store", comment="Gold dimension: store enriched with location details")
@dp.expect_or_fail("valid_store_id", "store_id IS NOT NULL")
def gold_dim_store():
    store = spark.read.table("silver_dim_store")
    location = spark.read.table("silver_dim_location")

    return (
        store
        .join(location, "location_id", "left")
        .select(
            "store_id", "store_name", "store_type", "store_size",
            "opening_date", "opening_date_id", "foot_traffic_index",
            "country", "state_province", "city", "location_type",
        )
    )


@dp.materialized_view(name="elecmart.gold.gold_dim_campaign", comment="Gold dimension: campaign (direct copy from silver)")
@dp.expect_or_fail("valid_campaign_id", "campaign_id IS NOT NULL")
def gold_dim_campaign():
    return spark.read.table("silver_dim_campaign")


@dp.materialized_view(name="elecmart.gold.gold_dim_promotion", comment="Gold dimension: promotion (direct copy from silver)")
@dp.expect_or_fail("valid_promo_id", "promo_id IS NOT NULL")
def gold_dim_promotion():
    return spark.read.table("silver_dim_promotion")
