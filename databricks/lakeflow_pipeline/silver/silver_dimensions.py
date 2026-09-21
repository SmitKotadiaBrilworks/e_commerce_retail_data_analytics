from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ============================================================================
# Silver Layer — Dimension Tables
# Cleans bronze data: drops _rescued_data, derives new columns, enforces quality
# ============================================================================


@dp.materialized_view(name="silver_dim_brand", comment="Silver dimension: brand (cleaned from bronze)")
@dp.expect_or_fail("valid_brand_id", "brand_id IS NOT NULL")
def silver_dim_brand():
    return spark.read.table("elecmart.bronze.dim_brand")


@dp.materialized_view(name="silver_dim_category", comment="Silver dimension: category (cleaned from bronze)")
@dp.expect_or_fail("valid_category_id", "category_id IS NOT NULL")
def silver_dim_category():
    return spark.read.table("elecmart.bronze.dim_category").drop("_rescued_data")


@dp.materialized_view(name="silver_dim_subcategory", comment="Silver dimension: subcategory (cleaned from bronze)")
@dp.expect_or_fail("valid_subcategory_id", "subcategory_id IS NOT NULL")
def silver_dim_subcategory():
    return spark.read.table("elecmart.bronze.dim_subcategory").drop("_rescued_data")


@dp.materialized_view(name="silver_dim_product", comment="Silver dimension: product with segment (cleaned from bronze)")
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL")
@dp.expect("valid_unit_cost", "unit_cost >= 0")
@dp.expect("valid_unit_price", "unit_price >= 0")
def silver_dim_product():
    return spark.read.table("elecmart.bronze.dim_product").drop("_rescued_data")


@dp.materialized_view(name="silver_dim_customer", comment="Silver dimension: customer with derived age, age_group, income_bracket")
@dp.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_email", "email_address IS NOT NULL AND email_address LIKE '%@%'")
def silver_dim_customer():
    return (
        spark.read.table("elecmart.bronze.dim_customer")
        .drop("_rescued_data", "birth_date")
        .withColumn("age", (2025 - F.col("birth_year")).cast("bigint"))
        .withColumn(
            "age_group",
            F.when(F.col("age") < 18, "Under 18")
            .when(F.col("age").between(18, 24), "18-24")
            .when(F.col("age").between(25, 34), "25-34")
            .when(F.col("age").between(35, 44), "35-44")
            .when(F.col("age").between(45, 54), "45-54")
            .when(F.col("age").between(55, 64), "55-64")
            .otherwise("65+"),
        )
        .withColumn(
            "income_bracket",
            F.when(F.col("estimated_annual_income") < 30000, "Low")
            .when(F.col("estimated_annual_income") <= 75000, "Medium")
            .when(F.col("estimated_annual_income") <= 150000, "High")
            .otherwise("Premium"),
        )
        .drop("birth_year", "estimated_annual_income")
    )


@dp.materialized_view(name="silver_dim_location", comment="Silver dimension: location (key geographic fields only)")
@dp.expect_or_fail("valid_location_id", "location_id IS NOT NULL")
def silver_dim_location():
    return spark.read.table("elecmart.bronze.dim_location").select(
        "location_id", "country", "state_province", "city", "location_type"
    )


@dp.materialized_view(name="silver_dim_store", comment="Silver dimension: store with opening_date cast to DATE")
@dp.expect_or_fail("valid_store_id", "store_id IS NOT NULL")
def silver_dim_store():
    return (
        spark.read.table("elecmart.bronze.dim_store")
        .drop("_rescued_data")
        .withColumn("opening_date", F.col("opening_date").cast("date"))
    )


@dp.materialized_view(name="silver_dim_campaign", comment="Silver dimension: campaign (cleaned from bronze)")
@dp.expect_or_fail("valid_campaign_id", "campaign_id IS NOT NULL")
def silver_dim_campaign():
    return spark.read.table("elecmart.bronze.dim_campaign").drop("_rescued_data")


@dp.materialized_view(name="silver_dim_promotion", comment="Silver dimension: promotion with computed is_active flag")
@dp.expect_or_fail("valid_promo_id", "promo_id IS NOT NULL")
def silver_dim_promotion():
    return (
        spark.read.table("elecmart.bronze.dim_promotion")
        .drop("_rescued_data", "is_active")
        .withColumn(
            "is_active",
            F.current_date().between(
                F.col("promo_start_date").cast("date"),
                F.col("promo_end_date").cast("date"),
            ),
        )
    )


@dp.materialized_view(name="silver_dim_date", comment="Silver dimension: date (preserves _rescued_data for gold cleanup)")
@dp.expect_or_fail("valid_date_id", "date_id IS NOT NULL")
def silver_dim_date():
    return spark.read.table("elecmart.bronze.dim_date")
