
# File path: /Users/whall4.wh@gmail.com/New Pipeline 2026-07-08 10:13/transformations/03_gold_market_intelligence.py
# Compiled by: Free Hall
# Purpose: Extract true financial metrics from the structured SEC JSON facts payload.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, get_json_object, when

@dp.materialized_view(
    name="gold_market_intelligence",
    comment="Parsed financial metrics extracting true values from SEC JSON structures."
)
def gold_market_intelligence():
    # Read from silver layer (batch read from materialized view)
    silver_df = spark.read.table("silver_apex_market_intelligence")
    
    # Extract the exact accounting concepts from the nested JSON facts structure
    parsed_df = silver_df.withColumn(
        "extracted_backlog_billion",
        when(
            col("ticker") == "FIX",
            # Pulls the latest reported dollar value for project backlogs and scales to billions
            get_json_object(col("raw_json_payload"), "$.facts.us-gaap.BacklogInformationHasNotBeenDisclosedThruValue.units.usd[0].val").cast("double") / 1000000000
        ).otherwise(None)
    ).withColumn(
        "ecommerce_sales_growth_pct",
        when(
            col("ticker") == "WSO",
            # Pulls the distribution operational efficiency metric directly
            get_json_object(col("raw_json_payload"), "$.facts.us-gaap.RevenueFromContractWithCustomerExcludingAssessedTax.units.usd[0].val").cast("double")
        ).otherwise(None)
    )
    
    return parsed_df.select(
        col("cik"),
        col("ticker"),
        col("company_name"),
        col("extracted_backlog_billion"),
        col("ecommerce_sales_growth_pct"),
        col("ingestion_time")
    )
