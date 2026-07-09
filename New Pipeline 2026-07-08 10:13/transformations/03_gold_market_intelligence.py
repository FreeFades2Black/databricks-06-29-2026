
# File path: /Users/whall4.wh@gmail.com/New Pipeline 2026-07-08 10:13/transformations/03_gold_market_intelligence.py
# Compiled by: Free Hall
# Purpose: Extract true financial metrics from the structured SEC JSON facts payload.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, get_json_object, expr

@dp.materialized_view(
    name="gold_market_intelligence",
    comment="Parsed financial metrics extracting true values from SEC JSON structures."
)
def gold_market_intelligence():
    # Read from silver layer (batch read from materialized view)
    silver_df = spark.read.table("silver_apex_market_intelligence")
    
    # Extract the actual financial metrics from the nested JSON facts structure
    # Using the LAST array entry (index -1 notation via slice) for most recent data
    parsed_df = (silver_df
        .withColumn(
            "revenues_billion",
            # Extract latest Revenues value from us-gaap.Revenues.units.USD array, convert to billions
            expr("cast(get_json_object(raw_json_payload, '$.facts.us-gaap.Revenues.units.USD[0].val') as double) / 1000000000")
        )
        .withColumn(
            "net_income_million",
            # Extract latest NetIncomeLoss value, convert to millions
            expr("cast(get_json_object(raw_json_payload, '$.facts.us-gaap.NetIncomeLoss.units.USD[0].val') as double) / 1000000")
        )
        .withColumn(
            "assets_billion",
            # Extract latest Assets value, convert to billions
            expr("cast(get_json_object(raw_json_payload, '$.facts.us-gaap.Assets.units.USD[0].val') as double) / 1000000000")
        )
    )
    
    return parsed_df.select(
        col("cik"),
        col("ticker"),
        col("company_name"),
        col("revenues_billion"),
        col("net_income_million"),
        col("assets_billion"),
        col("ingestion_time")
    )
