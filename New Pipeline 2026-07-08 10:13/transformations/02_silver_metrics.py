# File: transformations/02_silver_financial_metrics.py
# The Gunslinger's Creed: Separate the iron from the dross. 

from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.materialized_view(
    name="silver_apex_market_intelligence",
    comment="Cleansed, flattened, and validated SEC metrics from the Bronze layer."
)
def silver_apex_market_intelligence():
    return spark.read.table("bronze_apex_market_intelligence").select(
        F.col("cik"),
        F.col("ticker"),
        F.col("company_name"),
        F.col("raw_json_payload"),
        F.col("ingestion_time")
    )
