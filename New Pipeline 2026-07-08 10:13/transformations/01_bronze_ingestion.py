# File: transformations/01_bronze_ingestion.py
# Bronze Layer: SEC Data Ingestion

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit, udf
from pyspark.sql.types import StringType
import urllib.request
import json

# --- The Function: Fetching SEC Company Facts ---
def fetch_sec_company_facts(cik: str) -> str:
    """Retrieves live financial disclosures from the SEC using 10-digit zero-padding."""
    try:
        padded_cik = f"CIK{str(cik).zfill(10)}"
        url = f"https://data.sec.gov/api/xbrl/companyfacts/{padded_cik}.json"
        
        req = urllib.request.Request(
            url, 
            headers={'User-Agent': 'Scott Steiert ssteiert@apexservicepartners.com'}
        )
        
        with urllib.request.urlopen(req) as response:
            return response.read().decode('utf-8')
    except Exception as e:
        return json.dumps({"error": str(e), "attempted_url": url if 'url' in locals() else 'None'})

fetch_sec_udf = udf(fetch_sec_company_facts, StringType())

# --- Bronze Layer: Market Intelligence ---
@dp.materialized_view(
    name="bronze_apex_market_intelligence",
    comment="Live SEC scraper ingestion for Apex intelligence."
)
def bronze_apex_market_intelligence():
    target_competitors = spark.createDataFrame([
        ("0001035983", "FIX", "Comfort Systems USA"),
        ("0000105016", "WSO", "Watsco Inc")
    ], ["cik", "ticker", "company_name"])
    
    return (
        target_competitors
        .withColumn("raw_json_payload", fetch_sec_udf(col("cik")))
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("analyst_assigned", lit("Scott Steiert"))
    )
