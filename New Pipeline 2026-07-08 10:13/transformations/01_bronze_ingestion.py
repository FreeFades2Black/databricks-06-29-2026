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
# Create a dummy dataset of retail transactions
data = [
    (1, "Laptop", "Electronics", 1200, "NY"),
    (2, "Phone", "Electronics", 800, "CA"),
    (3, "Desk Chair", "Furniture", 250, "NY"),
    (4, "Monitor", "Electronics", 400, "TX"),
    (5, "Lamp", "Furniture", 50, "CA")
]
Markdown
# FILE: ~./pyspark_cheatsheet.md

# THE GUNSLINGER'S RECKONING: PYSPARK DATAFRAME BASICS
# Secure the perimeter and map the terrain before the showdown.

## 1. Wrangling the Data (Ingestion)
# Load the raw materials into the chamber.
df = spark.read.csv("dbfs:/path/to/data.csv", header=True, inferSchema=True)
df = spark.read.table("catalog.schema.table")

## 2. Scouting the Terrain (Inspection)
# Know your target before you pull the trigger.
df.show()               # Prints the rows to the console
df.printSchema()        # Reveals the data types and structure
df.count()              # Counts the total bounty (rows)

## 3. Painting the Target (Transformations)
# Refine your aim. Transformations are lazy; they only plan the path.
filtered_df = df.filter(df.category == "Electronics")
selected_df = df.select("product", "price")
grouped_df  = df.groupBy("state").avg("price")

## 4. Pulling the Trigger (Actions)
# Execute the plan across the frontier. This kicks off the real compute.
df.show()
df.write.mode("overwrite").saveAsTable("catalog.schema.target_table")
