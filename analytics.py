import os
import certifi
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min, avg as spark_avg, round, lag
from pyspark.sql.window import Window

print("Initializing Apache Spark...")
spark = SparkSession.builder \
    .appName("AcmeFinancialAnalytics") \
    .master("local[*]") \
    .getOrCreate()

print("Extracting data from Data Warehouse...")
load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"), tlsCAFile=certifi.where())
db = client["acme_financial"]
collection = db["market_data"]

raw_data = list(collection.find({"is_deleted": False}, {"_id": 0}))

if not raw_data:
    print("No data found in the database. Run ingest.py first!")
    exit()

flattened_data = []
for doc in raw_data:
    flat_doc = {
        "symbol": doc.get("symbol"),
        "asset_class": doc.get("asset_class"),
        "valid_date": doc.get("valid_date"),
        "price": float(doc.get("attributes", {}).get("price", 0))
    }
    if flat_doc["valid_date"]: 
        flattened_data.append(flat_doc)

print("Loading data into Spark DataFrame...")
df = spark.createDataFrame(flattened_data)
df = df.orderBy("symbol", "valid_date")

print("\nRunning Aggregations and Volatility...")
aggregations = df.groupBy("symbol", "asset_class").agg(
    round(spark_min("price"), 2).alias("min_price"),
    round(spark_max("price"), 2).alias("max_price"),
    round(spark_avg("price"), 2).alias("avg_price")
)

volatility_df = aggregations.withColumn(
    "volatility_pct", 
    round(((col("max_price") - col("min_price")) / col("min_price")) * 100, 2)
)

print("Running Forecasting...")
windowSpec = Window.partitionBy("symbol").orderBy("valid_date")
forecast_df = df.withColumn("prev_1", lag("price", 1).over(windowSpec)) \
                .withColumn("prev_2", lag("price", 2).over(windowSpec)) \
                
forecast_df = forecast_df.withColumn(
    "next_day_forecast", 
    round((col("price") + col("prev_1") + col("prev_2")) / 3, 2)
)

latest_dates = forecast_df.groupBy("symbol").agg(spark_max("valid_date").alias("valid_date"))
final_forecast = forecast_df.join(latest_dates, ["symbol", "valid_date"], "inner")

print("Writing compiled insights to MongoDB...")

summary_records = [row.asDict() for row in volatility_df.collect()]
forecast_records = [row.asDict() for row in final_forecast.select("symbol", "valid_date", "price", "next_day_forecast").collect()]

db["analytics_summary"].drop()
db["forecast_summary"].drop()

if summary_records:
    db["analytics_summary"].insert_many(summary_records)
if forecast_records:
    db["forecast_summary"].insert_many(forecast_records)

print("\n" + "="*50)
print("SPARK INSIGHTS SUCCESSFULLY DEPLOYED TO MONGO!")
print("="*50)

spark.stop()