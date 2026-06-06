import os
import certifi
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from dotenv import load_dotenv
import subprocess

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client["acme_financial"]
collection = db["market_data"]

app = FastAPI(title="Acme Financial Market Data API")


@app.get("/")
def read_root():
    return {"message": "Welcome to the Acme Financial API"}

@app.get("/assets")
def get_all_assets():
    symbols = collection.distinct("symbol")
    return {"assets": symbols}

@app.get("/assets/{asset_id}")
def get_asset_details(asset_id: str):
    pipeline = [
        {"$match": {"symbol": asset_id.upper(), "is_deleted": False}},
        {"$sort": {"timestamp": -1}}, 
        {"$limit": 1},
        {"$project": {"_id": 0, "attributes": 0}} 
    ]
    
    results = list(collection.aggregate(pipeline))
    
    if not results:
        raise HTTPException(status_code=404, detail="Asset not found or is marked as deleted.")
    
    return results[0]

@app.get("/sources")
def get_all_sources():
    sources = collection.distinct("provider")
    return {"sources": sources}

@app.get("/sources/{source_id}")
def get_source_details(source_id: str):
    pipeline = [
        {"$match": {"provider": {"$regex": f"^{source_id}$", "$options": "i"}}},
        {"$group": {
            "_id": "$provider", 
            "assets_provided": {"$addToSet": "$symbol"}, 
            "total_records": {"$sum": 1}
        }}
    ]
    results = list(collection.aggregate(pipeline))
    
    if not results:
        raise HTTPException(status_code=404, detail="Source not found.")
        
    return results[0]

@app.get("/timeseries/{source_id}/{asset_id}")
def get_timeseries(source_id: str, asset_id: str):
    pipeline = [
        {"$match": {
            "provider": {"$regex": f"^{source_id}$", "$options": "i"}, 
            "symbol": asset_id.upper(),
            "is_deleted": False
        }},
        {"$sort": {"timestamp": 1}}, 
        {"$project": {"_id": 0}} 
    ]
    
    results = list(collection.aggregate(pipeline))
    
    if not results:
        raise HTTPException(status_code=404, detail="No time-series data found for this combination.")
    
    return {"source": source_id, "asset": asset_id, "data_points": results}

@app.delete("/assets/{asset_id}")
def delete_asset(asset_id: str):
    existing = collection.find_one({"symbol": asset_id.upper(), "is_deleted": False})
    if not existing:
        raise HTTPException(status_code=404, detail="Asset not found or already deleted.")
    
    deletion_marker = {
        "symbol": asset_id.upper(),
        "provider": "System",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_deleted": True,
        "note": "Asset marked as unavailable from this timestamp forward."
    }
    
    collection.insert_one(deletion_marker)
    
    return {"message": f"Asset {asset_id.upper()} successfully marked as deleted."}


@app.get("/analytics/summary")
def get_analytics_summary():
    results = list(db["analytics_summary"].find({}, {"_id": 0}))
    return {"metrics": results}

@app.get("/analytics/forecasts")
def get_forecasts():
    results = list(db["forecast_summary"].find({}, {"_id": 0}))
    return {"forecasts": results}



@app.post("/admin/run-ingest")
def run_ingestion():
    try:
        result = subprocess.run(["python3", "ingest.py"], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(result.stderr)
        return {"message": "Ingestion completed successfully", "log": result.stdout}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion script failed: {str(e)}")

@app.post("/admin/run-analytics")
def run_analytics_script():
    try:
        result = subprocess.run(["python3", "analytics.py"], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(result.stderr)
        return {"message": "Spark Analytics completed successfully", "log": result.stdout}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analytics script failed: {str(e)}")