import os
import requests
import certifi
import yfinance as yf
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv


load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri, tlsCAFile=certifi.where())
db = client["acme_financial"]
collection = db["market_data"]


try:
    client.admin.command('ping')
    print("Successfully connected to MongoDB Atlas!")
except Exception as e:
    print(f"Connection failed: {e}")
    exit()

def fetch_yahoo_history(symbol, asset_class):
    """
    Fetches 1 month of historical data and metadata from Yahoo Finance.
    """
    print(f"Fetching {asset_class} data for {symbol} from Yahoo Finance...")
    ticker = yf.Ticker(symbol)
    
    
    info = ticker.info
    full_desc = info.get("longBusinessSummary", "")
    short_desc = full_desc.split(".")[0] + "." if full_desc else f"{asset_class.capitalize()} instrument tracked via Yahoo Finance."
    
    
    hist = ticker.history(period="1mo")
    
    if hist.empty:
        print(f"  -> No historical data found for {symbol}.")
        return

    records_to_insert = []
    
    
    for date, row in hist.iterrows():
        temporal_record = {
            "symbol": symbol.upper(),
            "asset_class": asset_class,
            "provider": "YahooFinance",
            "description": short_desc,
            "region": info.get("country", "Global"),
            "name": info.get("shortName", symbol),
            
            "valid_date": date.strftime('%Y-%m-%d'), 
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "is_deleted": False,
            "attributes": {
                "price": float(row["Close"]),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "volume": int(row["Volume"])
            }
        }
        records_to_insert.append(temporal_record)
        
    
    if records_to_insert:
        collection.insert_many(records_to_insert)
        print(f"  -> Successfully ingested {len(records_to_insert)} days of history for {symbol}.")


def fetch_coingecko_history(coin_id):
    """
    Fetches 30 days of historical data and metadata from CoinGecko.
    """
    print(f"Fetching cryptocurrency data for {coin_id} from CoinGecko...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    
    meta_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false&sparkline=false"
    meta_response = requests.get(meta_url, headers=headers)
    
    if meta_response.status_code != 200:
        print(f"  -> Failed to fetch metadata for {coin_id}.")
        return
        
    meta_data = meta_response.json()
    symbol = meta_data.get("symbol", "").upper()
    name = meta_data.get("name", coin_id)
    full_desc = meta_data.get("description", {}).get("en", "")
    short_desc = full_desc.split(".")[0] + "." if full_desc else "Cryptocurrency."

    
    hist_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days=30&interval=daily"
    hist_response = requests.get(hist_url, headers=headers)
    
    if hist_response.status_code != 200:
        print(f"  -> Failed to fetch history for {coin_id}.")
        return
        
    hist_data = hist_response.json()
    prices = hist_data.get("prices", [])
    market_caps = hist_data.get("market_caps", [])
    total_volumes = hist_data.get("total_volumes", [])
    
    records_to_insert = []
    
    for i in range(len(prices) - 1):
        date_str = datetime.fromtimestamp(prices[i][0] / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d')
        
        temporal_record = {
            "symbol": symbol,
            "asset_class": "cryptocurrency",
            "provider": "CoinGecko",
            "description": short_desc,
            "region": "Global",
            "name": name,
            "valid_date": date_str,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "is_deleted": False,
            "attributes": {
                "price": float(prices[i][1]),
                "market_cap": float(market_caps[i][1]),
                "volume": float(total_volumes[i][1])
            }
        }
        records_to_insert.append(temporal_record)
        
    if records_to_insert:
        collection.insert_many(records_to_insert)
        print(f"  -> Successfully ingested {len(records_to_insert)} days of history for {coin_id}.")


if __name__ == "__main__":
    print("--- Starting Acme Financial Data Ingestion ---")
    
    stocks = ["AAPL", "MSFT", "GOOGL", "AMZN"]
    for s in stocks:
        fetch_yahoo_history(s, "stock")
        
    bonds = ["TLT", "AGG", "^TNX", "^IRX"] 
    for b in bonds:
        fetch_yahoo_history(b, "bond")
        
    indexes = ["^GSPC", "^DJI", "^IXIC", "^RUT"] 
    for i in indexes:
        fetch_yahoo_history(i, "index")
        
    metals = ["GC=F", "SI=F", "HG=F", "PA=F"] 
    for m in metals:
        fetch_yahoo_history(m, "metal")
        
    cryptos = ["bitcoin", "ethereum", "solana", "ripple"]
    for c in cryptos:
        fetch_coingecko_history(c)

    print("--- Ingestion Complete! ---")