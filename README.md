

Project Overview

The Platform is an end-to-end data warehouse and analytics solution designed to ingest, store, analyze, and explore heterogeneous financial market data from multiple vendors. The system adheres to strict temporal database paradigms, utilizes big data processing engines for analytics, and features an integrated Large Language Model (LLM) assistant capable of real-time data querying.


### Tech Stack:
Python (3.9+) 
Java: Required under the hood to run the Apache Spark engine.
MongoDB: NoSQL database

FastAPI
Uvicorn
Apache Spark 

Streamlit: used for the interactive web dashboard
Pandas
Plotly

Google Gemini: LLM-powered assistant
mcp

Yahoo Finance 
CoinGecko
### 


### Installation
* Prerequisites
 Python 3.9+
 Java (Required for Apache Spark / PySpark)
 MongoDB Atlas Account (Free Tier)
 Google AI Studio Account (Free Tier API Key)

Clone the repository type the following commands:

python3 -m venv venv

source venv/bin/activate  

pip install fastapi uvicorn pymongo requests python-dotenv certifi yfinance streamlit pandas plotly pyspark google-generativeai

# Treminal 1:
uvicorn api:app --reload

# Terminal2
python mcp_server.py

# Terminal 3:
streamlit run dashboard.py

### Manual Pipeline Commands (optional)
python3 ingest.py

python3 analytics.py