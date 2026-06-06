from mcp.server.fastmcp import FastMCP
import requests

mcp = FastMCP("AcmeFinancialMCP")

API_URL = "http://127.0.0.1:8000"

@mcp.tool()
def list_database_assets() -> list:
    """Returns a list of all available financial asset symbols currently stored in the database."""
    res = requests.get(f"{API_URL}/assets")
    return res.json().get("assets", []) if res.status_code == 200 else []

@mcp.tool()
def get_asset_metadata(symbol: str) -> dict:
    """Returns metadata (description, region, class) for a specific financial asset symbol."""
    res = requests.get(f"{API_URL}/assets/{symbol.upper()}")
    return res.json() if res.status_code == 200 else {"error": f"Asset {symbol} not found"}

@mcp.tool()
def get_spark_analytics() -> list:
    """Returns the pre-calculated Apache Spark analytics including volatility and min/max/avg prices."""
    res = requests.get(f"{API_URL}/analytics/summary")
    return res.json().get("metrics", []) if res.status_code == 200 else []

@mcp.tool()
def get_spark_forecasts() -> list:
    """Returns the Apache Spark predictive analytics and next-day price forecasts."""
    res = requests.get(f"{API_URL}/analytics/forecasts")
    return res.json().get("forecasts", []) if res.status_code == 200 else []

if __name__ == "__main__":
    mcp.run(transport='sse')