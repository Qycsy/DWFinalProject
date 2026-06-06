import os
import sys
import asyncio
import nest_asyncio
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import google.generativeai as genai
from dotenv import load_dotenv

from mcp import ClientSession
from mcp.client.sse import sse_client


nest_asyncio.apply()

load_dotenv()

API_URL = "http://127.0.0.1:8001"

st.set_page_config(page_title="Acme Analytics Platform", layout="wide")
st.title(" Acme Financial Market Data Platform")


try:
    assets_response = requests.get(f"{API_URL}/assets")
    assets_response.raise_for_status()
    assets_list = assets_response.json().get("assets", [])
except requests.exceptions.RequestException:
    st.error(" Cannot connect to the API. Make sure your FastAPI server is running!")
    st.stop()

with st.sidebar:
    st.header(" Navigation")
    current_view = st.radio(
        "Choose a module:",
        [" Asset Explorer", "⚡ Apache Spark Insights", "🤖 AI Assistant"]
    )
    
    st.markdown("---")
    
    selected_asset = None
    if current_view == " Asset Explorer":
        st.header(" Explorer Controls")
        selected_asset = st.selectbox("Select an Asset to Explore", sorted(assets_list))
        st.markdown("---")

    st.header(" Admin Controls")
    

    if st.button(" 1. Fetch Latest Market Data", use_container_width=True):
        with st.spinner("Downloading from APIs... (This takes a minute)"):
            res = requests.post(f"{API_URL}/admin/run-ingest")
            if res.status_code == 200:
                st.success(" Ingestion complete!")
            else:
                st.error(" Ingestion failed. Check terminal for errors.")
                
    if st.button(" 2. Recalculate Spark Analytics", use_container_width=True):
        with st.spinner("Spinning up Apache Spark... (Please wait)"):
            res = requests.post(f"{API_URL}/admin/run-analytics")
            if res.status_code == 200:
                st.success("Spark Insights updated!")
                st.rerun() 
            else:
                st.error(" Analytics failed. Check terminal for errors.")
        
    st.markdown("---")
    if st.button(" Clear AI Chat History", use_container_width=True):
        st.session_state.messages = []
        st.rerun()


if current_view == " Asset Explorer":
    if selected_asset:
        details_res = requests.get(f"{API_URL}/assets/{selected_asset}")
        if details_res.status_code == 200:
            details = details_res.json()
            provider = details.get("provider")
            
            st.subheader(f"{details.get('name', selected_asset)} ({selected_asset})")
            cols = st.columns(3)
            cols[0].write(f"**Class:** {details.get('asset_class', 'Unknown').capitalize()}")
            cols[1].write(f"**Region:** {details.get('region', 'Global')}")
            cols[2].write(f"**Provider:** {provider}")
            st.write(f"**Description:** {details.get('description', 'No description available.')}")
            st.markdown("---")
            
            ts_res = requests.get(f"{API_URL}/timeseries/{provider}/{selected_asset}")
            if ts_res.status_code == 200:
                ts_data = ts_res.json().get("data_points", [])
                if ts_data:
                    df = pd.json_normalize(ts_data)
                    df['valid_date'] = pd.to_datetime(df['valid_date'])
                    df = df.sort_values('valid_date')
                    
                    latest_price = df.iloc[-1]['attributes.price']
                    st.metric(label="Current Price (Latest Record)", value=f"${latest_price:,.2f}")
                    
                    fig = px.line(df, x='valid_date', y='attributes.price', 
                                  title=f"{selected_asset} - 1 Month Price History")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No time-series data found.")


elif current_view == "⚡ Apache Spark Insights":
    st.header("⚡ Big Data Engine Reports (PySpark)")
    st.write("These metrics are computed via Apache Spark over historical market logs.")
    
    summary_res = requests.get(f"{API_URL}/analytics/summary")
    forecast_res = requests.get(f"{API_URL}/analytics/forecasts")
    
    if summary_res.status_code == 200 and forecast_res.status_code == 200:
        summary_data = summary_res.json().get("metrics", [])
        forecast_data = forecast_res.json().get("forecasts", [])
        
        if summary_data:
            df_metrics = pd.DataFrame(summary_data)
            df_forecast = pd.DataFrame(forecast_data)
            
            st.subheader(" Performance & Volatility Table")
            st.dataframe(df_metrics.rename(columns={
                "symbol": "Symbol", "asset_class": "Asset Class", 
                "min_price": "Min Price ($)", "max_price": "Max Price ($)", 
                "avg_price": "Avg Price ($)", "volatility_pct": "Volatility (%)"
            }), use_container_width=True)
            
            st.markdown("---")
            st.subheader(" Top Market Movers (Highest Historical Volatility)")
            fig_vol = px.bar(df_metrics.sort_values("volatility_pct", ascending=False), 
                             x="symbol", y="volatility_pct", color="asset_class",
                             labels={"volatility_pct": "Volatility Percentage (%)", "symbol": "Symbol"},
                             title="Asset Volatility Comparison")
            st.plotly_chart(fig_vol, use_container_width=True)
            
            st.markdown("---")
            st.subheader(" Predictive Analytics: Next-Day Price Forecasts")
            st.write("Calculated using a Spark Windowed 3-Day Rolling Moving Average.")
            
            df_f_display = df_forecast[["symbol", "valid_date", "price", "next_day_forecast"]].copy()
            df_f_display["Direction"] = df_f_display.apply(
                lambda r: " UP" if r["next_day_forecast"] > r["price"] else " DOWN", axis=1
            )
            
            st.dataframe(df_f_display.rename(columns={
                "symbol": "Symbol", "valid_date": "As Of Date", 
                "price": "Last Close Price ($)", "next_day_forecast": "Spark Predicted Tomorrow ($)"
            }), use_container_width=True)
            
        else:
            st.info("No analytics summaries found in database. Run 'python3 analytics.py' to generate.")
    else:
        st.error("Could not load Spark insights from API endpoints.")


elif current_view == "🤖 AI Assistant":
    st.header("🤖 Financial Data Assistant (Powered by MCP)")
    st.write("Ask me anything about your database! I am connected to the platform via the Model Context Protocol.")

    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

    
    if "messages" not in st.session_state:
        st.session_state.messages = []

    
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])


    async def run_mcp_chat(user_prompt):
                    main_loop = asyncio.get_running_loop()

                    def call_mcp_tool_sync(tool_name, arguments):
                        try:
                            future = asyncio.run_coroutine_threadsafe(
                                session.call_tool(tool_name, arguments), 
                                main_loop
                            )
                            result = future.result(timeout=15)
                            
                            if result and result.content:
                                return result.content[0].text
                            return "No data returned from tool."
                        except Exception as e:
                            return f"Error executing tool {tool_name}: {str(e)}"

                    def proxy_list_assets(): return call_mcp_tool_sync("list_database_assets", {})
                    def proxy_get_metadata(symbol: str): return call_mcp_tool_sync("get_asset_metadata", {"symbol": symbol})
                    def proxy_get_analytics(): return call_mcp_tool_sync("get_spark_analytics", {})
                    def proxy_get_forecasts(): return call_mcp_tool_sync("get_spark_forecasts", {})

                    model = genai.GenerativeModel(
                        model_name='gemini-3.5-flash',
                        tools=[proxy_list_assets, proxy_get_metadata, proxy_get_analytics, proxy_get_forecasts],
                        system_instruction="You are a financial assistant. Use your tools to fetch data from the database via MCP."
                    )
                    
                    gemini_history = []
                    for msg in st.session_state.messages[:-1]:
                        role = "model" if msg["role"] == "assistant" else "user"
                        if isinstance(msg["content"], str):
                            gemini_history.append({"role": role, "parts": [msg["content"]]})
                    
                    chat = model.start_chat(
                        history=gemini_history,
                        enable_automatic_function_calling=True
                    )
                        
                    response = await asyncio.to_thread(chat.send_message, user_prompt)
                    return response.text
    
    if prompt := st.chat_input("Ask me something like: 'Which asset has the highest volatility?'"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Connecting to MCP Server and thinking..."):
                try:
                    final_response = asyncio.run(run_mcp_chat(prompt))
                    st.markdown(final_response)
                    st.session_state.messages.append({"role": "assistant", "content": final_response})
                except Exception as e:
                    if "429" in str(e) or "Quota" in str(e):
                        st.warning(" Gemini API rate limit reached (Free Tier). Please wait 30 seconds and try again!")
                    else:
                        st.error(f"Error: {str(e)}")