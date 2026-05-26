import websocket
import json
import requests
import time
import yfinance as yf
import pandas as pd
import numpy as np

FINNHUB_TOKEN = "d86fpu1r01qgiu458c80d86fpu1r01qgiu458c8g"
N8N_WEBHOOK_URL = "https://go90ng-n8n.eq7icp.easypanel.host/webhook/bcca44dc-8944-41a2-8d96-3c5eb1f159e9"

# ==========================================
# PHASE 1: THE NATIVE QUANT SCREENER
# ==========================================
def get_morning_watchlist():
    print("\n--- BOOTING NATIVE QUANT SCREENER ---")
    print("Loading universe of tickers from local USSYMBOLS.json...")
    
    try:
        # 1. READ AND FILTER THE LOCAL JSON FILE
        with open('USSYMBOLS.json', 'r') as file:
            raw_data = json.load(file)
            
        all_tickers = []
        for item in raw_data:
            if item.get("type") == "Common Stock":
                # Clean up formatting for yfinance (e.g., BRK.B becomes BRK-B)
                clean_sym = item["symbol"].replace('.', '-')
                all_tickers.append(clean_sym)
                
        print(f"Isolated {len(all_tickers)} Common Stocks. Beginning Batched Scan...")
        
        qualified_symbols = []
        batch_size = 500 # Safe chunk size for a standard VPS memory limit
        
        # 2. BATCHED DOWNLOADING (The Memory Saver)
        for i in range(0, len(all_tickers), batch_size):
            batch = all_tickers[i : i + batch_size]
            print(f"Scanning batch {i} to {i + len(batch)} of {len(all_tickers)}...")
            
            # Download just this batch
            data = yf.download(batch, period="3mo", group_by='ticker', auto_adjust=True, threads=True, progress=False)
            
            # 3. CALCULATE METRICS FOR THIS BATCH
            for sym in batch:
                try:
                    # yfinance returns different structures depending on if the batch has 1 or many items
                    if len(batch) > 1:
                        df = data[sym]
                    else:
                        df = data
                        
                    if df.empty or len(df) < 15: 
                        continue
                    
                    # --- Metrics Math ---
                    current_price = df['Close'].iloc[-1]
                    prev_close = df['Close'].iloc[-2]
                    
                    # Prevent division by zero
                    if prev_close <= 0:
                        continue
                        
                    percent_change = ((current_price - prev_close) / prev_close) * 100
                    avg_vol_3m = df['Volume'].mean()
                    
                    # --- RSI (14) Math ---
                    delta = df['Close'].diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    
                    # Handle edge cases where loss might be 0
                    if loss.iloc[-1] == 0:
                        rsi_14 = 100
                    else:
                        rsi_14 = 100 - (100 / (1 + rs.iloc[-1]))
                    
                    # --- THE HARD LOGIC GATE ---
                    if percent_change >= 3.0 and avg_vol_3m >= 3000000 and current_price > 0 and rsi_14 > 0:
                        qualified_symbols.append({
                            "symbol": sym,
                            "percent_change": percent_change,
                            "rsi": rsi_14,
                            "avg_vol": avg_vol_3m,
                            "prev_close": prev_close
                        })
                except Exception:
                    continue
            
            # Pause between batches so Yahoo Finance doesn't IP ban the server
            time.sleep(2) 
                
        # 4. RANK AND RETURN
        # Sort by highest morning momentum and keep top 40 to respect Finnhub limits
        qualified_symbols.sort(key=lambda x: x["percent_change"], reverse=True)
        top_40 = qualified_symbols[:40]
