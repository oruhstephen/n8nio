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
        
        print(f"Full Market Scan Complete! Found {len(top_40)} high-momentum matches.")
        return top_40

    except FileNotFoundError:
        print("CRITICAL ERROR: USSYMBOLS.json file not found in the directory!")
        return [{"symbol": "AAPL", "prev_close": 150}, {"symbol": "NVDA", "prev_close": 800}]
    except Exception as e:
        print(f"Native screener failed: {e}")
        return [{"symbol": "AAPL", "prev_close": 150}, {"symbol": "NVDA", "prev_close": 800}]

# Run the screener to get today's targets
TODAYS_TARGETS = get_morning_watchlist()

# Finnhub requires the dot format (BRK.B), so we revert the dashes back to dots for the WebSocket
WATCHLIST = [target["symbol"].replace('-', '.') for target in TODAYS_TARGETS]

# Set up the live tracking dictionary using the previous close we just scraped
market_data = {}
for target in TODAYS_TARGETS:
    sym = target["symbol"].replace('-', '.')
    market_data[sym] = {
        "prev_close": target.get("prev_close", 0),
        "current_price": 0,
        "cumulative_volume": 0,
        "total_dollar_traded": 0,
        "percent_change": 0
    }

# ==========================================
# PHASE 2: THE INTRADAY WEBSOCKET SNIPER
# ==========================================
print("\n--- STARTING LIVE INTRADAY TRACKING ---")
last_n8n_trigger = time.time()

def on_message(ws, message):
    global last_n8n_trigger
    data = json.loads(message)
    
    if data.get('type') == 'trade':
        for trade in data['data']:
            sym = trade['s']
            price = trade['p']
            vol = trade['v']
            
            if sym in market_data:
                # Update live tracking variables
                market_data[sym]["current_price"] = price
                market_data[sym]["cumulative_volume"] += vol
                market_data[sym]["total_dollar_traded"] += (price * vol)
                
                prev_close = market_data[sym]["prev_close"]
                if prev_close > 0:
                    market_data[sym]["percent_change"] = ((price - prev_close) / prev_close) * 100

        # --- EVALUATION PHASE (Runs every 60 seconds) ---
        current_time = time.time()
        if current_time - last_n8n_trigger >= 60:
            last_n8n_trigger = current_time
            triggered_symbols = []
            
            print("\n--- 60 SECOND VWAP PULLBACK CHECK ---")
            
            for sym, metrics in market_data.items():
                p_change = metrics["percent_change"]
                cum_vol = metrics["cumulative_volume"]
                
                # We need a minimum amount of volume before VWAP becomes mathematically reliable
                if cum_vol > 5000: 
                    avg_price = metrics["total_dollar_traded"] / cum_vol
                    vwap_distance = ((metrics["current_price"] - avg_price) / avg_price) * 100
                    
                    print(f"[{sym}] Change: {p_change:.2f}% | Vol: {cum_vol} | VWAP Dist: {vwap_distance:.2f}%")
                    
                    # THE TRIGGER: Still up +3% on the day, but has pulled back to within +/-0.5% of VWAP
                    if p_change >= 3.0 and -0.5 <= vwap_distance <= 0.5:
                        triggered_symbols.append({
                            "symbol": sym,
                            "live_percent_change": round(p_change, 2),
                            "last_price": metrics["current_price"],
                            "intraday_vwap": round(avg_price, 2)
                        })
            
            # Fire the n8n Webhook if we caught any setups
            if triggered_symbols:
                print(f"\n>>> FIRING N8N WEBHOOK! {len(triggered_symbols)} stocks hit the VWAP zone! <<<")
                payload = {
                    "scan_type": "vwap_pullback_bounce",
                    "timestamp": current_time,
                    "top_ranked_symbols": triggered_symbols
                }
                try:
                    requests.post(N8N_WEBHOOK_URL, json=payload)
                except Exception as e:
                    print(f"Webhook failed: {e}")

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### WebSocket Connection Closed ###")

def on_open(ws):
    for sym in WATCHLIST:
        ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
    print(f"Subscribed to {len(WATCHLIST)} streams. Hunting for pullbacks...")

if __name__ == "__main__":
    websocket.enableTrace(False)
    
    # Infinite loop prevents the script from permanently dying if the WebSocket drops
    while True:
        ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
                                  on_open=on_open,
                                  on_message=on_message,
                                  on_error=on_error,
                                  on_close=on_close)
        
        # Ping interval acts as a heartbeat so Finnhub doesn't drop idle connections
        ws.run_forever(ping_interval=30, ping_timeout=10)
        print("Connection dropped! Reconnecting in 15 seconds...")
        time.sleep(15)
