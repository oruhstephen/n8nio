import websocket
import json
import requests
import time
import yfinance as yf
import pandas as pd

FINNHUB_TOKEN = "YOUR_FINNHUB_TOKEN_HERE"
N8N_WEBHOOK_URL = "YOUR_N8N_WEBHOOK_URL_HERE"

# ==========================================
# PHASE 1: THE NATIVE QUANT SCREENER
# ==========================================
def get_morning_watchlist():
    print("\n--- BOOTING NATIVE QUANT SCREENER ---")
    print("Loading universe of tickers from local symbols.json...")
    
    try:
       # 1. READ AND FILTER THE LOCAL JSON FILE
        with open('USSYMBOLS.json', 'r') as file:
            raw_data = json.load(file)
            
        all_tickers = []
        
        # Loop through the JSON and keep only Common Stocks
        for item in raw_data:
            if item.get("type") == "Common Stock":
                all_tickers.append(item["symbol"])
                
        print(f"Scanned {len(raw_data)} total assets. Isolated {len(all_tickers)} Common Stocks.")
        
        # 2. THE SAFETY SLICE
        # Finnhub's US Common Stock list still contains ~5,000 to 10,000 tickers.
        # We MUST slice this list to the first 1,500 to prevent your Easypanel server 
        # from crashing out of memory and Yahoo from banning your IP!
        tickers = all_tickers[:1500] 
        
        # Clean up formatting for yfinance (e.g., BRK.B becomes BRK-B)
        tickers = [t.replace('.', '-') for t in tickers]
        
        print(f"Executing morning scan on the top {len(tickers)} symbols...")
        print("Downloading 3-month historical data. This will take 15-30 seconds...")
        
        # 3. BULK DOWNLOAD DATA
        data = yf.download(tickers, period="3mo", group_by='ticker', auto_adjust=True, threads=True, progress=False)
                
        # 4. THE LOGIC GATE
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
                
        # Rank by morning momentum and keep top 40 for Finnhub limits
        qualified_symbols.sort(key=lambda x: x["percent_change"], reverse=True)
        top_40 = qualified_symbols[:40]
        
        print(f"Screener Complete! Found {len(top_40)} exact matches.")
        return top_40

    except FileNotFoundError:
        print("CRITICAL ERROR: symbols.json file not found in the directory!")
        return ["AAPL", "NVDA", "TSLA"] # Fallback
    except json.JSONDecodeError:
        print("CRITICAL ERROR: symbols.json is not formatted correctly. Check for missing quotes or trailing commas.")
        return ["AAPL", "NVDA", "TSLA"]
    except Exception as e:
        print(f"Native screener failed: {e}")
        return ["AAPL", "NVDA", "TSLA"]

# Run the screener to get today's targets
TODAYS_TARGETS = get_morning_watchlist()
WATCHLIST = [target["symbol"] for target in TODAYS_TARGETS]

# Set up the live tracking dictionary
market_data = {}
for target in TODAYS_TARGETS:
    sym = target["symbol"]
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
                
                if cum_vol > 5000: 
                    avg_price = metrics["total_dollar_traded"] / cum_vol
                    vwap_distance = ((metrics["current_price"] - avg_price) / avg_price) * 100
                    
                    print(f"[{sym}] Change: {p_change:.2f}% | VWAP Dist: {vwap_distance:.2f}%")
                    
                    if p_change >= 3.0 and -0.5 <= vwap_distance <= 0.5:
                        triggered_symbols.append({
                            "symbol": sym,
                            "live_percent_change": round(p_change, 2),
                            "last_price": metrics["current_price"],
                            "intraday_vwap": round(avg_price, 2)
                        })
            
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
    print(f"Subscribed to {len(WATCHLIST)} streams. Hunting for entries...")

if __name__ == "__main__":
    websocket.enableTrace(False)
    while True:
        ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
                                  on_open=on_open,
                                  on_message=on_message,
                                  on_error=on_error,
                                  on_close=on_close)
        ws.run_forever(ping_interval=30, ping_timeout=10)
        print("Reconnecting in 15 seconds...")
        time.sleep(15)
