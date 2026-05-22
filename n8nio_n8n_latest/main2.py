import websocket
import json
import requests
import time

FINNHUB_TOKEN = "d86fpu1r01qgiu458c80d86fpu1r01qgiu458c8g"
N8N_WEBHOOK_URL = "https://go90ng-n8n.eq7icp.easypanel.host/webhook/bcca44dc-8944-41a2-8d96-3c5eb1f159e9"

# The symbols you want to monitor (Finnhub standard tier usually allows up to 50 WS subscriptions)
WATCHLIST = ["AAPL", "NVDA", "TSLA", "AMD", "MSFT", "PLTR"]

# This dictionary will hold all our live math and fundamental data
market_data = {}

# --- BOOT UP PHASE: Fetch Fundamentals ---
print("Fetching Market Caps and Previous Close data...")
for sym in WATCHLIST:
    market_data[sym] = {
        "market_cap": 0,
        "prev_close": 0,
        "current_price": 0,
        "cumulative_volume": 0,
        "total_dollar_traded": 0, # Used to calculate Average Price (VWAP)
        "percent_change": 0
    }
    
    try:
        # Get Market Cap from Profile API
        prof_res = requests.get(f"https://finnhub.io/api/v1/stock/profile2?symbol={sym}&token={FINNHUB_TOKEN}").json()
        if "marketCapitalization" in prof_res:
            # Finnhub returns Market Cap in Millions. So 30 = $30M.
            market_data[sym]["market_cap"] = prof_res["marketCapitalization"]
            
        # Get Previous Close from Quote API
        quote_res = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_TOKEN}").json()
        if "pc" in quote_res:
            market_data[sym]["prev_close"] = quote_res["pc"]
            
    except Exception as e:
        print(f"Error fetching data for {sym}: {e}")

print("Fundamentals loaded. Opening WebSocket...")

# Timer to prevent spamming n8n (Send payload every 60 seconds)
last_n8n_trigger = time.time()

def on_message(ws, message):
    global last_n8n_trigger
    data = json.loads(message)
    
    if data.get('type') == 'trade':
        for trade in data['data']:
            sym = trade['s']
            price = trade['p']  # Last price
            vol = trade['v']    # Trade volume
            
            if sym in market_data:
                # Update live metrics
                market_data[sym]["current_price"] = price
                market_data[sym]["cumulative_volume"] += vol
                market_data[sym]["total_dollar_traded"] += (price * vol)
                
                # Calculate Daily % Change: ((Current - PrevClose) / PrevClose) * 100
                prev_close = market_data[sym]["prev_close"]
                if prev_close > 0:
                    market_data[sym]["percent_change"] = ((price - prev_close) / prev_close) * 100

        # --- EVALUATION & TRIGGER PHASE ---
        current_time = time.time()
        
        # Check if 60 seconds have passed since we last sent data to n8n
        if current_time - last_n8n_trigger >= 60:
            last_n8n_trigger = current_time
            
            qualified_symbols = []
            
            for sym, metrics in market_data.items():
                p_change = metrics["percent_change"]
                cum_vol = metrics["cumulative_volume"]
                mkt_cap = metrics["market_cap"]
                
                # FILTER LOGIC: +3% Change, 3M Volume, $30M Market Cap
                # (Note: Finnhub market cap is returned in Millions, so 30 = $30M)
                if p_change >= 3.0 and cum_vol >= 3000000 and mkt_cap >= 30:
                    
                    # Calculate Intraday Average Price (Volume Weighted Average Price)
                    avg_price = metrics["total_dollar_traded"] / cum_vol if cum_vol > 0 else metrics["current_price"]
                    
                    qualified_symbols.append({
                        "symbol": sym,
                        "percent_change": round(p_change, 2),
                        "last_price": metrics["current_price"],
                        "intraday_avg_price": round(avg_price, 2),
                        "volume_today": cum_vol,
                        "market_cap_millions": mkt_cap
                    })
            
            # If any stocks survived the strict filters, rank them and send to n8n!
            if qualified_symbols:
                # Sort the list by percent_change descending (highest first)
                qualified_symbols.sort(key=lambda x: x["percent_change"], reverse=True)
                
                print(f"Triggering n8n! Found {len(qualified_symbols)} qualified symbols.")
                
                payload = {
                    "scan_type": "high_momentum_breakout",
                    "timestamp": current_time,
                    "top_ranked_symbols": qualified_symbols
                }
                
                try:
                    requests.post(N8N_WEBHOOK_URL, json=payload)
                except Exception as e:
                    print(f"Failed to trigger n8n: {e}")


def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### WebSocket Closed ###")

def on_open(ws):
    for sym in WATCHLIST:
        ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
    print("Subscribed to live streams.")

if __name__ == "__main__":
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    ws.run_forever()
