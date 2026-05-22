import websocket
import json
import requests
import time

FINNHUB_TOKEN = "d86fpu1r01qgiu458c80d86fpu1r01qgiu458c8g"
N8N_WEBHOOK_URL = "https://go90ng-n8n.eq7icp.easypanel.host/webhook/bcca44dc-8944-41a2-8d96-3c5eb1f159e9"

# Add your symbols here (keep under 40 for optimal WebSocket performance)
WATCHLIST = ["AAPL", "NVDA", "TSLA", "AMD", "MSFT", "PLTR", "EDSA", "NXXT", "RGTI", "SIVEF", "SPCE", "QBTS", "INFQ", "XRX", "INDI", "BB", "DELL"]

market_data = {}

print("Fetching Market Caps and Previous Close data (Applying safe rate-limits)...")
for sym in WATCHLIST:
    market_data[sym] = {
        "market_cap": 0,
        "prev_close": 0,
        "current_price": 0,
        "cumulative_volume": 0,
        "total_dollar_traded": 0,
        "percent_change": 0
    }
    
    try:
        # 1. Fetch Market Cap
        prof_res = requests.get(f"https://finnhub.io/api/v1/stock/profile2?symbol={sym}&token={FINNHUB_TOKEN}")
        if prof_res.status_code == 200:
            prof_data = prof_res.json()
            if "marketCapitalization" in prof_data:
                market_data[sym]["market_cap"] = prof_data["marketCapitalization"]
                
        # 2. Fetch Previous Close
        quote_res = requests.get(f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_TOKEN}")
        if quote_res.status_code == 200:
            quote_data = quote_res.json()
            if "pc" in quote_data:
                market_data[sym]["prev_close"] = quote_data["pc"]
                
    except Exception as e:
        print(f"Error fetching fundamental data for {sym}: {e}")
        
    # ANTI-BAN PROTECTION: Wait 1.5 seconds between symbols to stay safely under the 60/min limit
    time.sleep(1.5)

print("Fundamentals successfully loaded! Opening live WebSocket...")

last_n8n_trigger = time.time()

def on_message(ws, message):
    global last_n8n_trigger
    data = json.loads(message)
    
    # Process only live trades
    if data.get('type') == 'trade':
        for trade in data['data']:
            sym = trade['s']
            price = trade['p']
            vol = trade['v']
            
            if sym in market_data:
                # Tally the live math
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
            qualified_symbols = []
            
            print("\n--- 60 SECOND METRIC CHECK ---")
            
            for sym, metrics in market_data.items():
                p_change = metrics["percent_change"]
                cum_vol = metrics["cumulative_volume"]
                mkt_cap = metrics["market_cap"]
                
                # Print the internal math to the logs so we can see why a stock is failing the test!
                print(f"[{sym}] Live % Change: {p_change:.2f}% | Session Vol: {cum_vol} | Mkt Cap: ${mkt_cap}M")
                
                # THE LOGIC GATE
                # NOTE: For testing purposes, Volume is temporarily lowered to 1,000 so it actually triggers.
                # Once you confirm it reaches n8n, change 1000 back to 3000000.
                if p_change >= 3.0 and cum_vol >= 3000000 and mkt_cap >= 30:
                    avg_price = metrics["total_dollar_traded"] / cum_vol if cum_vol > 0 else metrics["current_price"]
                    
                    qualified_symbols.append({
                        "symbol": sym,
                        "percent_change": round(p_change, 2),
                        "last_price": metrics["current_price"],
                        "intraday_avg_price": round(avg_price, 2),
                        "session_volume": cum_vol,
                        "market_cap_millions": mkt_cap
                    })
            
            if qualified_symbols:
                qualified_symbols.sort(key=lambda x: x["percent_change"], reverse=True)
                print(f"\n>>> SUCCESS! Triggering n8n with {len(qualified_symbols)} qualified symbols! <<<")
                
                payload = {
                    "scan_type": "high_momentum_breakout",
                    "timestamp": current_time,
                    "top_ranked_symbols": qualified_symbols
                }
                
                try:
                    requests.post(N8N_WEBHOOK_URL, json=payload)
                except Exception as e:
                    print(f"Failed to reach n8n Webhook: {e}")
            else:
                print("No symbols passed the +3% / 1k Vol / $30M Cap test this minute.")

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### WebSocket Connection Closed ###")

def on_open(ws):
    for sym in WATCHLIST:
        ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
    print("Successfully Subscribed. Listening for trades...")

if __name__ == "__main__":
    websocket.enableTrace(False)
    
    while True:
        print("Initializing WebSocket Connection...")
        ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
                                  on_open=on_open,
                                  on_message=on_message,
                                  on_error=on_error,
                                  on_close=on_close)
        
        # Adding Ping Interval to prevent ghost disconnections
        ws.run_forever(ping_interval=30, ping_timeout=10)
        
        print("Connection dropped! Waiting 15 seconds before reconnecting...")
        time.sleep(15)
