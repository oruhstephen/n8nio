import websocket
import json
import requests
import time
import threading
import sys
import yfinance as yf
from yfinance import EquityQuery

# ==========================================
# CONFIGURATION
# ==========================================
FINNHUB_TOKEN = "d86fpu1r01qgiu458c80d86fpu1r01qgiu458c8g"
N8N_WEBHOOK_URL = "https://go90ng-n8n.eq7icp.easypanel.host/webhook/bcca44dc-8944-41a2-8d96-3c5eb1f159e9"

# ==========================================
# PHASE 1: YAHOO NATIVE API SCREENER
# ==========================================
def get_morning_watchlist():
    print("\n--- BOOTING YAHOO NATIVE API SCREENER ---")
    
    try:
        print("Building institutional logic gate...")
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', 60000000]),  # $60M+ Market Cap
            EquityQuery('gt',  ['intradayprice', 1.00]),              # Price > $1
            EquityQuery('gte', ['percentchange', 3.0]),            # +3% Intraday Gain
            EquityQuery('gte', ['avgdailyvol3m', 250000])          # 250k+ 3-Month Average Vol
        ])

        print("Executing instant query against Yahoo's live servers...")
        response = yf.screen(q, sortField='percentchange', sortAsc=False)
        
        quotes = response.get('quotes', [])
        qualified_symbols = []
        
        for quote in quotes:
            sym = quote.get("symbol")
            prev_close = quote.get("regularMarketPreviousClose", 0)
            live_change = quote.get("regularMarketChangePercent", 0)
            day_volume = quote.get("regularMarketVolume", 0)

            # --- NEW: MACRO TREND CALCULATION ---
            fifty_day_avg = quote.get("fiftyDayAverage", 0)
            
            # --- SEED DATA FOR LATE-START VWAP & HOD ---
            day_high = quote.get("regularMarketDayHigh", 0)
            day_low = quote.get("regularMarketDayLow", 0)
            current_price = quote.get("regularMarketPrice", 0)
            day_volume = quote.get("regularMarketVolume", 0)

            # If today's price is higher than the 50-day average, the macro trend is Bullish.
            macro_uptrend = False
            if current_price > fifty_day_avg and fifty_day_avg > 0:
                macro_uptrend = True
                
            # Approximate the morning's Total Dollar Traded using the Typical Price formula
            typical_price = current_price
            if (day_high + day_low + current_price) > 0:
                typical_price = (day_high + day_low + current_price) / 3
                
            seed_dollar_traded = typical_price * day_volume
            
            print(f" -> [MATCH] {sym} | +{live_change:.2f}% | Seed Vol: {day_volume}")
            
            qualified_symbols.append({
                "symbol": sym,
                "prev_close": prev_close,
                "day_high": day_high,
                "day_volume": day_volume,
                "seed_dollar_traded": seed_dollar_traded,
                "macro_uptrend": macro_uptrend
            })
            
        top_40 = qualified_symbols[:40]
        print(f"\nFull Market Scan Complete! Found {len(top_40)} high-momentum matches.")
        return top_40

    except Exception as e:
        print(f"API screener failed: {e}")
        print("CRITICAL: Screener failed to build watchlist. Shutting down to protect capital.")
        sys.exit(1) # Instantly kills the script so it doesn't trade on fake data

# Boot up the watchlist
TODAYS_TARGETS = get_morning_watchlist()

# Finnhub requires the dot format (BRK.B instead of BRK-B)
WATCHLIST = [target["symbol"].replace('-', '.') for target in TODAYS_TARGETS]

# Set up the live tracking dictionary with seeded values
market_data = {}
for target in TODAYS_TARGETS:
    sym = target["symbol"].replace('-', '.')
    market_data[sym] = {
        "prev_close": target.get("prev_close", 0),
        "current_price": 0,
        "cumulative_volume": target.get("day_volume", 0),           
        "total_dollar_traded": target.get("seed_dollar_traded", 0), 
        "percent_change": 0,
        "high_of_day_price": target.get("day_high", 0),             
        "price_60s_ago": target.get("regularMarketPrice", 0),        # FIXED: Bot memory initialization
        "price_60s_ago": target.get("regularMarketPrice", 0),
        "macro_uptrend": target.get("macro_uptrend", False)
    }

# ==========================================
# PHASE 2: THE INTRADAY WEBSOCKET SNIPER
# ==========================================
print("\n--- STARTING LIVE INTRADAY TRACKING ---")
last_n8n_trigger = time.time()

def on_message(ws, message):
    global last_n8n_trigger
    
    try:
        data = json.loads(message)
        
        if data.get('type') == 'trade':
            for trade in data['data']:
                sym = trade['s']
                price = trade['p']
                vol = trade['v']
                
                if sym in market_data:
                    # 1. Update live tracking variables
                    market_data[sym]["current_price"] = price
                    market_data[sym]["cumulative_volume"] += vol
                    market_data[sym]["total_dollar_traded"] += (price * vol)
                    
                    # 2. Track the High of Day (HOD)
                    if price > market_data[sym]["high_of_day_price"]:
                        market_data[sym]["high_of_day_price"] = price
                    
                    # 3. Calculate Live Percent Change
                    prev_close = market_data[sym]["prev_close"]
                    if prev_close > 0:
                        market_data[sym]["percent_change"] = ((price - prev_close) / prev_close) * 100

            # --- EVALUATION PHASE (Runs every 60 seconds) ---
            current_time = time.time()
            if current_time - last_n8n_trigger >= 60:
                last_n8n_trigger = current_time
                triggered_symbols = []
                
                print("\n--- 60 SECOND VWAP EXPLOSION CHECK ---")
                
                for sym, metrics in market_data.items():
                    p_change = metrics["percent_change"]
                    cum_vol = metrics["cumulative_volume"]
                    hod_price = metrics["high_of_day_price"]
                    current_price = metrics["current_price"]
                    price_60s_ago = metrics["price_60s_ago"]
                    
                    if cum_vol > 50000 and current_price > 0: 
                        avg_price = metrics["total_dollar_traded"] / cum_vol
                        vwap_distance = ((current_price - avg_price) / avg_price) * 100
                        
                        upside_potential = 0
                        if avg_price > 0:
                            upside_potential = ((hod_price - avg_price) / avg_price) * 100

                        # --- NEW: SUPERNOVA CEILING ---
                        # Require 45% of the morning's total run, with a 5% floor, capping at a 15% maximum requirement.
                        dynamic_target =  max(5.0, p_change * 0.45)

                        # --- NEW: WHOLE-DOLLAR CONVERGENCE LOGIC ---
                        nearest_whole_dollar = round(current_price)
                        
                        is_converging = False
                        if current_price > 2.00:
                            cents_away = abs(current_price - nearest_whole_dollar)
                            if cents_away <= 0.05:
                                is_converging = True

                        # --- NEW: FALLING KNIFE FILTER ---
                        is_bouncing = current_price >= price_60s_ago

                        # --- NEW: STRICT STOP-LOSS CALCULATION ---
                        stop_loss_price = avg_price * 0.975
                        
                        print(f"[{sym}] +{p_change:.2f}% | Price: {current_price:.2f} | Vol: {cum_vol} | VWAP Dist: {vwap_distance:.2f}% | Upside to HOD: {upside_potential:.2f}% | Target Needed: {dynamic_target:.2f}% | Converging: {is_converging} | Bouncing: {is_bouncing}")
                        
                        # THE DYNAMIC RUNNER LOGIC GATE
                        if p_change >= 8.0 and upside_potential >= dynamic_target and -0.5 <= vwap_distance <= 1.0 and is_bouncing:
                            triggered_symbols.append({
                                "symbol": sym,
                                "live_percent_change": round(p_change, 2),
                                "last_price": current_price,
                                "intraday_vwap": round(avg_price, 2),
                                "upside_to_hod": round(upside_potential, 2),
                                "required_dynamic_target": round(dynamic_target, 2),
                                "live_volume": cum_vol,
                                "whole_dollar_convergence": is_converging,
                                "stop_loss_price": round(stop_loss_price, 2),
                                "daily_macro_uptrend": metrics["macro_uptrend"]
                            })

                # --- NEW: UPDATE BOT MEMORY ---
                # Properly indented OUTSIDE the evaluation loop, to update the memory for the next 60s cycle
                for sym in market_data:
                    if market_data[sym]["current_price"] > 0:
                        market_data[sym]["price_60s_ago"] = market_data[sym]["current_price"]
                
                # Fire the n8n Webhook asynchronously to prevent WebSocket freezing
                if triggered_symbols:
                    print(f"\n>>> FIRING N8N WEBHOOK! {len(triggered_symbols)} stocks triggered algorithmic execution! <<<")
                    payload = {
                        "scan_type": "vwap_dynamic_bounce",
                        "timestamp": current_time,
                        "top_ranked_symbols": triggered_symbols
                    }
                    
                    # Execute the POST request in a background thread
                    threading.Thread(
                        target=requests.post, 
                        args=(N8N_WEBHOOK_URL,), 
                        kwargs={'json': payload}
                    ).start()

    except json.JSONDecodeError:
        pass # Silently ignore weird non-JSON packets from Finnhub (e.g., connection pings)
    except Exception as e:
        print(f"WebSocket processing error: {e}")

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### WebSocket Connection Closed ###")

def on_open(ws):
    for sym in WATCHLIST:
        ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
    print(f"Subscribed to {len(WATCHLIST)} streams. Hunting for explosive pullbacks...")

if __name__ == "__main__":
    websocket.enableTrace(False)
    
    while True:
        ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
                                  on_open=on_open,
                                  on_message=on_message,
                                  on_error=on_error,
                                  on_close=on_close)
        
        ws.run_forever(ping_interval=30, ping_timeout=10)
        print("Connection dropped! Reconnecting in 15 seconds...")
        time.sleep(15)
