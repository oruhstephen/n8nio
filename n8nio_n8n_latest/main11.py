import websocket
import json
import requests
import time
import threading
import sys
import pandas as pd
import yfinance as yf
from yfinance import EquityQuery
from datetime import datetime
from zoneinfo import ZoneInfo

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
            
            day_high = quote.get("regularMarketDayHigh", 0)
            day_low = quote.get("regularMarketDayLow", 0)
            current_price = quote.get("regularMarketPrice", 0)
            day_volume = quote.get("regularMarketVolume", 0)
            fifty_day_avg = quote.get("fiftyDayAverage", 0) # Used for Alignment

            # If today's price is higher than the 50-day average, the macro trend is Bullish.
            macro_uptrend = False
            if current_price > fifty_day_avg and fifty_day_avg > 0:
                macro_uptrend = True
            
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
                "fifty_day_avg": fifty_day_avg,
                "macro_uptrend": macro_uptrend,
                "regularMarketPrice": current_price
            })
            
        top_40 = qualified_symbols[:40]
        
        # ==========================================
        # BATCH DOWNLOAD: ALIGNMENT & GRAVITY CALC
        # ==========================================
        print(f"\nDownloading 2-month history for {len(top_40)} symbols to calculate Alignments, Over-Extension and high-momentum matches...")
        sym_list = [t["symbol"] for t in top_40]
        
        if sym_list:
            hist_data = yf.download(sym_list, period="2mo", progress=False)
            closes = hist_data['Close'] if 'Close' in hist_data else hist_data
                
            for target in top_40:
                sym = target["symbol"]
                target["ma_5"] = 0.0 
                target["alignment_state"] = "MIXED"
                
                try:
                    stock_closes = closes.dropna() if len(sym_list) == 1 else closes[sym].dropna()
                    
                    if len(stock_closes) >= 21:
                        # Calculate all required moving averages
                        ma_5 = stock_closes.tail(5).mean()
                        ma_10 = stock_closes.tail(10).mean()
                        ma_21 = stock_closes.tail(21).mean()
                        ma_50 = target.get("fifty_day_avg", 0)
                        
                        target["ma_5"] = ma_5
                        
                        # Calculate Alignment State
                        if ma_50 > 0:
                            if ma_10 > ma_21 and ma_21 > ma_50:
                                target["alignment_state"] = "FULL_BULLISH"
                            elif ma_10 < ma_21 and ma_21 < ma_50:
                                target["alignment_state"] = "BEARISH"
                            else:
                                min_ma = min(ma_10, ma_21, ma_50)
                                max_ma = max(ma_10, ma_21, ma_50)
                                if ((max_ma - min_ma) / min_ma) <= 0.02:
                                    target["alignment_state"] = "COMPRESSION"
                                    
                    print(f" -> [{sym}] Alignment: {target['alignment_state']} | 5-MA: ${target['ma_5']:.2f}")
                except Exception:
                    print(f" -> [{sym}] MA calculation failed. Defaulting to safe values.")
        
        print(f"\nFull Market Scan Complete! Found {len(top_40)} high-momentum matches.")
        return top_40

    except Exception as e:
        print(f"API screener failed: {e}")
        sys.exit(1)

TODAYS_TARGETS = get_morning_watchlist()
WATCHLIST = [target["symbol"].replace('-', '.') for target in TODAYS_TARGETS]

market_data = {}
for target in TODAYS_TARGETS:
    sym = target["symbol"].replace('-', '.')
    market_data[sym] = {
        "prev_close": target.get("prev_close", 0),
        "current_price": target.get("regularMarketPrice", 0),
        "cumulative_volume": target.get("day_volume", 0),           
        "total_dollar_traded": target.get("seed_dollar_traded", 0), 
        "percent_change": 0,
        "high_of_day_price": target.get("day_high", 0),             
        "price_60s_ago": target.get("regularMarketPrice", 0),
        "ma_5": target.get("ma_5", 0.0), 
        "alignment_state": target.get("alignment_state", "MIXED"),
        "macro_uptrend": target.get("macro_uptrend", False),

        # --- NEW: ORDER FLOW TRACKING ---
        "prev_tick_price": target.get("regularMarketPrice", 0),
        "rolling_buy_vol": 0,
        "rolling_sell_vol": 0
        
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
                    market_data[sym]["current_price"] = price
                    market_data[sym]["cumulative_volume"] += vol
                    market_data[sym]["total_dollar_traded"] += (price * vol)
                    
                    if price > market_data[sym]["high_of_day_price"]:
                        market_data[sym]["high_of_day_price"] = price

                    # --- NEW: THE ORDER FLOW TICK TEST ---
                    prev_tick = market_data[sym]["prev_tick_price"]
                    if prev_tick > 0:
                        if price > prev_tick:
                            market_data[sym]["rolling_buy_vol"] += vol  # Aggressive Ask Slap
                        elif price < prev_tick:
                            market_data[sym]["rolling_sell_vol"] += vol # Aggressive Bid Hit
                    
                    # Save this tick's price for the next comparison
                    market_data[sym]["prev_tick_price"] = price
                    
                    prev_close = market_data[sym]["prev_close"]
                    if prev_close > 0:
                        market_data[sym]["percent_change"] = ((price - prev_close) / prev_close) * 100

            current_time = time.time()
            if current_time - last_n8n_trigger >= 60:
                last_n8n_trigger = current_time
                triggered_symbols = []

                # --- NEW: TIMESTAMP GENERATOR ---
                # Formats the time as HH:MM:SS (e.g., 09:45:30)
                uk_time = datetime.now(ZoneInfo("Europe/London"))
                timestamp_str = uk_time.strftime("%Y-%m-%d %H:%M:%S")
                
                print(f"\n--- {timestamp_str} | 60 SECOND VWAP EXPLOSION CHECK ---")
                
                for sym, metrics in market_data.items():
                    p_change = metrics["percent_change"]
                    cum_vol = metrics["cumulative_volume"]
                    hod_price = metrics["high_of_day_price"]
                    current_price = metrics["current_price"]
                    price_60s_ago = metrics["price_60s_ago"]
                    ma_5 = metrics["ma_5"]
                    alignment = metrics["alignment_state"]
                    
                    if cum_vol > 50000 and current_price > 0: 
                        avg_price = metrics["total_dollar_traded"] / cum_vol
                        vwap_distance = ((current_price - avg_price) / avg_price) * 100
                        
                        upside_potential = 0
                        if avg_price > 0:
                            upside_potential = ((hod_price - avg_price) / avg_price) * 100

                        dynamic_target = min(15.0, max(5.0, p_change * 0.45))

                        nearest_whole_dollar = round(current_price)
                        is_converging = False
                        if current_price > 2.00:
                            cents_away = abs(current_price - nearest_whole_dollar)
                            if cents_away <= 0.05:
                                is_converging = True

                        is_bouncing = current_price >= price_60s_ago
                        stop_loss_price = avg_price * 0.975
                        
                        extension_pct = 0.0
                        is_over_extended = False
                        if ma_5 > 0:
                            extension_pct = ((current_price - ma_5) / ma_5) * 100
                            if extension_pct >= 50.0:
                                is_over_extended = True

                        # --- NEW: 60-SECOND ORDER FLOW DELTA ---
                        buy_v = metrics["rolling_buy_vol"]
                        sell_v = metrics["rolling_sell_vol"]
                        total_tick_vol = buy_v + sell_v
                        
                        order_flow_delta_pct = 0.0
                        if total_tick_vol > 0:
                            # Formula: (Buys - Sells) / Total * 100
                            order_flow_delta_pct = ((buy_v - sell_v) / total_tick_vol) * 100
                            
                        # Reset the rolling volumes for the next 60-second window
                        market_data[sym]["rolling_buy_vol"] = 0
                        market_data[sym]["rolling_sell_vol"] = 0
                        
                        print(f"[{sym}] +{p_change:.2f}% | Align: {alignment} | Ext5MA: +{extension_pct:.1f}% | Converge: {is_converging} | Delta: {order_flow_delta_pct:.2f}% | Bounce: {is_bouncing} | VWAPDist: {vwap_distance:.2f}% | Up2HOD: {upside_potential:.2f}% | T.Need: {dynamic_target:.2f}%")
                        
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
                                "daily_alignment_state": alignment,
                                "extension_from_5ma": round(extension_pct, 2),
                                "is_over_extended": is_over_extended,
                                "DateTime":timestamp_str,
                                "daily_macro_uptrend": metrics["macro_uptrend"],
                                "order_flow_delta_1m": round(order_flow_delta_pct, 2)
                                
                            })

                for sym in market_data:
                    if market_data[sym]["current_price"] > 0:
                        market_data[sym]["price_60s_ago"] = market_data[sym]["current_price"]
                
                if triggered_symbols:
                    print(f"\n>>> FIRING N8N WEBHOOK! {len(triggered_symbols)} stocks triggered algorithmic execution! <<<")
                    payload = {
                        "scan_type": "vwap_dynamic_bounce",
                        "timestamp": current_time,
                        "top_ranked_symbols": triggered_symbols
                    }
                    
                    threading.Thread(
                        target=requests.post, 
                        args=(N8N_WEBHOOK_URL,), 
                        kwargs={'json': payload}
                    ).start()

    except json.JSONDecodeError:
        pass
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
