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
# CONFIGURATION & INSTITUTIONAL PARAMETERS
# ==========================================
FINNHUB_TOKEN = "d86fpu1r01qgiu458c80d86fpu1r01qgiu458c8g"
N8N_WEBHOOK_URL = "https://go90ng-n8n.eq7icp.easypanel.host/webhook/bcca44dc-8944-41a2-8d96-3c5eb1f159e9"

# BATS exchange accounts for ~12.5% of total US market volume. 
# We scale incoming tick volume by 8.0 to align VWAP calculations with consolidated market volume.
BATS_VOLUME_MULTIPLIER = 8.0      

# Prevent alert spamming: 15-minute (900 seconds) cooldown per ticker
ALERT_COOLDOWN_SECONDS = 900      

# Re-scan market every 30 minutes (1800 seconds) to catch mid-day breakout runners
REFRESH_INTERVAL_SECONDS = 1800   

# Shared thread-safe state
data_lock = threading.Lock()
market_data = {}
ws_global = None


# ==========================================
# PHASE 1: YAHOO NATIVE API SCREENER
# ==========================================
def get_morning_watchlist():
    print("\n--- SCREENING MARKET FOR MOMENTUM TARGETS ---")
    
    try:
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', 60000000]),  # $60M+ Market Cap
            EquityQuery('gt',  ['intradayprice', 1.00]),           # Price > $1
            EquityQuery('gte', ['percentchange', 3.0]),            # +3% Intraday Gain
            EquityQuery('gte', ['avgdailyvol3m', 250000])          # 250k+ 3-Month Average Vol
        ])

        response = yf.screen(q, sortField='percentchange', sortAsc=False)
        quotes = response.get('quotes', [])
        qualified_symbols = []
        
        for quote in quotes:
            sym = quote.get("symbol")
            if not sym:
                continue

            prev_close = quote.get("regularMarketPreviousClose", 0)
            live_change = quote.get("regularMarketChangePercent", 0)
            day_high = quote.get("regularMarketDayHigh", 0)
            day_low = quote.get("regularMarketDayLow", 0)
            current_price = quote.get("regularMarketPrice", 0)
            day_volume = quote.get("regularMarketVolume", 0)
            fifty_day_avg = quote.get("fiftyDayAverage", 0)

            macro_uptrend = (current_price > fifty_day_avg and fifty_day_avg > 0)
            
            typical_price = current_price
            if (day_high + day_low + current_price) > 0:
                typical_price = (day_high + day_low + current_price) / 3
                
            seed_dollar_traded = typical_price * day_volume

            qualified_symbols.append({
                "symbol": sym,
                "prev_close": prev_close,
                "live_change": live_change,
                "day_high": day_high,
                "day_volume": day_volume,
                "seed_dollar_traded": seed_dollar_traded,
                "fifty_day_avg": fifty_day_avg,
                "macro_uptrend": macro_uptrend,
                "regularMarketPrice": current_price
            })
            
        top_40 = qualified_symbols[:40]
        
        # Batch download 2-month history for moving average calculations
        sym_list = [t["symbol"] for t in top_40]
        if sym_list:
            hist_data = yf.download(sym_list, period="2mo", progress=False)
            closes = hist_data['Close'] if 'Close' in hist_data else hist_data
                
            for target in top_40:
                sym = target["symbol"]
                target["ma_5"] = 0.0 
                target["alignment_state"] = "MIXED"
                
                try:
                    if isinstance(closes, pd.DataFrame) and sym in closes.columns:
                        stock_closes = closes[sym].dropna()
                    else:
                        stock_closes = closes.dropna()
                    
                    if len(stock_closes) >= 21:
                        ma_5 = stock_closes.tail(5).mean()
                        ma_10 = stock_closes.tail(10).mean()
                        ma_21 = stock_closes.tail(21).mean()
                        ma_50 = target.get("fifty_day_avg", 0)
                        
                        target["ma_5"] = ma_5
                        
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
                except Exception:
                    pass

        return top_40

    except Exception as e:
        print(f"Screener execution error: {e}")
        return []


def register_symbol_in_memory(target):
    """Helper function to safely seed symbol metrics in market_data."""
    sym = target["symbol"].replace('-', '.')
    with data_lock:
        if sym not in market_data:
            market_data[sym] = {
                "prev_close": target.get("prev_close", 0),
                "current_price": target.get("regularMarketPrice", 0),
                "cumulative_volume": target.get("day_volume", 0),           
                "total_dollar_traded": target.get("seed_dollar_traded", 0), 
                "percent_change": target.get("live_change", 0),
                "high_of_day_price": target.get("day_high", 0),             
                "price_60s_ago": target.get("regularMarketPrice", 0),
                "ma_5": target.get("ma_5", 0.0), 
                "alignment_state": target.get("alignment_state", "MIXED"),
                "macro_uptrend": target.get("macro_uptrend", False),
                "prev_tick_price": target.get("regularMarketPrice", 0),
                "rolling_buy_vol": 0,
                "rolling_sell_vol": 0,
                "last_alert_time": 0  # Timestamp of last sent alert for cooldown tracking
            }


# ==========================================
# PHASE 2: INDEPENDENT BACKGROUND THREADS
# ==========================================
def evaluation_loop():
    """
    INDEPENDENT TIMER THREAD: Executes 60-second diagnostic scans on schedule, 
    decoupled from WebSocket tick callbacks.
    """
    print("--- [THREAD] Independent 60-Second Evaluation Loop Started ---")
    while True:
        time.sleep(60)
        current_time = time.time()
        triggered_symbols = []

        uk_time = datetime.now(ZoneInfo("Europe/London"))
        timestamp_str = uk_time.strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"\n--- {timestamp_str} | 60-SECOND ALGORITHMIC VWAP SCANS ---")
        
        with data_lock:
            for sym, metrics in market_data.items():
                p_change = metrics["percent_change"]
                cum_vol = metrics["cumulative_volume"]
                hod_price = metrics["high_of_day_price"]
                current_price = metrics["current_price"]
                price_60s_ago = metrics["price_60s_ago"]
                ma_5 = metrics["ma_5"]
                alignment = metrics["alignment_state"]
                last_alert = metrics["last_alert_time"]
                
                # Order flow calculation
                buy_v = metrics["rolling_buy_vol"]
                sell_v = metrics["rolling_sell_vol"]
                total_tick_vol = buy_v + sell_v
                
                order_flow_delta_pct = 0.0
                if total_tick_vol > 0:
                    order_flow_delta_pct = ((buy_v - sell_v) / total_tick_vol) * 100
                    
                # Unconditionally reset 1-minute rolling volumes
                metrics["rolling_buy_vol"] = 0
                metrics["rolling_sell_vol"] = 0
                
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

                    # Cooldown Check: Must be > 15 minutes since last alert
                    in_cooldown = (current_time - last_alert) < ALERT_COOLDOWN_SECONDS

                    print(f"[{sym}] +{p_change:.2f}% | Align: {alignment} | Ext5MA: +{extension_pct:.1f}% | "
                          f"Delta: {order_flow_delta_pct:.1f}% | VWAPDist: {vwap_distance:.2f}% | "
                          f"Up2HOD: {upside_potential:.2f}% | Cooldown: {in_cooldown}")
                    
                    # Execution Criteria Gate
                    if (p_change >= 8.0 and 
                        upside_potential >= dynamic_target and 
                        -0.5 <= vwap_distance <= 1.0 and 
                        is_bouncing and 
                        not in_cooldown):
                        
                        metrics["last_alert_time"] = current_time  # Update cooldown timestamp
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
                            "DateTime": timestamp_str,
                            "daily_macro_uptrend": metrics["macro_uptrend"],
                            "order_flow_delta_1m": round(order_flow_delta_pct, 2)
                        })

                metrics["price_60s_ago"] = current_price

        # Fire Webhook Payload asynchronously
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


def watchlist_refresher_loop():
    """
    MID-DAY WATCHLIST REFRESHER THREAD: Re-scans market every 30 minutes to auto-subscribe
    to newly emerging intraday momentum runners.
    """
    print("--- [THREAD] Mid-Day Watchlist Refresher Loop Started ---")
    while True:
        time.sleep(REFRESH_INTERVAL_SECONDS)
        print("\n--- RE-SCANNING MARKET FOR NEW MID-DAY RUNNERS ---")
        try:
            latest_targets = get_morning_watchlist()
            new_symbols = []
            
            for target in latest_targets:
                sym = target["symbol"].replace('-', '.')
                with data_lock:
                    if sym not in market_data:
                        register_symbol_in_memory(target)
                        new_symbols.append(sym)
            
            # Subscribe to newly discovered symbols dynamically over live WebSocket
            if new_symbols and ws_global and ws_global.sock and ws_global.sock.connected:
                print(f"Subscribing to {len(new_symbols)} new mid-day tickers: {new_symbols}")
                for sym in new_symbols:
                    ws_global.send(json.dumps({"type": "subscribe", "symbol": sym}))
        except Exception as e:
            print(f"Mid-day refresh error: {e}")


# ==========================================
# PHASE 3: WEBSOCKET EVENT LISTENERS
# ==========================================
def on_message(ws, message):
    try:
        data = json.loads(message)
        
        if data.get('type') == 'trade':
            for trade in data['data']:
                sym = trade['s']
                price = trade['p']
                vol = trade['v']
                
                # Scale incoming BATS volume to match consolidated US volume
                scaled_vol = vol * BATS_VOLUME_MULTIPLIER
                
                with data_lock:
                    if sym in market_data:
                        market_data[sym]["current_price"] = price
                        market_data[sym]["cumulative_volume"] += scaled_vol
                        market_data[sym]["total_dollar_traded"] += (price * scaled_vol)
                        
                        if price > market_data[sym]["high_of_day_price"]:
                            market_data[sym]["high_of_day_price"] = price

                        # Order Flow Tick-Rule Delta Tracking
                        prev_tick = market_data[sym]["prev_tick_price"]
                        if prev_tick > 0:
                            if price > prev_tick:
                                market_data[sym]["rolling_buy_vol"] += scaled_vol  # Ask Slap
                            elif price < prev_tick:
                                market_data[sym]["rolling_sell_vol"] += scaled_vol # Bid Hit
                        
                        market_data[sym]["prev_tick_price"] = price
                        
                        prev_close = market_data[sym]["prev_close"]
                        if prev_close > 0:
                            market_data[sym]["percent_change"] = ((price - prev_close) / prev_close) * 100

    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f"WebSocket processing error: {e}")

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### WebSocket Connection Closed ###")

def on_open(ws):
    global ws_global
    ws_global = ws
    
    with data_lock:
        symbols_to_sub = list(market_data.keys())
        
    for sym in symbols_to_sub:
        ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
    print(f"Subscribed to {len(symbols_to_sub)} streams over live Finnhub WebSocket.")


# ==========================================
# SCRIPT ENTRYPOINT
# ==========================================
if __name__ == "__main__":
    websocket.enableTrace(False)
    
    # 1. Run Initial Screener
    initial_targets = get_morning_watchlist()
    for target in initial_targets:
        register_symbol_in_memory(target)
        
    # 2. Launch Background Evaluation Thread
    eval_thread = threading.Thread(target=evaluation_loop, daemon=True)
    eval_thread.start()
    
    # 3. Launch Background Mid-Day Watchlist Refresher Thread
    refresh_thread = threading.Thread(target=watchlist_refresher_loop, daemon=True)
    refresh_thread.start()
    
    # 4. Connect Main Process to Finnhub WebSocket Loop
    while True:
        ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever(ping_interval=30, ping_timeout=10)
        print("Connection dropped! Reconnecting in 15 seconds...")
        time.sleep(15)
