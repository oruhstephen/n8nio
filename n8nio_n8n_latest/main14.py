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

# Prevent alert spamming: 15-minute (900 seconds) cooldown per ticker
ALERT_COOLDOWN_SECONDS = 900

# Re-scan market every 30 minutes (1800 seconds) to catch mid-day breakout runners
REFRESH_INTERVAL_SECONDS = 1800

# Reject any single trade print that implies a larger % move from the previous trade
MAX_TICK_MOVE_PCT = 20.0

# Screener parameters
MARKET_CAP_CEILING = 2_000_000_000_000_000  # $2QD Ceiling
MIN_RELATIVE_VOLUME = 1.5
MAX_FLOAT_SHARES = 50_000_000
ENABLE_FLOAT_FILTER = False
DEBUG_PRINT_QUOTE_FIELDS = False

# --- DEEP PULLBACK / HOD-RECLAIM SIGNAL ---
DEEP_PULLBACK_TIERS = [
    {"name": "TIER_1_SHALLOW",  "min_depth_pct": 3.1,  "min_order_flow_delta_pct": 15.0},
    {"name": "TIER_2_MODERATE", "min_depth_pct": 7.0,  "min_order_flow_delta_pct": 25.0},
    {"name": "TIER_3_DEEP",     "min_depth_pct": 10.0, "min_order_flow_delta_pct": 35.0},
]

RECLAIM_HOD_PROXIMITY_PCT = 3.0
RECLAIM_MIN_STREAK = 2
RECLAIM_MIN_ORDER_FLOW_DELTA_PCT = 20.0

# Shared thread-safe state
data_lock = threading.Lock()
market_data = {}
ws_global = None


# ==========================================
# PHASE 1: YAHOO NATIVE API SCREENER
# ==========================================
def calculate_relative_volume(day_volume, avg_vol_3m):
    if not avg_vol_3m or avg_vol_3m <= 0:
        return None

    now_et = datetime.now(ZoneInfo("America/New_York"))
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    session_minutes = 390.0  

    if now_et <= market_open:
        elapsed_minutes = 1.0  
    elif now_et >= market_close:
        elapsed_minutes = session_minutes
    else:
        elapsed_minutes = (now_et - market_open).total_seconds() / 60.0

    expected_volume_so_far = avg_vol_3m * (elapsed_minutes / session_minutes)
    if expected_volume_so_far <= 0:
        return None

    return day_volume / expected_volume_so_far


def get_morning_watchlist():
    print("\n--- SCREENING MARKET FOR MOMENTUM TARGETS ---")
    try:
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', 60000000]),      
            EquityQuery('lt',  ['intradaymarketcap', MARKET_CAP_CEILING]), 
            EquityQuery('gt',  ['intradayprice', 1.00]),           
            EquityQuery('gte', ['percentchange', 3.0]),            
            EquityQuery('gte', ['avgdailyvol3m', 250000])          
        ])

        response = yf.screen(q, sortField='percentchange', sortAsc=False)
        quotes = response.get('quotes', [])
        qualified_symbols = []
        rel_vol_unavailable_count = 0

        if DEBUG_PRINT_QUOTE_FIELDS and quotes:
            sample = quotes[0]
            print(f"\n  [DEBUG] Raw quote keys for {sample.get('symbol', '?')} "
                  f"({len(sample)} fields):")
            for key in sorted(sample.keys()):
                print(f"    {key}: {sample[key]!r}")
            has_avg_vol_3m = "averageDailyVolume3Month" in sample
            print(f"  [DEBUG] 'averageDailyVolume3Month' present: {has_avg_vol_3m}")
            if not has_avg_vol_3m:
                close_matches = [k for k in sample.keys() if "volume" in k.lower() or "vol" in k.lower()]
                print(f"  [DEBUG] Volume-related keys found instead: {close_matches}")
            print()

        for quote in quotes:
            sym = quote.get("symbol")
            if not sym: continue

            prev_close = quote.get("regularMarketPreviousClose", 0)
            live_change = quote.get("regularMarketChangePercent", 0)
            day_high = quote.get("regularMarketDayHigh", 0)
            day_low = quote.get("regularMarketDayLow", 0)
            current_price = quote.get("regularMarketPrice", 0)
            day_volume = quote.get("regularMarketVolume", 0)
            fifty_day_avg = quote.get("fiftyDayAverage", 0)
            avg_vol_3m = quote.get("averageDailyVolume3Month", 0) or quote.get("avgdailyvol3m", 0)

            relative_volume = calculate_relative_volume(day_volume, avg_vol_3m)
            if relative_volume is None:
                # Field wasn't available for this quote - don't punish the
                # candidate for a data gap we can't confirm; let it through
                # and just flag that the filter didn't actually get applied.
                rel_vol_unavailable_count += 1
            elif relative_volume < MIN_RELATIVE_VOLUME:
                continue  # trading at/below its typical pace for this time of day - skip

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
                "day_low": day_low,
                "day_volume": day_volume,
                "seed_dollar_traded": seed_dollar_traded,
                "fifty_day_avg": fifty_day_avg,
                "macro_uptrend": macro_uptrend,
                "regularMarketPrice": current_price,
                "relative_volume": relative_volume
            })

        if rel_vol_unavailable_count:
            print(f"  [WARN] Relative volume unavailable for {rel_vol_unavailable_count} "
                  f"quote(s) - those candidates were NOT filtered on RVOL. Check "
                  f"'averageDailyVolume3Month' against a raw quote dict if this "
                  f"count is high (set DEBUG_PRINT_QUOTE_FIELDS = True).")

        if ENABLE_FLOAT_FILTER and qualified_symbols:
            candidate_pool = qualified_symbols[:60]
            float_filtered = []
            debug_shown = False
            for cand in candidate_pool:
                try:
                    info = yf.Ticker(cand["symbol"]).info
                    if DEBUG_PRINT_QUOTE_FIELDS and not debug_shown:
                        has_float = "floatShares" in info
                        print(f"  [DEBUG] 'floatShares' present in .info for "
                              f"{cand['symbol']}: {has_float} "
                              f"(value: {info.get('floatShares')!r})")
                        debug_shown = True
                    float_shares = info.get("floatShares")
                    if float_shares is None or float_shares <= MAX_FLOAT_SHARES:
                        float_filtered.append(cand)
                except Exception:
                    float_filtered.append(cand)
                time.sleep(0.1)  
            qualified_symbols = float_filtered + qualified_symbols[60:]

        top_40 = qualified_symbols[:40]
        
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
                            if ma_10 > ma_21 and ma_21 > ma_50: target["alignment_state"] = "FULL_BULLISH"
                            elif ma_10 < ma_21 and ma_21 < ma_50: target["alignment_state"] = "BEARISH"
                            else:
                                min_ma, max_ma = min(ma_10, ma_21, ma_50), max(ma_10, ma_21, ma_50)
                                if ((max_ma - min_ma) / min_ma) <= 0.02: target["alignment_state"] = "COMPRESSION"
                except Exception:
                    pass

        return top_40
    except Exception as e:
        print(f"Screener execution error: {e}")
        return []


def register_symbol_in_memory(target):
    sym = target["symbol"].replace('-', '.')
    with data_lock:
        if sym not in market_data:
            seed_price = target.get("regularMarketPrice", 0)
            day_low = target.get("day_low", 0)
            seed_session_low_price = day_low if (day_low and day_low > 0 and day_low <= seed_price) else seed_price

            market_data[sym] = {
                "prev_close": target.get("prev_close", 0),
                "current_price": seed_price,
                "cumulative_volume": target.get("day_volume", 0),           
                "total_dollar_traded": target.get("seed_dollar_traded", 0), 
                "percent_change": target.get("live_change", 0),
                "high_of_day_price": target.get("day_high", 0),             
                "price_60s_ago": seed_price,
                "ma_5": target.get("ma_5", 0.0), 
                "alignment_state": target.get("alignment_state", "MIXED"),
                "macro_uptrend": target.get("macro_uptrend", False),
                "prev_tick_price": seed_price,
                "rolling_buy_vol": 0,
                "rolling_sell_vol": 0,
                "last_alert_time": 0,  
                "dipped_below_vwap": False,  
                "last_reclaim_alert_time": 0,  
                "session_low_price": seed_session_low_price,
                "session_low_vwap_distance": None,  
                "reclaim_streak": 0,
                "last_pullback_alert_time": 0  
            }


# ==========================================
# PHASE 2: INDEPENDENT BACKGROUND THREADS
# ==========================================
def evaluation_loop():
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
                
                buy_v = metrics["rolling_buy_vol"]
                sell_v = metrics["rolling_sell_vol"]
                total_tick_vol = buy_v + sell_v
                
                order_flow_delta_pct = 0.0
                if total_tick_vol > 0:
                    order_flow_delta_pct = ((buy_v - sell_v) / total_tick_vol) * 100
                    
                metrics["rolling_buy_vol"] = 0
                metrics["rolling_sell_vol"] = 0
                
                if cum_vol > 50000 and current_price > 0: 
                    avg_price = metrics["total_dollar_traded"] / cum_vol
                    vwap_distance = ((current_price - avg_price) / avg_price) * 100
                    
                    # Under-VWAP flag checks
                    if -2.5 <= vwap_distance <= -0.2:
                        metrics["dipped_below_vwap"] = True
                    if vwap_distance < -3.0:
                        metrics["dipped_below_vwap"] = False
                    
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

                    # Strictly requires an upward price tick to prevent flat consolidation fakes
                    is_bouncing = current_price > price_60s_ago  
                    stop_loss_price = avg_price * 0.975
                    
                    extension_pct = 0.0
                    is_over_extended = False
                    if ma_5 > 0:
                        extension_pct = ((current_price - ma_5) / ma_5) * 100
                        if extension_pct >= 50.0:
                            is_over_extended = True

                    session_low_vwap_dist = metrics["session_low_vwap_distance"]
                    if session_low_vwap_dist is None or vwap_distance < session_low_vwap_dist:
                        metrics["session_low_vwap_distance"] = vwap_distance
                        session_low_vwap_dist = vwap_distance
                    if current_price < metrics["session_low_price"]:
                        metrics["session_low_price"] = current_price

                    if is_bouncing:
                        metrics["reclaim_streak"] += 1
                    else:
                        metrics["reclaim_streak"] = 0

                    session_low_depth = abs(session_low_vwap_dist) if session_low_vwap_dist < 0 else 0.0
                    pullback_tier = None
                    for tier in DEEP_PULLBACK_TIERS:
                        if session_low_depth >= tier["min_depth_pct"]:
                            pullback_tier = tier

                    distance_to_hod_pct = ((hod_price - current_price) / current_price * 100) if current_price > 0 else 0.0
                    distance_to_hod_pct = max(0.0, distance_to_hod_pct)
                    is_reclaiming_hod = (distance_to_hod_pct <= RECLAIM_HOD_PROXIMITY_PCT)

                    # Cooldown state checks
                    in_cooldown = (current_time - metrics["last_alert_time"]) < ALERT_COOLDOWN_SECONDS
                    pullback_in_cooldown = (current_time - metrics["last_pullback_alert_time"]) < ALERT_COOLDOWN_SECONDS
                    reclaim_in_cooldown = (current_time - metrics["last_reclaim_alert_time"]) < ALERT_COOLDOWN_SECONDS

                    # Calculate how far we have bounced off the absolute bottom
                    distance_from_low_pct = 0.0
                    if metrics["session_low_price"] > 0:
                        distance_from_low_pct = ((current_price - metrics["session_low_price"]) / metrics["session_low_price"]) * 100

                    

                    print(f"[{sym}] +{p_change:.2f}% | Align: {alignment} | Ext5MA: +{extension_pct:.1f}% | "
                          f"Converge: {is_converging} | Danger: {is_over_extended} | "
                          f"Delta: {order_flow_delta_pct:.1f}% | VWAPDist: {vwap_distance:.2f}% | "
                          f"MovingUp: {is_bouncing} | DippedFlag: {metrics['dipped_below_vwap']} | "
                          f"SessionLowVWAP: {session_low_vwap_dist:.2f}% | "
                          f"Up2HOD: {upside_potential:.2f}% | T.Need: {dynamic_target:.2f}% | "
                          f"Dist2HOD: {distance_to_hod_pct:.2f}% | Streak: {metrics['reclaim_streak']}")

                    # ==============================================================
                    # EXECUTION GATE 1: UNDER-VWAP RECLAIM
                    # ==============================================================
                    if (metrics["dipped_below_vwap"] and
                        -0.5 <= vwap_distance <= 0.5 and
                        is_bouncing and
                        metrics["reclaim_streak"] >= RECLAIM_MIN_STREAK and
                        order_flow_delta_pct >= RECLAIM_MIN_ORDER_FLOW_DELTA_PCT and
                        not is_over_extended and
                        not reclaim_in_cooldown):

                        metrics["last_reclaim_alert_time"] = current_time
                        metrics["dipped_below_vwap"] = False  

                        triggered_symbols.append({
                            "symbol": sym,
                            "signal_type": "VWAP_RECLAIM_CROSSOVER",
                            "pullback_tier": "NONE",
                            "live_percent_change": round(p_change, 2),
                            "last_price": current_price,
                            "intraday_vwap": round(avg_price, 2),
                            "upside_to_hod": round(upside_potential, 2),
                            "required_dynamic_target": round(dynamic_target, 2),
                            "current_vwap_distance": round(vwap_distance, 2),
                            "session_low_vwap_distance": round(session_low_vwap_dist, 2),
                            "distance_to_hod_pct": round(distance_to_hod_pct, 2),
                            "reclaim_streak": metrics["reclaim_streak"],
                            "live_volume": cum_vol,
                            "whole_dollar_convergence": is_converging,
                            "stop_loss_price": round(stop_loss_price, 2),
                            "session_low_price": round(metrics["session_low_price"], 2),
                            "daily_alignment_state": alignment,
                            "extension_from_5ma": round(extension_pct, 2),
                            "is_over_extended": is_over_extended,
                            "DateTime": timestamp_str,
                            "daily_macro_uptrend": metrics["macro_uptrend"],
                            "order_flow_delta_1m": round(order_flow_delta_pct, 2)
                        })

                    # ==============================================================
                    # EXECUTION GATE 2: VWAP CONTINUATION (STANDARD HUG)
                    # ==============================================================
                    elif (p_change >= 8.0 and 
                        upside_potential >= dynamic_target and 
                        -0.5 <= vwap_distance <= 1.0 and 
                        is_bouncing and 
                        not is_over_extended and
                        not in_cooldown):
                        
                        metrics["last_alert_time"] = current_time  
                        triggered_symbols.append({
                            "symbol": sym,
                            "signal_type": "VWAP_CONTINUATION_BOUNCE",
                            "pullback_tier": "NONE",
                            "live_percent_change": round(p_change, 2),
                            "last_price": current_price,
                            "intraday_vwap": round(avg_price, 2),
                            "upside_to_hod": round(upside_potential, 2),
                            "required_dynamic_target": round(dynamic_target, 2),
                            "current_vwap_distance": round(vwap_distance, 2),
                            "session_low_vwap_distance": round(session_low_vwap_dist, 2),
                            "distance_to_hod_pct": round(distance_to_hod_pct, 2),
                            "reclaim_streak": metrics["reclaim_streak"],
                            "live_volume": cum_vol,
                            "whole_dollar_convergence": is_converging,
                            "stop_loss_price": round(stop_loss_price, 2),
                            "session_low_price": round(metrics["session_low_price"], 2),
                            "daily_alignment_state": alignment,
                            "extension_from_5ma": round(extension_pct, 2),
                            "is_over_extended": is_over_extended,
                            "DateTime": timestamp_str,
                            "daily_macro_uptrend": metrics["macro_uptrend"],
                            "order_flow_delta_1m": round(order_flow_delta_pct, 2)
                        })

                    # ==============================================================
                    # EXECUTION GATE 3: DEEP PULLBACK REVERSAL (THE V-BOTTOM CATCH)
                    # ==============================================================
                    elif (pullback_tier is not None and
                          vwap_distance < 0 and  # Must still be below VWAP (catching the move early)
                          distance_from_low_pct >= 1.0 and  # Must have bounced at least 1% off the exact bottom to confirm the pivot
                          metrics["reclaim_streak"] >= RECLAIM_MIN_STREAK and
                          order_flow_delta_pct >= pullback_tier["min_order_flow_delta_pct"] and
                          not pullback_in_cooldown):

                        metrics["last_pullback_alert_time"] = current_time
                        reversal_stop_loss_price = metrics["session_low_price"] * 0.98

                        triggered_symbols.append({
                            "symbol": sym,
                            "signal_type": "DEEP_PULLBACK_RECLAIM",
                            "pullback_tier": pullback_tier["name"],
                            "live_percent_change": round(p_change, 2),
                            "last_price": current_price,
                            "intraday_vwap": round(avg_price, 2),
                            "upside_to_hod": round(upside_potential, 2),
                            "required_dynamic_target": round(dynamic_target, 2),
                            "current_vwap_distance": round(vwap_distance, 2),
                            "session_low_vwap_distance": round(session_low_vwap_dist, 2),
                            "distance_to_hod_pct": round(distance_to_hod_pct, 2),
                            "reclaim_streak": metrics["reclaim_streak"],
                            "live_volume": cum_vol,
                            "whole_dollar_convergence": is_converging,
                            "stop_loss_price": round(reversal_stop_loss_price, 2),
                            "session_low_price": round(metrics["session_low_price"], 2),
                            "daily_alignment_state": alignment,
                            "extension_from_5ma": round(extension_pct, 2),
                            "is_over_extended": is_over_extended,
                            "DateTime": timestamp_str,
                            "daily_macro_uptrend": metrics["macro_uptrend"],
                            "order_flow_delta_1m": round(order_flow_delta_pct, 2)
                        })

                metrics["price_60s_ago"] = current_price

        if triggered_symbols:
            print(f"\n>>> FIRING N8N WEBHOOK! {len(triggered_symbols)} stocks triggered algorithmic execution! <<<")
            payload = {
                "scan_type": "multi_signal_scan",
                "timestamp": current_time,
                "top_ranked_symbols": triggered_symbols
            }
            threading.Thread(target=requests.post, args=(N8N_WEBHOOK_URL,), kwargs={'json': payload}).start()

def watchlist_refresher_loop():
    print("--- [THREAD] Mid-Day Watchlist Refresher Loop Started ---")
    while True:
        time.sleep(REFRESH_INTERVAL_SECONDS)
        try:
            latest_targets = get_morning_watchlist()
            new_symbols = []
            for target in latest_targets:
                sym = target["symbol"].replace('-', '.')
                with data_lock:
                    exists = sym in market_data
                if not exists:
                    register_symbol_in_memory(target)
                    new_symbols.append(sym)
            if new_symbols and ws_global and ws_global.sock and ws_global.sock.connected:
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
                if price <= 0 or vol <= 0: continue

                with data_lock:
                    if sym in market_data:
                        prev_tick = market_data[sym]["prev_tick_price"]
                        if prev_tick > 0:
                            tick_move_pct = abs(price - prev_tick) / prev_tick * 100
                            if tick_move_pct > MAX_TICK_MOVE_PCT:
                                print(f"  [ANOMALY] {sym} rejected tick ${price:.2f} "
                                      f"({tick_move_pct:.1f}% vs prev ${prev_tick:.2f}) - skipped")
                                continue

                        market_data[sym]["current_price"] = price
                        market_data[sym]["cumulative_volume"] += vol
                        market_data[sym]["total_dollar_traded"] += (price * vol)
                        if price > market_data[sym]["high_of_day_price"]: market_data[sym]["high_of_day_price"] = price

                        if prev_tick > 0:
                            if price > prev_tick: market_data[sym]["rolling_buy_vol"] += vol  
                            elif price < prev_tick: market_data[sym]["rolling_sell_vol"] += vol 
                        
                        market_data[sym]["prev_tick_price"] = price
                        prev_close = market_data[sym]["prev_close"]
                        if prev_close > 0: market_data[sym]["percent_change"] = ((price - prev_close) / prev_close) * 100
    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f"WebSocket processing error: {e}")

def on_error(ws, error): print(f"WebSocket Error: {error}")
def on_close(ws, close_status_code, close_msg): print("### WebSocket Connection Closed ###")

def on_open(ws):
    global ws_global
    ws_global = ws
    with data_lock:
        symbols_to_sub = list(market_data.keys())
    for sym in symbols_to_sub:
        ws.send(json.dumps({"type": "subscribe", "symbol": sym}))

if __name__ == "__main__":
    websocket.enableTrace(False)
    initial_targets = get_morning_watchlist()
    for target in initial_targets: register_symbol_in_memory(target)
        
    threading.Thread(target=evaluation_loop, daemon=True).start()
    threading.Thread(target=watchlist_refresher_loop, daemon=True).start()
    
    while True:
        ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}", on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
        ws.run_forever(ping_interval=30, ping_timeout=10)
        time.sleep(15)
