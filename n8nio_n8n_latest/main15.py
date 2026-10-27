import websocket
import json
import requests
import time
import threading
import sys
import numpy as np
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
# HELPER FUNCTIONS: TIERING, RVOL & SCORING
# ==========================================
def get_mcap_tier(mcap: float) -> str:
    if not mcap or mcap <= 0:
        return "Unknown Cap"
    if mcap >= 200_000_000_000:
        return "Mega Cap (>$200B)"
    if mcap >= 10_000_000_000:
        return "Large Cap ($10B-$200B)"
    if mcap >= 2_000_000_000:
        return "Mid Cap ($2B-$10B)"
    if mcap >= 300_000_000:
        return "Small Cap ($300M-$2B)"
    return "Micro Cap (<$300M)"


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


def evaluate_adr_feasibility(upside_potential: float, adr_pct: float):
    """Checks whether the upside to HOD is realistic relative to the stock's 21-day ADR %."""
    if not adr_pct or adr_pct <= 0:
        return 0.0, "Unknown ADR"

    target_vs_adr_pct = round((upside_potential / adr_pct) * 100.0, 1)
    if target_vs_adr_pct <= 100.0:
        feasibility = "Within typical ADR"
    elif target_vs_adr_pct <= 135.0:
        feasibility = "Stretch but plausible"
    elif target_vs_adr_pct <= 180.0:
        feasibility = "Large stretch"
    else:
        feasibility = "Unrealistic (>1.8x ADR)"

    return target_vs_adr_pct, feasibility


def score_intraday_setup(
    metrics: dict,
    live_rvol: float,
    order_flow_delta_pct: float,
    intraday_clv_pct: float,
    target_vs_adr_pct: float,
    extension_pct: float,
    vwap_20d_dist_pct: float
):
    """
    Composite 0-100 Setup Score (Hybrid Model):
    Combines Order Flow Delta, RVOL, Intraday CLV, 7D/Daily Trend, and ADR Headroom,
    with penalties for unrealistic ADR targets, 20D VWAP overextension, RSI > 85, and large overnight gaps.
    """
    score = 0.0

    # Component A: Order Flow Aggression / Volatility Headroom (Max 30 pts)
    score += min(max(order_flow_delta_pct, 0.0), 30.0) * 0.6  # Up to 18 pts from Delta
    score += min(metrics.get("adr_pct", 0.0), 12.0) * 1.0     # Up to 12 pts from ADR %

    # Component B: Relative Volume / Flow Interest (Max 20 pts)
    rvol_val = live_rvol if (live_rvol and live_rvol > 0) else (metrics.get("relative_volume") or 1.0)
    score += min(rvol_val, 10.0) * 2.0

    # Component C: Intraday Closing Strength / CLV % (Max 15 pts)
    score += (max(intraday_clv_pct, 0.0) / 100.0) * 15.0

    # Component D: 7-Day Trend Consistency (Max 15 pts)
    trend_7d = metrics.get("trend_7d", "Neutral")
    if trend_7d == "Bullish":
        score += 15.0
    elif trend_7d == "Neutral":
        score += 7.5

    # Component E: Daily Moving Average Alignment (Max 20 pts)
    alignment = metrics.get("alignment_state", "MIXED")
    if alignment in ("FULL_BULLISH", "COMPRESSION"):
        score += 20.0
    elif metrics.get("macro_uptrend", False):
        score += 10.0

    # Penalty 1: Volatility Feasibility Penalty (Requires > 1.8x historical ADR)
    if target_vs_adr_pct > 180.0:
        score -= 12.0

    # Penalty 2: Overextended from 20D Institutional VWAP (> 40%) or 5D MA (> 35%)
    if vwap_20d_dist_pct > 40.0 or extension_pct >= 35.0:
        score -= 15.0

    # Penalty 3: Severe Overbought Daily RSI (> 85)
    if metrics.get("daily_rsi", 50.0) > 85.0:
        score -= 15.0

    # Penalty 4: Exhaustion Overnight Gap (> 15%)
    if abs(metrics.get("gap_pct", 0.0)) > 15.0:
        score -= 8.0

    final_score = max(0.0, min(100.0, round(score, 1)))

    if final_score >= 75.0:
        setup_class = "Top-tier"
    elif final_score >= 60.0:
        setup_class = "Strong"
    elif final_score >= 45.0:
        setup_class = "Watch"
    elif final_score >= 30.0:
        setup_class = "Low"
    else:
        setup_class = "Avoid"

    return final_score, setup_class


# ==========================================
# PHASE 1: YAHOO NATIVE API SCREENER
# ==========================================
def get_morning_watchlist():
    print("\n--- SCREENING MARKET FOR MOMENTUM TARGETS ---")
    try:
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', 60000000]),      
            EquityQuery('lt',  ['intradaymarketcap', MARKET_CAP_CEILING]), 
            EquityQuery('gt',  ['intradayprice', 1.00]),           
            EquityQuery('gte', ['percentchange', -5.0]),            
            EquityQuery('gte', ['avgdailyvol3m', 2000000])          
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
            if not sym:
                continue

            prev_close = quote.get("regularMarketPreviousClose", 0)
            open_price = quote.get("regularMarketOpen", 0)
            live_change = quote.get("regularMarketChangePercent", 0)
            day_high = quote.get("regularMarketDayHigh", 0)
            day_low = quote.get("regularMarketDayLow", 0)
            current_price = quote.get("regularMarketPrice", 0)
            day_volume = quote.get("regularMarketVolume", 0)
            fifty_day_avg = quote.get("fiftyDayAverage", 0)
            avg_vol_3m = quote.get("averageDailyVolume3Month", 0) or quote.get("avgdailyvol3m", 0)

            # 5-Tier Market Cap Classification
            raw_market_cap = quote.get("marketCap", 0)
            cap_tier = get_mcap_tier(raw_market_cap)

            # Overnight Gap % (Open vs Prev Close)
            gap_pct = ((open_price - prev_close) / prev_close * 100.0) if (prev_close and prev_close > 0 and open_price and open_price > 0) else 0.0

            relative_volume = calculate_relative_volume(day_volume, avg_vol_3m)
            if relative_volume is None:
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
                "open_price": open_price if open_price > 0 else current_price,
                "gap_pct": round(gap_pct, 2),
                "live_change": live_change,
                "day_high": day_high,
                "day_low": day_low,
                "day_volume": day_volume,
                "avg_vol_3m": avg_vol_3m,
                "market_cap_tier": cap_tier, 
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
            highs = hist_data['High'] if 'High' in hist_data else None
            lows = hist_data['Low'] if 'Low' in hist_data else None
            volumes = hist_data['Volume'] if 'Volume' in hist_data else None
                
            for target in top_40:
                sym = target["symbol"]
                target["ma_5"] = 0.0 
                target["adr_pct"] = 0.0
                target["alignment_state"] = "MIXED"
                target["vwap_20d"] = 0.0
                target["daily_rsi"] = 50.0
                target["trend_7d"] = "Neutral"
                target["green_red_7d"] = "0G / 0R"
                target["ret_7d_pct"] = 0.0
                
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
                        ema_8 = stock_closes.ewm(span=8, adjust=False).mean().iloc[-1]
                        
                        target["ma_5"] = float(ma_5)

                        # 1. Average Daily Range (ADR %) & 20-Day Institutional VWAP
                        if highs is not None and lows is not None:
                            if isinstance(highs, pd.DataFrame) and sym in highs.columns:
                                s_highs = highs[sym].dropna()
                                s_lows = lows[sym].dropna()
                            else:
                                s_highs = highs.dropna()
                                s_lows = lows.dropna()
                                
                            daily_ranges = ((s_highs.tail(21) - s_lows.tail(21)) / s_lows.tail(21)) * 100
                            target["adr_pct"] = float(daily_ranges.mean())

                            # 20D Institutional VWAP
                            if volumes is not None:
                                if isinstance(volumes, pd.DataFrame) and sym in volumes.columns:
                                    s_vols = volumes[sym].dropna()
                                else:
                                    s_vols = volumes.dropna()
                                common_idx = stock_closes.tail(20).index.intersection(s_highs.index).intersection(s_lows.index).intersection(s_vols.index)
                                if len(common_idx) >= 5:
                                    typ_p = (s_highs.loc[common_idx] + s_lows.loc[common_idx] + stock_closes.loc[common_idx]) / 3.0
                                    v_sum = s_vols.loc[common_idx].sum()
                                    if v_sum > 0:
                                        target["vwap_20d"] = float((typ_p * s_vols.loc[common_idx]).sum() / v_sum)

                        # 2. Wilder's Daily RSI (14)
                        delta = stock_closes.diff()
                        avg_gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
                        avg_loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
                        last_gain = float(avg_gain.iloc[-1])
                        last_loss = float(avg_loss.iloc[-1])
                        if last_loss == 0:
                            target["daily_rsi"] = 100.0 if last_gain > 0 else 50.0
                        else:
                            rs = last_gain / last_loss
                            target["daily_rsi"] = float(100.0 - (100.0 / (1.0 + rs)))

                        # 3. 7-Day Trend Consistency & Green/Red Day Count
                        lookback = min(8, len(stock_closes))
                        rets = stock_closes.tail(lookback).pct_change().dropna()
                        green = int((rets > 0).sum())
                        red = int((rets < 0).sum())
                        p_prior = float(stock_closes.iloc[-lookback])
                        last_c = float(stock_closes.iloc[-1])
                        ret_7d = ((last_c - p_prior) / p_prior * 100.0) if p_prior > 0 else 0.0

                        target["green_red_7d"] = f"{green}G / {red}R"
                        target["ret_7d_pct"] = round(ret_7d, 2)
                        if ret_7d > 1.5 and last_c > ema_8 and green >= 4:
                            target["trend_7d"] = "Bullish"
                        elif ret_7d < -1.5 and last_c < ema_8 and red >= 4:
                            target["trend_7d"] = "Bearish"
                        else:
                            target["trend_7d"] = "Neutral"
                        
                        # 4. Daily Moving Average Stack Alignment
                        if ma_50 > 0:
                            if ma_10 > ma_21 and ma_21 > ma_50:
                                target["alignment_state"] = "FULL_BULLISH"
                            elif ma_10 < ma_21 and ma_21 < ma_50:
                                target["alignment_state"] = "BEARISH"
                            else:
                                min_ma, max_ma = min(ma_10, ma_21, ma_50), max(ma_10, ma_21, ma_50)
                                if ((max_ma - min_ma) / min_ma) <= 0.02:
                                    target["alignment_state"] = "COMPRESSION"
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
                "open_price": target.get("open_price", seed_price),
                "gap_pct": target.get("gap_pct", 0.0),
                "adr_pct": target.get("adr_pct", 0.0),
                "vwap_20d": target.get("vwap_20d", 0.0),
                "daily_rsi": target.get("daily_rsi", 50.0),
                "trend_7d": target.get("trend_7d", "Neutral"),
                "green_red_7d": target.get("green_red_7d", "0G / 0R"),
                "ret_7d_pct": target.get("ret_7d_pct", 0.0),
                "current_price": seed_price,
                "market_cap_tier": target.get("market_cap_tier", "Unknown Cap"),
                "cumulative_volume": target.get("day_volume", 0),           
                "avg_vol_3m": target.get("avg_vol_3m", 0),
                "relative_volume": target.get("relative_volume", 1.0),
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
                "last_pullback_alert_time": 0,
                "last_breakout_alert_time": 0
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
                    
                    upside_potential = 0.0
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
                    breakout_in_cooldown = (current_time - metrics["last_breakout_alert_time"]) < ALERT_COOLDOWN_SECONDS

                    # Calculate how far we have bounced off the absolute bottom
                    distance_from_low_pct = 0.0
                    if metrics["session_low_price"] > 0:
                        distance_from_low_pct = ((current_price - metrics["session_low_price"]) / metrics["session_low_price"]) * 100

                    # --- NEW ENRICHED METRICS: CLV %, Intraday Return, 20D VWAP Dist, ADR Feasibility & Hybrid Score ---
                    day_spread = hod_price - metrics["session_low_price"]
                    intraday_clv_pct = 0.0
                    if day_spread > 0:
                        intraday_clv_pct = (((current_price - metrics["session_low_price"]) - (hod_price - current_price)) / day_spread) * 100.0

                    open_p = metrics.get("open_price", current_price)
                    intraday_return_pct = ((current_price - open_p) / open_p * 100.0) if open_p > 0 else 0.0

                    vwap_20d = metrics.get("vwap_20d", 0.0)
                    vwap_20d_dist_pct = ((current_price - vwap_20d) / vwap_20d * 100.0) if vwap_20d > 0 else 0.0

                    live_rvol = calculate_relative_volume(cum_vol, metrics.get("avg_vol_3m", 0))
                    if live_rvol is not None:
                        metrics["relative_volume"] = live_rvol
                    else:
                        live_rvol = metrics.get("relative_volume") or 1.0

                    target_vs_adr_pct, adr_feasibility = evaluate_adr_feasibility(upside_potential, metrics["adr_pct"])

                    setup_score, setup_class = score_intraday_setup(
                        metrics=metrics,
                        live_rvol=live_rvol,
                        order_flow_delta_pct=order_flow_delta_pct,
                        intraday_clv_pct=intraday_clv_pct,
                        target_vs_adr_pct=target_vs_adr_pct,
                        extension_pct=extension_pct,
                        vwap_20d_dist_pct=vwap_20d_dist_pct
                    )

                    print(f"[{sym}] +{p_change:.2f}% | Score: {setup_score:.1f} ({setup_class}) | "
                          f"Align: {alignment} | 7D: {metrics['trend_7d']} ({metrics['green_red_7d']}) | "
                          f"Delta: {order_flow_delta_pct:.1f}% | VWAPDist: {vwap_distance:.2f}% | "
                          f"CLV: {intraday_clv_pct:.1f}% | ADR: {metrics['adr_pct']:.2f}% ({adr_feasibility}) | "
                          f"Up2HOD: {upside_potential:.2f}% | Dist2HOD: {distance_to_hod_pct:.2f}% | "
                          f"Streak: {metrics['reclaim_streak']}")

                    def build_signal_payload(signal_type_str, pb_tier_str, sl_price):
                        return {
                            "symbol": sym,
                            "signal_type": signal_type_str,
                            "setup_score": setup_score,
                            "setup_class": setup_class,
                            "pullback_tier": pb_tier_str,
                            "market_cap_tier": metrics["market_cap_tier"],
                            "live_percent_change": round(p_change, 2),
                            "intraday_return_pct": round(intraday_return_pct, 2),
                            "overnight_gap_pct": round(metrics["gap_pct"], 2),
                            "last_price": current_price,
                            "intraday_vwap": round(avg_price, 2),
                            "upside_to_hod": round(upside_potential, 2),
                            "required_dynamic_target": round(dynamic_target, 2),
                            "current_vwap_distance": round(vwap_distance, 2),
                            "session_low_vwap_distance": round(session_low_vwap_dist, 2),
                            "distance_to_hod_pct": round(distance_to_hod_pct, 2),
                            "intraday_clv_pct": round(intraday_clv_pct, 2),
                            "reclaim_streak": metrics["reclaim_streak"],
                            "live_volume": cum_vol,
                            "rvol": round(live_rvol, 2) if live_rvol else 0.0,
                            "whole_dollar_convergence": is_converging,
                            "stop_loss_price": round(sl_price, 2),
                            "session_low_price": round(metrics["session_low_price"], 2),
                            "daily_alignment_state": alignment,
                            "daily_macro_uptrend": metrics["macro_uptrend"],
                            "trend_7d": metrics["trend_7d"],
                            "green_red_7d": metrics["green_red_7d"],
                            "ret_7d_pct": metrics["ret_7d_pct"],
                            "vwap_20d": round(vwap_20d, 2),
                            "vwap_20d_dist_pct": round(vwap_20d_dist_pct, 2),
                            "daily_rsi": round(metrics["daily_rsi"], 1),
                            "extension_from_5ma": round(extension_pct, 2),
                            "is_over_extended": is_over_extended,
                            "order_flow_delta_1m": round(order_flow_delta_pct, 2),
                            "historical_adr_pct": round(metrics["adr_pct"], 2),
                            "target_vs_adr_pct": target_vs_adr_pct,
                            "adr_feasibility": adr_feasibility,
                            "DateTime": timestamp_str
                        }

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
                        triggered_symbols.append(
                            build_signal_payload("VWAP_RECLAIM_CROSSOVER", "NONE", stop_loss_price)
                        )

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
                        triggered_symbols.append(
                            build_signal_payload("VWAP_CONTINUATION_BOUNCE", "NONE", stop_loss_price)
                        )

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
                        triggered_symbols.append(
                            build_signal_payload("DEEP_PULLBACK_RECLAIM", pullback_tier["name"], reversal_stop_loss_price)
                        )

                    # ==============================================================
                    # EXECUTION GATE 4: HIGH-OF-DAY (HOD) IGNITION BREAKOUT
                    # ==============================================================
                    elif (p_change >= 6.0 and
                          1.0 < vwap_distance <= 8.0 and
                          distance_to_hod_pct <= 1.5 and
                          is_bouncing and
                          metrics["reclaim_streak"] >= RECLAIM_MIN_STREAK and
                          order_flow_delta_pct >= 25.0 and
                          alignment != "BEARISH" and
                          not is_over_extended and
                          not breakout_in_cooldown):

                        metrics["last_breakout_alert_time"] = current_time
                        breakout_stop_loss = max(avg_price, current_price * 0.975)
                        triggered_symbols.append(
                            build_signal_payload("HOD_IGNITION_BREAKOUT", "NONE", breakout_stop_loss)
                        )

                metrics["price_60s_ago"] = current_price

        if triggered_symbols:
            # Sort triggered setups by Setup Score descending so the best setups appear first
            triggered_symbols = sorted(triggered_symbols, key=lambda x: x["setup_score"], reverse=True)
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
                if price <= 0 or vol <= 0:
                    continue

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
                        if price > market_data[sym]["high_of_day_price"]:
                            market_data[sym]["high_of_day_price"] = price

                        if prev_tick > 0:
                            if price > prev_tick:
                                market_data[sym]["rolling_buy_vol"] += vol  
                            elif price < prev_tick:
                                market_data[sym]["rolling_sell_vol"] += vol 
                        
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

if __name__ == "__main__":
    websocket.enableTrace(False)
    initial_targets = get_morning_watchlist()
    for target in initial_targets:
        register_symbol_in_memory(target)
        
    threading.Thread(target=evaluation_loop, daemon=True).start()
    threading.Thread(target=watchlist_refresher_loop, daemon=True).start()
    
    while True:
        ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever(ping_interval=30, ping_timeout=10)
        time.sleep(15)
