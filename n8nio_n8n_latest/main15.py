import websocket
import json
import requests
import time
import threading
import sys
import warnings  # <--- ADD
import numpy as np
import pandas as pd
import yfinance as yf
from yfinance import EquityQuery
from datetime import datetime
from zoneinfo import ZoneInfo

# Silence internal yfinance / pandas deprecation warnings
warnings.filterwarnings('ignore')  # <--- ADD

# ==========================================
# CONFIGURATION & INSTITUTIONAL PARAMETERS
# ==========================================
FINNHUB_TOKEN = "d86fpu1r01qgiu458c80d86fpu1r01qgiu458c8g"
N8N_WEBHOOK_URL = "https://go90ng-n8n.eq7icp.easypanel.host/webhook/bcca44dc-8944-41a2-8d96-3c5eb1f159e9"

# Prevent alert spamming: 15-minute (900 seconds) cooldown per ticker
ALERT_COOLDOWN_SECONDS = 900

# Re-scan market every 30 minutes (1800 seconds) to catch mid-day breakout runners
REFRESH_INTERVAL_SECONDS = 1800

# Reject any single trade print that implies an unrealistic jump from the previous tick
MAX_TICK_MOVE_PCT = 20.0

# Screener parameters
MARKET_CAP_CEILING = 2_000_000_000_000_000  # $2QD Ceiling
MIN_RELATIVE_VOLUME = 1.2                   # Permissive floor to allow recovering pullbacks
MIN_CHANGE_PCT = -10.0                      # Captures deep pullbacks and recovering dip setups
MAX_WATCHLIST_SYMBOLS = 40                  # Sliced to 40 for Finnhub WebSocket cap & sub-ms loop latency
MAX_FLOAT_SHARES = 50_000_000
ENABLE_FLOAT_FILTER = False
DEBUG_PRINT_QUOTE_FIELDS = False

# Maximum duration (in seconds) an under-VWAP dip remains valid for a reclaim (30 mins)
MAX_DIP_AGE_SECONDS = 1800

# --- DEEP PULLBACK / HOD-RECLAIM SIGNAL ---
DEEP_PULLBACK_TIERS = [
    {"name": "TIER_1_SHALLOW",  "min_depth_pct": 3.1,  "min_order_flow_delta_pct": 15.0},
    {"name": "TIER_2_MODERATE", "min_depth_pct": 7.0,  "min_order_flow_delta_pct": 25.0},
    {"name": "TIER_3_DEEP",     "min_depth_pct": 10.0, "min_order_flow_delta_pct": 35.0},
]

RECLAIM_HOD_PROXIMITY_PCT = 3.0
RECLAIM_MIN_STREAK = 2
RECLAIM_MIN_ORDER_FLOW_DELTA_PCT = 20.0

# Re-entrant Lock prevents self-deadlock when the refresher calls register_symbol_in_memory
data_lock = threading.RLock()
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


def score_morning_setup(target: dict) -> tuple[float, str]:
    """
    Dedicated Pre-Market Structural Scoring (0-100 scale).
    Scores based strictly on verifiable historical data before live trading prints exist.
    """
    score = 0.0

    # 1. Historical Volatility Runway (Max 30 pts)
    score += min(target.get("adr_pct", 0.0), 12.0) * 2.5

    # 2. Early Relative Volume Interest (Max 25 pts)
    rvol = target.get("relative_volume") or 1.0
    score += min(rvol, 10.0) * 2.5

    # 3. 7-Day Trend Consistency (Max 20 pts)
    trend_7d = target.get("trend_7d", "Neutral")
    if trend_7d == "Bullish":
        score += 20.0
    elif trend_7d == "Neutral":
        score += 10.0

    # 4. Moving Average Alignment (Max 25 pts)
    alignment = target.get("alignment_state", "MIXED")
    if alignment == "FULL_BULLISH":
        score += 25.0
    elif alignment == "COMPRESSION":
        score += 20.0
    elif target.get("macro_uptrend", False):
        score += 10.0

    # --- Structural Penalties ---
    c_price = target.get("regularMarketPrice", 0)
    ma_5 = target.get("ma_5", 0)
    if ma_5 > 0 and ((c_price - ma_5) / ma_5 * 100.0) >= 35.0:
        score -= 15.0

    if target.get("daily_rsi", 50.0) > 85.0:
        score -= 15.0

    if abs(target.get("gap_pct", 0.0)) > 15.0:
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


def score_intraday_setup(
    metrics: dict,
    live_rvol: float,
    order_flow_delta_pct: float,
    intraday_clv_pct: float,
    target_vs_adr_pct: float,
    extension_pct: float,
    vwap_20d_dist_pct: float
) -> tuple[float, str]:
    score = 0.0

    # Component A: Order Flow Aggression / Volatility Headroom (Max 30 pts)
    score += min(max(order_flow_delta_pct, 0.0), 30.0) * 0.6
    score += min(metrics.get("adr_pct", 0.0), 12.0) * 1.0

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

    # Penalties
    if target_vs_adr_pct > 180.0:
        score -= 12.0
    if vwap_20d_dist_pct > 40.0 or extension_pct >= 35.0:
        score -= 15.0
    if metrics.get("daily_rsi", 50.0) > 85.0:
        score -= 15.0
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
# PHASE 1: SCREENER (HARVEST 500 -> SHORTLIST 100 -> SCORE -> SLICE 40)
# ==========================================
def get_morning_watchlist():
    print("\n--- SCREENING MARKET: HARVEST 500 -> SHORTLIST 100 -> SCORE ALL -> SLICE 40 ---")
    try:
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', 60_000_000]),      
            EquityQuery('lt',  ['intradaymarketcap', MARKET_CAP_CEILING]), 
            EquityQuery('gt',  ['intradayprice', 1.00]),           
            EquityQuery('gte', ['percentchange', MIN_CHANGE_PCT]),  # >= -10.0%
            EquityQuery('gte', ['avgdailyvol3m', 1_000_000])          
        ])

        quotes = []
        for offset in (0, 250):
            try:
                resp = yf.screen(q, sortField='dayvolume', sortAsc=False, size=250, offset=offset)
                batch = resp.get('quotes', [])
                quotes.extend(batch)
                if len(batch) < 250:
                    break
            except Exception as e:
                print(f"  [WARN] Screen failed at offset {offset}: {e}")

        print(f"Harvested {len(quotes)} raw quotes across 2 pages.")

        harvest_pool = []
        rel_vol_unavailable_count = 0

        for quote in quotes:
            raw_sym = quote.get("symbol")
            if not raw_sym:
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

            raw_market_cap = quote.get("marketCap", 0)
            cap_tier = get_mcap_tier(raw_market_cap)

            gap_pct = ((open_price - prev_close) / prev_close * 100.0) if (prev_close and prev_close > 0 and open_price and open_price > 0) else 0.0

            relative_volume = calculate_relative_volume(day_volume, avg_vol_3m)

            if relative_volume is not None and relative_volume < MIN_RELATIVE_VOLUME:
                continue
            elif relative_volume is None:
                rel_vol_unavailable_count += 1

            macro_uptrend = (current_price > fifty_day_avg and fifty_day_avg > 0)
            typical_price = current_price
            if (day_high + day_low + current_price) > 0:
                typical_price = (day_high + day_low + current_price) / 3.0
                
            seed_dollar_traded = typical_price * day_volume

            range_span = day_high - day_low
            intraday_location = ((current_price - day_low) / range_span) if range_span > 0 else 0.5
            rvol_mult = relative_volume if relative_volume is not None else 1.0
            activity_score = rvol_mult * (0.5 + intraday_location)

            harvest_pool.append({
                "symbol": raw_sym,
                "finnhub_symbol": raw_sym.replace('-', '.'),
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
                "relative_volume": relative_volume,
                "activity_score": activity_score
            })

        if rel_vol_unavailable_count:
            print(f"  [WARN] RVOL data unavailable for {rel_vol_unavailable_count} candidate(s); allowed through as fail-open.")

        harvest_pool = sorted(harvest_pool, key=lambda x: x["activity_score"], reverse=True)
        candidate_pool = harvest_pool[:100]
        sym_list = [t["symbol"] for t in candidate_pool]
        if not sym_list:
            return []

        print(f"Downloading 4-month historical metrics for {len(sym_list)} shortlisted candidates...")
        hist_data = yf.download(sym_list, period="4mo", progress=False)
        closes = hist_data['Close'] if 'Close' in hist_data else hist_data
        opens = hist_data['Open'] if 'Open' in hist_data else None
        highs = hist_data['High'] if 'High' in hist_data else None
        lows = hist_data['Low'] if 'Low' in hist_data else None
        volumes = hist_data['Volume'] if 'Volume' in hist_data else None

        today_et = datetime.now(ZoneInfo("America/New_York")).date()

        # Helper: Drops today's forming candle once with timezone-aware parsing
        def drop_today(s):
            if s is None or not len(s):
                return s
            d = pd.to_datetime(s.index[-1])
            if getattr(d, "tzinfo", None) is not None:
                d_date = d.tz_convert("America/New_York").date()
            else:
                d_date = d.date()
            return s.iloc[:-1] if d_date == today_et else s

        fully_scored_candidates = []
        for target in candidate_pool:
            sym = target["symbol"]
            target["ma_5"] = 0.0 
            target["adr_pct"] = 0.0
            target["alignment_state"] = "MIXED"
            target["vwap_20d"] = 0.0
            target["daily_rsi"] = 50.0
            target["trend_7d"] = "Neutral"
            target["green_red_7d"] = "0G / 0R"
            target["ret_7d_pct"] = 0.0
            target["prev_day_trend"] = "UNKNOWN"
            target["prev_day_color"] = "UNKNOWN"
            target["prev_day_return_pct"] = 0.0
            target["prev_day_candle"] = "UNKNOWN"
            
            try:
                stock_closes, stock_opens, s_highs, s_lows, s_vols = None, None, None, None, None

                # Hardened MultiIndex vs Single-Stock Series Extraction
                if isinstance(closes, pd.DataFrame):
                    if sym in closes.columns:
                        stock_closes = closes[sym].dropna()
                elif isinstance(closes, pd.Series):
                    if getattr(closes, "name", None) == sym or len(candidate_pool) == 1:
                        stock_closes = closes.dropna()

                if stock_closes is None or len(stock_closes) < 50:
                    continue

                if opens is not None:
                    if isinstance(opens, pd.DataFrame) and sym in opens.columns:
                        stock_opens = opens[sym].reindex(stock_closes.index).ffill()
                    elif isinstance(opens, pd.Series) and (getattr(opens, "name", None) == sym or len(candidate_pool) == 1):
                        stock_opens = opens.reindex(stock_closes.index).ffill()

                if highs is not None:
                    if isinstance(highs, pd.DataFrame) and sym in highs.columns:
                        s_highs = highs[sym].reindex(stock_closes.index).ffill()
                    elif isinstance(highs, pd.Series) and (getattr(highs, "name", None) == sym or len(candidate_pool) == 1):
                        s_highs = highs.reindex(stock_closes.index).ffill()

                if lows is not None:
                    if isinstance(lows, pd.DataFrame) and sym in lows.columns:
                        s_lows = lows[sym].reindex(stock_closes.index).ffill()
                    elif isinstance(lows, pd.Series) and (getattr(lows, "name", None) == sym or len(candidate_pool) == 1):
                        s_lows = lows.reindex(stock_closes.index).ffill()

                if volumes is not None:
                    if isinstance(volumes, pd.DataFrame) and sym in volumes.columns:
                        s_vols = volumes[sym].reindex(stock_closes.index).fillna(0)
                    elif isinstance(volumes, pd.Series) and (getattr(volumes, "name", None) == sym or len(candidate_pool) == 1):
                        s_vols = volumes.reindex(stock_closes.index).fillna(0)

                # Trim forming bar across all series uniformly
                stock_closes, stock_opens, s_highs, s_lows, s_vols = map(
                    drop_today, (stock_closes, stock_opens, s_highs, s_lows, s_vols)
                )

                if stock_closes is None or len(stock_closes) < 50:
                    continue

                # Moving averages on completed historical bars
                ma_5 = stock_closes.tail(5).mean()
                ma_10 = stock_closes.tail(10).mean()
                ma_21 = stock_closes.tail(21).mean()
                ma_50 = stock_closes.tail(50).mean()
                ema_8 = stock_closes.ewm(span=8, adjust=False).mean().iloc[-1]
                target["ma_5"] = float(ma_5)

                # Previous day metrics: index -1 is always yesterday, -2 is day before yesterday
                p_day_close = float(stock_closes.iloc[-1])
                p_day_prior_close = float(stock_closes.iloc[-2])
                p_day_open = float(stock_opens.iloc[-1]) if stock_opens is not None else p_day_close

                prev_ret = ((p_day_close - p_day_prior_close) / p_day_prior_close * 100.0) if p_day_prior_close > 0 else 0.0
                prev_color = "GREEN" if prev_ret >= 0 else "RED"
                candle_color = "BULLISH" if p_day_close >= p_day_open else "BEARISH"

                target["prev_day_return_pct"] = round(prev_ret, 2)
                target["prev_day_color"] = prev_color
                target["prev_day_candle"] = candle_color
                target["prev_day_trend"] = f"{prev_color} ({prev_ret:+.2f}%)"

                # ADR % & 20-Day VWAP
                if s_highs is not None and s_lows is not None:
                    daily_ranges = ((s_highs.tail(21) - s_lows.tail(21)) / s_lows.tail(21)) * 100
                    target["adr_pct"] = float(daily_ranges.mean())

                    if s_vols is not None:
                        common_idx = stock_closes.tail(20).index
                        typ_p = (s_highs.loc[common_idx] + s_lows.loc[common_idx] + stock_closes.loc[common_idx]) / 3.0
                        v_sum = s_vols.loc[common_idx].sum()
                        if v_sum > 0:
                            target["vwap_20d"] = float((typ_p * s_vols.loc[common_idx]).sum() / v_sum)

                # Wilder's Daily RSI (14)
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

                # 7-Day Trend Consistency & G/R Count
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
                
                # MA Alignment Stack
                if ma_50 > 0:
                    if ma_10 > ma_21 and ma_21 > ma_50:
                        target["alignment_state"] = "FULL_BULLISH"
                    elif ma_10 < ma_21 and ma_21 < ma_50:
                        target["alignment_state"] = "BEARISH"
                    else:
                        min_ma, max_ma = min(ma_10, ma_21, ma_50), max(ma_10, ma_21, ma_50)
                        if ((max_ma - min_ma) / min_ma) <= 0.02:
                            target["alignment_state"] = "COMPRESSION"

            except Exception as e:
                print(f"  [WARN] Failed calculating history metrics for {sym}: {e}")
                continue

            # Alignment Filter: Red/pullback names (< 3.0%) must have FULL_BULLISH or COMPRESSION structure
            is_recovering_pullback = target["live_change"] < 3.0
            if is_recovering_pullback and target["alignment_state"] not in ("FULL_BULLISH", "COMPRESSION"):
                continue
            elif target["alignment_state"] == "BEARISH":
                continue

            pre_score, pre_class = score_morning_setup(target)
            target["morning_setup_score"] = pre_score
            target["morning_setup_class"] = pre_class

            fully_scored_candidates.append(target)

        # Slice after scoring & filtering
        fully_scored_candidates = sorted(
            fully_scored_candidates, 
            key=lambda x: x["morning_setup_score"], 
            reverse=True
        )
        top_40 = fully_scored_candidates[:MAX_WATCHLIST_SYMBOLS]
        
        print(f"Qualified & Scored {len(fully_scored_candidates)} aligned targets. Selected top {len(top_40)} for WebSocket.")
        if top_40:
            print(f"  [TOP PICK] {top_40[0]['finnhub_symbol']} | Score: {top_40[0]['morning_setup_score']:.1f} ({top_40[0]['morning_setup_class']}) | "
                  f"PrevDay: {top_40[0]['prev_day_trend']} | Align: {top_40[0]['alignment_state']} | ADR: {top_40[0]['adr_pct']:.2f}% | RVOL: {top_40[0]['relative_volume']}")

        return top_40

    except Exception as e:
        print(f"Screener execution error: {e}")
        return []


def register_symbol_in_memory(target):
    sym = target["finnhub_symbol"]
    with data_lock:
        if sym not in market_data:
            seed_price = target.get("regularMarketPrice", 0)
            day_low = target.get("day_low", 0)
            seed_session_low_price = day_low if (day_low and day_low > 0 and day_low <= seed_price) else seed_price

            market_data[sym] = {
                "prev_close": target.get("prev_close", 0),
                "open_price": target.get("open_price", seed_price),
                "gap_pct": target.get("gap_pct", 0.0),
                "prev_day_trend": target.get("prev_day_trend", "UNKNOWN"),
                "prev_day_color": target.get("prev_day_color", "UNKNOWN"),
                "prev_day_return_pct": target.get("prev_day_return_pct", 0.0),
                "prev_day_candle": target.get("prev_day_candle", "UNKNOWN"),
                "adr_pct": target.get("adr_pct", 0.0),
                "vwap_20d": target.get("vwap_20d", 0.0),
                "daily_rsi": target.get("daily_rsi", 50.0),
                "trend_7d": target.get("trend_7d", "Neutral"),
                "green_red_7d": target.get("green_red_7d", "0G / 0R"),
                "ret_7d_pct": target.get("ret_7d_pct", 0.0),
                "morning_setup_score": target.get("morning_setup_score", 0.0),
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
                "dipped_below_vwap_time": 0,
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
        log_entries = []

        uk_time = datetime.now(ZoneInfo("Europe/London"))
        timestamp_str = uk_time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- {timestamp_str} | 60-SECOND ALGORITHMIC VWAP SCANS (RANKED BY SCORE) ---")
        
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
                    
                    # Under-VWAP flag checks with Stale Expiration
                    if -2.5 <= vwap_distance <= -0.2:
                        metrics["dipped_below_vwap"] = True
                        metrics["dipped_below_vwap_time"] = current_time

                    # Clear if price crashes through floor, extends too far above VWAP, or exceeds 30-min window
                    if (vwap_distance < -3.0 or 
                        vwap_distance > 1.5 or 
                        (current_time - metrics["dipped_below_vwap_time"]) > MAX_DIP_AGE_SECONDS):
                        metrics["dipped_below_vwap"] = False
                    
                    upside_potential = 0.0
                    if avg_price > 0:
                        upside_potential = ((hod_price - avg_price) / avg_price) * 100

                    dynamic_target = min(15.0, max(5.0, abs(p_change) * 0.45))
                    nearest_whole_dollar = round(current_price)
                    is_converging = False
                    if current_price > 2.00:
                        cents_away = abs(current_price - nearest_whole_dollar)
                        if cents_away <= 0.05:
                            is_converging = True

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

                    in_cooldown = (current_time - metrics["last_alert_time"]) < ALERT_COOLDOWN_SECONDS
                    pullback_in_cooldown = (current_time - metrics["last_pullback_alert_time"]) < ALERT_COOLDOWN_SECONDS
                    reclaim_in_cooldown = (current_time - metrics["last_reclaim_alert_time"]) < ALERT_COOLDOWN_SECONDS
                    breakout_in_cooldown = (current_time - metrics["last_breakout_alert_time"]) < ALERT_COOLDOWN_SECONDS

                    distance_from_low_pct = 0.0
                    if metrics["session_low_price"] > 0:
                        distance_from_low_pct = ((current_price - metrics["session_low_price"]) / metrics["session_low_price"]) * 100

                    day_spread = hod_price - metrics["session_low_price"]
                    intraday_clv_pct = 0.0
                    if day_spread > 0:
                        intraday_clv_pct = (((current_price - metrics["session_low_price"]) - (hod_price - current_price)) / day_spread) * 100.0

                    open_p = metrics.get("open_price", current_price)
                    intraday_return_pct = ((current_price - open_p) / open_p * 100.0) if open_p > 0 else 0.0

                    vwap_20d = metrics.get("vwap_20d", 0.0)
                    vwap_20d_dist_pct = ((current_price - vwap_20d) / vwap_20d * 100.0) if vwap_20d > 0 else 0.0

                    # True Fail-Open RVOL Check
                    raw_rvol = calculate_relative_volume(cum_vol, metrics.get("avg_vol_3m", 0))
                    rvol_ok = (raw_rvol is None) or (raw_rvol >= 1.2)
                    live_rvol = raw_rvol if raw_rvol is not None else (metrics.get("relative_volume") or 1.0)
                    metrics["relative_volume"] = live_rvol

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

                    # Store for score-sorted console display
                    formatted_line = (
                        f"[{sym:<5}] {p_change:>+6.2f}% | Score: {setup_score:>4.1f} ({setup_class:<8}) | "
                        f"PrevDay: {metrics['prev_day_trend']:<14} | Align: {alignment:<12} | "
                        f"7D: {metrics['trend_7d']:<7} ({metrics['green_red_7d']:<7}) | "
                        f"Delta: {order_flow_delta_pct:>+5.1f}% | VWAPDist: {vwap_distance:>+5.2f}% | "
                        f"CLV: {intraday_clv_pct:>+5.1f}% | ADR: {metrics['adr_pct']:>4.2f}% ({adr_feasibility:<18}) | "
                        f"Up2HOD: {upside_potential:>5.2f}% | Streak: {metrics['reclaim_streak']}"
                    )
                    log_entries.append((setup_score, formatted_line))

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
                            "prev_day_trend": metrics["prev_day_trend"],
                            "prev_day_color": metrics["prev_day_color"],
                            "prev_day_return_pct": metrics["prev_day_return_pct"],
                            "prev_day_candle": metrics["prev_day_candle"],
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
                    # Required: Structure must NOT be bearish
                    # Upper bound widened to 1.0% to prevent missing fast-moving reclaims
                    # ==============================================================
                    if (metrics["dipped_below_vwap"] and
                        alignment in ("FULL_BULLISH", "COMPRESSION") and  
                        -0.5 <= vwap_distance <= 1.0 and
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
                    # Required: Volume confirmation + structural alignment
                    # ==============================================================
                    elif (pullback_tier is not None and
                          alignment in ("FULL_BULLISH", "COMPRESSION") and  
                          cum_vol >= 100000 and rvol_ok and
                          vwap_distance < 0 and 
                          distance_from_low_pct >= 1.0 and 
                          metrics["reclaim_streak"] >= RECLAIM_MIN_STREAK and
                          order_flow_delta_pct >= pullback_tier["min_order_flow_delta_pct"] and
                          not pullback_in_cooldown):

                        metrics["last_pullback_alert_time"] = current_time
                        base_low = metrics["session_low_price"] if metrics["session_low_price"] > 0 else current_price
                        reversal_sl_buffer = max(0.04, base_low * 0.02)
                        reversal_stop_loss_price = base_low - reversal_sl_buffer
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

        # Print all lines sorted by setup_score descending
        if log_entries:
            log_entries.sort(key=lambda x: x[0], reverse=True)
            for _, line in log_entries:
                print(line)

        if triggered_symbols:
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
            if not latest_targets:
                continue

            with data_lock:
                current_time = time.time()
                current_active_syms = set(market_data.keys())
                new_targets = [t for t in latest_targets if t["finnhub_symbol"] not in current_active_syms]

                if new_targets:
                    for t in new_targets:
                        register_symbol_in_memory(t)

                    # Capacity management: protect active runners and tickers in cooldown from eviction
                    if len(market_data) > MAX_WATCHLIST_SYMBOLS:
                        def eviction_priority(s):
                            m = market_data[s]
                            in_cool = any([
                                (current_time - m.get("last_alert_time", 0)) < ALERT_COOLDOWN_SECONDS,
                                (current_time - m.get("last_reclaim_alert_time", 0)) < ALERT_COOLDOWN_SECONDS,
                                (current_time - m.get("last_pullback_alert_time", 0)) < ALERT_COOLDOWN_SECONDS,
                                (current_time - m.get("last_breakout_alert_time", 0)) < ALERT_COOLDOWN_SECONDS,
                            ])
                            active_streak = m.get("reclaim_streak", 0) >= 2
                            # Active stocks receive a priority boost of 1000 so they are never evicted first
                            priority = m.get("morning_setup_score", 0.0)
                            if in_cool or active_streak:
                                priority += 1000.0
                            return priority

                        sorted_syms = sorted(market_data.keys(), key=eviction_priority, reverse=True)
                        syms_to_keep = set(sorted_syms[:MAX_WATCHLIST_SYMBOLS])
                        syms_to_drop = [s for s in market_data.keys() if s not in syms_to_keep]

                        for s in syms_to_drop:
                            del market_data[s]
                            try:
                                if ws_global and getattr(ws_global, "sock", None) and ws_global.sock.connected:
                                    ws_global.send(json.dumps({"type": "unsubscribe", "symbol": s}))
                            except Exception:
                                pass

                    for t in new_targets:
                        sym = t["finnhub_symbol"]
                        if sym in market_data:
                            try:
                                if ws_global and getattr(ws_global, "sock", None) and ws_global.sock.connected:
                                    ws_global.send(json.dumps({"type": "subscribe", "symbol": sym}))
                            except Exception:
                                pass
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
        try:
            ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
        except Exception as e:
            print(f"Failed to subscribe {sym} on open: {e}")

if __name__ == "__main__":
    websocket.enableTrace(False)
    initial_targets = get_morning_watchlist()
    for target in initial_targets:
        register_symbol_in_memory(target)
        
    threading.Thread(target=evaluation_loop, daemon=True).start()
    threading.Thread(target=watchlist_refresher_loop, daemon=True).start()
    
    while True:
        try:
            ws = websocket.WebSocketApp(
                f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"WebSocket reconnect loop error: {e}")
        time.sleep(15)
