import os
import websocket
import json
import requests
import time
import threading
import sys
import warnings
import numpy as np
import pandas as pd
import yfinance as yf
from yfinance import EquityQuery
from datetime import datetime
from zoneinfo import ZoneInfo

# Silence internal yfinance / pandas deprecation warnings
warnings.filterwarnings('ignore')

# ==========================================
# CONFIGURATION & INSTITUTIONAL PARAMETERS
# ==========================================
FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN", "d86fpu1r01qgiu458c80d86fpu1r01qgiu458c8g")
N8N_WEBHOOK_URL = os.getenv(
    "N8N_WEBHOOK_URL",
    "https://go90ng-n8n.eq7icp.easypanel.host/webhook/bcca44dc-8944-41a2-8d96-3c5eb1f159e9"
)

# Prevent alert spamming: 15-minute (900 seconds) cooldown per ticker
ALERT_COOLDOWN_SECONDS = 900

# Re-scan market every 30 minutes (1800 seconds) to catch mid-day breakout runners
REFRESH_INTERVAL_SECONDS = 1800

# Reject any single trade print that implies an unrealistic jump from the previous tick
MAX_TICK_MOVE_PCT = 20.0

# Screener parameters
MARKET_CAP_CEILING = 2_000_000_000_000_000  # $2QD Ceiling
MIN_RELATIVE_VOLUME = 1.2                    # Permissive floor to allow recovering pullbacks
MIN_CHANGE_PCT = -20.0                       # Captures deep pullbacks and recovering dip setups
MAX_WATCHLIST_SYMBOLS = 40                   # Sliced to 40 for Finnhub WebSocket cap & sub-ms loop latency
MAX_FLOAT_SHARES = 50_000_000
ENABLE_FLOAT_FILTER = False
DEBUG_PRINT_QUOTE_FIELDS = False

# Harvest depth configuration across 5 pages
HARVEST_TARGET_COUNT = 1250
PAGE_SIZE = 250

# Target Calibration Parameters (+6% Target Engine)
MIN_TARGET_GAIN_PCT = 6.0          # Hard floor for Take Profit 2
MIN_ADR_PCT = 1.0                  # Baseline statistical daily range required to qualify
MAX_STOP_LOSS_PCT = 3.8            # Maximum allowable risk; rejects trade if structural stop exceeds this
MIN_1M_ORDER_FLOW_VOLUME = 500     # Minimum volume in 60s window to calculate valid order flow delta

# Episodic Pivot Override Parameters (Day-1 catalysts like YOU at +25% with low peacetime ADR)
EPISODIC_PIVOT_GAIN = 6.0          # Minimum gain % to qualify for catalyst ADR bypass
EPISODIC_PIVOT_RVOL = 2.5          # Minimum RVOL to confirm institutional catalyst expansion

# Maximum duration (in seconds) an under-VWAP dip remains valid for a reclaim (30 mins)
MAX_DIP_AGE_SECONDS = 1800

# Minimum Reward-to-Risk ratio required to authorize any gate execution
MIN_REWARD_RISK_RATIO = 1.5

# --- DEEP PULLBACK / HOD-RECLAIM SIGNAL ---
DEEP_PULLBACK_TIERS = [
    {"name": "TIER_1_SHALLOW",  "min_depth_pct": 3.1,  "min_order_flow_delta_pct": 15.0},
    {"name": "TIER_2_MODERATE", "min_depth_pct": 7.0,  "min_order_flow_delta_pct": 25.0},
    {"name": "TIER_3_DEEP",      "min_depth_pct": 10.0, "min_order_flow_delta_pct": 35.0},
]

RECLAIM_HOD_PROXIMITY_PCT = 3.0
RECLAIM_MIN_STREAK = 2
RECLAIM_MIN_ORDER_FLOW_DELTA_PCT = 20.0

# Concurrency primitives
data_lock = threading.RLock()
ws_send_lock = threading.Lock()
market_data = {}
ws_global = None


# ==========================================
# 1.A: THREAD-SAFE WEBSOCKET DISPATCHER
# ==========================================
def send_ws_message(payload: dict) -> bool:
    """
    Safely sends JSON over the active WebSocket, guarding against concurrent writes and stale sockets.
    """
    global ws_global
    with ws_send_lock:
        try:
            if ws_global is not None:
                sock = getattr(ws_global, "sock", None)
                if sock is not None and getattr(sock, "connected", False):
                    ws_global.send(json.dumps(payload))
                    return True
        except Exception as e:
            print(f"  [WS WARN] Failed to send {payload}: {e}")
        return False


# ==========================================
# HELPER FUNCTIONS: TIERING, RVOL, SCORING & TRADE MATH
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


def calculate_relative_volume(day_volume: float, avg_vol_3m: float) -> float | None:
    if not avg_vol_3m or avg_vol_3m <= 0 or not day_volume or day_volume <= 0:
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
        elapsed_minutes = max(1.0, (now_et - market_open).total_seconds() / 60.0)

    t = elapsed_minutes / session_minutes
    # Front-loaded institutional U-curve volume progression
    expected_fraction = 0.25 * np.sqrt(t) + 0.50 * t + 0.25 * (t ** 3)
    expected_fraction = max(0.01, min(1.0, float(expected_fraction)))

    expected_volume = avg_vol_3m * expected_fraction
    return round(day_volume / expected_volume, 2) if expected_volume > 0 else None


def evaluate_adr_feasibility(upside_potential: float, adr_pct: float) -> tuple[float, str]:
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


def adjust_for_whole_dollar(target_price: float, entry_price: float) -> float:
    """
    Front-runs psychological whole-dollar and half-dollar resistance walls.
    Eliminates float rounding noise.
    - Stocks > $2.00: $X.00 to $X.08 -> $(X-1).97
    - Stocks $1.00 to $5.00: $X.50 to $X.54 -> $(X).47
    """
    if target_price <= 1.00:
        return round(target_price, 2)

    whole_dollar = int(target_price)
    cents = round(target_price - whole_dollar, 2)

    # Whole dollar barrier: $X.00 - $X.08
    if 0.0 <= cents <= 0.08 and whole_dollar > entry_price:
        adjusted = whole_dollar - 0.03
        if adjusted > entry_price * 1.01:
            return round(adjusted, 2)

    # Half-dollar barrier for low-priced equities ($1.00 to $5.00): $X.50 - $X.54
    if 1.00 <= target_price <= 5.00:
        if 0.50 <= cents <= 0.54 and (whole_dollar + 0.50) > entry_price:
            adjusted = whole_dollar + 0.47
            if adjusted > entry_price * 1.01:
                return round(adjusted, 2)

    return round(target_price, 2)


def calculate_trade_levels(
    signal_type: str,
    current_price: float,
    avg_price: float,
    hod_price: float,
    session_low_price: float,
    adr_pct: float,
    atr_14: float,
    atr_pct: float,
    live_rvol: float,
    order_flow_delta_pct: float,
    remaining_vol_ratio: float = 1.0,
    vol_progress_pct: float = 100.0
) -> dict | None:
    """
    Deterministic institutional execution engine featuring empirical volatility,
    catalyst volatility expansion, live-RVOL volume fuel factor scaling,
    structural stop-loss integrity, and safe whole-dollar clearance logic.
    """
    day_range_pct = ((hod_price - session_low_price) / session_low_price * 100.0) if session_low_price > 0 else 0.0
    is_active_catalyst = (live_rvol >= EPISODIC_PIVOT_RVOL or vol_progress_pct >= 150.0)

    # Adopt today's realized range as the volatility canvas if an episodic catalyst is present
    eff_adr = max(adr_pct, atr_pct, day_range_pct if is_active_catalyst else 0.0)
    if eff_adr < MIN_ADR_PCT or current_price <= 0.0:
        return None

    eff_hod = max(hod_price, current_price)

    # Scale effective ATR dollars with realized range on catalyst days
    if is_active_catalyst:
        catalyst_atr_dollars = current_price * (eff_adr / 100.0) * 0.35
        eff_atr_dollars = max(atr_14, catalyst_atr_dollars)
    else:
        eff_atr_dollars = atr_14 if atr_14 > 0.0 else (current_price * (eff_adr / 100.0))

    # Compute Volume Fuel Factor anchored to live RVOL pacing to prevent afternoon decay collapse
    vol_fuel_factor = max(0.55, min(1.65, float(np.sqrt(max(0.01, live_rvol)))))

    # Volume Exhaustion Protection: clamp expansion if excessive turnover occurs with weak delta
    is_exhaustion_churn = (vol_progress_pct >= 350.0 and order_flow_delta_pct < 20.0)
    if is_exhaustion_churn:
        vol_fuel_factor = min(vol_fuel_factor, 0.75)

    # Dynamic Headroom Ceiling scaled by Volume Fuel Factor
    max_realistic_reach = current_price * (1.0 + (eff_adr / 100.0) * (0.85 * vol_fuel_factor))

    # Gate 2 Alignment Check (-0.5% to +0.4% from VWAP)
    if signal_type == "VWAP_CONTINUATION_BOUNCE":
        if current_price > avg_price * 1.004:
            return None
        entry = round(current_price, 2)
        atr_stop = entry - (eff_atr_dollars * 0.22)
        sl = round(max(avg_price * 0.972, min(avg_price * 0.985, atr_stop)), 2)

        raw_tp2 = min(eff_hod, max_realistic_reach)
        if vol_fuel_factor < 0.85:
            raw_tp2 = entry + ((raw_tp2 - entry) * 0.70)

        tp2 = adjust_for_whole_dollar(raw_tp2, entry)
        if ((tp2 - entry) / entry * 100.0) < MIN_TARGET_GAIN_PCT and vol_fuel_factor >= 1.0:
            tp2 = round(entry * (1.0 + (MIN_TARGET_GAIN_PCT / 100.0) * 1.05), 2)

        tp1 = round(entry + 0.55 * (tp2 - entry), 2)

    # Gate 1 Alignment Check (-0.5% to +0.6% from VWAP)
    elif signal_type == "VWAP_RECLAIM_CROSSOVER":
        if current_price > avg_price * 1.006:
            return None
        entry = round(current_price, 2)
        atr_stop = entry - (eff_atr_dollars * 0.22)
        sl = round(max(avg_price * 0.972, min(avg_price * 0.985, atr_stop)), 2)

        raw_tp2 = min(eff_hod, max_realistic_reach)
        if vol_fuel_factor >= 1.20:
            raw_tp2 = max(raw_tp2, eff_hod * (1.0 + (eff_adr / 100.0) * 0.15))

        tp2 = adjust_for_whole_dollar(raw_tp2, entry)
        if ((tp2 - entry) / entry * 100.0) < MIN_TARGET_GAIN_PCT and vol_fuel_factor >= 1.0:
            tp2 = round(entry * (1.0 + (MIN_TARGET_GAIN_PCT / 100.0) * 1.05), 2)

        tp1 = round(entry + 0.50 * (tp2 - entry), 2)

    elif signal_type == "DEEP_PULLBACK_RECLAIM":
        entry = round(current_price, 2)
        base_low = session_low_price if (0.0 < session_low_price <= current_price) else current_price * 0.985
        structural_low_stop = base_low - max(0.04, eff_atr_dollars * 0.15)
        sl = round(max(structural_low_stop, entry * 0.965), 2)
        sl = min(sl, round(entry * 0.985, 2))

        raw_tp2 = min(eff_hod, max_realistic_reach)
        tp2 = adjust_for_whole_dollar(max(raw_tp2, avg_price * 1.02), entry)
        if ((tp2 - entry) / entry * 100.0) < MIN_TARGET_GAIN_PCT and vol_fuel_factor >= 1.0:
            tp2 = round(entry * (1.0 + (MIN_TARGET_GAIN_PCT / 100.0) * 1.05), 2)

        raw_tp1 = min(max(avg_price, entry * 1.025), entry + 0.65 * (tp2 - entry))
        tp1 = adjust_for_whole_dollar(raw_tp1, entry)

    elif signal_type == "HOD_IGNITION_BREAKOUT":
        trigger_price = max(current_price, eff_hod)
        entry = round(trigger_price * 1.0015, 2)  # 15 bps execution slippage model
        atr_stop = entry - (eff_atr_dollars * 0.25)
        sl = round(max(entry * 0.972, min(entry * 0.985, atr_stop)), 2)

        # Baseline expansion padded so that post-clipping target retains >= 6.0%
        expansion_pct = max(MIN_TARGET_GAIN_PCT + 1.2, eff_adr * (0.35 * vol_fuel_factor))
        raw_tp2 = entry * (1.0 + (expansion_pct / 100.0))
        tp2 = adjust_for_whole_dollar(raw_tp2, entry)

        if ((tp2 - entry) / entry * 100.0) < MIN_TARGET_GAIN_PCT and vol_fuel_factor >= 1.0:
            tp2 = round(entry * (1.0 + (MIN_TARGET_GAIN_PCT / 100.0) * 1.05), 2)

        tp1 = round(entry + 0.50 * (tp2 - entry), 2)

    elif signal_type == "EOD_OVERNIGHT_IGNITION":
        entry = round(current_price, 2)
        vwap_stop = round(avg_price * 0.985, 2)
        hard_stop = round(entry * 0.968, 2)  # -3.2% max risk limit
        sl = max(vwap_stop, hard_stop)

        target_gain_pct = max(MIN_TARGET_GAIN_PCT + 1.2, eff_adr * 0.65 * vol_fuel_factor)
        raw_tp2 = entry * (1.0 + (target_gain_pct / 100.0))
        tp2 = adjust_for_whole_dollar(raw_tp2, entry)
        if ((tp2 - entry) / entry * 100.0) < MIN_TARGET_GAIN_PCT and vol_fuel_factor >= 1.0:
            tp2 = round(entry * (1.0 + (MIN_TARGET_GAIN_PCT / 100.0) * 1.05), 2)

        tp1 = round(entry + 0.50 * (tp2 - entry), 2)

    else:
        entry = round(current_price, 2)
        sl = round(current_price * 0.975, 2)
        tp1 = round(current_price * 1.03, 2)
        tp2 = round(current_price * 1.06, 2)

    # Enforce structural stop integrity: reject outright if risk exceeds MAX_STOP_LOSS_PCT
    min_sl_price = round(entry * (1.0 - (MAX_STOP_LOSS_PCT / 100.0)), 2)
    if sl < min_sl_price:
        return None

    if sl >= entry or tp2 <= entry:
        return None

    est_gain_tp2_pct = round(((tp2 - entry) / entry) * 100.0, 2) if entry > 0 else 0.0
    est_gain_tp1_pct = round(((tp1 - entry) / entry) * 100.0, 2) if entry > 0 else 0.0

    # Strictly reject any setup that cannot deliver >= 6% gain on TP2
    if est_gain_tp2_pct < MIN_TARGET_GAIN_PCT:
        return None

    risk_per_share = max(0.01, entry - sl)
    reward_per_share = max(0.0, tp2 - entry)
    rr_ratio = round(reward_per_share / risk_per_share, 2)

    if rr_ratio < MIN_REWARD_RISK_RATIO:
        return None

    return {
        "target_entry_price": entry,
        "tp1_price": tp1,
        "take_profit_price": tp2,
        "stop_loss_price": sl,
        "estimated_tp1_gain_pct": est_gain_tp1_pct,
        "estimated_gain_percentage": est_gain_tp2_pct,
        "reward_risk_ratio": rr_ratio,
        "volume_fuel_factor": round(vol_fuel_factor, 2)
    }


def score_morning_setup(target: dict) -> tuple[float, str]:
    score = 0.0
    score += min(target.get("adr_pct", 0.0), 12.0) * 2.5
    rvol = target.get("relative_volume") or 1.0
    score += min(rvol, 10.0) * 2.5

    trend_7d = target.get("trend_7d", "Neutral")
    if trend_7d == "Bullish":
        score += 20.0
    elif trend_7d == "Neutral":
        score += 10.0

    alignment = target.get("alignment_state", "MIXED")
    if alignment == "FULL_BULLISH":
        score += 25.0
    elif alignment == "COMPRESSION":
        score += 20.0
    elif target.get("macro_uptrend", False):
        score += 10.0

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
    score += min(max(order_flow_delta_pct, 0.0), 30.0) * 0.6
    score += min(metrics.get("adr_pct", 0.0), 12.0) * 1.0

    rvol_val = live_rvol if (live_rvol and live_rvol > 0) else (metrics.get("relative_volume") or 1.0)
    score += min(rvol_val, 10.0) * 2.0
    # Clean 0% to 100% intraday position mapping to [0, 15] points
    score += (max(0.0, min(100.0, intraday_clv_pct)) / 100.0) * 15.0

    trend_7d = metrics.get("trend_7d", "Neutral")
    if trend_7d == "Bullish":
        score += 15.0
    elif trend_7d == "Neutral":
        score += 7.5

    alignment = metrics.get("alignment_state", "MIXED")
    if alignment in ("FULL_BULLISH", "COMPRESSION"):
        score += 20.0
    elif metrics.get("macro_uptrend", False):
        score += 10.0

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
# PHASE 1: SCREENER (HARVEST 1250 -> SHORTLIST 100 -> SCORE ALL -> SLICE 40)
# ==========================================
def get_morning_watchlist() -> list[dict]:
    print(f"\n--- SCREENING MARKET: HARVEST {HARVEST_TARGET_COUNT} -> SHORTLIST 100 -> ADR FILTER -> SLICE 40 ---")
    try:
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', 60_000_000]),      
            EquityQuery('lt',  ['intradaymarketcap', MARKET_CAP_CEILING]), 
            EquityQuery('gt',  ['intradayprice', 1.00]),           
            EquityQuery('gte', ['percentchange', MIN_CHANGE_PCT]),
            EquityQuery('gte', ['avgdailyvol3m', 1_000_000])          
        ])

        quotes = []
        offsets = list(range(0, HARVEST_TARGET_COUNT, PAGE_SIZE))

        for offset in offsets:
            try:
                resp = yf.screen(q, sortField='dayvolume', sortAsc=False, size=PAGE_SIZE, offset=offset)
                batch = resp.get('quotes', [])
                quotes.extend(batch)
                if len(batch) < PAGE_SIZE:
                    break
                time.sleep(0.1)  # 100ms throttle between pages to prevent rate limiting
            except Exception as e:
                print(f"  [WARN] Screen failed at offset {offset}: {e}")

        print(f"Harvested {len(quotes)} raw quotes across {len(offsets)} pages.")

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

            # Fallback between marketCap and intradaymarketcap
            raw_market_cap = quote.get("marketCap", 0) or quote.get("intradaymarketcap", 0)
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
        # threads=False prevents SQLite database locks in yfinance cache
        hist_data = yf.download(sym_list, period="4mo", progress=False, group_by='column', threads=False)

        def extract_series(data_frame, col_name, ticker):
            if col_name not in data_frame:
                return None
            sub = data_frame[col_name]
            if isinstance(sub, pd.DataFrame):
                return sub[ticker].dropna() if ticker in sub.columns else None
            elif isinstance(sub, pd.Series):
                return sub.dropna()
            return None

        today_et = datetime.now(ZoneInfo("America/New_York")).date()

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
            target["atr_14"] = 0.0
            target["atr_pct"] = 0.0
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
                stock_closes = drop_today(extract_series(hist_data, 'Close', sym))
                stock_opens = drop_today(extract_series(hist_data, 'Open', sym))
                s_highs = drop_today(extract_series(hist_data, 'High', sym))
                s_lows = drop_today(extract_series(hist_data, 'Low', sym))
                s_vols = drop_today(extract_series(hist_data, 'Volume', sym))

                if stock_closes is None or len(stock_closes) < 50:
                    continue

                ma_5 = stock_closes.tail(5).mean()
                ma_10 = stock_closes.tail(10).mean()
                ma_21 = stock_closes.tail(21).mean()
                ma_50 = stock_closes.tail(50).mean()
                ema_8 = stock_closes.ewm(span=8, adjust=False).mean().iloc[-1]
                target["ma_5"] = float(ma_5)

                p_day_close = float(stock_closes.iloc[-1])
                p_day_prior_close = float(stock_closes.iloc[-2])
                
                has_valid_open = stock_opens is not None and len(stock_opens) > 0 and not pd.isna(stock_opens.iloc[-1])
                p_day_open = float(stock_opens.iloc[-1]) if has_valid_open else p_day_prior_close

                prev_ret = ((p_day_close - p_day_prior_close) / p_day_prior_close * 100.0) if p_day_prior_close > 0 else 0.0
                prev_color = "GREEN" if prev_ret >= 0 else "RED"

                if has_valid_open:
                    candle_color = "BULLISH" if p_day_close > p_day_open else ("BEARISH" if p_day_close < p_day_open else ("BULLISH" if prev_ret >= 0 else "BEARISH"))
                else:
                    candle_color = "BULLISH" if prev_ret >= 0 else "BEARISH"

                target["prev_day_return_pct"] = round(prev_ret, 2)
                target["prev_day_color"] = prev_color
                target["prev_day_candle"] = candle_color
                target["prev_day_trend"] = f"{prev_color} ({prev_ret:+.2f}%)"

                if s_highs is not None and s_lows is not None:
                    daily_ranges = ((s_highs.tail(21) - s_lows.tail(21)) / s_lows.tail(21)) * 100
                    target["adr_pct"] = float(daily_ranges.mean())

                    prev_close_shift = stock_closes.shift(1)
                    tr1 = s_highs - s_lows
                    tr2 = (s_highs - prev_close_shift).abs()
                    tr3 = (s_lows - prev_close_shift).abs()
                    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

                    atr_series = true_range.ewm(alpha=1/14, adjust=False).mean()
                    atr_val = float(atr_series.iloc[-1])
                    last_close_val = float(stock_closes.iloc[-1])

                    target["atr_14"] = round(atr_val, 2)
                    target["atr_pct"] = round((atr_val / last_close_val * 100.0), 2) if last_close_val > 0 else 0.0

                    if s_vols is not None:
                        common_idx = stock_closes.tail(20).index
                        typ_p = (s_highs.loc[common_idx] + s_lows.loc[common_idx] + stock_closes.loc[common_idx]) / 3.0
                        v_sum = s_vols.loc[common_idx].sum()
                        if v_sum > 0:
                            target["vwap_20d"] = float((typ_p * s_vols.loc[common_idx]).sum() / v_sum)

                # Episodic Pivot Override: allow low peacetime ADR if volume/momentum confirms catalyst expansion
                is_episodic_pivot = (
                    target["live_change"] >= EPISODIC_PIVOT_GAIN and 
                    (target["relative_volume"] or 0) >= EPISODIC_PIVOT_RVOL
                )
                if target["adr_pct"] < MIN_ADR_PCT and not is_episodic_pivot:
                    continue

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

            is_recovering_pullback = target["live_change"] < 3.0
            if is_recovering_pullback and target["alignment_state"] not in ("FULL_BULLISH", "COMPRESSION"):
                continue
            elif target["alignment_state"] == "BEARISH":
                continue

            pre_score, pre_class = score_morning_setup(target)
            target["morning_setup_score"] = pre_score
            target["morning_setup_class"] = pre_class

            fully_scored_candidates.append(target)

        fully_scored_candidates = sorted(
            fully_scored_candidates, 
            key=lambda x: x["morning_setup_score"], 
            reverse=True
        )
        top_40 = fully_scored_candidates[:MAX_WATCHLIST_SYMBOLS]
        
        # Seed true VWAP and historical dip/low states from 1-minute intraday bars for the top watchlist
        top_sym_list = [t["symbol"] for t in top_40]
        if top_sym_list:
            print(f"Downloading 1-minute bars to seed precision intraday VWAP & dip history for {len(top_sym_list)} targets...")
            try:
                bars_1m = yf.download(
                    top_sym_list,
                    period="1d",
                    interval="1m",
                    progress=False,
                    group_by='column',
                    threads=False
                )
                current_unix = time.time()
                for target in top_40:
                    sym = target["symbol"]
                    c_1m = extract_series(bars_1m, 'Close', sym)
                    h_1m = extract_series(bars_1m, 'High', sym)
                    l_1m = extract_series(bars_1m, 'Low', sym)
                    v_1m = extract_series(bars_1m, 'Volume', sym)

                    if c_1m is not None and h_1m is not None and l_1m is not None and v_1m is not None and len(c_1m) > 0:
                        raw_idx = c_1m.index.intersection(h_1m.index).intersection(l_1m.index).intersection(v_1m.index).sort_values()
                        
                        # Strict date check: Exclude yesterday's bars when running pre-market or during weekends
                        common_idx = [
                            dt for dt in raw_idx 
                            if (dt.tz_convert("America/New_York").date() if getattr(dt, "tzinfo", None) is not None else pd.to_datetime(dt).date()) == today_et
                        ]

                        if len(common_idx) > 0:
                            common_idx = pd.DatetimeIndex(common_idx)
                            h_sub = h_1m.loc[common_idx]
                            l_sub = l_1m.loc[common_idx]
                            c_sub = c_1m.loc[common_idx]
                            v_sub = v_1m.loc[common_idx].fillna(0)

                            typ_p = (h_sub + l_sub + c_sub) / 3.0
                            dollar_bars = typ_p * v_sub
                            
                            cum_v = v_sub.cumsum()
                            cum_d = dollar_bars.cumsum()

                            dollar_traded = float(cum_d.iloc[-1])
                            total_bar_vol = float(cum_v.iloc[-1])

                            if total_bar_vol > 0 and dollar_traded > 0:
                                true_vwap = dollar_traded / total_bar_vol
                                quote_vol = float(target.get("day_volume", 0))

                                if quote_vol > total_bar_vol:
                                    target["seed_dollar_traded"] = float(true_vwap * quote_vol)
                                else:
                                    target["seed_dollar_traded"] = dollar_traded
                                    target["day_volume"] = int(total_bar_vol)

                                bar_high = float(h_sub.max())
                                bar_low = float(l_sub.min())
                                if bar_high > target.get("day_high", 0):
                                    target["day_high"] = bar_high
                                if bar_low > 0 and (target.get("day_low", 0) <= 0 or bar_low < target["day_low"]):
                                    target["day_low"] = bar_low

                                # Cumulative VWAP curve across all 1-minute bars
                                running_vwap = np.where(cum_v > 0, cum_d / cum_v, typ_p)
                                running_vwap = pd.Series(running_vwap, index=common_idx)

                                dist_low_series = ((l_sub - running_vwap) / running_vwap) * 100.0
                                dist_close_series = ((c_sub - running_vwap) / running_vwap) * 100.0

                                # Seed true historical session-low VWAP distance
                                valid_low_dists = dist_low_series.dropna()
                                if not valid_low_dists.empty:
                                    target["session_low_vwap_distance"] = float(valid_low_dists.min())

                                # Replay dip qualification across 1-minute bars
                                dipped_flag = False
                                last_dip_time = 0.0

                                for idx, bar_dt in enumerate(common_idx):
                                    if getattr(bar_dt, "tzinfo", None) is not None:
                                        bar_ts = bar_dt.timestamp()
                                    else:
                                        bar_ts = pd.to_datetime(bar_dt).tz_localize("America/New_York").timestamp()

                                    d_low = dist_low_series.iloc[idx]
                                    d_close = dist_close_series.iloc[idx]

                                    if pd.isna(d_low) or pd.isna(d_close):
                                        continue

                                    if (-2.5 <= d_close <= -0.2) or (-2.5 <= d_low <= -0.2):
                                        dipped_flag = True
                                        last_dip_time = bar_ts
                                    elif d_close < -3.0 or d_close > 1.5:
                                        dipped_flag = False

                                # Check dip expiration against current system time (max 30 mins)
                                if dipped_flag and (current_unix - last_dip_time) <= MAX_DIP_AGE_SECONDS:
                                    target["dipped_below_vwap"] = True
                                    target["dipped_below_vwap_time"] = last_dip_time
                                else:
                                    target["dipped_below_vwap"] = False
                                    target["dipped_below_vwap_time"] = 0.0

            except Exception as e:
                print(f"  [WARN] Failed downloading 1m bars for VWAP seeding: {e}")

        print(f"Qualified & Scored {len(fully_scored_candidates)} aligned targets (ADR >= {MIN_ADR_PCT}% or Catalyst). Selected top {len(top_40)} for WebSocket.")
        if top_40:
            print(f"  [TOP PICK] {top_40[0]['finnhub_symbol']} | Score: {top_40[0]['morning_setup_score']:.1f} ({top_40[0]['morning_setup_class']}) | "
                  f"PrevDay: {top_40[0]['prev_day_trend']} | Align: {top_40[0]['alignment_state']} | ADR: {top_40[0]['adr_pct']:.2f}% | ATR: ${top_40[0]['atr_14']:.2f} ({top_40[0]['atr_pct']:.1f}%) | RVOL: {top_40[0]['relative_volume']}")

        return top_40

    except Exception as e:
        print(f"Screener execution error: {e}")
        return []


def register_symbol_in_memory(target: dict, force_update: bool = False):
    sym = target["finnhub_symbol"]
    with data_lock:
        if sym not in market_data or force_update:
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
                "atr_14": target.get("atr_14", 0.0),
                "atr_pct": target.get("atr_pct", 0.0),
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
                "last_tick_direction": 1,  # Direction persistence for flat ticks
                "rolling_buy_vol": 0,
                "rolling_sell_vol": 0,
                "reject_count": 0,
                "near_hod_streak": 0,
                "last_alert_time": 0,  
                "dipped_below_vwap": target.get("dipped_below_vwap", False),  
                "dipped_below_vwap_time": target.get("dipped_below_vwap_time", 0.0),
                "last_reclaim_alert_time": 0,  
                "session_low_price": seed_session_low_price,
                "session_low_vwap_distance": target.get("session_low_vwap_distance", None),  
                "reclaim_streak": 0,
                "last_pullback_alert_time": 0,
                "last_breakout_alert_time": 0,
                "last_overnight_alert_time": 0
            }


# ==========================================
# PHASE 2: INDEPENDENT BACKGROUND THREADS
# ==========================================
def evaluation_loop():
    print("--- [THREAD] Independent 60-Second Evaluation Loop Started ---")
    now_et = datetime.now(ZoneInfo("America/New_York"))
    current_date = now_et.date()
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)

    # If started after market open, session is marked active; if pre-market, it waits for 09:30 ET
    session_opened_today = (now_et >= market_open)
    last_session_date = current_date

    while True:
        time.sleep(60)
        current_time = time.time()
        triggered_symbols = []
        log_entries = []

        now_et = datetime.now(ZoneInfo("America/New_York"))
        current_date = now_et.date()
        market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)

        # Midnight rollover reset
        if current_date != last_session_date:
            session_opened_today = False
            last_session_date = current_date

        # Multi-day daemon state reset at 09:30 ET: Re-seed from fresh morning watchlist & re-subscribe WS
        if now_et >= market_open and not session_opened_today:
            print(f"\n--- [SESSION RESET] Market open reached: Re-seeding state for {current_date} ---")
            try:
                refreshed_targets = get_morning_watchlist()
                with data_lock:
                    for target in refreshed_targets:
                        register_symbol_in_memory(target, force_update=True)
                    symbols_to_sub = list(market_data.keys())
                for sym in symbols_to_sub:
                    send_ws_message({"type": "subscribe", "symbol": sym})
                    time.sleep(0.02)
            except Exception as e:
                print(f"  [RESET WARN] Failed to re-seed session: {e}")
            session_opened_today = True

        # Guard: Restrict execution gate logic strictly to regular market hours
        is_market_session = (market_open <= now_et < market_close)
        if not is_market_session:
            with data_lock:
                for sym, metrics in market_data.items():
                    metrics["price_60s_ago"] = metrics["current_price"]
            continue

        uk_time = datetime.now(ZoneInfo("Europe/London"))
        timestamp_str = uk_time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n--- {timestamp_str} | 60-SECOND ALGORITHMIC VWAP SCANS (RANKED BY SCORE) ---")

        session_minutes = 390.0
        elapsed_minutes = max(1.0, (now_et - market_open).total_seconds() / 60.0)
        t_session = min(1.0, elapsed_minutes / session_minutes)

        # Non-linear cumulative curve expected progress
        curve_progress = 0.25 * np.sqrt(t_session) + 0.50 * t_session + 0.25 * (t_session ** 3)
        remaining_session_fraction = max(0.0, 1.0 - curve_progress)

        # Power Hour Evaluation Window (15:15 to 15:58 ET)
        is_power_hour = (now_et.hour == 15 and 15 <= now_et.minute <= 58)
        # Avoid erratic opening uncross prints for HOD breakouts
        is_settled_market = (now_et >= market_open.replace(minute=35))
        
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
                
                # Require volume floor in 60s window before calculating delta
                order_flow_delta_pct = 0.0
                if total_tick_vol >= MIN_1M_ORDER_FLOW_VOLUME:
                    order_flow_delta_pct = ((buy_v - sell_v) / total_tick_vol) * 100.0
                    
                metrics["rolling_buy_vol"] = 0
                metrics["rolling_sell_vol"] = 0
                
                if cum_vol > 50000 and current_price > 0: 
                    avg_price = metrics["total_dollar_traded"] / cum_vol
                    vwap_distance = ((current_price - avg_price) / avg_price) * 100.0
                    
                    if -2.5 <= vwap_distance <= -0.2:
                        metrics["dipped_below_vwap"] = True
                        metrics["dipped_below_vwap_time"] = current_time

                    if (vwap_distance < -3.0 or 
                        vwap_distance > 1.5 or 
                        (current_time - metrics["dipped_below_vwap_time"]) > MAX_DIP_AGE_SECONDS):
                        metrics["dipped_below_vwap"] = False
                    
                    # Actual headroom: current price directly to HOD
                    upside_potential = ((hod_price - current_price) / current_price) * 100.0 if current_price > 0 else 0.0

                    dynamic_target = min(15.0, max(5.0, abs(p_change) * 0.40))
                    nearest_whole_dollar = round(current_price)
                    is_converging = False
                    if current_price > 2.00:
                        cents_away = abs(current_price - nearest_whole_dollar)
                        if cents_away <= 0.05:
                            is_converging = True

                    is_bouncing = current_price > price_60s_ago  
                    
                    extension_pct = 0.0
                    is_over_extended = False
                    if ma_5 > 0:
                        extension_pct = ((current_price - ma_5) / ma_5) * 100.0
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

                    session_low_depth = abs(session_low_vwap_dist) if (session_low_vwap_dist and session_low_vwap_dist < 0) else 0.0
                    pullback_tier = None
                    for tier in DEEP_PULLBACK_TIERS:
                        if session_low_depth >= tier["min_depth_pct"]:
                            pullback_tier = tier

                    distance_to_hod_pct = max(0.0, upside_potential)

                    if distance_to_hod_pct <= 2.0:
                        metrics["near_hod_streak"] += 1
                    else:
                        metrics["near_hod_streak"] = 0

                    in_cooldown = (current_time - metrics["last_alert_time"]) < ALERT_COOLDOWN_SECONDS
                    pullback_in_cooldown = (current_time - metrics["last_pullback_alert_time"]) < ALERT_COOLDOWN_SECONDS
                    reclaim_in_cooldown = (current_time - metrics["last_reclaim_alert_time"]) < ALERT_COOLDOWN_SECONDS
                    breakout_in_cooldown = (current_time - metrics["last_breakout_alert_time"]) < ALERT_COOLDOWN_SECONDS
                    overnight_in_cooldown = (current_time - metrics.get("last_overnight_alert_time", 0)) < ALERT_COOLDOWN_SECONDS

                    distance_from_low_pct = 0.0
                    if metrics["session_low_price"] > 0:
                        distance_from_low_pct = ((current_price - metrics["session_low_price"]) / metrics["session_low_price"]) * 100.0

                    # Clean 0% to 100% position within daily range
                    day_spread = hod_price - metrics["session_low_price"]
                    intraday_clv_pct = 0.0
                    if day_spread > 0:
                        intraday_clv_pct = ((current_price - metrics["session_low_price"]) / day_spread) * 100.0

                    open_p = metrics.get("open_price", current_price)
                    intraday_return_pct = ((current_price - open_p) / open_p * 100.0) if open_p > 0 else 0.0

                    vwap_20d = metrics.get("vwap_20d", 0.0)
                    vwap_20d_dist_pct = ((current_price - vwap_20d) / vwap_20d * 100.0) if vwap_20d > 0 else 0.0

                    avg_3m = metrics.get("avg_vol_3m", 0)
                    raw_rvol = calculate_relative_volume(cum_vol, avg_3m)
                    rvol_ok = (raw_rvol is None) or (raw_rvol >= 1.2)
                    live_rvol = raw_rvol if raw_rvol is not None else (metrics.get("relative_volume") or 1.0)
                    metrics["relative_volume"] = live_rvol

                    # Remaining Volume & Runway Projections
                    vol_progress_pct = (cum_vol / avg_3m * 100.0) if avg_3m > 0 else 0.0
                    projected_remaining_vol = (avg_3m * remaining_session_fraction * live_rvol) if avg_3m > 0 else 0.0
                    remaining_vol_ratio = projected_remaining_vol / avg_3m if avg_3m > 0 else 0.0

                    day_range_pct = ((hod_price - metrics["session_low_price"]) / metrics["session_low_price"] * 100.0) if metrics["session_low_price"] > 0 else 0.0
                    adr_exhaustion_ratio = (day_range_pct / metrics["adr_pct"]) if metrics["adr_pct"] > 0 else 1.0
                    has_adr_fuel = (adr_exhaustion_ratio <= 1.35) or (live_rvol >= 3.0)

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

                    formatted_line = (
                        f"[{sym:<5}] {p_change:>+6.2f}% | Score: {setup_score:>4.1f} ({setup_class:<8}) | "
                        f"PrevDay: {metrics['prev_day_trend']:<14} | Align: {alignment:<12} | "
                        f"7D: {metrics['trend_7d']:<7} ({metrics['green_red_7d']:<7}) | "
                        f"Delta: {order_flow_delta_pct:>+5.1f}% | VWAPDist: {vwap_distance:>+5.2f}% | "
                        f"RemVol: {projected_remaining_vol/1000:>5.0f}k ({remaining_vol_ratio:>4.2f}x) | "
                        f"ADR: {metrics['adr_pct']:>4.2f}% | ATR: ${metrics['atr_14']:.2f} ({metrics['atr_pct']:.1f}%) | "
                        f"Up2HOD: {upside_potential:>5.2f}% | Streak: {metrics['reclaim_streak']}"
                    )
                    log_entries.append((setup_score, formatted_line))

                    def build_signal_payload(signal_type_str, pb_tier_str, levels_dict):
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
                            "session_low_vwap_distance": round(session_low_vwap_dist, 2) if session_low_vwap_dist else None,
                            "distance_to_hod_pct": round(distance_to_hod_pct, 2),
                            "intraday_clv_pct": round(intraday_clv_pct, 2),
                            "reclaim_streak": metrics["reclaim_streak"],
                            "live_volume": cum_vol,
                            "avg_vol_3m": avg_3m,
                            "volume_progress_pct": round(vol_progress_pct, 1),
                            "projected_remaining_vol": int(projected_remaining_vol),
                            "remaining_volume_ratio": round(remaining_vol_ratio, 2),
                            "volume_fuel_factor": levels_dict.get("volume_fuel_factor", 1.0),
                            "session_time_remaining_pct": round(remaining_session_fraction * 100.0, 1),
                            "rvol": round(live_rvol, 2) if live_rvol else 0.0,
                            "whole_dollar_convergence": is_converging,
                            "target_entry_price": levels_dict["target_entry_price"],
                            "tp1_price": levels_dict["tp1_price"],
                            "take_profit_price": levels_dict["take_profit_price"],
                            "stop_loss_price": levels_dict["stop_loss_price"],
                            "estimated_tp1_gain_pct": levels_dict["estimated_tp1_gain_pct"],
                            "estimated_gain_percentage": levels_dict["estimated_gain_percentage"],
                            "reward_risk_ratio": levels_dict["reward_risk_ratio"],
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
                            "atr_14": metrics["atr_14"],
                            "atr_pct": metrics["atr_pct"],
                            "target_vs_adr_pct": target_vs_adr_pct,
                            "adr_feasibility": adr_feasibility,
                            "DateTime": timestamp_str
                        }

                    # ==============================================================
                    # EXECUTION GATE 1: UNDER-VWAP RECLAIM (Threshold <= 0.6%)
                    # ==============================================================
                    if (metrics["dipped_below_vwap"] and
                        alignment in ("FULL_BULLISH", "COMPRESSION") and  
                        -0.5 <= vwap_distance <= 0.6 and
                        distance_to_hod_pct >= MIN_TARGET_GAIN_PCT and
                        has_adr_fuel and
                        is_bouncing and
                        metrics["reclaim_streak"] >= RECLAIM_MIN_STREAK and
                        order_flow_delta_pct >= RECLAIM_MIN_ORDER_FLOW_DELTA_PCT and
                        remaining_vol_ratio >= 0.15 and
                        projected_remaining_vol >= 150_000 and
                        not is_over_extended and
                        not reclaim_in_cooldown):

                        levels = calculate_trade_levels(
                            "VWAP_RECLAIM_CROSSOVER", current_price, avg_price, hod_price,
                            metrics["session_low_price"], metrics["adr_pct"], metrics["atr_14"], metrics["atr_pct"],
                            live_rvol, order_flow_delta_pct, remaining_vol_ratio, vol_progress_pct
                        )
                        if levels:
                            metrics["last_reclaim_alert_time"] = current_time
                            metrics["dipped_below_vwap"] = False  
                            triggered_symbols.append(
                                build_signal_payload("VWAP_RECLAIM_CROSSOVER", "NONE", levels)
                            )

                    # ==============================================================
                    # EXECUTION GATE 2: VWAP CONTINUATION (Threshold <= 0.4%)
                    # ==============================================================
                    elif (p_change >= 6.0 and 
                          upside_potential >= max(MIN_TARGET_GAIN_PCT, dynamic_target) and 
                          -0.5 <= vwap_distance <= 0.4 and 
                          order_flow_delta_pct >= 15.0 and
                          live_rvol >= 2.0 and
                          has_adr_fuel and
                          is_bouncing and 
                          remaining_vol_ratio >= 0.15 and
                          projected_remaining_vol >= 150_000 and
                          not is_over_extended and
                          not in_cooldown):
                        
                        levels = calculate_trade_levels(
                            "VWAP_CONTINUATION_BOUNCE", current_price, avg_price, hod_price,
                            metrics["session_low_price"], metrics["adr_pct"], metrics["atr_14"], metrics["atr_pct"],
                            live_rvol, order_flow_delta_pct, remaining_vol_ratio, vol_progress_pct
                        )
                        if levels:
                            metrics["last_alert_time"] = current_time  
                            triggered_symbols.append(
                                build_signal_payload("VWAP_CONTINUATION_BOUNCE", "NONE", levels)
                            )

                    # ==============================================================
                    # EXECUTION GATE 3: DEEP PULLBACK REVERSAL (Natural 6% Mean Reversion)
                    # ==============================================================
                    elif (pullback_tier is not None and
                          alignment in ("FULL_BULLISH", "COMPRESSION") and  
                          cum_vol >= 100000 and rvol_ok and
                          vwap_distance <= -2.5 and 
                          distance_to_hod_pct >= MIN_TARGET_GAIN_PCT and
                          distance_from_low_pct >= 1.0 and 
                          metrics["reclaim_streak"] >= RECLAIM_MIN_STREAK and
                          order_flow_delta_pct >= pullback_tier["min_order_flow_delta_pct"] and
                          vol_progress_pct >= 40.0 and
                          remaining_session_fraction >= 0.20 and
                          projected_remaining_vol >= 250_000 and
                          not pullback_in_cooldown):

                        levels = calculate_trade_levels(
                            "DEEP_PULLBACK_RECLAIM", current_price, avg_price, hod_price,
                            metrics["session_low_price"], metrics["adr_pct"], metrics["atr_14"], metrics["atr_pct"],
                            live_rvol, order_flow_delta_pct, remaining_vol_ratio, vol_progress_pct
                        )
                        if levels:
                            metrics["last_pullback_alert_time"] = current_time
                            triggered_symbols.append(
                                build_signal_payload("DEEP_PULLBACK_RECLAIM", pullback_tier["name"], levels)
                            )

                    # ==============================================================
                    # EXECUTION GATE 5: EOD OVERNIGHT IGNITION (NEXT-DAY +6% GAP)
                    # Placed above Gate 4 to prioritize overnight accumulation during Power Hour
                    # ==============================================================
                    elif (is_power_hour and
                          p_change >= 6.0 and
                          distance_to_hod_pct <= 1.5 and
                          intraday_clv_pct >= 80.0 and
                          metrics["adr_pct"] >= 1.0 and
                          vol_progress_pct >= 150.0 and
                          live_rvol >= 2.0 and
                          order_flow_delta_pct >= 20.0 and
                          alignment == "FULL_BULLISH" and
                          current_price >= avg_price and
                          not is_over_extended and
                          not overnight_in_cooldown):

                        levels = calculate_trade_levels(
                            "EOD_OVERNIGHT_IGNITION", current_price, avg_price, hod_price,
                            metrics["session_low_price"], metrics["adr_pct"], metrics["atr_14"], metrics["atr_pct"],
                            live_rvol, order_flow_delta_pct, remaining_vol_ratio, vol_progress_pct
                        )
                        if levels:
                            metrics["last_overnight_alert_time"] = current_time
                            triggered_symbols.append(
                                build_signal_payload("EOD_OVERNIGHT_IGNITION", "OVERNIGHT_SWING", levels)
                            )

                    # ==============================================================
                    # EXECUTION GATE 4: HIGH-OF-DAY (HOD) IGNITION BREAKOUT
                    # Suppressed during Power Hour and before 09:35 ET
                    # ==============================================================
                    elif (not is_power_hour and is_settled_market and
                          p_change >= 6.0 and
                          1.0 < vwap_distance <= 8.0 and
                          distance_to_hod_pct <= 1.5 and
                          metrics["near_hod_streak"] >= 2 and     
                          intraday_clv_pct >= 70.0 and            
                          live_rvol >= 2.5 and
                          remaining_vol_ratio >= 0.25 and
                          is_bouncing and
                          metrics["reclaim_streak"] >= RECLAIM_MIN_STREAK and
                          order_flow_delta_pct >= 30.0 and
                          alignment == "FULL_BULLISH" and
                          not is_over_extended and
                          not breakout_in_cooldown):

                        levels = calculate_trade_levels(
                            "HOD_IGNITION_BREAKOUT", current_price, avg_price, hod_price,
                            metrics["session_low_price"], metrics["adr_pct"], metrics["atr_14"], metrics["atr_pct"],
                            live_rvol, order_flow_delta_pct, remaining_vol_ratio, vol_progress_pct
                        )
                        if levels:
                            metrics["last_breakout_alert_time"] = current_time
                            triggered_symbols.append(
                                build_signal_payload("HOD_IGNITION_BREAKOUT", "NONE", levels)
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

            def post_webhook(p):
                def numpy_serializer(obj):
                    if isinstance(obj, (np.integer, np.int64, np.int32)):
                        return int(obj)
                    elif isinstance(obj, (np.floating, np.float64, np.float32)):
                        return float(obj)
                    elif isinstance(obj, (np.bool_, bool)):
                        return bool(obj)
                    return str(obj)

                try:
                    payload_json = json.dumps(p, default=numpy_serializer)
                    resp = requests.post(
                        N8N_WEBHOOK_URL, 
                        data=payload_json, 
                        headers={"Content-Type": "application/json"}, 
                        timeout=5
                    )
                    if resp.status_code >= 400:
                        print(f"  [WEBHOOK ERR] n8n returned HTTP {resp.status_code}: {resp.text}")
                except Exception as ex:
                    print(f"  [WEBHOOK EXCEPTION] Failed to dispatch payload to n8n: {ex}")

            threading.Thread(target=post_webhook, args=(payload,), daemon=True).start()


def watchlist_refresher_loop():
    print("--- [THREAD] Mid-Day Watchlist Refresher Loop Started ---")
    while True:
        time.sleep(REFRESH_INTERVAL_SECONDS)
        try:
            latest_targets = get_morning_watchlist()
            if not latest_targets:
                continue

            syms_to_drop = []
            syms_to_sub = []

            # Isolate dictionary state updates inside data_lock
            with data_lock:
                current_time = time.time()
                current_active_syms = set(market_data.keys())
                new_targets = [t for t in latest_targets if t["finnhub_symbol"] not in current_active_syms]

                if new_targets:
                    for t in new_targets:
                        register_symbol_in_memory(t)

                if len(market_data) > MAX_WATCHLIST_SYMBOLS:
                    def eviction_priority(s):
                        m = market_data[s]
                        in_cool = any([
                            (current_time - m.get("last_alert_time", 0)) < ALERT_COOLDOWN_SECONDS,
                            (current_time - m.get("last_reclaim_alert_time", 0)) < ALERT_COOLDOWN_SECONDS,
                            (current_time - m.get("last_pullback_alert_time", 0)) < ALERT_COOLDOWN_SECONDS,
                            (current_time - m.get("last_breakout_alert_time", 0)) < ALERT_COOLDOWN_SECONDS,
                            (current_time - m.get("last_overnight_alert_time", 0)) < ALERT_COOLDOWN_SECONDS,
                        ])
                        active_streak = m.get("reclaim_streak", 0) >= 2
                        priority = m.get("morning_setup_score", 0.0)
                        if in_cool or active_streak:
                            priority += 1000.0  # Shield active runners
                        return priority

                    sorted_syms = sorted(market_data.keys(), key=eviction_priority, reverse=True)
                    syms_to_keep = set(sorted_syms[:MAX_WATCHLIST_SYMBOLS])
                    syms_to_drop = [s for s in market_data.keys() if s not in syms_to_keep]

                    for s in syms_to_drop:
                        del market_data[s]

                syms_to_sub = [t["finnhub_symbol"] for t in new_targets if t["finnhub_symbol"] in market_data]

            # Execute network calls and throttle sleep OUTSIDE the lock to eliminate lock contention
            for s in syms_to_drop:
                send_ws_message({"type": "unsubscribe", "symbol": s})
                time.sleep(0.03)

            for sym in syms_to_sub:
                send_ws_message({"type": "subscribe", "symbol": sym})
                time.sleep(0.03)

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
                        m = market_data[sym]
                        prev_tick = m["prev_tick_price"]
                        is_rebase = False

                        if prev_tick > 0:
                            tick_move_pct = abs(price - prev_tick) / prev_tick * 100.0

                            # Dynamic bad-print / volatility guard with 3-strike rebase
                            if tick_move_pct > MAX_TICK_MOVE_PCT:
                                m["reject_count"] = m.get("reject_count", 0) + 1
                                if m["reject_count"] < 3:
                                    continue  # Treat as transient bad tick
                                
                                # 3 consecutive prints confirm genuine displacement (e.g. halt resumption)
                                print(f"  [REBASE] {sym} accepting ${price:.2f} ({tick_move_pct:+.1f}%) after 3 consecutive rejects")
                                is_rebase = True

                        # Reset reject counter on valid print or confirmed rebase
                        m["reject_count"] = 0

                        # Update price and cumulative dollar volume
                        m["current_price"] = price
                        m["cumulative_volume"] += vol
                        m["total_dollar_traded"] += (price * vol)

                        if price > m["high_of_day_price"]:
                            m["high_of_day_price"] = price

                        # Capture true flush wick extremes on every tick
                        if m["session_low_price"] <= 0 or price < m["session_low_price"]:
                            m["session_low_price"] = price

                        # Tick-accurate live VWAP and session-low VWAP distance
                        if m["cumulative_volume"] > 0:
                            live_vwap = m["total_dollar_traded"] / m["cumulative_volume"]
                            tick_vwap_dist = ((price - live_vwap) / live_vwap) * 100.0

                            if m["session_low_vwap_distance"] is None or tick_vwap_dist < m["session_low_vwap_distance"]:
                                m["session_low_vwap_distance"] = tick_vwap_dist

                            # Real-time tick-accurate dip registration
                            if -2.5 <= tick_vwap_dist <= -0.2:
                                m["dipped_below_vwap"] = True
                                m["dipped_below_vwap_time"] = time.time()
                            elif tick_vwap_dist < -3.0 or tick_vwap_dist > 1.5:
                                m["dipped_below_vwap"] = False

                        # Stateful Lee-Ready Tick Rule (Direction Persistence for Flat Ticks)
                        if prev_tick > 0 and not is_rebase:
                            if price > prev_tick:
                                m["last_tick_direction"] = 1
                            elif price < prev_tick:
                                m["last_tick_direction"] = -1

                            if m["last_tick_direction"] == 1:
                                m["rolling_buy_vol"] += vol
                            else:
                                m["rolling_sell_vol"] += vol

                        m["prev_tick_price"] = price
                        prev_close = m["prev_close"]
                        if prev_close > 0:
                            m["percent_change"] = ((price - prev_close) / prev_close) * 100.0
    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f"WebSocket processing error: {e}")


def on_error(ws, error):
    print(f"WebSocket Error: {error}")


def on_close(ws, close_status_code, close_msg):
    global ws_global
    ws_global = None
    print("### WebSocket Connection Closed ###")


def on_open(ws):
    global ws_global
    ws_global = ws
    print("### WebSocket Connection Established ###")
    with data_lock:
        symbols_to_sub = list(market_data.keys())
    for sym in symbols_to_sub:
        send_ws_message({"type": "subscribe", "symbol": sym})
        time.sleep(0.03)


# ==========================================
# ENTRY POINT
# ==========================================
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
