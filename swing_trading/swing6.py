import os
import sys
import json
import time
import warnings
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
import requests
import yfinance as yf
from yfinance import EquityQuery

# Silence internal yfinance / pandas deprecation warnings
warnings.filterwarnings('ignore')

# ==========================================
# CONFIGURATION & INSTITUTIONAL PARAMETERS
# ==========================================
N8N_SWING_WEBHOOK_URL = os.getenv(
    "N8N_SWING_WEBHOOK_URL",
    "https://go90ng-n8n.eq7icp.easypanel.host/webhook/b5af74d8-d66a-4bc1-b615-ed572b5b4053"
)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
SEEN_SYMBOLS_FILE = os.path.join(DATA_DIR, "seen_symbols.json")

# --- SCREENER UNIVERSE CONSTRAINTS ---
MIN_MARKET_CAP = 250_000_000         # $500M minimum capitalization
MIN_AVG_VOLUME = 1_000_000             # Minimum 3-month average liquidity
MIN_PRICE = 1.00                    # Eliminates penny stock microstructure noise
UNIVERSE_SIZE = 1250                 # Screen depth
PAGE_SIZE = 250

# --- BENCHMARK ETFS ---
INDEX_BENCHMARKS = ["SPY", "QQQ", "IWM", "XLK", "XLF"]

# --- VOLATILITY & RISK PARAMETERS ---
MIN_ADR_PCT = 1.2                    # Baseline statistical daily range required to qualify
MAX_STOP_LOSS_PCT = 5.0              # Hard maximum risk per trade
MIN_REWARD_RISK_RATIO = 1.5          # Minimum R:R floor
MIN_TARGET_GAIN_PCT = 5.0            # Minimum profit target hurdle

# --- STRATEGY 1: PULLBACK GATES ---
MAX_EXTENSION_50_PCT = 15.0          # Max distance from 50 SMA
PULLBACK_LOW, PULLBACK_HIGH = -2.5, 3.0  # Allowable % proximity to 21 EMA
ATR_PULLBACK_STOP_MULT = 1.5

# --- STRATEGY 2: MOMENTUM GATES ---
MIN_RVOL_MOMENTUM = 1.2
MIN_ADX = 20.0                       # Established trend threshold
BREAKOUT_PROXIMITY_PCT = -3.0        # Max % below 20-day high
MIN_GREEN_DAYS_5D = 3
MAX_ATR_JUMP_MULTIPLIER = 3.0        # Exhaustion ceiling: max 3x ATR daily move

# --- STRATEGY 3: OVERNIGHT GAP GATES ---
GAP_MIN_RVOL = 1.5                   # Minimum institutional volume
GAP_MIN_CLOSE_RANGE = 0.88           # Closed in top 12% of intraday range
GAP_MIN_DAY_PCT = 3.0                # Minimum % expansion
GAP_ATR_STOP_MULT = 0.5

# --- SYSTEM HYGIENE & COOLDOWNS ---
EARNINGS_BLACKOUT_DAYS = 0           # Skip stocks with earnings within 7 days
DEDUP_WINDOW_DAYS = 21               # Prevent alert spamming for 21 days
MAX_ATR_GAP_MULTIPLIER = 1.5         # Drop gaps larger than 1.5x daily ATR

# ==========================================
# EXECUTION & MATHEMATICAL LOGIC
# ==========================================
def adjust_for_whole_dollar(target_price: float, entry_price: float, is_stop: bool = False) -> float:
    """
    Front-runs psychological whole-dollar ($X.00) and half-dollar ($X.50) resistance walls.
    Snaps profit targets just below resistance and stop-losses just below support.
    """
    if target_price <= 1.00:
        return round(target_price, 2)

    whole_dollar = int(target_price)
    cents = round(target_price - whole_dollar, 2)

    if not is_stop:
        # Target adjustment: front-run whole-dollar resistance ($X.00 - $X.08 -> $(X-1).97)
        if 0.0 <= cents <= 0.08 and whole_dollar > entry_price:
            adjusted = whole_dollar - 0.03
            if adjusted > entry_price * 1.01:
                return round(adjusted, 2)
        # Target adjustment: front-run half-dollar resistance ($X.50 - $X.54 -> $X.47)
        elif 0.50 <= cents <= 0.54 and (whole_dollar + 0.50) > entry_price:
            adjusted = whole_dollar + 0.47
            if adjusted > entry_price * 1.01:
                return round(adjusted, 2)
    else:
        # Stop-loss adjustment: place stop just below psychological round support ($X.00 -> $X.94)
        if 0.0 <= cents <= 0.05 and whole_dollar < entry_price:
            adjusted = whole_dollar - 0.06
            return round(adjusted, 2)

    return round(target_price, 2)


def evaluate_adr_feasibility(upside_potential: float, adr_pct: float) -> tuple[float, str]:
    """Validates whether a projected target is statistically realistic relative to ADR."""
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


def get_mcap_tier(mcap: float) -> str:
    if not mcap or mcap <= 0: return "Unknown Cap"
    if mcap >= 200_000_000_000: return "Mega Cap (>$200B)"
    if mcap >= 10_000_000_000: return "Large Cap ($10B-$200B)"
    if mcap >= 2_000_000_000: return "Mid Cap ($2B-$10B)"
    if mcap >= 300_000_000: return "Small Cap ($300M-$2B)"
    return "Micro Cap (<$300M)"


def score_swing_setup(
    setup_type: str,
    trend_alignment: str,
    rvol: float,
    adx_val: float,
    daily_rsi: float,
    ret_7d: float,
    green_days: int,
    adr_pct: float,
    ext_50: float,
    reward_risk: float
) -> tuple[float, str]:
    """Multi-factor quantitative scoring engine (0-100 scale)."""
    score = 0.0

    # Trend alignment & regime quality
    if trend_alignment == "Bullish":
        score += 25.0
    elif trend_alignment == "Mixed":
        score += 10.0

    # Trend strength (ADX) & directional momentum
    score += min(adx_val, 40.0) * 0.4
    score += min(rvol, 4.0) * 5.0
    score += min(adr_pct, 8.0) * 1.5

    # Consistency in trailing candles
    score += (green_days / 5.0) * 10.0
    if ret_7d > 2.0:
        score += 10.0

    # Risk-to-reward efficiency
    score += min(reward_risk, 4.0) * 5.0

    # Penalties for overextension & exhaustions
    if daily_rsi > 78.0:
        score -= 15.0
    elif daily_rsi < 40.0 and setup_type != "BUY_PULLBACK":
        score -= 10.0

    if ext_50 > 25.0:
        score -= 15.0
    elif ext_50 > 15.0:
        score -= 5.0

    final_score = max(0.0, min(100.0, round(score, 1)))

    if final_score >= 80.0:
        setup_class = "Top-tier"
    elif final_score >= 65.0:
        setup_class = "Strong"
    elif final_score >= 50.0:
        setup_class = "Watch"
    else:
        setup_class = "Low"

    return final_score, setup_class


# ==========================================
# UNIVERSE & METRIC EXTRACTION
# ==========================================
def extract_series(data_frame: pd.DataFrame, col_name: str, ticker: str) -> pd.Series | None:
    """Safe extraction utility for multi-index yfinance downloads."""
    if col_name not in data_frame:
        return None
    sub = data_frame[col_name]
    if isinstance(sub, pd.DataFrame):
        return sub[ticker].dropna() if ticker in sub.columns else None
    elif isinstance(sub, pd.Series):
        return sub.dropna()
    return None


def get_swing_universe() -> tuple[list[str], dict[str, str]]:
    print("\n--- INITIATING INSTITUTIONAL UNIVERSE DISCOVERY ---")
    try:
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', MIN_MARKET_CAP]),
            EquityQuery('gte', ['avgdailyvol3m', MIN_AVG_VOLUME]),
            EquityQuery('gt',  ['intradayprice', MIN_PRICE])
        ])

        symbols = []
        symbol_map = {}

        for offset in range(0, UNIVERSE_SIZE, PAGE_SIZE):
            response = yf.screen(q, sortField='intradaymarketcap', sortAsc=False, size=PAGE_SIZE, offset=offset)
            quotes = response.get('quotes', [])
            if not quotes:
                break

            for item in quotes:
                sym = item.get("symbol")
                if sym and sym not in symbols:
                    symbols.append(sym)
                    symbol_map[sym] = item.get("shortName", sym)
            time.sleep(0.08)

        symbols = symbols[:UNIVERSE_SIZE]

        print(f"Stitching {len(INDEX_BENCHMARKS)} benchmark ETFs for broad-market regime context...")
        for etf in INDEX_BENCHMARKS:
            if etf not in symbols:
                symbols.append(etf)
                symbol_map[etf] = etf

        print(f"Verified {len(symbols)} liquid targets. Proceeding to bulk historical data pull...")
        return symbols, symbol_map

    except Exception as e:
        print(f"Failed to discover stock universe: {e}")
        sys.exit(1)


# ==========================================
# STATE & PERSISTENCE
# ==========================================
def load_seen_symbols() -> dict:
    if os.path.exists(SEEN_SYMBOLS_FILE):
        try:
            with open(SEEN_SYMBOLS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_seen_symbols(seen: dict):
    try:
        with open(SEEN_SYMBOLS_FILE, "w") as f:
            json.dump(seen, f, indent=2)
    except Exception as e:
        print(f"Failed to persist state: {e}")


def clean_old_seen_symbols(seen: dict) -> dict:
    cutoff = datetime.now() - timedelta(days=DEDUP_WINDOW_DAYS)
    return {sym: d for sym, d in seen.items() if datetime.fromisoformat(d) > cutoff}


def recently_alerted(symbol: str, strategy_prefix: str, seen: dict) -> bool:
    key = f"{strategy_prefix}_{symbol}"
    if key not in seen:
        return False
    return (datetime.now() - datetime.fromisoformat(seen[key])) < timedelta(days=DEDUP_WINDOW_DAYS)


def has_upcoming_earnings(symbol: str, blackout_days: int = EARNINGS_BLACKOUT_DAYS) -> bool:
    if symbol in INDEX_BENCHMARKS:
        return False
    try:
        cal = yf.Ticker(symbol).get_earnings_dates(limit=4)
        if cal is None or cal.empty:
            return False
        today = pd.Timestamp.now(tz=cal.index.tz)
        upcoming = cal.index[cal.index >= today]
        if len(upcoming) == 0:
            return False
        return 0 <= (upcoming.min() - today).days <= blackout_days
    except Exception:
        return False


# ==========================================
# MAIN QUANT ENGINE
# ==========================================
def run_macro_analysis():
    symbols, symbol_map = get_swing_universe()
    if not symbols:
        return

    seen = clean_old_seen_symbols(load_seen_symbols())

    # Bulk vectorized download (threads=False prevents SQLite concurrency lockups)
    data = yf.download(symbols, period="1y", group_by='column', progress=False, threads=False)

    pullback_setups = []
    momentum_setups = []
    gap_setups = []
    index_metrics = {}

    dynamic_benchmarks = {
        "pullback": {"symbol": "N/A", "5d_return": -999.0, "metrics": {}},
        "momentum": {"symbol": "N/A", "5d_return": -999.0, "metrics": {}},
        "gap": {"symbol": "N/A", "today_change": -999.0, "metrics": {}}
    }

    uk_time = datetime.now(ZoneInfo("Europe/London"))
    scan_timestamp = uk_time.strftime("%Y-%m-%d %H:%M:%S BST")

    print(f"\n--- EXECUTING QUANTITATIVE SCAN & VOLATILITY BENCHMARKING @ {scan_timestamp} ---")

    for sym in symbols:
        try:
            s_close = extract_series(data, 'Close', sym)
            s_open = extract_series(data, 'Open', sym)
            s_high = extract_series(data, 'High', sym)
            s_low = extract_series(data, 'Low', sym)
            s_vol = extract_series(data, 'Volume', sym)

            if s_close is None or len(s_close) < 200:
                continue

            company_name = symbol_map.get(sym, sym)

            # Core Moving Averages
            ema_8 = s_close.ewm(span=8, adjust=False).mean()
            ema_21 = s_close.ewm(span=21, adjust=False).mean()
            sma_50 = s_close.rolling(window=50).mean()
            sma_200 = s_close.rolling(window=200).mean()

            # True Range & ATR(14) via Wilder Exponential Weighting
            high_low = s_high - s_low
            high_close = (s_high - s_close.shift()).abs()
            low_close = (s_low - s_close.shift()).abs()
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr_series = true_range.ewm(alpha=1/14, adjust=False).mean()

            # Directional Movement Index (ADX 14)
            up = s_high - s_high.shift(1)
            down = s_low.shift(1) - s_low
            plus_dm = np.where((up > down) & (up > 0), up, 0.0)
            minus_dm = np.where((down > up) & (down > 0), down, 0.0)
            tr_rma = true_range.ewm(alpha=1/14, adjust=False).mean()
            plus_di = 100 * (pd.Series(plus_dm, index=s_close.index).ewm(alpha=1/14, adjust=False).mean() / tr_rma)
            minus_di = 100 * (pd.Series(minus_dm, index=s_close.index).ewm(alpha=1/14, adjust=False).mean() / tr_rma)
            dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1))
            adx_series = dx.ewm(alpha=1/14, adjust=False).mean()

            # 14-Period RSI
            delta = s_close.diff()
            avg_gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
            avg_loss = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
            rs = avg_gain / avg_loss.replace(0, 1e-9)
            rsi_series = 100.0 - (100.0 / (1.0 + rs))

            # 21-Day ADR Calculation
            daily_pct_ranges = ((s_high.tail(21) - s_low.tail(21)) / s_low.tail(21)) * 100.0
            adr_pct = float(daily_pct_ranges.mean())

            # Rolling 20-Day VWAP
            vwap_20d = 0.0
            if s_vol is not None:
                common_idx = s_close.tail(20).index
                typ_p = (s_high.loc[common_idx] + s_low.loc[common_idx] + s_close.loc[common_idx]) / 3.0
                v_sum = s_vol.loc[common_idx].sum()
                if v_sum > 0:
                    vwap_20d = float((typ_p * s_vol.loc[common_idx]).sum() / v_sum)

            # Point-in-time Variables
            current_price = float(s_close.iloc[-1])
            prev_close = float(s_close.iloc[-2])
            today_change_pct = ((current_price - prev_close) / prev_close) * 100.0
            today_open = float(s_open.iloc[-1]) if s_open is not None else prev_close
            gap_pct = ((today_open - prev_close) / prev_close) * 100.0

            ema_8_val = float(ema_8.iloc[-1])
            ema_21_val = float(ema_21.iloc[-1])
            sma_50_val = float(sma_50.iloc[-1])
            sma_200_val = float(sma_200.iloc[-1])
            atr_val = float(atr_series.iloc[-1])
            adx_val = float(adx_series.iloc[-1])
            rsi_val = float(rsi_series.iloc[-1])

            atr_pct = (atr_val / current_price) * 100.0 if current_price > 0 else 0.0
            dynamic_max_jump_pct = MAX_ATR_JUMP_MULTIPLIER * atr_pct
            dynamic_max_gap_pct = MAX_ATR_GAP_MULTIPLIER * atr_pct

            # Trend Alignment State
            if ema_8_val > ema_21_val > sma_50_val > sma_200_val:
                trend_alignment = "Bullish"
            elif ema_8_val < ema_21_val < sma_50_val < sma_200_val:
                trend_alignment = "Bearish"
            else:
                trend_alignment = "Mixed"

            # Multi-Session Consistency
            daily_returns = s_close.pct_change() * 100.0
            last_5_returns = daily_returns.tail(5)
            green_days_5d = int((last_5_returns > 0).sum())
            max_daily_jump_5d = float(last_5_returns.max())
            ret_5d = ((current_price - float(s_close.iloc[-6])) / float(s_close.iloc[-6])) * 100.0

            lookback_7d = min(8, len(s_close))
            rets_7d = s_close.tail(lookback_7d).pct_change().dropna()
            green_7d = int((rets_7d > 0).sum())
            red_7d = int((rets_7d < 0).sum())
            p_prior_7d = float(s_close.iloc[-lookback_7d])
            ret_7d = ((current_price - p_prior_7d) / p_prior_7d) * 100.0

            # Volume & Range Profiling
            completed_vol = s_vol.iloc[:-1] if len(s_vol) > 1 else s_vol
            vol_20d_avg = float(completed_vol.tail(20).mean())
            today_vol = float(s_vol.iloc[-1])
            rvol = round(today_vol / vol_20d_avg, 2) if vol_20d_avg > 0 else 1.0

            # Volume Fuel Factor (bounded between 0.65x and 1.55x)
            vol_fuel_factor = max(0.65, min(1.55, float(np.sqrt(max(0.01, rvol)))))

            recent_20d_high = float(s_close.tail(20).max())
            dist_to_high_pct = ((current_price - recent_20d_high) / recent_20d_high) * 100.0
            dist_21 = ((current_price - ema_21_val) / ema_21_val) * 100.0
            ext_50 = ((current_price - sma_50_val) / sma_50_val) * 100.0

            # Intraday Candle Geometry
            high_today = float(s_high.iloc[-1])
            low_today = float(s_low.iloc[-1])
            intraday_range = high_today - low_today
            close_range_position = (current_price - low_today) / intraday_range if intraday_range > 0 else 0.5
            prev_20d_high_excl_today = float(s_high.iloc[-21:-1].max()) if len(s_high) >= 22 else recent_20d_high
            is_fresh_breakout = current_price > prev_20d_high_excl_today

            # --- STATIC BENCHMARK ETF CAPTURE ---
            if sym in INDEX_BENCHMARKS:
                index_metrics[sym] = {
                    "symbol": sym,
                    "company_name": company_name,
                    "5d_return_pct": round(ret_5d, 2),
                    "adx_strength": round(adx_val, 2),
                    "rsi": round(rsi_val, 1),
                    "green_days_last_5": green_days_5d,
                    "dist_to_21_ema_pct": round(dist_21, 2)
                }
                continue

            # --- DYNAMIC BENCHMARK LEADERS ---
            if ret_5d > dynamic_benchmarks["momentum"]["5d_return"] and current_price > sma_200_val:
                if max_daily_jump_5d <= dynamic_max_jump_pct:
                    dynamic_benchmarks["momentum"] = {
                        "symbol": sym, "5d_return": ret_5d,
                        "metrics": {
                            "symbol": sym,
                            "company_name": company_name,
                            "5d_return_pct": round(ret_5d, 2),
                            "green_days_last_5": green_days_5d,
                            "adx_strength": round(adx_val, 2),
                            "rvol": rvol
                        }
                    }

            price_5d_ago = float(s_close.iloc[-6])
            ema21_5d_ago = float(ema_21.iloc[-6])
            dist_21_5d_ago = ((price_5d_ago - ema21_5d_ago) / ema21_5d_ago) * 100.0
            if PULLBACK_LOW <= dist_21_5d_ago <= PULLBACK_HIGH:
                if ret_5d > dynamic_benchmarks["pullback"]["5d_return"]:
                    dynamic_benchmarks["pullback"] = {
                        "symbol": sym, "5d_return": ret_5d,
                        "metrics": {
                            "symbol": sym,
                            "company_name": company_name,
                            "5d_return_pct": round(ret_5d, 2),
                            "adx_strength": round(adx_val, 2)
                        }
                    }

            if today_change_pct > dynamic_benchmarks["gap"]["today_change"]:
                if rvol >= GAP_MIN_RVOL and close_range_position >= GAP_MIN_CLOSE_RANGE:
                    dynamic_benchmarks["gap"] = {
                        "symbol": sym, "today_change": today_change_pct,
                        "metrics": {
                            "symbol": sym,
                            "company_name": company_name,
                            "today_change_pct": round(today_change_pct, 2),
                            "rvol": rvol,
                            "close_range_position_pct": round(close_range_position * 100, 1)
                        }
                    }

            # Universal Quality Gates
            is_recent_swing = recently_alerted(sym, "SWING", seen)
            is_recent_gap = recently_alerted(sym, "GAP", seen)
            pending_earnings = has_upcoming_earnings(sym)
            recent_gap_exhaustion = abs(gap_pct) > dynamic_max_gap_pct
            is_above_200 = (sma_50_val > sma_200_val and current_price > sma_200_val)

            try:
                mcap = yf.Ticker(sym).fast_info.market_cap
            except Exception:
                mcap = 0

            # ==============================================================
            # STRATEGY 1: 21-EMA PULLBACK (SWING MEAN-REVERSION)
            # ==============================================================
            if is_above_200 and not recent_gap_exhaustion and not pending_earnings and not is_recent_swing:
                if ext_50 <= MAX_EXTENSION_50_PCT and PULLBACK_LOW <= dist_21 <= PULLBACK_HIGH:
                    if completed_vol.tail(3).mean() < vol_20d_avg:
                        upside_to_swing_high = abs(dist_to_high_pct)
                        if upside_to_swing_high >= MIN_TARGET_GAIN_PCT:
                            raw_stop = ema_21_val - (ATR_PULLBACK_STOP_MULT * atr_val)
                            stop_loss_pb = adjust_for_whole_dollar(raw_stop, current_price, is_stop=True)
                            risk_per_share = max(0.01, current_price - stop_loss_pb)
                            risk_pct_pb = (risk_per_share / current_price) * 100.0

                            # Dynamic Target scaled by Volume Fuel Factor & Whole-Dollar Alignment
                            raw_target = current_price + ((recent_20d_high - current_price) * vol_fuel_factor)
                            target_pb = adjust_for_whole_dollar(max(raw_target, recent_20d_high), current_price)
                            est_gain_pct = ((target_pb - current_price) / current_price) * 100.0
                            rr_ratio = round((target_pb - current_price) / risk_per_share, 2)

                            target_vs_adr, adr_feasibility = evaluate_adr_feasibility(est_gain_pct, adr_pct)

                            if risk_pct_pb <= MAX_STOP_LOSS_PCT and rr_ratio >= MIN_REWARD_RISK_RATIO:
                                score, s_class = score_swing_setup(
                                    "BUY_PULLBACK", trend_alignment, rvol, adx_val, rsi_val,
                                    ret_7d, green_days_5d, adr_pct, ext_50, rr_ratio
                                )
                                pullback_setups.append({
                                    "symbol": sym,
                                    "company_name": company_name,
                                    "action": "BUY_PULLBACK",
                                    "setup_score": score,
                                    "setup_class": s_class,
                                    "market_cap_tier": get_mcap_tier(mcap),
                                    "trend_alignment": trend_alignment,
                                    "signal_timestamp": scan_timestamp,
                                    "current_price": round(current_price, 2),
                                    "target_price": target_pb,
                                    "estimated_gain_pct": round(est_gain_pct, 2),
                                    "recommended_stop_loss": stop_loss_pb,
                                    "risk_pct": round(risk_pct_pb, 2),
                                    "reward_risk_ratio": rr_ratio,
                                    "adr_pct": round(adr_pct, 2),
                                    "target_vs_adr_pct": target_vs_adr,
                                    "adr_feasibility": adr_feasibility,
                                    "volume_fuel_factor": round(vol_fuel_factor, 2),
                                    "rvol": rvol,
                                    "daily_rsi": round(rsi_val, 1),
                                    "adx_strength": round(adx_val, 2),
                                    "ema_21": round(ema_21_val, 2),
                                    "vwap_20d": round(vwap_20d, 2)
                                })

            # ==============================================================
            # STRATEGY 2: CONSISTENT MOMENTUM (TREND ACCELERATION)
            # ==============================================================
            if is_above_200 and not recent_gap_exhaustion and not pending_earnings and not is_recent_swing:
                if dist_to_high_pct >= BREAKOUT_PROXIMITY_PCT and current_price > ema_8_val > ema_21_val:
                    if green_days_5d >= MIN_GREEN_DAYS_5D and max_daily_jump_5d <= dynamic_max_jump_pct:
                        if rvol >= MIN_RVOL_MOMENTUM and adx_val >= MIN_ADX and rsi_val < 78.0:
                            raw_stop = ema_8_val - (0.5 * atr_val)
                            stop_loss_mom = adjust_for_whole_dollar(raw_stop, current_price, is_stop=True)
                            risk_per_share = max(0.01, current_price - stop_loss_mom)
                            risk_pct_mom = (risk_per_share / current_price) * 100.0

                            # Asymmetric 2R Expansion scaled by Volume Fuel Factor
                            target_expansion = risk_per_share * 2.0 * vol_fuel_factor
                            raw_target = current_price + max(target_expansion, current_price * (MIN_TARGET_GAIN_PCT / 100.0))
                            target_mom = adjust_for_whole_dollar(raw_target, current_price)
                            est_gain_pct = ((target_mom - current_price) / current_price) * 100.0
                            rr_ratio = round((target_mom - current_price) / risk_per_share, 2)

                            target_vs_adr, adr_feasibility = evaluate_adr_feasibility(est_gain_pct, adr_pct)

                            if risk_pct_mom <= MAX_STOP_LOSS_PCT and rr_ratio >= MIN_REWARD_RISK_RATIO:
                                score, s_class = score_swing_setup(
                                    "BUY_MOMENTUM", trend_alignment, rvol, adx_val, rsi_val,
                                    ret_7d, green_days_5d, adr_pct, ext_50, rr_ratio
                                )
                                momentum_setups.append({
                                    "symbol": sym,
                                    "company_name": company_name,
                                    "action": "BUY_MOMENTUM",
                                    "setup_score": score,
                                    "setup_class": s_class,
                                    "market_cap_tier": get_mcap_tier(mcap),
                                    "trend_alignment": trend_alignment,
                                    "signal_timestamp": scan_timestamp,
                                    "current_price": round(current_price, 2),
                                    "target_price": target_mom,
                                    "estimated_gain_pct": round(est_gain_pct, 2),
                                    "recommended_stop_loss": stop_loss_mom,
                                    "risk_pct": round(risk_pct_mom, 2),
                                    "reward_risk_ratio": rr_ratio,
                                    "adr_pct": round(adr_pct, 2),
                                    "target_vs_adr_pct": target_vs_adr,
                                    "adr_feasibility": adr_feasibility,
                                    "volume_fuel_factor": round(vol_fuel_factor, 2),
                                    "rvol": rvol,
                                    "daily_rsi": round(rsi_val, 1),
                                    "adx_strength": round(adx_val, 2),
                                    "ema_8": round(ema_8_val, 2),
                                    "vwap_20d": round(vwap_20d, 2)
                                })

            # ==============================================================
            # STRATEGY 3: OVERNIGHT GAP & BREAKOUT
            # ==============================================================
            # ==============================================================
            # STRATEGY 3: OVERNIGHT GAP ANTICIPATION (EOD IGNITION)
            # Screens during the final 45 mins of market hours (15:15-15:58 ET)
            # for stocks structurally primed to gap up at tomorrow's open.
            # ==============================================================
            if not pending_earnings and not is_recent_gap:
                
                # --- 1. MICROSTRUCTURE & AUCTION METRICS ---
                day_spread = high_today - low_today
                intraday_clv_pct = ((current_price - low_today) / day_spread * 100.0) if day_spread > 0 else 50.0
                distance_to_hod_pct = ((high_today - current_price) / current_price * 100.0) if current_price > 0 else 0.0

                # Derive Order Flow Delta from 1m bars (or tick estimator)
                # Compares volume on up-bars (close >= open) vs down-bars (close < open) in the EOD window
                order_flow_delta_pct = 0.0
                try:
                    # Extracts 1m intraday bars for the current session
                    bars_1m_sym = extract_series(bars_1m, 'Close', sym)
                    if bars_1m_sym is not None and len(bars_1m_sym) >= 30:
                        c_sub = extract_series(bars_1m, 'Close', sym).tail(30)
                        o_sub = extract_series(bars_1m, 'Open', sym).tail(30)
                        v_sub = extract_series(bars_1m, 'Volume', sym).tail(30)
                        
                        buy_vol = v_sub[c_sub >= o_sub].sum()
                        sell_vol = v_sub[c_sub < o_sub].sum()
                        total_window_vol = buy_vol + sell_vol
                        
                        if total_window_vol > 0:
                            order_flow_delta_pct = ((buy_vol - sell_vol) / total_window_vol) * 100.0
                except Exception:
                    # Fallback to daily candle proxy if 1m feed is unavailable
                    order_flow_delta_pct = (intraday_clv_pct - 50.0) * 0.8

                # ADR Exhaustion Ratio & Catalyst Override
                day_range_pct = (day_spread / low_today * 100.0) if low_today > 0 else 0.0
                adr_exhaustion_ratio = (day_range_pct / adr_pct) if adr_pct > 0 else 1.0
                has_adr_fuel = (adr_exhaustion_ratio <= 1.35) or (rvol >= 3.0)

                # --- 2. THE UNIFIED OVERNIGHT GAP GATING CONDITIONS ---
                is_gap_anticipation = (
                    trend_alignment == "Bullish" and              # Full EMA/SMA bullish stack
                    current_price > ema_8_val and                 # Riding fastest trend guide
                    intraday_clv_pct >= 85.0 and                  # Pinned in top 15% of day's range
                    distance_to_hod_pct <= 1.5 and                # Imminent or breaking HOD
                    order_flow_delta_pct >= 20.0 and              # Aggressive net buyer delta
                    rvol >= 2.0 and                               # Heavy institutional sponsorship
                    vol_fuel_factor >= 1.40 and                   # Volume fuel confirms velocity
                    adr_pct >= 1.2 and                            # Sufficient baseline daily range
                    has_adr_fuel and                              # Unexhausted range or catalyst expansion
                    today_change_pct >= 2.5 and                   # Decisive green day
                    current_price >= vwap_20d                     # Above rolling baseline
                )

                if is_gap_anticipation:
                    # --- 3. RISK ENGINE & TARGET PROJECTION ---
                    # Stop is anchored to the intraday support floor / 8-EMA
                    structural_stop = max(low_today, ema_8_val - (0.35 * atr_val))
                    hard_risk_floor = current_price * (1.0 - (MAX_STOP_LOSS_PCT / 100.0))
                    raw_stop = max(structural_stop, hard_risk_floor)
                    stop_loss_gap = adjust_for_whole_dollar(raw_stop, current_price, is_stop=True)

                    risk_per_share = max(0.01, current_price - stop_loss_gap)
                    risk_pct_gap = (risk_per_share / current_price) * 100.0

                    # Target projects expected opening gap expansion scaled by ADR and Volume Fuel
                    projected_gap_expansion_pct = max(MIN_TARGET_GAIN_PCT, adr_pct * 0.75 * vol_fuel_factor)
                    raw_target = current_price * (1.0 + (projected_gap_expansion_pct / 100.0))
                    target_gap = adjust_for_whole_dollar(raw_target, current_price)

                    est_gain_pct = ((target_gap - current_price) / current_price) * 100.0
                    rr_ratio = round((target_gap - current_price) / risk_per_share, 2)

                    target_vs_adr, adr_feasibility = evaluate_adr_feasibility(est_gain_pct, adr_pct)

                    # --- 4. QUALIFICATION & INSTITUTIONAL SCORING ---
                    if risk_pct_gap <= MAX_STOP_LOSS_PCT and rr_ratio >= MIN_REWARD_RISK_RATIO:
                        score, s_class = score_swing_setup(
                            "BUY_GAP_ANTICIPATION", trend_alignment, rvol, adx_val, rsi_val,
                            ret_7d, green_days_5d, adr_pct, ext_50, rr_ratio
                        )
                        
                        # Add conviction points for extreme CLV and order flow delta
                        if intraday_clv_pct >= 92.0: score = min(100.0, score + 6.0)
                        if order_flow_delta_pct >= 35.0: score = min(100.0, score + 6.0)

                        gap_setups.append({
                            "symbol": sym,
                            "company_name": company_name,
                            "action": "BUY_GAP_ANTICIPATION",
                            "setup_score": score,
                            "setup_class": s_class,
                            "market_cap_tier": get_mcap_tier(mcap),
                            "trend_alignment": trend_alignment,
                            "signal_timestamp": scan_timestamp,
                            "current_price": round(current_price, 2),
                            "target_price": target_gap,
                            "estimated_gain_pct": round(est_gain_pct, 2),
                            "recommended_stop_loss": stop_loss_gap,
                            "risk_pct": round(risk_pct_gap, 2),
                            "reward_risk_ratio": rr_ratio,
                            "intraday_clv_pct": round(intraday_clv_pct, 1),
                            "distance_to_hod_pct": round(distance_to_hod_pct, 2),
                            "order_flow_delta_pct": round(order_flow_delta_pct, 1),
                            "rvol": rvol,
                            "volume_fuel_factor": round(vol_fuel_factor, 2),
                            "adr_pct": round(adr_pct, 2),
                            "target_vs_adr_pct": target_vs_adr,
                            "adr_feasibility": adr_feasibility,
                            "daily_rsi": round(rsi_val, 1),
                            "adx_strength": round(adx_val, 2),
                            "ema_8": round(ema_8_val, 2),
                            "vwap_20d": round(vwap_20d, 2)
                        })

        except Exception as e:
            continue

    # ==============================================================
    # RANKING, SERIALIZATION & WEBHOOK DISPATCH
    # ==============================================================
    total_setups = len(pullback_setups) + len(momentum_setups) + len(gap_setups)

    if total_setups > 0:
        top_pullbacks = sorted(pullback_setups, key=lambda x: x['setup_score'], reverse=True)[:3]
        top_momentums = sorted(momentum_setups, key=lambda x: x['setup_score'], reverse=True)[:3]
        top_gaps = sorted(gap_setups, key=lambda x: x['setup_score'], reverse=True)[:3]

        payload = {
            "scan_type": "adaptive_swing_screener_upgraded",
            "global_timestamp": scan_timestamp,
            "total_qualified_count": total_setups,
            "pullback_setups": top_pullbacks,
            "momentum_setups": top_momentums,
            "gap_setups": top_gaps,
            "benchmarks": {
                "market_indices": index_metrics,
                "dynamic_leaders_last_5_days": {
                    "pullback_leader": dynamic_benchmarks["pullback"]["metrics"],
                    "momentum_leader": dynamic_benchmarks["momentum"]["metrics"],
                    "gap_leader": dynamic_benchmarks["gap"]["metrics"]
                }
            }
        }

        print(f"\n>>> FIRING N8N WEBHOOK: {len(top_pullbacks)} Pullbacks, {len(top_momentums)} Momentum, {len(top_gaps)} Gaps.")

        def numpy_serializer(obj):
            if isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64, np.float32)):
                return float(obj)
            elif isinstance(obj, (np.bool_, bool)):
                return bool(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            return str(obj)

        try:
            payload_json = json.dumps(payload, default=numpy_serializer)
            response = requests.post(
                N8N_SWING_WEBHOOK_URL,
                data=payload_json,
                headers={"Content-Type": "application/json"},
                timeout=10
            )

            if response.status_code == 200:
                print("Webhook delivered successfully (HTTP 200). Updating persistent deduplication state...")
                for setup in top_pullbacks + top_momentums:
                    seen[f"SWING_{setup['symbol']}"] = datetime.now().isoformat()
                for setup in top_gaps:
                    seen[f"GAP_{setup['symbol']}"] = datetime.now().isoformat()
                save_seen_symbols(seen)
            else:
                print(f"Warning: Webhook failed with status code {response.status_code}: {response.text}")

        except Exception as ex:
            print(f"Failed to post payload to webhook: {ex}")

    else:
        print("\nNo setups met the institutional criteria today. Cash remains a valid position.")


if __name__ == "__main__":
    run_macro_analysis()
