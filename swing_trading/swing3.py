import pandas as pd
import numpy as np
import yfinance as yf
from yfinance import EquityQuery
import requests
import json
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ==========================================
# CONFIGURATION
# ==========================================
N8N_SWING_WEBHOOK_URL = "https://go90ng-n8n.eq7icp.easypanel.host/webhook/b5af74d8-d66a-4bc1-b615-ed572b5b4053"

# --- PERSISTENT VOLUME STORAGE SETUP ---
# Maps directly to Easypanel Mount Path: /app/data
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
SEEN_SYMBOLS_FILE = os.path.join(DATA_DIR, "seen_symbols.json")

# --- SCREENER PARAMETERS ---
MIN_MARKET_CAP = 500_000_000      # $500M+ Market Cap
MIN_AVG_VOLUME = 1_000_000          # 1M+ shares/day average volume
MIN_PRICE = 10.00                   # $10.00 Minimum price
UNIVERSE_SIZE = 1000                 # Top 1000 largest caps scanned weekly

# --- QUANTITATIVE LOGIC GATES ---
MAX_EXTENSION_PCT = 15.0            # Gate 2: Don't chase >15% above 50-SMA
PULLBACK_LOW, PULLBACK_HIGH = -2.5, 3.0   # Gate 3: Distance band around 21-EMA (%)
MIN_UPSIDE_TO_HIGH_PCT = 5.0        # Gate 4: Minimum upside potential to 20d high

# --- RISK & VOLATILITY FILTERS ---
ATR_PERIOD = 14                     # 14-day Average True Range
ATR_STOP_MULTIPLIER = 1.5           # Stop = 21-EMA minus (1.5 * ATR)
MIN_REWARD_RISK_RATIO = 1.5         # Reward must be >= 1.5x Risk

# --- FILTERS & STATE ---
EARNINGS_BLACKOUT_DAYS = 7          # Skip stocks with earnings in <= 7 days
DEDUP_WINDOW_DAYS = 21              # Don't re-alert same stock within 21 days

# --- GAP CONTAMINATION GUARD ---
GAP_THRESHOLD_PCT = 5.0             # Max allowed overnight gap (yesterday close to today open)

# --- CONFIDENCE SCORE WEIGHTS ---
CONFIDENCE_BASE = 50
CONFIDENCE_RR_CAP = 20                # max points contributed by reward:risk
CONFIDENCE_EXTENSION_FREE_PCT = 10.0  # no penalty until extension exceeds this


# ==========================================
# UNIVERSE SELECTION
# ==========================================
def get_swing_universe():
    print("\n--- BOOTING MACRO SWING SCREENER ---")
    try:
        print(f"Scanning for US stocks with Market Cap >= ${MIN_MARKET_CAP:,}, "
              f"Avg Vol >= {MIN_AVG_VOLUME:,}, Price >= ${MIN_PRICE}...")
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', MIN_MARKET_CAP]),
            EquityQuery('gte', ['avgdailyvol3m', MIN_AVG_VOLUME]),
            EquityQuery('gt',  ['intradayprice', MIN_PRICE])
        ])

        response = yf.screen(q, sortField='intradaymarketcap', sortAsc=False)
        quotes = response.get('quotes', [])

        symbols = [item.get("symbol") for item in quotes if item.get("symbol")][:UNIVERSE_SIZE]
        print(f"Found {len(symbols)} structural targets. Downloading 1-year historical data...")
        return symbols

    except Exception as e:
        print(f"API screener failed: {e}")
        sys.exit(1)


# ==========================================
# STATE MANAGEMENT (DEDUPLICATION)
# ==========================================
def load_seen_symbols():
    if os.path.exists(SEEN_SYMBOLS_FILE):
        try:
            with open(SEEN_SYMBOLS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_seen_symbols(seen):
    try:
        with open(SEEN_SYMBOLS_FILE, "w") as f:
            json.dump(seen, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not save state to persistent volume: {e}")

def clean_old_seen_symbols(seen):
    """Prevents JSON file bloat by purging timestamps older than the dedup window."""
    cutoff = datetime.now() - timedelta(days=DEDUP_WINDOW_DAYS)
    cleaned_seen = {}
    for sym, date_str in seen.items():
        try:
            if datetime.fromisoformat(date_str) > cutoff:
                cleaned_seen[sym] = date_str
        except Exception:
            pass # Drop malformed dates
    return cleaned_seen

def recently_alerted(symbol, seen):
    if symbol not in seen:
        return False
    try:
        last_alert = datetime.fromisoformat(seen[symbol])
        return (datetime.now() - last_alert) < timedelta(days=DEDUP_WINDOW_DAYS)
    except Exception:
        return False


# ==========================================
# EARNINGS BLACKOUT CHECK
# ==========================================
def has_upcoming_earnings(symbol, blackout_days=EARNINGS_BLACKOUT_DAYS):
    try:
        cal = yf.Ticker(symbol).get_earnings_dates(limit=4)
        if cal is None or cal.empty:
            return False
        today = pd.Timestamp.now(tz=cal.index.tz)
        upcoming = cal.index[cal.index >= today]
        if len(upcoming) == 0:
            return False
        next_earnings = upcoming.min()
        days_out = (next_earnings - today).days
        return 0 <= days_out <= blackout_days
    except Exception:
        return False


# ==========================================
# CONFIDENCE SCORE 
# ==========================================
def calculate_confidence_score(reward_risk_ratio, dist_21, ext_50):
    score = CONFIDENCE_BASE
    score += min(CONFIDENCE_RR_CAP, round(reward_risk_ratio * 5))
    score += round((PULLBACK_HIGH - abs(dist_21)) * 5)  
    score -= round(max(0, ext_50 - CONFIDENCE_EXTENSION_FREE_PCT) * 2)
    return max(1, min(100, score))


# ==========================================
# MAIN ANALYSIS ENGINE
# ==========================================
def run_macro_analysis():
    symbols = get_swing_universe()
    if not symbols:
        return

    # Load and immediately purge old data to prevent file bloating
    raw_seen = load_seen_symbols()
    seen = clean_old_seen_symbols(raw_seen)

    print("Downloading institutional daily market data...")
    data = yf.download(symbols, period="1y", group_by='ticker', progress=False)

    triggered_symbols = []
    skipped = {}

    print("\n--- INITIATING ALGORITHMIC SWING FILTERS ---")
    for sym in symbols:
        try:
            # Handle potential yfinance multi-index dropping bugs
            try:
                df = data[sym].copy() if len(symbols) > 1 else data.copy()
            except KeyError:
                skipped[sym] = "ticker missing from yfinance batch download"
                continue
                
            df = df.dropna()

            if len(df) < 200:
                skipped[sym] = "insufficient history (<200 days)"
                continue

            # --- 1. INDICATORS ---
            df['21_EMA'] = df['Close'].ewm(span=21, adjust=False).mean()
            df['50_SMA'] = df['Close'].rolling(window=50).mean()
            df['200_SMA'] = df['Close'].rolling(window=200).mean()

            # True ATR(14) Calculation (Wilder's Exponential Smoothing)
            high_low = df['High'] - df['Low']
            high_close = (df['High'] - df['Close'].shift()).abs()
            low_close = (df['Low'] - df['Close'].shift()).abs()
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            df['ATR'] = true_range.ewm(alpha=1/ATR_PERIOD, adjust=False).mean()

            current_price = df['Close'].iloc[-1]
            ema_21 = df['21_EMA'].iloc[-1]
            sma_50 = df['50_SMA'].iloc[-1]
            sma_200 = df['200_SMA'].iloc[-1]
            atr = df['ATR'].iloc[-1]

            recent_20d_high = df['Close'].tail(20).max()
            upside_to_swing_high = ((recent_20d_high - current_price) / current_price) * 100

            # --- GATE 1: MACRO UPTREND ---
            if not (sma_50 > sma_200 and current_price > sma_200):
                skipped[sym] = "failed trend gate (50/200 SMA)"
                continue

            # --- GATE 2: GRAVITY FILTER ---
            ext_50 = ((current_price - sma_50) / sma_50) * 100
            if ext_50 > MAX_EXTENSION_PCT:
                skipped[sym] = f"too extended above 50-SMA ({ext_50:.1f}%)"
                continue

            # --- GATE 3: SUPPORT PROXIMITY ---
            dist_21 = ((current_price - ema_21) / ema_21) * 100
            if not (PULLBACK_LOW <= dist_21 <= PULLBACK_HIGH):
                skipped[sym] = f"not resting on 21-EMA ({dist_21:.1f}%)"
                continue

            # --- GATE 4: VOLUME PROFILE (Prevent Intraday Data Leakage) ---
            # Exclude today's forming candle if running intraday
            completed_volume = df['Volume'].iloc[:-1] if len(df) > 1 else df['Volume']
            vol_3d_avg = completed_volume.tail(3).mean()
            vol_20d_avg = completed_volume.tail(20).mean()
            is_low_vol_pullback = vol_3d_avg < vol_20d_avg

            if not is_low_vol_pullback:
                skipped[sym] = "failed low-volume pullback check"
                continue

            if upside_to_swing_high < MIN_UPSIDE_TO_HIGH_PCT:
                skipped[sym] = f"insufficient upside potential ({upside_to_swing_high:.1f}%)"
                continue

            # --- GATE 4b: OVERNIGHT GAP CHECK ---
            # Calculates true gap (yesterday close to today open) instead of daily range
            today_open = df['Open'].iloc[-1]
            prev_close = df['Close'].iloc[-2]
            
            gap_pct = abs((today_open - prev_close) / prev_close) * 100
            if gap_pct > GAP_THRESHOLD_PCT:
                skipped[sym] = f"gap contamination (overnight gap {gap_pct:.2f}%)"
                continue
                
            quote_pct_change_today = ((current_price - prev_close) / prev_close) * 100

            # --- GATE 5: ATR STOP & RISK/REWARD ---
            stop_loss = ema_21 - (ATR_STOP_MULTIPLIER * atr)
            risk_pct = ((current_price - stop_loss) / current_price) * 100

            if risk_pct <= 0:
                skipped[sym] = "invalid risk (stop above current price)"
                continue

            reward_risk_ratio = upside_to_swing_high / risk_pct
            if reward_risk_ratio < MIN_REWARD_RISK_RATIO:
                skipped[sym] = f"poor reward:risk ratio ({reward_risk_ratio:.2f})"
                continue

            # --- GATE 6: DE-DUPLICATION CHECK ---
            if recently_alerted(sym, seen):
                skipped[sym] = "already alerted within 21-day window"
                continue

            # --- GATE 7: EARNINGS BLACKOUT CHECK ---
            if has_upcoming_earnings(sym):
                skipped[sym] = "earnings report within 7 days"
                continue

            # --- CONFIDENCE SCORE ---
            confidence_score = calculate_confidence_score(reward_risk_ratio, dist_21, ext_50)

            print(f" -> [MATCH] {sym} | Price: ${current_price:.2f} | Dist 21-EMA: {dist_21:.2f}% | "
                  f"Ext 50-SMA: {ext_50:.2f}% | R:R Ratio: {reward_risk_ratio:.2f} | "
                  f"Today's Move: {quote_pct_change_today:.2f}% | Confidence: {confidence_score}")

            triggered_symbols.append({
                "symbol": sym,
                "action": "BUY",
                "confidence_score": confidence_score,
                "current_price": round(current_price, 2),
                "ema_21": round(ema_21, 2),
                "sma_50": round(sma_50, 2),
                "atr_14": round(atr, 2),
                "dist_to_21_ema_pct": round(dist_21, 2),
                "ext_from_50_sma_pct": round(ext_50, 2),
                "recent_swing_high": round(recent_20d_high, 2),
                "upside_to_swing_high_pct": round(upside_to_swing_high, 2),
                "risk_pct": round(risk_pct, 2),
                "reward_risk_ratio": round(reward_risk_ratio, 2),
                "low_volume_pullback_verified": is_low_vol_pullback,
                "quote_percent_change_today": round(quote_pct_change_today, 2),
                "recommended_stop_loss": round(stop_loss, 2)
            })

        except Exception as e:
            skipped[sym] = f"error processing: {e}"
            continue

    # --- TERMINAL SUMMARY LOGGING ---
    print(f"\n--- SCAN COMPLETE: {len(skipped)} skipped, {len(triggered_symbols)} passed all gates ---")

    # --- FIRE PAYLOAD TO N8N ---
    if triggered_symbols:
        triggered_symbols = sorted(triggered_symbols, key=lambda x: x['confidence_score'], reverse=True)
        top_5_setups = triggered_symbols[:5]

        uk_time = datetime.now(ZoneInfo("Europe/London"))
        timestamp_str = uk_time.strftime("%Y-%m-%d %H:%M:%S")

        print(f"\n>>> FIRING N8N SWING WEBHOOK! Found {len(top_5_setups)} pristine structural setups. <<<")
        payload = {
            "scan_type": "weekly_structural_swing",
            "timestamp": timestamp_str,
            "top_ranked_symbols": top_5_setups
        }

        try:
            response = requests.post(N8N_SWING_WEBHOOK_URL, json=payload)
            
            # RACE CONDITION FIX: Only save state to persistent volume if n8n receives the alert
            if response.status_code == 200:
                print("Webhook successful! Saving deduplication state to volume...")
                for setup in top_5_setups:
                    seen[setup["symbol"]] = datetime.now().isoformat()
                save_seen_symbols(seen)
            else:
                print(f"Warning: n8n webhook failed with status {response.status_code}. State NOT saved (symbols will re-trigger next run).")
        except Exception as e:
            print(f"Error firing webhook: {e}. State NOT saved.")

    else:
        print("\nNo setups met the strict institutional swing criteria today. Cash is a position.")

if __name__ == "__main__":
    run_macro_analysis()
