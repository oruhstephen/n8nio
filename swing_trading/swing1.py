import pandas as pd
import numpy as np
import yfinance as yf
from yfinance import EquityQuery
import requests
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys

# ==========================================
# CONFIGURATION
# ==========================================
N8N_SWING_WEBHOOK_URL = "https://go90ng-n8n.eq7icp.easypanel.host/webhook/b5af74d8-d66a-4bc1-b615-ed572b5b4053"

MIN_MARKET_CAP = 2_000_000_000      # $2B+
MIN_AVG_VOLUME = 1_000_000          # 1M+ shares/day
MIN_PRICE = 10.00                   # matches your write-up (was $1 in original code)
UNIVERSE_SIZE = 150                 # how many top-cap names to pull each week

MAX_EXTENSION_PCT = 15.0            # gate 2: don't chase >15% above 50-SMA
PULLBACK_LOW, PULLBACK_HIGH = -1.5, 3.0   # gate 3: distance band around 21-EMA
MIN_UPSIDE_TO_HIGH_PCT = 5.0        # minimum reward to bother with the trade

ATR_PERIOD = 14
ATR_STOP_MULTIPLIER = 1.5           # stop = 21-EMA minus 1.5x ATR
MIN_REWARD_RISK_RATIO = 1.5         # reward must be at least 1.5x the risk

EARNINGS_BLACKOUT_DAYS = 7          # skip names reporting earnings within N days

SEEN_SYMBOLS_FILE = "seen_symbols.json"
DEDUP_WINDOW_DAYS = 21              # don't re-alert the same symbol within N days


# ==========================================
# UNIVERSE SELECTION
# ==========================================
def get_swing_universe():
    """
    Pull the broadest reasonable set of large, liquid stocks — sorted by
    market cap, NOT by today's percent change. Sorting by percentchange
    (the original version) biases the universe toward stocks already
    moving hard *today*, which is the opposite of what a quiet pullback
    setup looks like — those names get excluded before the gates even run.
    """
    print("\n--- BOOTING MACRO SWING SCREENER ---")
    try:
        print(f"Scanning for stocks with mkt cap >= ${MIN_MARKET_CAP:,}, "
              f"avg vol >= {MIN_AVG_VOLUME:,}, price >= ${MIN_PRICE}...")
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', MIN_MARKET_CAP]),
            EquityQuery('gte', ['avgdailyvol3m', MIN_AVG_VOLUME]),
            EquityQuery('gt',  ['intradayprice', MIN_PRICE])
        ])

        # Sort by market cap (stable, structural) instead of percentchange (noisy, momentum-biased)
        response = yf.screen(q, sortField='intradaymarketcap', sortAsc=False)
        quotes = response.get('quotes', [])

        symbols = [item.get("symbol") for item in quotes][:UNIVERSE_SIZE]
        print(f"Found {len(symbols)} structural targets. Downloading 1-year historical data...")
        return symbols

    except Exception as e:
        print(f"API screener failed: {e}")
        sys.exit(1)


# ==========================================
# DE-DUP STATE (avoid re-firing the same symbol every week)
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
    with open(SEEN_SYMBOLS_FILE, "w") as f:
        json.dump(seen, f, indent=2)

def recently_alerted(symbol, seen):
    if symbol not in seen:
        return False
    last_alert = datetime.fromisoformat(seen[symbol])
    return (datetime.now() - last_alert) < timedelta(days=DEDUP_WINDOW_DAYS)


# ==========================================
# EARNINGS BLACKOUT CHECK
# ==========================================
def has_upcoming_earnings(symbol, blackout_days=EARNINGS_BLACKOUT_DAYS):
    """
    Returns True if the symbol has an earnings date within the blackout
    window, so we can skip it (avoid a stop-defeating earnings gap).
    Fails "open" (returns False) if the earnings calendar isn't available,
    since we'd rather occasionally miss a blackout than kill the whole run.
    """
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
# MAIN ANALYSIS
# ==========================================
def run_macro_analysis():
    symbols = get_swing_universe()
    if not symbols:
        return

    seen = load_seen_symbols()

    print("Downloading institutional order flow data...")
    data = yf.download(symbols, period="1y", group_by='ticker', progress=False)

    triggered_symbols = []
    skipped = {}  # symbol -> reason, for end-of-run visibility instead of silent `pass`

    print("\n--- INITIATING ALGORITHMIC SWING FILTERS ---")
    for sym in symbols:
        try:
            df = data[sym].copy() if len(symbols) > 1 else data.copy()
            df = df.dropna()

            if len(df) < 200:
                skipped[sym] = "insufficient history (<200 days)"
                continue

            # --- INDICATORS ---
            df['21_EMA'] = df['Close'].ewm(span=21, adjust=False).mean()
            df['50_SMA'] = df['Close'].rolling(window=50).mean()
            df['200_SMA'] = df['Close'].rolling(window=200).mean()

            # ATR(14) for a volatility-aware stop instead of a flat 3%
            high_low = df['High'] - df['Low']
            high_close = (df['High'] - df['Close'].shift()).abs()
            low_close = (df['Low'] - df['Close'].shift()).abs()
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            df['ATR'] = true_range.rolling(window=ATR_PERIOD).mean()

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

            # --- GATE 4: VOLUME PROFILE ---
            vol_3d_avg = df['Volume'].tail(3).mean()
            vol_20d_avg = df['Volume'].tail(20).mean()
            is_low_vol_pullback = vol_3d_avg < vol_20d_avg

            print(f"[{sym}] Price: ${current_price:.2f} | Dist to 21-EMA: {dist_21:.2f}% | "
                  f"Dist to 50-SMA: {ext_50:.2f}% | Low Vol Pullback: {is_low_vol_pullback}")

            if not (is_low_vol_pullback and upside_to_swing_high > MIN_UPSIDE_TO_HIGH_PCT):
                skipped[sym] = "failed volume/upside gate"
                continue

            # --- ATR-BASED STOP (replaces flat 3% below 21-EMA) ---
            stop_loss = ema_21 - (ATR_STOP_MULTIPLIER * atr)
            risk_pct = ((current_price - stop_loss) / current_price) * 100

            # --- RISK/REWARD FILTER ---
            if risk_pct <= 0:
                skipped[sym] = "invalid risk (stop above current price)"
                continue
            reward_risk_ratio = upside_to_swing_high / risk_pct
            if reward_risk_ratio < MIN_REWARD_RISK_RATIO:
                skipped[sym] = f"poor reward:risk ({reward_risk_ratio:.2f})"
                continue

            # --- DE-DUP CHECK ---
            if recently_alerted(sym, seen):
                skipped[sym] = "already alerted within dedup window"
                continue

            # --- EARNINGS BLACKOUT ---
            if has_upcoming_earnings(sym):
                skipped[sym] = "earnings within blackout window"
                continue

            triggered_symbols.append({
                "symbol": sym,
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
                "recommended_stop_loss": round(stop_loss, 2)
            })

        except Exception as e:
            skipped[sym] = f"error: {e}"
            continue

    # --- SUMMARY OF SKIPPED SYMBOLS (visibility instead of silent `pass`) ---
    print(f"\n--- {len(skipped)} symbols skipped, {len(triggered_symbols)} passed all gates ---")

    # --- FIRE PAYLOAD TO N8N ---
    if triggered_symbols:
        triggered_symbols = sorted(triggered_symbols, key=lambda x: x['reward_risk_ratio'], reverse=True)
        top_5_setups = triggered_symbols[:5]

        uk_time = datetime.now(ZoneInfo("Europe/London"))
        timestamp_str = uk_time.strftime("%Y-%m-%d %H:%M:%S")

        # Update de-dup state so these don't refire next week
        for setup in top_5_setups:
            seen[setup["symbol"]] = datetime.now().isoformat()
        save_seen_symbols(seen)

        print(f"\n>>> FIRING N8N SWING WEBHOOK! Found {len(top_5_setups)} pristine structural setups. <<<")
        payload = {
            "scan_type": "weekly_structural_swing",
            "timestamp": timestamp_str,
            "top_ranked_symbols": top_5_setups
        }

        requests.post(N8N_SWING_WEBHOOK_URL, json=payload)
    else:
        print("\nNo setups met the strict institutional swing criteria today. Cash is a position.")


if __name__ == "__main__":
    run_macro_analysis()
