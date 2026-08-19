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

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
SEEN_SYMBOLS_FILE = os.path.join(DATA_DIR, "seen_symbols.json")

# --- SCREENER PARAMETERS ---
MIN_MARKET_CAP = 500_000_000        # $500M+ Market Cap (Mid-caps included)
MIN_AVG_VOLUME = 1_000_000          # 1M+ shares/day average volume
MIN_PRICE = 10.00                   
UNIVERSE_SIZE = 1000                # Top 1000 largest caps scanned

# --- STRATEGY 1: PULLBACK GATES ---
MAX_EXTENSION_PCT = 15.0            
PULLBACK_LOW, PULLBACK_HIGH = -2.5, 3.0   
MIN_UPSIDE_TO_HIGH_PCT = 5.0        
ATR_STOP_MULTIPLIER = 1.5           
MIN_REWARD_RISK_RATIO = 1.5         

# --- STRATEGY 2: MOMENTUM GATES ---
MIN_RVOL = 1.5                      # Must have 1.5x normal daily volume
MIN_ADX = 25.0                      # ADX > 25 signifies a strong trend
BREAKOUT_PROXIMITY_PCT = -2.0       # Must be within 2% of (or above) the 20-day high

# --- SHARED FILTERS & STATE ---
EARNINGS_BLACKOUT_DAYS = 7          
DEDUP_WINDOW_DAYS = 21              
GAP_THRESHOLD_PCT = 5.0             

# ==========================================
# UNIVERSE SELECTION
# ==========================================
def get_swing_universe():
    print("\n--- BOOTING MULTI-STRATEGY SCREENER ---")
    try:
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', MIN_MARKET_CAP]),
            EquityQuery('gte', ['avgdailyvol3m', MIN_AVG_VOLUME]),
            EquityQuery('gt',  ['intradayprice', MIN_PRICE])
        ])
        response = yf.screen(q, sortField='intradaymarketcap', sortAsc=False)
        quotes = response.get('quotes', [])
        symbols = [item.get("symbol") for item in quotes if item.get("symbol")][:UNIVERSE_SIZE]
        print(f"Found {len(symbols)} targets. Downloading 1-year historical data...")
        return symbols
    except Exception as e:
        print(f"API screener failed: {e}")
        sys.exit(1)

# ==========================================
# STATE MANAGEMENT
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
        pass

def clean_old_seen_symbols(seen):
    cutoff = datetime.now() - timedelta(days=DEDUP_WINDOW_DAYS)
    return {sym: d for sym, d in seen.items() if datetime.fromisoformat(d) > cutoff}

def recently_alerted(symbol, seen):
    if symbol not in seen:
        return False
    return (datetime.now() - datetime.fromisoformat(seen[symbol])) < timedelta(days=DEDUP_WINDOW_DAYS)

def has_upcoming_earnings(symbol, blackout_days=EARNINGS_BLACKOUT_DAYS):
    try:
        cal = yf.Ticker(symbol).get_earnings_dates(limit=4)
        if cal is None or cal.empty: return False
        today = pd.Timestamp.now(tz=cal.index.tz)
        upcoming = cal.index[cal.index >= today]
        if len(upcoming) == 0: return False
        return 0 <= (upcoming.min() - today).days <= blackout_days
    except Exception:
        return False

# ==========================================
# MAIN ANALYSIS ENGINE
# ==========================================
def run_macro_analysis():
    symbols = get_swing_universe()
    if not symbols: return

    seen = clean_old_seen_symbols(load_seen_symbols())
    data = yf.download(symbols, period="1y", group_by='ticker', progress=False)

    pullback_setups = []
    momentum_setups = []
    skipped = {}

    print("\n--- INITIATING DUAL-CORE ALGORITHMIC FILTERS ---")
    for sym in symbols:
        try:
            try:
                df = data[sym].copy() if len(symbols) > 1 else data.copy()
            except KeyError:
                continue
                
            df = df.dropna()
            if len(df) < 200: continue

            # --- 1. CORE INDICATORS ---
            df['8_EMA'] = df['Close'].ewm(span=8, adjust=False).mean()
            df['21_EMA'] = df['Close'].ewm(span=21, adjust=False).mean()
            df['50_SMA'] = df['Close'].rolling(window=50).mean()
            df['200_SMA'] = df['Close'].rolling(window=200).mean()

            # ATR(14)
            high_low = df['High'] - df['Low']
            high_close = (df['High'] - df['Close'].shift()).abs()
            low_close = (df['Low'] - df['Close'].shift()).abs()
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            df['ATR'] = true_range.ewm(alpha=1/14, adjust=False).mean()

            # ADX (14) Calculation - Pure Pandas for Trend Strength
            up = df['High'] - df['High'].shift(1)
            down = df['Low'].shift(1) - df['Low']
            df['+DM'] = np.where((up > down) & (up > 0), up, 0.0)
            df['-DM'] = np.where((down > up) & (down > 0), down, 0.0)
            tr_rma = true_range.ewm(alpha=1/14, adjust=False).mean()
            plus_di = 100 * (df['+DM'].ewm(alpha=1/14, adjust=False).mean() / tr_rma)
            minus_di = 100 * (df['-DM'].ewm(alpha=1/14, adjust=False).mean() / tr_rma)
            dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1))
            df['ADX'] = dx.ewm(alpha=1/14, adjust=False).mean()

            # Current Variables
            current_price = df['Close'].iloc[-1]
            today_open = df['Open'].iloc[-1]
            prev_close = df['Close'].iloc[-2]
            ema_8 = df['8_EMA'].iloc[-1]
            ema_21 = df['21_EMA'].iloc[-1]
            sma_50 = df['50_SMA'].iloc[-1]
            sma_200 = df['200_SMA'].iloc[-1]
            atr = df['ATR'].iloc[-1]
            adx_val = df['ADX'].iloc[-1]

            # Volume & Price Metrics
            completed_volume = df['Volume'].iloc[:-1] if len(df) > 1 else df['Volume']
            vol_20d_avg = completed_volume.tail(20).mean()
            today_vol = df['Volume'].iloc[-1]
            rvol = today_vol / vol_20d_avg if vol_20d_avg > 0 else 0
            
            recent_20d_high = df['Close'].tail(20).max()
            dist_to_high_pct = ((current_price - recent_20d_high) / recent_20d_high) * 100
            dist_21 = ((current_price - ema_21) / ema_21) * 100
            ext_50 = ((current_price - sma_50) / sma_50) * 100
            
            # Global Filters (Apply to both strategies)
            if not (sma_50 > sma_200 and current_price > sma_200): continue
            if abs((today_open - prev_close) / prev_close) * 100 > GAP_THRESHOLD_PCT: continue
            if recently_alerted(sym, seen): continue
            if has_upcoming_earnings(sym): continue

            # ==========================================
            # STRATEGY 1: PULLBACK LOGIC
            # ==========================================
            is_pullback = False
            if ext_50 <= MAX_EXTENSION_PCT:
                if PULLBACK_LOW <= dist_21 <= PULLBACK_HIGH:
                    if completed_volume.tail(3).mean() < vol_20d_avg: # Low Vol Pullback
                        upside_to_swing_high = abs(dist_to_high_pct) # Upside needed to reach high
                        if upside_to_swing_high >= MIN_UPSIDE_TO_HIGH_PCT:
                            stop_loss_pb = ema_21 - (ATR_STOP_MULTIPLIER * atr)
                            risk_pct_pb = ((current_price - stop_loss_pb) / current_price) * 100
                            if risk_pct_pb > 0:
                                rr_ratio_pb = upside_to_swing_high / risk_pct_pb
                                if rr_ratio_pb >= MIN_REWARD_RISK_RATIO:
                                    is_pullback = True
                                    pullback_setups.append({
                                        "symbol": sym, "strategy": "PULLBACK", "action": "BUY",
                                        "current_price": round(current_price, 2),
                                        "reward_risk_ratio": round(rr_ratio_pb, 2),
                                        "dist_to_21_ema_pct": round(dist_21, 2),
                                        "recommended_stop_loss": round(stop_loss_pb, 2)
                                    })

            # ==========================================
            # STRATEGY 2: MOMENTUM LOGIC
            # ==========================================
            is_momentum = False
            if dist_to_high_pct >= BREAKOUT_PROXIMITY_PCT: # Breaking out or near breakout
                if current_price > ema_8 and ema_8 > ema_21: # Fast trend alignment
                    if rvol >= MIN_RVOL: # Massive volume injection
                        if adx_val >= MIN_ADX: # Fierce ADX Trend
                            stop_loss_mom = ema_8 - (0.5 * atr) # Tighter stop for momentum
                            risk_pct_mom = ((current_price - stop_loss_mom) / current_price) * 100
                            if risk_pct_mom > 0:
                                is_momentum = True
                                momentum_setups.append({
                                    "symbol": sym, "strategy": "MOMENTUM", "action": "BUY",
                                    "current_price": round(current_price, 2),
                                    "relative_volume": round(rvol, 2),
                                    "adx_strength": round(adx_val, 2),
                                    "dist_to_20d_high_pct": round(dist_to_high_pct, 2),
                                    "recommended_stop_loss": round(stop_loss_mom, 2)
                                })

            if is_pullback:
                print(f" -> [PULLBACK MATCH] {sym} | Price: ${current_price:.2f} | R:R: {rr_ratio_pb:.2f}")
            if is_momentum:
                print(f" -> [MOMENTUM MATCH] {sym} | Price: ${current_price:.2f} | RVOL: {rvol:.1f}x | ADX: {adx_val:.1f}")

        except Exception as e:
            continue

    # --- FIRE PAYLOAD TO N8N ---
    total_setups = len(pullback_setups) + len(momentum_setups)
    
    if total_setups > 0:
        # Sort both lists by their strongest respective metrics
        top_pullbacks = sorted(pullback_setups, key=lambda x: x['reward_risk_ratio'], reverse=True)[:3]
        top_momentums = sorted(momentum_setups, key=lambda x: x['adx_strength'], reverse=True)[:3]

        uk_time = datetime.now(ZoneInfo("Europe/London"))
        
        payload = {
            "scan_type": "dual_strategy_swing",
            "timestamp": uk_time.strftime("%Y-%m-%d %H:%M:%S"),
            "pullback_setups": top_pullbacks,
            "momentum_setups": top_momentums
        }

        print(f"\n>>> FIRING N8N WEBHOOK! {len(top_pullbacks)} Pullbacks, {len(top_momentums)} Momentum breakouts. <<<")
        
        try:
            response = requests.post(N8N_SWING_WEBHOOK_URL, json=payload)
            if response.status_code == 200:
                print("Webhook successful! Saving deduplication state...")
                for setup in top_pullbacks + top_momentums:
                    seen[setup["symbol"]] = datetime.now().isoformat()
                save_seen_symbols(seen)
        except Exception as e:
            print(f"Error firing webhook: {e}")

    else:
        print("\nNo setups met the strict institutional criteria today. Cash is a position.")

if __name__ == "__main__":
    run_macro_analysis()
