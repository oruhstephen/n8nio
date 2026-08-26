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
MIN_MARKET_CAP = 2_000_000_000        
MIN_AVG_VOLUME = 250_000          
MIN_PRICE = 10.00                   
UNIVERSE_SIZE = 1000                

# --- BENCHMARK ETFS (Indexes & Sectors) ---
INDEX_BENCHMARKS = ["SPY", "QQQ", "IWM", "XLK", "XLF"]

# --- STRATEGY 1: PULLBACK GATES ---
MAX_EXTENSION_PCT = 15.0            
PULLBACK_LOW, PULLBACK_HIGH = -2.5, 3.0   
MIN_UPSIDE_TO_HIGH_PCT = 5.0        
ATR_STOP_MULTIPLIER = 1.5           
MIN_REWARD_RISK_RATIO = 1.5         

# --- STRATEGY 2: CONSISTENT MOMENTUM GATES ---
MIN_RVOL_MOMENTUM = 1.0             
MIN_ADX = 20.0                      
BREAKOUT_PROXIMITY_PCT = -3.0       
MIN_GREEN_DAYS_5D = 2               
MAX_SINGLE_DAY_JUMP_PCT = 40.0      

# --- STRATEGY 3: OVERNIGHT GAP & GO GATES (NEW) ---
GAP_MIN_RVOL = 1.5                  # Needs heavy accumulation today
GAP_MIN_CLOSE_RANGE = 0.90          # Must close in the top 10% of its daily range
GAP_MIN_DAY_PCT = 3.0               # Stock must be up at least 3% on the day
GAP_ATR_STOP = 0.5                  # Tight stop for gap setups (0.5x ATR below close)

# --- SHARED FILTERS & STATE ---
EARNINGS_BLACKOUT_DAYS = 7          
DEDUP_WINDOW_DAYS = 21              
GAP_THRESHOLD_PCT = 5.0             

# ==========================================
# HELPER FUNCTIONS & STATE
# ==========================================
def get_mcap_tier(mcap):
    if not mcap or mcap <= 0: return "Unknown Cap"
    if mcap >= 200_000_000_000: return "Mega Cap (>$200B)"
    if mcap >= 10_000_000_000: return "Large Cap ($10B-$200B)"
    if mcap >= 2_000_000_000: return "Mid Cap ($2B-$10B)"
    if mcap >= 300_000_000: return "Small Cap ($300M-$2B)"
    return "Micro Cap (<$300M)"

def get_swing_universe():
    print("\n--- BOOTING TRI-STRATEGY SCREENER ---")
    try:
        print(f"Scanning for US stocks with Market Cap >= ${MIN_MARKET_CAP:,}, "
              f"Avg Vol >= {MIN_AVG_VOLUME:,}, Price >= ${MIN_PRICE}...")
        
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', MIN_MARKET_CAP]),
            EquityQuery('gte', ['avgdailyvol3m', MIN_AVG_VOLUME]),
            EquityQuery('gt',  ['intradayprice', MIN_PRICE])
        ])
        
        symbols = []
        symbol_map = {}
        
        for offset in [0, 250, 500, 750]:
            response = yf.screen(q, sortField='intradaymarketcap', sortAsc=False, size=250, offset=offset)
            quotes = response.get('quotes', [])
            
            if not quotes:
                break 
                
            for item in quotes:
                sym = item.get("symbol")
                if sym and sym not in symbols:
                    symbols.append(sym)
                    symbol_map[sym] = item.get("shortName", sym) 
                
        symbols = symbols[:UNIVERSE_SIZE]
        
        print(f"Fetching dynamic metadata for {len(INDEX_BENCHMARKS)} benchmark ETFs...")
        for etf in INDEX_BENCHMARKS:
            if etf not in symbols: 
                symbols.append(etf)
            try:
                etf_info = yf.Ticker(etf).info
                symbol_map[etf] = etf_info.get('shortName', etf)
            except Exception:
                symbol_map[etf] = etf
                
        print(f"Found {len(symbols)} targets (including {len(INDEX_BENCHMARKS)} ETFs). Downloading data...")
        return symbols, symbol_map
        
    except Exception as e:
        print(f"API screener failed: {e}")
        sys.exit(1)

def load_seen_symbols():
    if os.path.exists(SEEN_SYMBOLS_FILE):
        try:
            with open(SEEN_SYMBOLS_FILE, "r") as f: return json.load(f)
        except Exception: return {}
    return {}

def save_seen_symbols(seen):
    try:
        with open(SEEN_SYMBOLS_FILE, "w") as f: json.dump(seen, f, indent=2)
    except Exception: pass

def clean_old_seen_symbols(seen):
    cutoff = datetime.now() - timedelta(days=DEDUP_WINDOW_DAYS)
    return {sym: d for sym, d in seen.items() if datetime.fromisoformat(d) > cutoff}

def recently_alerted(symbol, strategy_prefix, seen):
    key = f"{strategy_prefix}_{symbol}"
    if key not in seen: return False
    return (datetime.now() - datetime.fromisoformat(seen[key])) < timedelta(days=DEDUP_WINDOW_DAYS)

def has_upcoming_earnings(symbol, blackout_days=EARNINGS_BLACKOUT_DAYS):
    if symbol in INDEX_BENCHMARKS: return False
    try:
        cal = yf.Ticker(symbol).get_earnings_dates(limit=4)
        if cal is None or cal.empty: return False
        today = pd.Timestamp.now(tz=cal.index.tz)
        upcoming = cal.index[cal.index >= today]
        if len(upcoming) == 0: return False
        return 0 <= (upcoming.min() - today).days <= blackout_days
    except Exception: return False

# ==========================================
# MAIN ANALYSIS ENGINE
# ==========================================
def run_macro_analysis():
    symbols, symbol_map = get_swing_universe()
    if not symbols: return

    seen = clean_old_seen_symbols(load_seen_symbols())
    data = yf.download(symbols, period="1y", group_by='ticker', progress=False)

    pullback_setups = []
    momentum_setups = []
    gap_setups = []
    index_metrics = {}
    
    dynamic_benchmarks = {
        "pullback": {"symbol": "N/A", "5d_return": -999, "metrics": {}},
        "momentum": {"symbol": "N/A", "5d_return": -999, "metrics": {}}
    }
    
    uk_time = datetime.now(ZoneInfo("Europe/London"))
    scan_timestamp = uk_time.strftime("%Y-%m-%d %H:%M:%S BST")

    print("\n--- INITIATING ALGORITHMIC FILTERS & BENCHMARKING @ {scan_timestamp} ---")
    for sym in symbols:
        try:
            try:
                df = data[sym].copy() if len(symbols) > 1 else data.copy()
            except KeyError: continue
                
            df = df.dropna()
            if len(df) < 200: continue
            
            company_name = symbol_map.get(sym, sym)

            # --- CORE INDICATORS ---
            df['8_EMA'] = df['Close'].ewm(span=8, adjust=False).mean()
            df['21_EMA'] = df['Close'].ewm(span=21, adjust=False).mean()
            df['50_SMA'] = df['Close'].rolling(window=50).mean()
            df['200_SMA'] = df['Close'].rolling(window=200).mean()

            high_low = df['High'] - df['Low']
            high_close = (df['High'] - df['Close'].shift()).abs()
            low_close = (df['Low'] - df['Close'].shift()).abs()
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            df['ATR'] = true_range.ewm(alpha=1/14, adjust=False).mean()

            up = df['High'] - df['High'].shift(1)
            down = df['Low'].shift(1) - df['Low']
            df['+DM'] = np.where((up > down) & (up > 0), up, 0.0)
            df['-DM'] = np.where((down > up) & (down > 0), down, 0.0)
            tr_rma = true_range.ewm(alpha=1/14, adjust=False).mean()
            plus_di = 100 * (df['+DM'].ewm(alpha=1/14, adjust=False).mean() / tr_rma)
            minus_di = 100 * (df['-DM'].ewm(alpha=1/14, adjust=False).mean() / tr_rma)
            dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1))
            df['ADX'] = dx.ewm(alpha=1/14, adjust=False).mean()

            # Context Variables
            current_price = df['Close'].iloc[-1]
            prev_close = df['Close'].iloc[-2]
            today_change_pct = ((current_price - prev_close) / prev_close) * 100
            today_open = df['Open'].iloc[-1]
            
            ema_8 = df['8_EMA'].iloc[-1]
            ema_21 = df['21_EMA'].iloc[-1]
            sma_50 = df['50_SMA'].iloc[-1]
            sma_200 = df['200_SMA'].iloc[-1]
            atr = df['ATR'].iloc[-1]
            adx_val = df['ADX'].iloc[-1]

            if ema_8 > ema_21 and ema_21 > sma_50 and sma_50 > sma_200:
                trend_alignment = "Bullish"
            elif ema_8 < ema_21 and ema_21 < sma_50 and sma_50 < sma_200:
                trend_alignment = "Bearish"
            else:
                trend_alignment = "Mixed"
            
            daily_returns = df['Close'].pct_change() * 100
            last_5_returns = daily_returns.tail(5)
            green_days_5d = int((last_5_returns > 0).sum())
            max_daily_jump_5d = float(last_5_returns.max())
            ret_5d = ((current_price - df['Close'].iloc[-6]) / df['Close'].iloc[-6]) * 100
            
            completed_volume = df['Volume'].iloc[:-1] if len(df) > 1 else df['Volume']
            vol_20d_avg = completed_volume.tail(20).mean()
            today_vol = df['Volume'].iloc[-1]
            rvol = today_vol / vol_20d_avg if vol_20d_avg > 0 else 0
            
            recent_20d_high = df['Close'].tail(20).max()
            dist_to_high_pct = ((current_price - recent_20d_high) / recent_20d_high) * 100
            dist_21 = ((current_price - ema_21) / ema_21) * 100
            ext_50 = ((current_price - sma_50) / sma_50) * 100
            
            # --- GAP METRICS (NEW) ---
            high_today = df['High'].iloc[-1]
            low_today = df['Low'].iloc[-1]
            intraday_range = high_today - low_today
            close_range_position = (current_price - low_today) / intraday_range if intraday_range > 0 else 0
            prev_20d_high_excl_today = df['High'].iloc[-21:-1].max()
            is_fresh_breakout = current_price > prev_20d_high_excl_today

            # --- STATIC ETF BASELINE LOGIC ---
            if sym in INDEX_BENCHMARKS:
                index_metrics[sym] = {
                    "symbol": sym,
                    "company_name": company_name,
                    "5d_return_pct": round(ret_5d, 2),
                    "adx_strength": round(adx_val, 2),
                    "green_days_last_5": green_days_5d,
                    "dist_to_21_ema_pct": round(dist_21, 2)
                }
                continue 

            # --- DYNAMIC BENCHMARKING ---
            if ret_5d > dynamic_benchmarks["momentum"]["5d_return"] and current_price > sma_200:
                if max_daily_jump_5d <= MAX_SINGLE_DAY_JUMP_PCT: 
                    dynamic_benchmarks["momentum"] = {
                        "symbol": sym, "5d_return": ret_5d,
                        "metrics": {
                            "symbol": sym, 
                            "company_name": company_name,
                            "5d_return_pct": round(ret_5d, 2),
                            "green_days_last_5": green_days_5d,
                            "adx_strength": round(adx_val, 2)
                        }
                    }
                
            price_5d_ago = df['Close'].iloc[-6]
            ema21_5d_ago = df['21_EMA'].iloc[-6]
            dist_21_5d_ago = ((price_5d_ago - ema21_5d_ago) / ema21_5d_ago) * 100
            
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

            # Pre-evaluate constraints
            is_recent_alert = recently_alerted(sym, seen)
            pending_earnings = has_upcoming_earnings(sym)
            recent_gap = abs((today_open - prev_close) / prev_close) * 100 > GAP_THRESHOLD_PCT
            is_above_200 = (sma_50 > sma_200 and current_price > sma_200)

            is_pullback = False
            is_momentum = False
            is_gap_go = False
            
            try: mcap = yf.Ticker(sym).fast_info.market_cap
            except: mcap = 0
            
            # --- STRATEGY 1 & 2 GATES (SWING TRADING) ---
            # Swing trades require no recent gaps, no pending earnings, and trend alignment
            if is_above_200 and not recent_gap and not pending_earnings and not is_recent_alert:
                
                # STRATEGY 1: PULLBACK LOGIC
                if ext_50 <= MAX_EXTENSION_PCT and PULLBACK_LOW <= dist_21 <= PULLBACK_HIGH:
                    if completed_volume.tail(3).mean() < vol_20d_avg:
                        upside_to_swing_high_pct = abs(dist_to_high_pct)
                        if upside_to_swing_high_pct >= MIN_UPSIDE_TO_HIGH_PCT:
                            stop_loss_pb = ema_21 - (ATR_STOP_MULTIPLIER * atr)
                            risk_pct_pb = ((current_price - stop_loss_pb) / current_price) * 100
                            if risk_pct_pb > 0 and (upside_to_swing_high_pct / risk_pct_pb) >= MIN_REWARD_RISK_RATIO:
                                is_pullback = True
                                pullback_setups.append({
                                    "symbol": sym,
                                    "company_name": company_name,
                                    "market_cap_tier": get_mcap_tier(mcap),
                                    "trend_alignment": trend_alignment,
                                    "signal_timestamp": scan_timestamp,
                                    "action": "BUY_PULLBACK",
                                    "current_price": round(current_price, 2),
                                    "prev_close": round(prev_close, 2),
                                    "today_change_pct": round(today_change_pct, 2),
                                    "target_price": round(recent_20d_high, 2),
                                    "estimated_gain_pct": round(upside_to_swing_high_pct, 2),
                                    "recommended_stop_loss": round(stop_loss_pb, 2),
                                    "reward_risk_ratio": round((upside_to_swing_high_pct / risk_pct_pb), 2),
                                    "ema_21": round(ema_21, 2),
                                    "sma_50": round(sma_50, 2)
                                })

                # STRATEGY 2: CONSISTENT MOMENTUM LOGIC
                if dist_to_high_pct >= BREAKOUT_PROXIMITY_PCT and current_price > ema_8 > ema_21:
                    if green_days_5d >= MIN_GREEN_DAYS_5D and max_daily_jump_5d <= MAX_SINGLE_DAY_JUMP_PCT:
                        if rvol >= MIN_RVOL_MOMENTUM and adx_val >= MIN_ADX:
                            stop_loss_mom = ema_8 - (0.5 * atr)
                            risk_pct_mom = ((current_price - stop_loss_mom) / current_price) * 100
                            if risk_pct_mom > 0 and not is_pullback:
                                risk_amount = current_price - stop_loss_mom
                                momentum_target_price = current_price + (risk_amount * 2.0)
                                is_momentum = True
                                momentum_setups.append({
                                    "symbol": sym,
                                    "company_name": company_name,
                                    "market_cap_tier": get_mcap_tier(mcap),
                                    "trend_alignment": trend_alignment,
                                    "signal_timestamp": scan_timestamp,
                                    "action": "BUY_MOMENTUM",
                                    "current_price": round(current_price, 2),
                                    "prev_close": round(prev_close, 2),
                                    "today_change_pct": round(today_change_pct, 2),
                                    "target_price": round(momentum_target_price, 2),
                                    "estimated_gain_pct": round(momentum_estimated_gain_pct, 2),
                                    "recommended_stop_loss": round(stop_loss_mom, 2),
                                    "green_days_last_5": green_days_5d,
                                    "adx_strength": round(adx_val, 2),
                                    "ema_8": round(ema_8, 2),
                                    "ema_21": round(ema_21, 2),
                                    "sma_50": round(sma_50, 2)
                                })

            # --- STRATEGY 3: OVERNIGHT GAP & GO (DAY TRADE SETUP) ---
            # Gap setups ignore the 200_SMA constraint, but still avoid binary earnings reports
            if not pending_earnings and not is_recent_alert:
                if today_change_pct >= GAP_MIN_DAY_PCT:
                    if rvol >= GAP_MIN_RVOL and close_range_position >= GAP_MIN_CLOSE_RANGE:
                        if is_fresh_breakout:
                            is_gap_go = True
                            stop_loss_gap = current_price - (GAP_ATR_STOP * atr)
                            gap_setups.append({
                                "symbol": sym,
                                "company_name": company_name,
                                "market_cap_tier": get_mcap_tier(mcap),
                                "trend_alignment": trend_alignment,
                                "signal_timestamp": scan_timestamp,
                                "action": "BUY_GAP_ANTICIPATION",
                                "current_price": round(current_price, 2),
                                "prev_close": round(prev_close, 2),
                                "today_change_pct": round(today_change_pct, 2),
                                "rvol": round(rvol, 2),
                                "close_range_position_pct": round(close_range_position * 100, 1),
                                "recommended_stop_loss": round(stop_loss_gap, 2),
                                "ema_8": round(ema_8, 2),
                                "ema_21": round(ema_21, 2),
                                "sma_50": round(sma_50, 2)
                            })

            if is_pullback: print(f" -> [PULLBACK] {sym} | R:R: {upside_to_swing_high_pct / risk_pct_pb:.2f}")
            if is_momentum: print(f" -> [MOMENTUM] {sym} | ADX: {adx_val:.1f}")
            if is_gap_go:   print(f" -> [GAP & GO] {sym} | RVOL: {rvol:.1f}x | Close Pos: {close_range_position*100:.0f}%")

        except Exception as e: continue

    # --- FIRE PAYLOAD TO N8N ---
    total_setups = len(pullback_setups) + len(momentum_setups) + len(gap_setups)
    
    if total_setups > 0:
        top_pullbacks = sorted(pullback_setups, key=lambda x: x['reward_risk_ratio'], reverse=True)[:3]
        top_momentums = sorted(momentum_setups, key=lambda x: x['adx_strength'], reverse=True)[:3]
        # Sort Gap setups by how close they are to the absolute high of day, then by volume
        top_gaps = sorted(gap_setups, key=lambda x: (x['close_range_position_pct'], x['rvol']), reverse=True)[:3]
        
        payload = {
            "scan_type": "adaptive_multi_strategy_swing",
            "global_timestamp": scan_timestamp,
            "pullback_setups": top_pullbacks,
            "momentum_setups": top_momentums,
            "gap_setups": top_gaps,
            "benchmarks": {
                "market_indices": index_metrics,
                "dynamic_leaders_last_5_days": {
                    "pullback_leader": dynamic_benchmarks["pullback"]["metrics"],
                    "consistent_momentum_leader": dynamic_benchmarks["momentum"]["metrics"]
                }
            }
        }

        print(f"\n>>> FIRING N8N WEBHOOK! Found {len(top_pullbacks)} Pullbacks, {len(top_momentums)} Grinders, {len(top_gaps)} Gap Setups.")
        
        try:
            response = requests.post(N8N_SWING_WEBHOOK_URL, json=payload)
            if response.status_code == 200:
                print("Webhook successful! Saving deduplication state to volume...")
                for setup in top_pullbacks + top_momentums + top_gaps:
                    seen[setup["symbol"]] = datetime.now().isoformat()
                save_seen_symbols(seen)
            else:
                print(f"Warning: Webhook failed (HTTP {response.status_code}). State NOT saved.")
        except Exception as e:
            print(f"Error firing webhook: {e}")
    else:
        print("\nNo setups met the strict institutional criteria today. Cash is a position.")

if __name__ == "__main__":
    run_macro_analysis()
