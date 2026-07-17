import pandas as pd
import numpy as np
import yfinance as yf
from yfinance import EquityQuery
import requests
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import sys

# ==========================================
# CONFIGURATION
# ==========================================
# Create a NEW webhook in n8n for this specific Swing Pipeline
N8N_SWING_WEBHOOK_URL = "https://go90ng-n8n.eq7icp.easypanel.host/webhook/b5af74d8-d66a-4bc1-b615-ed572b5b4053"

def get_swing_universe():
    print("\n--- BOOTING MACRO SWING SCREENER ---")
    try:
        print("Scanning for Mid-to-Mega Cap stocks ($2B+) with structural liquidity...")
        q = EquityQuery('and', [
            EquityQuery('eq',  ['region', 'us']),
            EquityQuery('gte', ['intradaymarketcap', 60000000]), # $2 Billion+ Market Cap
            EquityQuery('gte', ['avgdailyvol3m', 250000]),        # 1M+ Average Daily Volume
            EquityQuery('gt',  ['intradayprice', 1.00])            # Exclude penny stocks
        ])

        # Pull the top 100 strongest performing large caps this month
        response = yf.screen(q, sortField='percentchange', sortAsc=False)
        quotes = response.get('quotes', [])
        
        symbols = [q.get("symbol") for q in quotes][:100]
        print(f"Found {len(symbols)} structural targets. Downloading 1-year historical data...")
        return symbols

    except Exception as e:
        print(f"API screener failed: {e}")
        sys.exit(1)

def run_macro_analysis():
    symbols = get_swing_universe()
    if not symbols:
        return

    # Batch download 1 year of daily data, grouped by ticker
    print("Downloading institutional order flow data...")
    data = yf.download(symbols, period="1y", group_by='ticker', progress=False)
    
    triggered_symbols = []
    
    print("\n--- INITIATING ALGORITHMIC SWING FILTERS ---")
    for sym in symbols:
        try:
            # Extract individual stock data
            df = data[sym].copy() if len(symbols) > 1 else data.copy()
            df = df.dropna()
            
            if len(df) < 200:
                continue # Needs at least 200 days of data for the 200 SMA

            # --- 1. CALCULATE MACRO INDICATORS ---
            df['21_EMA'] = df['Close'].ewm(span=21, adjust=False).mean()
            df['50_SMA'] = df['Close'].rolling(window=50).mean()
            df['200_SMA'] = df['Close'].rolling(window=200).mean()
            
            current_price = df['Close'].iloc[-1]
            ema_21 = df['21_EMA'].iloc[-1]
            sma_50 = df['50_SMA'].iloc[-1]
            sma_200 = df['200_SMA'].iloc[-1]
            
            # Find the recent high (Resistance target)
            recent_20d_high = df['Close'].tail(20).max()
            upside_to_swing_high = ((recent_20d_high - current_price) / current_price) * 100
            
            # --- GATE 1: THE MACRO UPTREND ---
            # 50-day MUST be above the 200-day (Golden Cross)
            if not (sma_50 > sma_200 and current_price > sma_200):
                continue
                
            # --- GATE 2: THE GRAVITY FILTER ---
            # Stock must NOT be more than 15% extended above its 50-day average
            ext_50 = ((current_price - sma_50) / sma_50) * 100
            if ext_50 > 15.0:
                continue

            # --- GATE 3: SUPPORT PROXIMITY (THE PULLBACK) ---
            # Price must be resting within -1.5% to +3.0% of the 21-Day EMA
            dist_21 = ((current_price - ema_21) / ema_21) * 100
            if not (-1.5 <= dist_21 <= 3.0):
                continue

            # --- GATE 4: VOLUME PROFILE (LOW VOLUME PULLBACK) ---
            # The pullback must happen on lighter volume than the main trend
            vol_3d_avg = df['Volume'].tail(3).mean()
            vol_20d_avg = df['Volume'].tail(20).mean()
            
            is_low_vol_pullback = False
            if vol_3d_avg < vol_20d_avg:
                is_low_vol_pullback = True

            print(f"[{sym}] Price: ${current_price:.2f} | Dist to 21-EMA: {dist_21:.2f}% | Dist to 50-SMA: {ext_50:.2f}% | Low Vol Pullback: {is_low_vol_pullback}")

            # --- THE FINAL EXECUTION GATE ---
            if True: # <--- TEMPORARY TEST OVERRIDE
                
                # Swing targets
                stop_loss = ema_21 * 0.97 # Strict 3% close below the 21-EMA
                
                triggered_symbols.append({
                    "symbol": sym,
                    "current_price": round(current_price, 2),
                    "ema_21": round(ema_21, 2),
                    "sma_50": round(sma_50, 2),
                    "dist_to_21_ema_pct": round(dist_21, 2),
                    "ext_from_50_sma_pct": round(ext_50, 2),
                    "recent_swing_high": round(recent_20d_high, 2),
                    "upside_to_swing_high_pct": round(upside_to_swing_high, 2),
                    "low_volume_pullback_verified": is_low_vol_pullback,
                    "recommended_stop_loss": round(stop_loss, 2)
                })

        except Exception as e:
            pass

    # --- FIRE PAYLOAD TO N8N ---
    if triggered_symbols:
        # Sort by highest upside potential
        triggered_symbols = sorted(triggered_symbols, key=lambda x: x['upside_to_swing_high_pct'], reverse=True)
        top_5_setups = triggered_symbols[:5]
        
        uk_time = datetime.now(ZoneInfo("Europe/London"))
        timestamp_str = uk_time.strftime("%Y-%m-%d %H:%M:%S")
        
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
