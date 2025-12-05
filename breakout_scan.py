import yfinance as yf
import pandas as pd
import requests
import numpy as np
import concurrent.futures
from io import StringIO
import time

# ==============================================================================
# KONFIGURATION: Generalisiertes Quantitatives Breakout-Modell (GQBM)
# ==============================================================================
# Basierend auf - Gewichtung der Dimensionen
SCORE_THRESHOLD = 70      # Ab diesem Score landet die Aktie auf der Liste
MAX_WORKERS = 10          # Anzahl der parallelen Threads (nicht zu hoch setzen wegen API-Limit)

# Gewichtung (Max 100 Punkte)
W_TREND = 20    # Trend-Struktur (EMA Stacking)
W_VOLAT = 20    # Volatilität (Bollinger Squeeze)
W_MOMENT = 15   # Momentum (RSI Regime)
W_VOLUME = 25   # Volumen (RVol)
W_PATTERN = 20  # Muster (Nähe zum High / Konsolidierung)

# ==============================================================================
# 1. DATENQUELLEN (REGIORNEN: US, EU, ASIEN)
# ==============================================================================

def get_sp500_tickers():
    """Lade S&P 500 (USA)"""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        df = tables[0]
        tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
        return [(t, "US") for t in tickers]
    except:
        return []

def get_euro_tickers():
    """Lade wichtige Euro-Werte (Auswahl)"""
    # Manuelle Auswahl starker EU-Werte + Euro Stoxx 50 Vertreter
    tickers = [
        "ASML.AS", "MC.PA", "SAP.DE", "SIE.DE", "AIR.PA", "RMS.PA", "OR.PA",
        "ITX.MC", "TTE.PA", "IBE.MC", "ALV.DE", "DTE.DE", "SU.PA", "MBG.DE",
        "BMW.DE", "VOW3.DE", "BAS.DE", "ADYEN.AS", "PRX.AS", "ABI.BR",
        "EL.PA", "KER.PA", "BNP.PA", "SAN.MC", "BBVA.MC", "CS.PA", "RI.PA",
        "RACE.MI", "STLAM.MI", "ENEL.MI", "ISP.MI", "ZAL.DE", "HFG.DE"
    ]
    return [(t, "EU") for t in tickers]

def get_asia_tickers():
    """Lade Asien (Japan & Hong Kong Tech/Growth Auswahl)"""
    # Fokus auf Liquide Werte aus Nikkei 225 und Hang Seng
    tickers = [
        # Japan (.T)
        "7203.T", # Toyota
        "6758.T", # Sony
        "8035.T", # Tokyo Electron (Semi)
        "9984.T", # SoftBank Group
        "6861.T", # Keyence (Automation)
        "6920.T", # Lasertec (Semi)
        "7974.T", # Nintendo
        "6501.T", # Hitachi
        "4063.T", # Shin-Etsu Chemical
        "7741.T", # HOYA
        # Hong Kong (.HK)
        "0700.HK", # Tencent
        "9988.HK", # Alibaba
        "3690.HK", # Meituan
        "1810.HK", # Xiaomi
        "1211.HK", # BYD
        "0981.HK", # SMIC (Semi)
        "9618.HK", # JD.com
        "2015.HK", # Li Auto
    ]
    return [(t, "ASIA") for t in tickers]

# ==============================================================================
# 2. TECHNISCHE INDIKATOREN (HELFER)
# ==============================================================================

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_indicators(df):
    if len(df) < 200: return None
    
    # Gleitende Durchschnitte für EMA Stacking 
    df['EMA_10'] = df['Close'].ewm(span=10, adjust=False).mean()
    df['EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()
    df['SMA_50'] = df['Close'].rolling(window=50).mean()
    df['SMA_200'] = df['Close'].rolling(window=200).mean()
    
    # Bollinger Bänder für Squeeze 
    df['BB_Mid'] = df['Close'].rolling(window=20).mean()
    df['BB_Std'] = df['Close'].rolling(window=20).std()
    df['BB_Up'] = df['BB_Mid'] + (2 * df['BB_Std'])
    df['BB_Low'] = df['BB_Mid'] - (2 * df['BB_Std'])
    # Bandwidth: Wie eng ziehen sich die Bänder zusammen?
    df['BBW'] = (df['BB_Up'] - df['BB_Low']) / df['BB_Mid']
    
    # RSI 
    df['RSI'] = calculate_rsi(df['Close'])
    
    # Volumen Durchschnitt (50 Tage) für RVol 
    df['Vol_SMA50'] = df['Volume'].rolling(window=50).mean()
    
    return df

# ==============================================================================
# 3. DER ANALYST (GQBM LOGIK)
# ==============================================================================

def analyze_stock_gqbm(data_packet):
    symbol, region = data_packet
    
    try:
        # Lade Historie (1 Jahr) für technische Analyse
        stock = yf.Ticker(symbol)
        df = stock.history(period="1y")
        
        if df.empty or len(df) < 200:
            return None

        # Indikatoren berechnen
        df = calculate_indicators(df)
        if df is None: return None
        
        # Wir betrachten den aktuellsten Tag
        curr = df.iloc[-1]
        
        # --- SCORING SYSTEM (GQBM) ---
        score = 0
        reasons = []
        
        # 1. TREND STRUKTUR (Max 20 Pkt) 
        # Ideal: Preis > EMA10 > EMA20 > SMA50 > SMA200
        trend_perfect = (curr['Close'] > curr['EMA_10'] > curr['EMA_20'] > curr['SMA_50'] > curr['SMA_200'])
        trend_strong = (curr['Close'] > curr['SMA_50'] > curr['SMA_200'])
        
        if trend_perfect:
            score += 20
            reasons.append("Perfect Trend Order")
        elif trend_strong:
            score += 10
            
        # 2. VOLATILITÄT (Max 20 Pkt) 
        # BBW Squeeze: Ist die aktuelle Bandbreite sehr gering (Konsolidierung)?
        # Wir vergleichen BBW heute mit dem 6-Monats-Schnitt der BBW
        avg_bbw = df['BBW'].rolling(120).mean().iloc[-1]
        
        if curr['BBW'] < avg_bbw * 0.8: # Squeeze (20% enger als normal)
            score += 20
            reasons.append("Volatility Squeeze")
        elif curr['BBW'] < avg_bbw:
            score += 10

        # 3. MOMENTUM / RSI (Max 15 Pkt) 
        # Power Zone: RSI > 68 (kurz vor oder im Breakout)
        # Aber nicht "Toxic" (> 85 zu lange)
        if 65 <= curr['RSI'] <= 80:
            score += 15
            reasons.append(f"RSI Power Zone ({curr['RSI']:.1f})")
        elif 50 <= curr['RSI'] < 65:
            score += 5 # Neutral bis Bullisch
            
        # 4. VOLUMEN / RVOL (Max 25 Pkt) 
        # Volumen heute vs 50-Tage Schnitt
        if curr['Vol_SMA50'] > 0:
            rvol = curr['Volume'] / curr['Vol_SMA50']
        else:
            rvol = 0
            
        if rvol > 2.0: # Doppeltes Volumen
            score += 25
            reasons.append(f"Explosive Volume (x{rvol:.1f})")
        elif rvol > 1.5:
            score += 15
            reasons.append(f"High Volume (x{rvol:.1f})")
            
        # 5. MUSTER / PRICE GEOMETRY (Max 20 Pkt) 
        # Proxy: Preis nahe dem 52-Wochen-Hoch (innerhalb von 10-15%)
        # Das deutet auf "High Tight Flag" oder Konsolidierung am Top hin
        high_52 = df['Close'].max()
        dist_to_high = (high_52 - curr['Close']) / high_52
        
        if dist_to_high < 0.05: # Weniger als 5% vom Top entfernt
            score += 20
            reasons.append("Near All-Time-High")
        elif dist_to_high < 0.15:
            score += 10
            
        # --- FILTER ---
        if score >= SCORE_THRESHOLD:
            return {
                'Region': region,
                'Symbol': symbol,
                'Price': round(curr['Close'], 2),
                'Score': score,
                'RVol': round(rvol, 2),
                'RSI': round(curr['RSI'], 1),
                'Setup': ", ".join(reasons),
                'Link': f"https://finance.yahoo.com/quote/{symbol}"
            }
            
    except Exception as e:
        return None
    return None

# ==============================================================================
# 4. HAUPTPROGRAMM
# ==============================================================================

if __name__ == "__main__":
    start_time = time.time()
    
    # 1. Listen generieren
    print("Sammle Ticker aus US, EU und ASIEN...")
    us = get_sp500_tickers()
    eu = get_euro_tickers()
    asia = get_asia_tickers()
    
    all_jobs = us + eu + asia
    # Nur zum Testen begrenzen wir es (entferne [:50] für vollen Scan)
    # all_jobs = all_jobs[:50] 
    
    print(f"Starte GQBM-Scan für {len(all_jobs)} Aktien.")
    print(f"Filter: Score >= {SCORE_THRESHOLD} | Momentum & Breakout Fokus")
    print("="*80)
    print(f"{'Region':<6} | {'Symbol':<10} | {'Score':<5} | {'Price':<8} | {'RVol':<5} | {'RSI':<5} | Setup")
    print("-" * 80)
    
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {executor.submit(analyze_stock_gqbm, job): job for job in all_jobs}
        
        counter = 0
        total = len(all_jobs)
        
        for future in concurrent.futures.as_completed(future_to_stock):
            counter += 1
            if counter % 10 == 0:
                print(f"Progress: {counter}/{total}...", end="\r")
            
            res = future.result()
            if res:
                results.append(res)
                # Sofortige Ausgabe bei Treffer
                print(f"{res['Region']:<6} | {res['Symbol']:<10} | {res['Score']:<5} | {res['Price']:<8} | {res['RVol']:<5} | {res['RSI']:<5} | {res['Setup']}")

    print("\n" + "="*80)
    print(f"SCAN ABGESCHLOSSEN in {round(time.time() - start_time, 2)}s")
    print(f"{len(results)} Breakout-Kandidaten gefunden.")
    
    if results:
        df_res = pd.DataFrame(results)
        df_res = df_res.sort_values(by='Score', ascending=False)
        filename = "gqbm_breakout_list.csv"
        df_res.to_csv(filename, index=False)
        print(f"Ergebnisse gespeichert in '{filename}'")