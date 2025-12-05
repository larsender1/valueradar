import yfinance as yf
import pandas as pd
import requests
import concurrent.futures
from io import StringIO
import time

# ==============================================================================
# KONFIGURATION: Generalisiertes Quantitatives Breakout-Modell (GQBM)
# ==============================================================================
# Basierend auf [cite: 81, 82] - Gewichtung der Dimensionen
SCORE_THRESHOLD = 70      # Ab diesem Score landet die Aktie auf der Liste
MAX_WORKERS = 20          # Performance für Massen-Scan

# ==============================================================================
# 1. DATENQUELLEN (ROBUST & GEFIXT)
# ==============================================================================
# Browser-Header gegen Blockaden
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def get_sp500_tickers():
    """Lade S&P 500 (USA) dynamisch (funktioniert gut)"""
    print("Lade S&P 500 (USA)...")
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        r = requests.get(url, headers=HEADERS)
        df = pd.read_html(StringIO(r.text))[0]
        tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
        return [(t, "US") for t in tickers]
    except Exception as e:
        print(f"Fehler US-Liste: {e}")
        return []

def get_dax_tickers():
    """DAX 40 (Deutschland) - Hardcoded für 100% Stabilität"""
    print("Lade DAX 40 (Deutschland)...")
    # Liste aller DAX-40 Werte mit korrekter .DE Endung für Yahoo
    tickers = [
        "ADS.DE", "AIR.DE", "ALV.DE", "BAS.DE", "BAYN.DE", "BEI.DE", "BMW.DE", "BNR.DE",
        "CBK.DE", "CON.DE", "1COV.DE", "DTG.DE", "DBK.DE", "DB1.DE", "DHL.DE", "DTE.DE",
        "EOAN.DE", "FRE.DE", "HNR1.DE", "HEI.DE", "HEN3.DE", "IFX.DE", "MBG.DE", "MRK.DE",
        "MTX.DE", "MUV2.DE", "PUM.DE", "QIA.DE", "RWE.DE", "SAP.DE", "SRT3.DE", "SIE.DE",
        "ENR.DE", "SY1.DE", "VOW3.DE", "VNA.DE", "ZAL.DE", "SHL.DE", "HLAG.DE", "RHM.DE"
    ]
    return [(t, "DE") for t in tickers]

def get_asia_tickers():
    """Top Asien Auswahl (Japan .T & Hong Kong .HK)"""
    print("Lade Top Asien (Japan & China Tech)...")
    # Wir nehmen die liquidesten High-Beta Werte aus Asien (Tech, Auto, Semi)
    # Statt blind 225 langweilige Werte zu scannen, fokussieren wir auf Alpha-Kandidaten.
    tickers = [
        # --- JAPAN (Nikkei Leaders) ---
        "7203.T", # Toyota
        "6758.T", # Sony
        "8035.T", # Tokyo Electron (Chip)
        "9984.T", # SoftBank (Tech Invest)
        "6861.T", # Keyence (Automation)
        "6920.T", # Lasertec (Semi - High Volatility!)
        "7974.T", # Nintendo
        "6501.T", # Hitachi
        "4063.T", # Shin-Etsu
        "7741.T", # HOYA
        "6146.T", # Disco Corp
        "6954.T", # Fanuc
        "7011.T", # Mitsubishi Heavy
        "8058.T", # Mitsubishi Corp
        "8306.T", # MUFG Bank
        "9432.T", # NTT
        "4502.T", # Takeda Pharma
        "6367.T", # Daikin
        # --- HONG KONG / CHINA (Tech Giants) ---
        "0700.HK", # Tencent
        "9988.HK", # Alibaba
        "3690.HK", # Meituan
        "1810.HK", # Xiaomi
        "1211.HK", # BYD (EV Leader)
        "0981.HK", # SMIC (Chips)
        "9618.HK", # JD.com
        "2015.HK", # Li Auto
        "9868.HK", # Xpeng
        "1024.HK", # Kuaishou
        "0992.HK", # Lenovo
        "2269.HK", # WuXi Biologics
    ]
    return [(t, "ASIA") for t in tickers]

def get_euro_stoxx_tickers():
    """Erweiterte Euro-Liste (Frankreich, Niederlande, Spanien, Italien)"""
    print("Lade Euro Stoxx (EU)...")
    tickers = [
        "ASML.AS", "MC.PA", "OR.PA", "SAP.DE", "SIE.DE", "TTE.PA", "IDEX.PA",
        "SAN.MC", "AIR.PA", "IBE.MC", "RMS.PA", "SU.PA", "AI.PA", "DTE.DE", 
        "BNP.PA", "ABI.BR", "ITX.MC", "VOW3.DE", "BAYN.DE", "BMW.DE", "INGA.AS", 
        "BAS.DE", "MBG.DE", "KER.PA", "AD.AS", "CS.PA", "SAF.PA", "MUV2.DE", 
        "ENEL.MI", "ISP.MI", "ENI.MI", "STLAM.MI", "RACE.MI", "ORA.PA", "DG.PA", 
        "BN.PA", "CAP.PA", "NOKIA.HE", "AH.AS", "UNA.AS", "PHIA.AS", "BBVA.MC",
        "STM.PA", "LR.PA", "RI.PA"
    ]
    return [(t, "EU") for t in tickers]

# ==============================================================================
# 2. TECHNISCHE INDIKATOREN (HELFER)
# ==============================================================================

def calculate_indicators(df):
    if len(df) < 200: return None
    
    # EMA Stacking [cite: 82]
    df['EMA_10'] = df['Close'].ewm(span=10, adjust=False).mean()
    df['EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()
    df['SMA_50'] = df['Close'].rolling(window=50).mean()
    df['SMA_200'] = df['Close'].rolling(window=200).mean()
    
    # Bollinger Bands [cite: 82]
    df['BB_Mid'] = df['Close'].rolling(window=20).mean()
    df['BB_Std'] = df['Close'].rolling(window=20).std()
    df['BB_Up'] = df['BB_Mid'] + (2 * df['BB_Std'])
    df['BB_Low'] = df['BB_Mid'] - (2 * df['BB_Std'])
    df['BBW'] = (df['BB_Up'] - df['BB_Low']) / df['BB_Mid']
    
    # RSI [cite: 82]
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # Volumen Schnitt [cite: 82]
    df['Vol_SMA50'] = df['Volume'].rolling(window=50).mean()
    
    return df

# ==============================================================================
# 3. DER ANALYST (GQBM LOGIK)
# ==============================================================================

def analyze_stock_gqbm(data_packet):
    symbol, region = data_packet
    
    try:
        stock = yf.Ticker(symbol)
        df = stock.history(period="1y") # 1 Jahr History für 200 SMA
        
        if df.empty or len(df) < 200: return None

        df = calculate_indicators(df)
        if df is None: return None
        
        curr = df.iloc[-1]
        score = 0
        reasons = []
        
        # 1. TREND (20 Pkt) [cite: 82]
        if curr['Close'] > curr['EMA_10'] > curr['EMA_20'] > curr['SMA_50'] > curr['SMA_200']:
            score += 20
            reasons.append("Trend: Perfect Order")
        elif curr['Close'] > curr['SMA_50'] > curr['SMA_200']:
            score += 10
            
        # 2. VOLATILITÄT / SQUEEZE (20 Pkt) [cite: 82]
        # Vergleich aktuelle BBW mit 6-Monats-Schnitt
        avg_bbw = df['BBW'].rolling(120).mean().iloc[-1]
        if curr['BBW'] < avg_bbw * 0.8: 
            score += 20
            reasons.append("Vol: Squeeze")
        elif curr['BBW'] < avg_bbw:
            score += 10

        # 3. MOMENTUM / RSI (15 Pkt) [cite: 82]
        # Power Zone 55-70 vor Ausbruch, >70 am Ausbruch
        if 65 <= curr['RSI'] <= 80:
            score += 15
            reasons.append(f"Mom: Power Zone ({int(curr['RSI'])})")
        elif 50 <= curr['RSI'] < 65:
            score += 5
            
        # 4. VOLUMEN / RVOL (25 Pkt) [cite: 82]
        # Volumen > 150% (1.5x) des Schnitts
        rvol = curr['Volume'] / curr['Vol_SMA50'] if curr['Vol_SMA50'] > 0 else 0
        if rvol > 2.0:
            score += 25
            reasons.append(f"Vol: Explosive (x{round(rvol,1)})")
        elif rvol > 1.5:
            score += 15
            reasons.append(f"Vol: High (x{round(rvol,1)})")
            
        # 5. PATTERN / NEAR HIGH (20 Pkt) [cite: 82]
        # Ersatz für geometrische Muster: Nähe zum 52-Wochen-Hoch (<5%)
        high_52 = df['Close'].max()
        dist = (high_52 - curr['Close']) / high_52
        if dist < 0.05:
            score += 20
            reasons.append("Pat: Near ATH")
        elif dist < 0.15:
            score += 10
            
        if score >= SCORE_THRESHOLD:
            return {
                'Region': region, 'Symbol': symbol,
                'Price': round(curr['Close'], 2), 'Score': score,
                'RVol': round(rvol, 2), 'RSI': int(curr['RSI']),
                'Setup': ", ".join(reasons)
            }
            
    except:
        return None
    return None

# ==============================================================================
# 4. HAUPTPROGRAMM
# ==============================================================================

if __name__ == "__main__":
    start = time.time()
    
    # 1. Listen laden
    us = get_sp500_tickers()
    de = get_dax_tickers()
    asia = get_asia_tickers()
    eu = get_euro_stoxx_tickers()
    
    # Zusammenführen und Duplikate entfernen (z.B. SAP ist in DAX und EuroStoxx)
    seen = set()
    all_jobs = []
    for item in us + de + asia + eu:
        if item[0] not in seen:
            all_jobs.append(item)
            seen.add(item[0])

    print("\n" + "="*80)
    print(f"STARTE GLOBAL SCAN (FIXED): {len(all_jobs)} Aktien")
    print(f"Märkte: US S&P500 ({len(us)}), DAX 40 ({len(de)}), Asia Top 30 ({len(asia)}), Euro Stoxx ({len(eu)})")
    print(f"Strategie: GQBM Breakout (Score >= {SCORE_THRESHOLD})")
    print("="*80)
    print(f"{'Reg':<4} | {'Sym':<8} | {'Scr':<3} | {'Price':<8} | {'RVol':<4} | Setup")
    print("-" * 80)
    
    results = []
    
    # Scan starten
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {executor.submit(analyze_stock_gqbm, job): job for job in all_jobs}
        counter = 0
        
        for future in concurrent.futures.as_completed(future_to_stock):
            counter += 1
            if counter % 50 == 0:
                print(f"Progress: {counter}/{len(all_jobs)}...", end="\r")
            
            res = future.result()
            if res:
                results.append(res)
                print(f"{res['Region']:<4} | {res['Symbol']:<8} | {res['Score']:<3} | {res['Price']:<8} | {res['RVol']:<4} | {res['Setup']}")

    # Speichern
    if results:
        df = pd.DataFrame(results).sort_values(by='Score', ascending=False)
        filename = "global_breakout_scan_v2.csv"
        df.to_csv(filename, index=False)
        print("\n" + "="*80)
        print(f"FERTIG! {len(results)} Treffer gespeichert in '{filename}'")
    else:
        print("\nKeine Treffer gefunden.")
    
    print(f"Dauer: {round((time.time() - start)/60, 1)} Minuten")