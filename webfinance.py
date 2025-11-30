import yfinance as yf
import pandas as pd
import requests
from io import StringIO
import concurrent.futures

# ==========================================
# KONFIGURATION (DEINE STRATEGIE)
# ==========================================
# 1. Wachstums-Check:
#    Wir erlauben minimales negatives Wachstum (-5%), da Europa gerade schwächelt.
#    Wenn du streng sein willst, setze es auf 0.01 (1%).
MIN_REVENUE_GROWTH = -0.05 

# 2. Bewertung:
#    PEG < 1.5 ist super.
#    Falls PEG fehlt, nehmen wir KGV < 16 (Graham-Regel).
MAX_PEG_RATIO = 1.5
MAX_PE_RATIO = 16

# 3. Sicherheit:
#    Schulden dürfen maximal 2x so hoch wie Eigenkapital sein.
MAX_DEBT_EQUITY = 200 
MIN_EPS = 0          # Muss profitabel sein
CHECK_FCF = True     # Muss Free Cash Flow haben

# 4. Trend-Filter (WICHTIG!):
#    Verhindert den Kauf von "fallenden Messern" (wie PayPal aktuell).
#    Aktie muss über dem 200-Tage-Schnitt liegen.
CHECK_TREND = True

# 5. Geschwindigkeit:
MAX_WORKERS = 10 

# ==========================================
# 1. DATENQUELLEN (REGIORNEN)
# ==========================================

def get_sp500_tickers():
    print("Lade S&P 500 (USA)...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    try:
        response = requests.get(url, headers=headers)
        tables = pd.read_html(StringIO(response.text))
        
        # Robuste Suche nach der richtigen Tabelle
        df = None
        for t in tables:
            if 'Symbol' in t.columns and 'Security' in t.columns:
                df = t
                break
        
        if df is None: return []

        # Yahoo braucht Bindestriche statt Punkte (BRK-B statt BRK.B)
        tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
        print(f"-> {len(tickers)} US-Aktien bereit.")
        return tickers
    except Exception as e:
        print(f"Fehler US-Liste: {e}")
        return []

def get_dax_tickers():
    print("Lade DAX 40 (Deutschland)...")
    # Hardcoded Liste ist sicherer wegen .DE Endungen
    dax = [
        "ADS.DE", "AIR.DE", "ALV.DE", "BAS.DE", "BAYN.DE", "BEI.DE", "BMW.DE", "BNR.DE",
        "CBK.DE", "CON.DE", "1COV.DE", "DTG.DE", "DBK.DE", "DB1.DE", "DHL.DE", "DTE.DE",
        "EOAN.DE", "FRE.DE", "HNR1.DE", "HEI.DE", "HEN3.DE", "IFX.DE", "MBG.DE", "MRK.DE",
        "MTX.DE", "MUV2.DE", "PUM.DE", "QIA.DE", "RWE.DE", "SAP.DE", "SRT3.DE", "SIE.DE",
        "ENR.DE", "SY1.DE", "VOW3.DE", "VNA.DE", "ZAL.DE", "SHL.DE", "HLAG.DE", "RHM.DE"
    ]
    print(f"-> {len(dax)} DE-Aktien bereit.")
    return dax

def get_euro_tickers():
    print("Lade EURO STOXX 50 (Europa)...")
    # Enthält ASML, LVMH, Ferrari etc. mit korrekten Länder-Endungen
    euro_stoxx = [
        "ASML.AS", "MC.PA", "SAP.DE", "PRX.AS", "SIE.DE", "TTE.PA", "SAN.MC", "OR.PA",
        "ALV.DE", "AIR.PA", "IBE.MC", "RMS.PA", "SU.PA", "AI.PA", "DTE.DE", "BNP.PA",
        "ABI.BR", "ITX.MC", "VOW3.DE", "BAYN.DE", "BMW.DE", "INGA.AS", "BAS.DE", "MBG.DE",
        "KER.PA", "AD.AS", "CS.PA", "SAF.PA", "MUV2.DE", "ENEL.MI", "ISP.MI", "ENI.MI",
        "STLAM.MI", "RACE.MI", "ORA.PA", "DG.PA", "BN.PA", "CAP.PA", "NOKIA.HE", "AH.AS",
        "UNA.AS", "PHIA.AS", "HEIA.AS", "KNEBV.HE", "BBVA.MC", "CRH.L"
    ]
    print(f"-> {len(euro_stoxx)} EU-Aktien bereit.")
    return euro_stoxx

# ==========================================
# 2. DER ANALYST (LOGIK)
# ==========================================

def analyze_stock(data_packet):
    symbol, region = data_packet
    
    try:
        stock = yf.Ticker(symbol)
        
        # Info abrufen (Netzwerk-Call)
        try:
            info = stock.info
        except:
            return None # Überspringen bei Fehler

        # --- A. PREIS & TREND ---
        price = info.get('currentPrice')
        sma_200 = info.get('twoHundredDayAverage')
        
        if price is None: return None
        
        # Trend-Filter: Nur kaufen, wenn Kurs ÜBER dem 200er Schnitt ist
        if CHECK_TREND and sma_200 is not None:
            if price < sma_200:
                return None 

        # --- B. FUNDAMENTALDATEN ---
        pe = info.get('trailingPE')
        peg = info.get('pegRatio')
        growth = info.get('revenueGrowth')
        debt = info.get('debtToEquity')
        eps = info.get('trailingEps')
        fcf = info.get('freeCashflow')
        name = info.get('shortName')

        # 1. Profitabilität (Muss Gewinn machen)
        if eps is None or eps <= MIN_EPS: return None
        
        # 2. Cash Flow (Muss Geld verdienen)
        if CHECK_FCF and (fcf is None or fcf <= 0): return None
        
        # 3. Schulden (Keine Pleitekandidaten)
        if debt is None or debt > MAX_DEBT_EQUITY: return None
        
        # 4. Wachstum (Keine sterbenden Firmen)
        if growth is None or growth < MIN_REVENUE_GROWTH: return None

        # --- C. BEWERTUNG (VALUATION) ---
        is_undervalued = False
        reason = ""

        # Szenario 1: PEG Ratio ist verfügbar und gut
        if peg is not None and peg <= MAX_PEG_RATIO:
            is_undervalued = True
            reason = f"PEG {peg}"
        
        # Szenario 2: Kein PEG, aber KGV ist sehr niedrig (Graham)
        elif pe is not None and pe < MAX_PE_RATIO:
            is_undervalued = True
            reason = f"KGV {pe} (Kein PEG)"
        
        if not is_undervalued: return None

        # --- TREFFER! ---
        return {
            'Region': region,
            'Symbol': symbol,
            'Name': name,
            'Price': price,
            'Reason': reason,
            'Link': f"https://finance.yahoo.com/quote/{symbol}"
        }

    except Exception:
        return None

# ==========================================
# 3. HAUPTPROGRAMM (START)
# ==========================================

if __name__ == "__main__":
    
    # Listen zusammenführen
    all_jobs = []
    
    # Dubletten vermeiden (z.B. SAP ist in DAX und EuroStoxx)
    seen_symbols = set()

    for t in get_sp500_tickers():
        if t not in seen_symbols:
            all_jobs.append((t, "US"))
            seen_symbols.add(t)
            
    for t in get_dax_tickers():
        if t not in seen_symbols:
            all_jobs.append((t, "DE"))
            seen_symbols.add(t)
            
    for t in get_euro_tickers():
        if t not in seen_symbols:
            all_jobs.append((t, "EU"))
            seen_symbols.add(t)

    print(f"\nStarte GLOBAL-SCAN von {len(all_jobs)} Aktien...")
    print("="*60)
    print(f"{'Region':<8} | {'Symbol':<10} | {'Name':<30} | Grund")
    print("-" * 75)

    results = []
    
    # Multithreading starten
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {executor.submit(analyze_stock, job): job for job in all_jobs}
        
        counter = 0
        total = len(all_jobs)
        
        for future in concurrent.futures.as_completed(future_to_stock):
            counter += 1
            # Fortschrittsanzeige (überschreibt sich selbst)
            print(f"Fortschritt: {counter}/{total} checked...", end="\r")
            
            res = future.result()
            if res:
                results.append(res)
                # Ausgabe löscht den Fortschrittsbalken und schreibt den Treffer
                print(f"✅ [{res['Region']}] | {res['Symbol']:<10} | {res['Name'][:30]:<30} | {res['Reason']}")

    # --- ABSCHLUSS ---
    print("\n" + "="*60)
    print(f"SCAN BEENDET. {len(results)} Treffer gefunden.")
    print("="*60)

    if results:
        df = pd.DataFrame(results)
        # Sortieren: Erst nach Region, dann nach Bewertung
        df = df.sort_values(by=['Region', 'Symbol'])
        
        # Speichern
        filename = "global_watchlist.csv"
        df.to_csv(filename, index=False)
        print(f"Liste wurde gespeichert als '{filename}'")
        print("Viel Erfolg bei der Analyse!")
    else:
        print("Keine Aktien gefunden. Der Markt ist aktuell teuer oder im Abwärtstrend.")