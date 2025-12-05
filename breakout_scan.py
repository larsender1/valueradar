import yfinance as yf
import pandas as pd
import concurrent.futures
import warnings
import pandas_ta as ta
import requests

# FutureWarnings unterdrücken
warnings.filterwarnings("ignore", category=FutureWarning)

# ==========================================
# KONFIGURATION
# ==========================================
MAX_WORKERS = 15
MIN_BARS = 150
MIN_SCORE_TO_SAVE = 80


# ==========================================
# 1. DATENQUELLEN (REGIONEN)
# ==========================================

def get_us_watchlist():
    print("Lade US High-Beta / Growth-Watchlist...")
    tickers = [
        "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA",
        "PLTR", "NET", "SNOW", "UBER",
        "CELH", "ELF", "ONON", "IOT",
        "AVAV", "APP", "ARM", "SHOP",
        "MSTR", "COIN", "RIOT", "MARA",
    ]
    tickers = sorted(set(tickers))
    print(f"-> {len(tickers)} US-Watchlist-Aktien bereit.")
    return tickers


def get_sp500_tickers():
    print("Lade S&P 500 (USA)...")
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        html = requests.get(url, headers=headers, timeout=10).text
        tables = pd.read_html(html)

        df = None
        for tbl in tables:
            if "Symbol" in tbl.columns:
                df = tbl
                break

        if df is None:
            print("[WARN] Konnte S&P 500 Tabelle nicht finden.")
            return []

        tickers = df["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist()
        tickers = sorted(set(tickers))
        print(f"-> {len(tickers)} S&P 500 Aktien bereit.")
        return tickers

    except Exception as e:
        print(f"[ERROR] S&P 500 Laden: {e}")
        return []


def get_dax_tickers():
    print("Lade DAX 40 (Deutschland)...")
    dax = [
        "ADS.DE", "AIR.DE", "ALV.DE", "BAS.DE", "BAYN.DE", "BEI.DE", "BMW.DE", "BNR.DE",
        "CBK.DE", "CON.DE", "1COV.DE", "DTG.DE", "DBK.DE", "DB1.DE", "DHL.DE", "DTE.DE",
        "EOAN.DE", "FRE.DE", "HNR1.DE", "HEI.DE", "HEN3.DE", "IFX.DE", "MBG.DE", "MRK.DE",
        "MTX.DE", "MUV2.DE", "PUM.DE", "QIA.DE", "RWE.DE", "SAP.DE", "SRT3.DE", "SIE.DE",
        "ENR.DE", "SY1.DE", "VOW3.DE", "VNA.DE", "ZAL.DE", "SHL.DE", "HLAG.DE", "RHM.DE",
    ]
    dax = sorted(set(dax))
    print(f"-> {len(dax)} DE-Aktien bereit.")
    return dax


def get_euro_tickers():
    print("Lade EURO STOXX 50 (Europa)...")
    euro = [
        "ASML.AS", "MC.PA", "SAP.DE", "PRX.AS", "SIE.DE", "TTE.PA", "SAN.MC", "OR.PA",
        "ALV.DE", "AIR.PA", "IBE.MC", "RMS.PA", "SU.PA", "AI.PA", "DTE.DE", "BNP.PA",
        "ABI.BR", "ITX.MC", "VOW3.DE", "BAYN.DE", "BMW.DE", "INGA.AS", "BAS.DE", "MBG.DE",
        "KER.PA", "AD.AS", "CS.PA", "SAF.PA", "MUV2.DE", "ENEL.MI", "ISP.MI", "ENI.MI",
        "STLAM.MI", "RACE.MI", "ORA.PA", "DG.PA", "BN.PA", "CAP.PA", "NOKIA.HE", "AH.AS",
        "UNA.AS", "PHIA.AS", "HEIA.AS", "KNEBV.HE", "BBVA.MC", "CRH.L",
    ]
    euro = sorted(set(euro))
    print(f"-> {len(euro)} EU-Aktien bereit.")
    return euro


def get_asia_tickers():
    print("Lade Asien High-Beta / Tech / EV...")
    asia = [
        "9984.T", "6758.T", "7203.T", "6861.T", "7974.T",
        "005930.KS", "000660.KS", "035420.KS", "035720.KS",
        "0700.HK", "9988.HK", "3690.HK", "9618.HK", "1211.HK", "2318.HK",
        "TSM", "NIO", "LI", "XPEV",
    ]
    asia = sorted(set(asia))
    print(f"-> {len(asia)} ASIA-Aktien bereit.")
    return asia


# ==========================================
# TECHNISCHE INDIKATOREN
# ==========================================

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if "Close" not in df or "Volume" not in df:
        return df

    close = df["Close"]
    vol = df["Volume"]

    # Stabile EMA-Berechnung über Preisreihe
    df["ema10"] = ta.ema(close, length=10)
    df["ema20"] = ta.ema(close, length=20)
    df["sma50"] = ta.sma(close, length=50)
    df["sma200"] = ta.sma(close, length=200)

    df["rsi14"] = compute_rsi(close, 14)

    # Bollinger Band Breite
    ma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    upper = ma20 + 2 * std20
    lower = ma20 - 2 * std20
    df["bb_width"] = (upper - lower) / close

    df["vol_50d"] = vol.rolling(50).mean()
    df["rvol"] = vol / df["vol_50d"]

    df["high_252"] = close.rolling(252, min_periods=50).max()

    return df


# ==========================================
# BREAKOUT-SCORE
# ==========================================

def score_breakout(df: pd.DataFrame):
    if len(df) < MIN_BARS:
        return None

    df = compute_indicators(df)
    last = df.iloc[-1]

    # Fehlende Kernwerte -> unbrauchbar
    for col in ["sma200", "sma50", "ema10", "ema20", "rsi14"]:
        if pd.isna(last.get(col)):
            return None

    close = float(last["Close"])
    ema10 = float(last["ema10"])
    ema20 = float(last["ema20"])
    sma50 = float(last["sma50"])
    sma200 = float(last["sma200"])
    rsi = float(last["rsi14"])
    rvol = float(last["rvol"]) if not pd.isna(last["rvol"]) else None
    bb = float(last["bb_width"]) if not pd.isna(last["bb_width"]) else None
    high_252 = float(last["high_252"]) if not pd.isna(last["high_252"]) else None

    bbw_50d_mean = df["bb_width"].rolling(50).mean().iloc[-1]

    # Trendfilter
    if not (close > sma50 and close > sma200):
        return None

    # Volatilitätskompression
    if bb is None or pd.isna(bbw_50d_mean) or not (bb < bbw_50d_mean):
        return None

    # Momentum
    if rsi < 50 or rsi > 85:
        return None

    # Volumenfilter
    if rvol is None or rvol < 1.3:
        return None

    # 52W Nähe
    if high_252 is None or close / high_252 < 0.9:
        return None

    score = 0
    flags = []

    # Trend: EMA stacking
    if close > ema10 > ema20 > sma50 > sma200:
        score += 20
        flags.append("Trend: perfektes EMA-Stacking")
    else:
        score += 10
        flags.append("Trend: über 50/200 SMA")

    # Volatilität
    bbw_120_min = df["bb_width"].rolling(120, min_periods=50).min().iloc[-1]
    if not pd.isna(bbw_120_min) and bb <= bbw_120_min * 1.2:
        score += 20
        flags.append("Volatilität: starker Squeeze (VCP-Proxy)")
    elif bb < bbw_50d_mean * 0.8:
        score += 15
        flags.append("Volatilität: enger Squeeze")
    else:
        score += 8
        flags.append("Volatilität: leicht komprimiert")

    # Momentum
    if 55 <= rsi <= 70:
        score += 15
        flags.append("Momentum: RSI Pre-Breakout")
    elif 70 < rsi <= 80:
        score += 10
        flags.append("Momentum: RSI Power Zone")
    else:
        score += 5

    # Volume
    if rvol >= 2.0:
        score += 25
        flags.append("Volumen: RVol > 2")
    elif rvol >= 1.6:
        score += 18
        flags.append("Volumen: RVol > 1.6")
    else:
        score += 10

    # Pattern-Proximity
    rel = close / high_252
    if rel >= 0.98:
        score += 20
        flags.append("Muster: direkt am 52W-High")
    elif rel >= 0.95:
        score += 15
        flags.append("Muster: nahe 52W-High")
    else:
        score += 8

    return {
        "score": score,
        "close": close,
        "rsi": rsi,
        "rvol": rvol,
        "bb_width": bb,
        "flags": "; ".join(flags),
    }


# ==========================================
# EINZELAKTIEN-ANALYSE
# ==========================================

def analyze_stock(job):
    symbol, region = job
    try:
        df = yf.download(symbol, period="1y", interval="1d", auto_adjust=False, progress=False)

        # Kein DataFrame → skip
        if df is None or df.empty:
            print(f"[WARN] Keine Daten für {symbol}")
            return None

        # MultiIndex flatten
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        if "Close" not in df or "Volume" not in df:
            print(f"[WARN] {symbol}: Close/Volume fehlen")
            return None

        result = score_breakout(df)
        if result is None:
            return None

        return {
            "Region": region,
            "Symbol": symbol,
            "Price": round(result["close"], 2),
            "Score": int(result["score"]),
            "RSI14": round(result["rsi"], 2),
            "RVol": round(result["rvol"], 2) if result["rvol"] else None,
            "BBW": result["bb_width"],
            "Flags": result["flags"],
            "Link": f"https://finance.yahoo.com/quote/{symbol}",
        }

    except Exception as e:
        print(f"[ERROR] {symbol}: {e}")
        return None


# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":

    all_jobs = []
    seen = set()

    # Listen hinzufügen
    for t in get_us_watchlist():
        if t not in seen:
            seen.add(t)
            all_jobs.append((t, "US"))

    for t in get_dax_tickers():
        if t not in seen:
            seen.add(t)
            all_jobs.append((t, "DE"))

    for t in get_euro_tickers():
        if t not in seen:
            seen.add(t)
            all_jobs.append((t, "EU"))

    for t in get_asia_tickers():
        if t not in seen:
            seen.add(t)
            all_jobs.append((t, "ASIA"))

    for t in get_sp500_tickers():
        if t not in seen:
            seen.add(t)
            all_jobs.append((t, "US"))

    print(f"\nStarte GLOBAL-BREAKOUT-SCAN von {len(all_jobs)} Aktien...")
    print("=" * 80)
    print(f"{'Region':<6} | {'Symbol':<10} | {'Score':<5} | {'RSI':<6} | {'RVol':<6} | Kurzinfo")
    print("-" * 80)

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(analyze_stock, job): job for job in all_jobs}
        counter = 0
        total = len(all_jobs)

        for f in concurrent.futures.as_completed(futures):
            counter += 1
            print(f"Fortschritt: {counter}/{total} checked...", end="\r")

            res = f.result()
            if res:
                results.append(res)
                print(
                    f"✅ [{res['Region']}] | {res['Symbol']:<10} | {res['Score']:<5} | "
                    f"{res['RSI14']:<6} | {str(res['RVol']):<6} | {(res['Flags'] or '')[:40]}..."
                )

    print("\n" + "=" * 80)

    if results:
        df = pd.DataFrame(results).sort_values(by=["Score", "Region", "Symbol"], ascending=[False, True, True])
        candidates = df[df["Score"] >= MIN_SCORE_TO_SAVE]

        if not candidates.empty:
            candidates.to_csv("breakout_scan.csv", index=False)
            print(f"{len(candidates)} Breakout-Kandidaten gefunden und gespeichert!")
        else:
            print("Keine Kandidaten über Score-Grenze.")
            print("Top 10 nach Score:")
            print(df.head(10)[["Region", "Symbol", "Score", "RSI14", "RVol", "Flags"]])
    else:
        print("Gar keine auswertbaren Daten!")

    print("=" * 80)
