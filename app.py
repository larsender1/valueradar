from flask import Flask, render_template, jsonify, request, redirect, url_for
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import os

# --- AUTH IMPORTS (Das ist neu) ---
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from flask import render_template, request, session

app = Flask(__name__)

# --- KONFIGURATION (Das ist neu) ---
app.config['SECRET_KEY'] = 'hier-deinen-geheimen-key-einfuegen' 
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# DB & Login Init
db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' 

# User Modell
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(150), unique=True, nullable=False)
    email = db.Column(db.String(150), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)

# Datenbank erstellen
with app.app_context():
    db.create_all()

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# Globale Variable für die Daten aus global_watchlist.csv
latest_scan_results = []


def run_background_scan():
    """
    Lädt die global_watchlist.csv, die von webfinance.py erzeugt wird.
    Diese Datei enthält: Region, Symbol, Name, Price, Reason, Link.
    """
    global latest_scan_results
    try:
        if os.path.exists("global_watchlist.csv"):
            df = pd.read_csv("global_watchlist.csv")
            latest_scan_results = df.to_dict(orient="records")
            print(f"[SCAN] {len(latest_scan_results)} Zeilen aus global_watchlist.csv geladen.")
        else:
            latest_scan_results = []
            print("[SCAN] Datei global_watchlist.csv nicht gefunden.")
    except Exception as e:
        print(f"[SCAN-ERROR] Konnte global_watchlist.csv nicht laden: {e}")
        latest_scan_results = []

# --- LOGIN / REGISTER ROUTEN (NEU) ---

@app.route("/auth/register", methods=['POST'])
def register():
    data = request.json
    username = data.get('username')
    email = data.get('email')
    password = data.get('password')

    if User.query.filter_by(email=email).first():
        return jsonify({"success": False, "message": "Email existiert bereits"}), 400
    
    hashed_password = generate_password_hash(password, method='scrypt')
    new_user = User(username=username, email=email, password=hashed_password)
    
    try:
        db.session.add(new_user)
        db.session.commit()
        login_user(new_user)
        return jsonify({"success": True, "redirect": "/dashboard"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/auth/login", methods=['POST'])
def auth_login():
    data = request.json
    email = data.get('email')
    password = data.get('password')
    user = User.query.filter_by(email=email).first()

    if user and check_password_hash(user.password, password):
        login_user(user)
        return jsonify({"success": True, "redirect": "/dashboard"})
    else:
        return jsonify({"success": False, "message": "Falsche Email oder Passwort"}), 401

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


# --- SEITEN ROUTEN ---

@app.route("/")
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return render_template("index.html")   

@app.route("/dashboard")
@login_required # Schützt das Dashboard
def dashboard():
    # Wir geben den Usernamen an das HTML weiter
    return render_template("dashboard.html", username=current_user.username)


@app.route("/api/stocks")
@login_required
def get_stocks():
    if not latest_scan_results:
        run_background_scan()
    return jsonify(latest_scan_results)


@app.route("/api/details/<symbol>")
@login_required
def get_details(symbol):
    global latest_scan_results
    try:
        ticker = yf.Ticker(symbol)

        # Versuche verschiedene Wege, an Infos zu kommen
        try:
            info = ticker.info
        except Exception:
            info = {}
        # Währung
        currency = info.get("currency", "USD")

        # Name fallback
        name = info.get("longName") or info.get("shortName") or symbol
# ... (in der Funktion get_details) ...

                # Beschreibung und 52-Wochen Range ---
        full_description = info.get("longBusinessSummary", "Keine Beschreibung verfügbar.")

        # einfache „Pseudo-AI“-Kurzfassung: erste 2–3 Sätze
        description = full_description
        if full_description and len(full_description) > 600:
            parts = full_description.split(". ")
            short = ". ".join(parts[:3]).strip()
            if not short.endswith("."):
                short += "."
            description = short

        year_high = info.get("fiftyTwoWeekHigh")
        year_low = info.get("fiftyTwoWeekLow")

        # ---------------------------------------------


        # Land, Branche, Sektor
        country = info.get("country", "Unknown")
        sector = info.get("sector", "-")
        industry = info.get("industry", "-")

        # Kennzahlen: PE, PEG, PB, PS
        pe = info.get("trailingPE") or info.get("forwardPE")
        peg = info.get("pegRatio") or info.get("peg_ratio")
        pb = info.get("priceToBook") or info.get("priceToBookRatio")
        ps = info.get("priceToSalesTrailing12Months")

        # Market Cap
        market_cap = info.get("marketCap")

        # Margen, Wachstum
        profit_margin = info.get("profitMargins")
        revenue_growth = info.get("revenueGrowth")

        # Bewertung / Risiko / Qualität
        beta = info.get("beta")
        roe = info.get("returnOnEquity")
        roa = info.get("returnOnAssets")
        roic = info.get("returnOnCapitalEmployed")
        debt_to_equity = info.get("debtToEquity")
        current_ratio = info.get("currentRatio")
        quick_ratio = info.get("quickRatio")
        ev_to_ebitda = info.get("enterpriseToEbitda")
        ev_to_sales = info.get("enterpriseToRevenue")

        # Cashflows & Dividende
        free_cash_flow = info.get("freeCashflow")
        payout_ratio = info.get("payoutRatio")
        div_yield = info.get("dividendYield")


                # Preis und Performance
        price_now = info.get("currentPrice") or info.get("regularMarketPrice")

        hist = None
        perf_1y = None
        perf_3m = None
        try:
            hist = ticker.history(period="2y", interval="1d")
            if hist is not None and not hist.empty:
                hist = hist.dropna(subset=["Close"])
                last_close = float(hist["Close"].iloc[-1])

                # Nur falls currentPrice fehlt, Fallback auf letzten Close
                if price_now is None:
                    price_now = last_close

                # 1Y Performance auf Basis des Schlusskurses
                if len(hist) > 250:
                    price_1y_ago = float(hist["Close"].iloc[-252])
                    perf_1y = (last_close / price_1y_ago - 1) * 100

                # 3M Performance
                if len(hist) > 60:
                    idx_3m = max(0, len(hist) - 60)
                    price_3m_ago = float(hist["Close"].iloc[idx_3m])
                    perf_3m = (last_close / price_3m_ago - 1) * 100
        except Exception as e:
            print(f"History-Fehler {symbol}: {e}")


        # News (Yahoo Finance)
        news_list = []
        try:
            raw_news = ticker.news or []
            for n in raw_news[:6]:
                title = n.get("title")
                publisher = n.get("publisher")
                link = n.get("link")
                if not title or not publisher or not link:
                    continue

                ts = n.get("providerPublishTime")
                if ts:
                    date_str = datetime.fromtimestamp(ts).strftime("%d.%m.%Y")
                else:
                    date_str = ""

                news_list.append(
                    {
                        "title": title,
                        "publisher": publisher,
                        "link": link,
                        "date": date_str,
                    }
                )
        except Exception as e:
            print(f"News-Fehler {symbol}: {e}")

        # Pros & Risks aus Watchlist + Heuristiken
        pros = []
        risks = []

        # 1) Reason aus globaler Watchlist (falls vorhanden)
        try:
            row = next(
                (
                    r
                    for r in latest_scan_results
                    if (r.get("Symbol") or r.get("symbol")) == symbol
                ),
                None,
            )
        except Exception:
            row = None

        if row and row.get("Reason"):
            pros.append(str(row["Reason"]))

        # 2) Automatische Stärken
        if pe is not None and pe < 16:
            pros.append(f"Günstiges KGV ({pe:.1f})")
        if peg is not None and peg < 1.5:
            pros.append(f"Gutes Wachstum relativ zur Bewertung (PEG {peg:.2f})")
        if revenue_growth is not None and revenue_growth > 0:
            pros.append(f"Stabiles Umsatzwachstum ({revenue_growth*100:.1f}%)")
        if perf_1y is not None and perf_1y > 0:
            pros.append(f"Positive 1J-Performance ({perf_1y:.1f}%)")

        # 3) Automatische Risiken
        if pe is not None and pe > 25:
            risks.append(f"Hohe Bewertung (KGV {pe:.1f})")
        if revenue_growth is not None and revenue_growth < 0:
            risks.append(f"Rückläufiger Umsatz ({revenue_growth*100:.1f}%)")
        if perf_1y is not None and perf_1y < 0:
            risks.append(f"Schwache 1J-Performance ({perf_1y:.1f}%)")
        if profit_margin is not None and profit_margin < 0:
            risks.append("Negative Gewinnmarge")

        if not risks:
            risks.append("-- Keine weiteren Warnungen")

        return jsonify({
            "symbol": symbol,
            "name": name,
            "sector": sector,
            "industry": industry,

            # Preis
            "price": price_now,
            "price_now": price_now,      # für das Frontend
            "currency": currency,

            # KPIs
            "market_cap": market_cap,
            "pe": pe,
            "peg": peg,
            "pb": pb,
            "ps": ps,
            "profit_margin": profit_margin,
            "revenue_growth": revenue_growth,
            "div_yield": div_yield,
            "payout_ratio": payout_ratio,

            # Additional KPIs
            "beta": beta,
            "debt_to_equity": debt_to_equity,
            "roe": roe,
            "roa": roa,
            "roic": roic,
            "ev_to_ebitda": ev_to_ebitda,
            "ev_to_sales": ev_to_sales,
            "current_ratio": current_ratio,
            "quick_ratio": quick_ratio,
            "free_cash_flow": free_cash_flow,

            # Company
            "description": description,
            "year_high": year_high,
            "year_low": year_low,

            # Performance (für VR-Score, falls du sie nutzen willst)
            "perf_1y": perf_1y,
            "perf_3m": perf_3m,

            # Lists
            "pros": pros,
            "risks": risks,
            "news": news_list
        })




    except Exception as e:
        print(f"Fehler bei Details zu {symbol}: {e}")
        return jsonify({"error": str(e)})


@app.route("/api/history/<symbol>/<period>")
@login_required
def get_history(symbol, period):
    """Liefert OHLC + MAs + RSI für Chart."""
    try:
        ticker = yf.Ticker(symbol)

        # --- Mapping Frontend-Period -> yfinance ---
        if period == "1D":
            yf_interval, yf_period = "5m", "1d"
        elif period == "1W":   # 5 Handelstage, 30-Minuten-Kerzen
            yf_interval, yf_period = "30m", "5d"
        elif period == "1M":
            yf_interval, yf_period = "1h", "1mo"
        elif period == "6M":
            yf_interval, yf_period = "1d", "6mo"
        elif period == "1Y":
            yf_interval, yf_period = "1d", "1y"
        elif period == "5Y":
            yf_interval, yf_period = "1wk", "5y"
        elif period == "MAX":
            yf_interval, yf_period = "1wk", "max"
        else:  # Fallback
            yf_interval, yf_period = "1d", "1y"

        hist = ticker.history(interval=yf_interval, period=yf_period)
        if hist is None or hist.empty:
            return jsonify({"error": "Keine historischen Daten gefunden."})

        # nur OHLC und NaNs raus
        hist = hist[["Open", "High", "Low", "Close"]].dropna()

        # Index in Spalte umwandeln, Spaltenname kann "Date" ODER "Datetime" sein
        hist.reset_index(inplace=True)
        time_col = "Date"
        if "Datetime" in hist.columns:
            time_col = "Datetime"

        # Timestamp in ms für ApexCharts
        hist["timestamp"] = hist[time_col].astype("int64") // 10**6

        # Moving Averages (von Anfang an zeichnen)
        hist["MA20"] = hist["Close"].rolling(window=20, min_periods=1).mean()
        hist["MA50"] = hist["Close"].rolling(window=50, min_periods=1).mean()
        hist["MA200"] = hist["Close"].rolling(window=200, min_periods=1).mean()

        # RSI
        delta = hist["Close"].diff()
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        roll_up = pd.Series(gain).rolling(window=14).mean()
        roll_down = pd.Series(loss).rolling(window=14).mean()
        rs = roll_up / roll_down
        rsi = 100.0 - (100.0 / (1.0 + rs))
        hist["RSI"] = rsi

        ohlc = []
        ma20 = []
        ma50 = []
        ma200 = []
        rsi_list = []

        for _, row in hist.iterrows():
            ts = int(row["timestamp"])
            ohlc.append([
                ts,
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
            ])

            if not np.isnan(row["MA20"]):
                ma20.append({"x": ts, "y": float(row["MA20"])})
            if not np.isnan(row["MA50"]):
                ma50.append({"x": ts, "y": float(row["MA50"])})
            if not np.isnan(row["MA200"]):
                ma200.append({"x": ts, "y": float(row["MA200"])})
            if not np.isnan(row["RSI"]):
                rsi_list.append({"x": ts, "y": float(row["RSI"])})

        return jsonify({
            "candle": ohlc,
            "ma20": ma20,
            "ma50": ma50,
            "ma200": ma200,
            "rsi": rsi_list
        })

    except Exception as e:
        print(f"Chart Fehler: {e}")
        return jsonify({"error": str(e)})



if __name__ == "__main__":
    run_background_scan()
    app.run(debug=True, port=5000)