from flask import Flask, render_template, request, abort, redirect, url_for
from pathlib import Path
from datetime import datetime
import os
import pandas as pd

app = Flask(__name__)

DATA_DIR = Path(os.getenv("SCANNER_DATA_DIR", "/Volumes/1TBT7/dev/options-scanner-plus/feature_store_archives"))
FILE_PREFIX = "convergence_signals_"
FILE_SUFFIX = ".csv"

def _date_from_filename(path: Path) -> str | None:
    name = path.stem
    if not name.startswith(FILE_PREFIX):
        return None
    raw = name.replace(FILE_PREFIX, "")
    try:
        return datetime.strptime(raw, "%Y%m%d").date().isoformat()
    except ValueError:
        return None

def _date_map() -> dict:
    date_map = {}
    for path in DATA_DIR.glob(f"{FILE_PREFIX}*{FILE_SUFFIX}"):
        date_str = _date_from_filename(path)
        if date_str:
            date_map[date_str] = path
    return dict(sorted(date_map.items(), reverse=True))

def _load_day(date_str: str) -> pd.DataFrame:
    date_map = _date_map()
    path = date_map.get(date_str)
    if not path:
        abort(404, f"No scan file found for {date_str}")
    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        df["timestamp"] = date_str
    return df

def sec_url(symbol: str, form: str | None = None) -> str:
    base = "https://www.sec.gov/cgi-bin/browse-edgar"
    query = f"action=getcompany&CIK={symbol}&owner=include&count=40"
    if form:
        query += f"&type={form}"
    return f"{base}?{query}"

def _ticker_history(symbol: str) -> pd.DataFrame:
    symbol = symbol.upper()
    date_map = _date_map()
    frames = []
    for date_str, path in date_map.items():
        df = pd.read_csv(path)
        if "symbol" not in df.columns:
            continue
        match = df[df["symbol"].astype(str).str.upper() == symbol]
        if not match.empty:
            match = match.copy()
            match["scan_date"] = date_str
            frames.append(match)
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()

app.jinja_env.globals["sec_url"] = sec_url

@app.route("/")
def index():
    dates = list(_date_map().keys())
    return render_template("index.html", dates=dates)

@app.route("/day/<date_str>")
def day_view(date_str):
    df = _load_day(date_str)
    rows = df.to_dict(orient="records")
    columns = list(df.columns)
    selected_symbol = request.args.get("symbol") or (rows[0]["symbol"] if rows else "")
    return render_template(
        "day.html",
        date_str=date_str,
        columns=columns,
        rows=rows,
        selected_symbol=selected_symbol,
    )

def _rolling_frames(days: int) -> pd.DataFrame:
    date_map = _date_map()
    date_list = list(date_map.keys())[:days]
    frames = []
    for d in date_list:
        df = _load_day(d)
        df["scan_date"] = d
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

@app.route("/rolling/<int:days>")
def rolling_view(days: int):
    df = _rolling_frames(days)
    if df.empty:
        return render_template("rolling.html", days=days, rows=[], industries=[])

    counts = (
        df.groupby("symbol")
        .agg(
            appearances=("symbol", "count"),
            days=("scan_date", lambda s: sorted(set(s))),
            industry=("industry", "first")  # Pulls industry straight from your daily CSV rows
        )
        .reset_index()
        .sort_values(["appearances", "symbol"], ascending=[False, True])
    )
    counts = counts[counts["appearances"] > 1]
    counts["industry"] = counts["industry"].fillna("Unknown")

    industry_breakdown = (
        counts.groupby("industry")["symbol"]
        .nunique()
        .reset_index()
        .rename(columns={"symbol": "ticker_count"})
        .sort_values("ticker_count", ascending=False)
    )

    rows = counts.to_dict(orient="records")
    industries = industry_breakdown.to_dict(orient="records")

    return render_template(
        "rolling.html",
        days=days,
        rows=rows,
        industries=industries,
    )

@app.route("/weekly")
def weekly():
    return rolling_view(7)

@app.route("/monthly")
def monthly():
    return rolling_view(30)

@app.route("/search")
def search():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return redirect(url_for("index"))
    return redirect(url_for("ticker_view", symbol=symbol))


@app.route("/ticker/<symbol>")
def ticker_view(symbol):
    symbol = symbol.upper()
    df = _ticker_history(symbol)

    if df.empty:
        return render_template(
            "ticker.html", symbol=symbol, rows=[], columns=[], found=False
        )

    df = df.sort_values("scan_date", ascending=False)
    columns = [c for c in df.columns if c != "scan_date"]
    rows = df.to_dict(orient="records")

    return render_template(
        "ticker.html", symbol=symbol, rows=rows, columns=columns, found=True
    )

if __name__ == "__main__":
    app.run(debug=True, port=5000)