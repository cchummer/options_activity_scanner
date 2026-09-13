from flask import Blueprint, render_template, request, abort, redirect, url_for, jsonify
from db import SessionLocal
from models import Signal, OptionTick
from sqlalchemy import func
from pathlib import Path
from datetime import datetime
import os
import pandas as pd

bp = Blueprint("api", __name__)

DATA_DIR = Path(os.getenv("SCANNER_DATA_DIR", "/Volumes/1TBT7/dev/options-scanner-plus/feature_store_archives"))
FILE_PREFIX = "convergence_signals_"
FILE_SUFFIX = ".csv"

def _to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

# Tier result rows, currently based on insider_conviction_score. TODO: Implement more sophisticated scoring in the future.
def _tier_rows(rows):
    """
    3-tier prioritization by insider_conviction_score:
      Tier 1 (highest): score >= 1
      Tier 2 (medium): score > 0
      Tier 3 (lowest):  score == 0 OR missing/unparseable
    """
    tier1, tier2, tier3 = [], [], []

    for r in rows:
        # Missing or non-numeric goes to tier3 by default
        score = _to_float(r.get("insider_conviction_score"), default=-999999)

        if score >= 1:
            tier1.append(r)
        elif score > 0:
            tier2.append(r)
        else:
            tier3.append(r)

    # Sort each tier descending by score
    key_fn = lambda row: _to_float(row.get("insider_conviction_score"), default=-999999)
    tier1.sort(key=key_fn, reverse=True)
    tier2.sort(key=key_fn, reverse=True)
    tier3.sort(key=key_fn, reverse=True)

    return tier1, tier2, tier3

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

@bp.add_app_template_global
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

#bp.jinja_env.globals["sec_url"] = sec_url

@bp.route("/api/signals/latest")
def api_signals_latest():
    session = SessionLocal()
    try:
        run_date = request.args.get("run_date")  # optional
        q = session.query(Signal)
        if run_date:
            q = q.filter(Signal.run_date == run_date)
        rows = q.order_by(Signal.created_at.desc()).limit(500).all()
        return jsonify([r.raw for r in rows])
    finally:
        session.close()


@bp.route("/api/option_matrix")
def api_option_matrix():
    symbol = request.args.get("symbol")
    run_date = request.args.get("run_date")
    if not symbol or not run_date:
        return jsonify({"error": "symbol and run_date required"}), 400
    session = SessionLocal()
    try:
        rows = session.query(OptionTick).filter(
            OptionTick.symbol == symbol,
            OptionTick.run_date == run_date
        ).all()
        # assemble maps
        expiries = sorted({r.expiry for r in rows})
        strikes = sorted({r.strike for r in rows})
        # map expiry->strike->iv / oi / volume
        iv_map = { (e,s): None for e in expiries for s in strikes }
        oi_map = { (e,s): 0 for e in expiries for s in strikes }
        vol_map = { (e,s): 0 for e in expiries for s in strikes }
        for r in rows:
            key = (r.expiry, r.strike)
            # choose average if multiple
            iv_map[key] = (iv_map[key] + r.implied_vol)/2 if iv_map[key] else r.implied_vol
            oi_map[key] = max(oi_map[key] or 0, r.open_interest or 0)
            vol_map[key] = (vol_map[key] or 0) + (r.volume or 0)

        # build 2D lists for Plotly
        iv_matrix = [[iv_map.get((e,s)) for s in strikes] for e in expiries]
        oi_matrix = [[oi_map.get((e,s)) for s in strikes] for e in expiries]
        vol_matrix = [[vol_map.get((e,s)) for s in strikes] for e in expiries]

        return jsonify({
            "expiries": expiries,
            "strikes": strikes,
            "iv_matrix": iv_matrix,
            "oi_matrix": oi_matrix,
            "vol_matrix": vol_matrix
        })
    finally:
        session.close()

@bp.route("/")
def index():
    dates = list(_date_map().keys())
    return render_template("index.html", dates=dates)

@bp.route("/day/<date_str>")
def day_view(date_str):
    df = _load_day(date_str)
    rows = df.to_dict(orient="records")
    columns = list(df.columns)
    selected_symbol = request.args.get("symbol") or (rows[0]["symbol"] if rows else "")

    tier1_rows, tier2_rows, tier3_rows = _tier_rows(rows)

    return render_template(
        "day.html",
        date_str=date_str,
        columns=columns,
        rows=rows,  # keep for backward compatibility if needed
        tier1_rows=tier1_rows,
        tier2_rows=tier2_rows,
        tier3_rows=tier3_rows,
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

@bp.route("/rolling/<int:days>")
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

@bp.route("/weekly")
def weekly():
    return rolling_view(7)

@bp.route("/monthly")
def monthly():
    return rolling_view(30)

@bp.route("/search")
def search():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return redirect(url_for("api.index"))
    return redirect(url_for("api.ticker_view", symbol=symbol))


@bp.route("/ticker/<symbol>")
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