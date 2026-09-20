from flask import Blueprint, render_template, request, abort, redirect, url_for, jsonify
from .db import SessionLocal
from .models import Signal, OptionTick
from sqlalchemy import func
from pathlib import Path
from datetime import datetime
import os
import pandas as pd

bp = Blueprint("api", __name__)

DATA_DIR = Path(os.getenv("SCANNER_DATA_DIR", "/Volumes/1TBT7/dev/options-scanner-plus/feature_store_archives"))
FILE_PREFIX = "convergence_signals_"
FILE_SUFFIX = ".csv"

DISPLAY_COLUMN_CANDIDATES = {
    "iv_rank": ["iv_rank_52wk", "iv_rank_13wk"],
    "iv_percentile": ["iv_pct_52wk", "iv_pct_13wk"],
    "option_volume_expansion": ["opt_vol_expansion_ratio"],
    "call_put_skew": ["call_put_iv_skew_pct", "leap_volume_skews"],
    "call_volume_concentration": ["call_vol_concentration_pct", "deal_band_concentration_pct"],
    "atm_oi_skew": ["atm_oi_skews", "leap_oi_skews"],
    "insider_conviction": ["insider_conviction_score"],
    "catalyst_flag": ["catalyst_flag"],
    "market_regime": ["market_regime"],
    "is_coiling": ["is_coiling"],
    "term_structure_flag": ["term_structure_flag"],
    "call_skew_flag": ["call_skew_flag"],
}

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

def _first_existing(columns, candidates):
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None

def _display_columns(columns):
    selected = {}
    for label, candidates in DISPLAY_COLUMN_CANDIDATES.items():
        selected[label] = _first_existing(columns, candidates)
    return selected

def _compact_signal_columns(columns):
    display = _display_columns(columns)
    compact = []
    for key in (
        "last_price",
        "iv_rank",
        "iv_percentile",
        "option_volume_expansion",
        "call_put_skew",
        "call_volume_concentration",
        "atm_oi_skew",
        "insider_conviction",
        "catalyst_flag",
        "market_regime",
        "is_coiling",
        "term_structure_flag",
        "call_skew_flag",
    ):
        if key == "last_price" and "last_price" in columns:
            compact.append("last_price")
            continue
        col = display.get(key)
        if col and col not in compact:
            compact.append(col)
    return compact, display

def _validated_days(raw_days, default=14, min_days=1, max_days=180):
    if raw_days in (None, ""):
        return default
    try:
        days = int(raw_days)
    except (TypeError, ValueError):
        return default
    return min(max(days, min_days), max_days)

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
    """
    Return end-of-day option matrices for one symbol and scan date.

    Matrix dimensions:
        rows    = expiries
        columns = strikes

    The database is expected to contain one call row and one put row for
    each symbol/run_date/expiry/strike combination.
    """
    symbol = (request.args.get("symbol") or "").strip().upper()
    run_date = (request.args.get("run_date") or "").strip()

    if not symbol or not run_date:
        return jsonify({
            "error": "symbol and run_date required"
        }), 400

    session = SessionLocal()

    try:
        rows = (
            session.query(OptionTick)
            .filter(
                OptionTick.symbol == symbol,
                OptionTick.run_date == run_date,
            )
            .all()
        )

        if not rows:
            return jsonify({
                "error": f"No option data found for {symbol} on {run_date}"
            }), 404

        def safe_float(value):
            if value is None:
                return None

            try:
                value = float(value)
            except (TypeError, ValueError):
                return None

            if pd.isna(value):
                return None

            return value

        def safe_int(value):
            if value is None:
                return 0

            try:
                value = int(value)
            except (TypeError, ValueError):
                return 0

            return max(0, value)

        # Keep only rows that can participate in a heatmap cell.
        valid_rows = [
            row for row in rows
            if row.expiry is not None
            and row.strike is not None
            and str(row.right).upper() in {"C", "P"}
        ]

        if not valid_rows:
            return jsonify({
                "error": f"No valid option rows found for {symbol} on {run_date}"
            }), 404

        expiries = sorted({
            str(row.expiry)
            for row in valid_rows
        })

        strikes = sorted({
            float(row.strike)
            for row in valid_rows
        })

        # Each map is keyed by exactly one heatmap cell:
        #
        #     (expiry, strike)
        #
        # Calls and puts are intentionally kept separate.
        call_data = {}
        put_data = {}

        for row in valid_rows:
            expiry = str(row.expiry)
            strike = float(row.strike)
            right = str(row.right).upper()
            key = (expiry, strike)

            contract_data = {
                "iv": safe_float(row.implied_vol),
                "oi": safe_int(row.open_interest),
                "volume": safe_int(row.volume),
                "delta": safe_float(row.delta),
                "gamma": safe_float(row.gamma),
                "vega": safe_float(row.vega),
                "theta": safe_float(row.theta),
                "bid": safe_float(row.bid),
                "ask": safe_float(row.ask),
                "last": safe_float(row.last),
            }

            if right == "C":
                call_data[key] = contract_data
            else:
                put_data[key] = contract_data

        def value(side_data, key, field, default=None):
            """
            Get one field from one call/put cell.

            Missing call or put contracts become None for market metrics such
            as IV, and zero for count-like metrics such as OI and volume.
            """
            contract = side_data.get(key)

            if contract is None:
                return default

            return contract.get(field, default)

        def ratio(numerator, denominator):
            """
            Return a ratio only when the denominator is positive.

            Returning None instead of 0 avoids incorrectly implying that a
            legitimate zero ratio was observed when the denominator was zero.
            """
            if numerator is None or denominator is None:
                return None

            if denominator <= 0:
                return None

            return numerator / denominator

        # Initialize all matrices with the same dimensions:
        #
        #     len(expiries) rows
        #     len(strikes) columns
        #
        call_iv_matrix = []
        put_iv_matrix = []
        iv_diff_matrix = []
        iv_skew_pct_matrix = []

        call_oi_matrix = []
        put_oi_matrix = []
        total_oi_matrix = []

        call_vol_matrix = []
        put_vol_matrix = []
        total_vol_matrix = []

        call_put_oi_ratio_matrix = []
        call_put_vol_ratio_matrix = []

        for expiry in expiries:
            call_iv_row = []
            put_iv_row = []
            iv_diff_row = []
            iv_skew_pct_row = []

            call_oi_row = []
            put_oi_row = []
            total_oi_row = []

            call_vol_row = []
            put_vol_row = []
            total_vol_row = []

            call_put_oi_ratio_row = []
            call_put_vol_ratio_row = []

            for strike in strikes:
                key = (expiry, strike)

                call_iv = value(call_data, key, "iv", default=None)
                put_iv = value(put_data, key, "iv", default=None)

                call_oi = value(call_data, key, "oi", default=0)
                put_oi = value(put_data, key, "oi", default=0)

                call_volume = value(call_data, key, "volume", default=0)
                put_volume = value(put_data, key, "volume", default=0)

                # IV difference is expressed in volatility points.
                #
                # Example:
                #     call IV = 0.31
                #     put IV  = 0.34
                #     difference = -0.03
                #
                # This means call IV is 3 volatility points below put IV.
                if call_iv is not None and put_iv is not None:
                    iv_difference = call_iv - put_iv
                    iv_skew_pct = (
                        ((call_iv / put_iv) - 1.0) * 100.0
                        if put_iv > 0
                        else None
                    )
                else:
                    iv_difference = None
                    iv_skew_pct = None

                call_iv_row.append(call_iv)
                put_iv_row.append(put_iv)
                iv_diff_row.append(iv_difference)
                iv_skew_pct_row.append(iv_skew_pct)

                call_oi_row.append(call_oi)
                put_oi_row.append(put_oi)
                total_oi_row.append(call_oi + put_oi)

                call_vol_row.append(call_volume)
                put_vol_row.append(put_volume)
                total_vol_row.append(call_volume + put_volume)

                call_put_oi_ratio_row.append(
                    ratio(call_oi, put_oi)
                )
                call_put_vol_ratio_row.append(
                    ratio(call_volume, put_volume)
                )

            call_iv_matrix.append(call_iv_row)
            put_iv_matrix.append(put_iv_row)
            iv_diff_matrix.append(iv_diff_row)
            iv_skew_pct_matrix.append(iv_skew_pct_row)

            call_oi_matrix.append(call_oi_row)
            put_oi_matrix.append(put_oi_row)
            total_oi_matrix.append(total_oi_row)

            call_vol_matrix.append(call_vol_row)
            put_vol_matrix.append(put_vol_row)
            total_vol_matrix.append(total_vol_row)

            call_put_oi_ratio_matrix.append(call_put_oi_ratio_row)
            call_put_vol_ratio_matrix.append(call_put_vol_ratio_row)

        return jsonify({
            "symbol": symbol,
            "run_date": run_date,

            "expiries": expiries,
            "strikes": strikes,

            "call_iv_matrix": call_iv_matrix,
            "put_iv_matrix": put_iv_matrix,
            "iv_diff_matrix": iv_diff_matrix,
            "iv_skew_pct_matrix": iv_skew_pct_matrix,

            "call_oi_matrix": call_oi_matrix,
            "put_oi_matrix": put_oi_matrix,
            "total_oi_matrix": total_oi_matrix,

            "call_vol_matrix": call_vol_matrix,
            "put_vol_matrix": put_vol_matrix,
            "total_vol_matrix": total_vol_matrix,

            "call_put_oi_ratio_matrix": call_put_oi_ratio_matrix,
            "call_put_vol_ratio_matrix": call_put_vol_ratio_matrix,
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
    compact_columns, display_columns = _compact_signal_columns(columns)
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
        compact_columns=compact_columns,
        display_columns=display_columns,
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

@bp.route("/rolling")
@bp.route("/rolling/")
def rolling_query_view():
    days = _validated_days(request.args.get("days"))
    return rolling_view(days)

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
    compact_columns, display_columns = _compact_signal_columns(columns)
    rows = df.to_dict(orient="records")

    return render_template(
        "ticker.html",
        symbol=symbol,
        rows=rows,
        columns=columns,
        compact_columns=compact_columns,
        display_columns=display_columns,
        found=True,
    )