from flask import Blueprint, render_template, request, abort, redirect, url_for, jsonify
from .query_helpers import get_symbol_metrics_range, apply_metric_filters, compute_trend_metrics, get_symbol_recurrences
from .db import SessionLocal
from .models import Signal, OptionTick, SymbolDayMetric
from pathlib import Path
from datetime import datetime
import os
import pandas as pd

bp = Blueprint("api", __name__)

DATA_DIR = Path(os.getenv("SCANNER_DATA_DIR", "/Volumes/1TBT7/dev/options-scanner-plus/feature_store_archives"))
FILE_PREFIX = "convergence_signals_"
FILE_SUFFIX = ".csv"

# --- Utility: SEC URL formatter ---
@bp.add_app_template_global
def sec_url(symbol: str, form: str | None = None) -> str:
    base = "https://www.sec.gov/cgi-bin/browse-edgar"
    query = f"action=getcompany&CIK={symbol}&owner=include&count=40"
    if form:
        query += f"&type={form}"
    return f"{base}?{query}"

# --- CSV helpers (for /day view only) ---
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

# --- Tier rows by insider conviction ---
def _to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def _tier_rows(rows):
    """
    3-tier prioritization by insider_conviction_score:
      Tier 1 (highest): score >= 1
      Tier 2 (medium): score > 0
      Tier 3 (lowest):  score == 0 OR missing
    """
    tier1, tier2, tier3 = [], [], []

    for r in rows:
        score = _to_float(r.get("insider_conviction_score"), default=-999999)

        if score >= 1:
            tier1.append(r)
        elif score > 0:
            tier2.append(r)
        else:
            tier3.append(r)

    key_fn = lambda row: _to_float(row.get("insider_conviction_score"), default=-999999)
    tier1.sort(key=key_fn, reverse=True)
    tier2.sort(key=key_fn, reverse=True)
    tier3.sort(key=key_fn, reverse=True)

    return tier1, tier2, tier3

# --- Helper: build symbol-day metrics map for a single date ---
def _get_metrics_map_for_date(run_date: str) -> dict:
    """
    Query SymbolDayMetric for a single date, return dict keyed by symbol.
    Prefers .raw JSON if available; falls back to model fields.
    """
    session = SessionLocal()
    try:
        rows = session.query(SymbolDayMetric).filter(SymbolDayMetric.run_date == run_date).all()
        out = {}
        for r in rows:
            if isinstance(r.raw, dict) and r.raw:
                out[r.symbol] = r.raw
            else:
                out[r.symbol] = {
                    "symbol": r.symbol,
                    "run_date": str(r.run_date),
                    "last_price": r.last_price,
                    "total_call_volume": r.total_call_volume,
                    "total_put_volume": r.total_put_volume,
                    "call_volume_share": r.call_volume_share,
                    "total_call_oi": r.total_call_oi,
                    "total_put_oi": r.total_put_oi,
                    "call_put_oi_ratio": r.call_put_oi_ratio,
                    "call_oi_change": r.call_oi_change,
                    "avg_call_iv": r.avg_call_iv,
                    "avg_put_iv": r.avg_put_iv,
                    "call_put_iv_diff": r.call_put_iv_diff,
                    "consecutive_days_call_build": r.consecutive_days_call_build,
                    "call_build_flag": r.call_build_flag,
                    "insider_conviction_score": r.insider_conviction_score,
                    "market_regime": r.market_regime,
                    "is_coiling": r.is_coiling,
                    "catalyst_flag": r.catalyst_flag,
                }
        return out
    finally:
        session.close()

# --- Routes ---

@bp.route("/")
def index():
    dates = list(_date_map().keys())
    return render_template("index.html", dates=dates)

@bp.route("/api/option_matrix")
def api_option_matrix():
    """
    Return end-of-day option matrices for one symbol and scan date.
    Used by detail drill-down views.
    """
    symbol = (request.args.get("symbol") or "").strip().upper()
    run_date = (request.args.get("run_date") or "").strip()

    if not symbol or not run_date:
        return jsonify({"error": "symbol and run_date required"}), 400

    session = SessionLocal()

    try:
        rows = (
            session.query(OptionTick)
            .filter(OptionTick.symbol == symbol, OptionTick.run_date == run_date)
            .all()
        )

        if not rows:
            return jsonify({"error": f"No option data found for {symbol} on {run_date}"}), 404

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

        valid_rows = [
            row for row in rows
            if row.expiry is not None and row.strike is not None and str(row.right).upper() in {"C", "P"}
        ]

        if not valid_rows:
            return jsonify({"error": f"No valid option rows found for {symbol} on {run_date}"}), 404

        expiries = sorted({str(row.expiry) for row in valid_rows})
        strikes = sorted({float(row.strike) for row in valid_rows})

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
            contract = side_data.get(key)
            if contract is None:
                return default
            return contract.get(field, default)

        def ratio(numerator, denominator):
            if numerator is None or denominator is None or denominator <= 0:
                return None
            return numerator / denominator

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

                if call_iv is not None and put_iv is not None:
                    iv_difference = call_iv - put_iv
                    iv_skew_pct = (((call_iv / put_iv) - 1.0) * 100.0) if put_iv > 0 else None
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
                call_put_oi_ratio_row.append(ratio(call_oi, put_oi))
                call_put_vol_ratio_row.append(ratio(call_volume, put_volume))

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

@bp.route("/api/iv_distribution")
def api_iv_distribution():
    """
    Return contract-level info for a symbol + run_date:
      - conid, expiry, strike, right, iv, delta, oi, volume, bid/ask/last, moneyness, multiplier
    Example: /api/iv_distribution?symbol=AAPL&run_date=2026-06-12
    """
    symbol = (request.args.get("symbol") or "").strip().upper()
    run_date = (request.args.get("run_date") or "").strip()
    if not symbol or not run_date:
        return jsonify({"error": "symbol and run_date required"}), 400

    session = SessionLocal()
    try:
        # optionally cache the result for a short time
        #@cache_ttl(300)
        def _query(symbol_inner, run_date_inner):
            rows = session.query(OptionTick).filter(
                OptionTick.symbol == symbol_inner,
                OptionTick.run_date == run_date_inner
            ).all()
            # try to get a last price from Signal or OptionTick underlying
            sig = session.query(Signal).filter(Signal.symbol == symbol_inner, Signal.run_date == run_date_inner).first()
            last_price = getattr(sig, "last_price", None) if sig else None

            out = []
            for r in rows:
                if r.expiry is None or r.strike is None:
                    continue
                try:
                    strike_f = float(r.strike)
                except Exception:
                    continue
                moneyness = None
                try:
                    up = getattr(r, "underlying_price", None)
                    base_price = last_price or up
                    if base_price:
                        moneyness = (strike_f / float(base_price)) - 1.0
                except Exception:
                    moneyness = None

                out.append({
                    "conid": int(r.conid) if r.conid is not None else None,
                    "expiry": str(r.expiry),
                    "strike": strike_f,
                    "right": (r.right or "").upper() if r.right is not None else None,
                    "iv": float(r.implied_vol) if r.implied_vol is not None else None,
                    "delta": float(r.delta) if r.delta is not None else None,
                    "oi": int(r.open_interest or 0),
                    "volume": int(r.volume or 0),
                    "bid": float(r.bid) if r.bid is not None else None,
                    "ask": float(r.ask) if r.ask is not None else None,
                    "last": float(r.last) if r.last is not None else None,
                    "moneyness": float(moneyness) if moneyness is not None else None,
                    "multiplier": float(r.multiplier) if r.multiplier else 100.0,
                    "raw": r.raw or {}
                })
            # simple sort: calls first, then by oi desc
            out.sort(key=lambda x: (x["right"] != "C", -x["oi"]))
            return out

        data = _query(symbol, run_date)
        return jsonify(data)
    finally:
        session.close()

from sqlalchemy import func

@bp.route("/api/expiry_oi_timeseries")
def api_expiry_oi_timeseries():
    """
    Return expiry -> list of {date, total_oi, total_volume} for CALLs for a symbol over a date range.
    Query parameters:
      - symbol (required)
      - date_from (optional YYYY-MM-DD)
      - date_to   (optional YYYY-MM-DD)
    Response format:
      { "dates": [...], "series": { "2026-09-17": [ints...], ... } }  (expiry keys)
    """
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "symbol required"}), 400
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")

    session = SessionLocal()
    try:
        # group by run_date + expiry for CALLs
        q = session.query(
            OptionTick.run_date.label("run_date"),
            OptionTick.expiry.label("expiry"),
            func.sum(OptionTick.open_interest).label("total_oi"),
            func.sum(OptionTick.volume).label("total_volume")
        ).filter(OptionTick.symbol == symbol, func.upper(OptionTick.right) == "C")

        if date_from:
            q = q.filter(OptionTick.run_date >= date_from)
        if date_to:
            q = q.filter(OptionTick.run_date <= date_to)

        q = q.group_by(OptionTick.run_date, OptionTick.expiry).order_by(OptionTick.run_date.asc())
        rows = q.all()

        # pivot-> dict[date][expiry] = oi
        pivot = {}
        totals_by_expiry = {}
        for run_date, expiry, total_oi, total_vol in rows:
            d = str(run_date)
            e = str(expiry) if expiry is not None else "None"
            pivot.setdefault(d, {})[e] = int(total_oi or 0)
            totals_by_expiry[e] = totals_by_expiry.get(e, 0) + int(total_oi or 0)

        # choose top expiries by total OI across range
        top_n = 4
        top_expiries = [e for e,_ in sorted(totals_by_expiry.items(), key=lambda kv: kv[1], reverse=True)[:top_n]]

        dates = sorted(pivot.keys())
        series = {}
        for e in top_expiries:
            series[e] = [pivot.get(d, {}).get(e, 0) for d in dates]

        # group the rest into 'Other'
        series["Other"] = []
        for d in dates:
            rest = 0
            for e, _ in totals_by_expiry.items():
                if e not in top_expiries:
                    rest += pivot.get(d, {}).get(e, 0)
            series["Other"].append(rest)

        return jsonify({"dates": dates, "series": series, "top_expiries": top_expiries})
    finally:
        session.close()

# ============================================
# PRIMARY VIEWS: Three main routes
# ============================================

@bp.route("/day/<date_str>")
def day_view(date_str):
    """
    Single-date scan view: show all candidates from the CSV for this date,
    enriched with SymbolDayMetric data. Filter by metric fields.
    """
    df = _load_day(date_str)
    csv_rows = df.to_dict(orient="records")
    columns = list(df.columns)

    metrics_map = _get_metrics_map_for_date(date_str)

    extended_rows = []
    for r in csv_rows:
        symbol = (r.get("symbol") or "").upper()
        m = metrics_map.get(symbol)
        merged = r.copy()
        merged["metrics"] = m or {}
        # Expose key metrics at top-level for easier filtering/display
        merged["call_volume_share"] = m.get("call_volume_share") if m else None
        merged["call_build_flag"] = m.get("call_build_flag") if m else None
        merged["consecutive_days_call_build"] = m.get("consecutive_days_call_build") if m else None
        merged["insider_conviction_score"] = m.get("insider_conviction_score") if m else None
        merged["total_call_volume"] = m.get("total_call_volume") if m else None
        merged["total_call_oi"] = m.get("total_call_oi") if m else None
        merged["market_regime"] = m.get("market_regime") if m else None
        merged["catalyst_flag"] = m.get("catalyst_flag") if m else None
        extended_rows.append(merged)

    # Filter rows based on query parameters
    symbol_filter = (request.args.get("symbol") or "").strip().upper()
    market_regime = (request.args.get("market_regime") or "").strip()
    min_insider = request.args.get("min_insider")
    call_build_flag = request.args.get("call_build_flag")

    filtered_rows = []
    for r in extended_rows:
        if symbol_filter and (r.get("symbol") or "").upper() != symbol_filter:
            continue
        if market_regime and (r.get("market_regime") or "") != market_regime:
            continue
        if min_insider:
            try:
                score = float(r.get("insider_conviction_score") or 0)
                if score < float(min_insider):
                    continue
            except Exception:
                continue
        if call_build_flag == "1":
            if r.get("call_build_flag") not in [1, "1", True]:
                continue
        elif call_build_flag == "0":
            if r.get("call_build_flag") in [1, "1", True]:
                continue
        filtered_rows.append(r)

    tier1_rows, tier2_rows, tier3_rows = _tier_rows(filtered_rows)
    initial_symbol_filter = symbol_filter or (extended_rows[0]["symbol"] if extended_rows else "")

    return render_template(
        "day.html",
        date_str=date_str,
        columns=columns,
        rows=extended_rows,
        tier1_rows=tier1_rows,
        tier2_rows=tier2_rows,
        tier3_rows=tier3_rows,
        initial_symbol_filter=initial_symbol_filter,
    )

@bp.route("/ticker/<symbol>")
def ticker_view(symbol):
    """
    Multi-day symbol history: query SymbolDayMetric over custom date range,
    show trends, charts, and multi-day metrics.
    """
    symbol = symbol.upper()
    date_from = request.args.get("date_from") or ""
    date_to = request.args.get("date_to") or ""

    metrics_rows = get_symbol_metrics_range(symbol, date_from=date_from, date_to=date_to)

    if not metrics_rows:
        return render_template(
            "ticker.html",
            symbol=symbol,
            rows=[],
            found=False,
            date_from=date_from,
            date_to=date_to,
            latest_metric=None,
            recurrence_summary={},
            chart_points=[],
        )

    metrics_rows = sorted(metrics_rows, key=lambda r: r.get("run_date", ""))
    latest_metric = metrics_rows[-1]
    trend = compute_trend_metrics(metrics_rows)

    chart_points = [{
        "date": r.get("run_date"),
        "call_oi": r.get("total_call_oi"),
        "call_vol_share": r.get("call_volume_share"),
        "insider": r.get("insider_conviction_score"),
        "avg_call_iv": r.get("avg_call_iv"),
        "call_build_flag": r.get("call_build_flag"),
    } for r in metrics_rows]

    return render_template(
        "ticker.html",
        symbol=symbol,
        rows=metrics_rows,
        found=True,
        latest_metric=latest_metric,
        date_from=date_from,
        date_to=date_to,
        recurrence_summary=trend,
        chart_points=chart_points,
    )

@bp.route("/recurrences")
def recurrences():
    """
    Cross-symbol recurrence search: find symbols appearing N+ times over a date range,
    with configurable metric filters.
    """
    date_from = request.args.get("date_from") or ""
    date_to = request.args.get("date_to") or ""
    min_occurrences = request.args.get("min_occurrences", "2")

    try:
        min_occurrences = max(1, int(min_occurrences))
    except ValueError:
        min_occurrences = 2

    filters = {
        "min_call_volume_share": request.args.get("min_call_volume_share") or None,
        "max_call_volume_share": request.args.get("max_call_volume_share") or None,
        "min_insider_conviction": request.args.get("min_insider_conviction") or None,
        "max_insider_conviction": request.args.get("max_insider_conviction") or None,
        "min_consecutive_call_build": request.args.get("min_consecutive_call_build") or None,
        "has_call_build_flag": request.args.get("has_call_build_flag") == "on",
        "has_catalyst": request.args.get("has_catalyst") == "on",
        "market_regimes": request.args.getlist("market_regime"),
        "min_call_oi": request.args.get("min_call_oi") or None,
        "max_call_oi": request.args.get("max_call_oi") or None,
        "min_call_iv": request.args.get("min_call_iv") or None,
        "max_call_iv": request.args.get("max_call_iv") or None,
    }

    matches = get_symbol_recurrences(date_from=date_from, date_to=date_to, min_occurrences=min_occurrences)

    filtered = []
    for item in matches:
        row_metrics = apply_metric_filters(item["raw_metrics"], filters)
        if not row_metrics:
            continue
        filtered.append({
            "symbol": item["symbol"],
            "occurrences": len(row_metrics),
            "dates": [r.get("run_date") for r in row_metrics],
            "trends": compute_trend_metrics(row_metrics),
            "raw_metrics": row_metrics,
        })

    return render_template(
        "recurrences.html",
        date_from=date_from,
        date_to=date_to,
        min_occurrences=min_occurrences,
        rows=filtered,
        filters=filters,
    )

# ============================================
# AUXILIARY ROUTES (kept for backward compat)
# ============================================

@bp.route("/search")
def search():
    """Redirect: search by symbol → ticker view."""
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return redirect(url_for("api.index"))
    return redirect(url_for("api.ticker_view", symbol=symbol))

@bp.route("/api/signals/latest")
def api_signals_latest():
    """Legacy API: return raw Signal rows for a date."""
    session = SessionLocal()
    try:
        run_date = request.args.get("run_date")
        q = session.query(Signal)
        if run_date:
            q = q.filter(Signal.run_date == run_date)
        rows = q.order_by(Signal.created_at.desc()).limit(500).all()
        return jsonify([r.raw for r in rows])
    finally:
        session.close()

@bp.route("/api/metrics/latest")
def api_metrics_latest():
    """Legacy API: return SymbolDayMetric rows for a date."""
    run_date = request.args.get("run_date")
    if not run_date:
        return jsonify({"error": "run_date required"}), 400
    metrics = _get_metrics_map_for_date(run_date)
    return jsonify(list(metrics.values()))

# --- DEPRECATED: /rolling/* routes now redirect to /recurrences ---
@bp.route("/rolling")
@bp.route("/rolling/")
@bp.route("/rolling/<int:days>")
@bp.route("/weekly")
@bp.route("/monthly")
def rolling_deprecated(days=None):
    """Redirect old rolling views to the new recurrences view."""
    return redirect(url_for("api.recurrences"))