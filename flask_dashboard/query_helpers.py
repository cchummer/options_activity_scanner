from datetime import datetime, date
from typing import List, Dict, Any, Optional

from .db import SessionLocal
from .models import SymbolDayMetric

def parse_date_arg(date_arg: str | None, default: Optional[date] = None) -> Optional[date]:
    if not date_arg:
        return default
    try:
        if isinstance(date_arg, str):
            return datetime.strptime(date_arg.strip(), "%Y-%m-%d").date()
        return date_arg
    except Exception:
        return default


def get_symbol_metrics_range(
    symbol: str,
    date_from: Optional[str | date] = None,
    date_to: Optional[str | date] = None,
) -> List[Dict]:
    """
    Return all SymbolDayMetric rows for this symbol in a date range,
    sorted ascending by run_date.
    """
    session = SessionLocal()
    try:
        df = parse_date_arg(date_from)
        dt = parse_date_arg(date_to)

        q = session.query(SymbolDayMetric).filter(SymbolDayMetric.symbol == symbol)

        if df:
            q = q.filter(SymbolDayMetric.run_date >= df)
        if dt:
            q = q.filter(SymbolDayMetric.run_date <= dt)

        rows = q.order_by(SymbolDayMetric.run_date.asc()).all()

        out = []
        for r in rows:
            if isinstance(r.raw, dict) and r.raw:
                out.append(r.raw)
            else:
                out.append({
                    "symbol": r.symbol,
                    "run_date": str(r.run_date),
                    "last_price": r.last_price,
                    "pct_change": r.pct_change,
                    "total_call_volume": r.total_call_volume,
                    "total_put_volume": r.total_put_volume,
                    "call_volume_share": r.call_volume_share,
                    "total_call_oi": r.total_call_oi,
                    "total_put_oi": r.total_put_oi,
                    "call_put_oi_ratio": r.call_put_oi_ratio,
                    "call_oi_change": r.call_oi_change,
                    "call_volume_change": r.call_volume_change,
                    "avg_call_iv": r.avg_call_iv,
                    "avg_put_iv": r.avg_put_iv,
                    "call_put_iv_diff": r.call_put_iv_diff,
                    "consecutive_days_call_build": r.consecutive_days_call_build,
                    "call_build_flag": r.call_build_flag,
                    "insider_conviction_score": r.insider_conviction_score,
                    "market_regime": r.market_regime,
                    "is_coiling": r.is_coiling,
                    "catalyst_flag": r.catalyst_flag,
                    "catalyst_type": r.catalyst_type,
                    "catalyst_confidence": r.catalyst_confidence,
                    "deal_band_call_share": r.deal_band_call_share,
                    "top_call_line_share": r.top_call_line_share,
                })
        return out
    finally:
        session.close()


def get_symbols_in_range(
    date_from: Optional[str | date] = None,
    date_to: Optional[str | date] = None,
) -> List[str]:
    session = SessionLocal()
    try:
        df = parse_date_arg(date_from)
        dt = parse_date_arg(date_to)

        q = session.query(SymbolDayMetric.symbol).distinct()

        if df:
            q = q.filter(SymbolDayMetric.run_date >= df)
        if dt:
            q = q.filter(SymbolDayMetric.run_date <= dt)

        rows = q.order_by(SymbolDayMetric.symbol.asc()).all()
        return [r[0] for r in rows]
    finally:
        session.close()


def apply_metric_filters(metrics_rows: List[Dict], filters: Dict[str, Any]) -> List[Dict]:
    """
    Flexible filtering for recurring symbol analysis.

    Example allowed filters:
      - min_call_volume_share
      - max_call_volume_share
      - min_insider_conviction
      - max_insider_conviction
      - min_consecutive_call_build
      - has_call_build_flag
      - has_catalyst
      - market_regimes
      - min_call_oi
      - max_call_oi
      - min_call_iv
      - max_call_iv
    """
    filtered = []

    for row in metrics_rows:
        if "min_call_volume_share" in filters and filters["min_call_volume_share"] is not None:
            if row.get("call_volume_share") is None or row.get("call_volume_share") < float(filters["min_call_volume_share"]):
                continue

        if "max_call_volume_share" in filters and filters["max_call_volume_share"] is not None:
            if row.get("call_volume_share") is None or row.get("call_volume_share") > float(filters["max_call_volume_share"]):
                continue

        if "min_insider_conviction" in filters and filters["min_insider_conviction"] is not None:
            score = row.get("insider_conviction_score") or 0
            if float(score) < float(filters["min_insider_conviction"]):
                continue

        if "max_insider_conviction" in filters and filters["max_insider_conviction"] is not None:
            score = row.get("insider_conviction_score") or 0
            if float(score) > float(filters["max_insider_conviction"]):
                continue

        if "min_consecutive_call_build" in filters and filters["min_consecutive_call_build"] is not None:
            ccb = row.get("consecutive_days_call_build") or 0
            if int(ccb) < int(filters["min_consecutive_call_build"]):
                continue

        if filters.get("has_call_build_flag"):
            if row.get("call_build_flag") not in [1, "1", True]:
                continue

        if filters.get("has_catalyst"):
            if row.get("catalyst_flag") not in [1, "1", True]:
                continue

        if filters.get("market_regimes"):
            if row.get("market_regime") not in filters["market_regimes"]:
                continue

        if "min_call_oi" in filters and filters["min_call_oi"] is not None:
            val = row.get("total_call_oi") or 0
            if int(val) < int(filters["min_call_oi"]):
                continue

        if "max_call_oi" in filters and filters["max_call_oi"] is not None:
            val = row.get("total_call_oi") or 0
            if int(val) > int(filters["max_call_oi"]):
                continue

        if "min_call_iv" in filters and filters["min_call_iv"] is not None:
            val = row.get("avg_call_iv")
            if val is None or float(val) < float(filters["min_call_iv"]):
                continue

        if "max_call_iv" in filters and filters["max_call_iv"] is not None:
            val = row.get("avg_call_iv")
            if val is None or float(val) > float(filters["max_call_iv"]):
                continue

        filtered.append(row)

    return filtered


def compute_trend_metrics(metrics_rows: List[Dict]) -> Dict[str, Any]:
    """
    Summarize a symbol's multi-day behavior over a date range.
    """
    if not metrics_rows:
        return {"count": 0}

    def safe_float(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    def safe_int(v):
        try:
            return int(v) if v is not None else None
        except Exception:
            return None

    call_oi = [safe_int(r.get("total_call_oi")) for r in metrics_rows if r.get("total_call_oi") is not None]
    call_vol_share = [safe_float(r.get("call_volume_share")) for r in metrics_rows if r.get("call_volume_share") is not None]
    call_iv = [safe_float(r.get("avg_call_iv")) for r in metrics_rows if r.get("avg_call_iv") is not None]
    insider = [safe_float(r.get("insider_conviction_score")) for r in metrics_rows if r.get("insider_conviction_score") is not None]

    return {
        "count": len(metrics_rows),
        "first_date": metrics_rows[0].get("run_date"),
        "last_date": metrics_rows[-1].get("run_date"),
        "call_oi_min": min(call_oi) if call_oi else None,
        "call_oi_max": max(call_oi) if call_oi else None,
        "call_oi_avg": sum(call_oi) / len(call_oi) if call_oi else None,
        "call_oi_first": metrics_rows[0].get("total_call_oi"),
        "call_oi_last": metrics_rows[-1].get("total_call_oi"),
        "call_oi_change": (
            safe_int(metrics_rows[-1].get("total_call_oi")) - safe_int(metrics_rows[0].get("total_call_oi"))
            if metrics_rows else None
        ),
        "call_vol_share_avg": sum(call_vol_share) / len(call_vol_share) if call_vol_share else None,
        "call_iv_avg": sum(call_iv) / len(call_iv) if call_iv else None,
        "insider_avg": sum(insider) / len(insider) if insider else None,
        "max_consecutive_call_build": max((int(r.get("consecutive_days_call_build") or 0) for r in metrics_rows), default=0),
        "days_with_call_build_flag": sum(1 for r in metrics_rows if r.get("call_build_flag") in [1, "1", True]),
        "days_with_catalyst": sum(1 for r in metrics_rows if r.get("catalyst_flag") in [1, "1", True]),
    }


def get_symbol_recurrences(
    date_from: Optional[str | date] = None,
    date_to: Optional[str | date] = None,
    min_occurrences: int = 2,
) -> List[Dict]:
    """
    Return symbols that recur and their time-series metrics.
    """
    symbols = get_symbols_in_range(date_from, date_to)

    results = []
    for symbol in symbols:
        rows = get_symbol_metrics_range(symbol, date_from, date_to)
        if len(rows) >= min_occurrences:
            results.append({
                "symbol": symbol,
                "occurrences": len(rows),
                "dates": [r.get("run_date") for r in rows],
                "raw_metrics": rows,
                "trends": compute_trend_metrics(rows),
            })

    return sorted(results, key=lambda x: x["occurrences"], reverse=True)