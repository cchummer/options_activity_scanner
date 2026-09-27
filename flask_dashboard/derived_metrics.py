"""
Derived metrics helpers for options positioning profiles.

Functions provided:
- get_option_contracts(symbol, run_date) -> list[dict]
- get_previous_run_date(symbol, run_date) -> date | None
- compute_day_over_day_deltas(symbol, run_date) -> dict
- compute_persistence_flags(symbol, run_date, lookback_days=3) -> dict
- compute_rolling_symbol_history(symbol, lookback_days) -> list[dict]
- compute_symbol_day_metrics(symbol, run_date, save=True) -> dict

This module uses SQLAlchemy SessionLocal and flask_dashboard models.
"""

from datetime import datetime, timedelta, date
from decimal import Decimal
import math
from typing import List, Dict, Optional

from .db import SessionLocal
from .models import OptionTick, Signal, SymbolDayMetric

# define your deal band bounds (fractional above spot), adjust to your rules
DEAL_BAND_LO = 0.20 # TODO MAKE settings.py SINGLE SOURCE OF TRUTH for here and ibkr_eod_option_scan.py
DEAL_BAND_HI = 0.40

MULTIPLIER_DEFAULT = 100.0

# TODO put in utils.py for shared use
def _safe_float(val, default=None):
    try:
        if val is None:
            return default
        return float(val)
    except Exception:
        return default


def _safe_int(val, default=0):
    try:
        if val is None:
            return default
        return int(val)
    except Exception:
        return default


def get_option_contracts(symbol: str, run_date: str) -> List[Dict]:
    """
    Query OptionTick rows for symbol and run_date and return a list of normalized dicts.
    Each dict contains:
      conid, expiry (string), strike (float), right, volume, open_interest,
      implied_vol, delta, gamma, vega, theta, bid, ask, last, multiplier
    """
    session = SessionLocal()
    try:
        rows = (
            session.query(OptionTick)
            .filter(OptionTick.symbol == symbol, OptionTick.run_date == run_date)
            .all()
        )
        contracts = []
        for r in rows:
            mult = None
            # multiplier may be stored as '100' or '100.0' string in raw or multiplier column
            try:
                mult = float(r.multiplier) if r.multiplier is not None else None
            except Exception:
                mult = None

            contracts.append({
                "conid": int(r.conid) if r.conid is not None else None,
                "expiry": str(r.expiry) if r.expiry is not None else None,
                "strike": float(r.strike) if r.strike is not None else None,
                "right": (r.right or "").upper() if r.right is not None else None,
                "volume": _safe_int(r.volume, 0),
                "open_interest": _safe_int(r.open_interest, 0),
                "implied_vol": _safe_float(r.implied_vol, None),
                "delta": _safe_float(r.delta, None),
                "gamma": _safe_float(r.gamma, None),
                "vega": _safe_float(r.vega, None),
                "theta": _safe_float(r.theta, None),
                "bid": _safe_float(r.bid, None),
                "ask": _safe_float(r.ask, None),
                "last": _safe_float(r.last, None),
                "multiplier": mult or MULTIPLIER_DEFAULT,
                "raw": r.raw or {},
            })
        return contracts
    finally:
        session.close()


def get_previous_run_date(symbol: str, run_date: str) -> Optional[str]:
    """
    Find the previous run_date (string YYYY-MM-DD) for which OptionTick rows exist
    for this symbol, before the provided run_date.
    """
    session = SessionLocal()
    try:
        # query distinct run_date < given date ordered desc limit 1
        prev = (
            session.query(OptionTick.run_date)
            .filter(OptionTick.symbol == symbol, OptionTick.run_date < run_date)
            .distinct()
            .order_by(OptionTick.run_date.desc())
            .limit(1)
            .first()
        )
        if prev:
            # prev[0] might be datetime.date
            return str(prev[0])
        return None
    finally:
        session.close()


def compute_day_over_day_deltas(symbol: str, run_date: str) -> Dict:
    """
    Compute deltas between today's option rows and previous run_date's rows using conid matching (preferred).
    Returns a dict with aggregated deltas: call_volume_change, put_volume_change, call_oi_change, put_oi_change.
    """
    today_contracts = get_option_contracts(symbol, run_date)
    prev_date = get_previous_run_date(symbol, run_date)
    if not prev_date:
        return {
            "call_volume_change": None,
            "put_volume_change": None,
            "call_oi_change": None,
            "put_oi_change": None,
        }

    prev_contracts = get_option_contracts(symbol, prev_date)

    # map by conid if available, else (expiry,strike,right)
    prev_map = {}
    for p in prev_contracts:
        key = p["conid"] if p["conid"] else (p["expiry"], p["strike"], p["right"])
        prev_map[key] = p

    call_vol_change = 0
    put_vol_change = 0
    call_oi_change = 0
    put_oi_change = 0

    for t in today_contracts:
        key = t["conid"] if t["conid"] else (t["expiry"], t["strike"], t["right"])
        p = prev_map.get(key)
        if p:
            vol_delta = t["volume"] - p.get("volume", 0)
            oi_delta = t["open_interest"] - p.get("open_interest", 0)
        else:
            # if no previous row, treat today's values as deltas (new lines)
            vol_delta = t["volume"]
            oi_delta = t["open_interest"]

        if t["right"] == "C":
            call_vol_change += vol_delta
            call_oi_change += oi_delta
        else:
            put_vol_change += vol_delta
            put_oi_change += oi_delta

    return {
        "call_volume_change": call_vol_change,
        "put_volume_change": put_vol_change,
        "call_oi_change": call_oi_change,
        "put_oi_change": put_oi_change,
    }


def compute_persistence_flags(symbol: str, run_date: str, lookback_days: int = 3) -> Dict:
    """
    Compute simple persistence flags such as consecutive_days_call_build.
    Uses SymbolDayMetric rows when available (fast). If not available, falls
    back to examining OptionTick call shares over the last lookback_days.
    """
    session = SessionLocal()
    try:
        # try to use persisted symbol_day_metrics if present
        rows = (
            session.query(SymbolDayMetric)
            .filter(SymbolDayMetric.symbol == symbol, SymbolDayMetric.run_date <= run_date)
            .order_by(SymbolDayMetric.run_date.desc())
            .limit(lookback_days)
            .all()
        )
        if rows and len(rows) >= 1:
            # count consecutive rows from latest where call_build_flag is truthy
            consecutive = 0
            for r in rows:
                if r.call_build_flag:
                    consecutive += 1
                else:
                    break
            return {"consecutive_days_call_build": consecutive, "lookback_days_found": len(rows)}

        # fallback: compute call_build for each date by running a small aggregation
        # gather last N run_dates from OptionTick
        dates = (
            session.query(OptionTick.run_date)
            .filter(OptionTick.symbol == symbol, OptionTick.run_date <= run_date)
            .distinct()
            .order_by(OptionTick.run_date.desc())
            .limit(lookback_days)
            .all()
        )
        if not dates:
            return {"consecutive_days_call_build": 0, "lookback_days_found": 0}

        consecutive = 0
        for d_row in dates:
            d = str(d_row[0])
            contracts = get_option_contracts(symbol, d)
            total_call_vol = sum([c["volume"] for c in contracts if c["right"] == "C"])
            total_put_vol = sum([c["volume"] for c in contracts if c["right"] == "P"])
            total = total_call_vol + total_put_vol
            call_share = (total_call_vol / total) if total > 0 else 0.0
            # heuristics: call share >= 0.65 indicates call build for that day (adjustable)
            if call_share >= 0.65:
                consecutive += 1
            else:
                break
        return {"consecutive_days_call_build": consecutive, "lookback_days_found": len(dates)}
    finally:
        session.close()


def compute_rolling_symbol_history(symbol: str, lookback_days: int = 14) -> List[Dict]:
    """
    Return list of symbol_day_metrics dicts for the last N run_dates (most recent first).
    If SymbolDayMetric rows exist, return them; otherwise, compute on the fly (lightweight).
    """
    session = SessionLocal()
    try:
        rows = (
            session.query(SymbolDayMetric)
            .filter(SymbolDayMetric.symbol == symbol)
            .order_by(SymbolDayMetric.run_date.desc())
            .limit(lookback_days)
            .all()
        )
        if rows and len(rows) > 0:
            out = []
            for r in rows:
                # convert model to dict - minimal fields; raw contains full dict
                out.append({
                    "symbol": r.symbol,
                    "run_date": str(r.run_date),
                    "last_price": r.last_price,
                    "total_call_volume": r.total_call_volume,
                    "total_put_volume": r.total_put_volume,
                    "call_volume_share": r.call_volume_share,
                    "total_call_oi": r.total_call_oi,
                    "total_put_oi": r.total_put_oi,
                    "consecutive_days_call_build": r.consecutive_days_call_build,
                    "raw": r.raw,
                })
            return out

        # fallback: gather run_dates then compute metrics
        dates = (
            session.query(OptionTick.run_date)
            .filter(OptionTick.symbol == symbol)
            .distinct()
            .order_by(OptionTick.run_date.desc())
            .limit(lookback_days)
            .all()
        )
        out = []
        for d_row in dates:
            d = str(d_row[0])
            md = compute_symbol_day_metrics(symbol, d, save=False)
            out.append(md)
        return out
    finally:
        session.close()


def compute_symbol_day_metrics(symbol: str, run_date: str, save: bool = True) -> Dict:
    """
    Compute the symbol-day metrics for the given symbol and run_date.

    If save=True, persist to symbol_day_metrics table (insert or update).
    Returns the computed dict.
    """
    # Basic metadata and option contract extraction
    contracts = get_option_contracts(symbol, run_date)
    rows_ingested = len(contracts)

    # Acquire last_price from Signal if present, else from OptionTick rows 'last' or raw
    session = SessionLocal()
    try:
        sig = (
            session.query(Signal)
            .filter(Signal.symbol == symbol, Signal.run_date == run_date)
            .order_by(Signal.created_at.desc())
            .first()
        )
    finally:
        session.close()

    last_price = None
    prev_close = None
    if sig:
        last_price = _safe_float(sig.last_price, None)
        prev_close = _safe_float(sig.raw.get("prev_close") if sig.raw else None, None)
    if last_price is None and contracts:
        # try to find an underlying price from any contract
        for c in contracts:
            if c.get("last") is not None:
                last_price = float(c["last"])
                break

    if prev_close is None:
        prev_close = None

    pct_change = None
    if last_price is not None and prev_close not in (None, 0):
        try:
            pct_change = (last_price / prev_close - 1.0) * 100.0
        except Exception:
            pct_change = None

    # Basic aggregates by side
    call_contracts = [c for c in contracts if c["right"] == "C"]
    put_contracts = [c for c in contracts if c["right"] == "P"]

    total_call_volume = sum(c["volume"] for c in call_contracts)
    total_put_volume = sum(c["volume"] for c in put_contracts)
    total_option_volume = total_call_volume + total_put_volume

    total_call_oi = sum(c["open_interest"] for c in call_contracts)
    total_put_oi = sum(c["open_interest"] for c in put_contracts)
    total_oi = total_call_oi + total_put_oi

    call_volume_share = (total_call_volume / total_option_volume) if total_option_volume > 0 else None
    call_put_oi_ratio = (total_call_oi / total_put_oi) if total_put_oi > 0 else None

    # concentration measures
    top_call_line_volume = max((c["volume"] for c in call_contracts), default=0)
    top_call_line_share = (top_call_line_volume / total_call_volume) if total_call_volume > 0 else None

    # top-3 concentration
    top3 = sorted((c["volume"] for c in call_contracts), reverse=True)[:3]
    top_3_call_concentration_pct = (sum(top3) / total_call_volume) if total_call_volume > 0 else None

    # dominant expiry by call vol
    expiry_call_vol = {}
    for c in call_contracts:
        expiry_call_vol[c["expiry"]] = expiry_call_vol.get(c["expiry"], 0) + c["volume"]
    dominant_expiry_by_call_vol = None
    if expiry_call_vol:
        dominant_expiry_by_call_vol = max(expiry_call_vol.items(), key=lambda kv: kv[1])[0]

    # IV and greek aggregates (volume-weighted)
    def weighted_avg(items, value_key, weight_key="volume"):
        num = 0.0
        denom = 0.0
        for it in items:
            v = it.get(value_key)
            w = it.get(weight_key, 0) or 0
            if v is None or w == 0:
                continue
            num += float(v) * float(w)
            denom += float(w)
        return (num / denom) if denom > 0 else None

    avg_call_iv = weighted_avg(call_contracts, "implied_vol", "volume")
    avg_put_iv = weighted_avg(put_contracts, "implied_vol", "volume")
    call_put_iv_diff = None
    if avg_call_iv is not None and avg_put_iv is not None:
        call_put_iv_diff = avg_call_iv - avg_put_iv

    # delta-weighted volume (approx underlying-equivalent contract exposure)
    call_delta_weighted_volume = sum(
        (abs(c.get("delta") or 0.0) * c.get("volume", 0) * (c.get("multiplier") or MULTIPLIER_DEFAULT))
        for c in call_contracts
    )
    put_delta_weighted_volume = sum(
        (abs(c.get("delta") or 0.0) * c.get("volume", 0) * (c.get("multiplier") or MULTIPLIER_DEFAULT))
        for c in put_contracts
    )

    # Deal-band: calls whose strike is DEAL_BAND_LO..DEAL_BAND_HI above spot
    deal_band_call_volume = 0
    if last_price is not None:
        for c in call_contracts:
            strike = c.get("strike")
            if strike is None:
                continue
            moneyness = (strike / last_price) - 1.0
            if DEAL_BAND_LO <= moneyness <= DEAL_BAND_HI:
                deal_band_call_volume += c.get("volume", 0)
    deal_band_call_share = (deal_band_call_volume / total_call_volume) if total_call_volume > 0 else None

    # day-over-day deltas
    deltas = compute_day_over_day_deltas(symbol, run_date)

    # persistence flags
    persistence = compute_persistence_flags(symbol, run_date, lookback_days=3)
    consecutive_days_call_build = persistence.get("consecutive_days_call_build", 0)

    # simple call build flag heuristics (phase-1): elevated call share and positive oi change
    call_build_flag = 0
    try:
        if call_volume_share is not None and call_volume_share >= 0.65 and deltas.get("call_oi_change", 0) > 0:
            call_build_flag = 1
    except Exception:
        call_build_flag = 0

    # insider / signal context
    insider_conv = None
    market_regime = None
    is_coiling = None
    catalyst_flag = None
    catalyst_type = None
    catalyst_confidence = None

    session = SessionLocal()
    try:
        sig = (
            session.query(Signal)
            .filter(Signal.symbol == symbol, Signal.run_date == run_date)
            .order_by(Signal.created_at.desc())
            .first()
        )
        if sig:
            insider_conv = sig.insider_conviction_score
            market_regime = sig.market_regime
            is_coiling = sig.is_coiling
            catalyst_flag = sig.catalyst_flag
            catalyst_type = sig.catalyst_type
            catalyst_confidence = sig.catalyst_confidence
    finally:
        session.close()

    result = {
        "symbol": symbol,
        "run_date": run_date,
        "last_price": last_price,
        "prev_close": prev_close,
        "pct_change": pct_change,
        "market_regime": market_regime,
        "is_coiling": is_coiling,
        "insider_conviction_score": insider_conv,
        "catalyst_flag": catalyst_flag,
        "catalyst_type": catalyst_type,
        "catalyst_confidence": catalyst_confidence,
        "total_call_volume": total_call_volume,
        "total_put_volume": total_put_volume,
        "total_option_volume": total_option_volume,
        "call_volume_share": call_volume_share,
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "total_oi": total_oi,
        "call_put_oi_ratio": call_put_oi_ratio,
        "top_call_line_volume": top_call_line_volume,
        "top_call_line_share": top_call_line_share,
        "top_3_call_concentration_pct": top_3_call_concentration_pct,
        "dominant_expiry_by_call_vol": dominant_expiry_by_call_vol,
        "avg_call_iv": avg_call_iv,
        "avg_put_iv": avg_put_iv,
        "call_put_iv_diff": call_put_iv_diff,
        "call_delta_weighted_volume": call_delta_weighted_volume,
        "deal_band_call_volume": deal_band_call_volume,
        "deal_band_call_share": deal_band_call_share,
        "call_volume_change": deltas.get("call_volume_change"),
        "call_oi_change": deltas.get("call_oi_change"),
        "put_volume_change": deltas.get("put_volume_change"),
        "put_oi_change": deltas.get("put_oi_change"),
        "consecutive_days_call_build": consecutive_days_call_build,
        "call_build_flag": call_build_flag,
        "rows_ingested": rows_ingested,
        "raw_sample": contracts[:50],  # keep small sample for traceability
        "computed_at": datetime.utcnow().isoformat(),
    }

    # optionally save to DB
    if save:
        session = SessionLocal()
        try:
            # upsert-like behavior: check existing
            existing = (
                session.query(SymbolDayMetric)
                .filter(SymbolDayMetric.symbol == symbol, SymbolDayMetric.run_date == run_date)
                .one_or_none()
            )
            if existing:
                # update fields
                for k, v in result.items():
                    if hasattr(existing, k):
                        setattr(existing, k, v if not isinstance(v, (dict, list)) else None)
                existing.raw = result
                session.add(existing)
            else:
                # create new
                obj = SymbolDayMetric(
                    symbol=symbol,
                    run_date=run_date,
                    last_price=result.get("last_price"),
                    prev_close=result.get("prev_close"),
                    pct_change=result.get("pct_change"),
                    market_regime=result.get("market_regime"),
                    is_coiling=result.get("is_coiling"),
                    insider_conviction_score=result.get("insider_conviction_score"),
                    catalyst_flag=result.get("catalyst_flag"),
                    catalyst_type=result.get("catalyst_type"),
                    catalyst_confidence=result.get("catalyst_confidence"),
                    total_call_volume=result.get("total_call_volume"),
                    total_put_volume=result.get("total_put_volume"),
                    total_option_volume=result.get("total_option_volume"),
                    call_volume_share=result.get("call_volume_share"),
                    total_call_oi=result.get("total_call_oi"),
                    total_put_oi=result.get("total_put_oi"),
                    total_oi=result.get("total_oi"),
                    call_put_oi_ratio=result.get("call_put_oi_ratio"),
                    top_call_line_volume=result.get("top_call_line_volume"),
                    top_call_line_share=result.get("top_call_line_share"),
                    top_3_call_concentration_pct=result.get("top_3_call_concentration_pct"),
                    dominant_expiry_by_call_vol=result.get("dominant_expiry_by_call_vol"),
                    avg_call_iv=result.get("avg_call_iv"),
                    avg_put_iv=result.get("avg_put_iv"),
                    call_put_iv_diff=result.get("call_put_iv_diff"),
                    call_delta_weighted_volume=result.get("call_delta_weighted_volume"),
                    deal_band_call_volume=result.get("deal_band_call_volume"),
                    deal_band_call_share=result.get("deal_band_call_share"),
                    call_volume_change=result.get("call_volume_change"),
                    call_oi_change=result.get("call_oi_change"),
                    put_volume_change=result.get("put_volume_change"),
                    put_oi_change=result.get("put_oi_change"),
                    consecutive_days_call_build=result.get("consecutive_days_call_build"),
                    call_build_flag=result.get("call_build_flag"),
                    rows_ingested=result.get("rows_ingested"),
                    raw=result,
                )
                session.add(obj)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    return result