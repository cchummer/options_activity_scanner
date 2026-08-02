"""
pipeline_draft.py — Convergence Pipeline (TWS raw ibapi edition)
=================================================================
All ib_async dependencies removed. Pillar1MarketData is fully synchronous,
built on the official ibapi (EWrapper/EClient) library.

Changes vs original:
  - `from ib_async import *` removed; replaced by ibapi imports
  - Pillar1MarketData: all async methods → synchronous; IB() → IBKRApp()
  - ConvergencePipeline: async def → def; await → direct call; asyncio.run() removed
  - Pillar2SECData, Pillar3CustomParser, internal_sec_filing_fetcher: unchanged

Install: pip install ibapi
TWS ports:  Live TWS=7496, Paper TWS=7497, Live GW=4001, Paper GW=4002
"""

import sys
import os
import re
import math
import threading
import time
from datetime import datetime, timedelta
import logging

import pandas as pd
import numpy as np

# ── IBKR official Python API ──────────────────────────────────────────────────
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.scanner import ScannerSubscription

# ── Other dependencies (unchanged) ────────────────────────────────────────────
from edgar import Company, set_identity, httpclient
from filing_parser import MasterParserClass
import settings as settings

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) %(message)s'
)
# IBKR is very verbose by default, so we silence the ibapi logger to WARNING level to reduce noise.
logging.getLogger("ibapi").setLevel(logging.WARNING)

# Mandatory SEC EDGAR identification string
set_identity("SpecialSituationsQuant Engine securedhummer@gmail.com")
#httpclient.update_rate_limiter(requests_per_second=5) # 5 requests per second, to be safe


# ══════════════════════════════════════════════════════════════════════════════
# Tick-type constants
# Reference: ibapi/ticktype.py + IBKR TWS API documentation
# ══════════════════════════════════════════════════════════════════════════════

# Prices (arrive via tickPrice callback)
TICK_LAST            = 4    # Last trade price — STK
TICK_CLOSE           = 9    # Prior close price — STK

# Sizes / counts (arrive via tickSize callback)
#TICK_VOLUME          = 8    # Day volume — OPT specific contract
#TICK_OPEN_INTEREST   = 22   # Open interest — OPT specific contract (needs generic "101")
TICK_CALL_OI         = 27
TICK_PUT_OI          = 28
TICK_CALL_VOL_UND        = 29   # Day call volume for underlying — STK (needs generic "100")
TICK_PUT_VOL_UND         = 30   # Day put volume for underlying — STK (needs generic "100")
TICK_OPT_CONTRACT_VOLUME = 8   # Day volume for the option contract itself — OPT specific contract (arrives via tickSize or tickGeneric; needs generic "100")

# Average option volume (arrives via tickSize or tickGeneric; needs generic "105")
GEN_TICK_AVG_OPT_VOL     = 105
TICK_AVG_OPT_VOL         = 87

# ── Option computation tick-type IDs (arrive via tickOptionComputation, NOT tickGeneric) ──
OPT_COMP_BID   = 10   # BID_OPTION_COMPUTATION
OPT_COMP_ASK   = 11   # ASK_OPTION_COMPUTATION
OPT_COMP_LAST  = 12   # LAST_OPTION_COMPUTATION
OPT_COMP_MODEL = 13   # MODEL_OPTION_COMPUTATION — TWS's canonical displayed IV
GEN_TICK_OPTION_IV = 106  # generic tick code to request in reqMktData

# ── Informational TWS codes that are not real errors ──────────────────────────
_TWS_INFO_CODES = {2104, 2106, 2107, 2108, 2119, 2158}


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _safe_int(val, default=0):
    """
    Convert a raw tick value (int, float, Decimal, or None) to int.
    IBKR uses -1 as "not available" sentinel; those are treated as default.
    NaN is also mapped to default.
    """
    if val is None:
        return default
    try:
        f = float(val)
        if math.isnan(f) or f < 0:
            return default
        return int(f)
    except (ValueError, TypeError):
        return default


class TickerSnapshot:
    """
    Lightweight stand-in for ib_async's Ticker, exposing exactly the fields
    this pipeline reads. Populated from raw TWS tick callbacks.
    """
    __slots__ = (
        'contract',
        'last', 'close',
        'callVolume', 'putVolume', 'avOptionVolume',  # STK-level option activity
        'openInterest', 'volume',                       # OPT contract-level
        'pcRatio',  # Put/Call ratio
        'impliedVol',
        'delta',
    )

    def __init__(self, contract: Contract):
        self.contract        = contract
        self.last            = None
        self.close           = None
        self.callVolume      = None
        self.putVolume       = None
        self.avOptionVolume  = None
        self.openInterest    = None
        self.volume          = None
        self.pcRatio         = None
        self.impliedVol      = None
        self.delta           = None

# ══════════════════════════════════════════════════════════════════════════════
# IBKRApp  —  thin EWrapper / EClient
# ══════════════════════════════════════════════════════════════════════════════
class IBKRApp(EWrapper, EClient):
    """
    Low-level TWS message bridge.

    Pattern
    -------
    1.  Caller allocates a slot:  _alloc(req_id)
    2.  Caller fires a request:   reqXxx(req_id, ...)
    3.  Callbacks append to the slot's 'rows' list or update its 'ticks' dict.
    4.  The terminal callback (XxxEnd / tickSnapshotEnd) fires a threading.Event.
    5.  Caller blocks:            _wait(req_id, timeout)
    6.  Caller reads results:     _get(req_id)['rows'] or ['ticks']

    For streaming market data (snapshot=False) there is no terminal event;
    callers use time.sleep() followed by cancelMktData(req_id).
    """

    def __init__(self):
        EClient.__init__(self, self)
        self._lock           = threading.Lock()
        self._req_id_ctr     = 1
        self.connected_event = threading.Event()
        self._req_store: dict = {}   # reqId → slot dict

    # ── reqId management ──────────────────────────────────────────────────────

    def _next_req_id(self) -> int:
        with self._lock:
            rid = self._req_id_ctr
            self._req_id_ctr += 1
            return rid

    def _alloc(self, req_id: int, **extras):
        """
        Create a fresh slot for req_id before the request is sent.
        extras: pass ticks={} for market-data requests.
        """
        with self._lock:
            self._req_store[req_id] = {
                "event": threading.Event(),
                "rows":  [],
                **extras,
            }

    def _get(self, req_id: int) -> dict:
        with self._lock:
            return self._req_store.get(req_id, {})

    def _signal(self, req_id: int):
        slot = self._get(req_id)
        ev   = slot.get("event")
        if ev:
            ev.set()

    def _wait(self, req_id: int, timeout: float = 30.0) -> bool:
        slot = self._get(req_id)
        ev   = slot.get("event")
        return ev.wait(timeout=timeout) if ev else False

    # ── Connection ────────────────────────────────────────────────────────────

    def nextValidId(self, orderId: int):
        with self._lock:
            self._req_id_ctr = max(self._req_id_ctr, orderId)
        self.connected_event.set()

    # ── Error handling ────────────────────────────────────────────────────────

    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in _TWS_INFO_CODES:
            logging.debug(f"[TWS info] code={errorCode}: {errorString} at {errorTime}")
            return
        logging.error(f"[TWS error] reqId={reqId}, code={errorCode}: {errorString} at {errorTime}")
        # Unblock any waiter so we don't deadlock on fatal errors (200, 321, 502…)
        if reqId > 0:
            self._signal(reqId)

    # ── Scanner callbacks ─────────────────────────────────────────────────────

    def scannerData(self, reqId, rank, contractDetails,
                    distance, benchmark, projection, legsStr):
        slot = self._get(reqId)
        rows = slot.get("rows")
        if rows is not None:
            contract = contractDetails.contract
            logging.info(
                f"Scanner raw fields — {contract.symbol}: "
                f"distance={distance!r}, benchmark={benchmark!r}, "
                f"projection={projection!r}, legsStr={legsStr!r}"
            )
        
            rows.append({
                "contract":   contract,
                "projection": projection,
            })

    def scannerDataEnd(self, reqId):
        self._signal(reqId)

    # ── Contract details callbacks ────────────────────────────────────────────

    def contractDetails(self, reqId, contractDetails):
        slot = self._get(reqId)
        rows = slot.get("rows")
        if rows is not None:
            rows.append(contractDetails)

    def contractDetailsEnd(self, reqId):
        self._signal(reqId)

    # ── Market data callbacks ─────────────────────────────────────────────────

    def tickPrice(self, reqId, tickType, price, attrib):
        slot  = self._get(reqId)
        ticks = slot.get("ticks")
        # -1.0 = IBKR sentinel for "not available"; skip those
        if ticks is not None and price is not None and price != -1:
            ticks[tickType] = price

    def tickSize(self, reqId, tickType, size):
        slot  = self._get(reqId)
        ticks = slot.get("ticks")
        # -1 = IBKR sentinel for "not available"; skip those
        if ticks is not None and size is not None and size != -1:
            ticks[tickType] = size

    def tickGeneric(self, reqId, tickType, value):
        """Some tick types (e.g. 105 avg opt volume) arrive here instead of tickSize."""
        slot  = self._get(reqId)
        ticks = slot.get("ticks")
        if ticks is not None and value is not None:
            ticks[tickType] = value

    def tickString(self, reqId, tickType, value):
        pass  # Not needed for our data fields

    def tickSnapshotEnd(self, reqId):
        """Fired when a snapshot=True request has delivered all ticks."""
        self._signal(reqId)

    # ── Historical data callbacks ─────────────────────────────────────────────

    def historicalData(self, reqId, bar):
        slot = self._get(reqId)
        rows = slot.get("rows")
        if rows is not None:
            rows.append(bar)

    def historicalDataEnd(self, reqId, start: str, end: str):
        self._signal(reqId)

    # ── Option chain parameter callbacks ──────────────────────────────────────

    def securityDefinitionOptionParameter(self, reqId, exchange, underlyingConId,
                                          tradingClass, multiplier, expirations, strikes):
        slot = self._get(reqId)
        rows = slot.get("rows")
        if rows is not None:
            rows.append({
                "exchange":        exchange,
                "underlyingConId": underlyingConId,
                "tradingClass":    tradingClass,
                "multiplier":      multiplier,
                "expirations":     expirations,   # frozenset of "YYYYMMDD" strings
                "strikes":         strikes,        # frozenset of floats
            })

    def securityDefinitionOptionParameterEnd(self, reqId):
        self._signal(reqId)

    # ── Option computation callback (IV, delta, gamma, etc.) ───────────────────

    def tickOptionComputation(self, reqId, tickType, tickAttrib,
                              impliedVol, delta, optPrice, pvDividend,
                              gamma, vega, theta, undPrice):
        """
        Fires when generic tick 106 is requested on an OPT contract.
        impliedVol arrives as -1 or DBL_MAX when TWS can't compute it
        (e.g. no valid bid/ask to solve against) — filter both sentinels.
        Stores per computation-source (model/bid/ask/last) so callers can
        prefer the model IV, matching what TWS displays in the option chain.
        """
        slot  = self._get(reqId)
        ticks = slot.get("ticks")
        if ticks is None:
            return
        if impliedVol is not None and 0 < impliedVol <= 5:
            ticks.setdefault("iv_by_source", {})[tickType] = impliedVol
        if delta is not None and abs(delta) <= 1:
            ticks.setdefault("delta_by_source", {})[tickType] = delta


# ══════════════════════════════════════════════════════════════════════════════
# Pillar 1 — Market Data  (synchronous TWS raw API)
# ══════════════════════════════════════════════════════════════════════════════
class Pillar1MarketData:
    """
    Synchronous drop-in replacement for the original async Pillar1MarketData.

    Every formerly-async method is now a plain blocking call.  The TWS message
    loop runs in a dedicated daemon thread; callers block on threading.Events
    that are signalled by the EWrapper callbacks inside IBKRApp.

    Streaming market data (snapshot=False) has no terminal event, so those
    methods use time.sleep() — identical to the original asyncio.sleep() logic.
    """

    # ── Timing constants — mirror the original asyncio.sleep() values ─────────
    STK_STREAM_WAIT  = 5.0    # seconds: wait for STK option-volume ticks
    OPT_STREAM_WAIT  = 10.0   # seconds: OI ticks arrive once daily, can be slow
    SCANNER_TIMEOUT  = 30.0   # seconds: scanner + option-chain param requests
    HIST_TIMEOUT     = 60.0   # seconds: 5-year daily bar pulls can be large
    QUALIFY_TIMEOUT  = 15.0   # seconds: per contract-details request
    QUALIFY_DELAY    = 0.05   # seconds: pacing pause between batched qualifications

    OI_ANALYSIS_STRIKE_COUNT = 6 # how many nearest strikes to pull for the OI analysis
    WING_STRIKE_COUNT = 13   # how many nearest strikes to pull for the delta-band wing search
    WING_DELTA_LOW    = 0.15
    WING_DELTA_HIGH   = 0.35
    TERM_STRUCTURE_ANOMALY_THRESHOLD_PCT = 15.0  # flag expiries priced >15% off their neighbor-interpolated level

    DEAL_PREMIUM_LOW  = 0.20   # 20% above spot
    DEAL_PREMIUM_HIGH = 0.40   # 40% above spot

    def __init__(self, host='127.0.0.1', port=4001, client_id=1): # TWS Port=7496, Paper=7497, Live GW=4001, Paper GW=4002
        self._app      = IBKRApp()
        self.host      = host
        self.port      = port
        self.client_id = client_id
        self._thread   = None

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self):
        """
        Replacement for async connect_async().
        Connects, starts the message-loop thread, and blocks until TWS
        sends nextValidId (the readiness handshake).
        """
        logging.info(
            f"Establishing socket gateway to TWS at {self.host}:{self.port} "
            f"(clientId={self.client_id})..."
        )
        self._app.connect(self.host, self.port, clientId=self.client_id)
        self._thread = threading.Thread(
            target=self._app.run, daemon=True, name="twsapi-msg-loop"
        )
        self._thread.start()

        if not self._app.connected_event.wait(timeout=10):
            raise ConnectionError(
                "TWS did not acknowledge the connection within 10 s. "
                "Is 'Enable ActiveX and Socket Clients' checked in TWS API settings?"
            )
        self._app.reqMarketDataType(1)  # 1 = Live, 2 = Frozen, 3 = Delayed
        logging.info("TWS connection established successfully.")

    def disconnect(self):
        if self._app.isConnected():
            self._app.disconnect()
            logging.info("IBKR connection cleanly disconnected.")

    # ── Internal: contract qualification ──────────────────────────────────────

    def _qualify_one(self, contract: Contract) -> Contract | None:
        """
        Replacement for ib_async's qualifyContractsAsync() for a single contract.
        Sends reqContractDetails and blocks until contractDetailsEnd fires.
        Returns the first matching qualified Contract, or None on failure/timeout.
        """
        rid = self._app._next_req_id()
        self._app._alloc(rid)
        self._app.reqContractDetails(rid, contract)
        ok   = self._app._wait(rid, timeout=self.QUALIFY_TIMEOUT)
        rows = self._app._get(rid).get("rows", [])
        if not ok or not rows:
            logging.warning(f"Could not qualify {getattr(contract, 'symbol', '?')}")
            return None
        return rows[0].contract

    def _qualify_many(self, contracts: list) -> list:
        """
        Batch-qualify a list of contracts.
        All reqContractDetails requests are fired concurrently (with mild pacing),
        then we wait for each in order.  Returns a same-length list; failed
        entries are None.

        This replicates qualifyContractsAsync(*contracts) from ib_async.
        """
        if not contracts:
            return []

        req_ids = []
        for c in contracts:
            rid = self._app._next_req_id()
            self._app._alloc(rid)
            req_ids.append(rid)
            self._app.reqContractDetails(rid, c)
            time.sleep(self.QUALIFY_DELAY)  # mild pacing — avoids rate-limit errors

        results = []
        for rid in req_ids:
            self._app._wait(rid, timeout=self.QUALIFY_TIMEOUT)
            rows = self._app._get(rid).get("rows", [])
            results.append(rows[0].contract if rows else None)
        return results

    # ── Internal: historical bars ─────────────────────────────────────────────

    def _get_historical_bars(self, contract: Contract,
                             duration: str = '5 Y',
                             bar_size: str = '1 day',
                             what_to_show: str = 'TRADES') -> list:
        """
        Synchronous replacement for ib_async's reqHistoricalDataAsync().
        Returns a list of BarData objects (ibapi.common.BarData).
        """
        rid = self._app._next_req_id()
        self._app._alloc(rid)
        self._app.reqHistoricalData(
            rid, contract,
            '',           # endDateTime — '' = now
            duration,     # e.g. '5 Y', '5 D'
            bar_size,     # e.g. '1 day'
            what_to_show, # 'TRADES', 'OPTION_IMPLIED_VOLATILITY', etc.
            1,            # useRTH
            1,            # formatDate (1 = string "YYYYMMDD HH:MM:SS")
            False,        # keepUpToDate
            []            # chartOptions
        )
        ok = self._app._wait(rid, timeout=self.HIST_TIMEOUT)
        if not ok:
            logging.warning(
                f"Historical data timed out for {getattr(contract, 'symbol', '?')} "
                f"(whatToShow={what_to_show})"
            )
        return self._app._get(rid).get("rows", [])

    # ── IV Rank / Percentile ─────────────────────────────────────────────────

    def get_iv_rank_percentile(self, contract: Contract) -> dict | None:
        """
        Derives 52wk and 13wk IV Rank/Percentile for `contract`'s underlying,
        since TWS API exposes no native IV Rank field. Pulls one year of daily
        'OPTION_IMPLIED_VOLATILITY' bars on the STOCK contract and slices the
        tail for the 13wk window rather than firing a second request.

        Returns None if no IV bars come back (e.g. missing options data
        subscription for this underlying).
        """
        logging.info(
            f"Fetching 1-year daily IV history for underlying {getattr(contract, 'symbol', '?')}..."
        )
        bars = self._get_historical_bars(
            contract,
            duration='1 Y',
            bar_size='1 day',
            what_to_show='OPTION_IMPLIED_VOLATILITY',
        )
        time.sleep(self.QUALIFY_DELAY)
        if not bars:
            logging.warning(
                f"No IV history for underlying {getattr(contract, 'symbol', '?')}; "
                "check options market data subscription."
            )
            return None

        bars = sorted(bars, key=lambda b: b.date)
        closes = [b.close for b in bars]

        def _rank_pct(window: list) -> tuple:
            current = window[-1]
            hi, lo = max(window), min(window)
            rank = ((current - lo) / (hi - lo) * 100) if hi != lo else float('nan')
            pct  = sum(1 for v in window if v < current) / len(window) * 100
            return current, rank, pct

        cur_52, rank_52, pct_52 = _rank_pct(closes)
        cur_13, rank_13, pct_13 = _rank_pct(closes[-65:])  # ~13wk trading days

        return {
            #"symbol":        getattr(contract, 'symbol', '?'),
            "current_iv":    cur_52,
            "iv_rank_52wk":  rank_52,
            "iv_pct_52wk":   pct_52,
            "iv_rank_13wk":  rank_13,
            "iv_pct_13wk":   pct_13,
        }

    # ── Scanner ───────────────────────────────────────────────────────────────

    def scan_accumulation_candidates(self, limit=60):
        """
        Synchronous replacement for async scan_accumulation_candidates().

        Fires reqScannerData, waits for scannerDataEnd, then filters by exchange.
        The scanner's contractDetails already have primaryExchange populated, so a
        separate qualification pass is only needed when conId is missing.

        Returns: valid_contracts: list[Contract]
        """
        logging.info("Executing TWS Scanner: Low P/C Volume Ratio...")

        sub              = ScannerSubscription()
        sub.instrument   = 'STK'
        sub.locationCode = 'STK.US.MAJOR'
        sub.scanCode     = 'LOW_OPT_VOLUME_PUT_CALL_RATIO'

        rid = self._app._next_req_id()
        self._app._alloc(rid)
        # scannerSubscriptionOptions and scannerSubscriptionFilterOptions = [] (none)
        self._app.reqScannerSubscription(rid, sub, [], [])
        ok = self._app._wait(rid, timeout=self.SCANNER_TIMEOUT)
        self._app.cancelScannerSubscription(rid)  # clean up regardless of outcome
        time.sleep(0.2)

        if not ok:
            logging.error("Scanner request timed out.")
            return [], {}

        scan_rows = self._app._get(rid).get("rows", [])
        logging.info(f"Scanner returned {len(scan_rows)} raw results.")

        valid_exchanges  = {'NYSE', 'NASDAQ', 'AMEX', 'ARCA', 'BATS'}
        valid_contracts  = []

        for i, row in enumerate(scan_rows):
            contract   = row["contract"]
            projection = row["projection"]

            primary_exch = (
                getattr(contract, 'primaryExchange', '') or
                getattr(contract, 'exchange',    '')
            )

            logging.info(f'Inspecting scanner result {i}: {contract.symbol} on {primary_exch} with projection {projection}...')

            if primary_exch not in valid_exchanges:
                logging.info(
                    f"Filtered {contract.symbol}: exchange '{primary_exch}' not in valid set."
                )
                continue

            # Scanner results usually have conId; qualify only if missing
            if not getattr(contract, 'conId', 0):
                qualified = self._qualify_one(contract)
                if not qualified:
                    logging.warning(f"Could not qualify scanner result {contract.symbol} on {primary_exch}; skipping.")
                    continue
                contract = qualified

            valid_contracts.append(contract)
            logging.info(f"Scanner candidate validated: {contract.symbol} on {primary_exch}")

            if len(valid_contracts) >= limit:
                break

        return valid_contracts

    # ── STK market data snapshot (volume filter) ──────────────────────────────

    def fetch_ticker_snapshots(self, contracts: list) -> list:
        """
        Synchronous replacement for async fetch_ticker_snapshots().

        Streams STK market data with genericTickList='100,105':
          100 → call/put option volume    (tick types 29, 30)
          105 → average option volume     (tick type 105)

        All streams are opened simultaneously, held open for STK_STREAM_WAIT
        seconds (matching the original asyncio.sleep(5)), then cancelled.
        Contracts passing the volume filter are returned as TickerSnapshot objects.
        """
        logging.info(f"Requesting market snapshots for {len(contracts)} assets...")

        if not contracts:
            return []

        # Open all streaming subscriptions at once
        rid_to_ticker: dict[int, TickerSnapshot] = {}
        for contract in contracts:
            rid    = self._app._next_req_id()
            ticker = TickerSnapshot(contract)
            self._app._alloc(rid, ticks={})
            rid_to_ticker[rid] = ticker
            self._app.reqMktData(rid, contract, '100,105', False, False, [])

        # Mirror original asyncio.sleep(5.0) — give TWS time to push ticks
        time.sleep(self.STK_STREAM_WAIT)

        valid_candidates = []
        for rid, ticker in rid_to_ticker.items():
            ticks = self._app._get(rid).get("ticks", {})
            self._app.cancelMktData(rid)

            # Populate last/close price (used in execute_daily_build as last_price)
            ticker.last  = ticks.get(TICK_LAST) or ticks.get(TICK_CLOSE)
            ticker.close = ticks.get(TICK_CLOSE)

            call_vol = _safe_int(ticks.get(TICK_CALL_VOL_UND))
            put_vol  = _safe_int(ticks.get(TICK_PUT_VOL_UND))
            avg_vol  = _safe_int(ticks.get(TICK_AVG_OPT_VOL), default=1) or 1  # avoid /0

            ticker.callVolume     = call_vol
            ticker.putVolume      = put_vol
            ticker.avOptionVolume = avg_vol

            ticker.pcRatio = round(put_vol / call_vol, 4) if call_vol > 0 else 0.0

            opt_vol = call_vol + put_vol
            if opt_vol > avg_vol and opt_vol >= 1000:
                logging.info(
                    f"{ticker.contract.symbol} passed: Option Vol {opt_vol} "
                    f"(> Avg {avg_vol})"
                )
                valid_candidates.append(ticker)
            else:
                logging.info(
                    f"{ticker.contract.symbol} filtered: opt_vol={opt_vol}, avg={avg_vol}"
                )

        return valid_candidates

    # ── Single-contract STK ticker (weekend / test mode) ─────────────────────

    def get_stk_ticker(self, contract: Contract, wait: float = 3.0) -> TickerSnapshot:
        """
        Fetch a brief market data snapshot for one STK contract.
        Replacement for ib_async's reqTickersAsync().
        Used by test_execute_daily_build() for manual symbol overrides.
        """
        rid    = self._app._next_req_id()
        ticker = TickerSnapshot(contract)
        self._app._alloc(rid, ticks={})
        self._app.reqMktData(rid, contract, '100,105', False, False, [])
        time.sleep(wait)
        ticks = self._app._get(rid).get("ticks", {})
        self._app.cancelMktData(rid)

        ticker.last           = ticks.get(TICK_LAST) or ticks.get(TICK_CLOSE)
        ticker.close          = ticks.get(TICK_CLOSE)
        ticker.callVolume     = _safe_int(ticks.get(TICK_CALL_VOL_UND))
        ticker.putVolume      = _safe_int(ticks.get(TICK_PUT_VOL_UND))
        ticker.avOptionVolume = _safe_int(ticks.get(TICK_AVG_OPT_VOL), default=1) or 1
        ticker.pcRatio = round(ticker.putVolume / ticker.callVolume, 4) if ticker.callVolume > 0 else 0.0
        return ticker

    # ── Volatility contraction detection (pure pandas — unchanged) ────────────

    def detect_volatility_contraction(self, weekly_df, window=20, lookback=26,
                                      contraction_threshold=0.45):
        """
        Detects multi-week volatility contraction ("coiling base") using BB width.
        Logic identical to original — no IBKR dependency.
        """
        logging.info(
            "Calculating volatility contraction metrics using Bollinger Band width "
            "on weekly data..."
        )
        closes    = weekly_df['close']
        ma        = closes.rolling(window).mean()
        std       = closes.rolling(window).std()
        bb_width  = (std * 4) / ma

        recent_width = bb_width.iloc[-lookback:].mean()

        if len(bb_width) > lookback:
            prior_series  = bb_width.iloc[:-lookback]
            prior_mean    = prior_series.mean()
            prior_median  = prior_series.median()
        else:
            prior_mean    = bb_width.mean()
            prior_median  = bb_width.median()

        contraction_mean   = recent_width / prior_mean   if prior_mean   else np.nan
        contraction_median = recent_width / prior_median if prior_median else np.nan
        coiling_flag       = contraction_mean < contraction_threshold

        return coiling_flag, contraction_mean, contraction_median

    # ── Converging triangle detection (pure pandas — unchanged) ───────────────

    def detect_converging_triangle(self, weekly_df, lookback=26, slope_threshold=0.0):
        """
        Detects converging triangle patterns by fitting trend lines to highs/lows.
        Logic identical to original — no IBKR dependency.
        """
        logging.info("Calculating converging triangle metrics...")
        highs = weekly_df['high'].iloc[-lookback:]
        lows  = weekly_df['low'].iloc[-lookback:]
        x     = np.arange(lookback)
        high_slope    = np.polyfit(x, highs.values, 1)[0]
        low_slope     = np.polyfit(x, lows.values,  1)[0]
        triangle_flag = (high_slope < slope_threshold) and (low_slope > -slope_threshold)
        return triangle_flag, (high_slope, low_slope)

    # ── Market regime (historical data pull) ──────────────────────────────────

    def determine_market_regime(self, contract: Contract) -> dict:
        """
        Synchronous replacement for async determine_market_regime().

        Fetches 5 years of daily OHLCV via reqHistoricalData, then runs
        the same SMA/BB/triangle calculations as the original.
        """
        try:
            logging.info(
                f"Calculating market regime metrics for {contract.symbol}..."
            )
            bars = self._get_historical_bars(contract, '5 Y', '1 day')

            if len(bars) < 200:
                return self._regime_error()

            # Build DataFrame from BarData objects (ibapi.common.BarData)
            df = pd.DataFrame([{
                'date':   bar.date,
                'open':   bar.open,
                'high':   bar.high,
                'low':    bar.low,
                'close':  bar.close,
                'volume': bar.volume,
            } for bar in bars])

            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)

            # Resample to weekly bars for coiling/triangle detection
            weekly_df = df.resample('W', label='right', closed='right').agg({
                'open':   'first',
                'high':   'max',
                'low':    'min',
                'close':  'last',
                'volume': 'sum',
            }).dropna()
            logging.info(
                f"Computed weekly data for {contract.symbol}: {len(weekly_df)} weeks."
            )

            close          = df['close']
            sma200         = close.rolling(200).mean().iloc[-1]
            current        = close.iloc[-1]
            dist_to_200dma = (current - sma200) / sma200

            coiling_flag, contraction_mean, contraction_median = \
                self.detect_volatility_contraction(weekly_df)

            triangle_flag, (high_slope, low_slope) = \
                self.detect_converging_triangle(weekly_df)

            if coiling_flag and abs(dist_to_200dma) < 0.08:
                regime_label = "Accumulation Base"
            elif current > sma200:
                regime_label = "Bullish Trend"
            else:
                regime_label = "Bearish/Distribution"

            return {
                "regime":             regime_label,
                "coiling":            int(coiling_flag),
                "contraction_mean":   float(contraction_mean),
                "contraction_median": float(contraction_median),
                "triangle_flag":      int(triangle_flag),
                "high_slope":         float(high_slope),
                "low_slope":          float(low_slope),
                "dist_to_200dma":     float(dist_to_200dma),
            }
        except Exception as e:
            logging.error(
                f"Regime baseline matrix calculation failed for {contract.symbol}: {e}"
            )
            return self._regime_error()

    @staticmethod
    def _regime_error() -> dict:
        return {
            "regime": "Error", "coiling": False,
            "contraction_mean": 0.0, "contraction_median": 0.0,
            "triangle_flag": 0, "high_slope": 0.0,
            "low_slope": 0.0, "dist_to_200dma": 0.0,
        }

    # ── Option market data — chunked streaming (OI + volume) ─────────────────

    def safe_fetch_tickers(self, contracts: list, chunk_size: int = 40) -> list:
        """
        Synchronous replacement for async safe_fetch_tickers().

        Streams OPT market data in chunks using:
          genericTickList:
            100 → option volume  (tick type 8 for the specific OPT contract)
            101 → open interest  (tick type 22 for the specific OPT contract)
            106 -> implied volatility

        Uses streaming (snapshot=False) to allow OI ticks — which TWS publishes
        once per day from the OCC — to trickle in over OPT_STREAM_WAIT seconds.
        Streams are cancelled after the wait, honouring IBKR's active-line limit.

        NOTE on tick types for OPT contracts:
          TICK_CALL_VOL (29)       → day's call volume
          TICK_PUT_VOL (30)        → day's put volume
          TICK_CALL_OI (27)         → call open interest 
          TICK_PUT_OI (28)          → put open interest

          TICK_VOLUME (8)        → day's volume for this specific option
          TICK_OPEN_INTEREST (22)→ open interest for this specific option
        """
        all_tickers     = []
        valid_contracts = [c for c in contracts if getattr(c, 'conId', 0)]

        for chunk_start in range(0, len(valid_contracts), chunk_size):
            chunk      = valid_contracts[chunk_start:chunk_start + chunk_size]
            chunk_rids: dict[int, TickerSnapshot] = {}

            try:
                
                # --- Pass 1: Type 1 live streaming → OI (ticks 27/28, weekend-safe) ---
                self._app.reqMarketDataType(1)
                for contract in chunk:
                    rid    = self._app._next_req_id()
                    ticker = TickerSnapshot(contract)
                    self._app._alloc(rid, ticks={})
                    chunk_rids[rid] = ticker
                    self._app.reqMktData(rid, contract, '101,106', False, False, [])

                time.sleep(self.OPT_STREAM_WAIT)

                chunk_tickers = []
                for rid, ticker in chunk_rids.items():
                    ticks = self._app._get(rid).get("ticks", {})
                    self._app.cancelMktData(rid)
                    if ticker.contract is None:
                        continue
                    if ticker.contract.right == 'C':
                        ticker.openInterest = _safe_int(ticks.get(TICK_CALL_OI))
                    elif ticker.contract.right == 'P':
                        ticker.openInterest = _safe_int(ticks.get(TICK_PUT_OI))

                    # Prefer MODEL IV (what TWS displays); fall back to bid/ask/last
                    iv_by_source = ticks.get("iv_by_source", {})
                    ticker.impliedVol = (
                        iv_by_source.get(OPT_COMP_MODEL)
                        or iv_by_source.get(OPT_COMP_LAST)
                        or iv_by_source.get(OPT_COMP_ASK)
                        or iv_by_source.get(OPT_COMP_BID)
                    )

                    delta_by_source = ticks.get("delta_by_source", {})
                    ticker.delta = (
                        delta_by_source.get(OPT_COMP_MODEL)
                        or delta_by_source.get(OPT_COMP_LAST)
                        or delta_by_source.get(OPT_COMP_ASK)
                        or delta_by_source.get(OPT_COMP_BID)
                    )

                    chunk_tickers.append(ticker)

                # --- Pass 2: Type 2 frozen streaming → volume (tick 8, weekend-safe) ---
                self._app.reqMarketDataType(2)
                vol_rids: dict[int, TickerSnapshot] = {}
                for ticker in chunk_tickers:
                    rid = self._app._next_req_id()
                    self._app._alloc(rid, ticks={})
                    vol_rids[rid] = ticker
                    self._app.reqMktData(rid, ticker.contract, '100', False, False, [])  

                time.sleep(5.0)  # frozen ticks are served from cache, arrive fast

                for rid, ticker in vol_rids.items():
                    ticks = self._app._get(rid).get("ticks", {})
                    self._app.cancelMktData(rid)
                    ticker.volume = _safe_int(ticks.get(TICK_OPT_CONTRACT_VOLUME))

                self._app.reqMarketDataType(1)  # restore
                all_tickers.extend(chunk_tickers)

            except Exception as e:
                logging.error(f"Error during chunked ticker fetch: {e}")
                self._app.reqMarketDataType(1)
                for rid in chunk_rids:
                    try:
                        self._app.cancelMktData(rid)
                    except Exception:
                        pass
                continue
        
        return all_tickers

    # ── Positioning footprint ─────────────────────────────────────────────────

    def analyze_positioning_footprint(self, contract: Contract,
                                      underlying_price: float,
                                      min_days_to_expiry: int = 60,
                                      max_days_to_expiry: int = 650) -> dict:
        """
        Synchronous replacement for async analyze_positioning_footprint().

        Steps:
          1. reqSecDefOptParams   → get chain expirations + strikes
          2. Filter to target expiry window; pick 5 nearest strikes
          3. Build Option Contracts for up to X expiries × X strikes × C/P
          4. _qualify_many()      → resolve conIds (replaces qualifyContractsAsync)
          5. safe_fetch_tickers() → stream OI + volume for each option
          6. Aggregate call/put OI, volume and IV skew metrics
        """
        logging.info(
            f"Extracting derivative positioning footprints for {contract.symbol}... Current underlying price: {underlying_price:.2f}"
        )
        _err = {
            "atm_volume_skews": 0, "dominant_expiry_by_vol": "None",
            "dominant_expiry_by_oi": "None", "atm_oi_depth": 0, "atm_oi_skews": 0,
        }
        try:
            if not underlying_price or underlying_price == 0:
                return _err

            # ── 1. Fetch option chain parameters ──────────────────────────────
            rid = self._app._next_req_id()
            self._app._alloc(rid)
            # Replacement for: await self.ib.reqSecDefOptParamsAsync(...)
            self._app.reqSecDefOptParams(
                rid,
                contract.symbol,
                '',               # futFopExchange (blank for STK)
                contract.secType, # 'STK'
                contract.conId,
            )
            ok = self._app._wait(rid, timeout=self.SCANNER_TIMEOUT)
            if not ok:
                logging.warning(
                    f"Option chain param request timed out for {contract.symbol}"
                )
                return _err

            chain_rows = self._app._get(rid).get("rows", [])
            if not chain_rows:
                return _err

            # Prefer SMART exchange routing
            chain = next(
                (r for r in chain_rows if r["exchange"] == "SMART"),
                chain_rows[0]
            )

            # ── 2. Filter expirations to target window ────────────────────────
            now      = datetime.now()
            min_date = (now + timedelta(days=min_days_to_expiry)).strftime('%Y%m%d')
            max_date = (now + timedelta(days=max_days_to_expiry)).strftime('%Y%m%d')

            target_expiries = sorted([
                e for e in chain["expirations"]
                if min_date <= e <= max_date
            ])
            logging.info(
                f"Found {len(target_expiries)} target expirations for {contract.symbol} "
                f"between {min_days_to_expiry} and {max_days_to_expiry} days out."
            )
            if not target_expiries:
                return _err

            # ── 3. Select the nearest strikes for OI analysis and wing IV analysis ──────
            strikes        = sorted(chain["strikes"])
            strikes_by_dist = sorted(strikes, key=lambda s: abs(s - underlying_price))

            oi_strikes = strikes_by_dist[:self.OI_ANALYSIS_STRIKE_COUNT]
            if not oi_strikes:
                oi_strikes = [min(strikes, key=lambda s: abs(s - underlying_price))]

            wing_strikes = strikes_by_dist[:self.WING_STRIKE_COUNT]
            if not wing_strikes:
                wing_strikes = oi_strikes

            logging.info(f"Selected {len(oi_strikes)} nearest strikes for OI analysis: {oi_strikes}")
            logging.info(f"Selected {len(wing_strikes)} nearest strikes for wing IV analysis: {wing_strikes}")

            # Fast membership check for the OI aggregation gate later
            oi_strike_set = {round(s, 4) for s in oi_strikes}
            wing_strike_set = {round(s, 4) for s in wing_strikes}  # Gate IV skew calc to wing only

            deal_low  = underlying_price * (1 + self.DEAL_PREMIUM_LOW)
            deal_high = underlying_price * (1 + self.DEAL_PREMIUM_HIGH)
            deal_strikes = [s for s in strikes if deal_low <= s <= deal_high]
            deal_strike_set = {round(s, 4) for s in deal_strikes}

            logging.info(
                f"Deal-premium band for {contract.symbol}: "
                f"${deal_low:.2f}-${deal_high:.2f} → {len(deal_strikes)} strikes: {deal_strikes}"
            )

            # Union with wing_strikes drives the contract grid — oi_strikes is
            # unaffected and still only used for the OI-leg gate
            strikes_to_pull = sorted(set(wing_strikes) | set(deal_strikes))

            # Grab first 8 expiries that fit our time constraints above
            sampled_expiries = target_expiries[:8]

            # ── 4. Build option contracts ─────────────────────────────────────
            # (oi_strikes is a subset of wing_strikes which is now a subset of strikes_to_pull, so this single grid covers both legs —
            #  no duplicate requests, no separate qualify/stream pass needed)
            option_contracts = []
            for expiry in sampled_expiries:
                exp_date    = datetime.strptime(expiry, '%Y%m%d')
                days_to_exp = (exp_date - now).days
                logging.info(
                    f"Processing expiry {expiry} for {contract.symbol} "
                    f"with {days_to_exp} days to expiration..."
                )
                for strike in strikes_to_pull:
                    # Skip non-integer strikes for long-dated <- testing without this filter
                    #if days_to_exp > 90 and (strike % 1 != 0):
                        #continue
                    for right in ['C', 'P']:
                        opt = Contract()
                        opt.symbol                       = contract.symbol
                        opt.secType                      = 'OPT'
                        opt.exchange                     = 'SMART'
                        opt.currency                     = 'USD'
                        opt.lastTradeDateOrContractMonth = expiry
                        opt.strike                       = strike
                        opt.right                        = right
                        opt.multiplier                   = '100'
                        option_contracts.append(opt)
                        logging.info(
                            f"Prepared option contract: {opt.symbol} "
                            f"{opt.lastTradeDateOrContractMonth} {opt.strike} {opt.right}"
                        )

            # ── 5. Qualify (replaces qualifyContractsAsync) ───────────────────
            logging.info(
                f"Qualifying {len(option_contracts)} option contracts for {contract.symbol}..."
            )
            # _qualify_many fires all reqContractDetails concurrently then waits
            qualified = self._qualify_many(option_contracts)
            qualified = [q for q in qualified if q and getattr(q, 'conId', 0)]

            if not qualified:
                logging.warning(f"No valid qualified option contracts for {contract.symbol}")
                return _err

            # ── 6. Fetch contract data  ──────────────────────────────────────────
            logging.info(
                f"Requesting market data for {len(qualified)} option contracts for {contract.symbol}..."
            )
            raw_opt_tickers = self.safe_fetch_tickers(qualified, chunk_size=40)
            opt_tickers = [
                t for t in raw_opt_tickers
                if t is not None and getattr(t, 'contract', None) is not None
            ]

            # ── 7. Aggregate metrics ──────────────────────────────────────────
            total_call_vol = total_put_vol = 0
            total_call_oi  = total_put_oi  = 0
            max_oi = max_vol = 0
            total_atm_oi        = 0
            dominant_expiry_oi  = "None"
            dominant_expiry_vol = "None"
            deal_band_call_vol = deal_band_call_oi = deal_band_max_vol = 0
            deal_band_dominant_expiry = "None"
            deal_band_dominant_strike = 0.0

            # Per-expiry OTM call/put IV buckets — NOT gated to oi_strikes, uses the full wing pool
            iv_by_expiry = {}  # expiry -> {"call_ivs": [...], "put_ivs": [...]}
            call_vol_by_line = {}        # (expiry, strike) -> volume, ACROSS THE WHOLE GRID
            total_call_vol_all = 0

            logging.info(f"Analysing {len(opt_tickers)} qualified option tickers returned...")
            for ot in opt_tickers:
                oi  = _safe_int(getattr(ot, 'openInterest', 0))
                vol = _safe_int(getattr(ot, 'volume',       0))
                strike_key = round(ot.contract.strike, 4)
                is_oi_strike = strike_key in oi_strike_set
                
                logging.info(
                    f"Option {ot.contract.symbol} "
                    f"{ot.contract.lastTradeDateOrContractMonth} "
                    f"{ot.contract.strike} {ot.contract.right}: "
                    f"OI={oi}, Vol={vol}, IV={ot.impliedVol}, delta={ot.delta}"
                    f"{'  [OI leg]' if is_oi_strike else '  [wing/deal only]'}"
                )

                # ── ATM OI/volume leg: only OI_ANALYSIS_STRIKE_COUNT strikes ──
                if is_oi_strike:
                    total_atm_oi += oi
                    if oi > max_oi:
                        max_oi             = oi
                        dominant_expiry_oi = ot.contract.lastTradeDateOrContractMonth
                    if vol > max_vol:
                        max_vol             = vol
                        dominant_expiry_vol = ot.contract.lastTradeDateOrContractMonth

                    if ot.contract.right == 'C':
                        total_call_vol += vol
                        total_call_oi  += oi
                    else:
                        total_put_vol  += vol
                        total_put_oi   += oi

                # ── Call concentration tracking: EVERY call contract in the pulled grid ──
                if ot.contract.right == 'C':
                    line_key = (ot.contract.lastTradeDateOrContractMonth, ot.contract.strike)
                    call_vol_by_line[line_key] = call_vol_by_line.get(line_key, 0) + vol
                    total_call_vol_all += vol
                    logging.info(
                        f"Accumulated call volume for line {line_key}: "
                        f"{call_vol_by_line[line_key]} (total across grid: {total_call_vol_all})"
                    )

                # M&A Deal band (further OTM strikes) analysis 
                is_deal_strike = strike_key in deal_strike_set
                if is_deal_strike and ot.contract.right == 'C':
                    logging.info(
                        f"Deal-band call strike {strike_key} detected within "
                        f"premium band ${deal_low:.2f}-${deal_high:.2f}: "
                        f"OI={oi}, Vol={vol}"
                    )
                    deal_band_call_vol += vol
                    deal_band_call_oi  += oi
                    if vol > deal_band_max_vol:
                        deal_band_max_vol         = vol
                        deal_band_dominant_expiry = ot.contract.lastTradeDateOrContractMonth
                        deal_band_dominant_strike = ot.contract.strike

                # ── IV skew leg: gated to wing strikes only, so deal-band strikes ──
                # ── (which are much further OTM) don't distort the near-wing skew ──
                if ot.impliedVol is not None and ot.delta is not None and strike_key in wing_strike_set:
                    exp = ot.contract.lastTradeDateOrContractMonth
                    bucket = iv_by_expiry.setdefault(exp, {"call_ivs": [], "put_ivs": []})
                    d = ot.delta
                    if ot.contract.right == 'C' and self.WING_DELTA_LOW <= d <= self.WING_DELTA_HIGH:
                        logging.info(
                            f"OTM call strike {strike_key} with delta {d:.3f} "
                            f"falls within wing delta range "
                            f"[{self.WING_DELTA_LOW}, {self.WING_DELTA_HIGH}]; "
                            f"adding IV {ot.impliedVol:.3f} to expiry {exp} bucket."
                        )
                        bucket["call_ivs"].append(ot.impliedVol)
                    elif ot.contract.right == 'P' and -self.WING_DELTA_HIGH <= d <= -self.WING_DELTA_LOW:
                        logging.info(
                            f"OTM put strike {strike_key} with delta {d:.3f} "
                            f"falls within wing delta range "
                            f"[-{self.WING_DELTA_HIGH}, -{self.WING_DELTA_LOW}]; adding IV {ot.impliedVol:.3f} to expiry {exp} bucket."
                        )
                        bucket["put_ivs"].append(ot.impliedVol)

            # ── 8. OTM Call/Put IV skew per expiry — find the strongest call skew ──
            logging.info(f"Analyzing OTM call/put IV skew across {len(iv_by_expiry)} expiries for symbol {contract.symbol}...")
            best_expiry     = "None"
            best_skew_pct   = 0.0
            best_call_iv    = 0.0
            best_put_iv     = 0.0
            skew_samples    = []  # for an across-expiries average

            for exp, bucket in iv_by_expiry.items():
                logging.info(f"Expiry {exp}: {len(bucket['call_ivs'])} OTM call IVs, {len(bucket['put_ivs'])} OTM put IVs")

                if not bucket["call_ivs"] or not bucket["put_ivs"]:
                    logging.info(f"Skipping expiry {exp} due to insufficient IV data for skew calculation.")
                    continue
                call_iv = sum(bucket["call_ivs"]) / len(bucket["call_ivs"])
                put_iv  = sum(bucket["put_ivs"])  / len(bucket["put_ivs"])
                if put_iv <= 0:
                    continue
                skew_pct = (call_iv / put_iv - 1.0) * 100.0
                skew_samples.append(skew_pct)

                logging.info(
                    f"{contract.symbol} {exp}: OTM Call IV={call_iv:.3f} "
                    f"(n={len(bucket['call_ivs'])}) vs OTM Put IV={put_iv:.3f} "
                    f"(n={len(bucket['put_ivs'])})  skew={skew_pct:+.1f}%"
                )

                if skew_pct > best_skew_pct:
                    logging.info(
                        f"New best skew identified for {contract.symbol}: {skew_pct:+.1f}% at expiry {exp} (Call IV={call_iv:.3f}, Put IV={put_iv:.3f})"
                    )
                    best_skew_pct = skew_pct
                    best_expiry   = exp
                    best_call_iv  = call_iv
                    best_put_iv   = put_iv

            avg_skew_pct = sum(skew_samples) / len(skew_samples) if skew_samples else 0.0

            # ── 8b. Call volume concentration — is one line dominating the flow? ──
            concentration_pct    = 0.0
            concentration_expiry = "None"
            concentration_strike = 0.0

            if call_vol_by_line and total_call_vol_all > 0:
                (concentration_expiry, concentration_strike), max_line_vol = max(
                    call_vol_by_line.items(), key=lambda kv: kv[1]
                )
                concentration_pct = max_line_vol / total_call_vol_all * 100.0
                logging.info(
                    f"{contract.symbol}: peak call volume concentration "
                    f"{concentration_pct:.1f}% at {concentration_expiry} ${concentration_strike}"
                )

            # ── 9. Term structure anomaly — expiries priced off the local curve ────
            expiry_days = sorted(
                ((exp, (datetime.strptime(exp, '%Y%m%d') - now).days)
                 for exp in iv_by_expiry),
                key=lambda x: x[1]
            )

            expiry_rep_iv = {}
            for exp, bucket in iv_by_expiry.items():
                all_ivs = bucket["call_ivs"] + bucket["put_ivs"]
                if all_ivs:
                    expiry_rep_iv[exp] = sum(all_ivs) / len(all_ivs)

            worst_anomaly_expiry = "None"
            worst_anomaly_pct    = 0.0

            # Interior points only — first/last expiry in the sampled window has no
            # neighbor on one side to interpolate against, so we skip those rather
            # than guess with a one-sided extrapolation.
            for i in range(1, len(expiry_days) - 1):
                exp, _      = expiry_days[i]
                prev_exp, _ = expiry_days[i - 1]
                next_exp, _ = expiry_days[i + 1]
                logging.info(
                    f"Checking term structure anomaly for {contract.symbol} expiry {exp} "  
                    f"with neighbors {prev_exp} and {next_exp}."
                )

                if exp not in expiry_rep_iv or prev_exp not in expiry_rep_iv or next_exp not in expiry_rep_iv:
                    logging.info(
                        f"Skipping expiry {exp} for term structure anomaly check due to missing representative IVs."
                    )
                    continue

                expected_iv = (expiry_rep_iv[prev_exp] + expiry_rep_iv[next_exp]) / 2.0
                if expected_iv <= 0:
                    logging.info(
                        f"Skipping expiry {exp} for term structure anomaly check due to non-positive expected IV."
                    )
                    continue
                anomaly_pct = (expiry_rep_iv[exp] - expected_iv) / expected_iv * 100.0

                logging.info(
                    f"{contract.symbol} {exp}: rep IV={expiry_rep_iv[exp]:.3f} vs "
                    f"neighbor-expected={expected_iv:.3f}  anomaly={anomaly_pct:+.1f}%"
                )
                if abs(anomaly_pct) > abs(worst_anomaly_pct):
                    logging.info(
                        f"New worst term structure anomaly for {contract.symbol}: "
                        f"{anomaly_pct:+.1f}% at expiry {exp} (rep IV={expiry_rep_iv[exp]:.3f}, expected={expected_iv:.3f})"
                    )
                    worst_anomaly_pct    = anomaly_pct
                    worst_anomaly_expiry = exp

            term_structure_flag = int(worst_anomaly_pct >= self.TERM_STRUCTURE_ANOMALY_THRESHOLD_PCT)

            return {
                "atm_volume_skews":    round(float(total_call_vol) / float(total_put_vol or 1), 2),
                "atm_dominant_expiry_by_vol": dominant_expiry_vol,
                "atm_dominant_expiry_by_oi":  dominant_expiry_oi,
                "atm_oi_depth":          total_atm_oi,
                "atm_oi_skews":         round(float(total_call_oi) / float(total_put_oi or 1), 2),
                "otm_call_iv_max_skew_expiry": best_expiry,
                "otm_call_iv":                 round(best_call_iv, 4),
                "otm_put_iv":                  round(best_put_iv, 4),
                "call_put_iv_skew_pct":        round(best_skew_pct, 2),
                "call_put_iv_skew_pct_avg":    round(avg_skew_pct, 2),
                "call_skew_flag":              int(best_skew_pct >= 15.0),  # tune threshold
                "iv_skew_expiries_matched":    len(skew_samples),
                "call_vol_concentration_pct":    round(concentration_pct, 2),
                "concentration_expiry":          concentration_expiry,
                "concentration_strike":          concentration_strike,
                "term_structure_anomaly_expiry": worst_anomaly_expiry,
                "term_structure_anomaly_pct":    round(worst_anomaly_pct, 2),
                "term_structure_flag":           term_structure_flag,
                "deal_band_call_vol":          deal_band_call_vol,
                "deal_band_call_oi":           deal_band_call_oi,
                "deal_band_vol_oi_ratio":      round(deal_band_call_vol / deal_band_call_oi, 2) if deal_band_call_oi else 0.0,
                "deal_band_concentration_pct": round(deal_band_call_vol / total_call_vol_all * 100.0, 2) if total_call_vol_all else 0.0,
                "deal_band_dominant_expiry":   deal_band_dominant_expiry,
                "deal_band_dominant_strike":   deal_band_dominant_strike,
            }

        except Exception as err:
            logging.error(
                f"Option chain analysis failed for {contract.symbol}: {err}"
            )
            return _err


# ══════════════════════════════════════════════════════════════════════════════
# Pillar 2 — SEC Data  (unchanged — no IBKR dependency)
# ══════════════════════════════════════════════════════════════════════════════
class Pillar2SECData:
    """Handles time-decayed Form 4 extraction and XBRL capital-structure metrics via EDGAR."""
    
    @staticmethod
    def extract_sic_industry(symbol):
        """
        Pull SIC metadata using filing_parser.py pipeline.
        Returns:
            {
                "whole_sic_code": str|None,
                "sic_desc": str|None,
                "sic_major_group": str|None,
            }
        """
        try:
            logging.info(f"Extracting SIC industry metadata for {symbol}...")

            # Get most recent filing from the company
            # Exclude beneficial ownership filings, they are returned by get_filings() but are filed by the owner, not the company itself.  We want the company's own filings for SIC.
            excluded_forms = { '13F-HR', '13F-NT', 'SC 13D', 'SC 13G', 'SC 13G/A', 'SC 13D/A' } # Exclude beneficial ownership filings, they are returned by 
            company = Company(symbol)
            all_filings = company.get_filings()

            filtered_filings = [f for f in all_filings if f.form not in excluded_forms]
    
            most_recent_filing = filtered_filings[0] if filtered_filings else None
            if not most_recent_filing:
                raise ValueError("No valid filings found for the given symbol.")

            filing_fulltext = most_recent_filing.full_text_submission()
            filing_type = most_recent_filing.form
            logging.info(f"Most recent filing for {symbol}: {filing_type}, going to scrape for SIC...")

            # SEC header (which contains SIC) parsing is triggered via init of MasterParserClass with filing text and type
            parser = MasterParserClass(filing_fulltext, filing_type)

            sic_code = parser.filing_info.at[0, "whole_sic_code"]
            sic_desc = parser.filing_info.at[0, "sic_desc"]

            # preserve leading zero if SIC arrives numeric
            sic_code = str(sic_code).zfill(4) if sic_code not in (None, "", "nan") else None
            major_group = sic_code[:2] if sic_code else None
            logging.info(f"Determined SIC for {symbol}: {sic_code} ({sic_desc}), major group: {major_group}")

            return {
                "whole_sic_code": sic_code,
                "sic_desc": sic_desc,
                "sic_major_group": major_group,
            }
        except Exception as e:
            logging.info(f"SIC extraction failed for {symbol}: {e}")
            return {
                "whole_sic_code": None,
                "sic_desc": None,
                "sic_major_group": None,
            }
    
    @staticmethod
    def compute_insider_conviction(symbol, lookback_days=90):
        """Calculates time-decayed net institutional buying IN DOLLARS (Notional USD)."""
        logging.info(
            f"Computing insider conviction for {symbol} with a {lookback_days}-day "
            f"lookback window..."
        )
        try:
            company = Company(symbol)
            filings = company.get_filings(form="4").head(50)
            if not filings:
                logging.info(f"No recent Form 4 filings found for {symbol}.")
                return {"insider_conviction_score": 0.0, "cfo_involved": 0}

            net_notional_usd = 0.0
            unique_insiders  = set()
            cfo_activity_flag = 0

            for f in filings:
                logging.info(
                    f"Processing Form 4 filing {f.accession_no} for {symbol} "
                    f"dated {f.filing_date}..."
                )

                if isinstance(f.filing_date, str):
                    parsed_date = datetime.strptime(f.filing_date, "%Y-%m-%d").date()
                else:
                    parsed_date = f.filing_date

                days_old = (datetime.now().date() - parsed_date).days
                if days_old > lookback_days:
                    logging.info(
                        f"Skipping filing {f.accession_no} for {symbol}: "
                        f"{days_old} days old, beyond lookback window."
                    )
                    continue

                weight      = max(0.1, (lookback_days - days_old) / float(lookback_days))
                xml_content = f.xml()
                if not xml_content:
                    logging.info(
                        f"Skipping filing {f.accession_no} for {symbol}: "
                        f"No XML content available."
                    )
                    continue

                title_match   = re.search(
                    r'<officerTitle>(.*?)</officerTitle>', xml_content, re.IGNORECASE
                )
                officer_title = title_match.group(1).upper() if title_match else "UNKNOWN"

                is_cfo = 1 if any(
                    keyword in officer_title
                    for keyword in ["CFO", "FINANCIAL", "ACCOUNTING", "TREASURER"]
                ) else 0
                title_multiplier = 1.5 if is_cfo else 1.0

                if is_cfo:
                    cfo_activity_flag = 1

                transactions   = re.findall(
                    r'<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>',
                    xml_content, re.DOTALL
                )
                filing_net_usd = 0.0
                for trans in transactions:
                    code_match = re.search(
                        r'<transactionCode>(P|S)</transactionCode>', trans
                    )
                    if not code_match:
                        logging.info(
                            f"Transaction in filing {f.accession_no} for {symbol} "
                            f"is not a purchase or sale, skipping.."
                        )
                        continue
                    code = code_match.group(1)

                    shares_match = re.search(
                        r'<transactionShares>\s*<value>([\d\.]+)</value>', trans
                    )
                    price_match  = re.search(
                        r'<transactionPricePerShare>\s*<value>([\d\.]+)</value>', trans
                    )

                    if shares_match and price_match:
                        shares       = float(shares_match.group(1))
                        price        = float(price_match.group(1))
                        notional_val = (shares * price) * title_multiplier
                        unique_insiders.add(f.accession_no)

                        if code == 'P':
                            filing_net_usd += notional_val
                        elif code == 'S':
                            filing_net_usd -= notional_val

                net_notional_usd += (filing_net_usd * weight)

            cluster_multiplier = 1.5 if len(unique_insiders) >= 3 else 1.0
            final_score = round((net_notional_usd * cluster_multiplier) / 1_000_000, 3)

            return {
                "insider_conviction_score": final_score,
                "cfo_involved": cfo_activity_flag,
            }
        except Exception as e:
            logging.info(f"Insider context tracking exception on {symbol}: {e}")
            return {"insider_conviction_score": 0.0, "cfo_involved": 0}

    @staticmethod
    def extract_debt_reduction_metrics(symbol):
        """Evaluates liability reduction vectors through us-gaap sequential XBRL elements."""
        logging.info(
            f"Extracting debt reduction metrics for {symbol} from XBRL filings..."
        )
        try:
            company  = Company(symbol)
            facts    = company.get_facts()
            target_concepts = ["LongTermDebt", "DebtInstrumentCarryingAmount"]

            for concept in target_concepts:
                try:
                    df = facts.to_pandas(f"us-gaap:{concept}")
                    if df.empty:
                        continue
                    df = df.sort_values('end', ascending=False)
                    current_val = df.iloc[0]['val']
                    prev_val    = df.iloc[1]['val']
                    if prev_val == 0:
                        continue
                    reduction_pct = ((prev_val - current_val) / prev_val) * 100.0
                    return round(reduction_pct, 2)
                except Exception:
                    continue
            return 0.0
        except Exception:
            return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# Pillar 3 — Custom Parser  (unchanged — no IBKR dependency)
# ══════════════════════════════════════════════════════════════════════════════
class Pillar3CustomParser:
    """Orchestrates structural text decomposition and zero-shot NLP classification."""

    def __init__(self, filing_fetcher_func):
        self.fetch_filings = filing_fetcher_func

        logging.info("Loading Zero-Shot NLP Engine into memory...")
        from transformers import pipeline as hf_pipeline
        self.classifier = hf_pipeline(
            "zero-shot-classification", model="facebook/bart-large-mnli"
        )

        self.catalyst_dictionaries = {
            "M&A / Strategic Review":  [
                r"strategic alternatives", r"evaluate alternatives",
                r"retained.*?advisor", r"potential sale",
            ],
            "Activism / Spinoff":      [
                r"spin-off", r"spinoff", r"carve-out",
                r"value creation", r"board seat", r"dissident",
            ],
            "Restructuring":           [
                r"turnaround plan", r"cost reduction",
                r"headcount reduction", r"impairment",
            ],
            "Management Alignment":    [
                r"free cash flow", r"return on invested capital",
                r"roic", r"performance target",
                r"debt reduction", r"incentive plan",
            ],
        }
        self.target_sections = ["item 1.01", "item 7.01", "item 8.01", "item 4"]

    def process_corporate_catalysts(self, symbol):
        logging.info(f"[{symbol}] Processing corporate text structures...")
        features = {
            "catalyst_flag": 0, "catalyst_type": "None",
            "catalyst_confidence": 0.0, "has_activist_pressure": 0,
        }

        try:
            recent_filings = self.fetch_filings(
                symbol,
                forms=['8-K', '8-K/A', 'SC 13D', 'SC 13D/A', 'DEF 14A'],
                days_lookback=90,
            )
            if not recent_filings:
                return features

            highest_confidence = 0.0
            best_catalyst      = "None"

            for filing in recent_filings:
                logging.info(
                    f"Parsing {filing['type']} filed on {filing['date']} "
                    f"for potential catalyst sections..."
                )
                parser    = MasterParserClass(filing['fulltext'], filing['type'])
                dfs       = parser.output_dfs()
                target_df = pd.DataFrame()

                if filing['type'] in ['8-K', '8-K/A', 'SC 13D', 'SC 13D/A']:
                    named_sections_df = dfs.get(settings.NAMED_SECTIONS_TABLE, pd.DataFrame())
                    if not named_sections_df.empty:
                        target_df = named_sections_df[
                            named_sections_df['section_name']
                            .str.lower()
                            .isin(self.target_sections)
                        ]
                elif filing['type'] == 'DEF 14A':
                    toc_df = dfs.get(settings.TOC_SECTIONS_TABLE, pd.DataFrame())
                    if not toc_df.empty:
                        target_df = toc_df[
                            toc_df['section_name'].str.contains(
                                r'compensation discussion|cd&a', case=False, na=False
                            )
                        ]

                if target_df.empty:
                    logging.info(
                        f"No relevant sections found in {filing['type']} for {symbol}. "
                        f"Skipping NLP classification."
                    )
                    continue

                for _, row in target_df.iterrows():
                    logging.info(
                        f"Evaluating section '{row.get('section_name', 'Unknown')}' "
                        f"for catalyst indicators..."
                    )
                    text_chunk = row.get('text', '')
                    if not text_chunk or len(text_chunk) < 50:
                        logging.info("Section text too short. Skipping.")
                        continue

                    paragraphs = [
                        p.strip() for p in text_chunk.split('\n\n')
                        if len(p.strip()) > 50
                    ]
                    for para in paragraphs:
                        keyword_hit = False
                        for cat_name, patterns in self.catalyst_dictionaries.items():
                            for pattern in patterns:
                                if re.search(pattern, para, re.IGNORECASE):
                                    keyword_hit = True
                                    logging.info(
                                        f"Keyword match for '{cat_name}' in section "
                                        f"'{row.get('section_name', 'Unknown')}'. "
                                        f"Proceeding to NLP classification."
                                    )
                                    break
                            if keyword_hit:
                                break

                        if not keyword_hit:
                            continue

                        candidate_labels = (
                            list(self.catalyst_dictionaries.keys())
                            + ["Routine Business Operations"]
                        )
                        nlp_res    = self.classifier(para[:1024], candidate_labels=candidate_labels)
                        top_label  = nlp_res['labels'][0]
                        confidence = nlp_res['scores'][0]

                        if top_label == "Routine Business Operations":
                            continue

                        logging.info(
                            f"NLP: '{top_label}' confidence={confidence:.3f} "
                            f"in section '{row.get('section_name', 'Unknown')}'"
                        )
                        if confidence > highest_confidence:
                            highest_confidence        = confidence
                            best_catalyst             = top_label
                            features["catalyst_flag"] = 1
                            if top_label == "Activism / Spinoff":
                                features["has_activist_pressure"] = 1

            features["catalyst_type"]       = best_catalyst
            features["catalyst_confidence"] = round(highest_confidence, 3)
            return features

        except Exception as e:
            logging.error(f"Text architecture evaluation faulted for {symbol}: {e}")
            return features


# ══════════════════════════════════════════════════════════════════════════════
# Convergence Pipeline  (now fully synchronous — no asyncio)
# ══════════════════════════════════════════════════════════════════════════════
class ConvergencePipeline:
    """Orchestrates and merges feature streams into an analytical Daily Option Signal sheet."""

    def __init__(self, custom_parser_engine):
        self.ib_broker     = Pillar1MarketData()
        self.sec_broker    = Pillar2SECData()
        self.text_broker   = custom_parser_engine
        self.feature_store = []

    def _passes_advanced_filter(self, feature_row) -> bool:
        symbol          = feature_row.get("symbol", "N/A")
        dist_200dma     = feature_row.get("dist_to_200dma", 0)
        ic_score        = feature_row.get("insider_conviction_score", 0)
        oi_skew          = feature_row.get("atm_oi_skews", 1.0)
        regime          = feature_row.get("market_regime", "")
        contraction_mean = float(feature_row.get("contraction_mean", float('nan')))

        if ic_score < 0:
            logging.info(f"Filtering out {symbol}: negative insider conviction ({ic_score}).")
            return False

        '''
        if oi_skew <= 1.0:
            logging.info(f"Filtering out {symbol}: leap_oi_skews ({oi_skew}) out of bounds.")
            return False

        if regime != "Accumulation Base":
            logging.info(f"Filtering out {symbol}: regime '{regime}' ≠ 'Accumulation Base'.")
            return False

        if dist_200dma > 0.08 or dist_200dma < -0.12:
            logging.info(
                f"Filtering out {symbol}: dist_to_200dma ({dist_200dma:.3f}) out of bounds."
            )
            return False

        if math.isnan(contraction_mean):
            logging.info(f"Filtering out {symbol}: contraction_mean is NaN.")
            return False

        if contraction_mean >= 0.45:
            logging.info(
                f"Filtering out {symbol}: contraction_mean too high "
                f"({contraction_mean:.2f} >= 0.45)."
            )
            return False
        '''

        return True

    # ── Live daily build (was async) ──────────────────────────────────────────

    def execute_daily_build(self):
        """
        Main pipeline entry point.
        Previously: async def execute_daily_build() + asyncio.run()
        Now: plain synchronous call — no event loop needed.
        """
        self.ib_broker.connect()   # was: await self.ib_broker.connect_async()

        try:
            raw_contracts = self.ib_broker.scan_accumulation_candidates(limit=75)

            active_tickers = self.ib_broker.fetch_ticker_snapshots(raw_contracts)

            logging.info(
                f"Cross-referencing telemetry points across "
                f"{len(active_tickers)} qualified scanner assets..."
            )

            for t in active_tickers:
                sym = t.contract.symbol
                logging.info(f"Processing convergence features for {sym}...")

                regime_metrics = self.ib_broker.determine_market_regime(t.contract)

                last_price = float(t.last) if t.last and t.last > 0 else 0.0

                positioning_footprint = self.ib_broker.analyze_positioning_footprint(
                    t.contract, last_price
                )

                stock_iv          = self.ib_broker.get_iv_rank_percentile(t.contract)
                insider_score     = self.sec_broker.compute_insider_conviction(sym)
                debt_delta        = self.sec_broker.extract_debt_reduction_metrics(sym)
                sic_industry      = self.sec_broker.extract_sic_industry(sym)
                catalyst_features = self.text_broker.process_corporate_catalysts(sym)

                feature_row = {
                    "timestamp":              datetime.now().strftime("%Y-%m-%d"),
                    "symbol":                 sym,
                    "sic_code":               sic_industry.get("whole_sic_code"),
                    "industry":               sic_industry.get("sic_desc"),
                    "sic_major_group":        sic_industry.get("sic_major_group"),
                    "last_price":             last_price,
                    **stock_iv,
                    "put_call_ratio":         float(t.pcRatio) if t.pcRatio else 0.00,
                    "call_volume":            int(t.callVolume)      if t.callVolume      else 0,
                    "put_volume":             int(t.putVolume)       if t.putVolume       else 0,
                    "opt_volume":             int((t.callVolume or 0) + (t.putVolume or 0)),
                    "av_option_volume":       int(t.avOptionVolume)  if t.avOptionVolume  else 0,
                    "opt_vol_expansion_ratio": round(
                        float((t.callVolume or 0) + (t.putVolume or 0))
                        / float(t.avOptionVolume or 1),
                        2,
                    ),
                    "market_regime":          str(regime_metrics["regime"]),
                    "is_coiling":             regime_metrics["coiling"],
                    "dist_to_200dma":         float(regime_metrics["dist_to_200dma"]),
                    "contraction_mean":       float(regime_metrics.get("contraction_mean", 0.0)),
                    "contraction_median":     float(regime_metrics.get("contraction_median", 0.0)),
                    "triangle_flag":          regime_metrics.get("triangle_flag", 0),
                    "high_slope":             float(regime_metrics.get("high_slope", 0.0)),
                    "low_slope":              float(regime_metrics.get("low_slope", 0.0)),
                    **positioning_footprint,
                    **insider_score,
                    "debt_reduction_pct":     float(debt_delta),
                    **catalyst_features,
                }

                if not self._passes_advanced_filter(feature_row):
                    continue
                self.feature_store.append(feature_row)

        finally:
            self.ib_broker.disconnect()

        self.export_to_feature_store()

    # ── Weekend / test build (was async) ─────────────────────────────────────

    def test_execute_daily_build(self):
        """
        Weekend execution simulation with explicit fallback data arrays.
        Previously: async def test_execute_daily_build()
        Now: plain synchronous.
        """
        self.ib_broker.connect()

        try:
            logging.info("WEEKEND TEST MODE: Bypassing live TWS scanner...")
            test_symbols = ["AAPL"]

            active_tickers = []
            for sym in test_symbols:
                # Build a Stock contract and qualify it
                # (replacement for: Stock(sym,'SMART','USD') + qualifyContractsAsync)
                contract          = Contract()
                contract.symbol   = sym
                contract.secType  = 'STK'
                contract.exchange = 'SMART'
                contract.currency = 'USD'

                qualified = self.ib_broker._qualify_one(contract)
                if not qualified:
                    logging.warning(f"Could not qualify {sym}, skipping.")
                    continue

                # Brief market data stream to get a Ticker object
                # (replacement for: reqTickersAsync)
                ticker = self.ib_broker.get_stk_ticker(qualified, wait=0.5)
                active_tickers.append(ticker)

            for t in active_tickers:
                sym = t.contract.symbol
                logging.info(f"Processing convergence features for {sym}...")

                regime_metrics = self.ib_broker.determine_market_regime(t.contract)

                # Fetch last 5 days of daily bars as a fallback price reference
                # (replacement for: reqHistoricalDataAsync with durationStr='5 D')
                historical_bars = self.ib_broker._get_historical_bars(
                    t.contract, duration='5 D', bar_size='1 day'
                )
                fallback_price = float(historical_bars[-1].close) if historical_bars else 100.0

                positioning_footprint = self.ib_broker.analyze_positioning_footprint(
                    t.contract, fallback_price
                )
                stock_iv          = self.ib_broker.get_iv_rank_percentile(t.contract)
                insider_score     = self.sec_broker.compute_insider_conviction(sym)
                debt_delta        = self.sec_broker.extract_debt_reduction_metrics(sym)
                sic_industry      = self.sec_broker.extract_sic_industry(sym)
                catalyst_features = self.text_broker.process_corporate_catalysts(sym)

                feature_row = {
                    "timestamp":              datetime.now().strftime("%Y-%m-%d"),
                    "symbol":                 sym,
                    "sic_code":               sic_industry.get("whole_sic_code"),
                    "industry":               sic_industry.get("sic_desc"),
                    "sic_major_group":        sic_industry.get("sic_major_group"),
                    "last_price":             fallback_price,
                    **stock_iv,
                    "opt_vol_expansion_ratio": 1.0,
                    "market_regime":          str(regime_metrics["regime"]),
                    "is_coiling":             regime_metrics["coiling"],
                    "dist_to_200dma":         float(regime_metrics["dist_to_200dma"]),
                    "contraction_mean":       float(regime_metrics.get("contraction_mean", 0.0)),
                    "contraction_median":     float(regime_metrics.get("contraction_median", 0.0)),
                    "triangle_flag":          regime_metrics.get("triangle_flag", 0),
                    "high_slope":             float(regime_metrics.get("high_slope", 0.0)),
                    "low_slope":              float(regime_metrics.get("low_slope", 0.0)),
                    **positioning_footprint,
                    **insider_score,
                    "debt_reduction_pct":     float(debt_delta),
                    **catalyst_features,
                }

                if not self._passes_advanced_filter(feature_row):
                    continue
                self.feature_store.append(feature_row)

        finally:
            self.ib_broker.disconnect()

        self.export_to_feature_store()

    # ── Export ────────────────────────────────────────────────────────────────

    def export_to_feature_store(self):
        df = pd.DataFrame(self.feature_store)
        if df.empty:
            logging.warning("Scanner execution sweep concluded with zero matching signals.")
            return

        df = df.sort_values(
            by=["is_coiling", "atm_volume_skews", "catalyst_flag", "insider_conviction_score"],
            ascending=[False, False, False, False],
        )

        output_dir = "feature_store_archives"
        os.makedirs(output_dir, exist_ok=True)

        file_path = os.path.join(
            output_dir,
            f"convergence_signals_{datetime.now().strftime('%Y%m%d')}.csv",
        )
        df.to_csv(file_path, index=False)
        logging.info(
            f"Signal matrix generation completed. Output written to: {file_path}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Internal SEC filing fetcher  (unchanged — no IBKR dependency)
# ══════════════════════════════════════════════════════════════════════════════
def internal_sec_filing_fetcher(symbol, forms, days_lookback):
    """
    Localized delivery module acting as the proxy fetch pipeline for python-edgar
    data wrappers. Converts streaming document payloads into standardised dicts.
    """
    logging.info(
        f"Fetching SEC filings for {symbol}, forms={forms}, "
        f"lookback={days_lookback} days..."
    )
    file_payloads  = []
    try:
        comp           = Company(symbol)
        lookback_cutoff = datetime.now() - timedelta(days=days_lookback)

        for form_type in forms:
            filing_list = comp.get_filings(form=form_type)
            if not filing_list:
                continue

            for filing_entry in filing_list.head(10):
                if isinstance(filing_entry.filing_date, str):
                    f_date = datetime.strptime(filing_entry.filing_date, "%Y-%m-%d")
                else:
                    f_date = datetime.combine(
                        filing_entry.filing_date, datetime.min.time()
                    )

                if f_date < lookback_cutoff:
                    continue

                file_payloads.append({
                    "type":     str(form_type),
                    "fulltext": filing_entry.full_text_submission(),
                    "date":     str(filing_entry.filing_date),
                })

    except Exception as err:
        logging.warning(
            f"Historical filing pull cancelled for {symbol}: {err}"
        )

    return file_payloads


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser_module = Pillar3CustomParser(filing_fetcher_func=internal_sec_filing_fetcher)
    pipeline      = ConvergencePipeline(custom_parser_engine=parser_module)

    # Previously: asyncio.run(pipeline.execute_daily_build())
    # Now just a plain synchronous call — no event loop required.
    pipeline.execute_daily_build()