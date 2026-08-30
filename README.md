# Daily Option Activity Scanner

A daily market scanner and signal generator that looks for unusually bullish option activity by combining an IBKR-based low put/call (P/C) ratio scanner with deeper option-chain analysis, market-regime / technical chart metrics, insider activity extracted from SEC filings, and corporate-catalyst detection via filing text parsing and zero-shot NLP.

The repository contains:
- A synchronous IBKR scanner and analysis pipeline: `ibkr_eod_option_scan.py`
- SEC filing parsing / text extraction: `filing_parser.py`
- A lightweight Flask dashboard for exploring CSV outputs: `flask_dashboard/`
- Archived daily outputs: `feature_store_archives/` (CSV files named `convergence_signals_YYYYMMDD.csv`)

---

## Overview

How it works (high level)

- Pillar 1: Connects to Interactive Brokers (TWS / Gateway, using `ibapi`) to run a TWS scanner (`LOW_OPT_VOLUME_PUT_CALL_RATIO`), fetch STK-level option volumes and option-contract OI/IV/delta, and compute market-regime metrics (SMA200 distance, Bollinger coiling, converging triangle).
- Pillar 2: Pulls EDGAR filings (Form 4, XBRL facts, SIC) to compute time-decayed insider conviction and balance-sheet / debt metrics.
- Pillar 3: Parses filing HTML/PDF content and runs zero-shot NLP to flag corporate catalysts (M&A, activism, restructuring, etc.).
- `ConvergencePipeline` merges these feature streams and exports a daily signal CSV to `feature_store_archives/`. The Flask dashboard reads these CSVs for exploration.

---

## Stack

- Language: Python 3.10+
- Runtime / frameworks: Flask (dashboard)
- Notable libraries:
  - `ibapi` — official Interactive Brokers API
  - `pandas`, `numpy` — data processing
  - `edgar` (python-edgar wrapper) — SEC filings
  - `beautifulsoup4` (bs4), `pymupdf` — filing extraction
  - `transformers` (HuggingFace) + a zero-shot model (`facebook/bart-large-mnli`) — catalyst classification

---

## Repository layout

```
.
├─ ibkr_eod_option_scan.py        # Main scanner and pipeline (Pillar1, 2, 3 + ConvergencePipeline)
├─ filing_parser.py               # Filing parsing, TOC crawling, PDF parsing
├─ settings.py                    # Settings (SEC headers, table names, model defaults)
├─ flask_dashboard/
│   ├─ app.py                     # Flask app - reads CSV outputs for UI
│   └─ templates/
│       ├─ base.html
│       ├─ index.html
│       ├─ day.html
│       ├─ rolling.html
│       └─ ticker.html
└─ feature_store_archives/        # Output CSVs (convergence_signals_YYYYMMDD.csv)
```

How it fits together:
- `ibkr_eod_option_scan.py` orchestrates discovery (TWS scanner) → STK snapshots → option-chain qualification → chunked option market-data streaming → market-regime and SEC/text enrichment → export CSV.
- The Flask dashboard (`flask_dashboard/app.py`) displays CSV contents, supports day/rolling/ticker views, and links to SEC browsing for a ticker.

---

## Requirements & installation

1. Clone the repo:
```bash
git clone https://github.com/cchummer/options_activity_scanner.git
cd options_activity_scanner
```

2. Create a virtual environment (recommended) and install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install ibapi pandas numpy flask beautifulsoup4 lxml pymupdf edgar transformers torch requests
```

Notes:
- `ibapi` is required for TWS/Gateway.
- The zero-shot classifier requires a model backend (PyTorch or TensorFlow). Installing `torch` is recommended if you plan to run NLP locally.
- If you prefer reproducible installs, generate a `requirements.txt` from the installed packages.

---

## Configuration

- IBKR / TWS:
  - Default ports referenced in the code header:
    - TWS Live: 7496
    - TWS Paper: 7497
    - Gateway Live: 4001
    - Gateway Paper: 4002
  - Configure host/port/client_id in `Pillar1MarketData` constructor or update the script before running.

- SEC / EDGAR:
  - Per SEC policy you must set an identity string (contact email) for EDGAR requests.
  - In `ibkr_eod_option_scan.py` there is:
    ```py
    set_identity("")  # fill with "Your Name <email@example.com>"
    ```
    Replace the empty string with a valid contact, or ensure `settings.SEC_REQ_HEADERS["User-Agent"]` contains your contact email.

- Flask dashboard:
  - By default the dashboard reads CSVs from `feature_store_archives/`.
  - Override with:
    ```bash
    export SCANNER_DATA_DIR=/path/to/your/feature_store_archives
    ```

- Rate limits & entitlements:
  - The code enforces local pacing and has a configurable `MAX_ACTIVE_MKT_LINES_BUDGET`. Ensure your IBKR market-data entitlements include OPRA / option data and underlying option subscriptions.

---

## Running

1. Run the live end-of-day scanner:
```bash
python ibkr_eod_option_scan.py
```
This connects to TWS/Gateway, runs the scanner, pulls market and filings data, runs the analysis pipeline, and writes `feature_store_archives/convergence_signals_YYYYMMDD.csv`.

2. Run the Flask dashboard locally:
```bash
export FLASK_APP=flask_dashboard/app.py
export SCANNER_DATA_DIR=/path/to/feature_store_archives   # optional
flask run --port 5000
# or
python flask_dashboard/app.py
```
Open http://localhost:5000/

3. Weekend / test mode:
- The pipeline includes `test_execute_daily_build()` which runs with fallback data and a small test symbol list. Run interactively:
```bash
python - <<'PY'
from ibkr_eod_option_scan import Pillar3CustomParser, ConvergencePipeline, internal_sec_filing_fetcher
parser_module = Pillar3CustomParser(filing_fetcher_func=internal_sec_filing_fetcher)
pipeline = ConvergencePipeline(custom_parser_engine=parser_module)
pipeline.test_execute_daily_build()
PY
```

---

## Output

- CSV files: `feature_store_archives/convergence_signals_YYYYMMDD.csv`
- Typical columns: `timestamp`, `symbol`, `last_price`, `put_call_ratio`, `call_volume`, `put_volume`, `opt_volume`, `av_option_volume`, IV rank/percentile fields, `market_regime`, `is_coiling`, `dist_to_200dma`, contraction metrics, ATM/OTM option-derived features, `insider_conviction_score`, `debt_reduction_pct`, `catalyst_flag`, `catalyst_type`, `catalyst_confidence`, and other aggregated metrics.

These CSVs drive the Flask UI and are suitable for later ML/backtesting experiments.

---

## Troubleshooting & tips

- TWS connection:
  - Ensure "Enable ActiveX and Socket Clients" is enabled in TWS API settings.
  - Confirm host/port and `client_id` match your TWS/Gateway instance.
  - Check logs — the code sets `ibapi` logging to WARNING and pipeline logs to INFO.

- Missing IV / option ticks:
  - Verify IBKR option market-data subscriptions (OPRA and options for underlying tickers). The code logs warnings when IV or option ticks are missing.

- Pacing / throttling:
  - The script uses rate limiters and exponential backoff. If you hit pacing errors, reduce concurrency (lower `MAX_ACTIVE_MKT_LINES_BUDGET`) or increase pacing delays.

- SEC EDGAR:
  - Set `set_identity(...)` or include your contact email in `settings.SEC_REQ_HEADERS["User-Agent"]` to comply with SEC rules.

- NLP model resource usage:
  - The zero-shot classifier (`facebook/bart-large-mnli`) is large. If local resources are constrained, use a smaller model or run NLP inference remotely.

---

## Future work / roadmap ideas

- Integrate a news / web-scrape scanner for real-time catalyst signals.
- Implement day-over-day option-contract lineage / contract tracking.
- Add ML backtesting and feature-selection experiments (Random Forests, ensembles).
- Add unit tests for `filing_parser` and pipeline components.
- Add `requirements.txt` / `pyproject.toml`, CI checks, and a CONTRIBUTING guide.

---

## Security & license

- No LICENSE file present. Add a license (MIT / Apache-2.0 / etc.) if you intend to share or accept contributions.
- Do not commit API keys, credentials, or personal SEC identity strings to the repository. Use environment variables or local configuration files excluded from version control.

---

If you’d like, I can:
- produce a concise `requirements.txt` matching imports used in the code,
- create a small `CONFIG.md` that lists the minimal edits required to run the scanner (SEC identity, IBKR host/port, `SCANNER_DATA_DIR`), or
- commit this README as `README.md` in the repository for you. Which would you prefer?
