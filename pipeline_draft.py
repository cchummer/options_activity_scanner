import asyncio
import sys
from datetime import datetime, timedelta
import logging
import os
import re
import math
import pandas as pd
import numpy as np

# External API dependencies
from ib_async import *
from edgar import Company, set_identity

# Import your proprietary parser library and settings
from filing_parser import MasterParserClass
import settings as settings

# Configure professional logging matrix
logging.basicConfig(
    level=logging.INFO, 
    stream=sys.stdout,
    format='%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) %(message)s'
)

# Mandatory SEC EDGAR identification identification string
set_identity("SpecialSituationsQuant Engine securedhummer@gmail.com")

class Pillar1MarketData:
    """Handles real-time asynchronous communication with the IBKR / TWS terminal socket."""
    def __init__(self, host='127.0.0.1', port=7496, client_id=1):
        self.ib = IB()
        self.host = host
        self.port = port
        self.client_id = client_id

    async def connect_async(self):
        logging.info(f"Establishing async socket gateway to TWS at {self.host}:{self.port}...")
        await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)
        self.ib.reqMarketDataType(2)  # Request frozen data to avoid real-time streaming issues during off-hours

    async def scan_accumulation_candidates(self, limit=60):
        logging.info("Executing TWS Scanner: Low P/C Volume Ratio...")
        sub = ScannerSubscription(
            instrument='STK',
            locationCode='STK.US.MAJOR',
            scanCode='LOW_OPT_VOLUME_PUT_CALL_RATIO'
        )
        scan_results = await self.ib.reqScannerDataAsync(sub)
        
        raw_contracts = [item.contractDetails.contract for item in scan_results]
        await self.ib.qualifyContractsAsync(*raw_contracts)
        
        valid_exchanges = {'NYSE', 'NASDAQ', 'AMEX', 'ARCA', 'BATS'}
        
        candidate_data = {}
        valid_contracts = []
        
        for item, contract in zip(scan_results, raw_contracts):
            if contract.primaryExchange not in valid_exchanges:
                logging.debug(f"Filtered out {contract.symbol} due to exchange '{contract.primaryExchange}'.")
                continue
                
            valid_contracts.append(contract)
            candidate_data[contract.conId] = float(item.projection) if item.projection else 0.0
            logging.info(f"Scanner Candidate: {contract.symbol} on {contract.primaryExchange} with P/C Ratio {candidate_data[contract.conId]}")
            
            if len(valid_contracts) >= limit:
                break
                
        return valid_contracts, candidate_data

    async def fetch_ticker_snapshots(self, contracts):
        """Evaluates market snapshot buffers for unusual volume."""
        logging.info(f"Requesting market snapshots for {len(contracts)} assets...")
        
        # 1. Request 
        tickers = []
        for contract in contracts:
            ticker = self.ib.reqMktData(contract, genericTickList='100,105', snapshot=False)
            tickers.append(ticker)
        
        # 2. Wait for delivery
        # A short wait ensures the TWS gateway 
        # has time to propagate the cache to your client
        await asyncio.sleep(2.0) 
        
        valid_candidates = []
        for t in tickers:
            # Sanitization for volume fields
            call_vol = getattr(t, 'callVolume', 0) or 0
            put_vol = getattr(t, 'putVolume', 0) or 0
            opt_vol = call_vol + put_vol
            
            # Sanitization for average volume
            avg_opt_vol = getattr(t, 'avOptionVolume', 1) or 1
            
            if opt_vol > avg_opt_vol and opt_vol >= 1000:
                logging.info(f"{t.contract.symbol} passed: Option Vol {opt_vol} (> Avg {avg_opt_vol})")
                valid_candidates.append(t)
            
            self.ib.cancelMktData(t.contract)  # Clean up the snapshot subscription immediately
                
        return valid_candidates

    def detect_volatility_contraction(self, weekly_df, window=20, lookback=26, contraction_threshold=0.45):
        """
        Detects multi-week volatility contraction ("coiling base") periods using BB width.
        Compares average BB width in the recent lookback window to both the mean and median BB width
        in all prior data. Coiling is flagged if contraction (recent/prior_mean) < contraction_threshold.

        Returns:
            coiling_flag (bool),
            contraction_mean (float),
            contraction_median (float)
        """
        logging.info("Calculating volatility contraction metrics using Bollinger Band width on weekly data...")
        
        closes = weekly_df['close']
        ma = closes.rolling(window).mean()
        std = closes.rolling(window).std()
        bb_width = (std * 4) / ma

        # Recent window (compression lookback)
        recent_width = bb_width.iloc[-lookback:].mean()

        # Prior periods
        if len(bb_width) > lookback:
            prior_series = bb_width.iloc[:-lookback]
            prior_mean = prior_series.mean()
            prior_median = prior_series.median()
        else:
            prior_mean = bb_width.mean()
            prior_median = bb_width.median()

        contraction_mean = recent_width / prior_mean if prior_mean else np.nan
        contraction_median = recent_width / prior_median if prior_median else np.nan

        coiling_flag = contraction_mean < contraction_threshold

        return coiling_flag, contraction_mean, contraction_median
    
    def detect_converging_triangle(self, weekly_df, lookback=26, slope_threshold=0.0):
        """
        Detects converging triangle patterns by fitting lines to recent highs and lows.
        Returns triangle_flag, (high_slope, low_slope).
        """
        logging.info("Calculating converging triangle metrics...")
        
        highs = weekly_df['high'].iloc[-lookback:]
        lows = weekly_df['low'].iloc[-lookback:]
        x = np.arange(lookback)
        # Linear fit: recent highs and lows
        high_slope = np.polyfit(x, highs.values, 1)[0]
        low_slope = np.polyfit(x, lows.values, 1)[0]
        # Triangle: highs trending down, lows trending up
        triangle_flag = (high_slope < slope_threshold) and (low_slope > -slope_threshold)
        return triangle_flag, (high_slope, low_slope)
    
    async def determine_market_regime(self, contract):
        """Computes lookback standard deviations to flag structural trend and nested volatility compression. 
        TODO: BETTER LIMIT MITIGATION FOR HISTORICAL DATA REQUESTS."""
        try:
            logging.info(f"Calculating market regime metrics for {contract.symbol}...")
            bars = await self.ib.reqHistoricalDataAsync(
                contract, endDateTime='', durationStr='5 Y',
                barSizeSetting='1 day', whatToShow='TRADES', useRTH=True
            )
            if len(bars) < 200:
                return {"regime": "Error", "coiling": False, "contraction_mean": 0.0, "contraction_median": 0.0, "triangle_flag": 0, "high_slope": 0.0, "low_slope": 0.0, "dist_to_200dma": 0.0}

            df = pd.DataFrame(bars)
            # Ensure proper datetime index for resampling
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # Compute weekly bars for basing/coiling detection
            weekly_df = df.resample('W', label='right', closed='right').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
            logging.info(f"Computed weekly resampled data for {contract.symbol} with {len(weekly_df)} weeks of history.")

            # Calculate moving average distance
            close = df['close']
            sma200 = close.rolling(200).mean().iloc[-1]
            current = close.iloc[-1]
            dist_to_200dma = (current - sma200) / sma200

            # BB compression detection
            coiling_flag, contraction_mean, contraction_median = self.detect_volatility_contraction(weekly_df)

            # Converging triangle detection
            triangle_flag, (high_slope, low_slope) = self.detect_converging_triangle(weekly_df)
            
            # Streamlined regime assignment
            if coiling_flag and abs(dist_to_200dma) < 0.08:
                regime_label = "Accumulation Base"
            elif current > sma200:
                regime_label = "Bullish Trend"
            else:
                regime_label = "Bearish/Distribution"

            return {
                "regime": regime_label,
                "coiling": int(coiling_flag),
                "contraction_mean": float(contraction_mean),
                "contraction_median": float(contraction_median),
                "triangle_flag": int(triangle_flag),
                "high_slope": float(high_slope),
                "low_slope": float(low_slope),
                "dist_to_200dma": float(dist_to_200dma)
            }
        except Exception as e:
            logging.error(f"Regime baseline matrix calculation failed for {contract.symbol}: {e}")
            return {"regime": "Error", "coiling": False, "contraction_mean": 0.0, "contraction_median": 0.0, "triangle_flag": 0, "high_slope": 0.0, "low_slope": 0.0, "dist_to_200dma": 0.0}

    async def safe_fetch_tickers(self, contracts, chunk_size=40):
        """Highly defensive fetcher with explicit wait times for slow OI ticks."""
        all_tickers = []
        
        # Ensure all contracts are fully qualified
        valid_contracts = [c for c in contracts if c.conId > 0]
        
        for i in range(0, len(valid_contracts), chunk_size):
            chunk = valid_contracts[i:i + chunk_size]
            chunk_tickers = []
            
            try:
                # 1. Manually open the streams for the chunk
                # genericTickList='100,101' explicitly requests Volume (100) and Open Interest (101)
                for contract in chunk:
                    ticker = self.ib.reqMktData(contract, genericTickList='100,101', snapshot=False)
                    chunk_tickers.append(ticker)
                
                # 2. Wait for the OCC Open Interest ticks to trickle in
                await asyncio.sleep(5.0) 
                
                # 3. Validation and storage
                for t in chunk_tickers:
                    if t is not None and hasattr(t, 'contract') and t.contract is not None:
                        all_tickers.append(t)
                    else:
                        logging.warning("Received malformed ticker object from IBKR. Skipping.")
                        
                # 4. Cleanly cancel the streams so you don't hit IBKR's active pacing limits (usually 100 max)
                for contract in chunk:
                    self.ib.cancelMktData(contract)
                
            except Exception as e:
                logging.error(f"Error during chunked ticker fetch: {e}")
                # Fallback cleanup just in case
                for contract in chunk:
                    self.ib.cancelMktData(contract)
                continue
            
        return all_tickers

    async def analyze_positioning_footprint(self, contract, underlying_price, min_days_to_expiry=60, max_days_to_expiry=550):
        """Parses multi-month option chain structures looking for heavy accumulation near/at-the-money."""
        logging.info(f"Extracting derivative positioning footprints for {contract.symbol}...")
        try:
            if not underlying_price or underlying_price == 0:
                return {"leap_volume_skews": 0, "dominant_expiry": "None", "atm_oi_depth": 0}
                
            chains = await self.ib.reqSecDefOptParamsAsync(contract.symbol, '', contract.secType, contract.conId)
            if not chains:
                return {"leap_volume_skews": 0, "dominant_expiry": "None", "atm_oi_depth": 0}
            
            chain = next((c for c in chains if c.exchange == 'SMART'), chains[0])
            
            # Filter expiries between {min_days_to_expiry} days out and {max_days_to_expiry} days out
            now = datetime.now()
            min_date = (now + timedelta(days=min_days_to_expiry)).strftime('%Y%m%d')
            max_date = (now + timedelta(days=max_days_to_expiry)).strftime('%Y%m%d')
            target_expiries = [exp for exp in chain.expirations if min_date <= exp <= max_date]
            logging.info(f"Found {len(target_expiries)} target expirations for {contract.symbol} between {min_days_to_expiry} and {max_days_to_expiry} days out.")
            
            if not target_expiries:
                return {"leap_volume_skews": 0, "dominant_expiry": "None", "atm_oi_depth": 0}
            
            strikes = sorted(list(chain.strikes))
            nearest_strikes = sorted(
                strikes,
                key=lambda s: abs(s - underlying_price)
            )[:5]
            
            if not nearest_strikes:
                closest_strike = min(strikes, key=lambda x: abs(x - underlying_price))
                nearest_strikes = [closest_strike]

            # Sample the 5 nearest multi-month expirations
            sampled_expiries = sorted(target_expiries)[:5]
            
            option_contracts = []
            for expiry in sampled_expiries:
                # Calculate days to expiration for our heuristic
                exp_date = datetime.strptime(expiry, '%Y%m%d')
                days_to_exp = (exp_date - datetime.now()).days
                logging.info(f"Processing expiry {expiry} for {contract.symbol} with {days_to_exp} days to expiration...")

                for strike in nearest_strikes:
                    # If the expiry is long-dated, bypass standard fractions to avoid Error 200 drops.
                    if days_to_exp > 90 and (strike % 1 != 0):
                        continue

                    for right in ['C', 'P']:
                        opt = Option(contract.symbol, expiry, strike, right, 'SMART', currency='USD')
                        option_contracts.append(opt)
                        logging.info(f"Prepared option contract: {opt.symbol} {opt.lastTradeDateOrContractMonth} {opt.strike} {opt.right}")
                        
            qualified_options = await self.ib.qualifyContractsAsync(*option_contracts)

            # Filter out failed qualifications
            qualified_options = [
                q for q in qualified_options
                if q is not None and getattr(q, 'conId', 0)
            ]

            if not qualified_options:
                logging.warning(f"No valid qualified option contracts for {contract.symbol}")
                return {
                    "leap_volume_skews": 0,
                    "dominant_expiry": "None",
                    "atm_oi_depth": 0
                }
                
            logging.info(f"Requesting snapshots safely for {len(qualified_options)} ATM derivative contracts...")
            # Use the safe chunked request method here
            # 1. Fetch raw tickers
            raw_opt_tickers = await self.safe_fetch_tickers(qualified_options, chunk_size=40)
            
            # 2. SANITIZE: Filter out None or tickers with missing contract info
            logging.info(f"Filtering out invalid tickers from the fetched option data...")
            opt_tickers = [t for t in raw_opt_tickers if t is not None and getattr(t, 'contract', None) is not None]
            
            total_leap_call_vol = 0
            total_leap_put_vol = 0
            total_leap_call_oi = 0
            total_leap_put_oi = 0
            max_oi = 0
            max_vol = 0
            dominant_expiry_by_oi = "None"
            dominant_expiry_by_vol = "None"
            total_atm_oi = 0
            
            logging.info(f"Analyzing {len(opt_tickers)} qualified option tickers...")
            for ot in opt_tickers:
                # Safely extract and sanitize OI
                if ot.contract.right == 'C':
                    raw_oi = getattr(ot, 'callOpenInterest', 0)
                else:
                    raw_oi = getattr(ot, 'putOpenInterest', 0)
                oi = 0 if raw_oi is None or math.isnan(float(raw_oi)) else int(raw_oi)
                
                # Safely extract and sanitize Volume
                raw_vol = getattr(ot, 'volume', 0)
                #if ot.contract.right == 'C':
                #    raw_vol = getattr(ot, 'callVolume', 0)
                #else:
                #    raw_vol = getattr(ot, 'putVolume', 0)

                vol = 0 if raw_vol is None or math.isnan(float(raw_vol)) else int(raw_vol)
                total_atm_oi += oi
                # FOR TESTING ONLY
                logging.info(f"Option {ot.contract.symbol} {ot.contract.lastTradeDateOrContractMonth} {ot.contract.strike} {ot.contract.right}: OI={oi}, Vol={vol}") 
                
                if oi > max_oi:
                    max_oi = oi
                    dominant_expiry_by_oi = ot.contract.lastTradeDateOrContractMonth
                
                if vol > max_vol:
                    max_vol = vol
                    dominant_expiry_by_vol = ot.contract.lastTradeDateOrContractMonth
                
                if ot.contract.right == 'C':
                    total_leap_call_vol += vol
                    total_leap_call_oi += oi
                else:
                    total_leap_put_vol += vol
                    total_leap_put_oi += oi

            vol_skew = round(float(total_leap_call_vol) / float(total_leap_put_vol or 1), 2)
            oi_skew = round(float(total_leap_call_oi) / float(total_leap_put_oi or 1), 2)
            
            return {
                "leap_volume_skews": vol_skew,
                "dominant_expiry_by_vol": dominant_expiry_by_vol,
                "dominant_expiry_by_oi": dominant_expiry_by_oi,
                "atm_oi_depth": total_atm_oi,
                "leap_oi_skews": oi_skew
            }
            
        except Exception as err:
            logging.error(f"Derivative structural block parsing failed for {contract.symbol}: {err}")
            return {"leap_volume_skews": 0, "dominant_expiry_by_vol": "None", "dominant_expiry_by_oi": "None", "atm_oi_depth": 0, "leap_oi_skews": 0}

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            logging.info("IBKR Connection cleanly uncoupled.")

class Pillar2SECData:
    """Handles time-decayed Form 4 extraction and XBRL capital-structure metrics via EDGAR."""
    @staticmethod
    def compute_insider_conviction(symbol, lookback_days=90):
        """Calculates time-decayed net institutional buying IN DOLLARS (Notional USD)."""
        logging.info(f"Computing insider conviction for {symbol} with a {lookback_days}-day lookback window...")
        try:
            company = Company(symbol)
            filings = company.get_filings(form="4").head(50)
            if not filings:
                logging.info(f"No recent Form 4 filings found for {symbol}.")
                return {"insider_conviction_score": 0.0, "cfo_involved": 0}
            
            net_notional_usd = 0.0
            unique_insiders = set()
            cfo_activity_flag = 0

            for f in filings:
                if isinstance(f.filing_date, str):
                    parsed_date = datetime.strptime(f.filing_date, "%Y-%m-%d").date()
                else:
                    parsed_date = f.filing_date

                days_old = (datetime.now().date() - parsed_date).days
                if days_old > lookback_days:
                    continue

                weight = max(0.1, (lookback_days - days_old) / float(lookback_days))
                xml_content = f.xml()
                if not xml_content:
                    continue
                
                title_match = re.search(r'<officerTitle>(.*?)</officerTitle>', xml_content, re.IGNORECASE)
                officer_title = title_match.group(1).upper() if title_match else "UNKNOWN"
                
                is_cfo = 1 if any(keyword in officer_title for keyword in ["CFO", "FINANCIAL", "ACCOUNTING", "TREASURER"]) else 0
                title_multiplier = 1.5 if is_cfo else 1.0 
                
                if is_cfo:
                    cfo_activity_flag = 1
                
                transactions = re.findall(r'<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>', xml_content, re.DOTALL)
                filing_net_usd = 0.0
                for trans in transactions:
                    code_match = re.search(r'<transactionCode>(P|S)</transactionCode>', trans)
                    if not code_match:
                        continue
                    code = code_match.group(1)
                    
                    shares_match = re.search(r'<transactionShares>\s*<value>([\d\.]+)</value>', trans)
                    price_match = re.search(r'<transactionPricePerShare>\s*<value>([\d\.]+)</value>', trans)
                    
                    if shares_match and price_match:
                        shares = float(shares_match.group(1))
                        price = float(price_match.group(1))
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
                "cfo_involved": cfo_activity_flag
            }
        except Exception as e:
            logging.info(f"Insider context tracking exception on {symbol}: {e}")
            return {"insider_conviction_score": 0.0, "cfo_involved": 0}

    @staticmethod
    def extract_debt_reduction_metrics(symbol):
        """Evaluates liability reduction vectors through us-gaap sequential XBRL elements."""
        logging.info(f"Extracting debt reduction metrics for {symbol} from XBRL filings...")
        try:
            company = Company(symbol)
            facts = company.get_facts()
            target_concepts = ["LongTermDebt", "DebtInstrumentCarryingAmount"]
            
            for concept in target_concepts:
                try:
                    df = facts.to_pandas(f"us-gaap:{concept}")
                    if df.empty:
                        continue
                    df = df.sort_values('end', ascending=False)
                    current_val = df.iloc[0]['val']
                    prev_val = df.iloc[1]['val']
                    if prev_val == 0:
                        continue
                    reduction_pct = ((prev_val - current_val) / prev_val) * 100.0
                    return round(reduction_pct, 2)
                except Exception:
                    continue
            return 0.0
        except Exception:
            return 0.0

class Pillar3CustomParser:
    """Orchestrates structural text decomposition via MasterParserClass and targeted deep zero-shot NLP classification."""
    def __init__(self, filing_fetcher_func):
        self.fetch_filings = filing_fetcher_func
        
        logging.info("Loading Zero-Shot NLP Engine into memory...")
        from transformers import pipeline
        self.classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
        
        self.catalyst_dictionaries = {
            "M&A / Strategic Review": [r"strategic alternatives", r"evaluate alternatives", r"retained.*?advisor", r"potential sale"],
            "Activism / Spinoff": [r"spin-off", r"spinoff", r"carve-out", r"value creation", r"board seat", r"dissident"],
            "Restructuring": [r"turnaround plan", r"cost reduction", r"headcount reduction", r"impairment"],
            "Management Alignment": [r"free cash flow", r"return on invested capital", r"roic", r"performance target", r"debt reduction", r"incentive plan"]
        }
        self.target_sections = ["item 1.01", "item 7.01", "item 8.01", "item 4"]

    def process_corporate_catalysts(self, symbol):
        logging.info(f"[{symbol}] Processing corporate text structures...")
        features = {"catalyst_flag": 0, "catalyst_type": "None", "catalyst_confidence": 0.0, "has_activist_pressure": 0}
        
        try:
            recent_filings = self.fetch_filings(symbol, forms=['8-K', '8-K/A', 'SC 13D', 'SC 13D/A', 'DEF 14A'], days_lookback=90)
            if not recent_filings:
                return features

            highest_confidence = 0.0
            best_catalyst = "None"

            for filing in recent_filings:
                logging.info(f"Parsing {filing['type']} filed on {filing['date']} for potential catalyst sections...")
                parser = MasterParserClass(filing['fulltext'], filing['type'])
                dfs = parser.output_dfs()
                target_df = pd.DataFrame()
                
                if filing['type'] in ['8-K', '8-K/A', 'SC 13D', 'SC 13D/A']:
                    named_sections_df = dfs.get(settings.NAMED_SECTIONS_TABLE, pd.DataFrame())
                    if not named_sections_df.empty:
                        target_df = named_sections_df[named_sections_df['section_name'].str.lower().isin(self.target_sections)]
                elif filing['type'] == 'DEF 14A':
                    toc_df = dfs.get(settings.TOC_SECTIONS_TABLE, pd.DataFrame())
                    if not toc_df.empty:
                        target_df = toc_df[toc_df['section_name'].str.contains(r'compensation discussion|cd&a', case=False, na=False)]
                
                if target_df.empty:
                    logging.info(f"No relevant sections found in {filing['type']} for {symbol}. Skipping NLP classification.")
                    continue
                
                for _, row in target_df.iterrows():
                    logging.info(f"Evaluating section '{row.get('section_name', 'Unknown')}' for catalyst indicators...")
                    text_chunk = row.get('text', '')
                    if not text_chunk or len(text_chunk) < 50:
                        logging.info(f"Section text is too short for meaningful analysis. Skipping.")
                        continue
                    
                    paragraphs = [p.strip() for p in text_chunk.split('\n\n') if len(p.strip()) > 50]
                    for para in paragraphs:
                        keyword_hit = False
                        for cat_name, patterns in self.catalyst_dictionaries.items():
                            for pattern in patterns:
                                if re.search(pattern, para, re.IGNORECASE):
                                    keyword_hit = True
                                    logging.info(f"Keyword match for category '{cat_name}' found in section '{row.get('section_name', 'Unknown')}'. Proceeding to NLP classification.")
                                    break
                            if keyword_hit: break
                        
                        if not keyword_hit:
                            logging.info(f"No catalyst keywords found in paragraph. Skipping NLP classification for this segment.")
                            continue
                        
                        candidate_labels = list(self.catalyst_dictionaries.keys()) + ["Routine Business Operations"]
                        nlp_res = self.classifier(para[:1024], candidate_labels=candidate_labels)
                        
                        top_label = nlp_res['labels'][0]
                        confidence = nlp_res['scores'][0]
                        
                        if top_label == "Routine Business Operations":
                            continue
                            
                        logging.info(f"NLP Classification Result: '{top_label}' with confidence {confidence:.3f} for section '{row.get('section_name', 'Unknown')}'")
                        if confidence > highest_confidence:
                            highest_confidence = confidence
                            best_catalyst = top_label
                            features["catalyst_flag"] = 1
                            if top_label == "Activism / Spinoff":
                                features["has_activist_pressure"] = 1

            features["catalyst_type"] = best_catalyst
            features["catalyst_confidence"] = round(highest_confidence, 3)
            return features
            
        except Exception as e:
            logging.error(f"Text architecture evaluation matrix faulted for {symbol}: {e}")
            return features

class ConvergencePipeline:
    """Orchestrates and merges feature streams into an analytical Daily Option Signal sheet."""
    def __init__(self, custom_parser_engine):
        self.ib_broker = Pillar1MarketData()
        self.sec_broker = Pillar2SECData()
        self.text_broker = custom_parser_engine
        self.feature_store = []

    def _passes_advanced_filter(self, feature_row):
        
        symbol = feature_row.get("symbol", "N/A")
        dist_200dma = feature_row.get("dist_to_200dma", 0)
        ic_score = feature_row.get("insider_conviction_score", 0)
        regime = feature_row.get("market_regime", "")
        contraction_mean = float(feature_row.get("contraction_mean", float('nan')))

        # Negative insider conviction: always reject
        if ic_score < 0:
            logging.info(f"Filtering out {symbol}: negative insider conviction ({ic_score}).")
            return False

        # Not an accumulation base: reject
        if regime != "Accumulation Base":
            logging.info(f"Filtering out {symbol}: regime not 'Accumulation Base' (found '{regime}').")
            return False

        # Too far from 200dma: reject
        if dist_200dma > 0.08 or dist_200dma < -0.12:
            logging.info(f"Filtering out {symbol}: distance from 200dma ({dist_200dma:.3f}) out of bounds.")
            return False

        # Invalid contraction value: reject
        if math.isnan(contraction_mean):
            logging.info(f"Filtering out {symbol}: contraction_mean is NaN or missing.")
            return False

        # Not enough volatility contraction: reject
        if contraction_mean >= 0.45:
            logging.info(f"Filtering out {symbol}: contraction_mean too high ({contraction_mean:.2f} >= 0.45).")
            return False

        return True
    
    async def execute_daily_build(self):
        await self.ib_broker.connect_async()
        
        try:
            # Running with expanded limit
            raw_contracts, pc_ratios = await self.ib_broker.scan_accumulation_candidates(limit=75)
            active_tickers = await self.ib_broker.fetch_ticker_snapshots(raw_contracts)
            
            logging.info(f"Cross-referencing telemetry points across {len(active_tickers)} qualified scanner assets...")
            
            for t in active_tickers:
                sym = t.contract.symbol
                logging.info(f"Processing convergence features for {sym}...")
                
                regime_metrics = await self.ib_broker.determine_market_regime(t.contract)
                
                last_price = float(t.last) if t.last and t.last > 0 else 0.0
                positioning_footprint = await self.ib_broker.analyze_positioning_footprint(t.contract, last_price)
                
                insider_score = self.sec_broker.compute_insider_conviction(sym)
                debt_delta = self.sec_broker.extract_debt_reduction_metrics(sym)
                catalyst_features = self.text_broker.process_corporate_catalysts(sym)

                feature_row = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d"),
                    "symbol": sym,
                    "last_price": last_price,
                    "put_call_ratio": float(pc_ratios.get(t.contract.conId, 0.0)),
                    "call_volume": int(t.callVolume) if t.callVolume else 0,
                    "put_volume": int(t.putVolume) if t.putVolume else 0,
                    "opt_volume": int((t.callVolume or 0) + (t.putVolume or 0)),
                    "av_option_volume": int(t.avOptionVolume) if t.avOptionVolume else 0,
                    "opt_vol_expansion_ratio": round(float((t.callVolume or 0) + (t.putVolume or 0)) / float(t.avOptionVolume or 1), 2),
                    "market_regime": str(regime_metrics["regime"]),
                    "is_coiling": regime_metrics["coiling"],
                    "dist_to_200dma": float(regime_metrics["dist_to_200dma"]),
                    "contraction_mean": float(regime_metrics.get("contraction_mean", 0.0)),
                    "contraction_median": float(regime_metrics.get("contraction_median", 0.0)),
                    "triangle_flag": regime_metrics.get("triangle_flag", 0),
                    "high_slope": float(regime_metrics.get("high_slope", 0.0)),
                    "low_slope": float(regime_metrics.get("low_slope", 0.0)),
                    **positioning_footprint,
                    **insider_score, 
                    "debt_reduction_pct": float(debt_delta),
                    **catalyst_features 
                }
                
                if not self._passes_advanced_filter(feature_row):
                    continue
                self.feature_store.append(feature_row)
                
        finally:
            self.ib_broker.disconnect()
        
        self.export_to_feature_store()

    async def test_execute_daily_build(self):
        """Weekend execution simulation with explicit fallback data arrays."""
        await self.ib_broker.connect_async()
        
        try:
            logging.info("WEEKEND TEST MODE: Bypassing live TWS scanner...")
            test_symbols = ["ATEN", "SW", "OSPN", "CRBP", "PROP", "L", "NTCT", "CIB", "MEC", "PRM", "TAC", "LBRT", "CPT", "GTES", "MEI", "NMRA", "VTGN", "GETY", "PDFS"] 
            
            active_tickers = []
            for sym in test_symbols:
                contract = Stock(sym, 'SMART', 'USD')
                await self.ib_broker.ib.qualifyContractsAsync(contract)
                tickers = await self.ib_broker.ib.reqTickersAsync(contract)
                await asyncio.sleep(0.5)
                if tickers:
                    active_tickers.append(tickers[0])
            
            for t in active_tickers:
                sym = t.contract.symbol
                logging.info(f"Processing convergence features for {sym}...")
                
                regime_metrics = await self.ib_broker.determine_market_regime(t.contract)
                
                # Fetch recent historical daily close to serve as fallback price reference
                historical_bars = await self.ib_broker.ib.reqHistoricalDataAsync(t.contract, endDateTime='', durationStr='5 D', barSizeSetting='1 day', whatToShow='TRADES', useRTH=True)
                fallback_price = historical_bars[-1].close if historical_bars else 100.0
                
                positioning_footprint = await self.ib_broker.analyze_positioning_footprint(t.contract, fallback_price)
                insider_score = self.sec_broker.compute_insider_conviction(sym)
                debt_delta = self.sec_broker.extract_debt_reduction_metrics(sym)
                catalyst_features = self.text_broker.process_corporate_catalysts(sym)

                feature_row = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d"),
                    "symbol": sym,
                    "last_price": fallback_price, 
                    "opt_vol_expansion_ratio": 1.0, 
                    "market_regime": str(regime_metrics["regime"]),
                    "is_coiling": regime_metrics["coiling"],
                    "dist_to_200dma": float(regime_metrics["dist_to_200dma"]),
                    "contraction_mean": float(regime_metrics.get("contraction_mean", 0.0)),
                    "contraction_median": float(regime_metrics.get("contraction_median", 0.0)),
                    "triangle_flag": regime_metrics.get("triangle_flag", 0),
                    "high_slope": float(regime_metrics.get("high_slope", 0.0)),
                    "low_slope": float(regime_metrics.get("low_slope", 0.0)),
                    **positioning_footprint,
                    **insider_score, 
                    "debt_reduction_pct": float(debt_delta),
                    **catalyst_features 
                }
                
                if not self._passes_advanced_filter(feature_row):
                    continue
                self.feature_store.append(feature_row)
                
        finally:
            self.ib_broker.disconnect()
        
        self.export_to_feature_store()

    def export_to_feature_store(self):
        df = pd.DataFrame(self.feature_store)
        if df.empty:
            logging.warning("Scanner execution sweep concluded with zero matching signals.")
            return

        df = df.sort_values(
            by=["is_coiling", "leap_volume_skews", "catalyst_flag", "insider_conviction_score"], 
            ascending=[False, False, False, False]
        )
        
        output_dir = "feature_store_archives"
        os.makedirs(output_dir, exist_ok=True)
        
        file_path = os.path.join(output_dir, f"convergence_signals_{datetime.now().strftime('%Y%m%d')}.csv")
        df.to_csv(file_path, index=False)
        logging.info(f"Signal matrix generation completed. Output written to local engine storage: {file_path}")

def internal_sec_filing_fetcher(symbol, forms, days_lookback):
    """
    Localized delivery module acting as the proxy fetch pipeline for python-edgar data wrappers.
    Converts online streaming document payloads into a standardized structural dictionary array.
    """
    logging.info(f"Fetching and structuring recent SEC filings for {symbol} with forms {forms} and a {days_lookback}-day lookback window...")
    file_payloads = []
    try:
        comp = Company(symbol)
        lookback_cutoff = datetime.now() - timedelta(days=days_lookback)
        
        for form_type in forms:
            filing_list = comp.get_filings(form=form_type)
            if not filing_list:
                continue
            
            for filing_entry in filing_list.head(10):
                if isinstance(filing_entry.filing_date, str):
                    f_date = datetime.strptime(filing_entry.filing_date, "%Y-%m-%d")
                else:
                    f_date = datetime.combine(filing_entry.filing_date, datetime.min.time())
                
                if f_date < lookback_cutoff:
                    continue
                
                file_payloads.append({
                    "type": str(form_type),
                    "fulltext": filing_entry.full_text_submission(), 
                    "date": str(filing_entry.filing_date) 
                })

    except Exception as err:
        logging.debug(f"Historical filing pull canceled for symbol token {symbol}: {err}")
        
    return file_payloads

if __name__ == "__main__":
    parser_module = Pillar3CustomParser(filing_fetcher_func=internal_sec_filing_fetcher)
    pipeline = ConvergencePipeline(custom_parser_engine=parser_module)

    # Fire the event loop runner
    asyncio.run(pipeline.test_execute_daily_build())