-- signals
CREATE TABLE IF NOT EXISTS signals (
  id SERIAL PRIMARY KEY,
  run_date DATE NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  last_price DOUBLE PRECISION,
  current_iv DOUBLE PRECISION,
  iv_rank_52wk DOUBLE PRECISION,
  iv_pct_52wk DOUBLE PRECISION,
  iv_rank_13wk DOUBLE PRECISION,
  iv_pct_13wk DOUBLE PRECISION,
  put_call_ratio DOUBLE PRECISION,
  call_volume INTEGER,
  put_volume INTEGER,
  opt_volume INTEGER,
  av_option_volume INTEGER,
  opt_vol_expansion_ratio DOUBLE PRECISION,
  market_regime TEXT,
  is_coiling INTEGER,
  dist_to_200dma DOUBLE PRECISION,
  contraction_mean DOUBLE PRECISION,
  contraction_median DOUBLE PRECISION,
  triangle_flag INTEGER,
  high_slope DOUBLE PRECISION,
  low_slope DOUBLE PRECISION,

  -- positioning_footprint fields
  atm_volume_skews DOUBLE PRECISION,
  atm_dominant_expiry_by_vol TEXT,
  atm_dominant_expiry_by_oi TEXT,
  atm_oi_depth INTEGER,
  atm_oi_skews DOUBLE PRECISION,
  otm_call_iv_max_skew_expiry TEXT,
  otm_call_iv DOUBLE PRECISION,
  otm_put_iv DOUBLE PRECISION,
  call_put_iv_skew_pct DOUBLE PRECISION,
  call_put_iv_skew_pct_avg DOUBLE PRECISION,
  call_skew_flag INTEGER,
  iv_skew_expiries_matched INTEGER,
  call_vol_concentration_pct DOUBLE PRECISION,
  concentration_expiry TEXT,
  concentration_strike DOUBLE PRECISION,
  term_structure_anomaly_expiry TEXT,
  term_structure_anomaly_pct DOUBLE PRECISION,
  term_structure_flag INTEGER,
  deal_band_call_vol INTEGER,
  deal_band_call_oi INTEGER,
  deal_band_vol_oi_ratio DOUBLE PRECISION,
  deal_band_concentration_pct DOUBLE PRECISION,
  deal_band_dominant_expiry TEXT,
  deal_band_dominant_strike DOUBLE PRECISION,

  insider_conviction_score DOUBLE PRECISION,
  cfo_involved INTEGER,
  debt_reduction_pct DOUBLE PRECISION,

  catalyst_flag INTEGER,
  catalyst_type TEXT,
  catalyst_confidence DOUBLE PRECISION,
  has_activist_pressure INTEGER,

  -- full raw feature payload for forward compatibility
  raw JSONB,

  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_signals_symbol_date ON signals(symbol, run_date);


-- option_ticks
CREATE TABLE IF NOT EXISTS option_ticks (
  id SERIAL PRIMARY KEY,
  run_date DATE NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  conid BIGINT,
  expiry TEXT,                 -- keep YYYYMMDD or ISO date string
  strike DOUBLE PRECISION,
  "right" CHAR(1),
  multiplier TEXT,
  trading_class TEXT,
  bid DOUBLE PRECISION,
  ask DOUBLE PRECISION,
  last DOUBLE PRECISION,
  implied_vol DOUBLE PRECISION,
  delta DOUBLE PRECISION,
  gamma DOUBLE PRECISION,
  vega DOUBLE PRECISION,
  theta DOUBLE PRECISION,
  open_interest INTEGER,
  volume INTEGER,
  iv_source TEXT,
  sample_type TEXT,
  src TEXT,
  raw JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_option_ticks_symbol_date ON option_ticks(symbol, run_date);
CREATE INDEX IF NOT EXISTS idx_option_ticks_conid ON option_ticks(conid);


-- Create table for per-symbol daily metrics
CREATE TABLE IF NOT EXISTS symbol_day_metrics (
  id SERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  run_date DATE NOT NULL,
  last_price DOUBLE PRECISION,
  prev_close DOUBLE PRECISION,
  pct_change DOUBLE PRECISION,
  market_regime TEXT,
  is_coiling INTEGER,
  insider_conviction_score DOUBLE PRECISION,
  catalyst_flag INTEGER,
  catalyst_type TEXT,
  catalyst_confidence DOUBLE PRECISION,
  total_call_volume BIGINT,
  total_put_volume BIGINT,
  total_option_volume BIGINT,
  call_volume_share DOUBLE PRECISION,
  total_call_oi BIGINT,
  total_put_oi BIGINT,
  total_oi BIGINT,
  call_put_oi_ratio DOUBLE PRECISION,
  top_call_line_volume BIGINT,
  top_call_line_share DOUBLE PRECISION,
  top_3_call_concentration_pct DOUBLE PRECISION,
  dominant_expiry_by_call_vol TEXT,
  avg_call_iv DOUBLE PRECISION,
  avg_put_iv DOUBLE PRECISION,
  call_put_iv_diff DOUBLE PRECISION,
  call_delta_weighted_volume DOUBLE PRECISION,
  deal_band_call_volume BIGINT,
  deal_band_call_share DOUBLE PRECISION,
  call_volume_change BIGINT,
  call_oi_change BIGINT,
  put_volume_change BIGINT,
  put_oi_change BIGINT,
  consecutive_days_call_build INTEGER,
  call_build_flag INTEGER,
  rows_ingested INTEGER,
  raw JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_symbol_day_metrics_symbol_date ON symbol_day_metrics(symbol, run_date);
CREATE INDEX IF NOT EXISTS idx_symbol_day_metrics_run_date ON symbol_day_metrics(run_date);