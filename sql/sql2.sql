USE WAREHOUSE MORTGAGE_ETL_WH;
USE DATABASE MORTGAGE_RISK_DB;

/* =========================
   RAW: DAILY FILES
   ========================= */
USE SCHEMA RAW;

CREATE OR REPLACE TABLE daily_payments_raw (
  payment_id STRING,
  mortgage_id STRING,
  borrower_id STRING,
  payment_date DATE,
  payment_amount NUMBER(14,2),
  payment_status STRING,           -- PAID / PARTIAL / MISSED
  days_past_due NUMBER(5,0),
  principal_outstanding NUMBER(14,2),
  source_file STRING,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (payment_date, mortgage_id);

/* =========================
   RAW: MONTHLY FILES
   ========================= */
CREATE OR REPLACE TABLE monthly_mortgage_snapshot_raw (
  snapshot_month DATE,             -- use first day of month
  mortgage_id STRING,
  borrower_id STRING,
  current_interest_rate NUMBER(8,4),
  current_balance NUMBER(14,2),
  current_ltv NUMBER(10,4),
  property_value NUMBER(14,2),
  region STRING,
  delinquency_bucket STRING,       -- CURRENT / 1-30 / 31-60 / 61-90 / 90+
  source_file STRING,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (snapshot_month, region);

/* =========================
   RAW: QUARTERLY FILES
   ========================= */
CREATE OR REPLACE TABLE quarterly_portfolio_perf_raw (
  quarter_end_date DATE,
  region STRING,
  product_type STRING,
  mortgages_count NUMBER,
  delinquent_count NUMBER,
  defaults_count NUMBER,
  prepayment_count NUMBER,
  avg_ltv NUMBER(10,4),
  avg_dti NUMBER(10,4),
  source_file STRING,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (quarter_end_date, region);

/* =========================
   OPS: COPY AUDIT TABLE
   ========================= */
USE SCHEMA OPS;

CREATE OR REPLACE TABLE copy_run_audit (
  run_id STRING,
  file_type STRING,                -- DAILY / MONTHLY / QUARTERLY
  stage_path STRING,
  started_ts TIMESTAMP_NTZ,
  ended_ts TIMESTAMP_NTZ,
  rows_loaded NUMBER,
  status STRING,                   -- SUCCESS / FAILED
  message STRING
);
