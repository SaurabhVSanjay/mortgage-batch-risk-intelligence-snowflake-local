USE DATABASE MORTGAGE_RISK_DB;
USE WAREHOUSE MORTGAGE_ETL_WH;

/* =========================
   CURATED TABLES
   ========================= */
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE payments_curated (
  payment_id STRING,
  mortgage_id STRING,
  borrower_id STRING,
  payment_date DATE,
  payment_amount NUMBER(14,2),
  payment_status STRING,
  days_past_due NUMBER(5,0),
  principal_outstanding NUMBER(14,2),
  source_file STRING,
  load_ts TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE monthly_snapshot_curated (
  snapshot_month DATE,
  mortgage_id STRING,
  borrower_id STRING,
  current_interest_rate NUMBER(8,4),
  current_balance NUMBER(14,2),
  current_ltv NUMBER(10,4),
  property_value NUMBER(14,2),
  region STRING,
  delinquency_bucket STRING,
  source_file STRING,
  load_ts TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE quarterly_perf_curated (
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
  load_ts TIMESTAMP_NTZ
);

/* =========================
   STREAMS ON RAW TABLES
   ========================= */
USE SCHEMA RAW;

CREATE OR REPLACE STREAM st_daily_payments_raw ON TABLE daily_payments_raw;
CREATE OR REPLACE STREAM st_monthly_snapshot_raw ON TABLE monthly_mortgage_snapshot_raw;
CREATE OR REPLACE STREAM st_quarterly_perf_raw ON TABLE quarterly_portfolio_perf_raw;

/* =========================
   TASKS: RAW -> CURATED
   ========================= */
USE SCHEMA CURATED;

CREATE OR REPLACE TASK task_curate_daily_payments
  WAREHOUSE = MORTGAGE_ETL_WH
  SCHEDULE = 'USING CRON 30 1 * * * Europe/Dublin'
AS
MERGE INTO payments_curated t
USING (
  SELECT payment_id,mortgage_id,borrower_id,payment_date,payment_amount,payment_status,days_past_due,principal_outstanding,source_file,load_ts
  FROM RAW.st_daily_payments_raw
) s
ON t.payment_id = s.payment_id
WHEN MATCHED THEN UPDATE SET
  t.mortgage_id = s.mortgage_id,
  t.borrower_id = s.borrower_id,
  t.payment_date = s.payment_date,
  t.payment_amount = s.payment_amount,
  t.payment_status = s.payment_status,
  t.days_past_due = s.days_past_due,
  t.principal_outstanding = s.principal_outstanding,
  t.source_file = s.source_file,
  t.load_ts = s.load_ts
WHEN NOT MATCHED THEN INSERT
(payment_id,mortgage_id,borrower_id,payment_date,payment_amount,payment_status,days_past_due,principal_outstanding,source_file,load_ts)
VALUES
(s.payment_id,s.mortgage_id,s.borrower_id,s.payment_date,s.payment_amount,s.payment_status,s.days_past_due,s.principal_outstanding,s.source_file,s.load_ts);

CREATE OR REPLACE TASK task_curate_monthly_snapshot
  WAREHOUSE = MORTGAGE_ETL_WH
  SCHEDULE = 'USING CRON 45 1 1 * * Europe/Dublin'
AS
MERGE INTO monthly_snapshot_curated t
USING (
  SELECT snapshot_month,mortgage_id,borrower_id,current_interest_rate,current_balance,current_ltv,property_value,region,delinquency_bucket,source_file,load_ts
  FROM RAW.st_monthly_snapshot_raw
) s
ON t.snapshot_month = s.snapshot_month AND t.mortgage_id = s.mortgage_id
WHEN MATCHED THEN UPDATE SET
  t.borrower_id = s.borrower_id,
  t.current_interest_rate = s.current_interest_rate,
  t.current_balance = s.current_balance,
  t.current_ltv = s.current_ltv,
  t.property_value = s.property_value,
  t.region = s.region,
  t.delinquency_bucket = s.delinquency_bucket,
  t.source_file = s.source_file,
  t.load_ts = s.load_ts
WHEN NOT MATCHED THEN INSERT
(snapshot_month,mortgage_id,borrower_id,current_interest_rate,current_balance,current_ltv,property_value,region,delinquency_bucket,source_file,load_ts)
VALUES
(s.snapshot_month,s.mortgage_id,s.borrower_id,s.current_interest_rate,s.current_balance,s.current_ltv,s.property_value,s.region,s.delinquency_bucket,s.source_file,s.load_ts);

CREATE OR REPLACE TASK task_curate_quarterly_perf
  WAREHOUSE = MORTGAGE_ETL_WH
  SCHEDULE = 'USING CRON 0 2 1 1,4,7,10 * Europe/Dublin'
AS
MERGE INTO quarterly_perf_curated t
USING (
  SELECT quarter_end_date,region,product_type,mortgages_count,delinquent_count,defaults_count,prepayment_count,avg_ltv,avg_dti,source_file,load_ts
  FROM RAW.st_quarterly_perf_raw
) s
ON t.quarter_end_date = s.quarter_end_date AND t.region = s.region AND t.product_type = s.product_type
WHEN MATCHED THEN UPDATE SET
  t.mortgages_count = s.mortgages_count,
  t.delinquent_count = s.delinquent_count,
  t.defaults_count = s.defaults_count,
  t.prepayment_count = s.prepayment_count,
  t.avg_ltv = s.avg_ltv,
  t.avg_dti = s.avg_dti,
  t.source_file = s.source_file,
  t.load_ts = s.load_ts
WHEN NOT MATCHED THEN INSERT
(quarter_end_date,region,product_type,mortgages_count,delinquent_count,defaults_count,prepayment_count,avg_ltv,avg_dti,source_file,load_ts)
VALUES
(s.quarter_end_date,s.region,s.product_type,s.mortgages_count,s.delinquent_count,s.defaults_count,s.prepayment_count,s.avg_ltv,s.avg_dti,s.source_file,s.load_ts);

/* =========================
   OPS RETENTION METADATA
   ========================= */
USE SCHEMA OPS;

MERGE INTO file_ingestion_control t
USING (
  SELECT DISTINCT source_file AS file_name,
    CASE
      WHEN source_file ILIKE 'stg_mortgage/daily/%' THEN 'DAILY'
      WHEN source_file ILIKE 'stg_mortgage/monthly/%' THEN 'MONTHLY'
      WHEN source_file ILIKE 'stg_mortgage/quarterly/%' THEN 'QUARTERLY'
      ELSE 'UNKNOWN'
    END AS file_type,
    CURRENT_DATE() AS file_date
  FROM (
    SELECT source_file FROM RAW.daily_payments_raw
    UNION ALL
    SELECT source_file FROM RAW.monthly_mortgage_snapshot_raw
    UNION ALL
    SELECT source_file FROM RAW.quarterly_portfolio_perf_raw
  )
) s
ON t.file_name = s.file_name
WHEN NOT MATCHED THEN INSERT
(file_name,file_path,file_type,file_date,load_status,rows_loaded,archive_status,retention_days,archive_due_date,delete_due_date,last_updated_ts)
VALUES
(s.file_name,s.file_name,s.file_type,s.file_date,'LOADED',NULL,'ACTIVE',
 IFF(s.file_type='DAILY',90,IFF(s.file_type='MONTHLY',730,IFF(s.file_type='QUARTERLY',2555,30))),
 DATEADD(day, IFF(s.file_type='DAILY',90,IFF(s.file_type='MONTHLY',730,IFF(s.file_type='QUARTERLY',2555,30))), s.file_date),
 DATEADD(day, IFF(s.file_type='DAILY',120,IFF(s.file_type='MONTHLY',760,IFF(s.file_type='QUARTERLY',2600,60))), s.file_date),
 CURRENT_TIMESTAMP());

/* =========================
   RESUME TASKS
   ========================= */
USE SCHEMA CURATED;
ALTER TASK task_curate_daily_payments RESUME;
ALTER TASK task_curate_monthly_snapshot RESUME;
ALTER TASK task_curate_quarterly_perf RESUME;
