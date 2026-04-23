USE DATABASE MORTGAGE_RISK_DB;
USE WAREHOUSE MORTGAGE_ETL_WH;

/* =========================
   FEATURE LAYER
   ========================= */
USE SCHEMA FEATURE;

CREATE OR REPLACE DYNAMIC TABLE borrower_risk_features_dt
  TARGET_LAG = '1 day'
  WAREHOUSE = MORTGAGE_ETL_WH
AS
WITH latest_monthly AS (
  SELECT *
  FROM CURATED.monthly_snapshot_curated
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mortgage_id ORDER BY snapshot_month DESC, load_ts DESC) = 1
),
latest_payment AS (
  SELECT *
  FROM CURATED.payments_curated
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mortgage_id ORDER BY payment_date DESC, load_ts DESC) = 1
),
payment_90d AS (
  SELECT
    mortgage_id,
    borrower_id,
    SUM(IFF(payment_status='MISSED',1,0)) AS missed_payments_90d,
    AVG(days_past_due) AS avg_dpd_90d,
    MAX(days_past_due) AS max_dpd_90d
  FROM CURATED.payments_curated
  WHERE payment_date >= DATEADD(day,-90,CURRENT_DATE())
  GROUP BY mortgage_id, borrower_id
)
SELECT
  m.borrower_id,
  m.mortgage_id,
  m.region,
  m.current_balance,
  m.property_value,
  m.current_ltv,
  m.current_interest_rate,
  COALESCE(p.missed_payments_90d,0) AS missed_payments_90d,
  COALESCE(p.avg_dpd_90d,0) AS avg_dpd_90d,
  COALESCE(p.max_dpd_90d,0) AS max_dpd_90d,
  lp.payment_status AS latest_payment_status,
  lp.days_past_due AS latest_dpd,
  CURRENT_TIMESTAMP() AS feature_ts
FROM latest_monthly m
LEFT JOIN payment_90d p
  ON m.mortgage_id = p.mortgage_id
LEFT JOIN latest_payment lp
  ON m.mortgage_id = lp.mortgage_id;

/* =========================
   RISK SCORE TABLE
   ========================= */
USE SCHEMA MART;

CREATE OR REPLACE TABLE borrower_risk_scores (
  borrower_id STRING,
  mortgage_id STRING,
  region STRING,
  risk_score NUMBER(6,2),
  risk_band STRING,
  as_of_date DATE,
  scored_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/* =========================
   STORED PROCEDURE (SQL)
   ========================= */
CREATE OR REPLACE PROCEDURE sp_score_borrowers()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  DELETE FROM MART.borrower_risk_scores WHERE as_of_date = CURRENT_DATE();

  INSERT INTO MART.borrower_risk_scores (borrower_id,mortgage_id,region,risk_score,risk_band,as_of_date)
  SELECT
    borrower_id,
    mortgage_id,
    region,
    ROUND(
      LEAST(100,
        (COALESCE(missed_payments_90d,0) * 12) +
        (COALESCE(avg_dpd_90d,0) * 0.8) +
        (IFF(COALESCE(current_ltv,0) > 0.90, 20, IFF(COALESCE(current_ltv,0) > 0.80, 10, 0))) +
        (IFF(COALESCE(latest_dpd,0) >= 30, 15, 0))
      ), 2
    ) AS risk_score,
    CASE
      WHEN LEAST(100,
        (COALESCE(missed_payments_90d,0) * 12) +
        (COALESCE(avg_dpd_90d,0) * 0.8) +
        (IFF(COALESCE(current_ltv,0) > 0.90, 20, IFF(COALESCE(current_ltv,0) > 0.80, 10, 0))) +
        (IFF(COALESCE(latest_dpd,0) >= 30, 15, 0))
      ) >= 70 THEN 'HIGH'
      WHEN LEAST(100,
        (COALESCE(missed_payments_90d,0) * 12) +
        (COALESCE(avg_dpd_90d,0) * 0.8) +
        (IFF(COALESCE(current_ltv,0) > 0.90, 20, IFF(COALESCE(current_ltv,0) > 0.80, 10, 0))) +
        (IFF(COALESCE(latest_dpd,0) >= 30, 15, 0))
      ) >= 40 THEN 'MEDIUM'
      ELSE 'LOW'
    END AS risk_band,
    CURRENT_DATE()
  FROM FEATURE.borrower_risk_features_dt;

  RETURN 'Borrower scoring completed';
END;
$$;

/* Run scoring now */
CALL sp_score_borrowers();

/* =========================
   MART VIEWS
   ========================= */
CREATE OR REPLACE VIEW vw_portfolio_risk_summary AS
SELECT
  as_of_date,
  risk_band,
  COUNT(*) AS borrowers,
  ROUND(AVG(risk_score),2) AS avg_score
FROM borrower_risk_scores
GROUP BY as_of_date, risk_band;

CREATE OR REPLACE VIEW vw_region_risk AS
SELECT
  as_of_date,
  region,
  COUNT(*) AS borrowers,
  ROUND(AVG(risk_score),2) AS avg_risk_score,
  SUM(IFF(risk_band='HIGH',1,0)) AS high_risk_borrowers
FROM borrower_risk_scores
GROUP BY as_of_date, region;
