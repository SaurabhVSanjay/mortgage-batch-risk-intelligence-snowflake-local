USE DATABASE MORTGAGE_RISK_DB;
USE WAREHOUSE MORTGAGE_ETL_WH;

/* =========================
   GOVERNANCE ROLES
   ========================= */
USE SCHEMA GOV;

CREATE OR REPLACE ROLE RISK_ANALYST;
CREATE OR REPLACE ROLE REGIONAL_MANAGER_DUBLIN;
CREATE OR REPLACE ROLE REGIONAL_MANAGER_CORK;
CREATE OR REPLACE ROLE REGIONAL_MANAGER_GALWAY;

/* =========================
   TAGS (data classification)
   ========================= */
CREATE OR REPLACE TAG data_classification ALLOWED_VALUES 'PUBLIC','INTERNAL','CONFIDENTIAL','PII';
CREATE OR REPLACE TAG data_domain ALLOWED_VALUES 'MORTGAGE','RISK','FINANCE';

ALTER TABLE MART.borrower_risk_scores SET TAG data_classification='CONFIDENTIAL', data_domain='RISK';
ALTER TABLE FEATURE.borrower_risk_features_dt SET TAG data_classification='CONFIDENTIAL', data_domain='RISK';

/* =========================
   MASKING POLICY
   ========================= */
CREATE OR REPLACE MASKING POLICY mp_mask_borrower_id AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','RISK_ANALYST') THEN val
    ELSE CONCAT('MASKED_', RIGHT(val,2))
  END;

ALTER TABLE MART.borrower_risk_scores
  MODIFY COLUMN borrower_id
  SET MASKING POLICY GOV.mp_mask_borrower_id;

/* =========================
   REGION MAPPING TABLE
   ========================= */
CREATE OR REPLACE TABLE region_role_map (
  role_name STRING,
  region STRING
);

INSERT OVERWRITE INTO region_role_map(role_name, region) VALUES
  ('REGIONAL_MANAGER_DUBLIN', 'Dublin'),
  ('REGIONAL_MANAGER_CORK', 'Cork'),
  ('REGIONAL_MANAGER_GALWAY', 'Galway'),
  ('ACCOUNTADMIN', 'ALL'),
  ('RISK_ANALYST', 'ALL');

/* =========================
   ROW ACCESS POLICY
   ========================= */
CREATE OR REPLACE ROW ACCESS POLICY rap_region_filter AS (region_val STRING) RETURNS BOOLEAN ->
  EXISTS (
    SELECT 1
    FROM GOV.region_role_map m
    WHERE m.role_name = CURRENT_ROLE()
      AND (m.region = region_val OR m.region = 'ALL')
  );

ALTER TABLE MART.borrower_risk_scores
  ADD ROW ACCESS POLICY GOV.rap_region_filter ON (region);

/* =========================
   SECURE VIEW FOR CONSUMERS
   ========================= */
CREATE OR REPLACE SECURE VIEW MART.secure_vw_borrower_risk AS
SELECT
  as_of_date,
  region,
  borrower_id,
  mortgage_id,
  risk_score,
  risk_band
FROM MART.borrower_risk_scores;
