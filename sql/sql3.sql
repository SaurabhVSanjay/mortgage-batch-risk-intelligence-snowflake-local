USE WAREHOUSE MORTGAGE_ETL_WH;
USE DATABASE MORTGAGE_RISK_DB;
USE SCHEMA RAW;

COPY INTO daily_payments_raw
(payment_id,mortgage_id,borrower_id,payment_date,payment_amount,payment_status,days_past_due,principal_outstanding,source_file)
FROM (
  SELECT
    $1,$2,$3,TO_DATE($4),TO_DECIMAL($5,14,2),$6,TO_NUMBER($7),TO_DECIMAL($8,14,2),METADATA$FILENAME
  FROM @stg_mortgage/daily
)
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
ON_ERROR = CONTINUE;

COPY INTO monthly_mortgage_snapshot_raw
(snapshot_month,mortgage_id,borrower_id,current_interest_rate,current_balance,current_ltv,property_value,region,delinquency_bucket,source_file)
FROM (
  SELECT
    TO_DATE($1),$2,$3,TO_DECIMAL($4,8,4),TO_DECIMAL($5,14,2),TO_DECIMAL($6,10,4),TO_DECIMAL($7,14,2),$8,$9,METADATA$FILENAME
  FROM @stg_mortgage/monthly
)
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
ON_ERROR = CONTINUE;

COPY INTO quarterly_portfolio_perf_raw
(quarter_end_date,region,product_type,mortgages_count,delinquent_count,defaults_count,prepayment_count,avg_ltv,avg_dti,source_file)
FROM (
  SELECT
    TO_DATE($1),$2,$3,TO_NUMBER($4),TO_NUMBER($5),TO_NUMBER($6),TO_NUMBER($7),TO_DECIMAL($8,10,4),TO_DECIMAL($9,10,4),METADATA$FILENAME
  FROM @stg_mortgage/quarterly
)
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
ON_ERROR = CONTINUE;
