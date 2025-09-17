BEGIN;

DELETE FROM lake_stg.oracle_ebs.chargeback_us_stg;

COPY INTO lake_stg.oracle_ebs.chargeback_us_stg
FROM (
        SELECT
            $1 AS source,
            $2 AS gl_period,
            $3 AS gl_account,
            $4 AS settlement_date,
            $5 AS merchant_id,
            $6 AS chargeback_amt,
            $7 AS order_id,
            $8 AS order_date_placed,
            $9 AS shipping_country_code,
            $10 AS store_id,
            $11 AS store_credit_id,
            $12 AS membership_token_id,
            $13 AS membership_brand,
            $14 AS receipt_number,
            $15 AS receivables_trx_name,
            $16 AS creation_date,
            metadata$filename as file_name,
            TRIM(REGEXP_REPLACE(metadata$filename, '[a-z//A-z/./-]', ''))::INT AS file_date,
            $17 AS activity,
            $18 AS reason_code,
            $19 AS reason_description,
            $20 AS cycle,
            $21 AS merchant_grouping_id,
            $22 AS issuing_bank,
            $23 AS payment_type
            FROM '@lake_stg.public.tsos_da_int_vendor/inbound/svc_oracle_ebs/lake.oracle_ebs.chargeback/us/')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = ',',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = (''),
    TIMESTAMP_FORMAT = 'DD-MON-YYYY HH:MI:SS'
)
ON_ERROR = 'SKIP_FILE_1%'
;

CREATE OR REPLACE TEMP TABLE _gl_periods_base AS
SELECT
    gl_period,
    MAX(file_date) AS file_date
FROM lake_stg.oracle_ebs.chargeback_us_stg
GROUP BY gl_period;

DELETE FROM lake.oracle_ebs.chargeback_us us
USING _gl_periods_base gb
WHERE us.gl_period = gb.gl_period;

INSERT INTO lake.oracle_ebs.chargeback_us
(
    source,
    gl_period,
    gl_account,
    settlement_date,
    merchant_id,
    chargeback_amt,
    order_id,
    order_date_placed,
    shipping_country_code,
    store_id,
    store_credit_id,
    membership_token_id,
    membership_brand,
    receipt_number,
    receivables_trx_name,
    creation_date,
    activity,
    reason_code,
    reason_description,
    cycle,
    merchant_grouping_id,
    issuing_bank,
    payment_type,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    stg.source,
    stg.gl_period,
    stg.gl_account,
    stg.settlement_date,
    stg.merchant_id,
    stg.chargeback_amt,
    stg.order_id,
    stg.order_date_placed,
    stg.shipping_country_code,
    stg.store_id,
    stg.store_credit_id,
    stg.membership_token_id,
    stg.membership_brand,
    stg.receipt_number,
    stg.receivables_trx_name,
    stg.creation_date,
    stg.activity,
    stg.reason_code,
    stg.reason_description,
    stg.cycle,
    stg.merchant_grouping_id,
    stg.issuing_bank,
    stg.payment_type,
    hash(stg.*) AS meta_row_hash,
    current_timestamp AS meta_create_datetime,
    current_timestamp AS meta_update_datetime
FROM lake_stg.oracle_ebs.chargeback_us_stg stg
JOIN _gl_periods_base base
    ON base.gl_period = stg.gl_period
    AND base.file_date = stg.file_date;

COMMIT;
