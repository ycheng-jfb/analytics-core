
BEGIN;
DELETE FROM lake_stg.oracle_ebs.chargeback_eu_stg;

COPY INTO lake_stg.oracle_ebs.chargeback_eu_stg
FROM (
        SELECT
            $1 AS source,
            $2 AS gl_period,
            $3 AS sale_gl_account,
            $4 AS vat_gl_account,
            $5 AS settlement_date,
            $6 AS merchant_account,
            $7 AS chargeback_amt,
            $8 AS net_amount,
            $9 AS vat_amount,
            $10 AS order_id,
            $11 AS order_date_placed,
            $12 AS shipping_country_code,
            $13 AS store_id,
            $14 AS store_credit_id,
            $15 AS membership_token_id,
            $16 AS membership_brand,
            $17 AS receipt_number,
            $18 AS receivables_trx_name,
            $19 AS creation_date,
            metadata$filename as file_name,
            TRIM(REGEXP_REPLACE(metadata$filename, '[a-z//A-z/./-]', ''))::INT AS file_date,
            $20 AS activity,
            $21 AS reason_code,
            $22 AS reason_description,
            $23 AS cycle,
            $24 AS merchant_grouping_id,
            $25 AS issuing_bank,
            $26 AS payment_type
    FROM '@lake_stg.public.tsos_da_int_vendor/inbound/svc_oracle_ebs/lake.oracle_ebs.chargeback/eu/')
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
FROM lake_stg.oracle_ebs.chargeback_eu_stg
GROUP BY gl_period;

DELETE FROM lake.oracle_ebs.chargeback_eu eu
USING _gl_periods_base gb
WHERE eu.gl_period = gb.gl_period;


INSERT INTO lake.oracle_ebs.chargeback_eu
(
    source,
    gl_period,
    sale_gl_account,
    vat_gl_account,
    settlement_date,
    merchant_account,
    chargeback_amt,
    net_amount,
    vat_amount,
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
    stg.sale_gl_account,
    stg.vat_gl_account,
    stg.settlement_date,
    stg.merchant_account,
    stg.chargeback_amt,
    stg.net_amount,
    stg.vat_amount,
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
FROM lake_stg.oracle_ebs.chargeback_eu_stg stg
JOIN _gl_periods_base base
    ON base.gl_period = stg.gl_period
    AND base.file_date = stg.file_date;

COMMIT;
