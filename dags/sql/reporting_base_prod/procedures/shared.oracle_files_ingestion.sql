CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.token_recon
(
    record_type           VARCHAR,
    operating_unit        VARCHAR,
    org_id                VARCHAR,
    store_credit_id       VARCHAR,
    order_id              VARCHAR,
    refund_id             VARCHAR,
    token_id              VARCHAR,
    trx_date              VARCHAR,
    trx_number            VARCHAR,
    amount_original       VARCHAR,
    amount_remaining      VARCHAR,
    party_name            VARCHAR,
    store_id              VARCHAR,
    order_amount          VARCHAR,
    vat_amount            VARCHAR,
    vat_rate              VARCHAR,
    currency_code         VARCHAR,
    calc_amount_remaining VARCHAR,
    vat_prepaid_flag      VARCHAR,
    receivable_account    VARCHAR,
    customer_trx_id       VARCHAR,
    exchange_rate         VARCHAR,
    trx_type              VARCHAR,
    record_count          VARCHAR,
    min_trx_date          VARCHAR,
    max_trx_date          VARCHAR,
    sum_amount_orig       VARCHAR,
    sum_amount_remain     VARCHAR,
    extract_datetime      VARCHAR,
    file_name             VARCHAR
);

COPY INTO reporting_base_prod.shared.token_recon
    FROM (select $1 AS record_type,
       $2 AS operating_unit,
       $3 AS org_id,
       $4 AS store_credit_id,
       $5 AS order_id,
       $6 AS refund_id,
       $7 AS token_id,
       $8 AS trx_date,
       $9 AS trx_number,
       $10 AS amount_original,
       $11 AS amount_remaining,
       $12 AS party_name,
       $13 AS store_id,
       $14 AS order_amount,
       $15 AS vat_amount,
       $16 AS vat_rate,
       $17 AS currency_code,
       $18 AS calc_amount_remaining,
       $19 AS vat_prepaid_flag,
       $20 AS receivable_account,
       $21 AS customer_trx_id,
       $22 AS exchange_rate,
       $23 AS trx_type,
       $24 AS record_count,
       $25 AS min_trx_date,
       $26 AS max_trx_date,
       $27 AS sum_amount_orig,
       $28 AS sum_amount_remain,
       $29 AS extract_datetime,
       metadata$filename as file_name
    FROM '@lake_stg.public.tsos_da_int_vendor/inbound/svc_oracle_ebs/lake.oracle_ebs.credits/TOKEN_RECON')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1
)
ON_ERROR = 'SKIP_FILE_5%';

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.credit_recon
(
    record_type           VARCHAR,
    operating_unit        VARCHAR,
    org_id                VARCHAR,
    store_credit_id       VARCHAR,
    order_id              VARCHAR,
    refund_id             VARCHAR,
    trx_date              VARCHAR,
    trx_number            VARCHAR,
    amount_original       VARCHAR,
    amount_remaining      VARCHAR,
    fixed_vs_variable     VARCHAR,
    credit_reason         VARCHAR,
    party_name            VARCHAR,
    store_id              VARCHAR,
    order_amount          VARCHAR,
    vat_amount            VARCHAR,
    vat_rate              VARCHAR,
    currency_code         VARCHAR,
    calc_amount_remaining VARCHAR,
    vat_prepaid_flag      VARCHAR,
    receivable_account    VARCHAR,
    customer_trx_id       VARCHAR,
    exchange_rate         VARCHAR,
    trx_type              VARCHAR,
    record_count          VARCHAR,
    min_trx_date          VARCHAR,
    max_trx_date          VARCHAR,
    sum_amount_original   VARCHAR,
    sum_amount_remaining  VARCHAR,
    sum_order_amount      VARCHAR,
    sum_vat_amount        VARCHAR,
    extract_datetime      VARCHAR,
    file_name             VARCHAR
);

COPY INTO reporting_base_prod.shared.credit_recon
    FROM (select $1 AS record_type,
       $2 AS operating_unit          ,
       $3 AS org_id                  ,
       $4 AS store_credit_id         ,
       $5 AS order_id                ,
       $6 AS refund_id               ,
       $7 AS trx_date                ,
       $8 AS trx_number              ,
       $9 AS amount_original         ,
       $10 AS  amount_remaining      ,
       $11 AS  fixed_vs_variable     ,
       $12 AS  credit_reason         ,
       $13 AS  party_name            ,
       $14 AS  store_id              ,
       $15 AS  order_amount          ,
       $16 AS  vat_amount            ,
       $17 AS  vat_rate              ,
       $18 AS  currency_code         ,
       $19 AS  calc_amount_remaining ,
       $20 AS  vat_prepaid_flag      ,
       $21 AS  receivable_account    ,
       $22 AS  customer_trx_id       ,
       $23 AS  exchange_rate         ,
       $24 AS  trx_type              ,
       $25 AS  record_count          ,
       $26 AS  min_trx_date          ,
       $27 AS  max_trx_date          ,
       $28 AS  sum_amount_original   ,
       $29 AS  sum_amount_remaining  ,
       $30 AS  sum_order_amount      ,
       $31 AS  sum_vat_amount        ,
       $32 AS  extract_datetime      ,
       metadata$filename as file_name
    FROM '@lake_stg.public.tsos_da_int_vendor/inbound/svc_oracle_ebs/lake.oracle_ebs.credits/CREDIT_RECON')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1
)
ON_ERROR = 'SKIP_FILE_5%';

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.noncashbalance
(
    record_type       VARCHAR,
    noncash_credit_id VARCHAR,
    credit_type       VARCHAR,
    oracle_balance    VARCHAR,
    extract_datetime  VARCHAR,
    currency_code     VARCHAR,
    file_name         VARCHAR
);

COPY INTO reporting_base_prod.shared.noncashbalance
    FROM (select $1 AS record_type,
       $2 AS noncash_credit_id,
       $3 AS credit_type,
       $4 AS oracle_balance,
       $5 AS extract_datetime,
       $6 AS currency_code,
       metadata$filename as file_name
    FROM '@lake_stg.public.tsos_da_int_vendor/inbound/svc_oracle_ebs/lake.oracle_ebs.credits/NONCASH')
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1
)
ON_ERROR = 'SKIP_FILE_5%';

DELETE
FROM reporting_base_prod.shared.noncashbalance
WHERE record_type <> 'D';
