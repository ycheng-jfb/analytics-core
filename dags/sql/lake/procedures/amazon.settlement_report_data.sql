CREATE TABLE IF NOT EXISTS lake.amazon.selling_partner_settlement_report_data_stg (
    settlement_id INT,
    settlement_start_date VARCHAR,
    settlement_end_date VARCHAR,
    deposit_date VARCHAR,
    total_amount NUMBER(19,4),
    currency VARCHAR,
    transaction_type VARCHAR,
    order_id VARCHAR,
    merchant_order_id VARCHAR,
    adjustment_id VARCHAR,
    shipment_id VARCHAR,
    marketplace_name VARCHAR,
    amount_type VARCHAR,
    amount_description VARCHAR,
    amount NUMBER(19,4),
    fulfillment_id VARCHAR,
    posted_date DATE,
    posted_date_time VARCHAR,
    order_item_code VARCHAR,
    merchant_order_item_id VARCHAR,
    merchant_adjustment_item_id VARCHAR,
    sku VARCHAR,
    quantity_purchased INT,
    promotion_id VARCHAR
);


CREATE TABLE IF NOT EXISTS lake.amazon.selling_partner_settlement_report_data (
    settlement_id INT,
    settlement_start_date TIMESTAMP_NTZ,
    settlement_end_date TIMESTAMP_NTZ,
    deposit_date TIMESTAMP_NTZ,
    total_amount NUMBER(19,4),
    currency VARCHAR,
    transaction_type VARCHAR,
    order_id VARCHAR,
    merchant_order_id VARCHAR,
    adjustment_id VARCHAR,
    shipment_id VARCHAR,
    marketplace_name VARCHAR,
    amount_type VARCHAR,
    amount_description VARCHAR,
    amount NUMBER(19,4),
    fulfillment_id VARCHAR,
    posted_date DATE,
    posted_date_time VARCHAR,
    order_item_code VARCHAR,
    merchant_order_item_id VARCHAR,
    merchant_adjustment_item_id VARCHAR,
    sku VARCHAR,
    quantity_purchased INT,
    promotion_id VARCHAR,
    meta_create_datetime TIMESTAMP_LTZ(3)
);

ALTER SESSION SET QUERY_TAG='edm_inbound_amazon_selling_partner,amazon_selling_partner_settlement_report_data_to_snowflake';
BEGIN;

DELETE FROM lake.amazon.selling_partner_settlement_report_data_stg;


COPY INTO lake.amazon.selling_partner_settlement_report_data_stg (settlement_id, settlement_start_date, settlement_end_date, deposit_date, total_amount, currency, transaction_type, order_id, merchant_order_id, adjustment_id, shipment_id, marketplace_name, amount_type, amount_description, amount, fulfillment_id, posted_date, posted_date_time, order_item_code, merchant_order_item_id, merchant_adjustment_item_id, sku, quantity_purchased, promotion_id)
FROM '@lake_stg.public.tsos_da_int_inbound/lake/amazon.selling_partner_settlement_report_data/daily_v1'
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '\t',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_3%'
FORCE =TRUE;


DELETE
FROM lake.amazon.selling_partner_settlement_report_data
WHERE settlement_id IN (
    SELECT DISTINCT settlement_id
    FROM lake.amazon.selling_partner_settlement_report_data_stg
);


INSERT INTO lake.amazon.selling_partner_settlement_report_data
WITH cte AS (
    SELECT *
    FROM lake.amazon.selling_partner_settlement_report_data_stg
    WHERE total_amount IS NOT NULL
)
SELECT
    s.settlement_id,
    regexp_replace(t.settlement_start_date, 'UTC','',1,1) AS settlement_start_date,
    regexp_replace(t.settlement_end_date, 'UTC','',1,1) AS settlement_end_date,
    regexp_replace(t.deposit_date, 'UTC','',1,1) AS deposit_date,
    t.total_amount,
    t.currency,
    s.transaction_type,
    s.order_id,
    s.merchant_order_id,
    s.adjustment_id,
    s.shipment_id,
    s.marketplace_name,
    s.amount_type,
    s.amount_description,
    s.amount,
    s.fulfillment_id,
    s.posted_date,
    s.posted_date_time,
    s.order_item_code,
    s.merchant_order_item_id,
    s.merchant_adjustment_item_id,
    s.sku,
    s.quantity_purchased,
    s.promotion_id,
    current_timestamp()
FROM lake.amazon.selling_partner_settlement_report_data_stg s
    LEFT JOIN cte t ON t.settlement_id = s.settlement_id
WHERE s.total_amount IS NULL;

COMMIT;
