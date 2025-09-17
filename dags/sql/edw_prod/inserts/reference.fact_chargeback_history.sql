/*
This script is to pull the historical data for chargeback before september, 2022(sourcing from ultramerchant db).
From sep, 2022 we are sourcing the chargeback data from oracle to make sure it matches with our bank books.

This script is using reference.fact_chargeback_archive(cloned copy of fact_chargeback_um, while retiring it on Nov 29th, 2023),
this is the only source of historical data of chargeback sourcing data from ultra merchant.

CREATE OR REPLACE TABLE reference.fact_chargeback_archive
CLONE stg.fact_chargeback_um;

Please do not drop or truncate this object, doing so, we will lose all the chargeback data before september, 2022
*/

CREATE OR REPLACE TEMP TABLE _oracle_chargeback_us
AS
SELECT
       a.order_id
FROM lake.oracle_ebs.chargeback_us a
WHERE TO_DATE(a.gl_period, 'Mon-YY') >= '2022-09-01';

CREATE OR REPLACE TEMP table _oracle_chargeback_eu
AS
SELECT
       a.order_id
FROM lake.oracle_ebs.chargeback_eu a
WHERE to_date(a.gl_period, 'Mon-YY') >= '2022-09-01';

INSERT INTO reference.fact_chargeback_history
SELECT DISTINCT
    fc.order_id,
    fc.meta_original_order_id,
    fc.customer_id,
    fc.store_id,
    fc.chargeback_reason_key,
    fc.chargeback_status_key,
    fc.chargeback_payment_key,
    fc.chargeback_datetime,
    fc.chargeback_quantity,
    fc.chargeback_date_eur_conversion_rate,
    fc.chargeback_date_usd_conversion_rate,
    fc.chargeback_local_amount,
    fc.chargeback_payment_transaction_local_amount,
    fc.chargeback_tax_local_amount,
    fc.effective_vat_rate,
    fc.is_deleted,
    fc.meta_row_hash,
    fc.meta_create_datetime,
    fc.meta_update_datetime
FROM reference.fact_chargeback_archive fc
LEFT JOIN _oracle_chargeback_us us
    ON fc.meta_original_order_id = us.order_id
LEFT JOIN _oracle_chargeback_eu eu
    ON fc.meta_original_order_id = eu.order_id
WHERE convert_timezone('America/Los_Angeles',fc.chargeback_datetime) < '2022-09-01'
AND fc.order_id NOT IN (-1)
AND eu.order_id IS NULL
AND us.order_id IS NULL
AND NOT fc.is_deleted;
