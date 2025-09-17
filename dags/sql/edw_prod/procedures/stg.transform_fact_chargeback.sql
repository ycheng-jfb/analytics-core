SET target_table = 'stg.fact_chargeback';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_lake_oracle_ebs_chargeback_us = (SELECT stg.udf_get_watermark($target_table, 'lake.oracle_ebs.chargeback_us'));
SET wm_lake_oracle_ebs_chargeback_eu = (SELECT stg.udf_get_watermark($target_table, 'lake.oracle_ebs.chargeback_eu'));
SET wm_edw_stg_fact_order = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order'));
SET wm_edw_stg_fact_activation = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_activation'));
SET wm_edw_stg_dim_store = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_store'));
SET wm_reference_currency_exchange_rate = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.currency_exchange_rate'));
SET wm_lake_ultra_merchant_order_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_detail'));
SET wm_lake_ultra_merchant_order_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_classification'));
SET wm_edw_reference_test_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.test_customer'));

/*
SELECT
    $wm_lake_oracle_ebs_chargeback_us,
    $wm_lake_oracle_ebs_chargeback_eu,
    $wm_edw_stg_fact_order,
    $wm_edw_stg_fact_activation,
    $wm_edw_stg_fact_activation,
    $wm_edw_stg_dim_store;
*/

--order_id in base table is not concatenated with company_id
CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__order_base (order_id INT);

-- Full Refresh
INSERT INTO _fact_chargeback_ebs__order_base (order_id)
SELECT DISTINCT order_id
FROM
(
    SELECT
      ebs.order_id
    FROM lake.oracle_ebs.chargeback_us ebs
    WHERE ebs.order_id IS NOT NULL
    UNION ALL
    SELECT
      ebs.order_id
    FROM lake.oracle_ebs.chargeback_eu ebs
    WHERE ebs.order_id IS NOT NULL
    UNION ALL
    SELECT
        fch.meta_original_order_id
    FROM reference.fact_chargeback_history fch
    WHERE fch.meta_original_order_id <> -1
 )
WHERE $is_full_refresh = TRUE
ORDER BY order_id;

-- Incremental Refresh
INSERT INTO _fact_chargeback_ebs__order_base (order_id)
SELECT DISTINCT incr.order_id
FROM (

    SELECT
      ebs.order_id
    FROM lake.oracle_ebs.chargeback_us ebs
    WHERE ebs.order_id IS NOT NULL
    AND ebs.meta_update_datetime > $wm_lake_oracle_ebs_chargeback_us

    UNION ALL

    SELECT
      ebs.order_id
    FROM lake.oracle_ebs.chargeback_eu ebs
    WHERE ebs.order_id IS NOT NULL
    AND ebs.meta_update_datetime > $wm_lake_oracle_ebs_chargeback_eu

    UNION ALL

    SELECT
      ebs.order_id
    FROM lake.oracle_ebs.chargeback_us ebs
    JOIN  stg.fact_order AS fo
        ON fo.meta_original_order_id = ebs.order_id
    JOIN reference.test_customer AS tc
        ON tc.customer_id = fo.customer_id
    WHERE  ebs.order_id IS NOT NULL
    AND tc.meta_update_datetime > $wm_edw_reference_test_customer

    UNION ALL

    SELECT
      ebs.order_id
    FROM lake.oracle_ebs.chargeback_eu ebs
    JOIN  stg.fact_order AS fo
        ON fo.meta_original_order_id = ebs.order_id
    JOIN reference.test_customer AS tc
        ON tc.customer_id = fo.customer_id
    WHERE  ebs.order_id IS NOT NULL
    AND tc.meta_update_datetime > $wm_edw_reference_test_customer

    UNION ALL

    SELECT fo.meta_original_order_id AS order_id
    FROM stg.fact_order AS fo
    JOIN stg.dim_store AS ds
        ON ds.store_id = fo.store_id
    WHERE ds.meta_update_datetime > $wm_edw_stg_dim_store


    UNION ALL

    SELECT  fo.meta_original_order_id  AS order_id
    FROM stg.fact_order AS fo
    LEFT JOIN stg.fact_activation AS fa
        ON fa.customer_id = fo.customer_id
    WHERE fo.meta_update_datetime > $wm_edw_stg_fact_order
        OR fa.meta_update_datetime > $wm_edw_stg_fact_activation

    UNION ALL

    SELECT order_id
    FROM stg.fact_chargeback
    WHERE chargeback_datetime > (SELECT MIN(effective_start_datetime)
                                 FROM reference.currency_exchange_rate
                                 WHERE meta_update_datetime > $wm_reference_currency_exchange_rate)

    UNION ALL

    SELECT od.order_id
    FROM lake_consolidated.ultra_merchant.order_detail AS od
    WHERE od.meta_update_datetime > $wm_lake_ultra_merchant_order_detail
        AND od.name = 'original_store_id'

    UNION ALL

    SELECT oc.order_id
    FROM lake_consolidated.ultra_merchant.order_classification AS oc
    WHERE oc.meta_update_datetime > $wm_lake_ultra_merchant_order_classification
        AND oc.order_type_id = 40

    UNION ALL

    SELECT  e.meta_original_order_id AS order_id
    FROM excp.fact_chargeback AS e
    WHERE e.meta_is_current_excp
    AND e.meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.order_id;
-- SELECT * FROM _fact_chargeback_ebs__order_base;

CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__store_brands AS
SELECT DISTINCT store_brand_abbr, company_id FROM stg.dim_store;

CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__chargeback_eu AS
SELECT
  CAST(ebs.order_id||COALESCE(ods.company_id, ds.company_id, ds2.company_id) AS INT) AS order_id,
  ob.order_id as meta_original_order_id,
  ebs.settlement_date,
  SUM(ebs.net_amount) AS net_amount,
  SUM(ebs.chargeback_amt) AS chargeback_amt,
  SUM(ebs.vat_amount) AS vat_amount
FROM lake.oracle_ebs.chargeback_eu ebs
LEFT JOIN reference.gl_store_brand gsb
    ON NVL(split_part(ebs.sale_gl_account,'.',1), split_part(ebs.vat_gl_account,'.',1)) = gsb.gl_company_code
LEFT JOIN _fact_chargeback_ebs__store_brands ds
    ON ds.store_brand_abbr = gsb.reporting_store_brand
LEFT JOIN reference.gl_trx_store gts
    ON LOWER(ebs.receivables_trx_name) = LOWER(gts.transaction_name)
LEFT JOIN _fact_chargeback_ebs__store_brands ds2
    ON ds2.store_brand_abbr = gts.store_brand
LEFT JOIN stg.dim_store ods
    ON ods.store_id = ebs.store_id
JOIN _fact_chargeback_ebs__order_base ob
    ON ebs.order_id = ob.order_id
WHERE ebs.order_id IS NOT NULL
GROUP BY
    CAST(ebs.order_id||COALESCE(ods.company_id, ds.company_id, ds2.company_id) AS INT),
    ob.order_id,
    ebs.settlement_date;

CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__chargeback_us AS
SELECT
  CAST(ebs.order_id||COALESCE(ods.company_id, ds.company_id, ds2.company_id) AS INT) AS order_id,
  ob.order_id AS meta_original_order_id,
  ebs.settlement_date,
  SUM(ebs.chargeback_amt) AS chargeback_amt
FROM lake.oracle_ebs.chargeback_us ebs
LEFT JOIN reference.gl_store_brand gsb
    ON split_part(ebs.gl_account,'.',1) = gsb.gl_company_code
LEFT JOIN _fact_chargeback_ebs__store_brands ds
    ON ds.store_brand_abbr = gsb.reporting_store_brand
LEFT JOIN reference.gl_trx_store gts
    ON LOWER(ebs.receivables_trx_name) = LOWER(gts.transaction_name)
LEFT JOIN _fact_chargeback_ebs__store_brands ds2
    ON ds2.store_brand_abbr = gts.store_brand
LEFT JOIN stg.dim_store ods
    ON ods.store_id = ebs.store_id
JOIN _fact_chargeback_ebs__order_base ob
    ON ebs.order_id = ob.order_id
WHERE ebs.order_id IS NOT NULL
GROUP BY
    CAST(ebs.order_id||COALESCE(ods.company_id, ds.company_id, ds2.company_id) AS INT),
    ob.order_id,
    ebs.settlement_date;

CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__chargeback_reason AS
SELECT fc.meta_original_order_id,
       fc.settlement_date,
       dcr.chargeback_reason_key
FROM (SELECT us.order_id               AS meta_original_order_id,
             us.settlement_date,
             LOWER(us.reason_description) AS chargeback_reason
      FROM lake.oracle_ebs.chargeback_us us
               JOIN _fact_chargeback_ebs__order_base base
                    ON us.order_id = base.order_id
      UNION
      SELECT eu.order_id               AS meta_original_order_id,
             eu.settlement_date,
             LOWER(eu.reason_description) AS chargeback_reason
      FROM lake.oracle_ebs.chargeback_eu eu
               JOIN _fact_chargeback_ebs__order_base base
                    ON eu.order_id = base.order_id) AS fc
         JOIN stg.dim_chargeback_reason dcr
              ON dcr.chargeback_reason = fc.chargeback_reason
QUALIFY ROW_NUMBER() OVER (PARTITION BY fc.meta_original_order_id, fc.settlement_date ORDER BY fc.chargeback_reason DESC) =
        1;


CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__chargeback_payment AS
SELECT fc.meta_original_order_id,
       fc.settlement_date,
       dcp.chargeback_payment_key
FROM (SELECT us.order_id               AS meta_original_order_id,
             us.settlement_date,
             COALESCE(CASE WHEN LOWER(us.source) = 'payal' OR LOWER(us.source) LIKE 'paypal%'
                            THEN 'paypal'
                            ELSE LOWER(us.source) END, 'unknown') AS chargeback_payment_processor,
             COALESCE(LOWER(us.payment_type), 'unknown') AS chargeback_payment_method,
             COALESCE(LOWER(us.issuing_bank), 'unknown') AS chargeback_payment_bank
      FROM lake.oracle_ebs.chargeback_us us
               JOIN _fact_chargeback_ebs__order_base base
                    ON us.order_id = base.order_id
      UNION
      SELECT eu.order_id               AS meta_original_order_id,
             eu.settlement_date,
             COALESCE(CASE WHEN LOWER(eu.source) = 'payal' OR LOWER(eu.source) LIKE 'paypal%'
                            THEN 'paypal'
                            ELSE LOWER(eu.source) END, 'unknown') AS chargeback_payment_processor,
             COALESCE(LOWER(eu.payment_type), 'unknown') AS chargeback_payment_method,
             COALESCE(LOWER(eu.issuing_bank), 'unknown') AS chargeback_payment_bank
      FROM lake.oracle_ebs.chargeback_eu eu
               JOIN _fact_chargeback_ebs__order_base base
                    ON eu.order_id = base.order_id) AS fc
         JOIN stg.dim_chargeback_payment dcp
              ON LOWER(dcp.chargeback_payment_processor) = fc.chargeback_payment_processor
                AND LOWER(dcp.chargeback_payment_method) = fc.chargeback_payment_method
                AND LOWER(dcp.chargeback_payment_bank) = fc.chargeback_payment_bank
                AND LOWER(dcp.chargeback_payment_type) = 'unknown'
QUALIFY ROW_NUMBER() OVER (PARTITION BY fc.meta_original_order_id, fc.settlement_date ORDER BY fc.chargeback_payment_method DESC) =
        1;

CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__first_chargeback_reason AS
SELECT meta_original_order_id,
       settlement_date,
       chargeback_reason_key AS first_chargeback_reason_key
FROM _fact_chargeback_ebs__chargeback_reason
QUALIFY ROW_NUMBER() OVER (PARTITION BY meta_original_order_id ORDER BY settlement_date) =1;


CREATE OR REPLACE TEMP table _fact_chargeback_ebs__gl_store_map as
SELECT
    store_id,
    store_brand_abbr AS store_brand,
    store_country,
    -1 * store_id AS customer_id
FROM stg.dim_store
WHERE is_core_Store
    AND store_type = 'Online'
    AND store_country IS NOT NULL -- excluding dummy FL stores
    AND store_id NOT IN (116, 44)
QUALIFY ROW_NUMBER() OVER(PARTITION BY store_brand, store_country ORDER BY store_id) = 1;

-- Get chargebacks without order_id
CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__chargeback_without_order AS
SELECT
    NVL(gsm.customer_id, gsm2.customer_id) AS customer_id,
    NVL(gsm.store_id, gsm2.store_id) AS store_id,
    NVL(gsm.store_country, gsm2.store_country)  AS store_country,
    NVL(ebs.settlement_date, ebs.creation_date) AS chargeback_date,
    SUM(NVL(ebs.net_amount, 0)) AS chargeback_local_amount,
    SUM(NVL(ebs.chargeback_amt, 0)) AS chargeback_payment_transaction_local_amount,
    SUM(NVL(ebs.vat_amount, 0))  AS chargeback_vat_local_amount
FROM lake.oracle_ebs.chargeback_eu ebs
LEFT JOIN reference.gl_store_brand gsb
    ON NVL(split_part(ebs.sale_gl_account,'.',1), split_part(ebs.vat_gl_account,'.',1)) = gsb.gl_company_code
LEFT JOIN reference.gl_store_country gsc
    ON NVL(split_part(ebs.sale_gl_account,'.',3), split_part(ebs.vat_gl_account,'.',3)) = gsc.gl_country_code
LEFT JOIN reference.gl_trx_store gts
    ON LOWER(ebs.receivables_trx_name) = LOWER(gts.transaction_name)
LEFT JOIN _fact_chargeback_ebs__gl_store_map gsm
    ON gsm.store_brand = gsb.reporting_store_brand
    AND gsm.store_country = gsc.reporting_store_country
LEFT JOIN _fact_chargeback_ebs__gl_store_map gsm2
    ON gsm2.store_brand = gts.store_brand
    AND gsm2.store_country = gts.store_country
WHERE (ebs.meta_update_datetime > $wm_lake_oracle_ebs_chargeback_eu OR $is_full_refresh)
AND ebs.order_id IS NULL
GROUP BY
    NVL(gsm.customer_id, gsm2.customer_id),
    NVL(gsm.store_id, gsm2.store_id),
    NVL(gsm.store_country, gsm2.store_country),
    NVL(ebs.settlement_date, ebs.creation_date)

UNION ALL

SELECT
    NVL(gsm.customer_id, gsm2.customer_id) AS customer_id,
    NVL(gsm.store_id, gsm2.store_id) AS store_id,
    NVL(gsm.store_country, gsm2.store_country)  AS store_country,
    NVL(ebs.settlement_date, ebs.creation_date) AS chargeback_date,
    SUM(NVL(ebs.chargeback_amt, 0)) AS chargeback_local_amount,
    SUM(NVL(ebs.chargeback_amt,0)) AS chargeback_payment_transaction_local_amount,
    0 AS chargeback_vat_local_amount
FROM lake.oracle_ebs.chargeback_us ebs
LEFT JOIN reference.gl_store_brand gsb
    ON split_part(ebs.gl_account,'.',1) = gsb.gl_company_code
LEFT JOIN reference.gl_store_country gsc
    ON split_part(ebs.GL_ACCOUNT,'.',3) = gsc.gl_country_code
LEFT JOIN reference.gl_trx_store gts
    ON LOWER(ebs.receivables_trx_name) = LOWER(gts.transaction_name)
LEFT JOIN _fact_chargeback_ebs__gl_store_map gsm
    ON gsm.store_brand = gsb.reporting_store_brand
    AND gsm.store_country = gsc.reporting_store_country
LEFT JOIN _fact_chargeback_ebs__gl_store_map gsm2
    ON gsm2.store_brand = gts.store_brand
    AND gsm2.store_country = gts.store_country
WHERE (ebs.meta_update_datetime > $wm_lake_oracle_ebs_chargeback_eu OR $is_full_refresh)
AND ebs.order_id IS NULL
GROUP BY
    NVL(gsm.customer_id, gsm2.customer_id),
    NVL(gsm.store_id, gsm2.store_id),
    NVL(gsm.store_country, gsm2.store_country),
    NVL(ebs.settlement_date, ebs.creation_date);


CREATE OR REPLACE TEMPORARY TABLE _fact_chargeback_ebs__stg_fact_chargeback (
	order_id NUMBER(38,0),
    meta_original_order_id NUMBER(38,0),
	customer_id NUMBER(38,0),
	store_id NUMBER(38,0),
	chargeback_status VARCHAR,
	chargeback_payment_key NUMBER(38,0),
    chargeback_reason_key NUMBER(38,0),
    first_chargeback_reason_key NUMBER(38,0),
	chargeback_date DATE,
	chargeback_local_datetime TIMESTAMP_TZ(3),
	iso_currency_code VARCHAR,
	chargeback_local_amount NUMBER(19,4),
	chargeback_payment_transaction_local_amount NUMBER(19,4),
	chargeback_tax_local_amount NUMBER(19,4),
	chargeback_vat_local_amount NUMBER(19,4),
	effective_vat_rate NUMBER(18,6),
	chargeback_date_usd_conversion_rate NUMBER(18,6),
	chargeback_date_eur_conversion_rate NUMBER(18,6),
    source VARCHAR
);

INSERT INTO _fact_chargeback_ebs__stg_fact_chargeback
SELECT
    cu.order_id,
    cu.meta_original_order_id,
    fo.customer_id,
    fo.store_id,
    'Success' As chargeback_status,
    cp.chargeback_payment_key,
    IFNULL(cr.chargeback_reason_key, -1) AS chargeback_reason_key,
    IFNULL(fcr.first_chargeback_reason_key, -1) AS first_chargeback_reason_key,
    cu.settlement_date AS chargeback_date,
    NULL AS chargeback_local_datetime,
    NULL AS iso_currency_code,
    CASE
        WHEN cu.chargeback_amt > 0 THEN cu.chargeback_amt - NVL(fo.tax_credit_local_amount, 0) - NVL(fo.tax_cash_local_amount, 0)
        WHEN cu.chargeback_amt < 0 THEN cu.chargeback_amt + NVL(fo.tax_credit_local_amount, 0) + NVL(fo.tax_cash_local_amount, 0)
        ELSE 0
    END AS chargeback_local_amount,
    NVL(cu.chargeback_amt, 0) AS chargeback_payment_transaction_local_amount,
    CASE
        WHEN cu.chargeback_amt > 0 THEN NVL(fo.tax_credit_local_amount, 0)  + NVL(fo.tax_cash_local_amount, 0)
        WHEN cu.chargeback_amt < 0 THEN -1 * (NVL(fo.tax_credit_local_amount, 0)  + NVL(fo.tax_cash_local_amount, 0))
        ELSE 0
    END AS chargeback_tax_local_amount,
    0.00 AS chargeback_vat_local_amount,
    fo.effective_vat_rate,
    NULL AS chargeback_date_usd_conversion_rate,
    NULL AS chargeback_date_eur_conversion_rate,
    'oracle_ebs' AS source
FROM _fact_chargeback_ebs__chargeback_us cu
JOIN stg.fact_order fo
    ON cu.order_id = fo.order_id
LEFT JOIN _fact_chargeback_ebs__chargeback_reason cr
    ON cu.meta_original_order_id = cr.meta_original_order_id
    AND cu.settlement_date = cr.settlement_date
LEFT JOIN _fact_chargeback_ebs__chargeback_payment cp
    ON cu.meta_original_order_id = cp.meta_original_order_id
    AND cu.settlement_date = cp.settlement_date
LEFT JOIN _fact_chargeback_ebs__first_chargeback_reason fcr
    ON cu.meta_original_order_id = fcr.meta_original_order_id

UNION ALL

SELECT
    cu.order_id,
    cu.meta_original_order_id,
    fo.customer_id,
    fo.store_id,
    'Success' AS chargeback_status,
    cp.chargeback_payment_key,
    IFNULL(cr.chargeback_reason_key, -1) AS chargeback_reason_key,
    IFNULL(fcr.first_chargeback_reason_key, -1) AS first_chargeback_reason_key,
    cu.settlement_date AS chargeback_date,
    NULL AS chargeback_local_datetime,
    NULL AS iso_currency_code,
    NVL(cu.net_amount, 0) AS chargeback_local_amount,
    NVL(cu.chargeback_amt, 0) AS chargeback_payment_transaction_local_amount,
    0 AS chargeback_tax_local_amount,
    NVL(cu.vat_amount, 0) AS chargeback_vat_local_amount,
    fo.effective_vat_rate,
    NULL AS chargeback_date_usd_conversion_rate,
    NULL AS chargeback_date_eur_conversion_rate,
    'oracle_ebs' AS source
FROM _fact_chargeback_ebs__chargeback_eu cu
JOIN stg.fact_order fo
    ON cu.order_id = fo.order_id
LEFT JOIN _fact_chargeback_ebs__chargeback_reason cr
    ON cu.meta_original_order_id = cr.meta_original_order_id
    AND cu.settlement_date = cr.settlement_date
LEFT JOIN _fact_chargeback_ebs__chargeback_payment cp
    ON cu.meta_original_order_id = cp.meta_original_order_id
    AND cu.settlement_date = cp.settlement_date
LEFT JOIN _fact_chargeback_ebs__first_chargeback_reason fcr
    ON cu.meta_original_order_id = fcr.meta_original_order_id

UNION ALL

SELECT
    -1 AS order_id,
    -1 AS meta_original_order_id,
    cwo.customer_id,
    cwo.store_id,
    'Success' AS chargeback_status,
    -1 AS chargeback_payment_key,
    -1 AS chargeback_reason_key,
    -1 AS first_chargeback_reason_key,
    cwo.chargeback_date AS chargeback_date,
    NULL AS chargeback_local_datetime,
    NULL AS iso_currency_code,
    NVL(cwo.chargeback_local_amount, 0) AS chargeback_local_amount,
    NVL(cwo.chargeback_payment_transaction_local_amount, 0) AS chargeback_payment_transaction_local_amount,
    0 AS chargeback_tax_local_amount,
    NVL(cwo.chargeback_vat_local_amount, 0) AS chargeback_vat_local_amount,
    NVL(vrh.rate, 0) AS effective_vat_rate,
    NULL AS chargeback_date_usd_conversion_rate,
    NULL AS chargeback_date_eur_conversion_rate,
    'oracle_ebs' AS source
FROM _fact_chargeback_ebs__chargeback_without_order cwo
LEFT JOIN reference.vat_rate_history AS vrh
    ON vrh.country_code = cwo.store_country
    AND cwo.chargeback_date::DATE BETWEEN vrh.start_date AND vrh.expires_date;

UPDATE _fact_chargeback_ebs__stg_fact_chargeback AS stg
SET stg.chargeback_local_datetime = CONVERT_TIMEZONE(st.time_zone, stg.chargeback_date::TIMESTAMP_TZ(3))
FROM reference.store_timezone AS st
WHERE st.store_id = stg.store_id;

-- update currency code
UPDATE _fact_chargeback_ebs__stg_fact_chargeback AS stg
SET stg.iso_currency_code = sc.currency_code
FROM reference.store_currency AS sc
WHERE sc.store_id = stg.store_id
    AND stg.chargeback_local_datetime::TIMESTAMP_LTZ BETWEEN sc.effective_start_date AND sc.effective_end_date;

-- update exchange rates
UPDATE _fact_chargeback_ebs__stg_fact_chargeback AS stg
SET
    stg.chargeback_date_usd_conversion_rate = COALESCE(chargeback_er.exchange_rate, 1),
    stg.chargeback_date_eur_conversion_rate = COALESCE(eur_chargeback_er.exchange_rate, 1)
FROM _fact_chargeback_ebs__stg_fact_chargeback AS f
    LEFT JOIN reference.currency_exchange_rate_by_date AS chargeback_er
        ON f.iso_currency_code = chargeback_er.src_currency
        AND UPPER(chargeback_er.dest_currency) = 'USD'
        AND f.chargeback_local_datetime::TIMESTAMP_LTZ::DATE= chargeback_er.rate_date_pst
    LEFT JOIN reference.currency_exchange_rate_by_date AS eur_chargeback_er
        ON f.iso_currency_code = eur_chargeback_er.src_currency
        AND UPPER(eur_chargeback_er.dest_currency) = 'EUR'
        AND f.chargeback_local_datetime::TIMESTAMP_LTZ::DATE = eur_chargeback_er.rate_date_pst
WHERE f.order_id = stg.order_id
AND f.chargeback_date = stg.chargeback_date
AND f.store_id = stg.store_id;

CREATE OR REPLACE TEMPORARY TABLE _fact_chargeback_ebs__dropship AS
SELECT ob.order_id, master_order_id
FROM _fact_chargeback_ebs__order_base ob
         JOIN stg.fact_order fo
              ON ob.order_id = fo.order_id
                  AND fo.order_status_key = 8;

-- Getting data from historical data
INSERT INTO _fact_chargeback_ebs__stg_fact_chargeback
SELECT
    fch.order_id,
    fch.meta_original_order_id,
    fch.customer_id,
    fch.store_id,
    dcs.chargeback_status,
    fch.chargeback_payment_key,
    fch.chargeback_reason_key,
    fch.chargeback_reason_key as first_chargeback_reason_key,
    fch.chargeback_datetime AS chargeback_date,
    fch.chargeback_datetime AS chargeback_local_datetime,
    NULL AS iso_currency_code,
    NVL(-1 * fch.chargeback_local_amount, 0) AS chargeback_local_amount,
    NVL(-1 * fch.chargeback_payment_transaction_local_amount, 0) AS chargeback_payment_transaction_local_amount,
    NVL(-1 * fch.chargeback_tax_local_amount, 0) AS chargeback_tax_local_amount,
    NVL(-1 * round(fch.chargeback_local_amount * fch.effective_vat_rate, 2), 0) AS chargeback_vat_local_amount,
    fch.effective_vat_rate,
    fch.chargeback_date_usd_conversion_rate,
    fch.chargeback_date_eur_conversion_rate,
    'ultra_merchant' AS source
FROM reference.fact_chargeback_history fch
JOIN _fact_chargeback_ebs__order_base ob
    ON fch.meta_original_order_id = ob.order_id
LEFT JOIN _fact_chargeback_ebs__stg_fact_chargeback stg
    ON stg.order_id = fch.order_id
JOIN stg.dim_chargeback_status dcs
    ON dcs.chargeback_status_key = fch.chargeback_status_key
WHERE stg.order_id IS NULL
AND NOT fch.is_deleted;


CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__activation AS
SELECT
    stg.customer_id,
    fa.activation_key,
    fa.membership_event_key,
    fa.activation_local_datetime,
    fa.next_activation_local_datetime,
    fa.activation_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY stg.customer_id ORDER BY fa.activation_sequence_number, fa.activation_local_datetime) AS row_num
FROM (SELECT DISTINCT customer_id FROM _fact_chargeback_ebs__stg_fact_chargeback) AS stg
    JOIN stg.fact_activation AS fa
    	ON fa.customer_id = stg.customer_id
WHERE NOT NVL(fa.is_deleted, FALSE);

CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__first_activation AS
SELECT
    stg.order_id,
    stg.customer_id,
    a.activation_key,
    a.membership_event_key,
    a.activation_local_datetime,
    a.next_activation_local_datetime,
    a.activation_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY stg.order_id, stg.customer_id ORDER BY a.row_num) AS row_num
FROM _fact_chargeback_ebs__stg_fact_chargeback AS stg
    JOIN _fact_chargeback_ebs__activation AS a
		ON a.customer_id = stg.customer_id
        AND a.activation_local_datetime <= stg.chargeback_local_datetime;

CREATE OR REPLACE TEMP TABLE _fact_chargeback__bops_original_store_id AS
SELECT
    base.order_id,
    TRY_TO_NUMBER(od.value) AS bops_original_store_id
FROM _fact_chargeback_ebs__order_base AS base
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = base.order_id
    JOIN lake_consolidated.ultra_merchant.order_detail AS od
        ON od.order_id = base.order_id
WHERE oc.order_type_id = 40
    AND od.name = 'original_store_id';


CREATE OR REPLACE TEMP TABLE _fact_chargeback_ebs__stg AS
SELECT
    COALESCE(ds.master_order_id, f.order_id) AS order_id,
    f.order_id                        AS source_order_id,
    f.meta_original_order_id,
    COALESCE(f.customer_id, -1) AS customer_id,
    CASE WHEN COALESCE(bops.bops_original_store_id, f.store_id, -1) = 41 THEN 26 ELSE COALESCE(bops.bops_original_store_id, f.store_id, -1) END AS store_id,
    COALESCE(dcs.chargeback_status_key, -1) AS chargeback_status_key,
    COALESCE(f.chargeback_payment_key, -1) AS chargeback_payment_key,
    COALESCE(f.chargeback_reason_key, -1) AS chargeback_reason_key,
    COALESCE(f.first_chargeback_reason_key, -1) AS first_chargeback_reason_key,
    f.chargeback_local_datetime AS chargeback_local_datetime,
    COALESCE(f.chargeback_date_eur_conversion_rate, 0) AS chargeback_date_eur_conversion_rate,
    COALESCE(f.chargeback_date_usd_conversion_rate, 0) AS chargeback_date_usd_conversion_rate,
    COALESCE((-1*f.chargeback_local_amount), 0) AS chargeback_local_amount,
    COALESCE((-1*f.chargeback_payment_transaction_local_amount), 0) AS chargeback_payment_transaction_local_amount,
    COALESCE((-1*f.chargeback_tax_local_amount), 0) AS chargeback_tax_local_amount,
    COALESCE((-1*f.chargeback_vat_local_amount), 0) AS chargeback_vat_local_amount,
    COALESCE(f.effective_vat_rate, 0) AS effective_vat_rate,
    COALESCE(fa.activation_key, -1) AS activation_key,
    COALESCE(ffa.activation_key, -1) AS first_activation_key,
    f.source,
    IFF(tc.customer_id IS NOT NULL, TRUE, FALSE) AS is_test_customer,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime,
    FALSE AS is_deleted
FROM _fact_chargeback_ebs__stg_fact_chargeback AS f
    LEFT JOIN _fact_chargeback__bops_original_store_id AS bops
        ON bops.order_id = f.order_id
    LEFT JOIN stg.dim_chargeback_status AS dcs
        ON UPPER(dcs.chargeback_status) = UPPER(f.chargeback_status)
    LEFT JOIN reference.test_customer AS tc
        ON tc.customer_id = f.customer_id
    LEFT JOIN _fact_chargeback_ebs__activation AS fa
		ON fa.customer_id = f.customer_id
		AND (f.chargeback_local_datetime >= fa.activation_local_datetime
		AND  f.chargeback_local_datetime < fa.next_activation_local_datetime)
    LEFT JOIN _fact_chargeback_ebs__first_activation AS ffa
		ON ffa.order_id = f.order_id
        AND ffa.customer_id = f.customer_id
		AND ffa.row_num = 1
    LEFT JOIN _fact_chargeback_ebs__dropship ds
    ON f.order_id = ds.order_id;

-- Delete orphan data
UPDATE stg.fact_chargeback AS fc
SET fc.is_deleted = TRUE,
    fc.meta_row_hash = HASH(order_id, meta_original_order_id, customer_id, activation_key, first_activation_key, store_id, chargeback_status_key, chargeback_payment_key, chargeback_datetime, chargeback_date_eur_conversion_rate, chargeback_date_usd_conversion_rate, chargeback_local_amount, chargeback_payment_transaction_local_amount, chargeback_tax_local_amount, chargeback_vat_local_amount, effective_vat_rate, source, TRUE, is_test_customer, chargeback_reason_key, source_order_id, first_chargeback_reason_key),
    fc.meta_update_datetime = $execution_start_time
WHERE $is_full_refresh
    AND (NOT EXISTS (
        SELECT TRUE AS is_exists
        FROM _fact_chargeback_ebs__stg AS stg
        WHERE stg.source_order_id = fc.source_order_id
            AND stg.chargeback_local_datetime = fc.chargeback_datetime
            AND stg.store_id = fc.store_id)
        );

INSERT INTO stg.fact_chargeback_stg (
    order_id,
    meta_original_order_id,
    customer_id,
    store_id,
    chargeback_status_key,
    chargeback_payment_key,
    chargeback_reason_key,
    first_chargeback_reason_key,
    chargeback_datetime,
    chargeback_date_eur_conversion_rate,
    chargeback_date_usd_conversion_rate,
    chargeback_local_amount,
    chargeback_payment_transaction_local_amount,
    chargeback_tax_local_amount,
    chargeback_vat_local_amount,
    effective_vat_rate,
    activation_key,
    first_activation_key,
    source,
    is_test_customer,
    meta_create_datetime,
    meta_update_datetime,
    is_deleted,
    source_order_id
    )
SELECT
    order_id,
    meta_original_order_id,
    customer_id,
    store_id,
    chargeback_status_key,
    chargeback_payment_key,
    chargeback_reason_key,
    first_chargeback_reason_key,
    chargeback_local_datetime,
    chargeback_date_eur_conversion_rate,
    chargeback_date_usd_conversion_rate,
    chargeback_local_amount,
    chargeback_payment_transaction_local_amount,
    chargeback_tax_local_amount,
    chargeback_vat_local_amount,
    effective_vat_rate,
    activation_key,
    first_activation_key,
    source,
    is_test_customer,
    meta_create_datetime,
    meta_update_datetime,
    is_deleted,
    source_order_id
FROM _fact_chargeback_ebs__stg
ORDER BY
    order_id,
    source_order_id,
    chargeback_local_datetime,
    store_id;
