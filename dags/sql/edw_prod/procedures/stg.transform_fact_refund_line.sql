SET target_table = 'stg.fact_refund_line';
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

SET wm_lake_ultra_merchant_refund = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.refund'));
SET wm_lake_ultra_merchant_refund_line = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.refund_line'));
SET wm_edw_stg_fact_refund = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_refund'));
SET wm_edw_stg_dim_refund_payment_method = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_refund_payment_method'));
SET wm_edw_reference_test_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.test_customer'));
SET wm_lake_ultra_merchant_order_line_split_map = (SELECT stg.UDF_GET_WATERMARK($target_table, 'lake_consolidated.ultra_merchant.order_line_split_map'));

CREATE OR REPLACE TEMP TABLE _fact_refund_line__refund_base (refund_id INT);

INSERT INTO _fact_refund_line__refund_base
SELECT DISTINCT rl.refund_id
FROM lake_consolidated.ultra_merchant.refund_line AS rl
    JOIN lake_consolidated.ultra_merchant.refund AS r
        ON r.refund_id = rl.refund_id
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = r.order_id
    JOIN reference.store_timezone AS stz
        ON stz.store_id = o.store_id
WHERE $is_full_refresh
AND CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(r.datetime_added,'America/Los_Angeles'))::DATE >= '2020-01-01'
ORDER BY rl.refund_id;

INSERT INTO _fact_refund_line__refund_base
SELECT DISTINCT incr.refund_id
FROM (
         SELECT fr.refund_id
         FROM lake_consolidated.ultra_merchant.refund_line AS rl
            LEFT JOIN lake_consolidated.ultra_merchant.refund AS r
                ON r.refund_id = rl.refund_id
             LEFT JOIN stg.dim_refund_payment_method AS drpm
                ON drpm.source_refund_payment_method = r.payment_method
            LEFT JOIN stg.fact_refund AS fr
                ON fr.refund_id = rl.refund_id
            LEFT JOIN reference.test_customer AS tc
                ON tc.customer_id = fr.customer_id
            LEFT JOIN lake_consolidated.ultra_merchant.order_line_split_map AS olsm
                ON rl.order_line_id = olsm.order_line_id

         WHERE COALESCE(fr.refund_completion_local_datetime::DATE, fr.refund_request_local_datetime::DATE) >= '2020-01-01'
            AND (
                 rl.meta_update_datetime > $wm_lake_ultra_merchant_refund_line
                 OR r.meta_update_datetime > $wm_lake_ultra_merchant_refund
                 OR fr.meta_update_datetime > $wm_edw_stg_fact_refund
                 OR drpm.meta_update_datetime > $wm_edw_stg_dim_refund_payment_method
                 OR tc.meta_update_datetime > $wm_edw_reference_test_customer
                 OR olsm.meta_update_datetime > $wm_lake_ultra_merchant_order_line_split_map

                )
         UNION ALL
         SELECT refund_id
         FROM excp.fact_refund_line
         WHERE meta_is_current_excp
            AND meta_data_quality = 'error'
     ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.refund_id;

CREATE OR REPLACE TEMP TABLE _fact_refund_line__test_customers AS
SELECT DISTINCT tc.customer_id
FROM _fact_refund_line__refund_base AS rb
    JOIN stg.fact_refund AS fr
        ON fr.refund_id = rb.refund_id
    JOIN reference.test_customer AS tc
        ON tc.customer_id = fr.customer_id;

CREATE OR REPLACE TEMP TABLE _fact_refund_line__dropship AS
SELECT rb.refund_id,
       rl.refund_line_id,
       olsm.master_order_line_id,
       olsm.master_order_id
FROM _fact_refund_line__refund_base AS rb
         JOIN lake_consolidated.ultra_merchant.refund_line AS rl
              ON rb.refund_id = rl.refund_id
         LEFT JOIN lake_consolidated.ultra_merchant.order_line_split_map olsm
                   ON rl.order_line_id = olsm.order_line_id
         JOIN stg.fact_order fo
              ON olsm.order_id = fo.order_id
                  AND fo.order_status_key = 8;

CREATE OR REPLACE TEMP TABLE _fact_refund_line__stg AS
SELECT
    rb.refund_id,
    rl.refund_line_id,
    rl.meta_original_refund_line_id,
    fr.order_id                                             AS order_id,
    fr.source_order_id                                      AS source_order_id,
    COALESCE(ds.master_order_line_id, rl.order_line_id, -1) AS order_line_id,
    COALESCE(rl.order_line_id, -1)                          AS source_order_line_id,
    fr.customer_id,
    fr.activation_key,
    fr.first_activation_key,
    CASE WHEN fr.store_id = 41 THEN 26 ELSE fr.store_id END AS store_id,
    fr.refund_status_key,
    fr.refund_payment_method_key,
    fr.refund_request_local_datetime,
    fr.refund_completion_local_datetime,
    fr.refund_completion_date_usd_conversion_rate,
    fr.refund_completion_date_eur_conversion_rate,
    fr.effective_vat_rate,
    (rl.refund_amount / (1 + COALESCE(fr.effective_vat_rate, 0)))::NUMBER(19,4) AS product_refund_local_amount,
    IFF(drpm.refund_payment_method NOT IN ('Store Credit', 'Membership Token'), product_refund_local_amount,0) AS product_cash_refund_local_amount,
    IFF(drpm.refund_payment_method IN ('Store Credit', 'Membership Token'), product_refund_local_amount,0) AS product_store_credit_refund_local_amount,
    IFF(drpm.refund_payment_method IN ('Store Credit', 'Membership Token') AND drpm.refund_payment_method_type = 'Cash Credit', product_refund_local_amount,0) AS product_cash_store_credit_refund_local_amount,
    IFF(drpm.refund_payment_method IN ('Store Credit', 'Membership Token') AND drpm.refund_payment_method_type = 'NonCash Credit', product_refund_local_amount,0) AS product_noncash_store_credit_refund_local_amount,
    IFF(drpm.refund_payment_method IN ('Store Credit', 'Membership Token') AND drpm.refund_payment_method_type = 'Unknown', product_refund_local_amount,0) AS product_unknown_store_credit_refund_local_amount,
    COALESCE(fr.is_chargeback,FALSE) AS is_chargeback,
    IFF(tc.customer_id IS NOT NULL,TRUE,FALSE) AS is_test_customer,
    COALESCE(fr.is_deleted,FALSE) AS is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_refund_line__refund_base AS rb
    JOIN stg.fact_refund AS fr
        ON fr.refund_id = rb.refund_id
    JOIN lake_consolidated.ultra_merchant.refund_line AS rl
        ON rb.refund_id = rl.refund_id
    LEFT JOIN stg.dim_refund_payment_method AS drpm
        ON drpm.refund_payment_method_key = fr.refund_payment_method_key
    LEFT JOIN _fact_refund_line__test_customers AS tc
        ON tc.customer_id = fr.customer_id
    LEFT JOIN _fact_refund_line__dropship ds
        ON rl.refund_line_id = ds.refund_line_id;

-- Delete records with wrong concatanation
INSERT INTO _fact_refund_line__stg (
    refund_id,
    refund_line_id,
    meta_original_refund_line_id,
    order_id,
    source_order_id,
    order_line_id,
    source_order_line_id,
    customer_id,
    activation_key,
    first_activation_key,
    store_id,
    refund_status_key,
    refund_payment_method_key,
    refund_request_local_datetime,
    refund_completion_local_datetime,
    refund_completion_date_usd_conversion_rate,
    refund_completion_date_eur_conversion_rate,
    effective_vat_rate,
    product_refund_local_amount,
    product_cash_refund_local_amount,
    product_store_credit_refund_local_amount,
    product_cash_store_credit_refund_local_amount,
    product_noncash_store_credit_refund_local_amount,
    product_unknown_store_credit_refund_local_amount,
    is_chargeback,
    is_test_customer,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT t.refund_id,
       t.refund_line_id,
       t.meta_original_refund_line_id,
       t.order_id,
       f.source_order_id,
       t.order_line_id,
       f.source_order_line_id,
       t.customer_id,
       t.activation_key,
       t.first_activation_key,
       CASE WHEN t.store_id = 41 THEN 26 ELSE t.store_id END AS store_id,
       t.refund_status_key,
       t.refund_payment_method_key,
       t.refund_request_local_datetime,
       t.refund_completion_local_datetime,
       t.refund_completion_date_usd_conversion_rate,
       t.refund_completion_date_eur_conversion_rate,
       t.effective_vat_rate,
       t.product_refund_local_amount,
       t.product_cash_refund_local_amount,
       t.product_store_credit_refund_local_amount,
       t.product_cash_store_credit_refund_local_amount,
       t.product_noncash_store_credit_refund_local_amount,
       t.product_unknown_store_credit_refund_local_amount,
       t.is_chargeback,
       t.is_test_customer,
       TRUE AS is_deleted,
       $execution_start_time,
       $execution_start_time
FROM stg.fact_refund_line t
        JOIN _fact_refund_line__stg f
            ON t.meta_original_refund_line_id = f.meta_original_refund_line_id
         JOIN stg.dim_store ds
              ON t.store_id = ds.store_id
WHERE $is_full_refresh = TRUE
  AND t.is_deleted = FALSE
  AND RIGHT(t.refund_line_id, 2) <> ds.company_id
      AND NOT EXISTS (
        SELECT 1
        FROM _fact_refund_line__stg AS s
        WHERE s.refund_line_id = t.refund_line_id
        );



INSERT INTO stg.fact_refund_line_stg
(
    refund_id,
    refund_line_id,
    meta_original_refund_line_id,
    order_id,
    order_line_id,
    customer_id,
    activation_key,
    first_activation_key,
    store_id,
    refund_status_key,
    refund_payment_method_key,
    refund_request_local_datetime,
    refund_completion_local_datetime,
    refund_completion_date_usd_conversion_rate,
    refund_completion_date_eur_conversion_rate,
    effective_vat_rate,
    product_refund_local_amount,
    product_cash_refund_local_amount,
    product_store_credit_refund_local_amount,
    product_cash_store_credit_refund_local_amount,
    product_noncash_store_credit_refund_local_amount,
    product_unknown_store_credit_refund_local_amount,
    is_chargeback,
    is_test_customer,
    is_deleted,
    source_order_id,
    source_order_line_id,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    refund_id,
    refund_line_id,
    meta_original_refund_line_id,
    order_id,
    order_line_id,
    customer_id,
    activation_key,
    first_activation_key,
    store_id,
    refund_status_key,
    refund_payment_method_key,
    refund_request_local_datetime,
    refund_completion_local_datetime,
    refund_completion_date_usd_conversion_rate,
    refund_completion_date_eur_conversion_rate,
    effective_vat_rate,
    product_refund_local_amount,
    product_cash_refund_local_amount,
    product_store_credit_refund_local_amount,
    product_cash_store_credit_refund_local_amount,
    product_noncash_store_credit_refund_local_amount,
    product_unknown_store_credit_refund_local_amount,
    is_chargeback,
    is_test_customer,
    is_deleted,
    source_order_id,
    source_order_line_id,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_refund_line__stg
ORDER BY refund_id;
