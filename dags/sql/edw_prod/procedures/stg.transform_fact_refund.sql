SET target_table = 'stg.fact_refund';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_lake_ultra_merchant_refund = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.refund'));
SET wm_lake_ultra_merchant_refund_reason = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.refund_reason'));
SET wm_edw_stg_fact_chargeback = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_chargeback'));
SET wm_lake_ultra_merchant_order_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_detail'));
SET wm_lake_ultra_merchant_order_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_classification'));
SET wm_lake_ultra_merchant_address = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.address'));
SET wm_edw_stg_dim_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer'));
SET wm_edw_stg_dim_refund_payment_method = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_refund_payment_method'));
SET wm_edw_stg_fact_activation = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_activation'));
SET wm_edw_stg_fact_order = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order'));
SET wm_lake_ultra_merchant_rma_refund = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.rma_refund'));
SET wm_lake_ultra_merchant_rma = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.rma'));
SET wm_lake_ultra_merchant_return = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.return'));
SET wm_lake_ultra_merchant_refund_membership_token = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.refund_membership_token'));
SET wm_lake_ultra_merchant_membership_token = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_token'));
SET wm_lake_ultra_merchant_membership_token_reason = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_token_reason'));
SET wm_lake_ultra_merchant_refund_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.refund_store_credit'));
SET wm_lake_ultra_merchant_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_credit'));
SET wm_lake_ultra_merchant_store_credit_reason = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_credit_reason'));

/*
SELECT
    $wm_lake_ultra_merchant_refund,
    $wm_lake_ultra_merchant_refund_reason,
    $wm_edw_stg_fact_chargeback,
    $wm_lake_ultra_merchant_order_detail,
    $wm_lake_ultra_merchant_order_classification,
    $wm_lake_ultra_merchant_order,
    $wm_lake_ultra_merchant_address,
    $wm_edw_stg_dim_customer,
    $wm_edw_stg_dim_refund_payment_method,
    $wm_edw_stg_fact_activation,
    $wm_edw_stg_fact_order,
    $wm_lake_ultra_merchant_rma_refund,
    $wm_lake_ultra_merchant_rma,
    $wm_lake_ultra_merchant_return,
    $wm_lake_ultra_merchant_refund_membership_token,
    $wm_lake_ultra_merchant_membership_token,
    $wm_lake_ultra_merchant_membership_token_reason,
    $wm_lake_ultra_merchant_refund_store_credit,
    $wm_lake_ultra_merchant_store_credit,
    $wm_lake_ultra_merchant_store_credit_reason;
*/

CREATE OR REPLACE TEMP TABLE _fact_refund__refund_base (refund_id INT) CLUSTER BY (refund_id);

INSERT INTO _fact_refund__refund_base (refund_id)
-- Full Refresh
SELECT DISTINCT r.refund_id
FROM lake_consolidated.ultra_merchant.refund AS r
WHERE $is_full_refresh = TRUE
UNION ALL
-- Incremental Refresh
SELECT DISTINCT incr.refund_id
FROM (
    SELECT r.refund_id
    FROM lake_consolidated.ultra_merchant.refund AS r
        LEFT JOIN lake_consolidated.ultra_merchant.refund_reason AS rr
            ON rr.refund_reason_id = r.refund_reason_id
        LEFT JOIN stg.fact_chargeback AS fc
            ON fc.order_id = r.order_id
    WHERE r.meta_update_datetime > $wm_lake_ultra_merchant_refund
        OR rr.meta_update_datetime > $wm_lake_ultra_merchant_refund_reason
        OR fc.meta_update_datetime > $wm_edw_stg_fact_chargeback
    UNION ALL
    SELECT r.refund_id
    FROM lake_consolidated.ultra_merchant.refund AS r
        LEFT JOIN lake_consolidated.ultra_merchant.order_detail AS od
            ON od.order_id = r.order_id
            AND od.name IN ('retail_store_id','original_store_id')
        LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
            ON oc.order_id = r.order_id
            AND oc.order_type_id IN (10,40)
    WHERE od.meta_update_datetime > $wm_lake_ultra_merchant_order_detail
        OR oc.meta_update_datetime > $wm_lake_ultra_merchant_order_classification
    UNION ALL
    SELECT r.refund_id
    FROM lake_consolidated.ultra_merchant.refund AS r
        LEFT JOIN stg.fact_order AS o
            ON o.order_id = r.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.address AS a
            ON a.address_id = o.shipping_address_id
        LEFT JOIN stg.dim_customer AS dc
            ON dc.customer_id = o.customer_id
        LEFT JOIN stg.dim_refund_payment_method AS drpm
            ON drpm.source_refund_payment_method = r.payment_method
        LEFT JOIN stg.fact_activation AS fa
            ON fa.customer_id = o.customer_id
    WHERE o.meta_update_datetime > $wm_edw_stg_fact_order
        OR a.meta_update_datetime > $wm_lake_ultra_merchant_address
        OR dc.meta_update_datetime > $wm_edw_stg_dim_customer
        OR drpm.meta_update_datetime > $wm_edw_stg_dim_refund_payment_method
        OR fa.meta_update_datetime > $wm_edw_stg_fact_activation
    UNION ALL
    SELECT r.refund_id
    FROM lake_consolidated.ultra_merchant.refund AS r
        LEFT JOIN lake_consolidated.ultra_merchant.rma_refund AS rr
            ON rr.refund_id = r.refund_id
        LEFT JOIN lake_consolidated.ultra_merchant.rma AS rma
            ON rma.rma_id = rr.rma_id
        LEFT JOIN lake_consolidated.ultra_merchant.return AS ret
            ON ret.rma_id = rma.rma_id
    WHERE rr.meta_update_datetime > $wm_lake_ultra_merchant_rma_refund
        OR rma.meta_update_datetime > $wm_lake_ultra_merchant_rma
        OR ret.meta_update_datetime > $wm_lake_ultra_merchant_return
    UNION ALL
    SELECT rmt.refund_id
    FROM lake_consolidated.ultra_merchant.refund_membership_token AS rmt
        LEFT JOIN lake_consolidated.ultra_merchant.membership_token AS mt
            ON mt.membership_token_id = rmt.membership_token_id
        LEFT JOIN lake_consolidated.ultra_merchant.membership_token_reason AS mtr
            ON mtr.membership_token_reason_id = mt.membership_token_reason_id
    WHERE rmt.meta_update_datetime > $wm_lake_ultra_merchant_refund_membership_token
        OR mt.meta_update_datetime > $wm_lake_ultra_merchant_membership_token
        OR mtr.meta_update_datetime > $wm_lake_ultra_merchant_membership_token_reason
    UNION ALL
    SELECT rsc.refund_id
    FROM lake_consolidated.ultra_merchant.refund_store_credit AS rsc
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit AS sc
            ON sc.store_credit_id = rsc.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit_reason AS scr
            ON scr.store_credit_reason_id = sc.store_credit_reason_id
    WHERE rsc.meta_update_datetime > $wm_lake_ultra_merchant_refund_store_credit
        OR sc.meta_update_datetime > $wm_lake_ultra_merchant_store_credit
        OR scr.meta_update_datetime > $wm_lake_ultra_merchant_store_credit_reason
    UNION ALL
    SELECT refund_id
    FROM excp.fact_refund
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh;

CREATE OR REPLACE TEMP TABLE _fact_refund__refund_data AS
SELECT
    r.refund_id,
    r.meta_original_refund_id,
    r.payment_method,
    rr.label AS refund_reason,
    COALESCE(LOWER(TRIM(r.reason_comment)), 'Unknown') AS refund_comment,
    r.statuscode AS refund_status_code,
    r.statuscode AS raw_refund_status_code,
    IFF(order_status_key = 8, COALESCE(fo.master_order_id, fo.order_id), fo.order_id) AS order_id,
    fo.order_id                                                                       AS source_order_id,
    fo.customer_id,
    CONVERT_TIMEZONE('America/Los_Angeles', fo.order_local_datetime) AS order_datetime_added,
    fo.shipping_address_id,
    COALESCE(TRY_TO_NUMBER(od.value), fo.store_id) AS store_id,
    IFF(oc.order_type_id = 10, TRUE, FALSE) AS is_membership_credit
FROM _fact_refund__refund_base AS base
    JOIN lake_consolidated.ultra_merchant.refund AS r
        ON r.refund_id = base.refund_id
    LEFT JOIN lake_consolidated.ultra_merchant.refund_reason AS rr
        ON rr.refund_reason_id = r.refund_reason_id
    LEFT JOIN lake_consolidated.ultra_merchant.order_detail AS od
        ON od.order_id = r.order_id
        and od.name = 'retail_store_id'
    LEFT JOIN stg.fact_order AS fo
        ON fo.order_id = r.order_id
    LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = r.order_id
    	AND oc.order_type_id = 10;

ALTER TABLE _fact_refund__refund_data CLUSTER BY (refund_id);

CREATE OR REPLACE TEMP TABLE _fact_refund__rma AS
SELECT rd.refund_id,
       ret.return_id,
       rma.rma_id,
       rma.datetime_added
FROM _fact_refund__refund_data AS rd
         JOIN lake_consolidated.ultra_merchant.rma_refund AS rr
              ON rr.refund_id = rd.refund_id
         LEFT JOIN lake_consolidated.ultra_merchant.rma AS rma
                   ON rma.rma_id = rr.rma_id
         LEFT JOIN lake_consolidated.ultra_merchant.return AS ret
                   ON ret.rma_id = rma.rma_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rma.rma_id ORDER BY rma.datetime_added DESC, ret.return_id DESC, rd.refund_id DESC) =
            1;

CREATE OR REPLACE TEMP TABLE _fact_refund__rma_refund_data AS
SELECT DISTINCT r.refund_id,
                CASE
                    WHEN rma_history.refund_id IS NOT NULL THEN rma_history.return_id
                    ELSE COALESCE(rma.return_id, -1) END                                                             AS return_id,
                CASE
                    WHEN rma_history.refund_id IS NOT NULL THEN rma_history.refund_request_local_datetime
                    ELSE CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(
                        IFF(rd.is_membership_credit, r.datetime_added, rma.datetime_added), 'America/Los_Angeles')
                        ) END                                                                                        AS refund_request_local_datetime,
                CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(r.datetime_refunded, 'America/Los_Angeles')
                    )                                                                                                AS refund_completion_local_datetime,
                CASE
                    WHEN rma_history.refund_id IS NOT NULL THEN rma_history.rma_id
                    ELSE COALESCE(rma.rma_id, -1) END                                                                AS rma_id,
                COALESCE(r.administrator_id, -1)                                                                     AS refund_administrator_id,
                COALESCE(r.approved_administrator_id, -1)                                                            AS refund_approver_id,
                COALESCE(r.payment_transaction_id, -1)                                                               AS refund_payment_transaction_id,
                (r.product_refund / (1 + COALESCE(vrh.rate, 0)))                                                     AS refund_product_local_amount,
                r.tax_refund                                                                                         AS refund_tax_local_amount,
                (r.shipping_refund / (1 + COALESCE(vrh.rate, 0)))                                                    AS refund_freight_local_amount,
                ((r.product_refund / (1 + COALESCE(vrh.rate, 0))) +
                 (r.shipping_refund / (1 + IFNULL(vrh.rate, 0))))                                                    AS refund_total_local_amount,
                vrh.rate                                                                                             AS effective_vat_rate,
                IFF(fc.order_id IS NOT NULL, TRUE, FALSE)                                                            AS is_chargeback
FROM _fact_refund__refund_data AS rd
         JOIN lake_consolidated.ultra_merchant.refund AS r
              ON r.refund_id = rd.refund_id
         LEFT JOIN reference.fact_refund_rma_history rma_history
                   ON rma_history.refund_id = rd.refund_id
         LEFT JOIN _fact_refund__rma AS rma
                   ON rma.refund_id = rd.refund_id
         LEFT JOIN lake_consolidated.ultra_merchant.address AS a
                   ON a.address_id = rd.shipping_address_id
         LEFT JOIN reference.vat_rate_history AS vrh
                   ON vrh.country_code = REPLACE(UPPER(a.country_code), 'UK', 'GB')
                       AND rd.order_datetime_added::DATE BETWEEN vrh.start_date AND vrh.expires_date
         LEFT JOIN stg.fact_chargeback AS fc
                   ON fc.order_id = rd.order_id
                       AND NOT fc.is_deleted
         LEFT JOIN reference.store_timezone AS stz
                   ON stz.store_id = rd.store_id;

ALTER TABLE _fact_refund__rma_refund_data CLUSTER BY (refund_id);

CREATE OR REPLACE TEMP TABLE _fact_refund__customer_info AS
SELECT
    rd.refund_id,
    dc.customer_id,
    dc.is_test_customer
FROM _fact_refund__refund_data AS rd
    JOIN stg.dim_customer AS dc
        ON dc.customer_id = rd.customer_id;

ALTER TABLE _fact_refund__customer_info CLUSTER BY (refund_id);

CREATE OR REPLACE TEMPORARY TABLE _fact_refund__bops_store_id AS
SELECT
    rd.refund_id,
    rd.order_id,
    TRY_TO_NUMBER(od.value) AS bops_original_store_id
FROM _fact_refund__refund_data AS rd
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = rd.order_id
    JOIN lake_consolidated.ultra_merchant.order_detail AS od
        ON od.order_id = rd.order_id
        AND od.name = 'original_store_id'
WHERE oc.order_type_id = 40;

CREATE OR REPLACE TEMP table _fact_refund__membership_token_refund AS
SELECT
    rd.refund_id,
    mtr.cash
FROM _fact_refund__refund_data AS rd
    JOIN lake_consolidated.ultra_merchant.refund_membership_token AS rmt
        ON rmt.refund_id = rd.refund_id
    JOIN lake_consolidated.ultra_merchant.membership_token AS mt
        ON mt.membership_token_id = rmt.membership_token_id
    JOIN lake_consolidated.ultra_merchant.membership_token_reason AS mtr
        ON mtr.membership_token_reason_id = mt.membership_token_reason_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY rd.refund_id ORDER BY cash DESC) = 1;

CREATE OR REPLACE TEMP TABLE _fact_refund__refund_payment_method AS
WITH method_type AS (
    SELECT DISTINCT
        rd.refund_id,
        rd.payment_method AS source_refund_payment_method,
        CASE LOWER(rd.payment_method)
            WHEN 'membership_token' THEN CASE
                WHEN mtr.cash = 1 THEN 'Cash Credit'
                WHEN mtr.cash = 0 THEN 'NonCash Credit'
                ELSE 'Unknown' END
            WHEN 'store_credit' THEN CASE
                WHEN scr.cash = 1 THEN 'Cash Credit'
                WHEN scr.cash = 0 THEN 'NonCash Credit'
                ELSE 'Unknown' END
            WHEN 'check_request' THEN 'Cash'
            WHEN 'cash' THEN 'Cash'
            WHEN 'chargeback' THEN 'Cash'
            WHEN 'creditcard' THEN 'Cash'
            WHEN 'psp' THEN 'Cash'
            ELSE 'Unknown'
            END AS refund_payment_method_type
    FROM _fact_refund__refund_data AS rd
        LEFT JOIN _fact_refund__membership_token_refund AS mtr
            ON mtr.refund_id = rd.refund_id
        LEFT JOIN lake_consolidated.ultra_merchant.refund_store_credit AS rsc
            ON rsc.refund_id = rd.refund_id
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit AS sc
            ON sc.store_credit_id = rsc.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit_reason AS scr
            ON scr.store_credit_reason_id = sc.store_credit_reason_id
    )
SELECT DISTINCT
    mt.refund_id,
    drpm.refund_payment_method_key,
    drpm.refund_payment_method,
    drpm.refund_payment_method_type
FROM method_type AS mt
    JOIN stg.dim_refund_payment_method AS drpm
        ON drpm.source_refund_payment_method = COALESCE(mt.source_refund_payment_method, 'Unknown')
        AND drpm.refund_payment_method_type = COALESCE(mt.refund_payment_method_type, 'Unknown');

CREATE OR REPLACE TEMP TABLE _fact_refund__data AS
SELECT
    rd.refund_id,
    rd.meta_original_refund_id,
    COALESCE(rpm.refund_payment_method_key, -1) AS refund_payment_method_key,
	rd.refund_reason,
	rd.refund_comment,
	rd.refund_status_code,
    rd.raw_refund_status_code,
	rd.order_id,
	rd.source_order_id,
	rd.customer_id,
    COALESCE(bops.bops_original_store_id,rd.store_id) AS store_id,
    rrd.return_id,
	rrd.refund_request_local_datetime,
	rrd.refund_completion_local_datetime,
	rrd.rma_id,
	rrd.refund_administrator_id,
	rrd.refund_approver_id,
	rrd.refund_payment_transaction_id,
	rrd.refund_product_local_amount::NUMBER(19, 4) AS refund_product_local_amount,
	rrd.refund_tax_local_amount::NUMBER(19, 4) AS refund_tax_local_amount,
	rrd.refund_freight_local_amount::NUMBER(19, 4) AS refund_freight_local_amount,
	rrd.refund_total_local_amount::NUMBER(19, 4) AS refund_total_local_amount,
	rrd.effective_vat_rate::NUMBER(18, 6) AS effective_vat_rate,
	rrd.is_chargeback,
    ci.is_test_customer
FROM _fact_refund__refund_data AS rd
    JOIN _fact_refund__rma_refund_data AS rrd
        ON rrd.refund_id = rd.refund_id
    JOIN stg.dim_store AS ds
        ON ds.store_id = rd.store_id
    LEFT JOIN _fact_refund__refund_payment_method AS rpm
        ON rpm.refund_id = rd.refund_id
    LEFT JOIN _fact_refund__customer_info AS ci
        ON ci.refund_id = rd.refund_id
    LEFT JOIN _fact_refund__bops_store_id AS bops
        ON bops.refund_id = rd.refund_id
WHERE ds.store_brand_abbr != 'LGCY';

ALTER TABLE _fact_refund__data CLUSTER BY (refund_id);

CREATE OR REPLACE TEMP TABLE _fact_refund__exch_rates AS
SELECT
    dat.refund_id,
    usd_cer.exchange_rate AS refund_completion_date_usd_conversion_rate,
    eur_cer.exchange_rate AS refund_completion_date_eur_conversion_rate
FROM _fact_refund__data AS dat
    JOIN stg.dim_store AS ds
        ON ds.store_id = dat.store_id
    LEFT JOIN reference.currency_exchange_rate_by_date AS usd_cer
        ON usd_cer.rate_date_pst = dat.refund_completion_local_datetime::TIMESTAMP_LTZ::DATE
        AND usd_cer.src_currency = ds.store_currency
        AND LOWER(usd_cer.dest_currency) = 'usd'
    LEFT JOIN reference.currency_exchange_rate_by_date AS eur_cer
        ON eur_cer.rate_date_pst = dat.refund_completion_local_datetime::TIMESTAMP_LTZ::DATE
        AND eur_cer.src_currency = ds.store_currency
        AND LOWER(eur_cer.dest_currency) = 'eur'
WHERE dat.refund_id IS NOT NULL;

CREATE OR REPLACE TEMP TABLE _fact_refund__activation AS
SELECT
    stg.customer_id,
    fa.activation_key,
    fa.membership_event_key,
    fa.activation_local_datetime,
    fa.next_activation_local_datetime,
    fa.activation_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY stg.customer_id ORDER BY fa.activation_sequence_number, fa.activation_local_datetime) AS row_num
FROM (SELECT DISTINCT customer_id FROM _fact_refund__data) AS stg
    JOIN stg.fact_activation AS fa
    	ON fa.customer_id = stg.customer_id
WHERE NOT NVL(fa.is_deleted, FALSE);

CREATE OR REPLACE TEMP TABLE _fact_refund__first_activation AS
SELECT
    stg.refund_id,
    stg.customer_id,
    a.activation_key,
    a.membership_event_key,
    a.activation_local_datetime,
    a.next_activation_local_datetime,
    a.activation_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY stg.refund_id, stg.customer_id ORDER BY a.row_num) AS row_num
FROM _fact_refund__data AS stg
    JOIN _fact_refund__activation AS a
		ON a.customer_id = stg.customer_id
        AND a.activation_local_datetime <= COALESCE(stg.refund_completion_local_datetime, stg.refund_request_local_datetime);

CREATE OR REPLACE TEMP TABLE _fact_refund__keys AS
SELECT
    dat.refund_id,
    dat.customer_id,
    COALESCE(fa.activation_key, -1) AS activation_key,
    COALESCE(ffa.activation_key, -1) AS first_activation_key
FROM _fact_refund__data AS dat
     LEFT JOIN _fact_refund__activation AS fa
        ON fa.customer_id = dat.customer_id
        AND (COALESCE(dat.refund_completion_local_datetime, dat.refund_request_local_datetime) >= fa.activation_local_datetime
		    AND COALESCE(dat.refund_completion_local_datetime, dat.refund_request_local_datetime) < fa.next_activation_local_datetime)
    LEFT JOIN _fact_refund__first_activation AS ffa
        ON ffa.refund_id = dat.refund_id
        AND ffa.customer_id = dat.customer_id
        AND ffa.row_num = 1
WHERE dat.refund_id IS NOT NULL;

CREATE OR REPLACE TEMP TABLE _fact_refund__stg AS
SELECT DISTINCT /* Eliminate duplicates */
    dat.refund_id,
    dat.meta_original_refund_id,
    dat.refund_payment_method_key,
    dat.refund_reason,
    dat.refund_comment,
    dat.refund_status_code,
    dat.raw_refund_status_code,
    dat.order_id,
    dat.source_order_id,
    dat.customer_id,
    CASE WHEN dat.store_id = 41 THEN 26 ELSE dat.store_id END AS store_id,
    dat.return_id,
    dat.refund_request_local_datetime,
    dat.refund_completion_local_datetime,
    dat.rma_id,
    dat.refund_administrator_id,
    dat.refund_approver_id,
    dat.refund_payment_transaction_id,
    dat.refund_product_local_amount,
    dat.refund_tax_local_amount,
    dat.refund_freight_local_amount,
    dat.refund_total_local_amount,
    dat.effective_vat_rate,
    dat.is_chargeback,
    exch.refund_completion_date_usd_conversion_rate,
	exch.refund_completion_date_eur_conversion_rate,
    key.activation_key,
    key.first_activation_key,
    dat.is_test_customer,
    FALSE AS is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_refund__data AS dat
    LEFT JOIN _fact_refund__exch_rates AS exch
        ON exch.refund_id = dat.refund_id
    LEFT JOIN _fact_refund__keys AS key
        ON key.refund_id = dat.refund_id
WHERE dat.refund_id IS NOT NULL;
-- SELECT * FROM _fact_refund__stg;
-- SELECT refund_id, COUNT(1) FROM _fact_refund__stg GROUP BY 1 HAVING COUNT(1) > 1;

INSERT INTO stg.fact_refund_stg (
    refund_id,
    meta_original_refund_id,
    refund_payment_method_key,
    refund_reason,
    refund_comment,
    refund_status_code,
    raw_refund_status_code,
    order_id,
    source_order_id,
    customer_id,
    store_id,
    return_id,
    refund_request_local_datetime,
    refund_completion_local_datetime,
    rma_id,
    refund_administrator_id,
    refund_approver_id,
    refund_payment_transaction_id,
    refund_product_local_amount,
    refund_tax_local_amount,
    refund_freight_local_amount,
    refund_total_local_amount,
    effective_vat_rate,
    is_chargeback,
    refund_completion_date_usd_conversion_rate,
	refund_completion_date_eur_conversion_rate,
    activation_key,
    first_activation_key,
    is_test_customer,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    refund_id,
    meta_original_refund_id,
    refund_payment_method_key,
    refund_reason,
    refund_comment,
    refund_status_code,
    raw_refund_status_code,
    order_id,
    source_order_id,
    customer_id,
    store_id,
    return_id,
    refund_request_local_datetime,
    refund_completion_local_datetime,
    rma_id,
    refund_administrator_id,
    refund_approver_id,
    refund_payment_transaction_id,
    refund_product_local_amount,
    refund_tax_local_amount,
    refund_freight_local_amount,
    refund_total_local_amount,
    effective_vat_rate,
    is_chargeback,
    refund_completion_date_usd_conversion_rate,
	refund_completion_date_eur_conversion_rate,
    activation_key,
    first_activation_key,
    is_test_customer,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_refund__stg
ORDER BY
    refund_id;
