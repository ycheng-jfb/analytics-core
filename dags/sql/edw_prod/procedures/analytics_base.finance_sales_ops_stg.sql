--SET target_table = 'analytics_base.finance_sales_ops_stg';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = TRUE;
SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -4, CURRENT_DATE));
--ALTER SESSION SET QUERY_TAG = $target_table;
--
--SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', current_warehouse());
--USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

--MERGE INTO edw_prod.stg.meta_table_dependency_watermark AS w
--USING (
--    SELECT
--        $target_table AS table_name,
--        t.dependent_table_name,
--        IFF(t.dependent_table_name IS NOT NULL, t.new_high_watermark_datetime, (
--            SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime
--            FROM edw_prod.analytics_base.finance_sales_ops_stg
--        )) AS new_high_watermark_datetime
--    FROM (
--        SELECT -- For self table
--            NULL AS dependent_table_name,
--            NULL AS new_high_watermark_datetime
--        UNION ALL
--        SELECT
--            'edw_prod.stg.fact_order' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.fact_order
--        UNION ALL
--        SELECT
--            'edw_prod.stg.fact_activation' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.fact_activation
--        UNION ALL
--        SELECT
--            'edw_prod.stg.fact_order_credit' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.fact_order_credit
--        UNION ALL
--        SELECT
--            'edw_prod.stg.fact_order_line' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.fact_order_line
--        UNION ALL
--        SELECT
--            'edw_prod.stg.fact_return_line' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.fact_return_line
--        UNION ALL
--        SELECT
--            'edw_prod.stg.fact_refund' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.fact_refund
--        UNION ALL
--        SELECT
--            'edw_prod.stg.fact_chargeback' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.fact_chargeback
--        UNION ALL
--        SELECT
--            'edw_prod.stg.fact_credit_event' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.fact_credit_event
--        UNION ALL
--        SELECT
--            'edw_prod.stg.fact_order_product_cost' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.fact_order_product_cost
--        UNION ALL
--        SELECT
--            'edw_prod.stg.dim_credit' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.dim_credit
--        UNION ALL
--        SELECT
--            'edw_prod.reference.order_customer_change' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.reference.order_customer_change
--        UNION ALL
--        SELECT
--               'edw_prod.stg.dim_customer'    AS dependent_table_name,
--               MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM edw_prod.stg.dim_customer
--        UNION ALL
--        SELECT
--               'edw_prod.reference.order_date_change' AS dependent_table_name,
--               MAX(meta_update_datetime)         AS new_high_watermark_datetime
--        FROM edw_prod.reference.order_date_change
--         ) AS t
--    ORDER BY COALESCE(t.dependent_table_name, '')
--) AS s
--    ON w.table_name = s.table_name
--        AND w.dependent_table_name IS NOT DISTINCT FROM s.dependent_table_name
--    WHEN NOT MATCHED THEN
--    INSERT (
--        table_name,
--        dependent_table_name,
--        high_watermark_datetime,
--        new_high_watermark_datetime
--        )
--    VALUES (
--        s.table_name,
--        s.dependent_table_name,
--        '1900-01-01', -- current high_watermark_datetime
--        s.new_high_watermark_datetime
--        )
--WHEN MATCHED AND w.new_high_watermark_datetime IS DISTINCT FROM s.new_high_watermark_datetime THEN
--    UPDATE
--    SET w.new_high_watermark_datetime = s.new_high_watermark_datetime,
--        w.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
--SET wm_fact_order = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order'));
--SET wm_fact_order_line = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order_line'));
--SET wm_fact_activation = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_activation'));
--SET wm_fact_refund = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_refund'));
--SET wm_fact_chargeback = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_chargeback'));
--SET wm_fact_return_line = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_return_line'));
--SET wm_fact_credit_event = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_credit_event'));
--SET wm_dim_credit = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_credit'));
--SET wm_fact_order_credit = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order_credit'));
--SET wm_fact_order_product_cost = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order_product_cost'));
--SET wm_order_customer_change = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.reference.order_customer_change'));
--SET wm_dim_customer = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer'));
--SET wm_order_date_change = (SELECT edw_prod.stg.udf_get_watermark($target_table, 'edw_prod.reference.order_date_change'));

/*
SELECT
    $wm_fact_order,
    $wm_fact_order_line,
    $wm_fact_activation,
    $wm_fact_refund,
    $wm_fact_chargeback,
    $wm_fact_return_line,
    $wm_fact_credit_event,
    $wm_dim_credit,
    $wm_fact_order_credit,
    $wm_fact_order_product_cost,
    $wm_order_customer_change,
    $wm_dim_customer,
    $wm_order_date_change;
*/

-- Customer Base Table
CREATE OR REPLACE TEMP TABLE _finance_sales_ops__customer_base (customer_id INT, date DATE);

-- Full Refresh
INSERT INTO _finance_sales_ops__customer_base (customer_id, date)
SELECT DISTINCT fr.customer_id, fr.date
FROM (
    SELECT customer_id, order_local_datetime::DATE AS date
    FROM edw_prod.data_model_jfb.fact_order

    UNION ALL

    SELECT customer_id, order_completion_local_datetime::DATE AS date
    FROM edw_prod.data_model_jfb.fact_order
    WHERE order_completion_local_datetime IS NOT NULL

    UNION ALL

    SELECT customer_id, payment_transaction_local_datetime::DATE AS date
    FROM edw_prod.data_model_jfb.fact_order
    WHERE payment_transaction_local_datetime IS NOT NULL

    UNION ALL

    SELECT customer_id, refund_completion_local_datetime::DATE AS date
    FROM edw_prod.data_model_jfb.fact_refund

    UNION ALL

    SELECT customer_id, chargeback_datetime::DATE AS date
    FROM edw_prod.data_model_jfb.fact_chargeback

    UNION ALL

    SELECT customer_id, return_completion_local_datetime::DATE AS date
    FROM edw_prod.data_model_jfb.fact_return_line

    UNION ALL

    SELECT dc.customer_id, fce.credit_activity_local_datetime::DATE AS date
    FROM edw_prod.data_model_jfb.fact_credit_event AS fce
        JOIN edw_prod.data_model_jfb.dim_credit AS dc
            ON dc.credit_key = fce.credit_key

    UNION ALL

    SELECT IFF(occ.is_gift_order = TRUE, occ.new_customer_id, occ.original_customer_id) AS customer_id,
           o.order_local_datetime::DATE AS date
    FROM edw_prod.reference.order_customer_change AS occ
        JOIN edw_prod.data_model_jfb.fact_order AS o
            ON o.order_id = occ.order_id

    UNION ALL

    SELECT IFF(occ.is_gift_order = TRUE, occ.new_customer_id, occ.original_customer_id) AS customer_id,
           o.order_completion_local_datetime::DATE AS date
    FROM edw_prod.reference.order_customer_change AS occ
        JOIN edw_prod.data_model_jfb.fact_order AS o
            ON o.order_id = occ.order_id
    WHERE o.order_completion_local_datetime IS NOT NULL

    UNION ALL

    SELECT IFF(occ.is_gift_order = TRUE, occ.new_customer_id, occ.original_customer_id) AS customer_id,
           fr.refund_completion_local_datetime::DATE AS date
    FROM edw_prod.reference.order_customer_change AS occ
        JOIN edw_prod.data_model_jfb.fact_refund AS fr
            ON fr.order_id = occ.order_id

    UNION ALL

    SELECT IFF(occ.is_gift_order = TRUE, occ.new_customer_id, occ.original_customer_id) AS customer_id,
           fc.chargeback_datetime::DATE AS date
    FROM edw_prod.reference.order_customer_change AS occ
        JOIN edw_prod.data_model_jfb.fact_chargeback AS fc
            ON fc.order_id = occ.order_id

    UNION ALL

    SELECT IFF(occ.is_gift_order = TRUE, occ.new_customer_id, occ.original_customer_id) AS customer_id,
           frl.return_completion_local_datetime::DATE AS date
    FROM edw_prod.reference.order_customer_change AS occ
        JOIN edw_prod.data_model_jfb.fact_return_line AS frl
            ON frl.order_id = occ.order_id

    UNION ALL

    SELECT customer_id, date
    FROM edw_prod.reference.order_date_change
    ) AS fr
WHERE $is_full_refresh  /* SET is_full_refresh = TRUE; */
ORDER BY fr.customer_id, fr.date;

-- Incremental Refresh
--INSERT INTO _finance_sales_ops__customer_base (customer_id, date)
--SELECT DISTINCT incr.customer_id, incr.date
--FROM (
--    SELECT customer_id, order_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_order
--    WHERE meta_update_datetime > $wm_fact_order
--
--    UNION ALL
--
--    SELECT customer_id, payment_transaction_local_datetime:: DATE AS date
--    FROM edw_prod.stg.fact_order
--    WHERE meta_update_datetime > $wm_fact_order AND
--          payment_transaction_local_datetime IS NOT NULL
--
--    UNION ALL
--
--    SELECT customer_id, order_completion_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_order
--    WHERE meta_update_datetime > $wm_fact_order
--        AND order_completion_local_datetime IS NOT NULL
--
--    UNION ALL
--
--    SELECT o.customer_id, o.order_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_order AS o
--        JOIN edw_prod.stg.fact_activation AS fa
--            ON fa.activation_key = o.activation_key
--    WHERE fa.meta_update_datetime > $wm_fact_activation
--
--    UNION ALL
--
--    SELECT o.customer_id, o.order_completion_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_order AS o
--        JOIN edw_prod.stg.fact_activation AS fa
--            ON fa.activation_key = o.activation_key
--    WHERE fa.meta_update_datetime > $wm_fact_activation
--        AND o.order_completion_local_datetime IS NOT NULL
--
--    UNION ALL
--
--    SELECT fo.customer_id, fo.order_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_order AS fo
--        JOIN edw_prod.stg.fact_order_credit AS foc
--            ON foc.order_id = fo.order_id
--    WHERE foc.meta_update_datetime > $wm_fact_order_credit
--
--    UNION ALL
--
--    SELECT fo.customer_id, fo.order_completion_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_order AS fo
--        JOIN edw_prod.stg.fact_order_credit AS foc
--            ON foc.order_id = fo.order_id
--    WHERE foc.meta_update_datetime > $wm_fact_order_credit
--        AND fo.order_completion_local_datetime IS NOT NULL
--
--    UNION ALL
--
--    SELECT fo.customer_id, fo.order_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_order AS fo
--        JOIN edw_prod.stg.fact_order_product_cost AS fopc ON fopc.order_id = fo.order_id
--    WHERE fopc.meta_update_datetime > $wm_fact_order_product_cost
--
--    UNION ALL
--
--    SELECT fo.customer_id, fo.order_completion_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_order AS fo
--        JOIN edw_prod.stg.fact_order_product_cost AS fopc ON fopc.order_id = fo.order_id
--    WHERE fo.order_completion_local_datetime IS NOT NULL
--        AND fopc.meta_update_datetime > $wm_fact_order_product_cost
--
--    UNION ALL
--
--    SELECT fol.customer_id, fol.order_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_order_line AS fol
--    WHERE fol.meta_update_datetime > $wm_fact_order_line
--
--    UNION ALL
--
--    SELECT fol.customer_id, fol.order_completion_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_order_line AS fol
--    WHERE fol.meta_update_datetime > $wm_fact_order_line
--        AND fol.order_completion_local_datetime IS NOT NULL
--
--    UNION ALL
--
--    SELECT frl.customer_id, frl.return_completion_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_return_line AS frl
--        JOIN edw_prod.stg.fact_order AS o
--            ON o.order_id = frl.order_id
--        LEFT JOIN edw_prod.stg.fact_activation AS fa
--            ON fa.activation_key = frl.activation_key
--    WHERE frl.meta_update_datetime > $wm_fact_return_line
--        OR o.meta_update_datetime > $wm_fact_order
--        OR fa.meta_update_datetime > $wm_fact_activation
--
--    UNION ALL
--
--    SELECT fc.customer_id, fc.chargeback_datetime::DATE AS date
--    FROM edw_prod.stg.fact_chargeback AS fc
--        JOIN edw_prod.stg.fact_order AS o
--            ON o.order_id = fc.order_id
--        LEFT JOIN edw_prod.stg.fact_activation AS fa
--            ON fa.activation_key = fc.activation_key
--    WHERE fc.meta_update_datetime > $wm_fact_chargeback
--        OR o.meta_update_datetime > $wm_fact_order
--        OR fa.meta_update_datetime > $wm_fact_activation
--
--    UNION ALL
--
--    SELECT fr.customer_id, fr.refund_completion_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_refund AS fr
--        JOIN edw_prod.stg.fact_order AS o
--            ON o.order_id = fr.order_id
--        LEFT JOIN edw_prod.stg.fact_activation AS fa
--            ON fa.activation_key = fr.activation_key
--    WHERE fr.meta_update_datetime > $wm_fact_refund
--        OR o.meta_update_datetime > $wm_fact_order
--        OR fa.meta_update_datetime > $wm_fact_activation
--
--    UNION ALL
--
--    SELECT c.customer_id, fce.credit_activity_local_datetime::DATE AS date
--    FROM edw_prod.stg.fact_credit_event AS fce
--         JOIN edw_prod.stg.dim_credit AS c
--             ON c.credit_key = fce.credit_key
--         LEFT JOIN edw_prod.stg.fact_activation AS fa
--             ON fa.activation_key = fce.activation_key
--    WHERE (fce.meta_update_datetime > $wm_fact_credit_event
--        OR c.meta_update_datetime > $wm_dim_credit
--        OR fa.meta_update_datetime > $wm_fact_activation)
--        AND fce.credit_activity_type IN ('Issued', 'Expired', 'Cancelled')
--
--    UNION ALL
--
--    SELECT IFF(occ.is_gift_order = TRUE, occ.new_customer_id, occ.original_customer_id)  AS customer_id,
--           o.order_local_datetime::DATE AS date
--    FROM edw_prod.reference.order_customer_change AS occ
--        JOIN edw_prod.stg.fact_order AS o
--            ON o.order_id = occ.order_id
--    WHERE occ.meta_update_datetime > $wm_order_customer_change
--
--    UNION ALL
--
--    SELECT IFF(occ.is_gift_order = TRUE, occ.new_customer_id, occ.original_customer_id) AS customer_id,
--           o.order_completion_local_datetime::DATE AS date
--    FROM edw_prod.reference.order_customer_change AS occ
--        JOIN edw_prod.stg.fact_order AS o
--            ON o.order_id = occ.order_id
--    WHERE (occ.meta_update_datetime > $wm_order_customer_change
--               OR o.meta_update_datetime > $wm_fact_order)
--        AND o.order_completion_local_datetime IS NOT NULL
--
--    UNION ALL
--
--    SELECT IFF(occ.is_gift_order = TRUE, occ.new_customer_id, occ.original_customer_id) AS customer_id,
--           fr.refund_completion_local_datetime::DATE AS date
--    FROM edw_prod.reference.order_customer_change AS occ
--        JOIN edw_prod.stg.fact_refund AS fr
--            ON fr.order_id = occ.order_id
--    WHERE occ.meta_update_datetime > $wm_order_customer_change
--            OR fr.meta_update_datetime > $wm_fact_refund
--
--    UNION ALL
--
--    SELECT IFF(occ.is_gift_order = TRUE, occ.new_customer_id, occ.original_customer_id) AS customer_id,
--           fc.chargeback_datetime::DATE AS date
--    FROM edw_prod.reference.order_customer_change AS occ
--        JOIN edw_prod.stg.fact_chargeback AS fc
--            ON fc.order_id = occ.order_id
--    WHERE occ.meta_update_datetime > $wm_order_customer_change
--            OR fc.meta_update_datetime> $wm_fact_chargeback
--
--    UNION ALL
--
--    SELECT IFF(occ.is_gift_order = TRUE, occ.new_customer_id, occ.original_customer_id) AS customer_id,
--           frl.return_completion_local_datetime::DATE AS date
--    FROM edw_prod.reference.order_customer_change AS occ
--        JOIN edw_prod.stg.fact_return_line AS frl
--            ON frl.order_id = occ.order_id
--    WHERE occ.meta_update_datetime > $wm_order_customer_change
--            OR frl.meta_update_datetime > $wm_fact_return_line
--
--    UNION ALL
--
--    SELECT customer_id, date
--    FROM edw_prod.reference.order_date_change
--    WHERE meta_update_datetime > $wm_order_date_change
--
--    UNION ALL
--
--    SELECT customer_id, date
--    FROM edw_prod.analytics_base.finance_sales_ops_stg
--    WHERE vip_store_id IS NULL
--) AS incr
--WHERE NOT $is_full_refresh
--ORDER BY incr.customer_id, incr.date;

-- Switch warehouse if the delta is more than medium warehouse can handle
--SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', current_warehouse()) FROM _finance_sales_ops__customer_base);
--USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);


CREATE OR REPLACE TEMP TABLE _customer_date AS
SELECT
    base.customer_id AS customer_id,
    base.date,
    IFF(dc.gender = 'M' AND dc.registration_local_datetime >= '2020-01-01','M','F')::VARCHAR(10) AS gender,
    IFNULL(dc.is_cross_promo,FALSE) AS is_cross_promo,
    IFNULL(dc.finance_specialty_store, 'None') AS finance_specialty_store,
    IFNULL(dc.is_scrubs_customer,FALSE) AS is_scrubs_customer
FROM _finance_sales_ops__customer_base AS base
    LEFT JOIN edw_prod.data_model_jfb.dim_customer AS dc
        ON dc.customer_id = base.customer_id
where
    base.date >= $start_date
ORDER BY
    base.customer_id,
    base.date;

-- Creating a table that maps store_id to vip_store_id
CREATE OR REPLACE TEMP TABLE _vip_store_id AS
SELECT COALESCE(b.store_id, a.store_id) as vip_store_id,
       a.store_id,
       COALESCE(b.store_full_name, a.store_full_name) AS vip_store_full_name,
       a.store_full_name
FROM edw_prod.data_model_jfb.dim_store AS a
         LEFT JOIN edw_prod.data_model_jfb.dim_store AS b
            ON b.store_brand = a.store_brand
            AND b.store_country = a.store_country
            AND b.store_type = 'Online'
            AND a.store_type <> 'Online'
            AND b.is_core_store = TRUE
            AND b.store_full_name NOT IN ('JustFab - Wholesale', 'PS by JustFab')
            and b.store_id <> 315
WHERE a.is_core_store = TRUE;

-- DA-18807, DA-18808
CREATE OR REPLACE TEMP TABLE _customer_list_gender_fss AS
SELECT DISTINCT
    customer_id,
    IFF(gender = 'M' AND registration_local_datetime >= '2020-01-01','M','F') AS gender,
    IFNULL(finance_specialty_store,'None') AS finance_specialty_store,
    IFNULL(is_cross_promo,FALSE) AS is_cross_promo
FROM edw_prod.stg.dim_customer
WHERE $is_full_refresh;
--    OR meta_update_datetime > $wm_dim_customer;

CREATE OR REPLACE TEMPORARY TABLE _order_base as
WITH _order_ids AS (
    SELECT
        'shipped' as date_object,
        cd.date,
        CAST(convert_timezone('America/Los_Angeles',o.order_completion_local_datetime) AS DATE) AS date_hq,
        cd.gender,
        cd.is_cross_promo,
        cd.finance_specialty_store,
        cd.is_scrubs_customer,
        o.customer_id,
        o.store_id,
        o.order_id,
        o.order_membership_classification_key,
        o.order_sales_channel_key,
        o.order_status_key,
        o.activation_key,
        o.first_activation_key,
        o.is_credit_billing_on_retry,
        o.currency_key,
        o.administrator_id,
        o.unit_count,
        o.loyalty_unit_count,
        o.reporting_usd_conversion_rate,
        o.reporting_eur_conversion_rate,
        o.subtotal_excl_tariff_local_amount,
        o.product_discount_local_amount,
        o.shipping_discount_local_amount,
        o.payment_transaction_local_amount,
        o.shipping_revenue_before_discount_local_amount,
        o.tax_local_amount,
        o.amount_to_pay,
        o.product_gross_revenue_excl_shipping_local_amount,
        o.product_gross_revenue_local_amount,
        o.product_margin_pre_return_local_amount,
        o.product_margin_pre_return_local_amount_accounting,
        o.product_margin_pre_return_excl_shipping_local_amount,
        o.product_margin_pre_return_excl_shipping_local_amount_accounting,
        o.cash_gross_revenue_local_amount,
        o.estimated_landed_cost_local_amount,
        o.estimated_landed_cost_local_amount_accounting,
        o.misc_cogs_local_amount,
        o.estimated_shipping_supplies_cost_local_amount,
        o.estimated_variable_gms_cost_local_amount,
        (o.cash_gross_revenue_local_amount * o.estimated_variable_payment_processing_pct_cash_revenue) AS estimated_variable_payment_processing_cost_local_amount,
        o.estimated_variable_warehouse_cost_local_amount,
        o.shipping_cost_local_amount,
        o.tariff_revenue_local_amount,
        o.product_order_cash_margin_pre_return_local_amount,
        o.product_order_cash_margin_pre_return_local_amount_accounting,
        o.reporting_landed_cost_local_amount,
        o.reporting_landed_cost_local_amount_accounting,
        o.cash_credit_count
    FROM _customer_date AS cd
    JOIN edw_prod.data_model_jfb.fact_order AS o ON cd.customer_id = o.customer_id AND cd.date = o.order_completion_local_datetime::DATE
    JOIN edw_prod.data_model_jfb.dim_store AS ds ON o.store_id = ds.store_id AND ds.is_core_store = TRUE

    UNION ALL

     SELECT
        'placed' as date_object,
        cd.date,
        CAST(convert_timezone('America/Los_Angeles',o.order_local_datetime) AS DATE) AS date_hq,
        cd.gender,
        cd.is_cross_promo,
        cd.finance_specialty_store,
        cd.is_scrubs_customer,
        o.customer_id,
        o.store_id,
        o.order_id,
        o.order_membership_classification_key,
        o.order_sales_channel_key,
        o.order_status_key,
        o.activation_key,
        o.first_activation_key,
        o.is_credit_billing_on_retry,
        o.currency_key,
        o.administrator_id,
        o.unit_count,
        o.loyalty_unit_count,
        o.reporting_usd_conversion_rate,
        o.reporting_eur_conversion_rate,
        o.subtotal_excl_tariff_local_amount,
        o.product_discount_local_amount,
        o.shipping_discount_local_amount,
        o.payment_transaction_local_amount,
        o.shipping_revenue_before_discount_local_amount,
        o.tax_local_amount,
        o.amount_to_pay,
        o.product_gross_revenue_excl_shipping_local_amount,
        o.product_gross_revenue_local_amount,
        o.product_margin_pre_return_local_amount,
        o.product_margin_pre_return_local_amount_accounting,
        o.product_margin_pre_return_excl_shipping_local_amount,
        o.product_margin_pre_return_excl_shipping_local_amount_accounting,
        o.cash_gross_revenue_local_amount,
        o.estimated_landed_cost_local_amount,
        o.estimated_landed_cost_local_amount_accounting,
        o.misc_cogs_local_amount,
        o.estimated_shipping_supplies_cost_local_amount,
        o.estimated_variable_gms_cost_local_amount,
        (o.cash_gross_revenue_local_amount * o.estimated_variable_payment_processing_pct_cash_revenue) AS estimated_variable_payment_processing_cost_local_amount,
        o.estimated_variable_warehouse_cost_local_amount,
        o.shipping_cost_local_amount,
        o.tariff_revenue_local_amount,
        o.product_order_cash_margin_pre_return_local_amount,
        o.product_order_cash_margin_pre_return_local_amount_accounting,
        o.reporting_landed_cost_local_amount,
        o.reporting_landed_cost_local_amount_accounting,
        o.cash_credit_count
    FROM _customer_date AS cd
    JOIN edw_prod.data_model_jfb.fact_order AS o ON cd.customer_id = o.customer_id AND cd.date = o.order_local_datetime::DATE
    JOIN edw_prod.data_model_jfb.dim_store AS ds ON o.store_id = ds.store_id AND ds.is_core_store = TRUE
)
SELECT
    o.date_object,
    o.date,
    o.date_hq,
    o.customer_id,
    fa.activation_local_datetime,
    fa.cancellation_local_datetime,
    fa.vip_cohort_month_date,
    fa_first.activation_local_datetime AS first_activation_local_datetime,
    fa_first.cancellation_local_datetime AS first_cancellation_local_datetime,
    fa_first.vip_cohort_month_date AS first_vip_cohort_month_date,
    o.is_cross_promo,
    o.gender,
    IFF(fa.vip_cohort_month_date < date_trunc(month,o.date) AND fa.cancellation_local_datetime > date_trunc(month,o.date),1,0) AS is_bop_vip,
    fa.is_reactivated_vip,
    fa.is_retail_vip,
    oc.is_retail_ship_only_order,
    IFF(fa.sub_store_id IS NOT NULL AND fa.sub_store_id NOT IN (-1,0), fa.sub_store_id, vsi.vip_store_id) AS vip_store_id,
    o.store_id,
    o.finance_specialty_store,
    o.is_scrubs_customer,
    st.store_region,
    oc.order_classification_l1,
    oc.order_classification_l2,
    oc.order_sales_channel_l1,
    oc.is_membership_gift,
    oc.is_product_seeding_order,
    o.order_id,
    o.order_membership_classification_key,
    o.activation_key,
    o.first_activation_key,
    o.administrator_id AS associate_id,
    o.is_credit_billing_on_retry,
    o.currency_key,
    o.unit_count,
    o.loyalty_unit_count,
    o.reporting_usd_conversion_rate,
    o.reporting_eur_conversion_rate,
    o.subtotal_excl_tariff_local_amount,
    o.product_discount_local_amount,
    o.shipping_discount_local_amount,
    o.payment_transaction_local_amount,
    o.shipping_revenue_before_discount_local_amount,
    o.tax_local_amount,
    o.amount_to_pay,
    o.product_gross_revenue_excl_shipping_local_amount,
    o.product_gross_revenue_local_amount,
    o.product_margin_pre_return_local_amount,
    o.product_margin_pre_return_local_amount_accounting,
    o.product_margin_pre_return_excl_shipping_local_amount,
    o.product_margin_pre_return_excl_shipping_local_amount_accounting,
    o.cash_gross_revenue_local_amount,
    o.estimated_landed_cost_local_amount,
    o.estimated_landed_cost_local_amount_accounting,
    o.misc_cogs_local_amount,
    o.estimated_shipping_supplies_cost_local_amount,
    o.estimated_variable_gms_cost_local_amount,
    o.estimated_variable_payment_processing_cost_local_amount,
    o.estimated_variable_warehouse_cost_local_amount,
    o.shipping_cost_local_amount,
    o.tariff_revenue_local_amount,
    o.product_order_cash_margin_pre_return_local_amount,
    o.product_order_cash_margin_pre_return_local_amount_accounting,
    o.reporting_landed_cost_local_amount,
    o.reporting_landed_cost_local_amount_accounting,
    o.cash_credit_count,
    IFF(ol.order_id IS NOT NULL,ol.discounted_unit_count,0) AS discounted_unit_count,
    IFF(ol.order_id IS NOT NULL,ol.outfit_component_unit_count,0) AS outfit_component_unit_count,
    IFF(ol.order_id IS NOT NULL,ol.outfit_count,0) AS outfit_count,
    IFF(ol.order_id IS NOT NULL,ol.zero_revenue_unit_count,0) AS zero_revenue_unit_count,
    IFF(ol.order_id IS NOT NULL,ol.retail_unit_price, 0) AS retail_unit_price,
    IFF(ol.order_id IS NOT NULL,ol.air_vip_price, 0) AS air_vip_price,
    IFF(ol.order_id IS NOT NULL,ol.price_offered_local_amount, 0) AS price_offered_local_amount,
    IFF(foc.order_id IS NOT NULL,foc.noncash_credit_redeemed_local_amount, 0) AS noncash_credit_redeemed_local_amount,
    IFF(foc.order_id IS NOT NULL,foc.cash_credit_redeemed_local_amount,0) AS cash_credit_redeemed_local_amount,
    IFF(foc.order_id IS NOT NULL,foc.billed_cash_credit_redeemed_local_amount,0) AS billed_cash_credit_redeemed_local_amount,
    IFF(foc.order_id IS NOT NULL,foc.refund_cash_credit_redeemed_local_amount,0) AS refund_cash_credit_redeemed_local_amount,
    IFF(foc.order_id IS NOT NULL,foc.other_cash_credit_redeemed_local_amount,0) AS other_cash_credit_redeemed_local_amount,
    IFF(foc.order_id IS NOT NULL,foc.billed_cash_credit_redeemed_same_month_local_amount,0) AS billed_cash_credit_redeemed_same_month_local_amount,
    IFF(foc.order_id IS NOT NULL,foc.billed_cash_credit_redeemed_equivalent_count,0) AS billed_cash_credit_redeemed_equivalent_count,
    IFF(foc.order_id IS NOT NULL,foc.cash_gift_card_redeemed_local_amount,0) AS cash_gift_card_redeemed_local_amount,
    COALESCE(ol.non_token_unit_count, 0) AS non_token_unit_count,
    COALESCE(ol.non_token_subtotal_excl_tariff_amount, 0) AS non_token_subtotal_excl_tariff_amount
FROM _order_ids AS o
left JOIN edw_prod.data_model_jfb.fact_activation AS fa ON fa.activation_key = o.activation_key
left JOIN edw_prod.data_model_jfb.fact_activation AS fa_first ON fa_first.activation_key = o.first_activation_key
JOIN edw_prod.data_model_jfb.dim_store AS st ON st.store_id = o.store_id
LEFT JOIN _vip_store_id AS vsi ON vsi.store_id = st.store_id
JOIN edw_prod.data_model_jfb.dim_order_sales_channel oc ON oc.order_sales_channel_key = o.order_sales_channel_key
JOIN edw_prod.data_model_jfb.dim_order_status os ON os.order_status_key = o.order_status_key
LEFT JOIN edw_prod.data_model_jfb.fact_order_credit AS foc ON foc.order_id = o.order_id
LEFT JOIN (
    SELECT
        ol.order_id,
        SUM(IFF(ol.product_discount_local_amount > 0,ol.item_quantity,0)) AS discounted_unit_count,
        SUM(IFF(LOWER(pt.product_type_name) = 'bundle component', ol.item_quantity,0)) AS outfit_component_unit_count,
        COUNT(DISTINCT CASE WHEN ol.bundle_order_line_id <> -1 THEN ol.bundle_order_line_id END) AS outfit_count,
        SUM(IFF(ol.subtotal_excl_tariff_local_amount + ol.tariff_revenue_local_amount - ol.product_discount_local_amount  - ol.non_cash_credit_local_amount <= 0, ol.item_quantity,0))  AS zero_revenue_unit_count,
        SUM(ol.retail_unit_price) AS retail_unit_price,
        SUM(ol.air_vip_price) AS air_vip_price,
        SUM(ol.price_offered_local_amount) AS price_offered_local_amount,
        SUM(IFF(ol.token_count = 0, ol.item_quantity, 0)) AS non_token_unit_count,
        SUM(IFF(ol.token_count = 0, ol.subtotal_excl_tariff_local_amount, 0)) AS non_token_subtotal_excl_tariff_amount
    FROM edw_prod.data_model_jfb.fact_order_line ol
    JOIN edw_prod.data_model_jfb.dim_order_line_status ols ON ols.order_line_status_key = ol.order_line_status_key
    JOIN edw_prod.data_model_jfb.dim_product_type pt ON pt.product_type_key = ol.product_type_key
    WHERE
        LOWER(ols.order_line_status) <> 'cancelled'
        AND pt.is_free = 0
    GROUP BY ol.order_id
    ) AS ol ON ol.order_id = o.order_id
WHERE
    oc.is_test_order = 0
    AND LOWER(os.order_status) IN ('success', 'pending');
-- SELECT date, date_object, customer_id, order_id, count(1) FROM _order_base GROUP BY 1, 2, 3, 4 having count(1) > 1;

--select * from _order_base;

CREATE OR REPLACE TEMPORARY TABLE _product_orders as
WITH _product_orders AS (
    SELECT
        o.date,
        o.order_id,
        o.store_id,
        o.vip_store_id,
        o.finance_specialty_store,
        IFF(ds.store_brand = 'Fabletics', o.is_scrubs_customer, FALSE) AS is_scrubs_customer,
        o.store_region,
        o.customer_id,
        o.activation_local_datetime,
        o.vip_cohort_month_date,
        o.cancellation_local_datetime,
        o.first_activation_local_datetime,
        o.first_vip_cohort_month_date,
        o.first_cancellation_local_datetime,
        o.gender,
        o.is_cross_promo,
        o.is_reactivated_vip,
        o.is_retail_vip,
        o.activation_key,
        o.first_activation_key,
        o.associate_id,
        o.order_membership_classification_key,
        o.is_bop_vip,
        o.date_object,
        dc.iso_currency_code AS currency_type,
        o.reporting_usd_conversion_rate,
        o.reporting_eur_conversion_rate,
        cer.exchange_rate AS hyperion_exchange_rate,
        o.reporting_landed_cost_local_amount AS reporting_landed_cost_local_amount,
        o.reporting_landed_cost_local_amount_accounting AS reporting_landed_cost_local_amount_accounting,
        IFF(LOWER(o.order_classification_l1) = 'product order' and cash_credit_count >= 1,1,0) AS product_order_count_with_credit_redemption,
        IFF(LOWER(o.order_classification_l1) = 'product order',1,0) AS product_order_count,
        IFF(LOWER(o.order_classification_l1) = 'product order' AND o.is_product_seeding_order = FALSE, 1, 0) AS product_order_count_excl_seeding,
        IFF(LOWER(o.order_classification_l2) = 'product order'AND o.is_membership_gift,1,0) AS product_gift_order_count,
        IFF(LOWER(o.order_classification_l2) = 'gift certificate' AND o.is_membership_gift,1,0) AS gift_certificate_gift_order_count,
        IFF(LOWER(o.order_classification_l1) = 'product order',unit_count,0) AS product_order_unit_count,
        IFF(LOWER(o.order_classification_l1) = 'product order',loyalty_unit_count,0) AS product_order_loyalty_unit_count,
        IFF(LOWER(o.order_classification_l1) = 'product order',zero_revenue_unit_count,0) AS product_order_zero_revenue_unit_count,
        IFF(LOWER(o.order_sales_channel_l1) = 'billing order',1,0) AS billing_order_transaction_count,
        IFF(LOWER(o.order_classification_l2) IN ('credit billing', 'token billing'),1,0) AS billed_credit_cash_transaction_count,
        IFF(LOWER(o.order_classification_l2) IN ('credit billing', 'token billing') AND o.is_credit_billing_on_retry = 1,1,0) AS on_retry_billed_credit_cash_transaction_count,
        IFF(LOWER(o.order_sales_channel_l1) = 'billing order',cash_gross_revenue_local_amount ,0) AS billing_cash_gross_revenue,
        IFF(LOWER(o.order_classification_l1) = 'product order',subtotal_excl_tariff_local_amount ,0) AS product_order_subtotal_excl_tariff_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',tariff_revenue_local_amount,0) AS product_order_tariff_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',product_discount_local_amount,0) AS product_order_product_discount_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',shipping_discount_local_amount,0) AS product_order_shipping_discount_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',shipping_revenue_before_discount_local_amount,0) AS product_order_shipping_revenue_before_discount_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',tax_local_amount,0) AS product_order_tax_amount,
        IFF(LOWER(o.order_classification_l1) IN ('product order','reship','exchange'),cash_gross_revenue_local_amount,0) AS product_order_cash_gross_revenue_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',payment_transaction_local_amount,0) AS product_order_cash_transaction_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',cash_credit_redeemed_local_amount,0) AS product_order_cash_credit_redeemed_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',noncash_credit_redeemed_local_amount,0) AS product_order_noncash_credit_redeemed_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',discounted_unit_count,0) AS product_order_discounted_unit_count,
        IFF(LOWER(o.order_classification_l1) = 'product order' AND outfit_component_unit_count > 0,1,0) AS product_order_orders_with_outfit_count,
        IFF(LOWER(o.order_classification_l1) = 'product order',outfit_component_unit_count,0) AS product_order_outfit_component_unit_count,
        IFF(LOWER(o.order_classification_l1) = 'product order',outfit_count,0) AS product_order_outfit_count,
        IFF(LOWER(o.order_classification_l1) = 'product order',retail_unit_price, 0) AS product_order_retail_unit_price,
        IFF(LOWER(o.order_classification_l1) = 'product order',air_vip_price, 0) AS product_order_air_vip_price,
        IFF(LOWER(o.order_classification_l1) = 'product order',price_offered_local_amount, 0) AS product_order_price_offered_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',reporting_landed_cost_local_amount,0) AS product_order_landed_product_cost_amount, -- need to change this once changes to cost table are made,
        IFF(LOWER(o.order_classification_l1) = 'product order',estimated_landed_cost_local_amount,0) AS oracle_product_order_landed_product_cost_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',reporting_landed_cost_local_amount_accounting,0) AS product_order_landed_product_cost_amount_accounting,
        IFF(LOWER(o.order_classification_l1) = 'product order',estimated_landed_cost_local_amount_accounting,0) AS oracle_product_order_landed_product_cost_amount_accounting,
        IFF(LOWER(o.order_classification_l1) = 'product order',estimated_shipping_supplies_cost_local_amount,0) AS product_order_shipping_supplies_cost_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',shipping_cost_local_amount,0) AS product_order_shipping_cost_amount,
        IFF(LOWER(o.order_classification_l1) IN ('product order','reship','exchange'),misc_cogs_local_amount,0) AS product_order_misc_cogs_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',amount_to_pay,0) AS product_order_amount_to_pay,
        IFF(LOWER(o.order_classification_l1) = 'product order', non_token_unit_count, 0) AS product_order_non_token_unit_count,
        IFF(LOWER(o.order_classification_l1) = 'product order', non_token_subtotal_excl_tariff_amount, 0) AS product_order_non_token_subtotal_excl_tariff_amount,

        product_margin_pre_return_local_amount AS product_margin_pre_return, -- case statements checking for product orders are already in the FO view logic
        IFF(o.is_product_seeding_order = FALSE, product_margin_pre_return_local_amount, 0) AS product_margin_pre_return_excl_seeding,
        product_margin_pre_return_local_amount_accounting AS product_margin_pre_return_accounting,
        IFF(o.is_product_seeding_order = FALSE, product_margin_pre_return_local_amount_accounting, 0) AS product_margin_pre_return_excl_seeding_accounting,
        product_margin_pre_return_excl_shipping_local_amount AS product_margin_pre_return_excl_shipping,
        product_margin_pre_return_excl_shipping_local_amount_accounting AS product_margin_pre_return_excl_shipping_accounting,
        product_order_cash_margin_pre_return_local_amount AS product_order_cash_margin_pre_return,
        product_order_cash_margin_pre_return_local_amount_accounting AS product_order_cash_margin_pre_return_accounting,
        product_gross_revenue_local_amount AS product_gross_revenue,
        product_gross_revenue_excl_shipping_local_amount AS product_gross_revenue_excl_shipping,

        IFF(LOWER(o.order_classification_l1) = 'exchange',1,0) AS product_order_exchange_order_count,
        IFF(LOWER(o.order_classification_l1) = 'exchange',unit_count,0) AS product_order_exchange_unit_count,
        IFF(LOWER(o.order_classification_l1) = 'exchange',reporting_landed_cost_local_amount,0) AS product_order_exchange_product_cost_amount,
        IFF(LOWER(o.order_classification_l1) = 'exchange',estimated_landed_cost_local_amount,0) AS oracle_product_order_exchange_product_cost_amount,
        IFF(LOWER(o.order_classification_l1) = 'exchange',reporting_landed_cost_local_amount_accounting,0) AS product_order_exchange_product_cost_amount_accounting,
        IFF(LOWER(o.order_classification_l1) = 'exchange',estimated_landed_cost_local_amount_accounting,0) AS oracle_product_order_exchange_product_cost_amount_accounting,
        IFF(LOWER(o.order_classification_l1) = 'exchange',shipping_cost_local_amount,0) AS product_order_exchange_shipping_cost_amount,
        IFF(LOWER(o.order_classification_l1) = 'exchange',estimated_shipping_supplies_cost_local_amount,0) AS product_order_exchange_shipping_supplies_cost_amount,

        IFF(LOWER(o.order_classification_l1) = 'reship',1,0) AS product_order_reship_order_count,
        IFF(LOWER(o.order_classification_l1) = 'reship',unit_count,0) AS product_order_reship_unit_count,
        IFF(LOWER(o.order_classification_l1) = 'reship',reporting_landed_cost_local_amount,0) AS product_order_reship_product_cost_amount,
        IFF(LOWER(o.order_classification_l1) = 'reship',estimated_landed_cost_local_amount,0) AS oracle_product_order_reship_product_cost_amount,
        IFF(LOWER(o.order_classification_l1) = 'reship',reporting_landed_cost_local_amount_accounting,0) AS product_order_reship_product_cost_amount_accounting,
        IFF(LOWER(o.order_classification_l1) = 'reship',estimated_landed_cost_local_amount_accounting,0) AS oracle_product_order_reship_product_cost_amount_accounting,
        IFF(LOWER(o.order_classification_l1) = 'reship',shipping_cost_local_amount,0) AS product_order_reship_shipping_cost_amount,
        IFF(LOWER(o.order_classification_l1) = 'reship',estimated_shipping_supplies_cost_local_amount,0) AS product_order_reship_shipping_supplies_cost_amount,

        IFF(LOWER(o.order_sales_channel_l1) = 'billing order',estimated_variable_gms_cost_local_amount,0) AS billing_variable_gms_cost_amount,
        IFF(LOWER(o.order_classification_l1) IN ('product order','reship','exchange'),estimated_variable_gms_cost_local_amount,0) AS product_order_variable_gms_cost_amount,
        IFF(LOWER(o.order_sales_channel_l1) = 'billing order',estimated_variable_payment_processing_cost_local_amount,0) AS billing_payment_processing_cost_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order',estimated_variable_payment_processing_cost_local_amount,0) AS product_order_payment_processing_cost_amount,
        IFF(LOWER(o.order_classification_l1) IN ('product order','reship','exchange'),estimated_variable_warehouse_cost_local_amount,0) AS product_order_variable_warehouse_cost_amount,
        IFF(LOWER(o.order_classification_l2) in ('credit billing', 'token billing'),cash_gross_revenue_local_amount,0) AS billed_credit_cash_transaction_amount,
        IFF(LOWER(o.order_classification_l2) in ('membership fee'),cash_gross_revenue_local_amount,0) AS membership_fee_cash_transaction_amount,
        IFF(LOWER(o.order_classification_l2) in ('gift certificate'),cash_gross_revenue_local_amount,0) AS gift_card_transaction_amount,
        IFF(LOWER(o.order_classification_l2) in ('legacy credit'),cash_gross_revenue_local_amount,0) AS legacy_credit_cash_transaction_amount,
        billed_cash_credit_redeemed_local_amount AS billed_cash_credit_redeemed_amount,
        refund_cash_credit_redeemed_local_amount AS refund_cash_credit_redeemed_amount,
        other_cash_credit_redeemed_local_amount AS other_cash_credit_redeemed_amount,
        billed_cash_credit_redeemed_equivalent_count AS billed_cash_credit_redeemed_equivalent_count,
        billed_cash_credit_redeemed_same_month_local_amount AS billed_cash_credit_redeemed_same_month_amount,
        cash_gift_card_redeemed_local_amount AS cash_gift_card_redeemed_amount,
        IFF(LOWER(o.order_classification_l1) = 'product order' AND o.is_retail_ship_only_order = TRUE,1,0) AS retail_ship_only_product_order_count,
        IFF(LOWER(o.order_classification_l1) = 'product order' AND o.is_retail_ship_only_order = TRUE, o.unit_count, 0) AS retail_ship_only_product_order_unit_count,
        IFF(o.is_retail_ship_only_order = TRUE, product_gross_revenue_local_amount, 0) AS retail_ship_only_product_gross_revenue,
        IFF(o.is_retail_ship_only_order = TRUE, product_gross_revenue_excl_shipping_local_amount, 0) AS retail_ship_only_product_gross_revenue_excl_shipping,
        IFF(LOWER(o.order_classification_l1) = 'product order' AND o.is_retail_ship_only_order = TRUE, cash_gross_revenue_local_amount, 0) AS retail_ship_only_product_order_cash_gross_revenue,
        IFF(o.is_retail_ship_only_order = TRUE, product_margin_pre_return_local_amount, 0) AS retail_ship_only_product_margin_pre_return,
        IFF(o.is_retail_ship_only_order = TRUE, product_margin_pre_return_local_amount_accounting, 0) AS retail_ship_only_product_margin_pre_return_accounting,
        IFF(o.is_retail_ship_only_order = TRUE, product_margin_pre_return_excl_shipping_local_amount, 0) AS retail_ship_only_product_margin_pre_return_excl_shipping,
        IFF(o.is_retail_ship_only_order = TRUE, product_margin_pre_return_excl_shipping_local_amount_accounting, 0) AS retail_ship_only_product_margin_pre_return_excl_shipping_accounting,
        IFF(o.is_retail_ship_only_order = TRUE, product_order_cash_margin_pre_return_local_amount, 0) AS retail_ship_only_product_order_cash_margin_pre_return,
        IFF(o.is_retail_ship_only_order = TRUE, product_order_cash_margin_pre_return_local_amount_accounting, 0) AS retail_ship_only_product_order_cash_margin_pre_return_accounting,
        IFF(o.is_retail_ship_only_order = TRUE AND LOWER(o.order_classification_l1) IN ('reship','exchange'), reporting_landed_cost_local_amount,0) AS retail_ship_only_reship_exchange_product_cost_amount,
        IFF(o.is_retail_ship_only_order = TRUE AND LOWER(o.order_classification_l1) IN ('reship','exchange'), reporting_landed_cost_local_amount_accounting,0) AS retail_ship_only_reship_exchange_product_cost_amount_accounting,
        IFF(o.is_retail_ship_only_order = TRUE AND LOWER(o.order_classification_l1) IN ('reship','exchange'), shipping_cost_local_amount,0) AS retail_ship_only_reship_exchange_shipping_cost_amount,
        IFF(o.is_retail_ship_only_order = TRUE AND LOWER(o.order_classification_l1) IN ('reship','exchange'), estimated_shipping_supplies_cost_local_amount,0) AS retail_ship_only_reship_exchange_shipping_supplies_cost_amount,
        IFF(o.is_retail_ship_only_order = TRUE AND LOWER(o.order_classification_l1) = 'product order',misc_cogs_local_amount,0) AS retail_ship_only_misc_cogs_amount,
        IFF(LOWER(o.order_classification_l1) IN ('reship','exchange'),cash_gross_revenue_local_amount,0) AS product_order_reship_exchange_cash_gross_revenue
    FROM _order_base AS o
    JOIN edw_prod.data_model_jfb.dim_currency dc ON dc.currency_key = o.currency_key
    LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date AS cer ON cer.rate_date_pst = o.date_hq AND cer.src_currency = dc.iso_currency_code AND cer.dest_currency = 'GBP' AND dc.iso_currency_code IN ('DKK','SEK')
    LEFT JOIN edw_prod.data_model_jfb.dim_store ds ON ds.store_id = o.vip_store_id
)

--USD
SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    date_object,
    'usd' AS currency_object,
    'USD' AS currency_type,
    SUM(product_order_count_with_credit_redemption) AS product_order_count_with_credit_redemption,
    SUM(product_order_count) AS product_order_count,
    SUM(product_order_count_excl_seeding) AS product_order_count_excl_seeding,
    SUM(product_gift_order_count) AS product_gift_order_count,
    SUM(gift_certificate_gift_order_count) AS gift_certificate_gift_order_count,
    SUM(product_order_unit_count) AS product_order_unit_count,
    SUM(product_order_loyalty_unit_count) AS product_order_loyalty_unit_count,
    SUM(product_order_zero_revenue_unit_count) AS product_order_zero_revenue_unit_count,
    SUM(billing_order_transaction_count) AS billing_order_transaction_count,
    SUM(billed_credit_cash_transaction_count) AS billed_credit_cash_transaction_count,
    SUM(on_retry_billed_credit_cash_transaction_count) AS on_retry_billed_credit_cash_transaction_count,
    SUM(billing_cash_gross_revenue * reporting_usd_conversion_rate) AS billing_cash_gross_revenue,
    SUM(product_order_subtotal_excl_tariff_amount * reporting_usd_conversion_rate) AS product_order_subtotal_excl_tariff_amount,
    SUM(product_order_tariff_amount * reporting_usd_conversion_rate) AS product_order_tariff_amount,
    SUM(product_order_product_discount_amount * reporting_usd_conversion_rate) AS product_order_product_discount_amount,
    SUM(product_order_shipping_discount_amount * reporting_usd_conversion_rate) AS product_order_shipping_discount_amount,
    SUM(product_order_shipping_revenue_before_discount_amount * reporting_usd_conversion_rate) AS product_order_shipping_revenue_before_discount_amount,
    SUM(product_order_tax_amount * reporting_usd_conversion_rate) AS product_order_tax_amount,
    SUM(product_order_cash_gross_revenue_amount * reporting_usd_conversion_rate) AS product_order_cash_gross_revenue_amount,
    SUM(product_order_cash_transaction_amount * reporting_usd_conversion_rate) AS product_order_cash_transaction_amount,
    SUM(product_order_cash_credit_redeemed_amount * reporting_usd_conversion_rate) AS product_order_cash_credit_redeemed_amount,
    SUM(product_order_noncash_credit_redeemed_amount * reporting_usd_conversion_rate) AS product_order_noncash_credit_redeemed_amount,
    SUM(product_order_discounted_unit_count) AS product_order_discounted_unit_count,
    SUM(product_order_orders_with_outfit_count) AS product_order_orders_with_outfit_count,
    SUM(product_order_outfit_component_unit_count) AS product_order_outfit_component_unit_count,
    SUM(product_order_outfit_count) AS product_order_outfit_count,
    SUM(product_order_retail_unit_price * reporting_usd_conversion_rate) AS product_order_retail_unit_price,
    SUM(product_order_air_vip_price * reporting_usd_conversion_rate) AS product_order_air_vip_price,
    SUM(product_order_price_offered_amount * reporting_usd_conversion_rate) AS product_order_price_offered_amount,
    SUM(product_order_landed_product_cost_amount * reporting_usd_conversion_rate) AS product_order_landed_product_cost_amount,
    SUM(oracle_product_order_landed_product_cost_amount * reporting_usd_conversion_rate) AS oracle_product_order_landed_product_cost_amount,
    SUM(product_order_landed_product_cost_amount_accounting * reporting_usd_conversion_rate) AS product_order_landed_product_cost_amount_accounting,
    SUM(oracle_product_order_landed_product_cost_amount_accounting * reporting_usd_conversion_rate) AS oracle_product_order_landed_product_cost_amount_accounting,
    SUM(product_order_shipping_supplies_cost_amount * reporting_usd_conversion_rate) AS product_order_shipping_supplies_cost_amount,
    SUM(product_order_shipping_cost_amount * reporting_usd_conversion_rate) AS product_order_shipping_cost_amount,
    SUM(product_order_misc_cogs_amount * reporting_usd_conversion_rate) AS product_order_misc_cogs_amount,
    SUM(product_order_amount_to_pay * reporting_usd_conversion_rate) AS product_order_amount_to_pay,
    SUM(product_margin_pre_return * reporting_usd_conversion_rate) AS product_margin_pre_return,
    SUM(product_margin_pre_return_excl_seeding * reporting_usd_conversion_rate) AS product_margin_pre_return_excl_seeding,
    SUM(product_margin_pre_return_accounting * reporting_usd_conversion_rate) AS product_margin_pre_return_accounting,
    SUM(product_margin_pre_return_excl_seeding_accounting * reporting_usd_conversion_rate) AS product_margin_pre_return_excl_seeding_accounting,
    SUM(product_margin_pre_return_excl_shipping * reporting_usd_conversion_rate) AS product_margin_pre_return_excl_shipping,
    SUM(product_margin_pre_return_excl_shipping_accounting * reporting_usd_conversion_rate) AS product_margin_pre_return_excl_shipping_accounting,
    SUM(product_order_cash_margin_pre_return * reporting_usd_conversion_rate) AS product_order_cash_margin_pre_return,
    SUM(product_order_cash_margin_pre_return_accounting * reporting_usd_conversion_rate) AS product_order_cash_margin_pre_return_accounting,
    SUM(product_gross_revenue * reporting_usd_conversion_rate) AS product_gross_revenue,
    SUM(product_gross_revenue_excl_shipping * reporting_usd_conversion_rate) AS product_gross_revenue_excl_shipping,
    SUM(product_order_exchange_order_count) AS product_order_exchange_order_count,
    SUM(product_order_exchange_unit_count) AS product_order_exchange_unit_count,
    SUM(product_order_exchange_product_cost_amount * reporting_usd_conversion_rate) AS product_order_exchange_product_cost_amount,
    SUM(oracle_product_order_exchange_product_cost_amount * reporting_usd_conversion_rate) AS oracle_product_order_exchange_product_cost_amount,
    SUM(product_order_exchange_product_cost_amount_accounting * reporting_usd_conversion_rate) AS product_order_exchange_product_cost_amount_accounting,
    SUM(oracle_product_order_exchange_product_cost_amount_accounting * reporting_usd_conversion_rate) AS oracle_product_order_exchange_product_cost_amount_accounting,
    SUM(product_order_exchange_shipping_cost_amount * reporting_usd_conversion_rate) AS product_order_exchange_shipping_cost_amount,
    SUM(product_order_exchange_shipping_supplies_cost_amount * reporting_usd_conversion_rate) AS product_order_exchange_shipping_supplies_cost_amount,
    SUM(product_order_reship_order_count) AS product_order_reship_order_count,
    SUM(product_order_reship_unit_count) AS product_order_reship_unit_count,
    SUM(product_order_reship_product_cost_amount * reporting_usd_conversion_rate) AS product_order_reship_product_cost_amount,
    SUM(oracle_product_order_reship_product_cost_amount * reporting_usd_conversion_rate) AS oracle_product_order_reship_product_cost_amount,
    SUM(product_order_reship_product_cost_amount_accounting * reporting_usd_conversion_rate) AS product_order_reship_product_cost_amount_accounting,
    SUM(oracle_product_order_reship_product_cost_amount_accounting * reporting_usd_conversion_rate) AS oracle_product_order_reship_product_cost_amount_accounting,
    SUM(product_order_reship_shipping_cost_amount * reporting_usd_conversion_rate) AS product_order_reship_shipping_cost_amount,
    SUM(product_order_reship_shipping_supplies_cost_amount * reporting_usd_conversion_rate) AS product_order_reship_shipping_supplies_cost_amount,
    SUM(billing_variable_gms_cost_amount * reporting_usd_conversion_rate) AS billing_variable_gms_cost_amount,
    SUM(product_order_variable_gms_cost_amount * reporting_usd_conversion_rate) AS product_order_variable_gms_cost_amount,
    SUM(billing_payment_processing_cost_amount * reporting_usd_conversion_rate) AS billing_payment_processing_cost_amount,
    SUM(product_order_payment_processing_cost_amount * reporting_usd_conversion_rate) AS product_order_payment_processing_cost_amount,
    SUM(product_order_variable_warehouse_cost_amount * reporting_usd_conversion_rate) AS product_order_variable_warehouse_cost_amount,
    SUM(billed_credit_cash_transaction_amount * reporting_usd_conversion_rate) AS billed_credit_cash_transaction_amount,
    SUM(membership_fee_cash_transaction_amount * reporting_usd_conversion_rate) AS membership_fee_cash_transaction_amount,
    SUM(gift_card_transaction_amount * reporting_usd_conversion_rate) AS gift_card_transaction_amount,
    SUM(legacy_credit_cash_transaction_amount * reporting_usd_conversion_rate) AS legacy_credit_cash_transaction_amount,
    SUM(billed_cash_credit_redeemed_amount * reporting_usd_conversion_rate) AS billed_cash_credit_redeemed_amount,
    SUM(refund_cash_credit_redeemed_amount * reporting_usd_conversion_rate) AS refund_cash_credit_redeemed_amount,
    SUM(other_cash_credit_redeemed_amount * reporting_usd_conversion_rate) AS other_cash_credit_redeemed_amount,
    SUM(billed_cash_credit_redeemed_equivalent_count) AS billed_cash_credit_redeemed_equivalent_count,
    SUM(billed_cash_credit_redeemed_same_month_amount * reporting_usd_conversion_rate) AS billed_cash_credit_redeemed_same_month_amount,
    SUM(cash_gift_card_redeemed_amount * reporting_usd_conversion_rate) AS cash_gift_card_redeemed_amount,
    SUM(retail_ship_only_product_order_count) AS retail_ship_only_product_order_count,
    SUM(retail_ship_only_product_order_unit_count) AS retail_ship_only_product_order_unit_count,
    SUM(retail_ship_only_product_gross_revenue * reporting_usd_conversion_rate) AS retail_ship_only_product_gross_revenue,
    SUM(retail_ship_only_product_gross_revenue_excl_shipping * reporting_usd_conversion_rate) AS retail_ship_only_product_gross_revenue_excl_shipping,
    SUM(retail_ship_only_product_margin_pre_return * reporting_usd_conversion_rate) AS retail_ship_only_product_margin_pre_return,
    SUM(retail_ship_only_product_margin_pre_return_accounting * reporting_usd_conversion_rate) AS retail_ship_only_product_margin_pre_return_accounting,
    SUM(retail_ship_only_product_margin_pre_return_excl_shipping * reporting_usd_conversion_rate) AS retail_ship_only_product_margin_pre_return_excl_shipping,
    SUM(retail_ship_only_product_margin_pre_return_excl_shipping_accounting * reporting_usd_conversion_rate) AS retail_ship_only_product_margin_pre_return_excl_shipping_accounting,
    SUM(retail_ship_only_product_order_cash_margin_pre_return * reporting_usd_conversion_rate) AS retail_ship_only_product_order_cash_margin_pre_return,
    SUM(retail_ship_only_product_order_cash_margin_pre_return_accounting * reporting_usd_conversion_rate) AS retail_ship_only_product_order_cash_margin_pre_return_accounting,
    SUM(retail_ship_only_reship_exchange_product_cost_amount * reporting_usd_conversion_rate) AS retail_ship_only_reship_exchange_product_cost_amount,
    SUM(retail_ship_only_reship_exchange_product_cost_amount * reporting_usd_conversion_rate) AS retail_ship_only_reship_exchange_product_cost_amount_accounting,
    SUM(retail_ship_only_reship_exchange_shipping_cost_amount * reporting_usd_conversion_rate) AS retail_ship_only_reship_exchange_shipping_cost_amount,
    SUM(retail_ship_only_reship_exchange_shipping_supplies_cost_amount * reporting_usd_conversion_rate) AS retail_ship_only_reship_exchange_shipping_supplies_cost_amount,
    SUM(retail_ship_only_misc_cogs_amount * reporting_usd_conversion_rate) AS retail_ship_only_misc_cogs_amount,
    SUM(retail_ship_only_product_order_cash_gross_revenue * reporting_usd_conversion_rate) AS retail_ship_only_product_order_cash_gross_revenue,
    SUM(product_order_reship_exchange_cash_gross_revenue * reporting_usd_conversion_rate) AS product_order_reship_exchange_cash_gross_revenue,
    SUM(reporting_landed_cost_local_amount * reporting_usd_conversion_rate) AS reporting_landed_cost_local_amount,
    SUM(reporting_landed_cost_local_amount_accounting * reporting_usd_conversion_rate) AS reporting_landed_cost_local_amount_accounting,
    SUM(product_order_non_token_unit_count) AS product_order_non_token_unit_count,
    SUM(product_order_non_token_subtotal_excl_tariff_amount * reporting_usd_conversion_rate) AS product_order_non_token_subtotal_excl_tariff_amount
FROM _product_orders
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    date_object

UNION ALL

--LOCAL
SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    date_object,
    'local' AS currency_object,
    currency_type,
    SUM(product_order_count_with_credit_redemption) AS product_order_count_with_credit_redemption,
    SUM(product_order_count) AS product_order_count,
    SUM(product_order_count_excl_seeding) AS product_order_count_excl_seeding,
    SUM(product_gift_order_count) AS product_gift_order_count,
    SUM(gift_certificate_gift_order_count) AS gift_certificate_gift_order_count,
    SUM(product_order_unit_count) AS product_order_unit_count,
    SUM(product_order_loyalty_unit_count) AS product_order_loyalty_unit_count,
    SUM(product_order_zero_revenue_unit_count) AS product_order_zero_revenue_unit_count,
    SUM(billing_order_transaction_count) AS billing_order_transaction_count,
    SUM(billed_credit_cash_transaction_count) AS billed_credit_cash_transaction_count,
    SUM(on_retry_billed_credit_cash_transaction_count) AS on_retry_billed_credit_cash_transaction_count,
    SUM(billing_cash_gross_revenue) AS billing_cash_gross_revenue,
    SUM(product_order_subtotal_excl_tariff_amount) AS product_order_subtotal_excl_tariff_amount,
    SUM(product_order_tariff_amount) AS product_order_tariff_amount,
    SUM(product_order_product_discount_amount) AS product_order_product_discount_amount,
    SUM(product_order_shipping_discount_amount) AS product_order_shipping_discount_amount,
    SUM(product_order_shipping_revenue_before_discount_amount) AS product_order_shipping_revenue_before_discount_amount,
    SUM(product_order_tax_amount) AS product_order_tax_amount,
    SUM(product_order_cash_gross_revenue_amount) AS product_order_cash_gross_revenue_amount,
    SUM(product_order_cash_transaction_amount) AS product_order_cash_transaction_amount,
    SUM(product_order_cash_credit_redeemed_amount) AS product_order_cash_credit_redeemed_amount,
    SUM(product_order_noncash_credit_redeemed_amount) AS product_order_noncash_credit_redeemed_amount,
    SUM(product_order_discounted_unit_count) AS product_order_discounted_unit_count,
    SUM(product_order_orders_with_outfit_count) AS product_order_orders_with_outfit_count,
    SUM(product_order_outfit_component_unit_count) AS product_order_outfit_component_unit_count,
    SUM(product_order_outfit_count) AS product_order_outfit_count,
    SUM(product_order_retail_unit_price) AS product_order_retail_unit_price,
    SUM(product_order_air_vip_price) AS product_order_air_vip_price,
    SUM(product_order_price_offered_amount) AS product_order_price_offered_amount,
    SUM(product_order_landed_product_cost_amount) AS product_order_landed_product_cost_amount,
    SUM(oracle_product_order_landed_product_cost_amount) AS oracle_product_order_landed_product_cost_amount,
    SUM(product_order_landed_product_cost_amount_accounting) AS product_order_landed_product_cost_amount_accounting,
    SUM(oracle_product_order_landed_product_cost_amount_accounting) AS oracle_product_order_landed_product_cost_amount_accounting,
    SUM(product_order_shipping_supplies_cost_amount) AS product_order_shipping_supplies_cost_amount,
    SUM(product_order_shipping_cost_amount) AS product_order_shipping_cost_amount,
    SUM(product_order_misc_cogs_amount) AS product_order_misc_cogs_amount,
    SUM(product_order_amount_to_pay) AS product_order_amount_to_pay,
    SUM(product_margin_pre_return) AS product_margin_pre_return,
    SUM(product_margin_pre_return_excl_seeding) AS product_margin_pre_return_excl_seeding,
    SUM(product_margin_pre_return_accounting) AS product_margin_pre_return_accounting,
    SUM(product_margin_pre_return_excl_seeding_accounting) AS product_margin_pre_return_excl_seeding_accounting,
    SUM(product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping,
    SUM(product_margin_pre_return_excl_shipping_accounting) AS product_margin_pre_return_excl_shipping_accounting,
    SUM(product_order_cash_margin_pre_return) AS product_order_cash_margin_pre_return,
    SUM(product_order_cash_margin_pre_return_accounting) AS product_order_cash_margin_pre_return_accounting,
    SUM(product_gross_revenue) AS product_gross_revenue,
    SUM(product_gross_revenue_excl_shipping) AS product_gross_revenue_excl_shipping,
    SUM(product_order_exchange_order_count) AS product_order_exchange_order_count,
    SUM(product_order_exchange_unit_count) AS product_order_exchange_unit_count,
    SUM(product_order_exchange_product_cost_amount) AS product_order_exchange_product_cost_amount,
    SUM(oracle_product_order_exchange_product_cost_amount) AS oracle_product_order_exchange_product_cost_amount,
    SUM(product_order_exchange_product_cost_amount_accounting) AS product_order_exchange_product_cost_amount_accounting,
    SUM(oracle_product_order_exchange_product_cost_amount_accounting) AS oracle_product_order_exchange_product_cost_amount_accounting,
    SUM(product_order_exchange_shipping_cost_amount) AS product_order_exchange_shipping_cost_amount,
    SUM(product_order_exchange_shipping_supplies_cost_amount) AS product_order_exchange_shipping_supplies_cost_amount,
    SUM(product_order_reship_order_count) AS product_order_reship_order_count,
    SUM(product_order_reship_unit_count) AS product_order_reship_unit_count,
    SUM(product_order_reship_product_cost_amount) AS product_order_reship_product_cost_amount,
    SUM(oracle_product_order_reship_product_cost_amount) AS oracle_product_order_reship_product_cost_amount,
    SUM(product_order_reship_product_cost_amount_accounting) AS product_order_reship_product_cost_amount_accounting,
    SUM(oracle_product_order_reship_product_cost_amount_accounting) AS oracle_product_order_reship_product_cost_amount_accounting,
    SUM(product_order_reship_shipping_cost_amount) AS product_order_reship_shipping_cost_amount,
    SUM(product_order_reship_shipping_supplies_cost_amount) AS product_order_reship_shipping_supplies_cost_amount,
    SUM(billing_variable_gms_cost_amount) AS billing_variable_gms_cost_amount,
    SUM(product_order_variable_gms_cost_amount) AS product_order_variable_gms_cost_amount,
    SUM(billing_payment_processing_cost_amount) AS billing_payment_processing_cost_amount,
    SUM(product_order_payment_processing_cost_amount) AS product_order_payment_processing_cost_amount,
    SUM(product_order_variable_warehouse_cost_amount) AS product_order_variable_warehouse_cost_amount,
    SUM(billed_credit_cash_transaction_amount) AS billed_credit_cash_transaction_amount,
    SUM(membership_fee_cash_transaction_amount) AS membership_fee_cash_transaction_amount,
    SUM(gift_card_transaction_amount) AS gift_card_transaction_amount,
    SUM(legacy_credit_cash_transaction_amount) AS legacy_credit_cash_transaction_amount,
    SUM(billed_cash_credit_redeemed_amount) AS billed_cash_credit_redeemed_amount,
    SUM(refund_cash_credit_redeemed_amount) AS refund_cash_credit_redeemed_amount,
    SUM(other_cash_credit_redeemed_amount) AS other_cash_credit_redeemed_amount,
    SUM(billed_cash_credit_redeemed_equivalent_count) AS billed_cash_credit_redeemed_equivalent_count,
    SUM(billed_cash_credit_redeemed_same_month_amount) AS billed_cash_credit_redeemed_same_month_amount,
    SUM(cash_gift_card_redeemed_amount) AS cash_gift_card_redeemed_amount,
    0 AS retail_ship_only_product_order_count, -- no retail in EU
    0 AS retail_ship_only_product_order_unit_count,
    0 AS retail_ship_only_product_gross_revenue,
    0 AS retail_ship_only_product_gross_revenue_excl_shipping,
    0 AS retail_ship_only_product_margin_pre_return,
    0 AS retail_ship_only_product_margin_pre_return_accounting,
    0 AS retail_ship_only_product_margin_pre_return_excl_shipping,
    0 AS retail_ship_only_product_margin_pre_return_excl_shipping_accounting,
    0 AS retail_ship_only_product_order_cash_margin_pre_return,
    0 AS retail_ship_only_product_order_cash_margin_pre_return_accounting,
    0 AS retail_ship_only_reship_exchange_product_cost_amount,
    0 AS retail_ship_only_reship_exchange_product_cost_amount_accounting,
    0 AS retail_ship_only_reship_exchange_shipping_cost_amount,
    0 AS retail_ship_only_reship_exchange_shipping_supplies_cost_amount,
    0 AS retail_ship_only_misc_cogs_amount,
    0 AS retail_ship_only_product_order_cash_gross_revenue,
    SUM(product_order_reship_exchange_cash_gross_revenue) AS product_order_reship_exchange_cash_gross_revenue,
    SUM(reporting_landed_cost_local_amount) AS reporting_landed_cost_local_amount,
    SUM(reporting_landed_cost_local_amount_accounting) AS reporting_landed_cost_local_amount_accounting,
    SUM(product_order_non_token_unit_count) AS product_order_non_token_unit_count,
    SUM(product_order_non_token_subtotal_excl_tariff_amount * reporting_usd_conversion_rate) AS product_order_non_token_subtotal_excl_tariff_amount
FROM _product_orders
WHERE store_region = 'EU'
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    date_object,
    currency_object,
    currency_type

UNION ALL

-- HYPERION
SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    date_object,
    'hyperion' AS currency_object,
    'GBP' AS currency_type,
    SUM(product_order_count_with_credit_redemption) AS product_order_count_with_credit_redemption,
    SUM(product_order_count) AS product_order_count,
    SUM(product_order_count_excl_seeding) AS product_order_count_excl_seeding,
    SUM(product_gift_order_count) AS product_gift_order_count,
    SUM(gift_certificate_gift_order_count) AS gift_certificate_gift_order_count,
    SUM(product_order_unit_count) AS product_order_unit_count,
    SUM(product_order_loyalty_unit_count) AS product_order_loyalty_unit_count,
    SUM(product_order_zero_revenue_unit_count) AS product_order_zero_revenue_unit_count,
    SUM(billing_order_transaction_count) AS billing_order_transaction_count,
    SUM(billed_credit_cash_transaction_count) AS billed_credit_cash_transaction_count,
    SUM(on_retry_billed_credit_cash_transaction_count) AS on_retry_billed_credit_cash_transaction_count,
    SUM(billing_cash_gross_revenue * hyperion_exchange_rate) AS billing_cash_gross_revenue,
    SUM(product_order_subtotal_excl_tariff_amount * hyperion_exchange_rate) AS product_order_subtotal_excl_tariff_amount,
    SUM(product_order_tariff_amount * hyperion_exchange_rate) AS product_order_tariff_amount,
    SUM(product_order_product_discount_amount * hyperion_exchange_rate) AS product_order_product_discount_amount,
    SUM(product_order_shipping_discount_amount * hyperion_exchange_rate) AS product_order_shipping_discount_amount,
    SUM(product_order_shipping_revenue_before_discount_amount * hyperion_exchange_rate) AS product_order_shipping_revenue_before_discount_amount,
    SUM(product_order_tax_amount * hyperion_exchange_rate) AS product_order_tax_amount,
    SUM(product_order_cash_gross_revenue_amount * hyperion_exchange_rate) AS product_order_cash_gross_revenue_amount,
    SUM(product_order_cash_transaction_amount * hyperion_exchange_rate) AS product_order_cash_transaction_amount,
    SUM(product_order_cash_credit_redeemed_amount * hyperion_exchange_rate) AS product_order_cash_credit_redeemed_amount,
    SUM(product_order_noncash_credit_redeemed_amount * hyperion_exchange_rate) AS product_order_noncash_credit_redeemed_amount,
    SUM(product_order_discounted_unit_count) AS product_order_discounted_unit_count,
    SUM(product_order_orders_with_outfit_count) AS product_order_orders_with_outfit_count,
    SUM(product_order_outfit_component_unit_count) AS product_order_outfit_component_unit_count,
    SUM(product_order_outfit_count) AS product_order_outfit_count,
    SUM(product_order_retail_unit_price * hyperion_exchange_rate) AS product_order_retail_unit_price,
    SUM(product_order_air_vip_price * hyperion_exchange_rate) AS product_order_air_vip_price,
    SUM(product_order_price_offered_amount * hyperion_exchange_rate) AS product_order_price_offered_amount,
    SUM(product_order_landed_product_cost_amount * hyperion_exchange_rate) AS product_order_landed_product_cost_amount,
    SUM(oracle_product_order_landed_product_cost_amount * hyperion_exchange_rate) AS oracle_product_order_landed_product_cost_amount,
    SUM(product_order_landed_product_cost_amount_accounting * hyperion_exchange_rate) AS product_order_landed_product_cost_amount_accounting,
    SUM(oracle_product_order_landed_product_cost_amount_accounting * hyperion_exchange_rate) AS oracle_product_order_landed_product_cost_amount_accounting,
    SUM(product_order_shipping_supplies_cost_amount * hyperion_exchange_rate) AS product_order_shipping_supplies_cost_amount,
    SUM(product_order_shipping_cost_amount * hyperion_exchange_rate) AS product_order_shipping_cost_amount,
    SUM(product_order_misc_cogs_amount * hyperion_exchange_rate) AS product_order_misc_cogs_amount,
    SUM(product_order_amount_to_pay * hyperion_exchange_rate) AS product_order_amount_to_pay,
    SUM(product_margin_pre_return * hyperion_exchange_rate) AS product_margin_pre_return,
    SUM(product_margin_pre_return_excl_seeding * hyperion_exchange_rate ) AS product_margin_pre_return_excl_seeding,
    SUM(product_margin_pre_return_accounting * hyperion_exchange_rate) AS product_margin_pre_return_accounting,
    SUM(product_margin_pre_return_excl_seeding_accounting * hyperion_exchange_rate ) AS product_margin_pre_return_excl_seeding_accounting,
    SUM(product_margin_pre_return_excl_shipping * hyperion_exchange_rate) AS product_margin_pre_return_excl_shipping,
    SUM(product_margin_pre_return_excl_shipping_accounting * hyperion_exchange_rate) AS product_margin_pre_return_excl_shipping_accounting,
    SUM(product_order_cash_margin_pre_return * hyperion_exchange_rate) AS product_order_cash_margin_pre_return,
    SUM(product_order_cash_margin_pre_return_accounting * hyperion_exchange_rate) AS product_order_cash_margin_pre_return_accounting,
    SUM(product_gross_revenue * hyperion_exchange_rate) AS product_gross_revenue,
    SUM(product_gross_revenue_excl_shipping * hyperion_exchange_rate) AS product_gross_revenue_excl_shipping,
    SUM(product_order_exchange_order_count) AS product_order_exchange_order_count,
    SUM(product_order_exchange_unit_count) AS product_order_exchange_unit_count,
    SUM(product_order_exchange_product_cost_amount * hyperion_exchange_rate) AS product_order_exchange_product_cost_amount,
    SUM(oracle_product_order_exchange_product_cost_amount * hyperion_exchange_rate) AS oracle_product_order_exchange_product_cost_amount,
    SUM(product_order_exchange_product_cost_amount_accounting * hyperion_exchange_rate) AS product_order_exchange_product_cost_amount_accounting,
    SUM(oracle_product_order_exchange_product_cost_amount_accounting * hyperion_exchange_rate) AS oracle_product_order_exchange_product_cost_amount_accounting,
    SUM(product_order_exchange_shipping_cost_amount * hyperion_exchange_rate) AS product_order_exchange_shipping_cost_amount,
    SUM(product_order_exchange_shipping_supplies_cost_amount * hyperion_exchange_rate) AS product_order_exchange_shipping_supplies_cost_amount,
    SUM(product_order_reship_order_count) AS product_order_reship_order_count,
    SUM(product_order_reship_unit_count) AS product_order_reship_unit_count,
    SUM(product_order_reship_product_cost_amount * hyperion_exchange_rate) AS product_order_reship_product_cost_amount,
    SUM(oracle_product_order_reship_product_cost_amount * hyperion_exchange_rate) AS oracle_product_order_reship_product_cost_amount,
    SUM(product_order_reship_product_cost_amount_accounting * hyperion_exchange_rate) AS product_order_reship_product_cost_amount_accounting,
    SUM(oracle_product_order_reship_product_cost_amount_accounting * hyperion_exchange_rate) AS oracle_product_order_reship_product_cost_amount_accounting,
    SUM(product_order_reship_shipping_cost_amount * hyperion_exchange_rate) AS product_order_reship_shipping_cost_amount,
    SUM(product_order_reship_shipping_supplies_cost_amount * hyperion_exchange_rate) AS product_order_reship_shipping_supplies_cost_amount,
    SUM(billing_variable_gms_cost_amount * hyperion_exchange_rate) AS billing_variable_gms_cost_amount,
    SUM(product_order_variable_gms_cost_amount * hyperion_exchange_rate) AS product_order_variable_gms_cost_amount,
    SUM(billing_payment_processing_cost_amount * hyperion_exchange_rate) AS billing_payment_processing_cost_amount,
    SUM(product_order_payment_processing_cost_amount * hyperion_exchange_rate) AS product_order_payment_processing_cost_amount,
    SUM(product_order_variable_warehouse_cost_amount * hyperion_exchange_rate) AS product_order_variable_warehouse_cost_amount,
    SUM(billed_credit_cash_transaction_amount * hyperion_exchange_rate) AS billed_credit_cash_transaction_amount,
    SUM(membership_fee_cash_transaction_amount * hyperion_exchange_rate) AS membership_fee_cash_transaction_amount,
    SUM(gift_card_transaction_amount * hyperion_exchange_rate) AS gift_card_transaction_amount,
    SUM(legacy_credit_cash_transaction_amount * hyperion_exchange_rate) AS legacy_credit_cash_transaction_amount,
    SUM(billed_cash_credit_redeemed_amount * hyperion_exchange_rate) AS billed_cash_credit_redeemed_amount,
    SUM(refund_cash_credit_redeemed_amount * hyperion_exchange_rate) AS refund_cash_credit_redeemed_amount,
    SUM(other_cash_credit_redeemed_amount * hyperion_exchange_rate) AS other_cash_credit_redeemed_amount,
    SUM(billed_cash_credit_redeemed_equivalent_count) AS billed_cash_credit_redeemed_equivalent_count,
    SUM(billed_cash_credit_redeemed_same_month_amount * hyperion_exchange_rate) AS billed_cash_credit_redeemed_same_month_amount,
    SUM(cash_gift_card_redeemed_amount * hyperion_exchange_rate) AS cash_gift_card_redeemed_amount,
    0 AS retail_ship_only_product_order_count, --  no retail in eu
    0 AS retail_ship_only_product_order_unit_count,
    0 AS retail_ship_only_product_gross_revenue,
    0 AS retail_ship_only_product_gross_revenue_excl_shipping,
    0 AS retail_ship_only_product_margin_pre_return,
    0 AS retail_ship_only_product_margin_pre_return_accounting,
    0 AS retail_ship_only_product_margin_pre_return_excl_shipping,
    0 AS retail_ship_only_product_margin_pre_return_excl_shipping_accounting,
    0 AS retail_ship_only_product_order_cash_margin_pre_return,
    0 AS retail_ship_only_product_order_cash_margin_pre_return_accounting,
    0 AS retail_ship_only_reship_exchange_product_cost_amount,
    0 AS retail_ship_only_reship_exchange_product_cost_amount_accounting,
    0 AS retail_ship_only_reship_exchange_shipping_cost_amount,
    0 AS retail_ship_only_reship_exchange_shipping_supplies_cost_amount,
    0 AS retail_ship_only_misc_cogs_amount,
    0 AS retail_ship_only_product_order_cash_gross_revenue,
    SUM(product_order_reship_exchange_cash_gross_revenue * hyperion_exchange_rate) AS product_order_reship_exchange_cash_gross_revenue,
    SUM(reporting_landed_cost_local_amount * hyperion_exchange_rate) AS reporting_landed_cost_local_amount,
    SUM(reporting_landed_cost_local_amount_accounting * hyperion_exchange_rate) AS reporting_landed_cost_local_amount_accounting,
    SUM(product_order_non_token_unit_count) AS product_order_non_token_unit_count,
    SUM(product_order_non_token_subtotal_excl_tariff_amount * hyperion_exchange_rate) AS product_order_non_token_subtotal_excl_tariff_amount
FROM _product_orders
WHERE currency_type IN ('SEK','DKK')
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    date_object

--UNION ALL
---- SXUK EUR
--SELECT
--    date,
--    po.store_id,
--    vip_store_id,
--    finance_specialty_store,
--    is_scrubs_customer,
--    customer_id,
--    activation_local_datetime,
--    vip_cohort_month_date,
--    cancellation_local_datetime,
--    first_activation_local_datetime,
--    first_vip_cohort_month_date,
--    first_cancellation_local_datetime,
--    is_cross_promo,
--    gender,
--    is_reactivated_vip,
--    is_retail_vip,
--    activation_key,
--    first_activation_key,
--    associate_id,
--    order_membership_classification_key,
--    is_bop_vip,
--    date_object,
--    'eur' AS currency_object,
--    'EUR' AS currency_type,
--    SUM(product_order_count_with_credit_redemption) AS product_order_count_with_credit_redemption,
--    SUM(product_order_count) AS product_order_count,
--    SUM(product_order_count_excl_seeding) AS product_order_count_excl_seeding,
--    SUM(product_gift_order_count) AS product_gift_order_count,
--    SUM(gift_certificate_gift_order_count) AS gift_certificate_gift_order_count,
--    SUM(product_order_unit_count) AS product_order_unit_count,
--    SUM(product_order_loyalty_unit_count) AS product_order_loyalty_unit_count,
--    SUM(product_order_zero_revenue_unit_count) AS product_order_zero_revenue_unit_count,
--    SUM(billing_order_transaction_count) AS billing_order_transaction_count,
--    SUM(billed_credit_cash_transaction_count) AS billed_credit_cash_transaction_count,
--    SUM(on_retry_billed_credit_cash_transaction_count) AS on_retry_billed_credit_cash_transaction_count,
--    SUM(billing_cash_gross_revenue * reporting_eur_conversion_rate) AS billing_cash_gross_revenue,
--    SUM(product_order_subtotal_excl_tariff_amount * reporting_eur_conversion_rate) AS product_order_subtotal_excl_tariff_amount,
--    SUM(product_order_tariff_amount * reporting_eur_conversion_rate) AS product_order_tariff_amount,
--    SUM(product_order_product_discount_amount * reporting_eur_conversion_rate) AS product_order_product_discount_amount,
--    SUM(product_order_shipping_discount_amount * reporting_eur_conversion_rate) AS product_order_shipping_discount_amount,
--    SUM(product_order_shipping_revenue_before_discount_amount * reporting_eur_conversion_rate) AS product_order_shipping_revenue_before_discount_amount,
--    SUM(product_order_tax_amount * reporting_eur_conversion_rate) AS product_order_tax_amount,
--    SUM(product_order_cash_gross_revenue_amount * reporting_eur_conversion_rate) AS product_order_cash_gross_revenue_amount,
--    SUM(product_order_cash_transaction_amount * reporting_eur_conversion_rate) AS product_order_cash_transaction_amount,
--    SUM(product_order_cash_credit_redeemed_amount * reporting_eur_conversion_rate) AS product_order_cash_credit_redeemed_amount,
--    SUM(product_order_noncash_credit_redeemed_amount * reporting_eur_conversion_rate) AS product_order_noncash_credit_redeemed_amount,
--    SUM(product_order_discounted_unit_count) AS product_order_discounted_unit_count,
--    SUM(product_order_orders_with_outfit_count) AS product_order_orders_with_outfit_count,
--    SUM(product_order_outfit_component_unit_count) AS product_order_outfit_component_unit_count,
--    SUM(product_order_outfit_count) AS product_order_outfit_count,
--    SUM(product_order_retail_unit_price * reporting_eur_conversion_rate) AS product_order_retail_unit_price,
--    SUM(product_order_air_vip_price * reporting_eur_conversion_rate) AS product_order_air_vip_price,
--    SUM(product_order_price_offered_amount * reporting_eur_conversion_rate) AS product_order_price_offered_amount,
--    SUM(product_order_landed_product_cost_amount * reporting_eur_conversion_rate) AS product_order_landed_product_cost_amount,
--    SUM(oracle_product_order_landed_product_cost_amount * reporting_eur_conversion_rate) AS oracle_product_order_landed_product_cost_amount,
--    SUM(product_order_landed_product_cost_amount_accounting * reporting_eur_conversion_rate) AS product_order_landed_product_cost_amount_accounting,
--    SUM(oracle_product_order_landed_product_cost_amount_accounting * reporting_eur_conversion_rate) AS oracle_product_order_landed_product_cost_amount_accounting,
--    SUM(product_order_shipping_supplies_cost_amount * reporting_eur_conversion_rate) AS product_order_shipping_supplies_cost_amount,
--    SUM(product_order_shipping_cost_amount * reporting_eur_conversion_rate) AS product_order_shipping_cost_amount,
--    SUM(product_order_misc_cogs_amount * reporting_eur_conversion_rate) AS product_order_misc_cogs_amount,
--    SUM(product_order_amount_to_pay * reporting_eur_conversion_rate) AS product_order_amount_to_pay,
--    SUM(product_margin_pre_return * reporting_eur_conversion_rate) AS product_margin_pre_return,
--    SUM(product_margin_pre_return_excl_seeding * hyperion_exchange_rate ) AS product_margin_pre_return_excl_seeding,
--    SUM(product_margin_pre_return_accounting * reporting_eur_conversion_rate) AS product_margin_pre_return_accounting,
--    SUM(product_margin_pre_return_excl_seeding_accounting * hyperion_exchange_rate ) AS product_margin_pre_return_excl_seeding_accounting,
--    SUM(product_margin_pre_return_excl_shipping * reporting_eur_conversion_rate) AS product_margin_pre_return_excl_shipping,
--    SUM(product_margin_pre_return_excl_shipping_accounting * reporting_eur_conversion_rate) AS product_margin_pre_return_excl_shipping_accounting,
--    SUM(product_order_cash_margin_pre_return * reporting_eur_conversion_rate) AS product_order_cash_margin_pre_return,
--    SUM(product_order_cash_margin_pre_return_accounting * reporting_eur_conversion_rate) AS product_order_cash_margin_pre_return_accounting,
--    SUM(product_gross_revenue * reporting_eur_conversion_rate) AS product_gross_revenue,
--    SUM(product_gross_revenue_excl_shipping * reporting_eur_conversion_rate) AS product_gross_revenue_excl_shipping,
--    SUM(product_order_exchange_order_count) AS product_order_exchange_order_count,
--    SUM(product_order_exchange_unit_count) AS product_order_exchange_unit_count,
--    SUM(product_order_exchange_product_cost_amount * reporting_eur_conversion_rate) AS product_order_exchange_product_cost_amount,
--    SUM(oracle_product_order_exchange_product_cost_amount * reporting_eur_conversion_rate) AS oracle_product_order_exchange_product_cost_amount,
--    SUM(product_order_exchange_product_cost_amount_accounting * reporting_eur_conversion_rate) AS product_order_exchange_product_cost_amount_accounting,
--    SUM(oracle_product_order_exchange_product_cost_amount_accounting * reporting_eur_conversion_rate) AS oracle_product_order_exchange_product_cost_amount_accounting,
--    SUM(product_order_exchange_shipping_cost_amount * reporting_eur_conversion_rate) AS product_order_exchange_shipping_cost_amount,
--    SUM(product_order_exchange_shipping_supplies_cost_amount * reporting_eur_conversion_rate) AS product_order_exchange_shipping_supplies_cost_amount,
--    SUM(product_order_reship_order_count) AS product_order_reship_order_count,
--    SUM(product_order_reship_unit_count) AS product_order_reship_unit_count,
--    SUM(product_order_reship_product_cost_amount * reporting_eur_conversion_rate) AS product_order_reship_product_cost_amount,
--    SUM(oracle_product_order_reship_product_cost_amount * reporting_eur_conversion_rate) AS oracle_product_order_reship_product_cost_amount,
--    SUM(product_order_reship_product_cost_amount_accounting * reporting_eur_conversion_rate) AS product_order_reship_product_cost_amount_accounting,
--    SUM(oracle_product_order_reship_product_cost_amount_accounting * reporting_eur_conversion_rate) AS oracle_product_order_reship_product_cost_amount_accounting,
--    SUM(product_order_reship_shipping_cost_amount * reporting_eur_conversion_rate) AS product_order_reship_shipping_cost_amount,
--    SUM(product_order_reship_shipping_supplies_cost_amount * reporting_eur_conversion_rate) AS product_order_reship_shipping_supplies_cost_amount,
--    SUM(billing_variable_gms_cost_amount * reporting_eur_conversion_rate) AS billing_variable_gms_cost_amount,
--    SUM(product_order_variable_gms_cost_amount * reporting_eur_conversion_rate) AS product_order_variable_gms_cost_amount,
--    SUM(billing_payment_processing_cost_amount * reporting_eur_conversion_rate) AS billing_payment_processing_cost_amount,
--    SUM(product_order_payment_processing_cost_amount * reporting_eur_conversion_rate) AS product_order_payment_processing_cost_amount,
--    SUM(product_order_variable_warehouse_cost_amount * reporting_eur_conversion_rate) AS product_order_variable_warehouse_cost_amount,
--    SUM(billed_credit_cash_transaction_amount * reporting_eur_conversion_rate) AS billed_credit_cash_transaction_amount,
--    SUM(membership_fee_cash_transaction_amount * reporting_eur_conversion_rate) AS membership_fee_cash_transaction_amount,
--    SUM(gift_card_transaction_amount * reporting_eur_conversion_rate) AS gift_card_transaction_amount,
--    SUM(legacy_credit_cash_transaction_amount * reporting_eur_conversion_rate) AS legacy_credit_cash_transaction_amount,
--    SUM(billed_cash_credit_redeemed_amount * reporting_eur_conversion_rate) AS billed_cash_credit_redeemed_amount,
--    SUM(refund_cash_credit_redeemed_amount * reporting_eur_conversion_rate) AS refund_cash_credit_redeemed_amount,
--    SUM(other_cash_credit_redeemed_amount * reporting_eur_conversion_rate) AS other_cash_credit_redeemed_amount,
--    SUM(billed_cash_credit_redeemed_equivalent_count) AS billed_cash_credit_redeemed_equivalent_count,
--    SUM(billed_cash_credit_redeemed_same_month_amount * reporting_eur_conversion_rate) AS billed_cash_credit_redeemed_same_month_amount,
--    SUM(cash_gift_card_redeemed_amount * reporting_eur_conversion_rate) AS cash_gift_card_redeemed_amount,
--    0 AS retail_ship_only_product_order_count,
--    0 AS retail_ship_only_product_order_unit_count,
--    0 AS retail_ship_only_product_gross_revenue,
--    0 AS retail_ship_only_product_gross_revenue_excl_shipping,
--    0 AS retail_ship_only_product_margin_pre_return,
--    0 AS retail_ship_only_product_margin_pre_return_accounting,
--    0 AS retail_ship_only_product_margin_pre_return_excl_shipping,
--    0 AS retail_ship_only_product_margin_pre_return_excl_shipping_accounting,
--    0 AS retail_ship_only_product_order_cash_margin_pre_return,
--    0 AS retail_ship_only_product_order_cash_margin_pre_return_accounting,
--    0 AS retail_ship_only_reship_exchange_product_cost_amount,
--    0 AS retail_ship_only_reship_exchange_product_cost_amount_accounting,
--    0 AS retail_ship_only_reship_exchange_shipping_cost_amount,
--    0 AS retail_ship_only_reship_exchange_shipping_supplies_cost_amount,
--    0 AS retail_ship_only_misc_cogs_amount,
--    0 AS retail_ship_only_product_order_cash_gross_revenue,
--    SUM(product_order_reship_exchange_cash_gross_revenue * reporting_eur_conversion_rate) AS product_order_reship_exchange_cash_gross_revenue,
--    SUM(reporting_landed_cost_local_amount * reporting_eur_conversion_rate) AS reporting_landed_cost_local_amount,
--    SUM(reporting_landed_cost_local_amount_accounting * reporting_eur_conversion_rate) AS reporting_landed_cost_local_amount_accounting,
--    SUM(product_order_non_token_unit_count) AS product_order_non_token_unit_count,
--    SUM(product_order_non_token_subtotal_excl_tariff_amount * reporting_eur_conversion_rate) AS product_order_non_token_subtotal_excl_tariff_amount
--FROM _product_orders AS po
--JOIN edw_prod.data_model_jfb.dim_store AS st ON st.store_id = po.store_id
--WHERE
--    st.store_brand = 'Savage X'
--    AND st.store_country = 'UK'
--GROUP BY
--    date,
--    po.store_id,
--    vip_store_id,
--    finance_specialty_store,
--    is_scrubs_customer,
--    customer_id,
--    activation_local_datetime,
--    vip_cohort_month_date,
--    cancellation_local_datetime,
--    first_activation_local_datetime,
--    first_vip_cohort_month_date,
--    first_cancellation_local_datetime,
--    is_cross_promo,
--    gender,
--    is_reactivated_vip,
--    is_retail_vip,
--    activation_key,
--    first_activation_key,
--    associate_id,
--    order_membership_classification_key,
--    is_bop_vip,
--    date_object
;

CREATE OR REPLACE TEMPORARY TABLE _refund_base AS
SELECT
    fr.refund_completion_local_datetime :: DATE AS date,
    CAST(convert_timezone('America/Los_Angeles',fr.refund_completion_local_datetime) AS DATE) AS date_hq,
    fr.order_id,
    fr.store_id,
    IFF(fa.sub_store_id IS NOT NULL AND fa.sub_store_id NOT IN (-1,0), fa.sub_store_id, vsi.vip_store_id) AS vip_store_id,
    cd.finance_specialty_store,
    IFF(ds.store_brand = 'Fabletics', cd.is_scrubs_customer, FALSE) AS is_scrubs_customer,
    st.store_region,
    fo.customer_id,
    fa.activation_local_datetime,
    fa.vip_cohort_month_date,
    fa.cancellation_local_datetime,
    fa_first.activation_local_datetime AS first_activation_local_datetime,
    fa_first.vip_cohort_month_date AS first_vip_cohort_month_date,
    fa_first.cancellation_local_datetime AS first_cancellation_local_datetime,
    cd.is_cross_promo,
    cd.gender,
    fa.is_reactivated_vip,
    fa.is_retail_vip,
    fr.activation_key,
    fr.first_activation_key,
    fr.refund_administrator_id AS associate_id,
    fo.order_membership_classification_key,
    IFF(fa.vip_cohort_month_date < date_trunc(month,fr.refund_completion_local_datetime :: DATE) AND fa.cancellation_local_datetime > date_trunc(month,fr.refund_completion_local_datetime :: DATE),1,0) AS is_bop_vip,
    fo.reporting_usd_conversion_rate,
    fo.reporting_eur_conversion_rate,
    dc.iso_currency_code AS currency_type,
     IFF(osc.order_classification_l1 = 'Product Order',fr.cash_refund_local_amount,0) AS product_order_cash_refund_amount,
     IFF(osc.order_sales_channel_l1 = 'Billing Order',fr.cash_refund_local_amount,0) AS billing_cash_refund_amount,
     IFF(osc.order_classification_l1 = 'Product Order',fr.store_credit_refund_local_amount,0) AS product_order_credit_refund_amount,
     IFF(osc.order_classification_l1 = 'Product Order',fr.cash_store_credit_refund_local_amount + fr.unknown_store_credit_refund_local_amount,0) AS product_order_cash_credit_refund_amount,
     IFF(osc.order_classification_l1 = 'Product Order',fr.noncash_store_credit_refund_local_amount,0) AS product_order_noncash_credit_refund_amount,
     IFF(osc.order_classification_l2 IN ('Credit Billing', 'Token Billing'), fr.cash_refund_local_amount,0) AS billed_credit_cash_refund_amount,
     IFF(osc.order_classification_l2 IN ('Credit Billing', 'Token Billing'), 1,0) AS billed_credit_cash_refund_count,
     IFF(osc.order_classification_l2 = 'Membership Fee', fr.cash_refund_local_amount,0) AS membership_fee_cash_refund_amount,
     IFF(osc.order_classification_l2 = 'Gift Certificate', fr.cash_refund_local_amount,0) AS gift_card_cash_refund_amount,
     IFF(osc.order_classification_l2 = 'Legacy Credit', fr.cash_refund_local_amount,0) AS legacy_credit_cash_refund_amount,
     IFF(osc.is_retail_ship_only_order = TRUE AND osc.order_classification_l1 = 'Product Order',fr.cash_refund_local_amount,0) AS retail_ship_only_cash_refund_amount,
     IFF(osc.is_retail_ship_only_order = TRUE AND osc.order_classification_l1 = 'Product Order',fr.cash_store_credit_refund_local_amount,0) AS retail_ship_only_cash_credit_refund_amount
FROM _customer_date AS cd
JOIN edw_prod.data_model_jfb.fact_refund AS fr ON fr.customer_id = cd.customer_id AND cd.date = fr.refund_completion_local_datetime :: DATE
LEFT JOIN edw_prod.data_model_jfb.fact_order AS fo ON fr.order_id = fo.order_id
JOIN edw_prod.data_model_jfb.dim_order_sales_channel AS osc ON osc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model_jfb.dim_store AS st ON st.store_id = fr.store_id
LEFT JOIN _vip_store_id AS vsi ON vsi.store_id = fr.store_id
JOIN edw_prod.data_model_jfb.dim_refund_status rs ON rs.refund_status_key = fr.refund_status_key
JOIN edw_prod.data_model_jfb.dim_currency dc ON dc.currency_key = fo.currency_key
left JOIN edw_prod.data_model_jfb.fact_activation AS fa ON fa.activation_key = fr.activation_key
left JOIN edw_prod.data_model_jfb.fact_activation AS fa_first ON fa_first.activation_key = fr.activation_key
JOIN edw_prod.data_model_jfb.dim_store AS ds ON ds.store_id = IFF(fa.sub_store_id IS NOT NULL AND fa.sub_store_id NOT IN (-1,0), fa.sub_store_id, vsi.vip_store_id)
WHERE LOWER(rs.refund_status) = 'refunded'
  AND fr.is_chargeback = 0
  AND st.is_core_store = TRUE;

CREATE OR REPLACE TEMPORARY TABLE _refunds AS
SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
     'usd' AS currency_object,
     'USD' AS currency_type,
     SUM(product_order_cash_refund_amount * reporting_usd_conversion_rate) AS product_order_cash_refund_amount,
     SUM(billing_cash_refund_amount * reporting_usd_conversion_rate) AS billing_cash_refund_amount,
     SUM(product_order_credit_refund_amount * reporting_usd_conversion_rate) AS product_order_credit_refund_amount,
     SUM(product_order_cash_credit_refund_amount * reporting_usd_conversion_rate) AS product_order_cash_credit_refund_amount,
     SUM(product_order_noncash_credit_refund_amount * reporting_usd_conversion_rate) AS product_order_noncash_credit_refund_amount,
     SUM(billed_credit_cash_refund_amount * reporting_usd_conversion_rate) AS billed_credit_cash_refund_amount,
     SUM(membership_fee_cash_refund_amount * reporting_usd_conversion_rate) AS membership_fee_cash_refund_amount,
     SUM(gift_card_cash_refund_amount * reporting_usd_conversion_rate) AS gift_card_cash_refund_amount,
     SUM(legacy_credit_cash_refund_amount * reporting_usd_conversion_rate) AS legacy_credit_cash_refund_amount,
     SUM(retail_ship_only_cash_refund_amount * reporting_usd_conversion_rate) AS retail_ship_only_cash_refund_amount,
     SUM(retail_ship_only_cash_credit_refund_amount * reporting_usd_conversion_rate) AS retail_ship_only_cash_credit_refund_amount,
     SUM(billed_credit_cash_refund_count) AS billed_credit_cash_refund_count
FROM _refund_base
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip

UNION ALL

SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    'local' AS currency_object,
    currency_type,
     SUM(product_order_cash_refund_amount) AS product_order_cash_refund_amount,
     SUM(billing_cash_refund_amount) AS billing_cash_refund_amount,
     SUM(product_order_credit_refund_amount) AS product_order_credit_refund_amount,
     SUM(product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount,
     SUM(product_order_noncash_credit_refund_amount) AS product_order_noncash_credit_refund_amount,
     SUM(billed_credit_cash_refund_amount) AS billed_credit_cash_refund_amount,
     SUM(membership_fee_cash_refund_amount) AS membership_fee_cash_refund_amount,
     SUM(gift_card_cash_refund_amount) AS gift_card_cash_refund_amount,
     SUM(legacy_credit_cash_refund_amount) AS legacy_credit_cash_refund_amount,
     0 AS retail_ship_only_cash_refund_amount, -- retail doesn't exist in EU
     0 AS retail_ship_only_cash_credit_refund_amount,
     SUM(billed_credit_cash_refund_count) AS billed_credit_cash_refund_count
FROM _refund_base AS rb
WHERE store_region = 'EU'
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    currency_object,
    currency_type

UNION ALL

SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    'hyperion' AS currency_object,
    cer.dest_currency AS currency_type,
     SUM(product_order_cash_refund_amount * exchange_rate) AS product_order_cash_refund_amount,
     SUM(billing_cash_refund_amount * exchange_rate) AS billing_cash_refund_amount,
     SUM(product_order_credit_refund_amount * exchange_rate) AS product_order_credit_refund_amount,
     SUM(product_order_cash_credit_refund_amount * exchange_rate) AS product_order_cash_credit_refund_amount,
     SUM(product_order_noncash_credit_refund_amount * exchange_rate) AS product_order_noncash_credit_refund_amount,
     SUM(billed_credit_cash_refund_amount * exchange_rate) AS billed_credit_cash_refund_amount,
     SUM(membership_fee_cash_refund_amount * exchange_rate) AS membership_fee_cash_refund_amount,
     SUM(gift_card_cash_refund_amount * exchange_rate) AS gift_card_cash_refund_amount,
     SUM(legacy_credit_cash_refund_amount * exchange_rate) AS legacy_credit_cash_refund_amount,
     0 AS retail_ship_only_cash_refund_amount, -- retail doesn't exist in EU
     0 AS retail_ship_only_cash_credit_refund_amount,
     SUM(billed_credit_cash_refund_count) AS billed_credit_cash_refund_count
FROM _refund_base rb
JOIN edw_prod.reference.currency_exchange_rate_by_date AS cer ON cer.rate_date_pst = rb.date_hq AND cer.src_currency = rb.currency_type
WHERE
    LOWER(rb.currency_type) IN ('sek','dkk')
    AND LOWER(cer.dest_currency) = 'gbp'
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    currency_object,
    cer.dest_currency

--UNION ALL
--
--SELECT
--    date,
--    rb.store_id,
--    vip_store_id,
--    finance_specialty_store,
--    is_scrubs_customer,
--    customer_id,
--    activation_local_datetime,
--    vip_cohort_month_date,
--    cancellation_local_datetime,
--    first_activation_local_datetime,
--    first_vip_cohort_month_date,
--    first_cancellation_local_datetime,
--    is_cross_promo,
--    gender,
--    is_reactivated_vip,
--    is_retail_vip,
--    activation_key,
--    first_activation_key,
--    associate_id,
--    order_membership_classification_key,
--    is_bop_vip,
--     'eur' AS currency_object,
--     'EUR' AS currency_type,
--     SUM(product_order_cash_refund_amount * reporting_eur_conversion_rate) AS product_order_cash_refund_amount,
--     SUM(billing_cash_refund_amount * reporting_eur_conversion_rate) AS billing_cash_refund_amount,
--     SUM(product_order_credit_refund_amount * reporting_eur_conversion_rate) AS product_order_credit_refund_amount,
--     SUM(product_order_cash_credit_refund_amount * reporting_eur_conversion_rate) AS product_order_cash_credit_refund_amount,
--     SUM(product_order_noncash_credit_refund_amount * reporting_eur_conversion_rate) AS product_order_noncash_credit_refund_amount,
--     SUM(billed_credit_cash_refund_amount * reporting_eur_conversion_rate) AS billed_credit_cash_refund_amount,
--     SUM(membership_fee_cash_refund_amount * reporting_eur_conversion_rate) AS membership_fee_cash_refund_amount,
--     SUM(gift_card_cash_refund_amount * reporting_eur_conversion_rate) AS gift_card_cash_refund_amount,
--     SUM(legacy_credit_cash_refund_amount * reporting_eur_conversion_rate) AS legacy_credit_cash_refund_amount,
--     0 AS retail_ship_only_cash_refund_amount, -- retail doesn't exist in EU
--     0 AS retail_ship_only_cash_credit_refund_amount,
--     SUM(billed_credit_cash_refund_count) AS billed_credit_cash_refund_count
--FROM _refund_base AS rb
--JOIN edw_prod.data_model_jfb.dim_store AS st ON st.store_id = rb.store_id
--WHERE
--    st.store_brand = 'Savage X'
--    AND st.store_country = 'UK'
--GROUP BY
--    date,
--    rb.store_id,
--    vip_store_id,
--    finance_specialty_store,
--    is_scrubs_customer,
--    customer_id,
--    activation_local_datetime,
--    vip_cohort_month_date,
--    cancellation_local_datetime,
--    first_activation_local_datetime,
--    first_vip_cohort_month_date,
--    first_cancellation_local_datetime,
--    is_cross_promo,
--    gender,
--    is_reactivated_vip,
--    is_retail_vip,
--    activation_key,
--    first_activation_key,
--    associate_id,
--    order_membership_classification_key,
--    is_bop_vip
;

CREATE OR REPLACE TEMPORARY TABLE _chargeback_base AS
SELECT
    fc.chargeback_datetime :: DATE AS date,
    CAST(convert_timezone('America/Los_Angeles',fc.chargeback_datetime) AS DATE) AS date_hq,
    fc.order_id,
    IFF(fo.order_id = -1, fc.store_id, fo.store_id) AS store_id,
    IFF(fa.sub_store_id IS NOT NULL AND fa.sub_store_id NOT IN (-1,0), fa.sub_store_id, vsi.vip_store_id) AS vip_store_id,
    cd.finance_specialty_store,
    IFF(ds.store_brand = 'Fabletics', cd.is_scrubs_customer, FALSE) AS is_scrubs_customer,
    IFF(fo.order_id = -1, fc.customer_id, fo.customer_id) AS customer_id,
    fa.activation_local_datetime,
    fa.vip_cohort_month_date,
    fa.cancellation_local_datetime,
    fa_first.activation_local_datetime AS first_activation_local_datetime,
    fa_first.vip_cohort_month_date AS first_vip_cohort_month_date,
    fa_first.cancellation_local_datetime AS first_cancellation_local_datetime,
    cd.is_cross_promo,
    cd.gender,
    fa.is_reactivated_vip,
    COALESCE(fa.is_retail_vip,false) AS is_retail_vip,
    st.store_region,
    fc.activation_key,
    fc.first_activation_key,
    fo.administrator_id AS associate_id,
    fo.order_membership_classification_key,
    IFF(fa.vip_cohort_month_date < date_trunc(month,fc.chargeback_datetime :: DATE) AND fa.cancellation_local_datetime > date_trunc(month,fc.chargeback_datetime :: DATE),1,0) AS is_bop_vip,
    fc.chargeback_date_usd_conversion_rate,
    fc.chargeback_date_eur_conversion_rate,
    IFF(fo.order_id = -1,sc.currency_code,dc.iso_currency_code) AS currency_type,
    IFF(osc.order_classification_l1 = 'Product Order' OR fc.customer_id < -1,fc.chargeback_local_amount,0) AS product_order_cash_chargeback_amount,
    IFF(osc.order_sales_channel_l1 = 'Billing Order',fc.chargeback_local_amount,0) AS billing_cash_chargeback_amount,
    IFF(osc.order_classification_l2 IN ('Credit Billing', 'Token Billing'), fc.chargeback_local_amount,0) AS billed_credit_cash_chargeback_amount,
    IFF(osc.order_classification_l2 = 'Membership Fee', fc.chargeback_local_amount,0) AS membership_fee_cash_chargeback_amount,
    IFF(osc.order_classification_l2 = 'Gift Certificate', fc.chargeback_local_amount,0) AS gift_card_cash_chargeback_amount,
    IFF(osc.order_classification_l2 = 'Legacy Credit', fc.chargeback_local_amount,0) AS legacy_credit_cash_chargeback_amount,
    IFF(osc.is_retail_ship_only_order = TRUE AND osc.order_classification_l1 = 'Product Order',fc.chargeback_local_amount,0) AS retail_ship_only_cash_chargeback_amount
FROM _customer_date AS cd
JOIN edw_prod.data_model_jfb.fact_chargeback fc ON fc.customer_id = cd.customer_id AND cd.date = fc.chargeback_datetime :: DATE
JOIN edw_prod.data_model_jfb.fact_order fo ON fc.order_id = fo.order_id
JOIN edw_prod.data_model_jfb.dim_order_sales_channel osc ON osc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model_jfb.dim_store st ON st.store_id = fc.store_id
LEFT JOIN _vip_store_id AS vsi ON vsi.store_id = fc.store_id
JOIN edw_prod.data_model_jfb.dim_currency dc ON dc.currency_key = fo.currency_key
LEFT JOIN edw_prod.reference.store_currency AS sc
    ON sc.store_id = fc.store_id
    AND fc.chargeback_datetime::timestamp_ltz BETWEEN sc.effective_start_date AND sc.effective_end_date
LEFT JOIN edw_prod.data_model_jfb.fact_activation AS fa ON fa.activation_key = COALESCE(fc.activation_key,-1)
LEFT JOIN edw_prod.data_model_jfb.fact_activation AS fa_first ON fa_first.activation_key = COALESCE(fc.first_activation_key,-1)
JOIN edw_prod.data_model_jfb.dim_store ds ON ds.store_id = IFF(fa.sub_store_id IS NOT NULL AND fa.sub_store_id NOT IN (-1,0), fa.sub_store_id, vsi.vip_store_id)
WHERE st.is_core_store = TRUE;

CREATE OR REPLACE TEMPORARY TABLE _chargebacks AS
SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    'usd' AS currency_object,
    'USD' AS currency_type,
    SUM(product_order_cash_chargeback_amount * chargeback_date_usd_conversion_rate) AS product_order_cash_chargeback_amount,
    SUM(billing_cash_chargeback_amount * chargeback_date_usd_conversion_rate) AS billing_cash_chargeback_amount,
    SUM(billed_credit_cash_chargeback_amount * chargeback_date_usd_conversion_rate) AS billed_credit_cash_chargeback_amount,
    SUM(membership_fee_cash_chargeback_amount * chargeback_date_usd_conversion_rate) AS membership_fee_cash_chargeback_amount,
    SUM(gift_card_cash_chargeback_amount * chargeback_date_usd_conversion_rate) AS gift_card_cash_chargeback_amount,
    SUM(legacy_credit_cash_chargeback_amount * chargeback_date_usd_conversion_rate) AS legacy_credit_cash_chargeback_amount,
    SUM(retail_ship_only_cash_chargeback_amount * chargeback_date_usd_conversion_rate) AS retail_ship_only_cash_chargeback_amount
FROM _chargeback_base
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip

UNION ALL

SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    'local' AS currency_object,
    currency_type,
    SUM(product_order_cash_chargeback_amount) AS product_order_cash_chargeback_amount,
    SUM(billing_cash_chargeback_amount) AS billing_cash_chargeback_amount,
    SUM(billed_credit_cash_chargeback_amount) AS billed_credit_cash_chargeback_amount,
    SUM(membership_fee_cash_chargeback_amount) AS membership_fee_cash_chargeback_amount,
    SUM(gift_card_cash_chargeback_amount) AS gift_card_cash_chargeback_amount,
    SUM(legacy_credit_cash_chargeback_amount) AS legacy_credit_cash_chargeback_amount,
    0 AS retail_ship_only_cash_chargeback_amount
FROM _chargeback_base AS cb
WHERE store_region = 'EU'
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    currency_object,
    currency_type

UNION ALL

SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    'hyperion' AS currency_object,
    cer.dest_currency AS currency_type,
    SUM(product_order_cash_chargeback_amount * cer.exchange_rate) AS product_order_cash_chargeback_amount,
    SUM(billing_cash_chargeback_amount * cer.exchange_rate) AS billing_cash_chargeback_amount,
    SUM(billed_credit_cash_chargeback_amount * cer.exchange_rate) AS billed_credit_cash_chargeback_amount,
    SUM(membership_fee_cash_chargeback_amount * cer.exchange_rate) AS membership_fee_cash_chargeback_amount,
    SUM(gift_card_cash_chargeback_amount * cer.exchange_rate) AS gift_card_cash_chargeback_amount,
    SUM(legacy_credit_cash_chargeback_amount * cer.exchange_rate) AS legacy_credit_cash_chargeback_amount,
    0 AS retail_ship_only_cash_chargeback_amount
FROM _chargeback_base AS cb
JOIN edw_prod.reference.currency_exchange_rate_by_date AS cer ON cer.rate_date_pst = cb.date_hq AND cer.src_currency = cb.currency_type
where
    LOWER(cb.currency_type) IN ('sek','dkk')
    AND LOWER(cer.dest_currency) = 'gbp'
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    currency_object,
    cer.dest_currency

--UNION ALL
--
--SELECT
--    date,
--    cb.store_id,
--    vip_store_id,
--    finance_specialty_store,
--    is_scrubs_customer,
--    customer_id,
--    activation_local_datetime,
--    vip_cohort_month_date,
--    cancellation_local_datetime,
--    first_activation_local_datetime,
--    first_vip_cohort_month_date,
--    first_cancellation_local_datetime,
--    is_cross_promo,
--    gender,
--    is_reactivated_vip,
--    is_retail_vip,
--    activation_key,
--    first_activation_key,
--    associate_id,
--    order_membership_classification_key,
--    is_bop_vip,
--    'eur' AS currency_object,
--    'EUR' AS currency_type,
--    SUM(product_order_cash_chargeback_amount * chargeback_date_eur_conversion_rate) AS product_order_cash_chargeback_amount,
--    SUM(billing_cash_chargeback_amount * chargeback_date_eur_conversion_rate) AS billing_cash_chargeback_amount,
--    SUM(billed_credit_cash_chargeback_amount * chargeback_date_eur_conversion_rate) AS billed_credit_cash_chargeback_amount,
--    SUM(membership_fee_cash_chargeback_amount * chargeback_date_eur_conversion_rate) AS membership_fee_cash_chargeback_amount,
--    SUM(gift_card_cash_chargeback_amount * chargeback_date_eur_conversion_rate) AS gift_card_cash_chargeback_amount,
--    SUM(legacy_credit_cash_chargeback_amount * chargeback_date_eur_conversion_rate) AS legacy_credit_cash_chargeback_amount,
--    0 AS retail_ship_only_cash_chargeback_amount
--FROM _chargeback_base AS cb
--JOIN edw_prod.data_model_jfb.dim_store AS st ON st.store_id = cb.store_id
--WHERE
--    st.store_brand = 'Savage X'
--    AND st.store_country = 'UK'
--GROUP BY
--    date,
--    cb.store_id,
--    vip_store_id,
--    finance_specialty_store,
--    is_scrubs_customer,
--    customer_id,
--    activation_local_datetime,
--    vip_cohort_month_date,
--    cancellation_local_datetime,
--    first_activation_local_datetime,
--    first_vip_cohort_month_date,
--    first_cancellation_local_datetime,
--    is_cross_promo,
--    gender,
--    is_reactivated_vip,
--    is_retail_vip,
--    activation_key,
--    first_activation_key,
--    associate_id,
--    order_membership_classification_key,
--    is_bop_vip
;

CREATE OR REPLACE TEMPORARY TABLE _return_base AS
SELECT
    frl.return_completion_local_datetime :: DATE AS date,
    CAST(CONVERT_TIMEZONE('America/Los_Angeles',frl.return_completion_local_datetime)AS DATE) AS date_hq,
    fo.order_id,
    frl.store_id,
    IFF(fa.sub_store_id IS NOT NULL AND fa.sub_store_id NOT IN (-1,0), fa.sub_store_id, vsi.vip_store_id) AS vip_store_id,
    cd.finance_specialty_store,
    IFF(ds.store_brand = 'Fabletics', cd.is_scrubs_customer, FALSE) AS is_scrubs_customer,
    st.store_region,
    frl.customer_id,
    fa.activation_local_datetime,
    fa.vip_cohort_month_date,
    fa.cancellation_local_datetime,
    fa_first.activation_local_datetime AS first_activation_local_datetime,
    fa_first.vip_cohort_month_date AS first_vip_cohort_month_date,
    fa_first.cancellation_local_datetime AS first_cancellation_local_datetime,
    cd.is_cross_promo,
    cd.gender,
    fa.is_reactivated_vip,
    fa.is_retail_vip,
    frl.activation_key,
    frl.first_activation_key,
    frl.administrator_id AS associate_id,
    fo.order_membership_classification_key,
    IFF(fa.vip_cohort_month_date < date_trunc(month,frl.return_completion_local_datetime :: DATE) AND fa.cancellation_local_datetime > date_trunc(month,frl.return_completion_local_datetime :: DATE),1,0) AS is_bop_vip,
    fo.reporting_usd_conversion_rate,
    fo.reporting_eur_conversion_rate,
    dc.iso_currency_code AS currency_type,
    frl.return_item_quantity AS product_order_return_unit_count,
    frl.estimated_return_shipping_cost_local_amount AS product_order_return_shipping_cost_amount,
    frl.estimated_returned_product_cost_local_amount_resaleable AS product_order_cost_product_returned_resaleable_amount,
    frl.estimated_returned_product_cost_local_amount_resaleable_accounting AS product_order_cost_product_returned_resaleable_amount_accounting,
    frl.estimated_returned_product_cost_local_amount_damaged AS product_order_cost_product_returned_damaged_amount,
    frl.estimated_returned_product_cost_local_amount_damaged_accounting AS product_order_cost_product_returned_damaged_amount_accounting,
    IFF(osc.is_retail_ship_only_order = TRUE, frl.estimated_return_shipping_cost_local_amount, 0) AS retail_ship_only_return_shipping_cost,
    IFF(osc.is_retail_ship_only_order = TRUE, frl.estimated_returned_product_cost_local_amount_resaleable, 0) AS retail_ship_only_product_returned_resaleable_amount,
    IFF(osc.is_retail_ship_only_order = TRUE, frl.estimated_returned_product_cost_local_amount_resaleable_accounting, 0) AS retail_ship_only_product_returned_resaleable_amount_accounting
FROM _customer_date AS cd
JOIN edw_prod.data_model_jfb.fact_return_line AS frl ON cd.customer_id = frl.customer_id AND cd.date = frl.return_completion_local_datetime :: DATE
JOIN edw_prod.data_model_jfb.fact_order AS fo ON fo.order_id = frl.order_id
JOIN edw_prod.data_model_jfb.dim_order_sales_channel AS osc ON osc.order_sales_channel_key = fo.order_sales_channel_key
JOIN edw_prod.data_model_jfb.dim_store AS st ON st.store_id = frl.store_id
LEFT JOIN _vip_store_id AS vsi ON vsi.store_id = frl.store_id
LEFT JOIN edw_prod.data_model_jfb.dim_order_membership_classification AS domc ON fo.order_membership_classification_key = domc.order_membership_classification_key
JOIN edw_prod.data_model_jfb.dim_return_status AS rs ON rs.return_status_key = frl.return_status_key
JOIN edw_prod.data_model_jfb.dim_currency AS dc ON dc.currency_key = fo.currency_key
left JOIN edw_prod.data_model_jfb.fact_activation AS fa ON fa.activation_key = frl.activation_key
left JOIN edw_prod.data_model_jfb.fact_activation AS fa_first ON fa_first.activation_key = frl.first_activation_key
JOIN edw_prod.data_model_jfb.dim_store AS ds ON ds.store_id = IFF(fa.sub_store_id IS NOT NULL AND fa.sub_store_id NOT IN (-1,0), fa.sub_store_id, vsi.vip_store_id)
WHERE (rs.return_status = 'Resolved' OR frl.rma_product_id <> -1)
  AND return_completion_local_datetime <> '9999-12-31 00:00:00.000000000 -08:00'
  AND st.is_core_store = TRUE;

CREATE OR REPLACE TEMPORARY TABLE _returns AS
SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    'usd' AS currency_object,
    'USD' AS currency_type,
    SUM(product_order_return_unit_count) AS product_order_return_unit_count,
    SUM(product_order_return_shipping_cost_amount * reporting_usd_conversion_rate) AS product_order_return_shipping_cost_amount,
    SUM(product_order_cost_product_returned_resaleable_amount * reporting_usd_conversion_rate) AS product_order_cost_product_returned_resaleable_amount,
    SUM(product_order_cost_product_returned_resaleable_amount_accounting * reporting_usd_conversion_rate) AS product_order_cost_product_returned_resaleable_amount_accounting,
    SUM(product_order_cost_product_returned_damaged_amount * reporting_usd_conversion_rate) AS product_order_cost_product_returned_damaged_amount,
    SUM(product_order_cost_product_returned_damaged_amount_accounting * reporting_usd_conversion_rate) AS product_order_cost_product_returned_damaged_amount_accounting,
    SUM(retail_ship_only_return_shipping_cost * reporting_usd_conversion_rate) AS retail_ship_only_return_shipping_cost,
    SUM(retail_ship_only_product_returned_resaleable_amount * reporting_usd_conversion_rate) AS retail_ship_only_product_returned_resaleable_amount,
    SUM(retail_ship_only_product_returned_resaleable_amount_accounting * reporting_usd_conversion_rate) AS retail_ship_only_product_returned_resaleable_amount_accounting
from _return_base
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip

UNION ALL

SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    'local' AS currency_object,
     currency_type,
    SUM(product_order_return_unit_count) AS product_order_return_unit_count,
    SUM(product_order_return_shipping_cost_amount) AS product_order_return_shipping_cost_amount,
    SUM(product_order_cost_product_returned_resaleable_amount) AS product_order_cost_product_returned_resaleable_amount,
    SUM(product_order_cost_product_returned_resaleable_amount_accounting) AS product_order_cost_product_returned_resaleable_amount_accounting,
    SUM(product_order_cost_product_returned_damaged_amount) AS product_order_cost_product_returned_damaged_amount,
    SUM(product_order_cost_product_returned_damaged_amount_accounting) AS product_order_cost_product_returned_damaged_amount_accounting,
    0 AS retail_ship_only_return_shipping_cost,
    0 AS retail_ship_only_product_returned_resaleable_amount,
    0 AS retail_ship_only_product_returned_resaleable_amount_accounting
FROM _return_base AS rb
WHERE store_region = 'EU'
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    currency_object,
    currency_type

UNION ALL

SELECT
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    'hyperion' AS currency_object,
    cer.dest_currency AS currency_type,
    SUM(product_order_return_unit_count) AS product_order_return_unit_count,
    SUM(product_order_return_shipping_cost_amount * cer.exchange_rate) AS product_order_return_shipping_cost_amount,
    SUM(product_order_cost_product_returned_resaleable_amount * cer.exchange_rate) AS product_order_cost_product_returned_resaleable_amount,
    SUM(product_order_cost_product_returned_resaleable_amount_accounting * cer.exchange_rate) AS product_order_cost_product_returned_resaleable_amount_accounting,
    SUM(product_order_cost_product_returned_damaged_amount * cer.exchange_rate) AS product_order_cost_product_returned_damaged_amount,
    SUM(product_order_cost_product_returned_damaged_amount_accounting * cer.exchange_rate) AS product_order_cost_product_returned_damaged_amount_accounting,
    0 AS retail_ship_only_return_shipping_cost,
    0 AS retail_ship_only_product_returned_resaleable_amount,
    0 AS retail_ship_only_product_returned_resaleable_amount_accounting
FROM _return_base AS rb
JOIN edw_prod.reference.currency_exchange_rate_by_date AS cer ON cer.rate_date_pst = rb.date_hq  AND cer.src_currency = rb.currency_type
WHERE LOWER(rb.currency_type) IN ('sek','dkk')
    AND LOWER(cer.dest_currency) = 'gbp'
GROUP BY
    date,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    currency_object,
    cer.dest_currency

--UNION ALL
--
--SELECT
--    date,
--    rb.store_id,
--    vip_store_id,
--    finance_specialty_store,
--    is_scrubs_customer,
--    customer_id,
--    activation_local_datetime,
--    vip_cohort_month_date,
--    cancellation_local_datetime,
--    first_activation_local_datetime,
--    first_vip_cohort_month_date,
--    first_cancellation_local_datetime,
--    is_cross_promo,
--    gender,
--    is_reactivated_vip,
--    is_retail_vip,
--    activation_key,
--    first_activation_key,
--    associate_id,
--    order_membership_classification_key,
--    is_bop_vip,
--    'eur' AS currency_object,
--    'EUR' AS currency_type,
--    SUM(product_order_return_unit_count) AS product_order_return_unit_count,
--    SUM(product_order_return_shipping_cost_amount * reporting_eur_conversion_rate) AS product_order_return_shipping_cost_amount,
--    SUM(product_order_cost_product_returned_resaleable_amount * reporting_eur_conversion_rate) AS product_order_cost_product_returned_resaleable_amount,
--    SUM(product_order_cost_product_returned_resaleable_amount_accounting * reporting_eur_conversion_rate) AS product_order_cost_product_returned_resaleable_amount_accounting,
--    SUM(product_order_cost_product_returned_damaged_amount * reporting_eur_conversion_rate) AS product_order_cost_product_returned_damaged_amount,
--    SUM(product_order_cost_product_returned_damaged_amount_accounting * reporting_eur_conversion_rate) AS product_order_cost_product_returned_damaged_amount_accounting,
--    0 AS retail_ship_only_return_shipping_cost,
--    0 AS retail_ship_only_product_returned_resaleable_amount,
--    0 AS retail_ship_only_product_returned_resaleable_amount_accounting
--FROM _return_base AS rb
--JOIN edw_prod.data_model_jfb.dim_store AS st ON st.store_id = rb.store_id
--WHERE
--    st.store_brand = 'Savage X'
--    AND st.store_country = 'UK'
--GROUP BY
--    date,
--    rb.store_id,
--    vip_store_id,
--    finance_specialty_store,
--    is_scrubs_customer,
--    customer_id,
--    activation_local_datetime,
--    vip_cohort_month_date,
--    cancellation_local_datetime,
--    first_activation_local_datetime,
--    first_vip_cohort_month_date,
--    first_cancellation_local_datetime,
--    is_cross_promo,
--    gender,
--    is_reactivated_vip,
--    is_retail_vip,
--    activation_key,
--    first_activation_key,
--    associate_id,
--    order_membership_classification_key,
--    is_bop_vip
;

CREATE OR REPLACE TEMPORARY TABLE _credits AS
WITH _credits_base AS (
    SELECT DISTINCT
        dc.credit_key,
        dc.customer_id,
        dc.original_credit_tender,
        dc.original_credit_type,
        dc.original_credit_reason,
        dc.credit_report_mapping,
        dc.store_id,
        fce.credit_activity_type,
        fce.redemption_store_id,
        fce.administrator_id,
        fce.original_credit_activity_type_action,
        fce.activation_key,
        fce.first_activation_key,
        fce.credit_activity_local_datetime,
        fce.credit_activity_local_amount,
        fce.credit_activity_usd_conversion_rate,
        fce.credit_activity_equivalent_count
    FROM edw_prod.data_model_jfb.dim_credit dc
    JOIN (SELECT DISTINCT customer_id FROM _customer_date) cdci
        ON dc.customer_id = cdci.customer_id
    JOIN edw_prod.data_model_jfb.fact_credit_event fce
        ON fce.credit_key = dc.credit_key
    AND fce.original_credit_activity_type_action = 'Include' AND
        fce.credit_activity_type IN ('Issued','Expired','Cancelled')
    JOIN edw_prod.data_model_jfb.dim_store ds
        ON ds.store_id = dc.store_id
    WHERE ds.is_core_store = TRUE
)
SELECT
    cb.credit_activity_local_datetime :: DATE AS activity_date,
    CAST(convert_timezone('America/Los_Angeles',cb.credit_activity_local_datetime) AS DATE) AS date_hq,
    cd.customer_id,
    fa.activation_local_datetime,
    fa.vip_cohort_month_date,
    fa.cancellation_local_datetime,
    fa_first.activation_local_datetime AS first_activation_local_datetime,
    fa_first.vip_cohort_month_date AS first_vip_cohort_month_date,
    fa_first.cancellation_local_datetime AS first_cancellation_local_datetime,
    CASE WHEN cb.redemption_store_id = -1 THEN cb.store_id ELSE cb.redemption_store_id END AS store_id,
    IFF(fa.sub_store_id IS NOT NULL AND fa.sub_store_id NOT IN (-1,0), fa.sub_store_id, vsi.vip_store_id) AS vip_store_id,
    cd.finance_specialty_store,
    IFF(ds.store_brand='Fabletics', cd.is_scrubs_customer, FALSE) AS is_scrubs_customer,
    cd.is_cross_promo,
    cd.gender,
    fa.is_reactivated_vip,
    fa.is_retail_vip,
    cb.administrator_id,
    cb.original_credit_tender,
    cb.original_credit_type,
    cb.original_credit_reason,
    cb.credit_activity_equivalent_count,
    cb.credit_activity_type,
    cb.credit_report_mapping,
    cb.credit_activity_local_amount,
    cb.credit_activity_usd_conversion_rate,
    IFF(fa.vip_cohort_month_date < date_trunc(month,cb.credit_activity_local_datetime :: DATE) AND fa.cancellation_local_datetime > date_trunc(month,cb.credit_activity_local_datetime :: DATE),1,0) AS is_bop_vip,
    cb.activation_key,
    cb.first_activation_key
FROM _customer_date AS cd
    JOIN _credits_base cb
        ON cd.customer_id = cb.customer_id
        AND cd.date = cb.credit_activity_local_datetime::DATE
    LEFT JOIN edw_prod.data_model_jfb.fact_activation AS fa
        ON fa.activation_key = cb.activation_key
    LEFT JOIN edw_prod.data_model_jfb.fact_activation AS fa_first
        ON fa_first.activation_key = cb.first_activation_key
    LEFT JOIN _vip_store_id AS vsi
        ON vsi.store_id = cb.store_id
    LEFT JOIN edw_prod.data_model_jfb.dim_store AS ds
        ON ds.store_id = IFF(fa.sub_store_id IS NOT NULL AND fa.sub_store_id NOT IN (-1,0), fa.sub_store_id, vsi.vip_store_id);

CREATE OR REPLACE TEMPORARY TABLE _reporting_layer_credits AS
SELECT
    activity_date AS DATE,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    administrator_id,
     (SELECT order_membership_classification_key
    FROM edw_prod.data_model_jfb.dim_order_membership_classification
    WHERE membership_order_type_L3 = 'Repeat VIP') AS order_membership_classification_key,
    is_bop_vip,
    'usd' AS currency_object,
    'USD'AS currency_type,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS billed_cash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS billed_cash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS billed_cash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_issued_equivalent_count,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_cancelled_equivalent_count,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_expired_equivalent_count,

    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS refund_cash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS refund_cash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS refund_cash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_credit_issued_equivalent_count,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_credit_cancelled_equivalent_count,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_credit_expired_equivalent_count,

    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS noncash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS noncash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS noncash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_issued_equivalent_count,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_cancelled_equivalent_count,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_expired_equivalent_count,

    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS other_cash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS other_cash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount * credit_activity_usd_conversion_rate,0)) AS other_cash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_issued_equivalent_count,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_cancelled_equivalent_count,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_expired_equivalent_count
FROM _credits AS c
GROUP BY
    DATE,
    store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    administrator_id,
    order_membership_classification_key,
    is_bop_vip

UNION ALL

SELECT
    activity_date AS DATE,
    c.store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    administrator_id,
     (SELECT order_membership_classification_key
    FROM edw_prod.data_model_jfb.dim_order_membership_classification
    WHERE membership_order_type_L3 = 'Repeat VIP') AS order_membership_classification_key,
    is_bop_vip,
    'local' AS currency_object,
    st.store_currency AS currency_type,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount,0)) AS billed_cash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount,0)) AS billed_cash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount,0)) AS billed_cash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_issued_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_cancelled_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_expired_equivalent_counts,

    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount,0)) AS refund_cash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount,0)) AS refund_cash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount,0)) AS refund_cash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_credit_issued_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_credit_cancelled_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_credit_expired_equivalent_counts,

    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount,0)) AS noncash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount,0)) AS noncash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount,0)) AS noncash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_issued_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_cancelled_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_expired_equivalent_counts,

    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount,0)) AS other_cash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount,0)) AS other_cash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount,0)) AS other_cash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_issued_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_cancelled_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_expired_equivalent_counts
FROM _credits AS c
JOIN edw_prod.data_model_jfb.dim_store AS st ON st.store_id = c.store_id
WHERE st.store_region <> 'NA'
GROUP BY
    DATE,
    c.store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    administrator_id,
    order_membership_classification_key,
    is_bop_vip,
    currency_object,
    st.store_currency

UNION ALL

SELECT
    activity_date AS DATE,
    c.store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    administrator_id,
     (SELECT order_membership_classification_key
    FROM edw_prod.data_model_jfb.dim_order_membership_classification
    WHERE membership_order_type_L3 = 'Repeat VIP') AS order_membership_classification_key,
    is_bop_vip,
    'hyperion' AS currency_object,
    cer.dest_currency AS currency_type,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount * cer.exchange_rate,0)) AS billed_cash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount * cer.exchange_rate,0)) AS billed_cash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount * cer.exchange_rate,0)) AS billed_cash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_issued_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_cancelled_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_expired_equivalent_counts,

    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount * cer.exchange_rate,0)) AS refund_cash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount * cer.exchange_rate,0)) AS refund_cash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount * cer.exchange_rate,0)) AS refund_cash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_credit_issued_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_credit_cancelled_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_redit_expired_equivalent_counts,

    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS noncash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS noncash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS noncash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_issued_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_cancelled_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_expired_equivalent_counts,

    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS other_cash_credit_issued_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS other_cash_credit_cancelled_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS other_cash_credit_expired_amount,
    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_issued_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_cancelled_equivalent_counts,
    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_expired_equivalent_counts
FROM _credits AS c
JOIN edw_prod.data_model_jfb.dim_store AS st ON st.store_id = c.store_id
JOIN edw_prod.reference.currency_exchange_rate_by_date AS cer ON cer.rate_date_pst = c.date_hq AND cer.src_currency = st.store_currency
WHERE
    LOWER(st.store_currency) IN ('sek','dkk')
    AND LOWER(cer.dest_currency) = 'gbp'
GROUP BY
    DATE,
    c.store_id,
    vip_store_id,
    finance_specialty_store,
    is_scrubs_customer,
    customer_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    activation_key,
    first_activation_key,
    administrator_id,
    order_membership_classification_key,
    is_bop_vip,
    currency_object,
    cer.dest_currency

--UNION ALL
--
--SELECT
--    activity_date AS DATE,
--    c.store_id,
--    vip_store_id,
--    finance_specialty_store,
--    is_scrubs_customer,
--    customer_id,
--    activation_local_datetime,
--    vip_cohort_month_date,
--    cancellation_local_datetime,
--    first_activation_local_datetime,
--    first_vip_cohort_month_date,
--    first_cancellation_local_datetime,
--    is_cross_promo,
--    gender,
--    is_reactivated_vip,
--    is_retail_vip,
--    activation_key,
--    first_activation_key,
--    administrator_id,
--     (SELECT order_membership_classification_key
--    FROM edw_prod.data_model_jfb.dim_order_membership_classification
--    WHERE membership_order_type_L3 = 'Repeat VIP') AS order_membership_classification_key,
--    is_bop_vip,
--    'hyperion' AS currency_object,
--    cer.dest_currency AS currency_type,
--    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount * cer.exchange_rate,0)) AS billed_cash_credit_issued_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount * cer.exchange_rate,0)) AS billed_cash_credit_cancelled_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_local_amount * cer.exchange_rate,0)) AS billed_cash_credit_expired_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_issued_equivalent_counts,
--    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_cancelled_equivalent_counts,
--    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'billed credit',credit_activity_equivalent_count,0)) AS billed_cash_credit_expired_equivalent_counts,
--
--    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount * cer.exchange_rate,0)) AS refund_cash_credit_issued_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount * cer.exchange_rate,0)) AS refund_cash_credit_cancelled_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_local_amount * cer.exchange_rate,0)) AS refund_cash_credit_expired_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_credit_issued_equivalent_counts,
--    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_credit_cancelled_equivalent_counts,
--    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'refund credit',credit_activity_equivalent_count,0)) AS refund_cash_redit_expired_equivalent_counts,
--
--    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS noncash_credit_issued_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS noncash_credit_cancelled_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS noncash_credit_expired_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_issued_equivalent_counts,
--    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_cancelled_equivalent_counts,
--    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'noncash credit',credit_activity_equivalent_count,0)) AS noncash_credit_expired_equivalent_counts,
--
--    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS other_cash_credit_issued_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS other_cash_credit_cancelled_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_local_amount * cer.exchange_rate,0)) AS other_cash_credit_expired_amount,
--    SUM(IFF(LOWER(credit_activity_type) = 'issued' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_issued_equivalent_counts,
--    SUM(IFF(LOWER(credit_activity_type) = 'cancelled' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_cancelled_equivalent_counts,
--    SUM(IFF(LOWER(credit_activity_type) = 'expired' AND LOWER(credit_report_mapping) = 'other cash credit',credit_activity_equivalent_count,0)) AS other_cash_credit_expired_equivalent_counts
--FROM _credits AS c
--JOIN edw_prod.data_model_jfb.dim_store AS st ON st.store_id = c.store_id
--JOIN edw_prod.reference.currency_exchange_rate_by_date AS cer ON cer.rate_date_pst = c.date_hq AND cer.src_currency = st.store_currency
--WHERE
--    st.store_brand = 'Savage X'
--    AND st.store_country = 'UK'
--    AND LOWER(cer.dest_currency) = 'eur'
--GROUP BY
--    DATE,
--    c.store_id,
--    vip_store_id,
--    finance_specialty_store,
--    is_scrubs_customer,
--    customer_id,
--    activation_local_datetime,
--    vip_cohort_month_date,
--    cancellation_local_datetime,
--    first_activation_local_datetime,
--    first_vip_cohort_month_date,
--    first_cancellation_local_datetime,
--    is_cross_promo,
--    gender,
--    is_reactivated_vip,
--    is_retail_vip,
--    activation_key,
--    first_activation_key,
--    administrator_id,
--    order_membership_classification_key,
--    is_bop_vip,
--    currency_object,
--    cer.dest_currency
;


-- temp table to split the refund, chargeback AND return data by shipped/placed
CREATE OR REPLACE TEMPORARY TABLE _shipped_placed
(
    date_object varchar(20)
    ,currency_object varchar(20)
);

insert into _shipped_placed
values ('shipped','usd'),('placed','usd'),('shipped','local'),('placed','local'),('shipped','hyperion');

CREATE OR REPLACE TEMPORARY TABLE _finance_sales_ops_stg AS
SELECT
    COALESCE(po.date,rd.date,c.date,rt.date,rlc.date) AS date,
    COALESCE(po.date_object,rd.date_object,c.date_object,rt.date_object,rlc.date_object) AS date_object,
    COALESCE(po.store_id,rd.store_id,c.store_id,rt.store_id,rlc.store_id) AS store_id,
    COALESCE(po.vip_store_id,rd.vip_store_id,c.vip_store_id,rt.vip_store_id,rlc.vip_store_id) AS vip_store_id,
    COALESCE(po.finance_specialty_store,rd.finance_specialty_store,c.finance_specialty_store,rt.finance_specialty_store,rlc.finance_specialty_store) AS finance_specialty_store,
    COALESCE(po.is_scrubs_customer,rd.is_scrubs_customer,c.is_scrubs_customer,rt.is_scrubs_customer,rlc.is_scrubs_customer) AS is_scrubs_customer,
    COALESCE(po.customer_id,rd.customer_id,c.customer_id,rt.customer_id,rlc.customer_id) AS customer_id,
    COALESCE(po.activation_local_datetime,rd.activation_local_datetime,c.activation_local_datetime,rt.activation_local_datetime,rlc.activation_local_datetime) AS activation_local_datetime,
    COALESCE(po.vip_cohort_month_date,rd.vip_cohort_month_date,c.vip_cohort_month_date,rt.vip_cohort_month_date,rlc.vip_cohort_month_date) AS vip_cohort_month_date,
    COALESCE(po.cancellation_local_datetime,rd.cancellation_local_datetime,c.cancellation_local_datetime,rt.cancellation_local_datetime,rlc.cancellation_local_datetime) AS cancellation_local_datetime,
    COALESCE(po.first_activation_local_datetime,rd.first_activation_local_datetime,c.first_activation_local_datetime,rt.first_activation_local_datetime,rlc.first_activation_local_datetime) AS first_activation_local_datetime,
    COALESCE(po.first_vip_cohort_month_date,rd.first_vip_cohort_month_date,c.first_vip_cohort_month_date,rt.first_vip_cohort_month_date,rlc.first_vip_cohort_month_date) AS first_vip_cohort_month_date,
    COALESCE(po.first_cancellation_local_datetime,rd.first_cancellation_local_datetime,c.first_cancellation_local_datetime,rt.first_cancellation_local_datetime,rlc.first_cancellation_local_datetime) AS first_cancellation_local_datetime,
    COALESCE(po.is_cross_promo,rd.is_cross_promo,c.is_cross_promo,rt.is_cross_promo,rlc.is_cross_promo) AS is_cross_promo,
    COALESCE(po.gender,rd.gender,c.gender,rt.gender,rlc.gender) AS gender,
    COALESCE(po.is_reactivated_vip,rd.is_reactivated_vip,c.is_reactivated_vip,rt.is_reactivated_vip,rlc.is_reactivated_vip,FALSE) AS is_reactivated_vip,
    COALESCE(po.is_retail_vip,rd.is_retail_vip,c.is_retail_vip,rt.is_retail_vip,rlc.is_retail_vip) AS is_retail_vip,
    COALESCE(po.activation_key,rd.activation_key,c.activation_key,rt.activation_key,rlc.activation_key) AS activation_key,
    COALESCE(po.first_activation_key,rd.first_activation_key,c.first_activation_key,rt.first_activation_key,rlc.first_activation_key) AS first_activation_key,
    COALESCE(po.associate_id,rd.associate_id,c.associate_id,rt.associate_id,rlc.administrator_id) AS associate_id,
    COALESCE(po.order_membership_classification_key,rd.order_membership_classification_key,c.order_membership_classification_key,rt.order_membership_classification_key,rlc.order_membership_classification_key) AS order_membership_classification_key,
    COALESCE(po.is_bop_vip,rd.is_bop_vip,c.is_bop_vip,rt.is_bop_vip,rlc.is_bop_vip) AS is_bop_vip,
    COALESCE(po.currency_object,rd.currency_object,c.currency_object,rt.currency_object,rlc.currency_object) AS currency_object,
    COALESCE(po.currency_type,rd.currency_type,c.currency_type,rt.currency_type,rlc.currency_type) AS currency_type,
    IFNULL(po.reporting_landed_cost_local_amount,0) AS reporting_landed_cost_local_amount,
    IFNULL(po.reporting_landed_cost_local_amount_accounting,0) AS reporting_landed_cost_local_amount_accounting,
    IFNULL(po.product_order_subtotal_excl_tariff_amount,0) AS product_order_subtotal_excl_tariff_amount,
    IFNULL(po.product_order_tariff_amount,0) AS product_order_tariff_amount,
    IFNULL(po.product_order_subtotal_excl_tariff_amount,0) + IFNULL(po.product_order_tariff_amount,0) AS product_order_product_subtotal_amount,
    IFNULL(po.product_order_product_discount_amount,0) AS product_order_product_discount_amount,
    IFNULL(po.product_order_shipping_discount_amount,0) AS product_order_shipping_discount_amount,
    IFNULL(po.product_order_shipping_revenue_before_discount_amount,0) AS product_order_shipping_revenue_before_discount_amount,
    IFNULL(po.product_order_shipping_revenue_before_discount_amount,0) - IFNULL(po.product_order_shipping_discount_amount,0) AS product_order_shipping_revenue_amount,
    IFNULL(po.product_order_tax_amount,0) AS product_order_tax_amount,
    IFNULL(po.product_order_cash_transaction_amount,0) AS product_order_cash_transaction_amount,
    IFNULL(po.product_order_cash_credit_redeemed_amount,0) AS product_order_cash_credit_redeemed_amount,
    IFNULL(po.product_order_noncash_credit_redeemed_amount,0) AS product_order_noncash_credit_redeemed_amount,
    IFNULL(po.product_order_count_with_credit_redemption,0) AS product_order_count_with_credit_redemption,
    IFNULL(po.product_order_count,0) AS product_order_count,
    IFNULL(po.product_order_count_excl_seeding,0) AS product_order_count_excl_seeding,
    IFNULL(po.product_gift_order_count,0) AS product_gift_order_count,
    IFNULL(po.gift_certificate_gift_order_count,0) AS gift_certificate_gift_order_count,
    IFNULL(po.product_order_unit_count,0) AS product_order_unit_count,
    IFNULL(po.product_order_loyalty_unit_count,0) AS product_order_loyalty_unit_count,
    IFNULL(po.product_order_outfit_component_unit_count,0) AS product_order_outfit_component_unit_count,
    IFNULL(po.product_order_outfit_count,0) AS product_order_outfit_count,
    IFNULL(po.product_order_discounted_unit_count,0) AS product_order_discounted_unit_count,
    IFNULL(po.product_order_zero_revenue_unit_count,0) AS product_order_zero_revenue_unit_count,
    IFNULL(po.product_order_retail_unit_price, 0) AS product_order_retail_unit_price,
    IFNULL(po.product_order_air_vip_price, 0) AS product_order_air_vip_price,
    IFNULL(po.product_order_price_offered_amount,0) AS product_order_price_offered_amount,

    IFNULL(po.product_order_landed_product_cost_amount,0) AS product_order_landed_product_cost_amount,
    IFNULL(po.oracle_product_order_landed_product_cost_amount,0) AS oracle_product_order_landed_product_cost_amount,
    IFNULL(po.product_order_landed_product_cost_amount_accounting,0) AS product_order_landed_product_cost_amount_accounting,
    IFNULL(po.oracle_product_order_landed_product_cost_amount_accounting,0) AS oracle_product_order_landed_product_cost_amount_accounting,
    IFNULL(po.product_order_shipping_cost_amount,0) AS product_order_shipping_cost_amount,
    IFNULL(po.product_order_shipping_supplies_cost_amount,0) AS product_order_shipping_supplies_cost_amount,
    IFNULL(po.product_order_misc_cogs_amount,0) AS product_order_misc_cogs_amount,
    IFNULL(po.product_order_landed_product_cost_amount,0) + IFNULL(po.product_order_shipping_cost_amount,0) + IFNULL(po.product_order_shipping_supplies_cost_amount,0) AS product_order_direct_cogs_amount,
    IFNULL(po.product_order_landed_product_cost_amount_accounting,0) + IFNULL(po.product_order_shipping_cost_amount,0) + IFNULL(po.product_order_shipping_supplies_cost_amount,0) AS product_order_direct_cogs_amount_accounting,


    IFNULL(rd.product_order_cash_refund_amount,0) AS product_order_cash_refund_amount,
    IFNULL(rd.product_order_cash_credit_refund_amount,0) AS product_order_cash_credit_refund_amount,
    IFNULL(rd.product_order_noncash_credit_refund_amount,0) AS product_order_noncash_credit_refund_amount,
    IFNULL(rd.billed_credit_cash_refund_count,0) AS billed_credit_cash_refund_count,
    IFNULL(c.product_order_cash_chargeback_amount,0) AS product_order_cash_chargeback_amount,
    IFNULL(rt.product_order_cost_product_returned_resaleable_amount,0) AS product_order_cost_product_returned_resaleable_amount,
    IFNULL(rt.product_order_cost_product_returned_resaleable_amount_accounting,0) AS product_order_cost_product_returned_resaleable_amount_accounting,
    IFNULL(rt.product_order_cost_product_returned_damaged_amount,0) AS product_order_cost_product_returned_damaged_amount,
    IFNULL(rt.product_order_cost_product_returned_damaged_amount_accounting,0) AS product_order_cost_product_returned_damaged_amount_accounting,
    IFNULL(rt.product_order_return_shipping_cost_amount,0) AS product_order_return_shipping_cost_amount,
    IFNULL(rt.product_order_return_unit_count,0) AS product_order_return_unit_count,

    IFNULL(po.product_order_reship_order_count,0) AS product_order_reship_order_count,
    IFNULL(po.product_order_reship_unit_count,0) AS product_order_reship_unit_count,
    IFNULL(po.product_order_reship_product_cost_amount,0) + IFNULL(po.product_order_reship_shipping_cost_amount,0) + IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0) AS product_order_reship_direct_cogs_amount,
    IFNULL(po.product_order_reship_product_cost_amount,0) AS product_order_reship_product_cost_amount,
    IFNULL(po.oracle_product_order_reship_product_cost_amount,0) AS oracle_product_order_reship_product_cost_amount,
    IFNULL(po.product_order_reship_product_cost_amount_accounting,0) + IFNULL(po.product_order_reship_shipping_cost_amount,0) + IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0) AS product_order_reship_direct_cogs_amount_accounting,
    IFNULL(po.product_order_reship_product_cost_amount_accounting,0) AS product_order_reship_product_cost_amount_accounting,
    IFNULL(po.oracle_product_order_reship_product_cost_amount_accounting,0) AS oracle_product_order_reship_product_cost_amount_accounting,
    IFNULL(po.product_order_reship_shipping_cost_amount,0) AS product_order_reship_shipping_cost_amount,
    IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0) AS product_order_reship_shipping_supplies_cost_amount,
    IFNULL(po.product_order_exchange_order_count,0) AS product_order_exchange_order_count,
    IFNULL(po.product_order_exchange_unit_count,0) AS product_order_exchange_unit_count,
    IFNULL(po.product_order_exchange_product_cost_amount,0) + IFNULL(po.product_order_exchange_shipping_cost_amount,0) + IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0) AS product_order_exchange_direct_cogs_amount,
    IFNULL(po.product_order_exchange_product_cost_amount,0) AS product_order_exchange_product_cost_amount,
    IFNULL(po.oracle_product_order_exchange_product_cost_amount,0) AS oracle_product_order_exchange_product_cost_amount,
    IFNULL(po.product_order_exchange_product_cost_amount_accounting,0) + IFNULL(po.product_order_exchange_shipping_cost_amount,0) + IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0) AS product_order_exchange_direct_cogs_amount_accounting,
    IFNULL(po.product_order_exchange_product_cost_amount_accounting,0) AS product_order_exchange_product_cost_amount_accounting,
    IFNULL(po.oracle_product_order_exchange_product_cost_amount_accounting,0) AS oracle_product_order_exchange_product_cost_amount_accounting,
    IFNULL(po.product_order_exchange_shipping_cost_amount,0) AS product_order_exchange_shipping_cost_amount,
    IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0) AS product_order_exchange_shipping_supplies_cost_amount,

    IFNULL(po.product_order_payment_processing_cost_amount,0) + IFNULL(po.product_order_variable_gms_cost_amount,0) + IFNULL(po.product_order_variable_warehouse_cost_amount,0) AS product_order_selling_expenses_amount,
    IFNULL(po.product_order_payment_processing_cost_amount,0) AS product_order_payment_processing_cost_amount,
    IFNULL(po.product_order_variable_gms_cost_amount,0) AS product_order_variable_gms_cost_amount,
    IFNULL(po.product_order_variable_warehouse_cost_amount,0) AS product_order_variable_warehouse_cost_amount,
    IFNULL(po.billing_payment_processing_cost_amount,0) + IFNULL(po.billing_variable_gms_cost_amount,0) AS billing_selling_expenses_amount,
    IFNULL(po.billing_payment_processing_cost_amount,0) AS billing_payment_processing_cost_amount,
    IFNULL(po.billing_variable_gms_cost_amount,0) AS billing_variable_gms_cost_amount,

    IFNULL(po.product_order_amount_to_pay,0) AS product_order_amount_to_pay,
    IFNULL(po.product_gross_revenue_excl_shipping,0) AS product_gross_revenue_excl_shipping,
    IFNULL(po.product_gross_revenue,0) AS product_gross_revenue,
    IFNULL(po.product_gross_revenue,0)
         - IFNULL(rd.product_order_cash_refund_amount,0)
         - IFNULL(rd.product_order_cash_credit_refund_amount,0)
         - IFNULL(c.product_order_cash_chargeback_amount,0)
         AS product_net_revenue,
    IFNULL(po.product_margin_pre_return,0) AS product_margin_pre_return,
    IFNULL(po.product_margin_pre_return_excl_seeding,0 ) AS product_margin_pre_return_excl_seeding,
    IFNULL(po.product_margin_pre_return_accounting,0) AS product_margin_pre_return_accounting,
    IFNULL(po.product_margin_pre_return_excl_seeding_accounting,0 ) AS product_margin_pre_return_excl_seeding_accounting,
    IFNULL(po.product_margin_pre_return_excl_shipping,0) AS product_margin_pre_return_excl_shipping,
    IFNULL(po.product_margin_pre_return_excl_shipping_accounting,0) AS product_margin_pre_return_excl_shipping_accounting,
    IFNULL(po.product_margin_pre_return,0)
         - IFNULL(rd.product_order_cash_credit_refund_amount,0)
         - IFNULL(rd.product_order_cash_refund_amount,0)
         - IFNULL(c.product_order_cash_chargeback_amount,0)
         - IFNULL(rt.product_order_return_shipping_cost_amount,0)
         + IFNULL(rt.product_order_cost_product_returned_resaleable_amount,0)
         - IFNULL(po.product_order_reship_product_cost_amount,0)
         - IFNULL(po.product_order_reship_shipping_cost_amount,0)
         - IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0)
         - IFNULL(po.product_order_exchange_product_cost_amount,0)
         - IFNULL(po.product_order_exchange_shipping_cost_amount,0)
         - IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0)
         - IFNULL(po.product_order_misc_cogs_amount,0)
        AS product_gross_profit,
    IFNULL(po.product_margin_pre_return_accounting,0)
         - IFNULL(rd.product_order_cash_credit_refund_amount,0)
         - IFNULL(rd.product_order_cash_refund_amount,0)
         - IFNULL(c.product_order_cash_chargeback_amount,0)
         - IFNULL(rt.product_order_return_shipping_cost_amount,0)
         + IFNULL(rt.product_order_cost_product_returned_resaleable_amount_accounting,0)
         - IFNULL(po.product_order_reship_product_cost_amount_accounting,0)
         - IFNULL(po.product_order_reship_shipping_cost_amount,0)
         - IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0)
         - IFNULL(po.product_order_exchange_product_cost_amount_accounting,0)
         - IFNULL(po.product_order_exchange_shipping_cost_amount,0)
         - IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0)
         - IFNULL(po.product_order_misc_cogs_amount,0)
        AS product_gross_profit_accounting,
    IFNULL(po.product_margin_pre_return,0)
         - IFNULL(rd.product_order_cash_credit_refund_amount,0)
         - IFNULL(rd.product_order_cash_refund_amount,0)
         - IFNULL(c.product_order_cash_chargeback_amount,0)
         - IFNULL(rt.product_order_return_shipping_cost_amount,0)
         + IFNULL(rt.product_order_cost_product_returned_resaleable_amount,0)
         - IFNULL(po.product_order_reship_product_cost_amount,0)
         - IFNULL(po.product_order_reship_shipping_cost_amount,0)
         - IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0)
         - IFNULL(po.product_order_exchange_product_cost_amount,0)
         - IFNULL(po.product_order_exchange_shipping_cost_amount,0)
         - IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0)
         - IFNULL(po.product_order_variable_gms_cost_amount,0)
         - IFNULL(po.product_order_payment_processing_cost_amount,0)
         - IFNULL(po.product_order_misc_cogs_amount,0)
         - IFNULL(po.product_order_variable_warehouse_cost_amount,0)
         AS product_variable_contribution_profit,
    IFNULL(po.product_margin_pre_return_accounting,0)
         - IFNULL(rd.product_order_cash_credit_refund_amount,0)
         - IFNULL(rd.product_order_cash_refund_amount,0)
         - IFNULL(c.product_order_cash_chargeback_amount,0)
         - IFNULL(rt.product_order_return_shipping_cost_amount,0)
         + IFNULL(rt.product_order_cost_product_returned_resaleable_amount_accounting,0)
         - IFNULL(po.product_order_reship_product_cost_amount_accounting,0)
         - IFNULL(po.product_order_reship_shipping_cost_amount,0)
         - IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0)
         - IFNULL(po.product_order_exchange_product_cost_amount_accounting,0)
         - IFNULL(po.product_order_exchange_shipping_cost_amount,0)
         - IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0)
         - IFNULL(po.product_order_variable_gms_cost_amount,0)
         - IFNULL(po.product_order_payment_processing_cost_amount,0)
         - IFNULL(po.product_order_misc_cogs_amount,0)
         - IFNULL(po.product_order_variable_warehouse_cost_amount,0)
         AS product_variable_contribution_profit_accounting,

    IFNULL(po.product_order_cash_gross_revenue_amount,0) AS product_order_cash_gross_revenue_amount,
    IFNULL(po.product_order_cash_gross_revenue_amount,0)
        - IFNULL(rd.product_order_cash_refund_amount,0)
        - IFNULL(c.product_order_cash_chargeback_amount,0)
        AS product_order_cash_net_revenue,
    IFNULL(po.product_order_cash_margin_pre_return,0) AS product_order_cash_margin_pre_return,
    IFNULL(po.product_order_cash_margin_pre_return_accounting,0) AS product_order_cash_margin_pre_return_accounting,
    IFNULL(po.product_order_cash_margin_pre_return,0)
        - IFNULL(rd.product_order_cash_refund_amount,0)
        - IFNULL(c.product_order_cash_chargeback_amount,0)
        - IFNULL(rt.product_order_return_shipping_cost_amount,0)
        + IFNULL(rt.product_order_cost_product_returned_resaleable_amount,0)
        - IFNULL(po.product_order_reship_product_cost_amount,0)
        - IFNULL(po.product_order_reship_shipping_cost_amount,0)
        - IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_exchange_product_cost_amount,0)
        - IFNULL(po.product_order_exchange_shipping_cost_amount,0)
        - IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_misc_cogs_amount,0)
        AS product_order_cash_gross_profit,
    IFNULL(po.product_order_cash_margin_pre_return_accounting,0)
        - IFNULL(rd.product_order_cash_refund_amount,0)
        - IFNULL(c.product_order_cash_chargeback_amount,0)
        - IFNULL(rt.product_order_return_shipping_cost_amount,0)
        + IFNULL(rt.product_order_cost_product_returned_resaleable_amount_accounting,0)
        - IFNULL(po.product_order_reship_product_cost_amount_accounting,0)
        - IFNULL(po.product_order_reship_shipping_cost_amount,0)
        - IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_exchange_product_cost_amount_accounting,0)
        - IFNULL(po.product_order_exchange_shipping_cost_amount,0)
        - IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_misc_cogs_amount,0)
        AS product_order_cash_gross_profit_accounting,
    IFNULL(po.billing_cash_gross_revenue,0) AS billing_cash_gross_revenue,
    IFNULL(po.billing_cash_gross_revenue,0)
        - IFNULL(rd.billing_cash_refund_amount,0)
        - IFNULL(c.billing_cash_chargeback_amount,0)
        AS billing_cash_net_revenue,

    IFNULL(po.product_order_cash_gross_revenue_amount,0) + IFNULL(po.billing_cash_gross_revenue,0) AS cash_gross_revenue,
    IFNULL(po.product_order_cash_gross_revenue_amount,0)
        + IFNULL(po.billing_cash_gross_revenue,0)
        - IFNULL(rd.product_order_cash_refund_amount,0)
        - IFNULL(c.product_order_cash_chargeback_amount,0)
        - IFNULL(rd.billing_cash_refund_amount,0)
        - IFNULL(c.billing_cash_chargeback_amount,0)
        AS cash_net_revenue,
    IFNULL(po.product_order_cash_gross_revenue_amount,0)
        + IFNULL(po.billing_cash_gross_revenue,0)
        - IFNULL(rd.product_order_cash_refund_amount,0)
        - IFNULL(c.product_order_cash_chargeback_amount,0)
        - IFNULL(rd.billing_cash_refund_amount,0)
        - IFNULL(c.billing_cash_chargeback_amount,0)
        - IFNULL(po.product_order_landed_product_cost_amount,0)
        - IFNULL(po.product_order_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_shipping_cost_amount,0)
        - IFNULL(rt.product_order_return_shipping_cost_amount,0)
        + IFNULL(rt.product_order_cost_product_returned_resaleable_amount,0)
        - IFNULL(po.product_order_reship_product_cost_amount,0)
        - IFNULL(po.product_order_reship_shipping_cost_amount,0)
        - IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_exchange_product_cost_amount,0)
        - IFNULL(po.product_order_exchange_shipping_cost_amount,0)
        - IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_misc_cogs_amount,0)
        AS cash_gross_profit,
    IFNULL(po.product_order_cash_gross_revenue_amount,0)
        + IFNULL(po.billing_cash_gross_revenue,0)
        - IFNULL(rd.product_order_cash_refund_amount,0)
        - IFNULL(c.product_order_cash_chargeback_amount,0)
        - IFNULL(rd.billing_cash_refund_amount,0)
        - IFNULL(c.billing_cash_chargeback_amount,0)
        - IFNULL(po.product_order_landed_product_cost_amount_accounting,0)
        - IFNULL(po.product_order_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_shipping_cost_amount,0)
        - IFNULL(rt.product_order_return_shipping_cost_amount,0)
        + IFNULL(rt.product_order_cost_product_returned_resaleable_amount_accounting,0)
        - IFNULL(po.product_order_reship_product_cost_amount_accounting,0)
        - IFNULL(po.product_order_reship_shipping_cost_amount,0)
        - IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_exchange_product_cost_amount_accounting,0)
        - IFNULL(po.product_order_exchange_shipping_cost_amount,0)
        - IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_misc_cogs_amount,0)
        AS cash_gross_profit_accounting,
    IFNULL(po.product_order_cash_gross_revenue_amount,0)
        + IFNULL(po.billing_cash_gross_revenue,0)
        - IFNULL(rd.product_order_cash_refund_amount,0)
        - IFNULL(c.product_order_cash_chargeback_amount,0)
        - IFNULL(rd.billing_cash_refund_amount,0)
        - IFNULL(c.billing_cash_chargeback_amount,0)
        - IFNULL(po.product_order_landed_product_cost_amount,0)
        - IFNULL(po.product_order_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_shipping_cost_amount,0)
        - IFNULL(rt.product_order_return_shipping_cost_amount,0)
        + IFNULL(rt.product_order_cost_product_returned_resaleable_amount,0)
        - IFNULL(po.product_order_reship_product_cost_amount,0)
        - IFNULL(po.product_order_reship_shipping_cost_amount,0)
        - IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_exchange_product_cost_amount,0)
        - IFNULL(po.product_order_exchange_shipping_cost_amount,0)
        - IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_variable_gms_cost_amount,0)
        - IFNULL(po.product_order_payment_processing_cost_amount,0)
        - IFNULL(po.billing_variable_gms_cost_amount,0)
        - IFNULL(po.billing_payment_processing_cost_amount,0)
        - IFNULL(po.product_order_misc_cogs_amount,0)
        - IFNULL(po.product_order_variable_warehouse_cost_amount,0)
        AS cash_variable_contribution_profit,
    IFNULL(po.product_order_cash_gross_revenue_amount,0)
        + IFNULL(po.billing_cash_gross_revenue,0)
        - IFNULL(rd.product_order_cash_refund_amount,0)
        - IFNULL(c.product_order_cash_chargeback_amount,0)
        - IFNULL(rd.billing_cash_refund_amount,0)
        - IFNULL(c.billing_cash_chargeback_amount,0)
        - IFNULL(po.product_order_landed_product_cost_amount_accounting,0)
        - IFNULL(po.product_order_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_shipping_cost_amount,0)
        - IFNULL(rt.product_order_return_shipping_cost_amount,0)
        + IFNULL(rt.product_order_cost_product_returned_resaleable_amount_accounting,0)
        - IFNULL(po.product_order_reship_product_cost_amount_accounting,0)
        - IFNULL(po.product_order_reship_shipping_cost_amount,0)
        - IFNULL(po.product_order_reship_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_exchange_product_cost_amount_accounting,0)
        - IFNULL(po.product_order_exchange_shipping_cost_amount,0)
        - IFNULL(po.product_order_exchange_shipping_supplies_cost_amount,0)
        - IFNULL(po.product_order_variable_gms_cost_amount,0)
        - IFNULL(po.product_order_payment_processing_cost_amount,0)
        - IFNULL(po.billing_variable_gms_cost_amount,0)
        - IFNULL(po.billing_payment_processing_cost_amount,0)
        - IFNULL(po.product_order_misc_cogs_amount,0)
        - IFNULL(po.product_order_variable_warehouse_cost_amount,0)
        AS cash_variable_contribution_profit_accounting,
    IFNULL(po.billing_order_transaction_count,0) AS billing_order_transaction_count,
    IFNULL(po.billed_credit_cash_transaction_count,0) AS billed_credit_cash_transaction_count,
    IFNULL(po.on_retry_billed_credit_cash_transaction_count,0) AS on_retry_billed_credit_cash_transaction_count,
    IFNULL(rd.billing_cash_refund_amount,0) AS billing_cash_refund_amount,
    IFNULL(c.billing_cash_chargeback_amount,0) AS billing_cash_chargeback_amount,
    IFNULL(po.billed_credit_cash_transaction_amount,0) AS billed_credit_cash_transaction_amount,
    IFNULL(po.membership_fee_cash_transaction_amount,0) AS membership_fee_cash_transaction_amount,
    IFNULL(po.gift_card_transaction_amount,0) AS gift_card_transaction_amount,
    IFNULL(po.legacy_credit_cash_transaction_amount,0) AS legacy_credit_cash_transaction_amount,
    IFNULL(rd.billed_credit_cash_refund_amount,0) + IFNULL(c.billed_credit_cash_chargeback_amount,0) AS billed_credit_cash_refund_chargeback_amount,
    IFNULL(rd.membership_fee_cash_refund_amount,0) + IFNULL(c.membership_fee_cash_chargeback_amount,0) AS membership_fee_cash_refund_chargeback_amount,
    IFNULL(rd.gift_card_cash_refund_amount,0) + IFNULL(c.gift_card_cash_chargeback_amount,0) AS gift_card_cash_refund_chargeback_amount,
    IFNULL(rd.legacy_credit_cash_refund_amount,0)  + IFNULL(c.legacy_credit_cash_chargeback_amount,0) AS legacy_credit_cash_refund_chargeback_amount,
    IFNULL(rlc.billed_cash_credit_issued_amount,0) AS billed_cash_credit_issued_amount,
    IFNULL(po.billed_cash_credit_redeemed_amount,0) AS billed_cash_credit_redeemed_amount,
    IFNULL(rlc.billed_cash_credit_cancelled_amount,0) AS billed_cash_credit_cancelled_amount,
    IFNULL(rlc.billed_cash_credit_expired_amount,0) AS billed_cash_credit_expired_amount,
    IFNULL(rlc.billed_cash_credit_issued_equivalent_count,0) AS billed_cash_credit_issued_equivalent_count,
    IFNULL(po.billed_cash_credit_redeemed_equivalent_count,0) AS billed_cash_credit_redeemed_equivalent_count,
    IFNULL(po.billed_cash_credit_redeemed_same_month_amount,0) AS billed_cash_credit_redeemed_same_month_amount,
    IFNULL(rlc.billed_cash_credit_cancelled_equivalent_count,0) AS billed_cash_credit_cancelled_equivalent_count,
    IFNULL(rlc.billed_cash_credit_expired_equivalent_count,0) AS billed_cash_credit_expired_equivalent_count,
    IFNULL(rlc.refund_cash_credit_issued_amount,0) AS refund_cash_credit_issued_amount,
    IFNULL(po.refund_cash_credit_redeemed_amount,0) AS refund_cash_credit_redeemed_amount,
    IFNULL(rlc.refund_cash_credit_cancelled_amount,0) AS refund_cash_credit_cancelled_amount,
    IFNULL(rlc.refund_cash_credit_expired_amount,0) AS refund_cash_credit_expired_amount,
    IFNULL(rlc.other_cash_credit_issued_amount,0) AS other_cash_credit_issued_amount,
    IFNULL(po.other_cash_credit_redeemed_amount,0) AS other_cash_credit_redeemed_amount,
    IFNULL(rlc.other_cash_credit_cancelled_amount,0) AS other_cash_credit_cancelled_amount,
    IFNULL(rlc.other_cash_credit_expired_amount,0) AS other_cash_credit_expired_amount,
    IFNULL(rlc.noncash_credit_issued_amount,0) AS noncash_credit_issued_amount,
    IFNULL(rlc.noncash_credit_cancelled_amount,0) AS noncash_credit_cancelled_amount,
    IFNULL(rlc.noncash_credit_expired_amount,0) AS noncash_credit_expired_amount,
    IFNULL(po.cash_gift_card_redeemed_amount,0) AS cash_gift_card_redeemed_amount,
    IFNULL(po.retail_ship_only_product_order_count,0) AS retail_ship_only_product_order_count,
    IFNULL(po.retail_ship_only_product_order_unit_count,0) AS retail_ship_only_product_order_unit_count,
    IFNULL(po.retail_ship_only_product_gross_revenue,0) AS retail_ship_only_product_gross_revenue,
    IFNULL(po.retail_ship_only_product_gross_revenue_excl_shipping,0) AS retail_ship_only_product_gross_revenue_excl_shipping,
    IFNULL(po.retail_ship_only_product_margin_pre_return,0) AS retail_ship_only_product_margin_pre_return,
    IFNULL(po.retail_ship_only_product_margin_pre_return_accounting,0) AS retail_ship_only_product_margin_pre_return_accounting,
    IFNULL(po.retail_ship_only_product_margin_pre_return_excl_shipping,0) AS retail_ship_only_product_margin_pre_return_excl_shipping,
    IFNULL(po.retail_ship_only_product_margin_pre_return_excl_shipping_accounting,0) AS retail_ship_only_product_margin_pre_return_excl_shipping_accounting,
    IFNULL(po.retail_ship_only_product_order_cash_margin_pre_return,0) AS retail_ship_only_product_order_cash_margin_pre_return,
    IFNULL(po.retail_ship_only_product_order_cash_margin_pre_return_accounting,0) AS retail_ship_only_product_order_cash_margin_pre_return_accounting,
    IFNULL(po.retail_ship_only_product_order_cash_gross_revenue,0) AS retail_ship_only_product_order_cash_gross_revenue,
    IFNULL(po.retail_ship_only_product_gross_revenue,0)
        - IFNULL(c.retail_ship_only_cash_chargeback_amount,0)
        - IFNULL(rd.retail_ship_only_cash_refund_amount,0)
        - IFNULL(rd.retail_ship_only_cash_credit_refund_amount,0)
       AS retail_ship_only_product_net_revenue,
    IFNULL(po.retail_ship_only_product_margin_pre_return,0)
        - IFNULL(c.retail_ship_only_cash_chargeback_amount,0)
        - IFNULL(rd.retail_ship_only_cash_refund_amount,0)
        - IFNULL(rd.retail_ship_only_cash_credit_refund_amount,0)
        - IFNULL(rt.retail_ship_only_return_shipping_cost,0)
        + IFNULL(rt.retail_ship_only_product_returned_resaleable_amount,0)
        - IFNULL(retail_ship_only_reship_exchange_product_cost_amount,0)
        - IFNULL(retail_ship_only_reship_exchange_shipping_cost_amount,0)
        - IFNULL(retail_ship_only_reship_exchange_shipping_supplies_cost_amount,0)
        - IFNULL(retail_ship_only_misc_cogs_amount,0)
        AS retail_ship_only_product_gross_profit,
    IFNULL(po.retail_ship_only_product_margin_pre_return_accounting,0)
        - IFNULL(c.retail_ship_only_cash_chargeback_amount,0)
        - IFNULL(rd.retail_ship_only_cash_refund_amount,0)
        - IFNULL(rd.retail_ship_only_cash_credit_refund_amount,0)
        - IFNULL(rt.retail_ship_only_return_shipping_cost,0)
        + IFNULL(rt.retail_ship_only_product_returned_resaleable_amount_accounting,0)
        - IFNULL(retail_ship_only_reship_exchange_product_cost_amount_accounting,0)
        - IFNULL(retail_ship_only_reship_exchange_shipping_cost_amount,0)
        - IFNULL(retail_ship_only_reship_exchange_shipping_supplies_cost_amount,0)
        - IFNULL(retail_ship_only_misc_cogs_amount,0)
        AS retail_ship_only_product_gross_profit_accounting,
    IFNULL(po.retail_ship_only_product_order_cash_gross_revenue,0)
        - IFNULL(c.retail_ship_only_cash_chargeback_amount,0)
        - IFNULL(rd.retail_ship_only_cash_refund_amount,0)
        - IFNULL(rd.retail_ship_only_cash_credit_refund_amount,0)
        AS retail_ship_only_product_order_cash_net_revenue,
    IFNULL(po.retail_ship_only_product_order_cash_margin_pre_return,0)
        - IFNULL(c.retail_ship_only_cash_chargeback_amount,0)
        - IFNULL(rd.retail_ship_only_cash_refund_amount,0)
        - IFNULL(rd.retail_ship_only_cash_credit_refund_amount,0)
        - IFNULL(rt.retail_ship_only_return_shipping_cost,0)
        + IFNULL(rt.retail_ship_only_product_returned_resaleable_amount,0)
        - IFNULL(retail_ship_only_reship_exchange_product_cost_amount,0)
        - IFNULL(retail_ship_only_reship_exchange_shipping_cost_amount,0)
        - IFNULL(retail_ship_only_reship_exchange_shipping_supplies_cost_amount,0)
        - IFNULL(retail_ship_only_misc_cogs_amount,0)
        AS retail_ship_only_product_order_cash_gross_profit,
    IFNULL(po.retail_ship_only_product_order_cash_margin_pre_return_accounting,0)
        - IFNULL(c.retail_ship_only_cash_chargeback_amount,0)
        - IFNULL(rd.retail_ship_only_cash_refund_amount,0)
        - IFNULL(rd.retail_ship_only_cash_credit_refund_amount,0)
        - IFNULL(rt.retail_ship_only_return_shipping_cost,0)
        + IFNULL(rt.retail_ship_only_product_returned_resaleable_amount_accounting,0)
        - IFNULL(retail_ship_only_reship_exchange_product_cost_amount_accounting,0)
        - IFNULL(retail_ship_only_reship_exchange_shipping_cost_amount,0)
        - IFNULL(retail_ship_only_reship_exchange_shipping_supplies_cost_amount,0)
        - IFNULL(retail_ship_only_misc_cogs_amount,0)
        AS retail_ship_only_product_order_cash_gross_profit_accounting,
    IFNULL(po.product_order_reship_exchange_cash_gross_revenue,0) AS product_order_reship_exchange_cash_gross_revenue,
    IFNULL(po.product_order_non_token_unit_count, 0) AS product_order_non_token_unit_count,
    IFNULL(po.product_order_non_token_subtotal_excl_tariff_amount, 0) AS product_order_non_token_subtotal_excl_tariff_amount,
    hash(*) AS meta_row_hash
FROM _product_orders po
FULL JOIN (
    SELECT a.*, b.date_object
    FROM _refunds a
    JOIN _shipped_placed b ON a.currency_object = b.currency_object
    ) AS rd ON rd.date = po.date
            AND rd.store_id = po.store_id
            AND rd.customer_id = po.customer_id
            AND rd.activation_key = po.activation_key
            AND rd.first_activation_key = po.first_activation_key
            AND COALESCE(rd.associate_id,-1) = COALESCE(po.associate_id,-1)
            AND rd.order_membership_classification_key = po.order_membership_classification_key
            AND rd.is_bop_vip = po.is_bop_vip
            AND rd.currency_object = po.currency_object
            AND rd.currency_type = po.currency_type
            AND rd.date_object = po.date_object
            AND rd.vip_store_id = po.vip_store_id
            AND rd.finance_specialty_store = po.finance_specialty_store
            AND rd.is_scrubs_customer = po.is_scrubs_customer
            AND rd.is_cross_promo = po.is_cross_promo
            AND rd.gender = po.gender
            AND rd.is_retail_vip = po.is_retail_vip
FULL JOIN (
    SELECT  a.*, b.date_object
    FROM _chargebacks a
    JOIN _shipped_placed b ON a.currency_object = b.currency_object
    ) AS c ON c.date = COALESCE(po.date,rd.date)
            AND c.store_id = COALESCE(po.store_id ,rd.store_id)
            AND c.customer_id = COALESCE(po.customer_id,rd.customer_id)
            AND c.activation_key = COALESCE(po.activation_key, rd.activation_key )
            AND c.first_activation_key = COALESCE(po.first_activation_key, rd.first_activation_key )
            AND COALESCE(c.associate_id,-1) = COALESCE(po.associate_id, rd.associate_id,-1)
            AND c.order_membership_classification_key = COALESCE(po.order_membership_classification_key,rd.order_membership_classification_key)
            AND c.is_bop_vip = COALESCE(po.is_bop_vip,rd.is_bop_vip)
            AND c.currency_object = COALESCE(po.currency_object,rd.currency_object)
            AND c.currency_type = COALESCE(po.currency_type,rd.currency_type)
            AND c.date_object = COALESCE(po.date_object,rd.date_object)
            AND c.vip_store_id = COALESCE(po.vip_store_id,rd.vip_store_id)
            AND c.finance_specialty_store = COALESCE(po.finance_specialty_store,rd.finance_specialty_store)
            AND c.is_scrubs_customer = COALESCE(po.is_scrubs_customer,rd.is_scrubs_customer)
            AND c.is_cross_promo = COALESCE(po.is_cross_promo,rd.is_cross_promo)
            AND c.gender = COALESCE(po.gender,rd.gender)
            AND c.is_retail_vip = COALESCE(po.is_retail_vip,rd.is_retail_vip)
FULL JOIN (
    SELECT  a.*, b.date_object
    FROM _returns a
    JOIN _shipped_placed b ON a.currency_object = b.currency_object
    ) AS rt ON rt.date = COALESCE(po.date,rd.date,c.date)
            AND rt.store_id = COALESCE(po.store_id ,rd.store_id,c.store_id)
            AND rt.customer_id = COALESCE(po.customer_id,rd.customer_id,c.customer_id)
            AND rt.activation_key= COALESCE(po.activation_key, rd.activation_key, c.activation_key )
            AND rt.first_activation_key= COALESCE(po.first_activation_key, rd.first_activation_key, c.first_activation_key )
            AND COALESCE(rt.associate_id,-1) = COALESCE(po.associate_id, rd.associate_id, c.associate_id,-1)
            AND rt.order_membership_classification_key = COALESCE(po.order_membership_classification_key,rd.order_membership_classification_key, c.order_membership_classification_key)
            AND rt.is_bop_vip = COALESCE(po.is_bop_vip,rd.is_bop_vip, c.is_bop_vip)
            AND rt.currency_object = COALESCE(po.currency_object,rd.currency_object,c.currency_object)
            AND rt.currency_type = COALESCE(po.currency_type,rd.currency_type,c.currency_type)
            AND rt.date_object = COALESCE(po.date_object,rd.date_object,c.date_object)
            AND rt.vip_store_id = COALESCE(po.vip_store_id,rd.vip_store_id,c.vip_store_id)
            AND rt.finance_specialty_store = COALESCE(po.finance_specialty_store,rd.finance_specialty_store,c.finance_specialty_store)
            AND rt.is_scrubs_customer = COALESCE(po.is_scrubs_customer,rd.is_scrubs_customer,c.is_scrubs_customer)
            AND rt.is_cross_promo = COALESCE(po.is_cross_promo,rd.is_cross_promo,c.is_cross_promo)
            AND rt.gender = COALESCE(po.gender,rd.gender,c.gender)
            AND rt.is_retail_vip = COALESCE(po.is_retail_vip,rd.is_retail_vip,c.is_retail_vip)
FULL JOIN (
    SELECT  a.*, b.date_object
    FROM _reporting_layer_credits a
    JOIN _shipped_placed b ON a.currency_object = b.currency_object
    ) AS rlc ON rlc.date = COALESCE(po.date,rd.date,c.date,rt.date)
            AND rlc.store_id = COALESCE(po.store_id ,rd.store_id,c.store_id,rt.store_id)
            AND rlc.customer_id = COALESCE(po.customer_id,rd.customer_id,c.customer_id,rt.customer_id)
            AND rlc.activation_key= COALESCE(po.activation_key, rd.activation_key, c.activation_key,rt.activation_key)
            AND rlc.first_activation_key= COALESCE(po.first_activation_key, rd.first_activation_key, c.first_activation_key,rt.first_activation_key)
            AND COALESCE(rlc.administrator_id,-1) = COALESCE(po.associate_id, rd.associate_id, c.associate_id,rt.associate_id,-1)
            AND rlc.order_membership_classification_key = COALESCE(po.order_membership_classification_key,rd.order_membership_classification_key, c.order_membership_classification_key,rt.order_membership_classification_key)
            AND rlc.is_bop_vip = COALESCE(po.is_bop_vip,rd.is_bop_vip, c.is_bop_vip,rt.is_bop_vip)
            AND rlc.currency_object = COALESCE(po.currency_object,rd.currency_object,c.currency_object,rt.currency_object)
            AND rlc.currency_type = COALESCE(po.currency_type,rd.currency_type,c.currency_type,rt.currency_type)
            AND rlc.date_object = COALESCE(po.date_object,rd.date_object,c.date_object,rt.date_object)
            AND rlc.vip_store_id = COALESCE(po.vip_store_id,rd.vip_store_id,c.vip_store_id,rt.vip_store_id)
            AND rlc.finance_specialty_store = COALESCE(po.finance_specialty_store,rd.finance_specialty_store,c.finance_specialty_store,rt.finance_specialty_store)
            AND rlc.is_scrubs_customer = COALESCE(po.is_scrubs_customer,rd.is_scrubs_customer,c.is_scrubs_customer,rt.is_scrubs_customer)
            AND rlc.is_cross_promo = COALESCE(po.is_cross_promo,rd.is_cross_promo,c.is_cross_promo,rt.is_cross_promo)
            AND rlc.gender = COALESCE(po.gender,rd.gender,c.gender,rt.gender)
            AND rlc.is_retail_vip = COALESCE(po.is_retail_vip,rd.is_retail_vip,c.is_retail_vip,rt.is_retail_vip);
-- SELECT date, date_object, customer_id, order_id, count(1) FROM analytics_base.finance_sales_ops_stg GROUP BY 1, 2, 3, 4 having count(1) > 1;

--BEGIN TRANSACTION;

--UPDATE edw_prod.analytics_base.finance_sales_ops_stg t
--SET t.is_deleted = TRUE,
--    t.meta_update_datetime = $execution_start_time
--FROM _customer_date AS s
--WHERE t.date = s.date
--    AND t.customer_id = s.customer_id
--    AND NOT EXISTS ( SELECT 1 FROM _finance_sales_ops_stg stg
--                        WHERE stg.date = s.date
--                            AND stg.customer_id = s.customer_id
--                    );

-- set is_deleted = TRUE for orphan records in full refresh
--UPDATE edw_prod.analytics_base.finance_sales_ops_stg t
--SET t.is_deleted = TRUE,
--    t.meta_update_datetime = $execution_start_time
--FROM
--(
--    SELECT
--        fso.date,
--        fso.customer_id
--    FROM edw_prod.analytics_base.finance_sales_ops_stg fso
--    LEFT JOIN _customer_date cd
--        ON fso.date = cd.date
--        AND fso.customer_id = cd.customer_id
--    WHERE cd.date IS NULL
--        AND $is_full_refresh
--) s
--WHERE t.date = s.date
--    AND t.customer_id = s.customer_id;
--
--DELETE FROM edw_prod.analytics_base.finance_sales_ops_stg AS t
--USING _finance_sales_ops_stg AS s
--WHERE t.date = s.date
--    AND t.customer_id = s.customer_id;

truncate table edw_prod.analytics_base.finance_sales_ops_stg;

INSERT INTO edw_prod.analytics_base.finance_sales_ops_stg
(
    date,
    date_object,
    store_id,
    customer_id,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    currency_object,
    currency_type,
    is_deleted,
    product_order_subtotal_excl_tariff_amount,
    product_order_tariff_amount,
    product_order_product_subtotal_amount,
    product_order_product_discount_amount,
    product_order_shipping_discount_amount,
    product_order_shipping_revenue_before_discount_amount,
    product_order_shipping_revenue_amount,
    product_order_tax_amount,
    product_order_cash_transaction_amount,
    product_order_cash_credit_redeemed_amount,
    product_order_noncash_credit_redeemed_amount,
    product_order_count_with_credit_redemption,
    product_order_count,
    product_order_count_excl_seeding,
    product_gift_order_count,
    gift_certificate_gift_order_count,
    product_order_unit_count,
    product_order_loyalty_unit_count,
    product_order_outfit_component_unit_count,
    product_order_outfit_count,
    product_order_discounted_unit_count,
    product_order_zero_revenue_unit_count,
    product_order_retail_unit_price,
    product_order_air_vip_price,
    product_order_price_offered_amount,
    product_order_landed_product_cost_amount,
    oracle_product_order_landed_product_cost_amount,
    product_order_landed_product_cost_amount_accounting,
    oracle_product_order_landed_product_cost_amount_accounting,
    product_order_shipping_cost_amount,
    product_order_shipping_supplies_cost_amount,
    product_order_misc_cogs_amount,
    product_order_direct_cogs_amount,
    product_order_direct_cogs_amount_accounting,
    product_order_cash_refund_amount,
    product_order_cash_credit_refund_amount,
    product_order_noncash_credit_refund_amount,
    product_order_cash_chargeback_amount,
    product_order_cost_product_returned_resaleable_amount,
    product_order_cost_product_returned_resaleable_amount_accounting,
    product_order_cost_product_returned_damaged_amount,
    product_order_cost_product_returned_damaged_amount_accounting,
    product_order_return_shipping_cost_amount,
    product_order_return_unit_count,
    product_order_reship_order_count,
    product_order_reship_unit_count,
    product_order_reship_direct_cogs_amount,
    product_order_reship_direct_cogs_amount_accounting,
    product_order_reship_product_cost_amount,
    oracle_product_order_reship_product_cost_amount,
    product_order_reship_product_cost_amount_accounting,
    oracle_product_order_reship_product_cost_amount_accounting,
    product_order_reship_shipping_cost_amount,
    product_order_reship_shipping_supplies_cost_amount,
    product_order_exchange_order_count,
    product_order_exchange_unit_count,
    product_order_exchange_direct_cogs_amount,
    product_order_exchange_direct_cogs_amount_accounting,
    product_order_exchange_product_cost_amount,
    oracle_product_order_exchange_product_cost_amount,
    product_order_exchange_product_cost_amount_accounting,
    oracle_product_order_exchange_product_cost_amount_accounting,
    product_order_exchange_shipping_cost_amount,
    product_order_exchange_shipping_supplies_cost_amount,
    product_order_selling_expenses_amount,
    product_order_payment_processing_cost_amount,
    product_order_variable_gms_cost_amount,
    product_order_variable_warehouse_cost_amount,
    billing_selling_expenses_amount,
    billing_payment_processing_cost_amount,
    billing_variable_gms_cost_amount,
    product_order_amount_to_pay,
    product_gross_revenue_excl_shipping,
    product_gross_revenue,
    product_net_revenue,
    product_margin_pre_return,
    product_margin_pre_return_excl_seeding,
    product_margin_pre_return_accounting,
    product_margin_pre_return_excl_seeding_accounting,
    product_margin_pre_return_excl_shipping,
    product_margin_pre_return_excl_shipping_accounting,
    product_gross_profit,
    product_gross_profit_accounting,
    product_variable_contribution_profit,
    product_variable_contribution_profit_accounting,
    product_order_cash_gross_revenue_amount,
    product_order_cash_net_revenue,
    product_order_cash_margin_pre_return,
    product_order_cash_margin_pre_return_accounting,
    product_order_cash_gross_profit,
    product_order_cash_gross_profit_accounting,
    billing_cash_gross_revenue,
    billing_cash_net_revenue,
    cash_gross_revenue,
    cash_net_revenue,
    cash_gross_profit,
    cash_gross_profit_accounting,
    cash_variable_contribution_profit,
    cash_variable_contribution_profit_accounting,
    billing_order_transaction_count,
    billed_credit_cash_transaction_count,
    on_retry_billed_credit_cash_transaction_count,
    billed_credit_cash_refund_count,
    billing_cash_refund_amount,
    billing_cash_chargeback_amount,
    billed_credit_cash_transaction_amount,
    membership_fee_cash_transaction_amount,
    gift_card_transaction_amount,
    legacy_credit_cash_transaction_amount,
    billed_credit_cash_refund_chargeback_amount,
    membership_fee_cash_refund_chargeback_amount,
    gift_card_cash_refund_chargeback_amount,
    legacy_credit_cash_refund_chargeback_amount,
    billed_cash_credit_issued_amount,
    billed_cash_credit_redeemed_amount,
    billed_cash_credit_cancelled_amount,
    billed_cash_credit_expired_amount,
    billed_cash_credit_issued_equivalent_count,
    billed_cash_credit_redeemed_equivalent_count,
    billed_cash_credit_redeemed_same_month_amount,
    billed_cash_credit_cancelled_equivalent_count,
    billed_cash_credit_expired_equivalent_count,
    refund_cash_credit_issued_amount,
    refund_cash_credit_redeemed_amount,
    refund_cash_credit_cancelled_amount,
    refund_cash_credit_expired_amount,
    other_cash_credit_issued_amount,
    other_cash_credit_redeemed_amount,
    other_cash_credit_cancelled_amount,
    other_cash_credit_expired_amount,
    noncash_credit_issued_amount,
    noncash_credit_cancelled_amount,
    noncash_credit_expired_amount,
    cash_gift_card_redeemed_amount,
    vip_store_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    is_retail_vip,
    finance_specialty_store,
    is_scrubs_customer,
    reporting_landed_cost_local_amount,
    reporting_landed_cost_local_amount_accounting,
    retail_ship_only_product_order_count,
    retail_ship_only_product_order_unit_count,
    retail_ship_only_product_gross_revenue,
    retail_ship_only_product_gross_revenue_excl_shipping,
    retail_ship_only_product_margin_pre_return,
    retail_ship_only_product_margin_pre_return_accounting,
    retail_ship_only_product_margin_pre_return_excl_shipping,
    retail_ship_only_product_margin_pre_return_excl_shipping_accounting,
    retail_ship_only_product_order_cash_margin_pre_return,
    retail_ship_only_product_order_cash_margin_pre_return_accounting,
    retail_ship_only_product_order_cash_gross_revenue,
    retail_ship_only_product_net_revenue,
    retail_ship_only_product_gross_profit,
    retail_ship_only_product_gross_profit_accounting,
    retail_ship_only_product_order_cash_net_revenue,
    retail_ship_only_product_order_cash_gross_profit,
    retail_ship_only_product_order_cash_gross_profit_accounting,
    product_order_reship_exchange_cash_gross_revenue,
    product_order_non_token_unit_count,
    product_order_non_token_subtotal_excl_tariff_amount,
    meta_create_datetime,
    meta_update_datetime,
    meta_row_hash
    )
SELECT
    date,
    date_object,
    store_id,
    customer_id,
    activation_key,
    first_activation_key,
    associate_id,
    order_membership_classification_key,
    is_bop_vip,
    currency_object,
    currency_type,
    FALSE AS is_deleted,
    product_order_subtotal_excl_tariff_amount,
    product_order_tariff_amount,
    product_order_product_subtotal_amount,
    product_order_product_discount_amount,
    product_order_shipping_discount_amount,
    product_order_shipping_revenue_before_discount_amount,
    product_order_shipping_revenue_amount,
    product_order_tax_amount,
    product_order_cash_transaction_amount,
    product_order_cash_credit_redeemed_amount,
    product_order_noncash_credit_redeemed_amount,
    product_order_count_with_credit_redemption,
    product_order_count,
    product_order_count_excl_seeding,
    product_gift_order_count,
    gift_certificate_gift_order_count,
    product_order_unit_count,
    product_order_loyalty_unit_count,
    product_order_outfit_component_unit_count,
    product_order_outfit_count,
    product_order_discounted_unit_count,
    product_order_zero_revenue_unit_count,
    product_order_retail_unit_price,
    product_order_air_vip_price,
    product_order_price_offered_amount,
    product_order_landed_product_cost_amount,
    oracle_product_order_landed_product_cost_amount,
    product_order_landed_product_cost_amount_accounting,
    oracle_product_order_landed_product_cost_amount_accounting,
    product_order_shipping_cost_amount,
    product_order_shipping_supplies_cost_amount,
    product_order_misc_cogs_amount,
    product_order_direct_cogs_amount,
    product_order_direct_cogs_amount_accounting,
    product_order_cash_refund_amount,
    product_order_cash_credit_refund_amount,
    product_order_noncash_credit_refund_amount,
    product_order_cash_chargeback_amount,
    product_order_cost_product_returned_resaleable_amount,
    product_order_cost_product_returned_resaleable_amount_accounting,
    product_order_cost_product_returned_damaged_amount,
    product_order_cost_product_returned_damaged_amount_accounting,
    product_order_return_shipping_cost_amount,
    product_order_return_unit_count,
    product_order_reship_order_count,
    product_order_reship_unit_count,
    product_order_reship_direct_cogs_amount,
    product_order_reship_direct_cogs_amount_accounting,
    product_order_reship_product_cost_amount,
    oracle_product_order_reship_product_cost_amount,
    product_order_reship_product_cost_amount_accounting,
    oracle_product_order_reship_product_cost_amount_accounting,
    product_order_reship_shipping_cost_amount,
    product_order_reship_shipping_supplies_cost_amount,
    product_order_exchange_order_count,
    product_order_exchange_unit_count,
    product_order_exchange_direct_cogs_amount,
    product_order_exchange_direct_cogs_amount_accounting,
    product_order_exchange_product_cost_amount,
    oracle_product_order_exchange_product_cost_amount,
    product_order_exchange_product_cost_amount_accounting,
    oracle_product_order_exchange_product_cost_amount_accounting,
    product_order_exchange_shipping_cost_amount,
    product_order_exchange_shipping_supplies_cost_amount,
    product_order_selling_expenses_amount,
    product_order_payment_processing_cost_amount,
    product_order_variable_gms_cost_amount,
    product_order_variable_warehouse_cost_amount,
    billing_selling_expenses_amount,
    billing_payment_processing_cost_amount,
    billing_variable_gms_cost_amount,
    product_order_amount_to_pay,
    product_gross_revenue_excl_shipping,
    product_gross_revenue,
    product_net_revenue,
    product_margin_pre_return,
    product_margin_pre_return_excl_seeding,
    product_margin_pre_return_accounting,
    product_margin_pre_return_excl_seeding_accounting,
    product_margin_pre_return_excl_shipping,
    product_margin_pre_return_excl_shipping_accounting,
    product_gross_profit,
    product_gross_profit_accounting,
    product_variable_contribution_profit,
    product_variable_contribution_profit_accounting,
    product_order_cash_gross_revenue_amount,
    product_order_cash_net_revenue,
    product_order_cash_margin_pre_return,
    product_order_cash_margin_pre_return_accounting,
    product_order_cash_gross_profit,
    product_order_cash_gross_profit_accounting,
    billing_cash_gross_revenue,
    billing_cash_net_revenue,
    cash_gross_revenue,
    cash_net_revenue,
    cash_gross_profit,
    cash_gross_profit_accounting,
    cash_variable_contribution_profit,
    cash_variable_contribution_profit_accounting,
    billing_order_transaction_count,
    billed_credit_cash_transaction_count,
    on_retry_billed_credit_cash_transaction_count,
    billed_credit_cash_refund_count,
    billing_cash_refund_amount,
    billing_cash_chargeback_amount,
    billed_credit_cash_transaction_amount,
    membership_fee_cash_transaction_amount,
    gift_card_transaction_amount,
    legacy_credit_cash_transaction_amount,
    billed_credit_cash_refund_chargeback_amount,
    membership_fee_cash_refund_chargeback_amount,
    gift_card_cash_refund_chargeback_amount,
    legacy_credit_cash_refund_chargeback_amount,
    billed_cash_credit_issued_amount,
    billed_cash_credit_redeemed_amount,
    billed_cash_credit_cancelled_amount,
    billed_cash_credit_expired_amount,
    billed_cash_credit_issued_equivalent_count,
    billed_cash_credit_redeemed_equivalent_count,
    billed_cash_credit_redeemed_same_month_amount,
    billed_cash_credit_cancelled_equivalent_count,
    billed_cash_credit_expired_equivalent_count,
    refund_cash_credit_issued_amount,
    refund_cash_credit_redeemed_amount,
    refund_cash_credit_cancelled_amount,
    refund_cash_credit_expired_amount,
    other_cash_credit_issued_amount,
    other_cash_credit_redeemed_amount,
    other_cash_credit_cancelled_amount,
    other_cash_credit_expired_amount,
    noncash_credit_issued_amount,
    noncash_credit_cancelled_amount,
    noncash_credit_expired_amount,
    cash_gift_card_redeemed_amount,
    vip_store_id,
    activation_local_datetime,
    vip_cohort_month_date,
    cancellation_local_datetime,
    first_activation_local_datetime,
    first_vip_cohort_month_date,
    first_cancellation_local_datetime,
    is_cross_promo,
    gender,
    is_reactivated_vip,
    coalesce(is_retail_vip, false) as is_retail_vip,
    finance_specialty_store,
    is_scrubs_customer,
    reporting_landed_cost_local_amount,
    reporting_landed_cost_local_amount_accounting,
    retail_ship_only_product_order_count,
    retail_ship_only_product_order_unit_count,
    retail_ship_only_product_gross_revenue,
    retail_ship_only_product_gross_revenue_excl_shipping,
    retail_ship_only_product_margin_pre_return,
    retail_ship_only_product_margin_pre_return_accounting,
    retail_ship_only_product_margin_pre_return_excl_shipping,
    retail_ship_only_product_margin_pre_return_excl_shipping_accounting,
    retail_ship_only_product_order_cash_margin_pre_return,
    retail_ship_only_product_order_cash_margin_pre_return_accounting,
    retail_ship_only_product_order_cash_gross_revenue,
    retail_ship_only_product_net_revenue,
    retail_ship_only_product_gross_profit,
    retail_ship_only_product_gross_profit_accounting,
    retail_ship_only_product_order_cash_net_revenue,
    retail_ship_only_product_order_cash_gross_profit,
    retail_ship_only_product_order_cash_gross_profit_accounting,
    product_order_reship_exchange_cash_gross_revenue,
    product_order_non_token_unit_count,
    product_order_non_token_subtotal_excl_tariff_amount,
    $execution_start_time,
    $execution_start_time,
    meta_row_hash
FROM _finance_sales_ops_stg
ORDER BY
    date,
    customer_id;

UPDATE edw_prod.analytics_base.finance_sales_ops_stg AS fso
SET
    fso.gender = cl.gender,
    fso.finance_specialty_store = cl.finance_specialty_store,
    fso.is_cross_promo = cl.is_cross_promo,
    fso.meta_update_datetime = $execution_start_time
FROM _customer_list_gender_fss AS cl
WHERE fso.customer_id = cl.customer_id
    AND (NOT EQUAL_NULL(fso.gender, cl.gender)
        OR NOT EQUAL_NULL(fso.finance_specialty_store, cl.finance_specialty_store)
        OR NOT EQUAL_NULL(fso.is_cross_promo,cl.is_cross_promo));

--COMMIT;

--  Success
--UPDATE edw_prod.stg.meta_table_dependency_watermark
--SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
--        (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime FROM edw_prod.analytics_base.finance_sales_ops_stg)),
--    meta_update_datetime = CURRENT_TIMESTAMP()
--WHERE table_name = $target_table;
-- SELECT * FROM stg.meta_table_dependency_watermark;
