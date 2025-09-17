use warehouse medium_wh;
--SET target_table = 'analytics_base.customer_lifetime_value_monthly';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
--SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
SET is_full_refresh = TRUE;
--ALTER SESSION SET QUERY_TAG = $target_table;

--SET initial_warehouse = CURRENT_WAREHOUSE();
--USE WAREHOUSE IDENTIFIER ('da_wh_adhoc_large'); /* Always use Large Warehouse for this transform */

-- SELECT CURRENT_WAREHOUSE();
-- USE WAREHOUSE IDENTIFIER ('da_wh_adhoc_large'); -- Large Warehouse
-- USE WAREHOUSE IDENTIFIER ('da_wh_edw'); -- Normal Warehouse used by System (Low Concurrent Usage)
-- USE WAREHOUSE IDENTIFIER ('da_wh_etl'); -- Normal Warehouse used by System (High Concurrent Usage)
-- USE WAREHOUSE IDENTIFIER ('da_wh_analytics'); -- Normal Warehouse used by Individuals

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

--MERGE INTO stg.meta_table_dependency_watermark AS w
--USING (
--    SELECT
--        $target_table AS table_name,
--        t.dependent_table_name,
--        IFF(t.dependent_table_name IS NOT NULL, t.new_high_watermark_datetime, (
--            SELECT MAX(dt.max_meta_update_datetime) AS new_high_watermark_datetime
--            FROM (
--                SELECT MAX(meta_update_datetime) AS max_meta_update_datetime
--                FROM analytics_base.customer_lifetime_value_monthly_cust
--                UNION ALL
--                SELECT MAX(meta_update_datetime) AS max_meta_update_datetime
--                FROM analytics_base.customer_lifetime_value_monthly_agg
--                ) AS dt
--        )) AS new_high_watermark_datetime
--    FROM (
--        SELECT -- For self table
--            NULL AS dependent_table_name,
--            NULL AS new_high_watermark_datetime
--        UNION
--        SELECT
--            'edw_prod.analytics_base.finance_sales_ops_stg' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM analytics_base.finance_sales_ops_stg
--        UNION
--        SELECT
--            'edw_prod.stg.fact_activation' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM stg.fact_activation
--        UNION
--        SELECT
--            'edw_prod.stg.fact_order' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM stg.fact_order
--        UNION
--        SELECT
--            'edw_prod.stg.dim_customer' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM stg.dim_customer
--        UNION
--        SELECT
--            'edw_prod.stg.dim_customer_detail_history' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM stg.dim_customer_detail_history
--        UNION
--        SELECT
--            'edw_prod.stg.dim_order_membership_classification' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM stg.dim_order_membership_classification
--        UNION
--        SELECT
--            'lake_consolidated.ultra_merchant.membership' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM lake_consolidated.ultra_merchant.membership
--        UNION
--        SELECT
--            'lake_consolidated.ultra_merchant.membership_skip' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM lake_consolidated.ultra_merchant.membership_skip
--        UNION
--        SELECT
--            'lake_consolidated.ultra_merchant.membership_snooze_period' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM lake_consolidated.ultra_merchant.membership_snooze_period
--        UNION
--        SELECT
--            'lake_consolidated.ultra_merchant.session' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM lake_consolidated.ultra_merchant.session
--        UNION
--        SELECT
--            'reporting_base_prod.shared.session' AS dependent_table_name,
--            MAX(meta_update_datetime) AS new_high_watermark_datetime
--        FROM reporting_base_prod.shared.session
--        ) AS t
--    ORDER BY COALESCE(t.dependent_table_name, '')
--    ) AS s
--    ON w.table_name = s.table_name
--    AND w.dependent_table_name IS NOT DISTINCT FROM s.dependent_table_name
--WHEN NOT MATCHED THEN
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
--
---- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
--SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
--SET wm_edw_analytics_base_finance_sales_ops_stg = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.analytics_base.finance_sales_ops_stg'));
--SET wm_edw_stg_fact_activation = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_activation'));
--SET wm_edw_stg_fact_order = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order'));
--SET wm_edw_stg_dim_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer'));
--SET wm_edw_stg_dim_customer_detail_history = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer_detail_history'));
--SET wm_edw_stg_dim_order_membership_classification = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_order_membership_classification'));
--SET wm_lake_consolidated_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership'));
--SET wm_lake_consolidated_ultra_merchant_membership_skip = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_skip'));
--SET wm_lake_consolidated_ultra_merchant_membership_snooze_period = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_snooze_period'));
--SET wm_lake_consolidated_ultra_merchant_session = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.session'));
--SET wm_reporting_base_prod_shared_session = (SELECT stg.udf_get_watermark($target_table, 'reporting_base_prod.shared.session'));

/*
SELECT
    $wm_self,
    $wm_edw_analytics_base_finance_sales_ops_stg,
    $wm_edw_stg_fact_activation,
    $wm_edw_stg_dim_customer,
    $wm_edw_stg_dim_customer_detail_history,
    $wm_edw_stg_dim_order_membership_classification,
    $wm_lake_consolidated_ultra_merchant_membership,
    $wm_lake_consolidated_ultra_merchant_membership_skip,
    $wm_lake_consolidated_ultra_merchant_membership_snooze_period,
    $wm_lake_consolidated_ultra_merchant_session;
*/

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_prebase (customer_id INT, min_month_date DATE);

-- Full Refresh
INSERT INTO _customer_lifetime__customer_prebase (customer_id, min_month_date)
    SELECT src.customer_id, DATE_TRUNC(MONTH, MIN(src.date)) AS min_month_date
FROM (
    SELECT customer_id, date
    FROM analytics_base.finance_sales_ops_stg
    WHERE currency_object = 'usd'
        AND date_object = 'placed'
        AND NOT is_deleted
    UNION ALL
    SELECT customer_id, CAST(activation_local_datetime AS DATE) AS date
    FROM data_model_jfb.fact_activation
    ) AS src
WHERE $is_full_refresh  /* SET is_full_refresh = TRUE; */
GROUP BY src.customer_id
ORDER BY src.customer_id;

-- Incremental Refresh
--INSERT INTO _customer_lifetime__customer_prebase (customer_id, min_month_date)
--SELECT incr.customer_id, DATE_TRUNC(MONTH, MIN(incr.date)) AS min_month_date
--FROM (
--    SELECT customer_id, month_date AS date
--    FROM analytics_base.customer_lifetime_value_monthly_cust
--    WHERE meta_update_datetime > $wm_self
--    UNION ALL
--    SELECT customer_id, month_date AS date
--    FROM analytics_base.customer_lifetime_value_monthly_agg
--    WHERE meta_update_datetime > $wm_self
--    UNION ALL
--    SELECT fso.customer_id, fso.date
--    FROM analytics_base.finance_sales_ops_stg AS fso
--        LEFT JOIN stg.dim_order_membership_classification AS omc
--            ON omc.order_membership_classification_key = fso.order_membership_classification_key
--    WHERE (fso.meta_update_datetime > $wm_edw_analytics_base_finance_sales_ops_stg
--            OR omc.meta_update_datetime > $wm_edw_stg_dim_order_membership_classification)
--        AND fso.currency_object = 'usd'
--        AND fso.date_object = 'placed'
--    UNION ALL
--    SELECT customer_id, CAST(activation_local_datetime AS DATE) AS date
--    FROM data_model.fact_activation
--    WHERE meta_update_datetime > $wm_edw_stg_fact_activation
--    UNION ALL
--    SELECT customer_id, order_local_datetime::DATE AS date
--    FROM data_model.fact_order fo
--    JOIN data_model.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
--        AND dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing')
--    WHERE fo.meta_update_datetime > $wm_edw_stg_fact_order
--    UNION ALL /* Select all customers and the minimum change date from updated rows in dependent tables */
--    SELECT src.customer_id, MIN(src.date) AS date
--    FROM (
--        SELECT customer_id, CAST(meta_update_datetime AS DATE) AS date
--        FROM data_model.dim_customer
--        WHERE meta_update_datetime > $wm_edw_stg_dim_customer
--        UNION ALL
--        SELECT customer_id, CAST(meta_update_datetime AS DATE) AS date
--        FROM data_model.dim_customer
--        WHERE meta_update_datetime > $wm_edw_stg_dim_customer_detail_history
--        UNION ALL
--        SELECT m.customer_id, CAST(m.meta_update_datetime AS DATE) AS date
--        FROM lake_consolidated.ultra_merchant.membership AS m
--        WHERE m.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership
--        UNION ALL
--        SELECT m.customer_id, CAST(p.date_period_start AS DATE) AS date
--        FROM lake_consolidated.ultra_merchant.membership_skip AS ms
--            JOIN lake_consolidated.ultra_merchant.membership AS m
--                ON m.membership_id = ms.membership_id
--            JOIN lake_consolidated.ultra_merchant.period AS p
--                ON p.period_id = ms.period_id
--        WHERE ms.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership_skip
--        UNION ALL
--        SELECT m.customer_id, CAST(p.date_period_start AS DATE) AS date
--        FROM lake_consolidated.ultra_merchant.membership_snooze_period AS msp
--            JOIN lake_consolidated.ultra_merchant.membership AS m
--                ON m.membership_id = msp.membership_id
--            JOIN lake_consolidated.ultra_merchant.period AS p
--                ON p.period_id = msp.period_id
--        WHERE msp.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership_snooze_period
--        UNION ALL
--        SELECT s.customer_id, CAST(s.meta_update_datetime AS DATE) AS date
--        FROM lake_consolidated.ultra_merchant.session AS s
--        WHERE s.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_session
--        UNION ALL
--        SELECT s.customer_id, CAST(s.meta_update_datetime AS DATE) AS date
--        FROM reporting_base_prod.shared.session AS s
--        WHERE s.meta_update_datetime > $wm_reporting_base_prod_shared_session
--        ) AS src
--    GROUP BY src.customer_id
--    ) AS incr
--WHERE NOT $is_full_refresh
--GROUP BY incr.customer_id
--ORDER BY incr.customer_id;
-- SELECT * FROM _customer_lifetime__customer_prebase;

SET entering_new_month = (SELECT IFF(DAYOFMONTH($execution_start_time::DATE) = 2, TRUE, FALSE));

/* Get distinct activation keys for customer in prebase going forward from their min_month_date */
CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_base AS
SELECT base.customer_id, base.activation_key, base.vip_store_id, base.month_date, MIN(mc.month_date) AS guest_cohort_month_date
FROM (
    SELECT fso.customer_id, fso.activation_key, fso.vip_store_id, MIN(DATE_TRUNC(MONTH, fso.date)) AS month_date
    FROM analytics_base.finance_sales_ops_stg AS fso
        JOIN _customer_lifetime__customer_prebase AS clcp
            ON fso.customer_id = clcp.customer_id
            AND fso.date >= clcp.min_month_date
    WHERE fso.date_object = 'placed'
        AND fso.currency_object = 'usd'
        AND fso.is_deleted = FALSE
    GROUP BY fso.customer_id, fso.activation_key, fso.vip_store_id

    UNION ALL

    SELECT fa.customer_id, fa.activation_key, fa.sub_store_id as vip_store_id, fa.vip_cohort_month_date AS month_date
    FROM data_model_jfb.fact_activation AS fa
        JOIN _customer_lifetime__customer_prebase AS clcp
            ON fa.customer_id = clcp.customer_id
            AND fa.vip_cohort_month_date >= clcp.min_month_date

    UNION ALL

/* This makes sure we captured people who had a Failed Billing but didn't have a record in FSO or reactivation during this time span */
    SELECT fo.customer_id,
        fo.activation_key,
        IFF(fa.sub_store_id <> -1, fa.sub_store_id, vsi.vip_store_id) AS vip_store_id,
        MIN(DATE_TRUNC(MONTH, fo.order_local_datetime))::DATE AS month_date
    FROM _customer_lifetime__customer_prebase AS cp
    JOIN data_model_jfb.fact_order fo
        ON cp.customer_id = fo.customer_id
    JOIN data_model_jfb.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
        AND dosc.order_classification_l2 IN ('Credit Billing', 'Token Billing')
    JOIN data_model_jfb.fact_activation fa ON fa.activation_key = fo.activation_key
    LEFT JOIN (
        SELECT DISTINCT a.store_id,
           COALESCE(b.store_id, a.store_id) as vip_store_id
        FROM data_model_jfb.dim_store AS a
        LEFT JOIN data_model_jfb.dim_store AS b ON b.store_brand = a.store_brand
            AND b.store_country = a.store_country
            AND b.store_type = 'Online'
            AND a.store_type <> 'Online'
            AND b.is_core_store = TRUE
            AND b.store_full_name NOT IN ('JustFab - Wholesale', 'PS by JustFab')
        WHERE a.is_core_store = TRUE
    ) AS vsi ON vsi.store_id = fo.store_id
    GROUP BY fo.customer_id,
        fo.activation_key,
        IFF(fa.sub_store_id <> -1, fa.sub_store_id, vsi.vip_store_id)

    UNION ALL
/* This makes sure we captured people who skipped but didn't have a record in FSO or reactivation during this time span */
    SELECT
        m.customer_id,
        fa.activation_key,
        m.store_id,
        MIN(p.date_period_start::date) AS date
    FROM _customer_lifetime__customer_prebase AS cp
        JOIN lake_jfb.ultra_merchant.membership AS m
            ON m.customer_id = cp.customer_id
        JOIN lake_jfb.ultra_merchant.membership_skip AS ms
            ON ms.membership_id = m.membership_id
        JOIN lake_jfb.ultra_merchant.period AS p
            ON p.period_id = ms.period_id
        JOIN data_model_jfb.fact_activation AS fa
            ON fa.customer_id = m.customer_id
            AND fa.activation_local_datetime < p.date_period_start
            AND fa.cancellation_local_datetime >= p.date_period_start
--    WHERE ms.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership_skip
--    AND NOT $is_full_refresh
    where NOT $is_full_refresh
    GROUP BY
        m.customer_id,
        fa.activation_key,
        m.store_id
    ) AS base
    LEFT JOIN analytics_base.customer_lifetime_value_monthly_cust_jfb AS mc
        ON mc.customer_id = base.customer_id
        AND mc.vip_store_id = base.vip_store_id
        AND mc.activation_key = base.activation_key

GROUP BY
    base.customer_id,
    base.activation_key,
    base.vip_store_id,
    base.month_date;
-- SELECT * FROM _customer_lifetime__customer_base;
-- SELECT customer_id, activation_key, COUNT(1) FROM _customer_lifetime__customer_base GROUP BY 1, 2 HAVING COUNT(1) > 1;

/* if it's the 2nd of the month, we are refreshing through the 1st and need to carry all customers from the previous month over */
INSERT INTO _customer_lifetime__customer_base
SELECT
    mc.customer_id,
    mc.activation_key,
    mc.vip_store_id,
    DATE_TRUNC(MONTH, $execution_start_time::DATE) AS month_date,
    MIN(mc.guest_cohort_month_date) AS guest_cohort_month_date
FROM analytics_base.customer_lifetime_value_monthly_cust_jfb AS mc
WHERE $entering_new_month = TRUE
    AND mc.month_date = DATEADD(month, -1, DATE_TRUNC(MONTH, $execution_start_time::DATE))
    AND NOT EXISTS(
        SELECT TRUE AS is_exists
        FROM _customer_lifetime__customer_base AS base
        WHERE base.customer_id = mc.customer_id
            AND base.activation_key = mc.activation_key
            AND base.vip_store_id = mc.vip_store_id
        )
GROUP BY
    mc.customer_id,
    mc.activation_key,
    mc.vip_store_id;
-- SELECT * FROM _customer_lifetime__customer_base;
-- SELECT customer_id, activation_key, COUNT(1) FROM _customer_lifetime__customer_base GROUP BY 1, 2 HAVING COUNT(1) > 1;

/* Ensure there are no duplicates */
INSERT OVERWRITE INTO _customer_lifetime__customer_base
SELECT
    base.customer_id,
    base.activation_key,
    base.vip_store_id,
    MIN(COALESCE(base.month_date, '1900-01-01')) AS month_date,
    MIN(COALESCE(base.guest_cohort_month_date, '1900-01-01')) AS guest_cohort_month_date
FROM _customer_lifetime__customer_base AS base
WHERE base.customer_id != -1
GROUP BY
    base.customer_id,
    base.activation_key,
    base.vip_store_id;
-- SELECT * FROM _customer_lifetime__customer_base;
-- SELECT customer_id, activation_key, COUNT(1) FROM _customer_lifetime__customer_base GROUP BY 1, 2 HAVING COUNT(1) > 1;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__activation AS
SELECT
    stg.customer_id,
    fa.activation_key,
    fa.membership_event_key,
    fa.activation_local_datetime,
    fa.next_activation_local_datetime,
    fa.activation_sequence_number,
    fa.store_id,
    fa.sub_store_id,
    fa.vip_cohort_month_date,
    fa.source_next_activation_local_datetime,
    fa.cancellation_local_datetime,
    fa.cancel_type,
    fa.membership_type,
    fa.is_retail_vip,
    fa.is_reactivated_vip,
    ROW_NUMBER() OVER (PARTITION BY stg.customer_id ORDER BY fa.activation_sequence_number, fa.activation_local_datetime) AS row_num
FROM (SELECT DISTINCT customer_id FROM _customer_lifetime__customer_base) AS stg
    JOIN data_model_jfb.fact_activation AS fa
    	ON fa.customer_id = stg.customer_id
ORDER BY stg.customer_id;
-- SELECT * FROM _customer_lifetime__activation;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_month (
    month_date DATE,
    customer_id NUMBER(38, 0),
    activation_key NUMBER(38, 0),
    first_activation_key NUMBER(38, 0),
    gender VARCHAR(75),
    store_id NUMBER(38, 0),
    vip_store_id NUMBER(38, 0),
    guest_cohort_month_date DATE,
    vip_cohort_month_date DATE,
    is_bop_vip BOOLEAN,
    is_reactivated_vip BOOLEAN,
    is_cancel BOOLEAN,
    is_cancel_before_6th BOOLEAN,
    is_passive_cancel BOOLEAN,
    is_skip BOOLEAN,
    is_snooze BOOLEAN,
    is_login BOOLEAN,
    segment_activity BOOLEAN,
    is_successful_billing BOOLEAN,
    is_pending_billing BOOLEAN,
    is_failed_billing BOOLEAN,
    is_cross_promo BOOLEAN,
    is_retail_vip BOOLEAN,
    is_scrubs_customer BOOLEAN,
    membership_type VARCHAR(500),
    finance_specialty_store VARCHAR(75),
    membership_type_entering_month VARCHAR(500),
    vip_cohort_rank NUMBER(38, 0),
    vip_unredeemed_credits_rank NUMBER(38, 0)
    );

/* Create customer records going till current month starting from their min month */
INSERT INTO _customer_lifetime__customer_month (
    month_date,
    customer_id,
    activation_key,
    first_activation_key,
    gender,
    store_id,
    vip_store_id,
    guest_cohort_month_date,
    vip_cohort_month_date,
    is_bop_vip,
    is_reactivated_vip,
    is_cancel,
    is_cancel_before_6th,
    is_passive_cancel,
    is_cross_promo,
    is_retail_vip,
    is_scrubs_customer,
    membership_type,
    finance_specialty_store,
    membership_type_entering_month,
    vip_cohort_rank,
    vip_unredeemed_credits_rank
    )
SELECT
    d.month_date,
    cb.customer_id,
    COALESCE(a.activation_key, -1) AS activation_key,
    COALESCE(a1.activation_key, -1) AS first_activation_key,
    c.gender,
    IFF(a.store_id IS NOT NULL AND a.store_id != -1, a.store_id, cb.vip_store_id) AS store_id,
    IFF(a.sub_store_id IS NOT NULL AND a.sub_store_id != -1, a.sub_store_id, cb.vip_store_id) AS vip_store_id_calc,
    IFF(a.activation_key != -1, '1900-01-01', COALESCE(NULLIF(cb.guest_cohort_month_date,'1900-01-01'),cb.MONTH_DATE, '1900-01-01')) AS guest_cohort_month_date,
    COALESCE(a.vip_cohort_month_date, '1900-01-01') AS vip_cohort_month_date,
    CASE WHEN IFF(a.store_id IS NOT NULL AND a.store_id != -1, a.store_id, cb.vip_store_id) = 55
            AND a.cancellation_local_datetime > a.source_next_activation_local_datetime
            AND a.vip_cohort_month_date < d.month_date AND a.next_activation_local_datetime >= d.month_date THEN TRUE
        WHEN NOT (
                a.cancellation_local_datetime > a.source_next_activation_local_datetime
                AND IFF(a.store_id IS NOT NULL AND a.store_id != -1, a.store_id, cb.vip_store_id) = 55
            )
            AND a.vip_cohort_month_date < d.month_date AND a.cancellation_local_datetime::DATE >= d.month_date THEN TRUE
        ELSE FALSE END AS is_bop_vip,
    COALESCE(a.is_reactivated_vip, FALSE) AS is_reactivated_vip,
    IFF(DATEDIFF(month, d.month_date, a.cancellation_local_datetime::DATE) = 0, TRUE, FALSE) AS is_cancel,
    IFF(a.vip_cohort_month_date < d.month_date AND DATEDIFF(month, d.month_date, a.cancellation_local_datetime::DATE) = 0 AND DATE_PART(day, a.cancellation_local_datetime) < 6, TRUE, FALSE) AS is_cancel_before_6th,
    IFF(DATEDIFF(month, d.month_date, a.cancellation_local_datetime::DATE) = 0 AND a.cancel_type = 'Passive', TRUE, FALSE) AS is_passive_cancel,
    IFF(a.activation_key != -1, COALESCE(c.is_cross_promo, FALSE), FALSE) AS is_cross_promo,
    COALESCE(a.is_retail_vip, FALSE) AS is_retail_vip,
    IFF(ds.store_brand = 'Fabletics', c.is_scrubs_customer, FALSE) AS is_scrubs_customer,
    COALESCE(a.membership_type, 'Unknown') AS membership_type,
    IFF(a.activation_key != -1, COALESCE(c.finance_specialty_store, 'Unknown'), 'Unknown') AS finance_specialty_store,
    IFF(a.activation_key != -1, COALESCE(cdh.membership_type, 'Unknown'), 'Unknown') AS membership_type_entering_month,
    ROW_NUMBER() OVER (PARTITION BY d.month_date, cb.customer_id, vip_store_id_calc ORDER BY COALESCE(a.vip_cohort_month_date, '1900-01-01') DESC) AS vip_cohort_rank,
    ROW_NUMBER() OVER (PARTITION BY d.month_date, cb.customer_id ORDER BY is_bop_vip DESC,COALESCE(a.vip_cohort_month_date, '1900-01-01') DESC,  COALESCE(a.is_reactivated_vip, FALSE) DESC) AS vip_unredeemed_credits_rank
FROM _customer_lifetime__customer_base AS cb
    JOIN (SELECT DISTINCT month_date FROM stg.dim_date WHERE month_date < $execution_start_time::DATE AND is_current) AS d
        ON d.month_date >= cb.month_date
    JOIN data_model_jfb.dim_customer AS c
        ON c.customer_id = cb.customer_id
    LEFT JOIN _customer_lifetime__activation AS a
        ON a.activation_key = cb.activation_key
    LEFT JOIN _customer_lifetime__activation AS a1
        ON a1.customer_id = cb.customer_id
        AND a1.row_num = 1
        AND a.activation_key != -1
    LEFT JOIN data_model_jfb.dim_store AS ds
        ON ds.store_id = IFF(a.sub_store_id IS NOT NULL AND a.sub_store_id != -1, a.sub_store_id, cb.vip_store_id)
    LEFT JOIN data_model_jfb.dim_customer_detail_history AS cdh
        ON cdh.customer_id = cb.customer_id
        AND cdh.effective_start_datetime < d.month_date
        AND cdh.effective_end_datetime >= d.month_date;
-- SELECT * FROM _customer_lifetime__customer_month;

CREATE OR REPLACE TEMP TABLE _credit_token_final_activity AS
WITH _credit_token_activity AS
         (SELECT cm.customer_id,
               fce.credit_key,
               fce.credit_id,
               fce.credit_activity_type,
               fce.credit_activity_local_datetime
          FROM (SELECT DISTINCT customer_id FROM _customer_lifetime__customer_month) cm
                   JOIN data_model_jfb.dim_credit dc ON cm.customer_id = dc.customer_id
                   JOIN data_model_jfb.fact_credit_event fce ON dc.credit_key = fce.credit_key
          WHERE dc.credit_type IN ('Token', 'Fixed Credit')
            AND dc.credit_tender = 'Cash'
            AND dc.credit_reason IN ('Token Billing', 'Membership Credit', 'Converted Membership Credit'))

SELECT dates.customer_id,
       dates.credit_key,
       dates.credit_id,
       dates.issued AS                                             credit_issued_datetime,
       IFF(dates.redeemed IS NULL, '9999-12-01', dates.redeemed)   credit_redeemed_datetime,
       IFF(dates.cancelled IS NULL, '9999-12-01', dates.cancelled) credit_cancelled_datetime,
       IFF(dates.converted IS NULL, '9999-12-01', dates.converted) credit_converted_datetime
FROM _credit_token_activity
         PIVOT (MIN(credit_activity_local_datetime) FOR credit_activity_type
         IN ('Issued','Redeemed','Cancelled','Converted To Token')) AS dates (customer_id,
                                                                              credit_key,
                                                                              credit_id,
                                                                              issued,
                                                                              redeemed,
                                                                              cancelled,
                                                                              converted);

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_bop_unredeemed_credits AS
WITH _credits_identified AS
         (SELECT customer_id,
                 credit_key,
                 credit_id,
                 CASE
                     WHEN credit_redeemed_datetime IS NOT NULL AND credit_redeemed_datetime <> '9999-12-01'
                         THEN 'Redeemed'
                     WHEN credit_cancelled_datetime IS NOT NULL AND credit_cancelled_datetime <> '9999-12-01'
                         THEN 'Cancelled'
                     WHEN credit_converted_datetime IS NOT NULL AND credit_converted_datetime <> '9999-12-01'
                         THEN 'Converted'
                     ELSE 'Issued'
                     END                   credit_history,
                 credit_issued_datetime AS credit_start_datetime,
                 CASE
                     WHEN credit_history = 'Issued' THEN '9999-12-01'
                     WHEN credit_history = 'Redeemed' THEN credit_redeemed_datetime
                     WHEN credit_history = 'Cancelled' THEN credit_cancelled_datetime
                     WHEN credit_history = 'Converted' THEN credit_converted_datetime
                     ELSE '9999-12-01'
                     END                   credit_end_datetime
          FROM _credit_token_final_activity)

SELECT cm.customer_id,
       cm.activation_key,
       cm.month_date,
       COUNT(credit_id) AS entering_month_unredeemed_credits_per_customer
FROM _customer_lifetime__customer_month cm
         LEFT JOIN _credits_identified c
                   ON c.customer_id = cm.customer_id AND c.credit_start_datetime <= cm.month_date AND
                      c.credit_end_datetime > cm.month_date
WHERE vip_unredeemed_credits_rank = 1
GROUP BY cm.customer_id, cm.activation_key, cm.month_date;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_skip AS
SELECT DISTINCT cm.month_date, cm.customer_id, cm.activation_key, TRUE AS is_skip
FROM _customer_lifetime__customer_month AS cm
    JOIN lake_jfb.ultra_merchant.membership AS m
        ON m.customer_id = cm.customer_id
    JOIN lake_jfb.ultra_merchant.membership_skip AS ms
        ON ms.membership_id = m.membership_id
    JOIN lake_jfb.ultra_merchant.period AS p
        ON p.period_id = ms.period_id
WHERE cm.month_date = p.date_period_start
    AND cm.vip_cohort_rank = 1;
-- SELECT * FROM _customer_lifetime__customer_skip;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_snooze AS
SELECT DISTINCT cm.month_date, cm.customer_id, cm.activation_key, TRUE AS is_snooze
FROM _customer_lifetime__customer_month AS cm
    JOIN lake_jfb.ultra_merchant.membership AS m
        ON m.customer_id = cm.customer_id
    JOIN lake_jfb.ultra_merchant.membership_snooze_period AS msp
        ON msp.membership_id = m.membership_id
        AND (msp.membership_period_id IS NOT NULL OR msp.membership_billing_id IS NOT NULL)
    JOIN lake_jfb.ultra_merchant.period AS p
        ON p.period_id = msp.period_id
WHERE cm.month_date = p.date_period_start
    AND cm.vip_cohort_rank = 1;
-- SELECT * FROM _customer_lifetime__customer_snooze;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_login AS
SELECT DISTINCT cm.month_date, cm.customer_id, cm.activation_key, TRUE AS is_login
FROM _customer_lifetime__customer_month AS cm
    JOIN lake_jfb.ultra_merchant.session AS s
        ON s.customer_id = cm.customer_id
WHERE cm.month_date = DATE_TRUNC(MONTH, s.date_added)
    AND cm.vip_cohort_rank = 1;
-- SELECT * FROM _customer_lifetime__customer_login;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_segment_activity AS
SELECT DISTINCT cm.month_date, cm.customer_id, cm.activation_key, TRUE AS segment_activity
FROM _customer_lifetime__customer_month AS cm
    JOIN reporting_base_prod.shared.session AS s
        ON s.customer_id = cm.customer_id
WHERE cm.month_date = DATE_TRUNC(MONTH, s.session_local_datetime::DATE)
    AND s.is_bot = FALSE
    AND IFNULL(s.is_in_segment,TRUE) = TRUE
    AND cm.vip_cohort_rank = 1;
-- SELECT * FROM _customer_lifetime__customer_segment_activity;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_billing AS
SELECT
    cm.month_date,
    cm.customer_id,
    cm.activation_key,
    MAX(IFF(os.order_status IN ('Failure', 'Cancelled'), TRUE, FALSE)) AS is_failed_billing,
    MAX(IFF(os.order_status = 'Success', TRUE, FALSE)) AS is_successful_billing,
    MAX(IFF(os.order_status = 'Pending'
        AND DATE_TRUNC(MONTH, o.order_local_datetime::DATE) = DATE_TRUNC(MONTH, DATEADD(day, -1, current_date)), TRUE, FALSE)) AS is_pending_billing
FROM _customer_lifetime__customer_month AS cm
    JOIN data_model_jfb.fact_order AS o
        ON o.customer_id = cm.customer_id
        AND o.activation_key = cm.activation_key
        AND DATE_TRUNC(MONTH, o.order_local_datetime::DATE) = cm.month_date
    JOIN data_model_jfb.dim_order_sales_channel AS osc
        ON osc.order_sales_channel_key = o.order_sales_channel_key
        AND osc.order_classification_l2 IN ('Credit Billing', 'Token Billing')
    JOIN data_model_jfb.dim_order_status AS os
        ON os.order_status_key = o.order_status_key
GROUP BY cm.month_date, cm.customer_id, cm.activation_key;
-- SELECT * FROM _customer_lifetime__customer_billing;


CREATE OR REPLACE TEMP TABLE _customer_lifetime__vip_store_id AS
SELECT COALESCE(b.store_id, a.store_id) as vip_store_id,
       a.store_id,
       COALESCE(b.store_full_name, a.store_full_name) AS vip_store_full_name,
       a.store_full_name
FROM data_model_jfb.dim_store AS a
         LEFT JOIN data_model_jfb.dim_store AS b
            ON b.store_brand = a.store_brand
            AND b.store_country = a.store_country
            AND b.store_type = 'Online'
            AND a.store_type <> 'Online'
            AND b.is_core_store = TRUE
            AND b.store_full_name NOT IN ('JustFab - Wholesale', 'PS by JustFab')
WHERE a.is_core_store = TRUE;

/* Aggregate finance sales ops values only for the customers we need */
CREATE OR REPLACE TEMP TABLE _customer_lifetime__ltv_store_type AS
SELECT
    cm.month_date,
    cm.customer_id,
    cm.activation_key,
    cm.vip_store_id,
    LOWER(st.store_type) AS store_type,
    SUM(fso.product_order_count) AS product_order_count,
    SUM(fso.product_order_count_with_credit_redemption) AS product_order_count_with_credit_redemption,
    SUM(fso.product_order_unit_count) AS product_order_unit_count,
    SUM(fso.product_order_subtotal_excl_tariff_amount) AS product_order_subtotal_excl_tariff_amount,
    SUM(fso.product_order_product_subtotal_amount) AS product_order_product_subtotal_amount,
    SUM(fso.product_order_product_discount_amount) AS product_order_product_discount_amount,
    SUM(fso.product_order_shipping_revenue_amount) AS product_order_shipping_revenue_amount,
    SUM(fso.product_order_direct_cogs_amount) AS product_order_direct_cogs_amount,
    SUM(fso.product_order_selling_expenses_amount) AS product_order_selling_expenses_amount,
    SUM(fso.product_order_cash_refund_amount + fso.product_order_cash_chargeback_amount) AS product_order_cash_refund_amount_and_chargeback_amount,
    SUM(fso.product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount,
    SUM(fso.product_order_exchange_order_count + fso.product_order_reship_order_count) AS product_order_reship_exchange_order_count,
    SUM(fso.product_order_exchange_unit_count + fso.product_order_reship_unit_count) AS product_order_reship_exchange_unit_count,
    SUM(fso.product_order_reship_direct_cogs_amount + fso.product_order_exchange_direct_cogs_amount) AS product_order_reship_exchange_direct_cogs_amount,
    SUM(fso.product_order_return_shipping_cost_amount - fso.product_order_cost_product_returned_resaleable_amount) AS product_order_return_cogs_amount,
    SUM(fso.product_order_return_unit_count) AS product_order_return_unit_count,
    SUM(fso.product_order_amount_to_pay) AS product_order_amount_to_pay,
    SUM(fso.product_gross_revenue_excl_shipping) AS product_gross_revenue_excl_shipping,
    SUM(fso.product_gross_revenue) AS product_gross_revenue,
    SUM(fso.product_net_revenue) AS product_net_revenue,
    SUM(fso.product_margin_pre_return) AS product_margin_pre_return,
    SUM(fso.product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping,
    SUM(fso.product_gross_profit) AS product_gross_profit,
    SUM(fso.product_variable_contribution_profit) AS product_variable_contribution_profit,
    SUM(fso.product_order_cash_gross_revenue_amount) AS product_order_cash_gross_revenue_amount,
    SUM(fso.product_order_cash_net_revenue) AS product_order_cash_net_revenue,
    SUM(fso.product_order_landed_product_cost_amount) AS product_order_landed_product_cost_amount,
    SUM(fso.product_order_cash_margin_pre_return) AS product_order_cash_margin_pre_return,
    SUM(fso.product_order_cash_gross_profit) AS product_order_cash_gross_profit,
    SUM(fso.billing_cash_gross_revenue) AS billing_cash_gross_revenue,
    SUM(fso.billing_cash_net_revenue) AS billing_cash_net_revenue,
    SUM(fso.cash_gross_revenue) AS cash_gross_revenue,
    SUM(fso.cash_net_revenue) AS cash_net_revenue,
    SUM(fso.cash_gross_profit) AS cash_gross_profit,
    SUM(fso.cash_variable_contribution_profit) AS cash_variable_contribution_profit,
    SUM(fso.billed_credit_cash_transaction_amount) AS monthly_billed_credit_cash_transaction_amount,
    SUM(fso.membership_fee_cash_transaction_amount) AS membership_fee_cash_transaction_amount,
    SUM(fso.gift_card_transaction_amount) AS gift_card_transaction_amount,
    SUM(fso.legacy_credit_cash_transaction_amount) AS legacy_credit_cash_transaction_amount,
    SUM(fso.billed_credit_cash_refund_count) AS monthly_billed_credit_cash_refund_count,
    SUM(fso.billed_credit_cash_refund_chargeback_amount) AS monthly_billed_credit_cash_refund_chargeback_amount,
    SUM(fso.membership_fee_cash_refund_chargeback_amount) AS membership_fee_cash_refund_chargeback_amount,
    SUM(fso.gift_card_cash_refund_chargeback_amount) AS gift_card_cash_refund_chargeback_amount,
    SUM(fso.legacy_credit_cash_refund_chargeback_amount) AS legacy_credit_cash_refund_chargeback_amount,
    SUM(fso.billed_cash_credit_issued_amount) AS billed_cash_credit_issued_amount,
    SUM(fso.billed_cash_credit_redeemed_amount) AS billed_cash_credit_redeemed_amount,
    SUM(fso.billed_cash_credit_cancelled_amount) AS billed_cash_credit_cancelled_amount,
    SUM(fso.billed_cash_credit_expired_amount) AS billed_cash_credit_expired_amount,
    SUM(fso.billed_cash_credit_issued_equivalent_count) AS billed_cash_credit_issued_equivalent_count,
    SUM(fso.billed_cash_credit_redeemed_same_month_amount) AS billed_cash_credit_redeemed_same_month_amount,
    SUM(fso.billed_cash_credit_redeemed_equivalent_count) AS billed_cash_credit_redeemed_equivalent_count,
    SUM(fso.billed_cash_credit_cancelled_equivalent_count) AS billed_cash_credit_cancelled_equivalent_count,
    SUM(fso.billed_cash_credit_expired_equivalent_count) AS billed_cash_credit_expired_equivalent_count,
    SUM(fso.refund_cash_credit_issued_amount) AS refund_cash_credit_issued_amount,
    SUM(fso.refund_cash_credit_redeemed_amount) AS refund_cash_credit_redeemed_amount,
    SUM(fso.refund_cash_credit_cancelled_amount) AS refund_cash_credit_cancelled_amount,
    SUM(fso.refund_cash_credit_expired_amount) AS refund_cash_credit_expired_amount,
    SUM(fso.other_cash_credit_issued_amount) AS other_cash_credit_issued_amount,
    SUM(fso.other_cash_credit_redeemed_amount) AS other_cash_credit_redeemed_amount,
    SUM(fso.other_cash_credit_cancelled_amount) AS other_cash_credit_cancelled_amount,
    SUM(fso.other_cash_credit_expired_amount) AS other_cash_credit_expired_amount,
    SUM(fso.noncash_credit_issued_amount) AS noncash_credit_issued_amount,
    SUM(fso.noncash_credit_cancelled_amount) AS noncash_credit_cancelled_amount,
    SUM(fso.noncash_credit_expired_amount) AS noncash_credit_expired_amount,
    SUM(IFF(omc.membership_order_type_l3 = 'First Guest', fso.product_margin_pre_return, 0)) AS first_guest_product_margin_pre_return
FROM _customer_lifetime__customer_month AS cm
    JOIN analytics_base.finance_sales_ops_stg AS fso
        ON fso.customer_id = cm.customer_id
        AND fso.activation_key = cm.activation_key
        AND DATE_TRUNC(MONTH, fso.date) = cm.month_date
        AND cm.vip_store_id = fso.vip_store_id
    JOIN stg.dim_store AS st
        ON st.store_id = fso.store_id
    JOIN stg.dim_order_membership_classification AS omc
        ON omc.order_membership_classification_key = fso.order_membership_classification_key
WHERE fso.date_object = 'placed'
    AND fso.currency_object = 'usd'
    AND NOT fso.is_deleted
    AND fso.date < CURRENT_DATE
GROUP BY
    cm.month_date,
    cm.customer_id,
    cm.activation_key,
    cm.vip_store_id,
    LOWER(st.store_type);
-- SELECT * FROM _customer_lifetime__ltv_store_type;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__ltv AS
SELECT
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    SUM(product_order_count) AS product_order_count,
    SUM(IFF(store_type = 'online', product_order_count, 0)) AS online_product_order_count,
    SUM(IFF(store_type = 'retail', product_order_count, 0)) AS retail_product_order_count,
    SUM(IFF(store_type = 'mobile app', product_order_count, 0)) AS mobile_app_product_order_count,
    SUM(product_order_count_with_credit_redemption) AS product_order_count_with_credit_redemption,
    SUM(product_order_unit_count) AS product_order_unit_count,
    SUM(IFF(store_type = 'online', product_order_unit_count, 0)) AS online_product_order_unit_count,
    SUM(IFF(store_type = 'retail', product_order_unit_count, 0)) AS retail_product_order_unit_count,
    SUM(IFF(store_type = 'mobile app', product_order_unit_count, 0)) AS mobile_app_product_order_unit_count,
    SUM(product_order_subtotal_excl_tariff_amount) AS product_order_subtotal_excl_tariff_amount,
    SUM(product_order_product_subtotal_amount) AS product_order_product_subtotal_amount,
    SUM(IFF(store_type = 'online', product_order_subtotal_excl_tariff_amount, 0)) AS online_product_order_subtotal_excl_tariff_amount,
    SUM(IFF(store_type = 'retail', product_order_subtotal_excl_tariff_amount, 0)) AS retail_product_order_subtotal_excl_tariff_amount,
    SUM(IFF(store_type = 'mobile app', product_order_subtotal_excl_tariff_amount, 0)) AS mobile_app_product_order_subtotal_excl_tariff_amount,
    SUM(product_order_product_discount_amount) AS product_order_product_discount_amount,
    SUM(IFF(store_type = 'online', product_order_product_discount_amount, 0)) AS online_product_order_product_discount_amount,
    SUM(IFF(store_type = 'retail', product_order_product_discount_amount, 0)) AS retail_product_order_product_discount_amount,
    SUM(IFF(store_type = 'mobile app', product_order_product_discount_amount, 0)) AS mobile_app_product_order_product_discount_amount,
    SUM(product_order_shipping_revenue_amount) AS product_order_shipping_revenue_amount,
    SUM(IFF(store_type = 'online', product_order_shipping_revenue_amount, 0)) AS online_product_order_shipping_revenue_amount,
    SUM(IFF(store_type = 'retail', product_order_shipping_revenue_amount, 0)) AS retail_product_order_shipping_revenue_amount,
    SUM(IFF(store_type = 'mobile app', product_order_shipping_revenue_amount, 0)) AS mobile_app_product_order_shipping_revenue_amount,
    SUM(product_order_direct_cogs_amount) AS product_order_direct_cogs_amount,
    SUM(IFF(store_type = 'online', product_order_direct_cogs_amount, 0)) AS online_product_order_direct_cogs_amount,
    SUM(IFF(store_type = 'retail', product_order_direct_cogs_amount, 0)) AS retail_product_order_direct_cogs_amount,
    SUM(IFF(store_type = 'mobile app', product_order_direct_cogs_amount, 0)) AS mobile_app_product_order_direct_cogs_amount,
    SUM(product_order_selling_expenses_amount) AS product_order_selling_expenses_amount,
    SUM(IFF(store_type = 'online', product_order_selling_expenses_amount, 0)) AS online_product_order_selling_expenses_amount,
    SUM(IFF(store_type = 'retail', product_order_selling_expenses_amount, 0)) AS retail_product_order_selling_expenses_amount,
    SUM(IFF(store_type = 'mobile app', product_order_selling_expenses_amount, 0)) AS mobile_app_product_order_selling_expenses_amount,
    SUM(product_order_cash_refund_amount_and_chargeback_amount) AS product_order_cash_refund_amount_and_chargeback_amount,
    SUM(IFF(store_type = 'online', product_order_cash_refund_amount_and_chargeback_amount, 0)) AS online_product_order_cash_refund_chargeback_amount,
    SUM(IFF(store_type = 'retail', product_order_cash_refund_amount_and_chargeback_amount, 0)) AS retail_product_order_cash_refund_chargeback_amount,
    SUM(IFF(store_type = 'mobile app', product_order_cash_refund_amount_and_chargeback_amount, 0)) AS mobile_app_product_order_cash_refund_chargeback_amount,
    SUM(product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount,
    SUM(IFF(store_type = 'online', product_order_cash_credit_refund_amount, 0)) AS online_product_order_cash_credit_refund_amount,
    SUM(IFF(store_type = 'retail', product_order_cash_credit_refund_amount, 0)) AS retail_product_order_cash_credit_refund_amount,
    SUM(IFF(store_type = 'mobile app', product_order_cash_credit_refund_amount, 0)) AS mobile_app_product_order_cash_credit_refund_amount,
    SUM(product_order_reship_exchange_order_count) AS product_order_reship_exchange_order_count,
    SUM(IFF(store_type = 'online', product_order_reship_exchange_order_count, 0)) AS online_product_order_reship_exchange_order_count,
    SUM(IFF(store_type = 'retail', product_order_reship_exchange_order_count, 0)) AS retail_product_order_reship_exchange_order_count,
    SUM(IFF(store_type = 'mobile app', product_order_reship_exchange_order_count, 0)) AS mobile_app_product_order_reship_exchange_order_count,
    SUM(product_order_reship_exchange_unit_count) AS product_order_reship_exchange_unit_count,
    SUM(IFF(store_type = 'online', product_order_reship_exchange_unit_count, 0)) AS online_product_order_reship_exchange_unit_count,
    SUM(IFF(store_type = 'retail', product_order_reship_exchange_unit_count, 0)) AS retail_product_order_reship_exchange_unit_count,
    SUM(IFF(store_type = 'mobile app', product_order_reship_exchange_unit_count, 0)) AS mobile_app_product_order_reship_exchange_unit_count,
    SUM(product_order_reship_exchange_direct_cogs_amount) AS product_order_reship_exchange_direct_cogs_amount,
    SUM(IFF(store_type = 'online', product_order_reship_exchange_direct_cogs_amount, 0)) AS online_product_order_reship_exchange_direct_cogs_amount,
    SUM(IFF(store_type = 'retail', product_order_reship_exchange_direct_cogs_amount, 0)) AS retail_product_order_reship_exchange_direct_cogs_amount,
    SUM(IFF(store_type = 'mobile app', product_order_reship_exchange_direct_cogs_amount, 0)) AS mobile_app_product_order_reship_exchange_direct_cogs_amount,
    SUM(product_order_return_cogs_amount) AS product_order_return_cogs_amount,
    SUM(IFF(store_type = 'online', product_order_return_cogs_amount, 0)) AS online_product_order_return_cogs_amount,
    SUM(IFF(store_type = 'retail', product_order_return_cogs_amount, 0)) AS retail_product_order_return_cogs_amount,
    SUM(IFF(store_type = 'mobile app', product_order_return_cogs_amount, 0)) AS mobile_app_product_order_return_cogs_amount,
    SUM(product_order_return_unit_count) AS product_order_return_unit_count,
    SUM(IFF(store_type = 'online', product_order_return_unit_count, 0)) AS online_product_order_return_unit_count,
    SUM(IFF(store_type = 'retail', product_order_return_unit_count, 0)) AS retail_product_order_return_unit_count,
    SUM(IFF(store_type = 'mobile app', product_order_return_unit_count, 0)) AS mobile_app_product_order_return_unit_count,
    SUM(product_order_amount_to_pay) AS product_order_amount_to_pay,
    SUM(IFF(store_type = 'online', product_order_amount_to_pay, 0)) AS online_product_order_amount_to_pay,
    SUM(IFF(store_type = 'retail', product_order_amount_to_pay, 0)) AS retail_product_order_amount_to_pay,
    SUM(IFF(store_type = 'mobile app', product_order_amount_to_pay, 0)) AS mobile_app_product_order_amount_to_pay,
    SUM(product_gross_revenue_excl_shipping) AS product_gross_revenue_excl_shipping,
    SUM(IFF(store_type = 'online', product_gross_revenue_excl_shipping, 0)) AS online_product_gross_revenue_excl_shipping,
    SUM(IFF(store_type = 'retail', product_gross_revenue_excl_shipping, 0)) AS retail_product_gross_revenue_excl_shipping,
    SUM(IFF(store_type = 'mobile app', product_gross_revenue_excl_shipping, 0)) AS mobile_app_product_gross_revenue_excl_shipping,
    SUM(product_gross_revenue) AS product_gross_revenue,
    SUM(IFF(store_type = 'online', product_gross_revenue, 0)) AS online_product_gross_revenue,
    SUM(IFF(store_type = 'retail', product_gross_revenue, 0)) AS retail_product_gross_revenue,
    SUM(IFF(store_type = 'mobile app', product_gross_revenue, 0)) AS mobile_app_product_gross_revenue,
    SUM(product_net_revenue) AS product_net_revenue,
    SUM(IFF(store_type = 'online', product_net_revenue, 0)) AS online_product_net_revenue,
    SUM(IFF(store_type = 'retail', product_net_revenue, 0)) AS retail_product_net_revenue,
    SUM(IFF(store_type = 'mobile app', product_net_revenue, 0)) AS mobile_app_product_net_revenue,
    SUM(product_margin_pre_return) AS product_margin_pre_return,
    SUM(IFF(store_type = 'online', product_margin_pre_return, 0)) AS online_product_margin_pre_return,
    SUM(IFF(store_type = 'retail', product_margin_pre_return, 0)) AS retail_product_margin_pre_return,
    SUM(IFF(store_type = 'mobile app', product_margin_pre_return, 0)) AS mobile_app_product_margin_pre_return,
    SUM(product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping,
    SUM(IFF(store_type = 'online', product_margin_pre_return_excl_shipping, 0)) AS online_product_margin_pre_return_excl_shipping,
    SUM(IFF(store_type = 'retail', product_margin_pre_return_excl_shipping, 0)) AS retail_product_margin_pre_return_excl_shipping,
    SUM(IFF(store_type = 'mobile app', product_margin_pre_return_excl_shipping, 0)) AS mobile_app_product_margin_pre_return_excl_shipping,
    SUM(product_gross_profit) AS product_gross_profit,
    SUM(IFF(store_type = 'online', product_gross_profit, 0)) AS online_product_gross_profit,
    SUM(IFF(store_type = 'retail', product_gross_profit, 0)) AS retail_product_gross_profit,
    SUM(IFF(store_type = 'mobile app', product_gross_profit, 0)) AS mobile_app_product_gross_profit,
    SUM(product_variable_contribution_profit) AS product_variable_contribution_profit,
    SUM(IFF(store_type = 'online', product_variable_contribution_profit, 0)) AS online_product_variable_contribution_profit,
    SUM(IFF(store_type = 'retail', product_variable_contribution_profit, 0)) AS retail_product_variable_contribution_profit,
    SUM(IFF(store_type = 'mobile app', product_variable_contribution_profit, 0)) AS mobile_app_product_variable_contribution_profit,
    SUM(product_order_cash_gross_revenue_amount) AS product_order_cash_gross_revenue_amount,
    SUM(IFF(store_type = 'online', product_order_cash_gross_revenue_amount, 0)) AS online_product_order_cash_gross_revenue_amount,
    SUM(IFF(store_type = 'retail', product_order_cash_gross_revenue_amount, 0)) AS retail_product_order_cash_gross_revenue_amount,
    SUM(IFF(store_type = 'mobile app', product_order_cash_gross_revenue_amount, 0)) AS mobile_app_product_order_cash_gross_revenue_amount,
    SUM(product_order_cash_net_revenue) AS product_order_cash_net_revenue,
    SUM(IFF(store_type = 'online', product_order_cash_net_revenue, 0)) AS online_product_order_cash_net_revenue,
    SUM(IFF(store_type = 'retail', product_order_cash_net_revenue, 0)) AS retail_product_order_cash_net_revenue,
    SUM(IFF(store_type = 'mobile app', product_order_cash_net_revenue, 0)) AS mobile_app_product_order_cash_net_revenue,
    SUM(product_order_landed_product_cost_amount) AS product_order_landed_product_cost_amount,
    SUM(IFF(store_type = 'online', product_order_landed_product_cost_amount, 0)) AS online_product_order_landed_product_cost_amount,
    SUM(IFF(store_type = 'retail', product_order_landed_product_cost_amount, 0)) AS retail_product_order_landed_product_cost_amount,
    SUM(IFF(store_type = 'mobile app', product_order_landed_product_cost_amount, 0)) AS mobile_app_product_order_landed_product_cost_amount,
    SUM(product_order_cash_margin_pre_return) AS product_order_cash_margin_pre_return,
    SUM(IFF(store_type = 'online', product_order_cash_margin_pre_return, 0)) AS online_product_order_cash_margin_pre_return,
    SUM(IFF(store_type = 'retail', product_order_cash_margin_pre_return, 0)) AS retail_product_order_cash_margin_pre_return,
    SUM(IFF(store_type = 'mobile app', product_order_cash_margin_pre_return, 0)) AS mobile_app_product_order_cash_margin_pre_return,
    SUM(product_order_cash_gross_profit) AS product_order_cash_gross_profit,
    SUM(IFF(store_type = 'online', product_order_cash_gross_profit, 0)) AS online_product_order_cash_gross_profit,
    SUM(IFF(store_type = 'retail', product_order_cash_gross_profit, 0)) AS retail_product_order_cash_gross_profit,
    SUM(IFF(store_type = 'mobile app', product_order_cash_gross_profit, 0)) AS mobile_app_product_order_cash_gross_profit,
    SUM(billing_cash_gross_revenue) AS billing_cash_gross_revenue,
    SUM(billing_cash_net_revenue) AS billing_cash_net_revenue,
    SUM(cash_gross_revenue) AS cash_gross_revenue,
    SUM(cash_net_revenue) AS cash_net_revenue,
    SUM(cash_gross_profit) AS cash_gross_profit,
    SUM(cash_variable_contribution_profit) AS cash_variable_contribution_profit,
    SUM(monthly_billed_credit_cash_transaction_amount) AS monthly_billed_credit_cash_transaction_amount,
    SUM(membership_fee_cash_transaction_amount) AS membership_fee_cash_transaction_amount,
    SUM(gift_card_transaction_amount) AS gift_card_transaction_amount,
    SUM(legacy_credit_cash_transaction_amount) AS legacy_credit_cash_transaction_amount,
    SUM(monthly_billed_credit_cash_refund_count) AS monthly_billed_credit_cash_refund_count,
    SUM(monthly_billed_credit_cash_refund_chargeback_amount) AS monthly_billed_credit_cash_refund_chargeback_amount,
    SUM(membership_fee_cash_refund_chargeback_amount) AS membership_fee_cash_refund_chargeback_amount,
    SUM(gift_card_cash_refund_chargeback_amount) AS gift_card_cash_refund_chargeback_amount,
    SUM(legacy_credit_cash_refund_chargeback_amount) AS legacy_credit_cash_refund_chargeback_amount,
    SUM(billed_cash_credit_issued_amount) AS billed_cash_credit_issued_amount,
    SUM(billed_cash_credit_redeemed_amount) AS billed_cash_credit_redeemed_amount,
    SUM(IFF(store_type = 'online', billed_cash_credit_redeemed_amount, 0)) AS online_billed_cash_credit_redeemed_amount,
    SUM(IFF(store_type = 'retail', billed_cash_credit_redeemed_amount, 0)) AS retail_billed_cash_credit_redeemed_amount,
    SUM(IFF(store_type = 'mobile app', billed_cash_credit_redeemed_amount, 0)) AS mobile_billed_cash_credit_redeemed_amount,
    SUM(billed_cash_credit_cancelled_amount) AS billed_cash_credit_cancelled_amount,
    SUM(billed_cash_credit_expired_amount) AS billed_cash_credit_expired_amount,
    SUM(billed_cash_credit_issued_equivalent_count) AS billed_cash_credit_issued_equivalent_count,
    SUM(billed_cash_credit_redeemed_same_month_amount) AS billed_cash_credit_redeemed_same_month_amount,
    SUM(billed_cash_credit_redeemed_equivalent_count) AS billed_cash_credit_redeemed_equivalent_count,
    SUM(billed_cash_credit_cancelled_equivalent_count) AS billed_cash_credit_cancelled_equivalent_count,
    SUM(billed_cash_credit_expired_equivalent_count) AS billed_cash_credit_expired_equivalent_count,
    SUM(refund_cash_credit_issued_amount) AS refund_cash_credit_issued_amount,
    SUM(refund_cash_credit_redeemed_amount) AS refund_cash_credit_redeemed_amount,
    SUM(refund_cash_credit_cancelled_amount) AS refund_cash_credit_cancelled_amount,
    SUM(refund_cash_credit_expired_amount) AS refund_cash_credit_expired_amount,
    SUM(other_cash_credit_issued_amount) AS other_cash_credit_issued_amount,
    SUM(other_cash_credit_redeemed_amount) AS other_cash_credit_redeemed_amount,
    SUM(other_cash_credit_cancelled_amount) AS other_cash_credit_cancelled_amount,
    SUM(other_cash_credit_expired_amount) AS other_cash_credit_expired_amount,
    SUM(noncash_credit_issued_amount) AS noncash_credit_issued_amount,
    SUM(noncash_credit_cancelled_amount) AS noncash_credit_cancelled_amount,
    SUM(noncash_credit_expired_amount) AS noncash_credit_expired_amount,
    SUM(first_guest_product_margin_pre_return) AS first_guest_product_margin_pre_return
FROM _customer_lifetime__ltv_store_type st
GROUP BY month_date, customer_id, activation_key, vip_store_id;
-- SELECT noncash_credit_expired_amount, * FROM _customer_lifetime__ltv WHERE customer_id IN (24249541, 28319266, 32605993, 37857325);

CREATE OR REPLACE TEMP TABLE _customer_lifetime__agg_data AS
SELECT
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    IFNULL(product_order_count, 0) AS product_order_count,
    IFNULL(online_product_order_count, 0) AS online_product_order_count,
    IFNULL(retail_product_order_count, 0) AS retail_product_order_count,
    IFNULL(mobile_app_product_order_count, 0) AS mobile_app_product_order_count,
    IFNULL(product_order_count_with_credit_redemption, 0) AS product_order_count_with_credit_redemption,
    IFNULL(product_order_unit_count, 0) AS product_order_unit_count,
    IFNULL(online_product_order_unit_count, 0) AS online_product_order_unit_count,
    IFNULL(retail_product_order_unit_count, 0) AS retail_product_order_unit_count,
    IFNULL(mobile_app_product_order_unit_count, 0) AS mobile_app_product_order_unit_count,
    IFNULL(product_order_subtotal_excl_tariff_amount, 0) AS product_order_subtotal_excl_tariff_amount,
    IFNULL(product_order_product_subtotal_amount, 0) AS product_order_product_subtotal_amount,
    IFNULL(online_product_order_subtotal_excl_tariff_amount, 0) AS online_product_order_subtotal_excl_tariff_amount,
    IFNULL(retail_product_order_subtotal_excl_tariff_amount, 0) AS retail_product_order_subtotal_excl_tariff_amount,
    IFNULL(mobile_app_product_order_subtotal_excl_tariff_amount, 0) AS mobile_app_product_order_subtotal_excl_tariff_amount,
    IFNULL(product_order_product_discount_amount, 0) AS product_order_product_discount_amount,
    IFNULL(online_product_order_product_discount_amount, 0) AS online_product_order_product_discount_amount,
    IFNULL(retail_product_order_product_discount_amount, 0) AS retail_product_order_product_discount_amount,
    IFNULL(mobile_app_product_order_product_discount_amount, 0) AS mobile_app_product_order_product_discount_amount,
    IFNULL(product_order_shipping_revenue_amount, 0) AS product_order_shipping_revenue_amount,
    IFNULL(online_product_order_shipping_revenue_amount, 0) AS online_product_order_shipping_revenue_amount,
    IFNULL(retail_product_order_shipping_revenue_amount, 0) AS retail_product_order_shipping_revenue_amount,
    IFNULL(mobile_app_product_order_shipping_revenue_amount, 0) AS mobile_app_product_order_shipping_revenue_amount,
    IFNULL(product_order_direct_cogs_amount, 0) AS product_order_direct_cogs_amount,
    IFNULL(online_product_order_direct_cogs_amount, 0) AS online_product_order_direct_cogs_amount,
    IFNULL(retail_product_order_direct_cogs_amount, 0) AS retail_product_order_direct_cogs_amount,
    IFNULL(mobile_app_product_order_direct_cogs_amount, 0) AS mobile_app_product_order_direct_cogs_amount,
    IFNULL(product_order_selling_expenses_amount, 0) AS product_order_selling_expenses_amount,
    IFNULL(online_product_order_selling_expenses_amount, 0) AS online_product_order_selling_expenses_amount,
    IFNULL(retail_product_order_selling_expenses_amount, 0) AS retail_product_order_selling_expenses_amount,
    IFNULL(mobile_app_product_order_selling_expenses_amount, 0) AS mobile_app_product_order_selling_expenses_amount,
    IFNULL(product_order_cash_refund_amount_and_chargeback_amount, 0) AS product_order_cash_refund_amount_and_chargeback_amount,
    IFNULL(online_product_order_cash_refund_chargeback_amount, 0) AS online_product_order_cash_refund_chargeback_amount,
    IFNULL(retail_product_order_cash_refund_chargeback_amount, 0) AS retail_product_order_cash_refund_chargeback_amount,
    IFNULL(mobile_app_product_order_cash_refund_chargeback_amount, 0) AS mobile_app_product_order_cash_refund_chargeback_amount,
    IFNULL(product_order_cash_credit_refund_amount, 0) AS product_order_cash_credit_refund_amount,
    IFNULL(online_product_order_cash_credit_refund_amount, 0) AS online_product_order_cash_credit_refund_amount,
    IFNULL(retail_product_order_cash_credit_refund_amount, 0) AS retail_product_order_cash_credit_refund_amount,
    IFNULL(mobile_app_product_order_cash_credit_refund_amount, 0) AS mobile_app_product_order_cash_credit_refund_amount,
    IFNULL(product_order_reship_exchange_order_count, 0) AS product_order_reship_exchange_order_count,
    IFNULL(online_product_order_reship_exchange_order_count, 0) AS online_product_order_reship_exchange_order_count,
    IFNULL(retail_product_order_reship_exchange_order_count, 0) AS retail_product_order_reship_exchange_order_count,
    IFNULL(mobile_app_product_order_reship_exchange_order_count, 0) AS mobile_app_product_order_reship_exchange_order_count,
    IFNULL(product_order_reship_exchange_unit_count, 0) AS product_order_reship_exchange_unit_count,
    IFNULL(online_product_order_reship_exchange_unit_count, 0) AS online_product_order_reship_exchange_unit_count,
    IFNULL(retail_product_order_reship_exchange_unit_count, 0) AS retail_product_order_reship_exchange_unit_count,
    IFNULL(mobile_app_product_order_reship_exchange_unit_count, 0) AS mobile_app_product_order_reship_exchange_unit_count,
    IFNULL(product_order_reship_exchange_direct_cogs_amount, 0) AS product_order_reship_exchange_direct_cogs_amount,
    IFNULL(online_product_order_reship_exchange_direct_cogs_amount, 0) AS online_product_order_reship_exchange_direct_cogs_amount,
    IFNULL(retail_product_order_reship_exchange_direct_cogs_amount, 0) AS retail_product_order_reship_exchange_direct_cogs_amount,
    IFNULL(mobile_app_product_order_reship_exchange_direct_cogs_amount, 0) AS mobile_app_product_order_reship_exchange_direct_cogs_amount,
    IFNULL(product_order_return_cogs_amount, 0) AS product_order_return_cogs_amount,
    IFNULL(online_product_order_return_cogs_amount, 0) AS online_product_order_return_cogs_amount,
    IFNULL(retail_product_order_return_cogs_amount, 0) AS retail_product_order_return_cogs_amount,
    IFNULL(mobile_app_product_order_return_cogs_amount, 0) AS mobile_app_product_order_return_cogs_amount,
    IFNULL(product_order_return_unit_count, 0) AS product_order_return_unit_count,
    IFNULL(online_product_order_return_unit_count, 0) AS online_product_order_return_unit_count,
    IFNULL(retail_product_order_return_unit_count, 0) AS retail_product_order_return_unit_count,
    IFNULL(mobile_app_product_order_return_unit_count, 0) AS mobile_app_product_order_return_unit_count,
    IFNULL(product_order_amount_to_pay, 0) AS product_order_amount_to_pay,
    IFNULL(online_product_order_amount_to_pay, 0) AS online_product_order_amount_to_pay,
    IFNULL(retail_product_order_amount_to_pay, 0) AS retail_product_order_amount_to_pay,
    IFNULL(mobile_app_product_order_amount_to_pay, 0) AS mobile_app_product_order_amount_to_pay,
    IFNULL(product_gross_revenue_excl_shipping, 0) AS product_gross_revenue_excl_shipping,
    IFNULL(online_product_gross_revenue_excl_shipping, 0) AS online_product_gross_revenue_excl_shipping,
    IFNULL(retail_product_gross_revenue_excl_shipping, 0) AS retail_product_gross_revenue_excl_shipping,
    IFNULL(mobile_app_product_gross_revenue_excl_shipping, 0) AS mobile_app_product_gross_revenue_excl_shipping,
    IFNULL(product_gross_revenue, 0) AS product_gross_revenue,
    IFNULL(online_product_gross_revenue, 0) AS online_product_gross_revenue,
    IFNULL(retail_product_gross_revenue, 0) AS retail_product_gross_revenue,
    IFNULL(mobile_app_product_gross_revenue, 0) AS mobile_app_product_gross_revenue,
    IFNULL(product_net_revenue, 0) AS product_net_revenue,
    IFNULL(online_product_net_revenue, 0) AS online_product_net_revenue,
    IFNULL(retail_product_net_revenue, 0) AS retail_product_net_revenue,
    IFNULL(mobile_app_product_net_revenue, 0) AS mobile_app_product_net_revenue,
    IFNULL(product_margin_pre_return, 0) AS product_margin_pre_return,
    IFNULL(online_product_margin_pre_return, 0) AS online_product_margin_pre_return,
    IFNULL(retail_product_margin_pre_return, 0) AS retail_product_margin_pre_return,
    IFNULL(mobile_app_product_margin_pre_return, 0) AS mobile_app_product_margin_pre_return,
    IFNULL(product_margin_pre_return_excl_shipping, 0) AS product_margin_pre_return_excl_shipping,
    IFNULL(online_product_margin_pre_return_excl_shipping, 0) AS online_product_margin_pre_return_excl_shipping,
    IFNULL(retail_product_margin_pre_return_excl_shipping, 0) AS retail_product_margin_pre_return_excl_shipping,
    IFNULL(mobile_app_product_margin_pre_return_excl_shipping, 0) AS mobile_app_product_margin_pre_return_excl_shipping,
    IFNULL(product_gross_profit, 0) AS product_gross_profit,
    IFNULL(online_product_gross_profit, 0) AS online_product_gross_profit,
    IFNULL(retail_product_gross_profit, 0) AS retail_product_gross_profit,
    IFNULL(mobile_app_product_gross_profit, 0) AS mobile_app_product_gross_profit,
    IFNULL(product_variable_contribution_profit, 0) AS product_variable_contribution_profit,
    IFNULL(online_product_variable_contribution_profit, 0) AS online_product_variable_contribution_profit,
    IFNULL(retail_product_variable_contribution_profit, 0) AS retail_product_variable_contribution_profit,
    IFNULL(mobile_app_product_variable_contribution_profit, 0) AS mobile_app_product_variable_contribution_profit,
    IFNULL(product_order_cash_gross_revenue_amount, 0) AS product_order_cash_gross_revenue_amount,
    IFNULL(online_product_order_cash_gross_revenue_amount, 0) AS online_product_order_cash_gross_revenue_amount,
    IFNULL(retail_product_order_cash_gross_revenue_amount, 0) AS retail_product_order_cash_gross_revenue_amount,
    IFNULL(mobile_app_product_order_cash_gross_revenue_amount, 0) AS mobile_app_product_order_cash_gross_revenue_amount,
    IFNULL(product_order_cash_net_revenue, 0) AS product_order_cash_net_revenue,
    IFNULL(online_product_order_cash_net_revenue, 0) AS online_product_order_cash_net_revenue,
    IFNULL(retail_product_order_cash_net_revenue, 0) AS retail_product_order_cash_net_revenue,
    IFNULL(mobile_app_product_order_cash_net_revenue, 0) AS mobile_app_product_order_cash_net_revenue,
    IFNULL(product_order_landed_product_cost_amount, 0) AS product_order_landed_product_cost_amount,
    IFNULL(online_product_order_landed_product_cost_amount, 0) AS online_product_order_landed_product_cost_amount,
    IFNULL(retail_product_order_landed_product_cost_amount, 0) AS retail_product_order_landed_product_cost_amount,
    IFNULL(mobile_app_product_order_landed_product_cost_amount, 0) AS mobile_app_product_order_landed_product_cost_amount,
    IFNULL(product_order_cash_margin_pre_return, 0) AS product_order_cash_margin_pre_return,
    IFNULL(online_product_order_cash_margin_pre_return, 0) AS online_product_order_cash_margin_pre_return,
    IFNULL(retail_product_order_cash_margin_pre_return, 0) AS retail_product_order_cash_margin_pre_return,
    IFNULL(mobile_app_product_order_cash_margin_pre_return, 0) AS mobile_app_product_order_cash_margin_pre_return,
    IFNULL(product_order_cash_gross_profit, 0) AS product_order_cash_gross_profit,
    IFNULL(online_product_order_cash_gross_profit, 0) AS online_product_order_cash_gross_profit,
    IFNULL(retail_product_order_cash_gross_profit, 0) AS retail_product_order_cash_gross_profit,
    IFNULL(mobile_app_product_order_cash_gross_profit, 0) AS mobile_app_product_order_cash_gross_profit,
    IFNULL(billing_cash_gross_revenue, 0) AS billing_cash_gross_revenue,
    IFNULL(billing_cash_net_revenue, 0) AS billing_cash_net_revenue,
    IFNULL(cash_gross_revenue, 0) AS cash_gross_revenue,
    IFNULL(cash_net_revenue, 0) AS cash_net_revenue,
    IFNULL(cash_gross_profit, 0) AS cash_gross_profit,
    IFNULL(cash_variable_contribution_profit, 0) AS cash_variable_contribution_profit,
    IFNULL(monthly_billed_credit_cash_transaction_amount, 0) AS monthly_billed_credit_cash_transaction_amount,
    IFNULL(membership_fee_cash_transaction_amount, 0) AS membership_fee_cash_transaction_amount,
    IFNULL(gift_card_transaction_amount, 0) AS gift_card_transaction_amount,
    IFNULL(legacy_credit_cash_transaction_amount, 0) AS legacy_credit_cash_transaction_amount,
    IFNULL(monthly_billed_credit_cash_refund_count, 0) AS monthly_billed_credit_cash_refund_count,
    IFNULL(monthly_billed_credit_cash_refund_chargeback_amount, 0) AS monthly_billed_credit_cash_refund_chargeback_amount,
    IFNULL(membership_fee_cash_refund_chargeback_amount, 0) AS membership_fee_cash_refund_chargeback_amount,
    IFNULL(gift_card_cash_refund_chargeback_amount, 0) AS gift_card_cash_refund_chargeback_amount,
    IFNULL(legacy_credit_cash_refund_chargeback_amount, 0) AS legacy_credit_cash_refund_chargeback_amount,
    IFNULL(billed_cash_credit_issued_amount, 0) AS billed_cash_credit_issued_amount,
    IFNULL(billed_cash_credit_redeemed_amount, 0) AS billed_cash_credit_redeemed_amount,
    IFNULL(online_billed_cash_credit_redeemed_amount, 0) AS online_billed_cash_credit_redeemed_amount,
    IFNULL(retail_billed_cash_credit_redeemed_amount, 0) AS retail_billed_cash_credit_redeemed_amount,
    IFNULL(mobile_billed_cash_credit_redeemed_amount, 0) AS mobile_billed_cash_credit_redeemed_amount,
    IFNULL(billed_cash_credit_cancelled_amount, 0) AS billed_cash_credit_cancelled_amount,
    IFNULL(billed_cash_credit_expired_amount, 0) AS billed_cash_credit_expired_amount,
    IFNULL(billed_cash_credit_issued_equivalent_count, 0) AS billed_cash_credit_issued_equivalent_count,
    IFNULL(billed_cash_credit_redeemed_same_month_amount, 0) AS billed_cash_credit_redeemed_same_month_amount,
    IFNULL(billed_cash_credit_redeemed_equivalent_count, 0) AS billed_cash_credit_redeemed_equivalent_count,
    IFNULL(billed_cash_credit_cancelled_equivalent_count, 0) AS billed_cash_credit_cancelled_equivalent_count,
    IFNULL(billed_cash_credit_expired_equivalent_count, 0) AS billed_cash_credit_expired_equivalent_count,
    IFNULL(refund_cash_credit_issued_amount, 0) AS refund_cash_credit_issued_amount,
    IFNULL(refund_cash_credit_redeemed_amount, 0) AS refund_cash_credit_redeemed_amount,
    IFNULL(refund_cash_credit_cancelled_amount, 0) AS refund_cash_credit_cancelled_amount,
    IFNULL(refund_cash_credit_expired_amount, 0) AS refund_cash_credit_expired_amount,
    IFNULL(other_cash_credit_issued_amount, 0) AS other_cash_credit_issued_amount,
    IFNULL(other_cash_credit_redeemed_amount, 0) AS other_cash_credit_redeemed_amount,
    IFNULL(other_cash_credit_cancelled_amount, 0) AS other_cash_credit_cancelled_amount,
    IFNULL(other_cash_credit_expired_amount, 0) AS other_cash_credit_expired_amount,
    IFNULL(noncash_credit_issued_amount, 0) AS noncash_credit_issued_amount,
    IFNULL(noncash_credit_cancelled_amount, 0) AS noncash_credit_cancelled_amount,
    IFNULL(noncash_credit_expired_amount, 0) AS noncash_credit_expired_amount,
    IFNULL(first_guest_product_margin_pre_return, 0) AS first_guest_product_margin_pre_return
FROM _customer_lifetime__ltv;
-- SELECT * FROM _customer_lifetime__agg_data;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__monthly_agg_stg AS
SELECT
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    product_order_count,
    online_product_order_count,
    retail_product_order_count,
    mobile_app_product_order_count,
    product_order_count_with_credit_redemption,
    product_order_unit_count,
    online_product_order_unit_count,
    retail_product_order_unit_count,
    mobile_app_product_order_unit_count,
    product_order_subtotal_excl_tariff_amount,
    product_order_product_subtotal_amount,
    online_product_order_subtotal_excl_tariff_amount,
    retail_product_order_subtotal_excl_tariff_amount,
    mobile_app_product_order_subtotal_excl_tariff_amount,
    product_order_product_discount_amount,
    online_product_order_product_discount_amount,
    retail_product_order_product_discount_amount,
    mobile_app_product_order_product_discount_amount,
    product_order_shipping_revenue_amount,
    online_product_order_shipping_revenue_amount,
    retail_product_order_shipping_revenue_amount,
    mobile_app_product_order_shipping_revenue_amount,
    product_order_direct_cogs_amount,
    online_product_order_direct_cogs_amount,
    retail_product_order_direct_cogs_amount,
    mobile_app_product_order_direct_cogs_amount,
    product_order_selling_expenses_amount,
    online_product_order_selling_expenses_amount,
    retail_product_order_selling_expenses_amount,
    mobile_app_product_order_selling_expenses_amount,
    product_order_cash_refund_amount_and_chargeback_amount,
    online_product_order_cash_refund_chargeback_amount,
    retail_product_order_cash_refund_chargeback_amount,
    mobile_app_product_order_cash_refund_chargeback_amount,
    product_order_cash_credit_refund_amount,
    online_product_order_cash_credit_refund_amount,
    retail_product_order_cash_credit_refund_amount,
    mobile_app_product_order_cash_credit_refund_amount,
    product_order_reship_exchange_order_count,
    online_product_order_reship_exchange_order_count,
    retail_product_order_reship_exchange_order_count,
    mobile_app_product_order_reship_exchange_order_count,
    product_order_reship_exchange_unit_count,
    online_product_order_reship_exchange_unit_count,
    retail_product_order_reship_exchange_unit_count,
    mobile_app_product_order_reship_exchange_unit_count,
    product_order_reship_exchange_direct_cogs_amount,
    online_product_order_reship_exchange_direct_cogs_amount,
    retail_product_order_reship_exchange_direct_cogs_amount,
    mobile_app_product_order_reship_exchange_direct_cogs_amount,
    product_order_return_cogs_amount,
    online_product_order_return_cogs_amount,
    retail_product_order_return_cogs_amount,
    mobile_app_product_order_return_cogs_amount,
    product_order_return_unit_count,
    online_product_order_return_unit_count,
    retail_product_order_return_unit_count,
    mobile_app_product_order_return_unit_count,
    product_order_amount_to_pay,
    online_product_order_amount_to_pay,
    retail_product_order_amount_to_pay,
    mobile_app_product_order_amount_to_pay,
    product_gross_revenue_excl_shipping,
    online_product_gross_revenue_excl_shipping,
    retail_product_gross_revenue_excl_shipping,
    mobile_app_product_gross_revenue_excl_shipping,
    product_gross_revenue,
    online_product_gross_revenue,
    retail_product_gross_revenue,
    mobile_app_product_gross_revenue,
    product_net_revenue,
    online_product_net_revenue,
    retail_product_net_revenue,
    mobile_app_product_net_revenue,
    product_margin_pre_return,
    online_product_margin_pre_return,
    retail_product_margin_pre_return,
    mobile_app_product_margin_pre_return,
    product_margin_pre_return_excl_shipping,
    online_product_margin_pre_return_excl_shipping,
    retail_product_margin_pre_return_excl_shipping,
    mobile_app_product_margin_pre_return_excl_shipping,
    product_gross_profit,
    online_product_gross_profit,
    retail_product_gross_profit,
    mobile_app_product_gross_profit,
    product_variable_contribution_profit,
    online_product_variable_contribution_profit,
    retail_product_variable_contribution_profit,
    mobile_app_product_variable_contribution_profit,
    product_order_cash_gross_revenue_amount,
    online_product_order_cash_gross_revenue_amount,
    retail_product_order_cash_gross_revenue_amount,
    mobile_app_product_order_cash_gross_revenue_amount,
    product_order_cash_net_revenue,
    online_product_order_cash_net_revenue,
    retail_product_order_cash_net_revenue,
    mobile_app_product_order_cash_net_revenue,
    product_order_landed_product_cost_amount,
    online_product_order_landed_product_cost_amount,
    retail_product_order_landed_product_cost_amount,
    mobile_app_product_order_landed_product_cost_amount,
    product_order_cash_margin_pre_return,
    online_product_order_cash_margin_pre_return,
    retail_product_order_cash_margin_pre_return,
    mobile_app_product_order_cash_margin_pre_return,
    product_order_cash_gross_profit,
    online_product_order_cash_gross_profit,
    retail_product_order_cash_gross_profit,
    mobile_app_product_order_cash_gross_profit,
    billing_cash_gross_revenue,
    billing_cash_net_revenue,
    cash_gross_revenue,
    cash_net_revenue,
    cash_gross_profit,
    cash_variable_contribution_profit,
    monthly_billed_credit_cash_transaction_amount,
    membership_fee_cash_transaction_amount,
    gift_card_transaction_amount,
    legacy_credit_cash_transaction_amount,
    monthly_billed_credit_cash_refund_count,
    monthly_billed_credit_cash_refund_chargeback_amount,
    membership_fee_cash_refund_chargeback_amount,
    gift_card_cash_refund_chargeback_amount,
    legacy_credit_cash_refund_chargeback_amount,
    billed_cash_credit_issued_amount,
    billed_cash_credit_redeemed_amount,
    online_billed_cash_credit_redeemed_amount,
    retail_billed_cash_credit_redeemed_amount,
    mobile_billed_cash_credit_redeemed_amount,
    billed_cash_credit_cancelled_amount,
    billed_cash_credit_expired_amount,
    billed_cash_credit_issued_equivalent_count,
    billed_cash_credit_redeemed_same_month_amount,
    billed_cash_credit_redeemed_equivalent_count,
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
    first_guest_product_margin_pre_return,
    HASH (
        month_date,
        customer_id,
        activation_key,
        vip_store_id,
        product_order_count,
        online_product_order_count,
        retail_product_order_count,
        mobile_app_product_order_count,
        product_order_count_with_credit_redemption,
        product_order_unit_count,
        online_product_order_unit_count,
        retail_product_order_unit_count,
        mobile_app_product_order_unit_count,
        product_order_subtotal_excl_tariff_amount,
        product_order_product_subtotal_amount,
        online_product_order_subtotal_excl_tariff_amount,
        retail_product_order_subtotal_excl_tariff_amount,
        mobile_app_product_order_subtotal_excl_tariff_amount,
        product_order_product_discount_amount,
        online_product_order_product_discount_amount,
        retail_product_order_product_discount_amount,
        mobile_app_product_order_product_discount_amount,
        product_order_shipping_revenue_amount,
        online_product_order_shipping_revenue_amount,
        retail_product_order_shipping_revenue_amount,
        mobile_app_product_order_shipping_revenue_amount,
        product_order_direct_cogs_amount,
        online_product_order_direct_cogs_amount,
        retail_product_order_direct_cogs_amount,
        mobile_app_product_order_direct_cogs_amount,
        product_order_selling_expenses_amount,
        online_product_order_selling_expenses_amount,
        retail_product_order_selling_expenses_amount,
        mobile_app_product_order_selling_expenses_amount,
        product_order_cash_refund_amount_and_chargeback_amount,
        online_product_order_cash_refund_chargeback_amount,
        retail_product_order_cash_refund_chargeback_amount,
        mobile_app_product_order_cash_refund_chargeback_amount,
        product_order_cash_credit_refund_amount,
        online_product_order_cash_credit_refund_amount,
        retail_product_order_cash_credit_refund_amount,
        mobile_app_product_order_cash_credit_refund_amount,
        product_order_reship_exchange_order_count,
        online_product_order_reship_exchange_order_count,
        retail_product_order_reship_exchange_order_count,
        mobile_app_product_order_reship_exchange_order_count,
        product_order_reship_exchange_unit_count,
        online_product_order_reship_exchange_unit_count,
        retail_product_order_reship_exchange_unit_count,
        mobile_app_product_order_reship_exchange_unit_count,
        product_order_reship_exchange_direct_cogs_amount,
        online_product_order_reship_exchange_direct_cogs_amount,
        retail_product_order_reship_exchange_direct_cogs_amount,
        mobile_app_product_order_reship_exchange_direct_cogs_amount,
        product_order_return_cogs_amount,
        online_product_order_return_cogs_amount,
        retail_product_order_return_cogs_amount,
        mobile_app_product_order_return_cogs_amount,
        product_order_return_unit_count,
        online_product_order_return_unit_count,
        retail_product_order_return_unit_count,
        mobile_app_product_order_return_unit_count,
        product_order_amount_to_pay,
        online_product_order_amount_to_pay,
        retail_product_order_amount_to_pay,
        mobile_app_product_order_amount_to_pay,
        product_gross_revenue_excl_shipping,
        online_product_gross_revenue_excl_shipping,
        retail_product_gross_revenue_excl_shipping,
        mobile_app_product_gross_revenue_excl_shipping,
        product_gross_revenue,
        online_product_gross_revenue,
        retail_product_gross_revenue,
        mobile_app_product_gross_revenue,
        product_net_revenue,
        online_product_net_revenue,
        retail_product_net_revenue,
        mobile_app_product_net_revenue,
        product_margin_pre_return,
        online_product_margin_pre_return,
        retail_product_margin_pre_return,
        mobile_app_product_margin_pre_return,
        product_margin_pre_return_excl_shipping,
        online_product_margin_pre_return_excl_shipping,
        retail_product_margin_pre_return_excl_shipping,
        mobile_app_product_margin_pre_return_excl_shipping,
        product_gross_profit,
        online_product_gross_profit,
        retail_product_gross_profit,
        mobile_app_product_gross_profit,
        product_variable_contribution_profit,
        online_product_variable_contribution_profit,
        retail_product_variable_contribution_profit,
        mobile_app_product_variable_contribution_profit,
        product_order_cash_gross_revenue_amount,
        online_product_order_cash_gross_revenue_amount,
        retail_product_order_cash_gross_revenue_amount,
        mobile_app_product_order_cash_gross_revenue_amount,
        product_order_cash_net_revenue,
        online_product_order_cash_net_revenue,
        retail_product_order_cash_net_revenue,
        mobile_app_product_order_cash_net_revenue,
        product_order_landed_product_cost_amount,
        online_product_order_landed_product_cost_amount,
        retail_product_order_landed_product_cost_amount,
        mobile_app_product_order_landed_product_cost_amount,
        product_order_cash_margin_pre_return,
        online_product_order_cash_margin_pre_return,
        retail_product_order_cash_margin_pre_return,
        mobile_app_product_order_cash_margin_pre_return,
        product_order_cash_gross_profit,
        online_product_order_cash_gross_profit,
        retail_product_order_cash_gross_profit,
        mobile_app_product_order_cash_gross_profit,
        billing_cash_gross_revenue,
        billing_cash_net_revenue,
        cash_gross_revenue,
        cash_net_revenue,
        cash_gross_profit,
        cash_variable_contribution_profit,
        monthly_billed_credit_cash_transaction_amount,
        membership_fee_cash_transaction_amount,
        gift_card_transaction_amount,
        legacy_credit_cash_transaction_amount,
        monthly_billed_credit_cash_refund_count,
        monthly_billed_credit_cash_refund_chargeback_amount,
        membership_fee_cash_refund_chargeback_amount,
        gift_card_cash_refund_chargeback_amount,
        legacy_credit_cash_refund_chargeback_amount,
        billed_cash_credit_issued_amount,
        billed_cash_credit_redeemed_amount,
        online_billed_cash_credit_redeemed_amount,
        retail_billed_cash_credit_redeemed_amount,
        mobile_billed_cash_credit_redeemed_amount,
        billed_cash_credit_cancelled_amount,
        billed_cash_credit_expired_amount,
        billed_cash_credit_issued_equivalent_count,
        billed_cash_credit_redeemed_same_month_amount,
        billed_cash_credit_redeemed_equivalent_count,
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
        first_guest_product_margin_pre_return
        ) AS meta_row_hash,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _customer_lifetime__agg_data;
-- SELECT * FROM _customer_lifetime__monthly_agg_stg;


CREATE OR REPLACE TEMP TABLE _customer_lifetime__cust_data AS
SELECT
    cm.month_date,
    cm.customer_id,
    cm.activation_key,
    cm.first_activation_key,
    COALESCE(cm.gender, 'Unknown') AS gender,
    COALESCE(cm.store_id, -1) AS store_id,
    COALESCE(cm.vip_store_id, -1) AS vip_store_id,
    COALESCE(cm.finance_specialty_store, 'Unknown') AS finance_specialty_store,
    COALESCE(cm.guest_cohort_month_date, '1900-01-01') AS guest_cohort_month_date,
    COALESCE(cm.vip_cohort_month_date, '1900-01-01') AS vip_cohort_month_date,
    COALESCE(cm.membership_type, 'Unknown') AS membership_type,
    COALESCE(cm.membership_type_entering_month, 'Unknown') AS membership_type_entering_month,
    COALESCE(cm.is_bop_vip, FALSE) AS is_bop_vip,
    COALESCE(cm.is_reactivated_vip, FALSE) AS is_reactivated_vip,
    COALESCE(cm.is_cancel, FALSE) AS is_cancel,
    COALESCE(cm.is_cancel_before_6th, FALSE) AS is_cancel_before_6th,
    COALESCE(cm.is_passive_cancel, FALSE) AS is_passive_cancel,
    COALESCE(csk.is_skip, FALSE) AS is_skip,
    COALESCE(csz.is_snooze, FALSE) AS is_snooze,
    COALESCE(clg.is_login, FALSE) AS is_login,
    COALESCE(cls.segment_activity, FALSE) AS segment_activity,
    IFF(COALESCE(ltv.product_order_count, 0) > 0, TRUE, FALSE) AS is_merch_purchaser,
    COALESCE(cb.is_successful_billing, FALSE) AS is_successful_billing,
    COALESCE(cb.is_pending_billing, FALSE) AS is_pending_billing,
    COALESCE(cb.is_failed_billing, FALSE) AS is_failed_billing,
    COALESCE(cm.is_cross_promo, FALSE) AS is_cross_promo,
    COALESCE(cm.is_retail_vip, FALSE) AS is_retail_vip,
    COALESCE(cm.is_scrubs_customer, FALSE) AS is_scrubs_customer,
    COALESCE(cbuc.entering_month_unredeemed_credits_per_customer, 0) AS entering_month_unredeemed_credits_per_customer
FROM _customer_lifetime__customer_month AS cm
    LEFT JOIN _customer_lifetime__customer_skip AS csk
        ON csk.month_date = cm.month_date
        AND csk.customer_id = cm.customer_id
        AND csk.activation_key = cm.activation_key
    LEFT JOIN _customer_lifetime__customer_snooze AS csz
        ON csz.month_date = cm.month_date
        AND csz.customer_id = cm.customer_id
        AND csz.activation_key = cm.activation_key
    LEFT JOIN _customer_lifetime__customer_login AS clg
        ON clg.month_date = cm.month_date
        AND clg.customer_id = cm.customer_id
        AND clg.activation_key = cm.activation_key
    LEFT JOIN _customer_lifetime__customer_segment_activity AS cls
        ON cls.month_date = cm.month_date
        AND cls.customer_id = cm.customer_id
        AND cls.activation_key = cm.activation_key
    LEFT JOIN _customer_lifetime__customer_billing AS cb
        ON cb.month_date = cm.month_date
        AND cb.customer_id = cm.customer_id
        AND cb.activation_key = cm.activation_key
    LEFT JOIN _customer_lifetime__customer_bop_unredeemed_credits AS cbuc
        ON cbuc.month_date = cm.month_date
        AND cbuc.customer_id = cm.customer_id
        AND cbuc.activation_key = cm.activation_key
    LEFT JOIN _customer_lifetime__ltv AS ltv
        ON ltv.month_date = cm.month_date
        AND ltv.customer_id = cm.customer_id
        AND ltv.activation_key = cm.activation_key
        AND ltv.vip_store_id = cm.vip_store_id;
-- SELECT * FROM _customer_lifetime__cust_data;

/* Calculate cumulatives */
CREATE OR REPLACE TEMP TABLE _customer_lifetime__cumulative AS
SELECT
    cd.month_date,
    cd.customer_id,
    cd.activation_key,
    cd.store_id,
    cd.vip_store_id,
    cd.guest_cohort_month_date,
    cd.vip_cohort_month_date,
    SUM(COALESCE(ma.cash_gross_profit, 0)) OVER (PARTITION BY cd.customer_id, cd.activation_key, cd.vip_store_id ORDER BY cd.month_date ASC) AS cumulative_cash_gross_profit,
    SUM(COALESCE(ma.product_gross_profit, 0)) OVER (PARTITION BY cd.customer_id, cd.activation_key, cd.vip_store_id ORDER BY cd.month_date ASC) AS cumulative_product_gross_profit
FROM _customer_lifetime__cust_data AS cd
    LEFT JOIN _customer_lifetime__monthly_agg_stg AS ma
        ON ma.month_date = cd.month_date
        AND ma.customer_id = cd.customer_id
        AND ma.activation_key = cd.activation_key
        AND ma.vip_store_id = cd.vip_store_id;
-- SELECT * FROM _customer_lifetime__cumulative;

SET process_start = (SELECT MIN(month_date) AS min_month_date FROM _customer_lifetime__customer_month);
-- SELECT $process_start;

/* Grab customers' cumulative profit the month before the refresh date so cumulative profit reflects since beginning of time */
CREATE OR REPLACE TEMP TABLE _customer_lifetime__cumulative_last_month AS
SELECT
    clvmc.customer_id,
    clvmc.activation_key,
    clvmc.vip_store_id,
    clvmc.cumulative_product_gross_profit AS last_month_cumulative_product_gross_profit,
    clvmc.cumulative_cash_gross_profit AS last_month_cumulative_cash_gross_profit
FROM analytics_base.customer_lifetime_value_monthly_cust_jfb AS clvmc
    JOIN (
            SELECT customer_id, activation_key, vip_store_id, MIN(month_date) AS min_month_date
            FROM _customer_lifetime__customer_month
            GROUP BY customer_id, activation_key, vip_store_id
        ) AS a
        ON a.customer_id = clvmc.customer_id
        AND a.activation_key = clvmc.activation_key
        AND a.vip_store_id = clvmc.vip_store_id
        AND DATEADD(month, -1, a.min_month_date) = clvmc.month_date;
-- SELECT * FROM _customer_lifetime__cumulative_last_month;

UPDATE _customer_lifetime__cumulative AS stg
SET
    stg.cumulative_product_gross_profit = stg.cumulative_product_gross_profit + clm.last_month_cumulative_product_gross_profit,
    stg.cumulative_cash_gross_profit = stg.cumulative_cash_gross_profit + clm.last_month_cumulative_cash_gross_profit
FROM _customer_lifetime__cumulative_last_month AS clm
WHERE clm.customer_id = stg.customer_id
    AND clm.activation_key = stg.activation_key
    AND clm.vip_store_id = stg.vip_store_id;

/* To calculate deciles, we need all customers.  Extract customers that are not in _customer_lifetime__monthly_cust_stg from CLTVM.  Mark them as LTV. */
CREATE OR REPLACE TEMP TABLE _customer_lifetime__cumulative_all_customers AS
SELECT
    month_date,
    customer_id,
    activation_key,
    store_id,
    vip_store_id,
    guest_cohort_month_date,
    vip_cohort_month_date,
    cumulative_cash_gross_profit,
    cumulative_product_gross_profit,
    cumulative_cash_gross_profit_decile AS current_cumulative_cash_gross_profit_decile,
    cumulative_product_gross_profit_decile AS current_cumulative_product_gross_profit_decile,
    'ltv' AS source
FROM analytics_base.customer_lifetime_value_monthly_cust_jfb
WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM _customer_lifetime__customer_month)
    AND month_date >= $process_start
    AND activation_key != -1
UNION ALL
SELECT
    month_date,
    customer_id,
    activation_key,
    store_id,
    vip_store_id,
    guest_cohort_month_date,
    vip_cohort_month_date,
    cumulative_cash_gross_profit,
    cumulative_product_gross_profit,
    NULL AS current_cumulative_cash_gross_profit_decile,
    NULL AS current_cumulative_product_gross_profit_decile,
    'new' AS source
FROM _customer_lifetime__cumulative;
-- SELECT * FROM _customer_lifetime__cumulative_all_customers;

/* Calculate deciles */
CREATE OR REPLACE TEMP TABLE _customer_lifetime__decile_calc AS
SELECT
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    NTILE(10) OVER(PARTITION BY store_id, month_date, vip_cohort_month_date ORDER BY cumulative_cash_gross_profit ASC, customer_id ASC) AS cumulative_cash_gross_profit_decile,
    NTILE(10) OVER(PARTITION BY store_id, month_date, vip_cohort_month_date ORDER BY cumulative_product_gross_profit ASC, customer_id ASC) AS cumulative_product_gross_profit_decile,
    cumulative_cash_gross_profit,
    cumulative_product_gross_profit,
    current_cumulative_cash_gross_profit_decile,
    current_cumulative_product_gross_profit_decile,
    source
FROM _customer_lifetime__cumulative_all_customers
WHERE activation_key != -1
UNION ALL
SELECT
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    NULL AS cumulative_cash_gross_profit_decile,
    NULL AS cumulative_product_gross_profit_decile,
    cumulative_cash_gross_profit,
    cumulative_product_gross_profit,
    current_cumulative_cash_gross_profit_decile,
    current_cumulative_product_gross_profit_decile,
    source
FROM _customer_lifetime__cumulative_all_customers
WHERE activation_key = -1;
-- SELECT * FROM _customer_lifetime__decile_calc;

/* If a customer's decile did not change, remove them from the result set as we don't need to process them again */
DELETE FROM _customer_lifetime__decile_calc
WHERE current_cumulative_cash_gross_profit_decile = cumulative_cash_gross_profit_decile
    AND current_cumulative_product_gross_profit_decile = cumulative_product_gross_profit_decile
    AND source = 'ltv';
-- SELECT COUNT(1) FROM _customer_lifetime__decile_calc;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__monthly_cust_stg AS
SELECT
    month_date,
    customer_id,
    meta_original_customer_id,
    activation_key,
    first_activation_key,
    gender,
    store_id,
    vip_store_id,
    finance_specialty_store,
    guest_cohort_month_date,
    vip_cohort_month_date,
    membership_type,
    membership_type_entering_month,
    is_bop_vip,
    is_reactivated_vip,
    is_cancel,
    is_cancel_before_6th,
    is_passive_cancel,
    is_skip,
    is_snooze,
    is_login,
    segment_activity,
    is_merch_purchaser,
    is_successful_billing,
    is_pending_billing,
    is_failed_billing,
    is_cross_promo,
    is_retail_vip,
    is_scrubs_customer,
    entering_month_unredeemed_credits_per_customer,
    customer_action_category,
    cumulative_cash_gross_profit,
    cumulative_product_gross_profit,
    cumulative_cash_gross_profit_decile,
    cumulative_product_gross_profit_decile,
    HASH (
        month_date,
        customer_id,
        meta_original_customer_id,
        activation_key,
        first_activation_key,
        gender,
        store_id,
        vip_store_id,
        finance_specialty_store,
        guest_cohort_month_date,
        vip_cohort_month_date,
        membership_type,
        membership_type_entering_month,
        is_bop_vip,
        is_reactivated_vip,
        is_cancel,
        is_cancel_before_6th,
        is_passive_cancel,
        is_skip,
        is_snooze,
        is_login,
        segment_activity,
        is_merch_purchaser,
        is_successful_billing,
        is_pending_billing,
        is_failed_billing,
        is_cross_promo,
        is_retail_vip,
        is_scrubs_customer,
        entering_month_unredeemed_credits_per_customer,
        customer_action_category,
        cumulative_cash_gross_profit,
        cumulative_product_gross_profit,
        cumulative_cash_gross_profit_decile,
        cumulative_product_gross_profit_decile
        ) AS meta_row_hash,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM (
    SELECT
        cd.month_date,
        cd.customer_id,
        stg.udf_unconcat_brand(cd.customer_id) AS meta_original_customer_id,
        cd.activation_key,
        cd.first_activation_key,
        cd.gender,
        cd.store_id,
        cd.vip_store_id,
        cd.finance_specialty_store,
        cd.guest_cohort_month_date,
        cd.vip_cohort_month_date,
        cd.membership_type,
        cd.membership_type_entering_month,
        cd.is_bop_vip,
        cd.is_reactivated_vip,
        cd.is_cancel,
        cd.is_cancel_before_6th,
        cd.is_passive_cancel,
        cd.is_skip,
        cd.is_snooze,
        cd.is_login,
        cd.segment_activity,
        cd.is_merch_purchaser,
        cd.is_successful_billing,
        cd.is_pending_billing,
        cd.is_failed_billing,
        cd.is_cross_promo,
        cd.is_retail_vip,
        cd.is_scrubs_customer,
        cd.entering_month_unredeemed_credits_per_customer,
        CASE
            WHEN cd.is_successful_billing AND cd.is_merch_purchaser THEN 'Merch Purchase and Successful Billing'
            WHEN cd.is_failed_billing AND cd.is_merch_purchaser THEN 'Merch Purchase and Failed Billing'
            WHEN cd.is_skip AND cd.is_merch_purchaser THEN 'Merch Purchase and Skip'
            WHEN cd.is_merch_purchaser THEN 'Merch Purchaser'
            WHEN cd.is_successful_billing THEN 'Successful Billing'
            WHEN cd.is_failed_billing THEN 'Failed Billing'
            WHEN cd.is_skip THEN 'Skip Only'
            WHEN cd.is_cancel AND cd.is_bop_vip THEN 'Cancel Only'
            WHEN cd.is_bop_vip THEN 'Other/No Action'
            ELSE 'Unknown' END AS customer_action_category,
        dc.cumulative_cash_gross_profit,
        dc.cumulative_product_gross_profit,
        dc.cumulative_cash_gross_profit_decile,
        dc.cumulative_product_gross_profit_decile,
        dc.source
    FROM _customer_lifetime__cust_data AS cd
        JOIN _customer_lifetime__decile_calc AS dc
            ON dc.month_date = cd.month_date
            AND dc.customer_id = cd.customer_id
            AND dc.activation_key = cd.activation_key
            AND dc.vip_store_id = cd.vip_store_id
    WHERE dc.source = 'new'
    UNION ALL
    SELECT
        mc.month_date,
        mc.customer_id,
        stg.udf_unconcat_brand(mc.customer_id) AS meta_original_customer_id,
        mc.activation_key,
        mc.first_activation_key,
        mc.gender,
        mc.store_id,
        mc.vip_store_id,
        mc.finance_specialty_store,
        mc.guest_cohort_month_date,
        mc.vip_cohort_month_date,
        mc.membership_type,
        mc.membership_type_entering_month,
        mc.is_bop_vip,
        mc.is_reactivated_vip,
        mc.is_cancel,
        mc.is_cancel_before_6th,
        mc.is_passive_cancel,
        mc.is_skip,
        mc.is_snooze,
        mc.is_login,
        mc.segment_activity,
        mc.is_merch_purchaser,
        mc.is_successful_billing,
        mc.is_pending_billing,
        mc.is_failed_billing,
        mc.is_cross_promo,
        mc.is_retail_vip,
        mc.is_scrubs_customer,
        mc.entering_month_unredeemed_credits_per_customer,
        mc.customer_action_category,
        dc.cumulative_cash_gross_profit,
        dc.cumulative_product_gross_profit,
        dc.cumulative_cash_gross_profit_decile,
        dc.cumulative_product_gross_profit_decile,
        dc.source
    FROM analytics_base.customer_lifetime_value_monthly_cust_jfb AS mc
        JOIN _customer_lifetime__decile_calc AS dc
            ON dc.month_date = mc.month_date
            AND dc.customer_id = mc.customer_id
            AND dc.activation_key = mc.activation_key
            AND dc.vip_store_id = mc.vip_store_id
    WHERE dc.source = 'ltv'
    ) AS _cust_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY month_date, customer_id, activation_key, vip_store_id ORDER BY source DESC) = 1;
-- SELECT * FROM _customer_lifetime__monthly_cust_stg where customer_id = 15524149;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__fso_deleted AS
SELECT DISTINCT
        customer_id,
        DATE_TRUNC(MONTH, fso.date) AS month_date,
        activation_key,
        vip_store_id
FROM analytics_base.finance_sales_ops_stg fso
WHERE is_deleted
AND NOT $is_full_refresh
--AND fso.meta_update_datetime > $wm_edw_analytics_base_finance_sales_ops_stg
;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__fso_orphan AS
SELECT
    customer_id,
    month_date,
    activation_key,
    vip_store_id
FROM (
        SELECT
           cm.customer_id,
           cm.month_date,
           cm.activation_key,
           cm.vip_store_id,
           min(fso.is_deleted) as is_deleted
        FROM _customer_lifetime__fso_deleted AS cm
        JOIN analytics_base.finance_sales_ops_stg AS fso
            ON fso.customer_id = cm.customer_id
            AND fso.activation_key = cm.activation_key
            AND DATE_TRUNC(MONTH, fso.date) = cm.month_date
            AND cm.vip_store_id = fso.vip_store_id
        WHERE fso.date_object = 'placed'
            AND fso.currency_object = 'usd'
            AND NOT $is_full_refresh
        GROUP BY
             cm.customer_id,
             cm.month_date,
             cm.activation_key,
             cm.vip_store_id
     )
WHERE is_deleted = TRUE;


BEGIN TRANSACTION; /* analytics_base.customer_lifetime_value_monthly_cust */

/* For customers that had a status change, update their values on analytics_base.customer_lifetime_value_monthly_cust */
UPDATE analytics_base.customer_lifetime_value_monthly_cust_jfb AS t
SET
    --t.month_date = s.month_date
    --t.customer_id = s.customer_id
    --t.activation_key = s.activation_key
    t.meta_original_customer_id = s.meta_original_customer_id,
    t.first_activation_key = s.first_activation_key,
    t.gender = s.gender,
    t.store_id = s.store_id,
    t.finance_specialty_store = s.finance_specialty_store,
    t.guest_cohort_month_date = s.guest_cohort_month_date,
    t.vip_cohort_month_date = s.vip_cohort_month_date,
    t.membership_type = s.membership_type,
    t.membership_type_entering_month = s.membership_type_entering_month,
    t.is_bop_vip = s.is_bop_vip,
    t.is_reactivated_vip = s.is_reactivated_vip,
    t.is_cancel = s.is_cancel,
    t.is_cancel_before_6th = s.is_cancel_before_6th,
    t.is_passive_cancel = s.is_passive_cancel,
    t.is_skip = s.is_skip,
    t.is_snooze = s.is_snooze,
    t.is_login = s.is_login,
    t.segment_activity = s.segment_activity,
    t.is_merch_purchaser = s.is_merch_purchaser,
    t.is_successful_billing = s.is_successful_billing,
    t.is_pending_billing = s.is_pending_billing,
    t.is_failed_billing = s.is_failed_billing,
    t.is_cross_promo = s.is_cross_promo,
    t.is_retail_vip = s.is_retail_vip,
    t.is_scrubs_customer = s.is_scrubs_customer,
    t.entering_month_unredeemed_credits_per_customer = s.entering_month_unredeemed_credits_per_customer,
    t.customer_action_category = s.customer_action_category,
    t.cumulative_cash_gross_profit = s.cumulative_cash_gross_profit,
    t.cumulative_product_gross_profit = s.cumulative_product_gross_profit,
    t.cumulative_cash_gross_profit_decile = s.cumulative_cash_gross_profit_decile,
    t.cumulative_product_gross_profit_decile = s.cumulative_product_gross_profit_decile,
    t.meta_row_hash = s.meta_row_hash,
    --t.meta_create_datetime = s.meta_create_datetime,
    t.meta_update_datetime = s.meta_update_datetime
FROM _customer_lifetime__monthly_cust_stg AS s
WHERE s.month_date = t.month_date
    AND s.customer_id = t.customer_id
    AND s.activation_key = t.activation_key
    AND s.vip_store_id = t.vip_store_id
    AND s.meta_row_hash != t.meta_row_hash;

/* For new customers, insert their values into analytics_base.customer_lifetime_value_monthly_cust */
INSERT INTO analytics_base.customer_lifetime_value_monthly_cust_jfb (
    month_date,
    customer_id,
    meta_original_customer_id,
    activation_key,
    first_activation_key,
    gender,
    store_id,
    vip_store_id,
    finance_specialty_store,
    guest_cohort_month_date,
    vip_cohort_month_date,
    membership_type,
    membership_type_entering_month,
    is_bop_vip,
    is_reactivated_vip,
    is_cancel,
    is_cancel_before_6th,
    is_passive_cancel,
    is_skip,
    is_snooze,
    is_login,
    segment_activity,
    is_merch_purchaser,
    is_successful_billing,
    is_pending_billing,
    is_failed_billing,
    is_cross_promo,
    is_retail_vip,
    is_scrubs_customer,
    entering_month_unredeemed_credits_per_customer,
    customer_action_category,
    cumulative_cash_gross_profit,
    cumulative_product_gross_profit,
    cumulative_cash_gross_profit_decile,
    cumulative_product_gross_profit_decile,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    s.month_date,
    s.customer_id,
    s.meta_original_customer_id,
    s.activation_key,
    s.first_activation_key,
    s.gender,
    s.store_id,
    s.vip_store_id,
    s.finance_specialty_store,
    s.guest_cohort_month_date,
    s.vip_cohort_month_date,
    s.membership_type,
    s.membership_type_entering_month,
    s.is_bop_vip,
    s.is_reactivated_vip,
    s.is_cancel,
    s.is_cancel_before_6th,
    s.is_passive_cancel,
    s.is_skip,
    s.is_snooze,
    s.is_login,
    s.segment_activity,
    s.is_merch_purchaser,
    s.is_successful_billing,
    s.is_pending_billing,
    s.is_failed_billing,
    s.is_cross_promo,
    s.is_retail_vip,
    s.is_scrubs_customer,
    s.entering_month_unredeemed_credits_per_customer,
    s.customer_action_category,
    s.cumulative_cash_gross_profit,
    s.cumulative_product_gross_profit,
    s.cumulative_cash_gross_profit_decile,
    s.cumulative_product_gross_profit_decile,
    s.meta_row_hash,
    s.meta_create_datetime,
    s.meta_update_datetime
FROM _customer_lifetime__monthly_cust_stg AS s
    LEFT JOIN analytics_base.customer_lifetime_value_monthly_cust_jfb AS t
        ON t.month_date = s.month_date
        AND t.customer_id = s.customer_id
        AND t.activation_key = s.activation_key
        AND t.vip_store_id = s.vip_store_id
WHERE t.meta_row_hash IS NULL
ORDER BY
    s.month_date,
    s.customer_id,
    s.activation_key,
    s.vip_store_id;

-- Delete orphan data
DELETE FROM analytics_base.customer_lifetime_value_monthly_cust_jfb AS t
WHERE $is_full_refresh
    AND NOT EXISTS (
        SELECT TRUE AS is_exists
        FROM _customer_lifetime__customer_month AS cm
        WHERE cm.month_date = t.month_date
            AND cm.customer_id = t.customer_id
            AND cm.activation_key = t.activation_key
            AND cm.vip_store_id = t.vip_store_id
        );

DELETE FROM analytics_base.customer_lifetime_value_monthly_cust_jfb AS t
WHERE NOT $is_full_refresh
    AND EXISTS (
        SELECT TRUE AS is_exists
        FROM stg.fact_activation AS fa
        WHERE fa.activation_key = t.activation_key
--            AND fa.meta_update_datetime > $wm_edw_stg_fact_activation
            AND fa.is_deleted
        );

COMMIT; /* analytics_base.customer_lifetime_value_monthly_cust */

BEGIN TRANSACTION; /* analytics_base.customer_lifetime_value_monthly_agg */

/* For customers that had an aggregation change, update their values on analytics_base.customer_lifetime_value_monthly_agg */
UPDATE analytics_base.customer_lifetime_value_monthly_agg_jfb AS t
SET
    --t.month_date = s.month_date
    --t.customer_id = s.customer_id
    --t.activation_key = s.activation_key
    t.product_order_count = s.product_order_count,
    t.online_product_order_count = s.online_product_order_count,
    t.retail_product_order_count = s.retail_product_order_count,
    t.mobile_app_product_order_count = s.mobile_app_product_order_count,
    t.product_order_count_with_credit_redemption = s.product_order_count_with_credit_redemption,
    t.product_order_unit_count = s.product_order_unit_count,
    t.online_product_order_unit_count = s.online_product_order_unit_count,
    t.retail_product_order_unit_count = s.retail_product_order_unit_count,
    t.mobile_app_product_order_unit_count = s.mobile_app_product_order_unit_count,
    t.product_order_subtotal_excl_tariff_amount = s.product_order_subtotal_excl_tariff_amount,
    t.product_order_product_subtotal_amount = s.product_order_product_subtotal_amount,
    t.online_product_order_subtotal_excl_tariff_amount = s.online_product_order_subtotal_excl_tariff_amount,
    t.retail_product_order_subtotal_excl_tariff_amount = s.retail_product_order_subtotal_excl_tariff_amount,
    t.mobile_app_product_order_subtotal_excl_tariff_amount = s.mobile_app_product_order_subtotal_excl_tariff_amount,
    t.product_order_product_discount_amount = s.product_order_product_discount_amount,
    t.online_product_order_product_discount_amount = s.online_product_order_product_discount_amount,
    t.retail_product_order_product_discount_amount = s.retail_product_order_product_discount_amount,
    t.mobile_app_product_order_product_discount_amount = s.mobile_app_product_order_product_discount_amount,
    t.product_order_shipping_revenue_amount = s.product_order_shipping_revenue_amount,
    t.online_product_order_shipping_revenue_amount = s.online_product_order_shipping_revenue_amount,
    t.retail_product_order_shipping_revenue_amount = s.retail_product_order_shipping_revenue_amount,
    t.mobile_app_product_order_shipping_revenue_amount = s.mobile_app_product_order_shipping_revenue_amount,
    t.product_order_direct_cogs_amount = s.product_order_direct_cogs_amount,
    t.online_product_order_direct_cogs_amount = s.online_product_order_direct_cogs_amount,
    t.retail_product_order_direct_cogs_amount = s.retail_product_order_direct_cogs_amount,
    t.mobile_app_product_order_direct_cogs_amount = s.mobile_app_product_order_direct_cogs_amount,
    t.product_order_selling_expenses_amount = s.product_order_selling_expenses_amount,
    t.online_product_order_selling_expenses_amount = s.online_product_order_selling_expenses_amount,
    t.retail_product_order_selling_expenses_amount = s.retail_product_order_selling_expenses_amount,
    t.mobile_app_product_order_selling_expenses_amount = s.mobile_app_product_order_selling_expenses_amount,
    t.product_order_cash_refund_amount_and_chargeback_amount = s.product_order_cash_refund_amount_and_chargeback_amount,
    t.online_product_order_cash_refund_chargeback_amount = s.online_product_order_cash_refund_chargeback_amount,
    t.retail_product_order_cash_refund_chargeback_amount = s.retail_product_order_cash_refund_chargeback_amount,
    t.mobile_app_product_order_cash_refund_chargeback_amount = s.mobile_app_product_order_cash_refund_chargeback_amount,
    t.product_order_cash_credit_refund_amount = s.product_order_cash_credit_refund_amount,
    t.online_product_order_cash_credit_refund_amount = s.online_product_order_cash_credit_refund_amount,
    t.retail_product_order_cash_credit_refund_amount = s.retail_product_order_cash_credit_refund_amount,
    t.mobile_app_product_order_cash_credit_refund_amount = s.mobile_app_product_order_cash_credit_refund_amount,
    t.product_order_reship_exchange_order_count = s.product_order_reship_exchange_order_count,
    t.online_product_order_reship_exchange_order_count = s.online_product_order_reship_exchange_order_count,
    t.retail_product_order_reship_exchange_order_count = s.retail_product_order_reship_exchange_order_count,
    t.mobile_app_product_order_reship_exchange_order_count = s.mobile_app_product_order_reship_exchange_order_count,
    t.product_order_reship_exchange_unit_count = s.product_order_reship_exchange_unit_count,
    t.online_product_order_reship_exchange_unit_count = s.online_product_order_reship_exchange_unit_count,
    t.retail_product_order_reship_exchange_unit_count = s.retail_product_order_reship_exchange_unit_count,
    t.mobile_app_product_order_reship_exchange_unit_count = s.mobile_app_product_order_reship_exchange_unit_count,
    t.product_order_reship_exchange_direct_cogs_amount = s.product_order_reship_exchange_direct_cogs_amount,
    t.online_product_order_reship_exchange_direct_cogs_amount = s.online_product_order_reship_exchange_direct_cogs_amount,
    t.retail_product_order_reship_exchange_direct_cogs_amount = s.retail_product_order_reship_exchange_direct_cogs_amount,
    t.mobile_app_product_order_reship_exchange_direct_cogs_amount = s.mobile_app_product_order_reship_exchange_direct_cogs_amount,
    t.product_order_return_cogs_amount = s.product_order_return_cogs_amount,
    t.online_product_order_return_cogs_amount = s.online_product_order_return_cogs_amount,
    t.retail_product_order_return_cogs_amount = s.retail_product_order_return_cogs_amount,
    t.mobile_app_product_order_return_cogs_amount = s.mobile_app_product_order_return_cogs_amount,
    t.product_order_return_unit_count = s.product_order_return_unit_count,
    t.online_product_order_return_unit_count = s.online_product_order_return_unit_count,
    t.retail_product_order_return_unit_count = s.retail_product_order_return_unit_count,
    t.mobile_app_product_order_return_unit_count = s.mobile_app_product_order_return_unit_count,
    t.product_order_amount_to_pay = s.product_order_amount_to_pay,
    t.online_product_order_amount_to_pay = s.online_product_order_amount_to_pay,
    t.retail_product_order_amount_to_pay = s.retail_product_order_amount_to_pay,
    t.mobile_app_product_order_amount_to_pay = s.mobile_app_product_order_amount_to_pay,
    t.product_gross_revenue_excl_shipping = s.product_gross_revenue_excl_shipping,
    t.online_product_gross_revenue_excl_shipping = s.online_product_gross_revenue_excl_shipping,
    t.retail_product_gross_revenue_excl_shipping = s.retail_product_gross_revenue_excl_shipping,
    t.mobile_app_product_gross_revenue_excl_shipping = s.mobile_app_product_gross_revenue_excl_shipping,
    t.product_gross_revenue = s.product_gross_revenue,
    t.online_product_gross_revenue = s.online_product_gross_revenue,
    t.retail_product_gross_revenue = s.retail_product_gross_revenue,
    t.mobile_app_product_gross_revenue = s.mobile_app_product_gross_revenue,
    t.product_net_revenue = s.product_net_revenue,
    t.online_product_net_revenue = s.online_product_net_revenue,
    t.retail_product_net_revenue = s.retail_product_net_revenue,
    t.mobile_app_product_net_revenue = s.mobile_app_product_net_revenue,
    t.product_margin_pre_return = s.product_margin_pre_return,
    t.online_product_margin_pre_return = s.online_product_margin_pre_return,
    t.retail_product_margin_pre_return = s.retail_product_margin_pre_return,
    t.mobile_app_product_margin_pre_return = s.mobile_app_product_margin_pre_return,
    t.product_margin_pre_return_excl_shipping = s.product_margin_pre_return_excl_shipping,
    t.online_product_margin_pre_return_excl_shipping = s.online_product_margin_pre_return_excl_shipping,
    t.retail_product_margin_pre_return_excl_shipping = s.retail_product_margin_pre_return_excl_shipping,
    t.mobile_app_product_margin_pre_return_excl_shipping = s.mobile_app_product_margin_pre_return_excl_shipping,
    t.product_gross_profit = s.product_gross_profit,
    t.online_product_gross_profit = s.online_product_gross_profit,
    t.retail_product_gross_profit = s.retail_product_gross_profit,
    t.mobile_app_product_gross_profit = s.mobile_app_product_gross_profit,
    t.product_variable_contribution_profit = s.product_variable_contribution_profit,
    t.online_product_variable_contribution_profit = s.online_product_variable_contribution_profit,
    t.retail_product_variable_contribution_profit = s.retail_product_variable_contribution_profit,
    t.mobile_app_product_variable_contribution_profit = s.mobile_app_product_variable_contribution_profit,
    t.product_order_cash_gross_revenue_amount = s.product_order_cash_gross_revenue_amount,
    t.online_product_order_cash_gross_revenue_amount = s.online_product_order_cash_gross_revenue_amount,
    t.retail_product_order_cash_gross_revenue_amount = s.retail_product_order_cash_gross_revenue_amount,
    t.mobile_app_product_order_cash_gross_revenue_amount = s.mobile_app_product_order_cash_gross_revenue_amount,
    t.product_order_cash_net_revenue = s.product_order_cash_net_revenue,
    t.online_product_order_cash_net_revenue = s.online_product_order_cash_net_revenue,
    t.retail_product_order_cash_net_revenue = s.retail_product_order_cash_net_revenue,
    t.mobile_app_product_order_cash_net_revenue = s.mobile_app_product_order_cash_net_revenue,
    t.product_order_landed_product_cost_amount = s.product_order_landed_product_cost_amount,
    t.online_product_order_landed_product_cost_amount = s.online_product_order_landed_product_cost_amount,
    t.retail_product_order_landed_product_cost_amount = s.retail_product_order_landed_product_cost_amount,
    t.mobile_app_product_order_landed_product_cost_amount = s.mobile_app_product_order_landed_product_cost_amount,
    t.product_order_cash_margin_pre_return = s.product_order_cash_margin_pre_return,
    t.online_product_order_cash_margin_pre_return = s.online_product_order_cash_margin_pre_return,
    t.retail_product_order_cash_margin_pre_return = s.retail_product_order_cash_margin_pre_return,
    t.mobile_app_product_order_cash_margin_pre_return = s.mobile_app_product_order_cash_margin_pre_return,
    t.product_order_cash_gross_profit = s.product_order_cash_gross_profit,
    t.online_product_order_cash_gross_profit = s.online_product_order_cash_gross_profit,
    t.retail_product_order_cash_gross_profit = s.retail_product_order_cash_gross_profit,
    t.mobile_app_product_order_cash_gross_profit = s.mobile_app_product_order_cash_gross_profit,
    t.billing_cash_gross_revenue = s.billing_cash_gross_revenue,
    t.billing_cash_net_revenue = s.billing_cash_net_revenue,
    t.cash_gross_revenue = s.cash_gross_revenue,
    t.cash_net_revenue = s.cash_net_revenue,
    t.cash_gross_profit = s.cash_gross_profit,
    t.cash_variable_contribution_profit = s.cash_variable_contribution_profit,
    t.monthly_billed_credit_cash_transaction_amount = s.monthly_billed_credit_cash_transaction_amount,
    t.membership_fee_cash_transaction_amount = s.membership_fee_cash_transaction_amount,
    t.gift_card_transaction_amount = s.gift_card_transaction_amount,
    t.legacy_credit_cash_transaction_amount = s.legacy_credit_cash_transaction_amount,
    t.monthly_billed_credit_cash_refund_count = s.monthly_billed_credit_cash_refund_count,
    t.monthly_billed_credit_cash_refund_chargeback_amount = s.monthly_billed_credit_cash_refund_chargeback_amount,
    t.membership_fee_cash_refund_chargeback_amount = s.membership_fee_cash_refund_chargeback_amount,
    t.gift_card_cash_refund_chargeback_amount = s.gift_card_cash_refund_chargeback_amount,
    t.legacy_credit_cash_refund_chargeback_amount = s.legacy_credit_cash_refund_chargeback_amount,
    t.billed_cash_credit_issued_amount = s.billed_cash_credit_issued_amount,
    t.billed_cash_credit_redeemed_amount = s.billed_cash_credit_redeemed_amount,
    t.online_billed_cash_credit_redeemed_amount = s.online_billed_cash_credit_redeemed_amount,
    t.retail_billed_cash_credit_redeemed_amount = s.retail_billed_cash_credit_redeemed_amount,
    t.mobile_billed_cash_credit_redeemed_amount = s.mobile_billed_cash_credit_redeemed_amount,
    t.billed_cash_credit_cancelled_amount = s.billed_cash_credit_cancelled_amount,
    t.billed_cash_credit_expired_amount = s.billed_cash_credit_expired_amount,
    t.billed_cash_credit_issued_equivalent_count = s.billed_cash_credit_issued_equivalent_count,
    t.billed_cash_credit_redeemed_same_month_amount = s.billed_cash_credit_redeemed_same_month_amount,
    t.billed_cash_credit_redeemed_equivalent_count = s.billed_cash_credit_redeemed_equivalent_count,
    t.billed_cash_credit_cancelled_equivalent_count = s.billed_cash_credit_cancelled_equivalent_count,
    t.billed_cash_credit_expired_equivalent_count = s.billed_cash_credit_expired_equivalent_count,
    t.refund_cash_credit_issued_amount = s.refund_cash_credit_issued_amount,
    t.refund_cash_credit_redeemed_amount = s.refund_cash_credit_redeemed_amount,
    t.refund_cash_credit_cancelled_amount = s.refund_cash_credit_cancelled_amount,
    t.refund_cash_credit_expired_amount = s.refund_cash_credit_expired_amount,
    t.other_cash_credit_issued_amount = s.other_cash_credit_issued_amount,
    t.other_cash_credit_redeemed_amount = s.other_cash_credit_redeemed_amount,
    t.other_cash_credit_cancelled_amount = s.other_cash_credit_cancelled_amount,
    t.other_cash_credit_expired_amount = s.other_cash_credit_expired_amount,
    t.noncash_credit_issued_amount = s.noncash_credit_issued_amount,
    t.noncash_credit_cancelled_amount = s.noncash_credit_cancelled_amount,
    t.noncash_credit_expired_amount = s.noncash_credit_expired_amount,
    t.first_guest_product_margin_pre_return = s.first_guest_product_margin_pre_return,
    t.meta_row_hash = s.meta_row_hash,
    --t.meta_create_datetime = s.meta_create_datetime,
    t.meta_update_datetime = s.meta_update_datetime
FROM _customer_lifetime__monthly_agg_stg AS s
WHERE s.month_date = t.month_date
    AND s.customer_id = t.customer_id
    AND s.activation_key = t.activation_key
    AND s.vip_store_id = t.vip_store_id
    AND s.meta_row_hash != t.meta_row_hash;

/* For new customers, insert their values into analytics_base.customer_lifetime_value_monthly_agg */
INSERT INTO analytics_base.customer_lifetime_value_monthly_agg_jfb (
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    product_order_count,
    online_product_order_count,
    retail_product_order_count,
    mobile_app_product_order_count,
    product_order_count_with_credit_redemption,
    product_order_unit_count,
    online_product_order_unit_count,
    retail_product_order_unit_count,
    mobile_app_product_order_unit_count,
    product_order_subtotal_excl_tariff_amount,
    product_order_product_subtotal_amount,
    online_product_order_subtotal_excl_tariff_amount,
    retail_product_order_subtotal_excl_tariff_amount,
    mobile_app_product_order_subtotal_excl_tariff_amount,
    product_order_product_discount_amount,
    online_product_order_product_discount_amount,
    retail_product_order_product_discount_amount,
    mobile_app_product_order_product_discount_amount,
    product_order_shipping_revenue_amount,
    online_product_order_shipping_revenue_amount,
    retail_product_order_shipping_revenue_amount,
    mobile_app_product_order_shipping_revenue_amount,
    product_order_direct_cogs_amount,
    online_product_order_direct_cogs_amount,
    retail_product_order_direct_cogs_amount,
    mobile_app_product_order_direct_cogs_amount,
    product_order_selling_expenses_amount,
    online_product_order_selling_expenses_amount,
    retail_product_order_selling_expenses_amount,
    mobile_app_product_order_selling_expenses_amount,
    product_order_cash_refund_amount_and_chargeback_amount,
    online_product_order_cash_refund_chargeback_amount,
    retail_product_order_cash_refund_chargeback_amount,
    mobile_app_product_order_cash_refund_chargeback_amount,
    product_order_cash_credit_refund_amount,
    online_product_order_cash_credit_refund_amount,
    retail_product_order_cash_credit_refund_amount,
    mobile_app_product_order_cash_credit_refund_amount,
    product_order_reship_exchange_order_count,
    online_product_order_reship_exchange_order_count,
    retail_product_order_reship_exchange_order_count,
    mobile_app_product_order_reship_exchange_order_count,
    product_order_reship_exchange_unit_count,
    online_product_order_reship_exchange_unit_count,
    retail_product_order_reship_exchange_unit_count,
    mobile_app_product_order_reship_exchange_unit_count,
    product_order_reship_exchange_direct_cogs_amount,
    online_product_order_reship_exchange_direct_cogs_amount,
    retail_product_order_reship_exchange_direct_cogs_amount,
    mobile_app_product_order_reship_exchange_direct_cogs_amount,
    product_order_return_cogs_amount,
    online_product_order_return_cogs_amount,
    retail_product_order_return_cogs_amount,
    mobile_app_product_order_return_cogs_amount,
    product_order_return_unit_count,
    online_product_order_return_unit_count,
    retail_product_order_return_unit_count,
    mobile_app_product_order_return_unit_count,
    product_order_amount_to_pay,
    online_product_order_amount_to_pay,
    retail_product_order_amount_to_pay,
    mobile_app_product_order_amount_to_pay,
    product_gross_revenue_excl_shipping,
    online_product_gross_revenue_excl_shipping,
    retail_product_gross_revenue_excl_shipping,
    mobile_app_product_gross_revenue_excl_shipping,
    product_gross_revenue,
    online_product_gross_revenue,
    retail_product_gross_revenue,
    mobile_app_product_gross_revenue,
    product_net_revenue,
    online_product_net_revenue,
    retail_product_net_revenue,
    mobile_app_product_net_revenue,
    product_margin_pre_return,
    online_product_margin_pre_return,
    retail_product_margin_pre_return,
    mobile_app_product_margin_pre_return,
    product_margin_pre_return_excl_shipping,
    online_product_margin_pre_return_excl_shipping,
    retail_product_margin_pre_return_excl_shipping,
    mobile_app_product_margin_pre_return_excl_shipping,
    product_gross_profit,
    online_product_gross_profit,
    retail_product_gross_profit,
    mobile_app_product_gross_profit,
    product_variable_contribution_profit,
    online_product_variable_contribution_profit,
    retail_product_variable_contribution_profit,
    mobile_app_product_variable_contribution_profit,
    product_order_cash_gross_revenue_amount,
    online_product_order_cash_gross_revenue_amount,
    retail_product_order_cash_gross_revenue_amount,
    mobile_app_product_order_cash_gross_revenue_amount,
    product_order_cash_net_revenue,
    online_product_order_cash_net_revenue,
    retail_product_order_cash_net_revenue,
    mobile_app_product_order_cash_net_revenue,
    product_order_landed_product_cost_amount,
    online_product_order_landed_product_cost_amount,
    retail_product_order_landed_product_cost_amount,
    mobile_app_product_order_landed_product_cost_amount,
    product_order_cash_margin_pre_return,
    online_product_order_cash_margin_pre_return,
    retail_product_order_cash_margin_pre_return,
    mobile_app_product_order_cash_margin_pre_return,
    product_order_cash_gross_profit,
    online_product_order_cash_gross_profit,
    retail_product_order_cash_gross_profit,
    mobile_app_product_order_cash_gross_profit,
    billing_cash_gross_revenue,
    billing_cash_net_revenue,
    cash_gross_revenue,
    cash_net_revenue,
    cash_gross_profit,
    cash_variable_contribution_profit,
    monthly_billed_credit_cash_transaction_amount,
    membership_fee_cash_transaction_amount,
    gift_card_transaction_amount,
    legacy_credit_cash_transaction_amount,
    monthly_billed_credit_cash_refund_count,
    monthly_billed_credit_cash_refund_chargeback_amount,
    membership_fee_cash_refund_chargeback_amount,
    gift_card_cash_refund_chargeback_amount,
    legacy_credit_cash_refund_chargeback_amount,
    billed_cash_credit_issued_amount,
    billed_cash_credit_redeemed_amount,
    online_billed_cash_credit_redeemed_amount,
    retail_billed_cash_credit_redeemed_amount,
    mobile_billed_cash_credit_redeemed_amount,
    billed_cash_credit_cancelled_amount,
    billed_cash_credit_expired_amount,
    billed_cash_credit_issued_equivalent_count,
    billed_cash_credit_redeemed_same_month_amount,
    billed_cash_credit_redeemed_equivalent_count,
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
    first_guest_product_margin_pre_return,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    s.month_date,
    s.customer_id,
    s.activation_key,
    s.vip_store_id,
    s.product_order_count,
    s.online_product_order_count,
    s.retail_product_order_count,
    s.mobile_app_product_order_count,
    s.product_order_count_with_credit_redemption,
    s.product_order_unit_count,
    s.online_product_order_unit_count,
    s.retail_product_order_unit_count,
    s.mobile_app_product_order_unit_count,
    s.product_order_subtotal_excl_tariff_amount,
    s.product_order_product_subtotal_amount,
    s.online_product_order_subtotal_excl_tariff_amount,
    s.retail_product_order_subtotal_excl_tariff_amount,
    s.mobile_app_product_order_subtotal_excl_tariff_amount,
    s.product_order_product_discount_amount,
    s.online_product_order_product_discount_amount,
    s.retail_product_order_product_discount_amount,
    s.mobile_app_product_order_product_discount_amount,
    s.product_order_shipping_revenue_amount,
    s.online_product_order_shipping_revenue_amount,
    s.retail_product_order_shipping_revenue_amount,
    s.mobile_app_product_order_shipping_revenue_amount,
    s.product_order_direct_cogs_amount,
    s.online_product_order_direct_cogs_amount,
    s.retail_product_order_direct_cogs_amount,
    s.mobile_app_product_order_direct_cogs_amount,
    s.product_order_selling_expenses_amount,
    s.online_product_order_selling_expenses_amount,
    s.retail_product_order_selling_expenses_amount,
    s.mobile_app_product_order_selling_expenses_amount,
    s.product_order_cash_refund_amount_and_chargeback_amount,
    s.online_product_order_cash_refund_chargeback_amount,
    s.retail_product_order_cash_refund_chargeback_amount,
    s.mobile_app_product_order_cash_refund_chargeback_amount,
    s.product_order_cash_credit_refund_amount,
    s.online_product_order_cash_credit_refund_amount,
    s.retail_product_order_cash_credit_refund_amount,
    s.mobile_app_product_order_cash_credit_refund_amount,
    s.product_order_reship_exchange_order_count,
    s.online_product_order_reship_exchange_order_count,
    s.retail_product_order_reship_exchange_order_count,
    s.mobile_app_product_order_reship_exchange_order_count,
    s.product_order_reship_exchange_unit_count,
    s.online_product_order_reship_exchange_unit_count,
    s.retail_product_order_reship_exchange_unit_count,
    s.mobile_app_product_order_reship_exchange_unit_count,
    s.product_order_reship_exchange_direct_cogs_amount,
    s.online_product_order_reship_exchange_direct_cogs_amount,
    s.retail_product_order_reship_exchange_direct_cogs_amount,
    s.mobile_app_product_order_reship_exchange_direct_cogs_amount,
    s.product_order_return_cogs_amount,
    s.online_product_order_return_cogs_amount,
    s.retail_product_order_return_cogs_amount,
    s.mobile_app_product_order_return_cogs_amount,
    s.product_order_return_unit_count,
    s.online_product_order_return_unit_count,
    s.retail_product_order_return_unit_count,
    s.mobile_app_product_order_return_unit_count,
    s.product_order_amount_to_pay,
    s.online_product_order_amount_to_pay,
    s.retail_product_order_amount_to_pay,
    s.mobile_app_product_order_amount_to_pay,
    s.product_gross_revenue_excl_shipping,
    s.online_product_gross_revenue_excl_shipping,
    s.retail_product_gross_revenue_excl_shipping,
    s.mobile_app_product_gross_revenue_excl_shipping,
    s.product_gross_revenue,
    s.online_product_gross_revenue,
    s.retail_product_gross_revenue,
    s.mobile_app_product_gross_revenue,
    s.product_net_revenue,
    s.online_product_net_revenue,
    s.retail_product_net_revenue,
    s.mobile_app_product_net_revenue,
    s.product_margin_pre_return,
    s.online_product_margin_pre_return,
    s.retail_product_margin_pre_return,
    s.mobile_app_product_margin_pre_return,
    s.product_margin_pre_return_excl_shipping,
    s.online_product_margin_pre_return_excl_shipping,
    s.retail_product_margin_pre_return_excl_shipping,
    s.mobile_app_product_margin_pre_return_excl_shipping,
    s.product_gross_profit,
    s.online_product_gross_profit,
    s.retail_product_gross_profit,
    s.mobile_app_product_gross_profit,
    s.product_variable_contribution_profit,
    s.online_product_variable_contribution_profit,
    s.retail_product_variable_contribution_profit,
    s.mobile_app_product_variable_contribution_profit,
    s.product_order_cash_gross_revenue_amount,
    s.online_product_order_cash_gross_revenue_amount,
    s.retail_product_order_cash_gross_revenue_amount,
    s.mobile_app_product_order_cash_gross_revenue_amount,
    s.product_order_cash_net_revenue,
    s.online_product_order_cash_net_revenue,
    s.retail_product_order_cash_net_revenue,
    s.mobile_app_product_order_cash_net_revenue,
    s.product_order_landed_product_cost_amount,
    s.online_product_order_landed_product_cost_amount,
    s.retail_product_order_landed_product_cost_amount,
    s.mobile_app_product_order_landed_product_cost_amount,
    s.product_order_cash_margin_pre_return,
    s.online_product_order_cash_margin_pre_return,
    s.retail_product_order_cash_margin_pre_return,
    s.mobile_app_product_order_cash_margin_pre_return,
    s.product_order_cash_gross_profit,
    s.online_product_order_cash_gross_profit,
    s.retail_product_order_cash_gross_profit,
    s.mobile_app_product_order_cash_gross_profit,
    s.billing_cash_gross_revenue,
    s.billing_cash_net_revenue,
    s.cash_gross_revenue,
    s.cash_net_revenue,
    s.cash_gross_profit,
    s.cash_variable_contribution_profit,
    s.monthly_billed_credit_cash_transaction_amount,
    s.membership_fee_cash_transaction_amount,
    s.gift_card_transaction_amount,
    s.legacy_credit_cash_transaction_amount,
    s.monthly_billed_credit_cash_refund_count,
    s.monthly_billed_credit_cash_refund_chargeback_amount,
    s.membership_fee_cash_refund_chargeback_amount,
    s.gift_card_cash_refund_chargeback_amount,
    s.legacy_credit_cash_refund_chargeback_amount,
    s.billed_cash_credit_issued_amount,
    s.billed_cash_credit_redeemed_amount,
    s.online_billed_cash_credit_redeemed_amount,
    s.retail_billed_cash_credit_redeemed_amount,
    s.mobile_billed_cash_credit_redeemed_amount,
    s.billed_cash_credit_cancelled_amount,
    s.billed_cash_credit_expired_amount,
    s.billed_cash_credit_issued_equivalent_count,
    s.billed_cash_credit_redeemed_same_month_amount,
    s.billed_cash_credit_redeemed_equivalent_count,
    s.billed_cash_credit_cancelled_equivalent_count,
    s.billed_cash_credit_expired_equivalent_count,
    s.refund_cash_credit_issued_amount,
    s.refund_cash_credit_redeemed_amount,
    s.refund_cash_credit_cancelled_amount,
    s.refund_cash_credit_expired_amount,
    s.other_cash_credit_issued_amount,
    s.other_cash_credit_redeemed_amount,
    s.other_cash_credit_cancelled_amount,
    s.other_cash_credit_expired_amount,
    s.noncash_credit_issued_amount,
    s.noncash_credit_cancelled_amount,
    s.noncash_credit_expired_amount,
    s.first_guest_product_margin_pre_return,
    s.meta_row_hash,
    s.meta_create_datetime,
    s.meta_update_datetime
FROM _customer_lifetime__monthly_agg_stg AS s
    LEFT JOIN analytics_base.customer_lifetime_value_monthly_agg_jfb AS t
        ON t.month_date = s.month_date
        AND t.customer_id = s.customer_id
        AND t.activation_key = s.activation_key
        AND t.vip_store_id = s.vip_store_id
WHERE t.meta_row_hash IS NULL
ORDER BY
    s.month_date,
    s.customer_id,
    s.activation_key,
    s.vip_store_id;

-- Delete orphan data
DELETE FROM analytics_base.customer_lifetime_value_monthly_agg_jfb AS t
WHERE $is_full_refresh
    AND NOT EXISTS (
        SELECT TRUE AS is_exists
        FROM _customer_lifetime__monthly_agg_stg AS cm
        WHERE cm.month_date = t.month_date
            AND cm.customer_id = t.customer_id
            AND cm.activation_key = t.activation_key
            AND cm.vip_store_id = t.vip_store_id
        );

DELETE FROM analytics_base.customer_lifetime_value_monthly_agg_jfb AS t
WHERE NOT $is_full_refresh
    AND EXISTS (
        SELECT TRUE AS is_exists
        FROM stg.fact_activation AS fa
        WHERE fa.activation_key = t.activation_key
--            AND fa.meta_update_datetime > $wm_edw_stg_fact_activation
            AND fa.is_deleted
        );

DELETE FROM analytics_base.customer_lifetime_value_monthly_agg_jfb AS t
WHERE NOT $is_full_refresh
AND EXISTS (
                SELECT TRUE AS is_exists
                FROM _customer_lifetime__fso_orphan AS cm
                WHERE cm.month_date = t.month_date
                AND cm.customer_id = t.customer_id
                AND cm.activation_key = t.activation_key
                AND cm.vip_store_id = t.vip_store_id

        );

CREATE OR REPLACE TRANSIENT TABLE analytics_base.customer_lifetime_value_monthly_jfb as
SELECT
    c.month_date,
    c.customer_id,
    c.meta_original_customer_id,
    c.activation_key,
    c.vip_store_id,
    c.first_activation_key,
    c.gender,
    c.store_id,
    c.finance_specialty_store,
    c.guest_cohort_month_date,
    c.vip_cohort_month_date,
    c.membership_type,
    c.membership_type_entering_month,
    c.is_bop_vip,
    c.is_reactivated_vip,
    c.is_cancel,
    c.is_cancel_before_6th,
    c.is_passive_cancel,
    c.is_skip,
    c.is_snooze,
    c.is_login,
    c.segment_activity,
    c.is_merch_purchaser,
    c.is_successful_billing,
    c.is_pending_billing,
    c.is_failed_billing,
    c.is_cross_promo,
    c.is_retail_vip,
    c.is_scrubs_customer,
    c.customer_action_category,
    IFNULL(a.product_order_count, 0) AS product_order_count,
    IFNULL(a.online_product_order_count, 0) AS online_product_order_count,
    IFNULL(a.retail_product_order_count, 0) AS retail_product_order_count,
    IFNULL(a.mobile_app_product_order_count, 0) AS mobile_app_product_order_count,
    IFNULL(a.product_order_unit_count, 0) AS product_order_unit_count,
    IFNULL(a.online_product_order_unit_count, 0) AS online_product_order_unit_count,
    IFNULL(a.retail_product_order_unit_count, 0) AS retail_product_order_unit_count,
    IFNULL(a.mobile_app_product_order_unit_count, 0) AS mobile_app_product_order_unit_count,
    IFNULL(a.product_order_subtotal_excl_tariff_amount, 0) AS product_order_subtotal_excl_tariff_amount,
    IFNULL(a.product_order_product_subtotal_amount, 0) AS product_order_product_subtotal_amount,
    IFNULL(a.online_product_order_subtotal_excl_tariff_amount, 0) AS online_product_order_subtotal_excl_tariff_amount,
    IFNULL(a.retail_product_order_subtotal_excl_tariff_amount, 0) AS retail_product_order_subtotal_excl_tariff_amount,
    IFNULL(a.mobile_app_product_order_subtotal_excl_tariff_amount, 0) AS mobile_app_product_order_subtotal_excl_tariff_amount,
    IFNULL(a.product_order_product_discount_amount, 0) AS product_order_product_discount_amount,
    IFNULL(a.online_product_order_product_discount_amount, 0) AS online_product_order_product_discount_amount,
    IFNULL(a.retail_product_order_product_discount_amount, 0) AS retail_product_order_product_discount_amount,
    IFNULL(a.mobile_app_product_order_product_discount_amount, 0) AS mobile_app_product_order_product_discount_amount,
    IFNULL(a.product_order_shipping_revenue_amount, 0) AS product_order_shipping_revenue_amount,
    IFNULL(a.online_product_order_shipping_revenue_amount, 0) AS online_product_order_shipping_revenue_amount,
    IFNULL(a.retail_product_order_shipping_revenue_amount, 0) AS retail_product_order_shipping_revenue_amount,
    IFNULL(a.mobile_app_product_order_shipping_revenue_amount, 0) AS mobile_app_product_order_shipping_revenue_amount,
    IFNULL(a.product_order_direct_cogs_amount, 0) AS product_order_direct_cogs_amount,
    IFNULL(a.online_product_order_direct_cogs_amount, 0) AS online_product_order_direct_cogs_amount,
    IFNULL(a.retail_product_order_direct_cogs_amount, 0) AS retail_product_order_direct_cogs_amount,
    IFNULL(a.mobile_app_product_order_direct_cogs_amount, 0) AS mobile_app_product_order_direct_cogs_amount,
    IFNULL(a.product_order_selling_expenses_amount, 0) AS product_order_selling_expenses_amount,
    IFNULL(a.online_product_order_selling_expenses_amount, 0) AS online_product_order_selling_expenses_amount,
    IFNULL(a.retail_product_order_selling_expenses_amount, 0) AS retail_product_order_selling_expenses_amount,
    IFNULL(a.mobile_app_product_order_selling_expenses_amount, 0) AS mobile_app_product_order_selling_expenses_amount,
    IFNULL(a.product_order_cash_refund_amount_and_chargeback_amount, 0) AS product_order_cash_refund_amount_and_chargeback_amount,
    IFNULL(a.online_product_order_cash_refund_chargeback_amount, 0) AS online_product_order_cash_refund_chargeback_amount,
    IFNULL(a.retail_product_order_cash_refund_chargeback_amount, 0) AS retail_product_order_cash_refund_chargeback_amount,
    IFNULL(a.mobile_app_product_order_cash_refund_chargeback_amount, 0) AS mobile_app_product_order_cash_refund_chargeback_amount,
    IFNULL(a.product_order_cash_credit_refund_amount, 0) AS product_order_cash_credit_refund_amount,
    IFNULL(a.online_product_order_cash_credit_refund_amount, 0) AS online_product_order_cash_credit_refund_amount,
    IFNULL(a.retail_product_order_cash_credit_refund_amount, 0) AS retail_product_order_cash_credit_refund_amount,
    IFNULL(a.mobile_app_product_order_cash_credit_refund_amount, 0) AS mobile_app_product_order_cash_credit_refund_amount,
    IFNULL(a.product_order_reship_exchange_order_count, 0) AS product_order_reship_exchange_order_count,
    IFNULL(a.online_product_order_reship_exchange_order_count, 0) AS online_product_order_reship_exchange_order_count,
    IFNULL(a.retail_product_order_reship_exchange_order_count, 0) AS retail_product_order_reship_exchange_order_count,
    IFNULL(a.mobile_app_product_order_reship_exchange_order_count, 0) AS mobile_app_product_order_reship_exchange_order_count,
    IFNULL(a.product_order_reship_exchange_unit_count, 0) AS product_order_reship_exchange_unit_count,
    IFNULL(a.online_product_order_reship_exchange_unit_count, 0) AS online_product_order_reship_exchange_unit_count,
    IFNULL(a.retail_product_order_reship_exchange_unit_count, 0) AS retail_product_order_reship_exchange_unit_count,
    IFNULL(a.mobile_app_product_order_reship_exchange_unit_count, 0) AS mobile_app_product_order_reship_exchange_unit_count,
    IFNULL(a.product_order_reship_exchange_direct_cogs_amount, 0) AS product_order_reship_exchange_direct_cogs_amount,
    IFNULL(a.online_product_order_reship_exchange_direct_cogs_amount, 0) AS online_product_order_reship_exchange_direct_cogs_amount,
    IFNULL(a.retail_product_order_reship_exchange_direct_cogs_amount, 0) AS retail_product_order_reship_exchange_direct_cogs_amount,
    IFNULL(a.mobile_app_product_order_reship_exchange_direct_cogs_amount, 0) AS mobile_app_product_order_reship_exchange_direct_cogs_amount,
    IFNULL(a.product_order_return_cogs_amount, 0) AS product_order_return_cogs_amount,
    IFNULL(a.online_product_order_return_cogs_amount, 0) AS online_product_order_return_cogs_amount,
    IFNULL(a.retail_product_order_return_cogs_amount, 0) AS retail_product_order_return_cogs_amount,
    IFNULL(a.mobile_app_product_order_return_cogs_amount, 0) AS mobile_app_product_order_return_cogs_amount,
    IFNULL(a.product_order_return_unit_count, 0) AS product_order_return_unit_count,
    IFNULL(a.online_product_order_return_unit_count, 0) AS online_product_order_return_unit_count,
    IFNULL(a.retail_product_order_return_unit_count, 0) AS retail_product_order_return_unit_count,
    IFNULL(a.mobile_app_product_order_return_unit_count, 0) AS mobile_app_product_order_return_unit_count,
    IFNULL(a.product_order_amount_to_pay, 0) AS product_order_amount_to_pay,
    IFNULL(a.online_product_order_amount_to_pay, 0) AS online_product_order_amount_to_pay,
    IFNULL(a.retail_product_order_amount_to_pay, 0) AS retail_product_order_amount_to_pay,
    IFNULL(a.mobile_app_product_order_amount_to_pay, 0) AS mobile_app_product_order_amount_to_pay,
    IFNULL(a.product_gross_revenue_excl_shipping, 0) AS product_gross_revenue_excl_shipping,
    IFNULL(a.online_product_gross_revenue_excl_shipping, 0) AS online_product_gross_revenue_excl_shipping,
    IFNULL(a.retail_product_gross_revenue_excl_shipping, 0) AS retail_product_gross_revenue_excl_shipping,
    IFNULL(a.mobile_app_product_gross_revenue_excl_shipping, 0) AS mobile_app_product_gross_revenue_excl_shipping,
    IFNULL(a.product_gross_revenue, 0) AS product_gross_revenue,
    IFNULL(a.online_product_gross_revenue, 0) AS online_product_gross_revenue,
    IFNULL(a.retail_product_gross_revenue, 0) AS retail_product_gross_revenue,
    IFNULL(a.mobile_app_product_gross_revenue, 0) AS mobile_app_product_gross_revenue,
    IFNULL(a.product_net_revenue, 0) AS product_net_revenue,
    IFNULL(a.online_product_net_revenue, 0) AS online_product_net_revenue,
    IFNULL(a.retail_product_net_revenue, 0) AS retail_product_net_revenue,
    IFNULL(a.mobile_app_product_net_revenue, 0) AS mobile_app_product_net_revenue,
    IFNULL(a.product_margin_pre_return, 0) AS product_margin_pre_return,
    IFNULL(a.online_product_margin_pre_return, 0) AS online_product_margin_pre_return,
    IFNULL(a.retail_product_margin_pre_return, 0) AS retail_product_margin_pre_return,
    IFNULL(a.mobile_app_product_margin_pre_return, 0) AS mobile_app_product_margin_pre_return,
    IFNULL(a.product_margin_pre_return_excl_shipping, 0) AS product_margin_pre_return_excl_shipping,
    IFNULL(a.online_product_margin_pre_return_excl_shipping, 0) AS online_product_margin_pre_return_excl_shipping,
    IFNULL(a.retail_product_margin_pre_return_excl_shipping, 0) AS retail_product_margin_pre_return_excl_shipping,
    IFNULL(a.mobile_app_product_margin_pre_return_excl_shipping, 0) AS mobile_app_product_margin_pre_return_excl_shipping,
    IFNULL(a.product_gross_profit, 0) AS product_gross_profit,
    IFNULL(a.online_product_gross_profit, 0) AS online_product_gross_profit,
    IFNULL(a.retail_product_gross_profit, 0) AS retail_product_gross_profit,
    IFNULL(a.mobile_app_product_gross_profit, 0) AS mobile_app_product_gross_profit,
    IFNULL(a.product_variable_contribution_profit, 0) AS product_variable_contribution_profit,
    IFNULL(a.online_product_variable_contribution_profit, 0) AS online_product_variable_contribution_profit,
    IFNULL(a.retail_product_variable_contribution_profit, 0) AS retail_product_variable_contribution_profit,
    IFNULL(a.mobile_app_product_variable_contribution_profit, 0) AS mobile_app_product_variable_contribution_profit,
    IFNULL(a.product_order_cash_gross_revenue_amount, 0) AS product_order_cash_gross_revenue_amount,
    IFNULL(a.online_product_order_cash_gross_revenue_amount, 0) AS online_product_order_cash_gross_revenue_amount,
    IFNULL(a.retail_product_order_cash_gross_revenue_amount, 0) AS retail_product_order_cash_gross_revenue_amount,
    IFNULL(a.mobile_app_product_order_cash_gross_revenue_amount, 0) AS mobile_app_product_order_cash_gross_revenue_amount,
    IFNULL(a.product_order_cash_net_revenue, 0) AS product_order_cash_net_revenue,
    IFNULL(a.online_product_order_cash_net_revenue, 0) AS online_product_order_cash_net_revenue,
    IFNULL(a.retail_product_order_cash_net_revenue, 0) AS retail_product_order_cash_net_revenue,
    IFNULL(a.mobile_app_product_order_cash_net_revenue, 0) AS mobile_app_product_order_cash_net_revenue,
    IFNULL(a.product_order_landed_product_cost_amount, 0) AS product_order_landed_product_cost_amount,
    IFNULL(a.online_product_order_landed_product_cost_amount, 0) AS online_product_order_landed_product_cost_amount,
    IFNULL(a.retail_product_order_landed_product_cost_amount, 0) AS retail_product_order_landed_product_cost_amount,
    IFNULL(a.mobile_app_product_order_landed_product_cost_amount, 0) AS mobile_app_product_order_landed_product_cost_amount,
    IFNULL(a.product_order_cash_margin_pre_return, 0) AS product_order_cash_margin_pre_return,
    IFNULL(a.online_product_order_cash_margin_pre_return, 0) AS online_product_order_cash_margin_pre_return,
    IFNULL(a.retail_product_order_cash_margin_pre_return, 0) AS retail_product_order_cash_margin_pre_return,
    IFNULL(a.mobile_app_product_order_cash_margin_pre_return, 0) AS mobile_app_product_order_cash_margin_pre_return,
    IFNULL(a.product_order_cash_gross_profit, 0) AS product_order_cash_gross_profit,
    IFNULL(a.online_product_order_cash_gross_profit, 0) AS online_product_order_cash_gross_profit,
    IFNULL(a.retail_product_order_cash_gross_profit, 0) AS retail_product_order_cash_gross_profit,
    IFNULL(a.mobile_app_product_order_cash_gross_profit, 0) AS mobile_app_product_order_cash_gross_profit,
    IFNULL(a.billing_cash_gross_revenue, 0) AS billing_cash_gross_revenue,
    IFNULL(a.billing_cash_net_revenue, 0) AS billing_cash_net_revenue,
    IFNULL(a.cash_gross_revenue, 0) AS cash_gross_revenue,
    IFNULL(a.cash_net_revenue, 0) AS cash_net_revenue,
    IFNULL(a.cash_gross_profit, 0) AS cash_gross_profit,
    IFNULL(a.cash_variable_contribution_profit, 0) AS cash_variable_contribution_profit,
    IFNULL(a.monthly_billed_credit_cash_transaction_amount, 0) AS monthly_billed_credit_cash_transaction_amount,
    IFNULL(a.membership_fee_cash_transaction_amount, 0) AS membership_fee_cash_transaction_amount,
    IFNULL(a.gift_card_transaction_amount, 0) AS gift_card_transaction_amount,
    IFNULL(a.legacy_credit_cash_transaction_amount, 0) AS legacy_credit_cash_transaction_amount,
    IFNULL(a.monthly_billed_credit_cash_refund_count, 0) AS monthly_billed_credit_cash_refund_count,
    IFNULL(a.monthly_billed_credit_cash_refund_chargeback_amount, 0) AS monthly_billed_credit_cash_refund_chargeback_amount,
    IFNULL(a.membership_fee_cash_refund_chargeback_amount, 0) AS membership_fee_cash_refund_chargeback_amount,
    IFNULL(a.gift_card_cash_refund_chargeback_amount, 0) AS gift_card_cash_refund_chargeback_amount,
    IFNULL(a.legacy_credit_cash_refund_chargeback_amount, 0) AS legacy_credit_cash_refund_chargeback_amount,
    IFNULL(a.billed_cash_credit_issued_amount, 0) AS billed_cash_credit_issued_amount,
    IFNULL(a.billed_cash_credit_redeemed_amount, 0) AS billed_cash_credit_redeemed_amount,
    IFNULL(a.online_billed_cash_credit_redeemed_amount, 0) AS online_billed_cash_credit_redeemed_amount,
    IFNULL(a.retail_billed_cash_credit_redeemed_amount, 0) AS retail_billed_cash_credit_redeemed_amount,
    IFNULL(a.mobile_billed_cash_credit_redeemed_amount, 0) AS mobile_billed_cash_credit_redeemed_amount,
    IFNULL(a.billed_cash_credit_cancelled_amount, 0) AS billed_cash_credit_cancelled_amount,
    IFNULL(a.billed_cash_credit_expired_amount, 0) AS billed_cash_credit_expired_amount,
    IFNULL(a.billed_cash_credit_issued_equivalent_count, 0) AS billed_cash_credit_issued_equivalent_count,
    IFNULL(a.billed_cash_credit_redeemed_same_month_amount, 0) AS billed_cash_credit_redeemed_same_month_amount,
    IFNULL(a.billed_cash_credit_redeemed_equivalent_count, 0) AS billed_cash_credit_redeemed_equivalent_count,
    IFNULL(a.billed_cash_credit_cancelled_equivalent_count, 0) AS billed_cash_credit_cancelled_equivalent_count,
    IFNULL(a.billed_cash_credit_expired_equivalent_count, 0) AS billed_cash_credit_expired_equivalent_count,
    IFNULL(a.refund_cash_credit_issued_amount, 0) AS refund_cash_credit_issued_amount,
    IFNULL(a.refund_cash_credit_redeemed_amount, 0) AS refund_cash_credit_redeemed_amount,
    IFNULL(a.refund_cash_credit_cancelled_amount, 0) AS refund_cash_credit_cancelled_amount,
    IFNULL(a.refund_cash_credit_expired_amount, 0) AS refund_cash_credit_expired_amount,
    IFNULL(a.other_cash_credit_issued_amount, 0) AS other_cash_credit_issued_amount,
    IFNULL(a.other_cash_credit_redeemed_amount, 0) AS other_cash_credit_redeemed_amount,
    IFNULL(a.other_cash_credit_cancelled_amount, 0) AS other_cash_credit_cancelled_amount,
    IFNULL(a.other_cash_credit_expired_amount, 0) AS other_cash_credit_expired_amount,
    IFNULL(a.noncash_credit_issued_amount, 0) AS noncash_credit_issued_amount,
    IFNULL(a.noncash_credit_cancelled_amount, 0) AS noncash_credit_cancelled_amount,
    IFNULL(a.noncash_credit_expired_amount, 0) AS noncash_credit_expired_amount,
    IFNULL(a.first_guest_product_margin_pre_return, 0) AS first_guest_product_margin_pre_return,
    IFNULL(c.cumulative_cash_gross_profit, 0) AS cumulative_cash_gross_profit,
    IFNULL(c.cumulative_product_gross_profit, 0) AS cumulative_product_gross_profit,
    IFNULL(c.cumulative_cash_gross_profit_decile, 0) AS cumulative_cash_gross_profit_decile,
    IFNULL(c.cumulative_product_gross_profit_decile, 0) AS cumulative_product_gross_profit_decile,
    IFNULL(a.product_order_count_with_credit_redemption,0) AS product_order_count_with_credit_redemption,
    IFNULL(c.entering_month_unredeemed_credits_per_customer,0) AS entering_month_unredeemed_credits_per_customer,
    LEAST(COALESCE(c.meta_create_datetime, '9999-12-31'), COALESCE(a.meta_create_datetime, '9999-12-31')) AS meta_create_datetime,
    GREATEST(COALESCE(c.meta_update_datetime, '1900-01-01'), COALESCE(a.meta_update_datetime, '1900-01-01')) AS meta_update_datetime
FROM analytics_base.customer_lifetime_value_monthly_cust_jfb AS c
    LEFT JOIN analytics_base.customer_lifetime_value_monthly_agg_jfb AS a
        ON a.month_date = c.month_date
        AND a.customer_id = c.customer_id
        AND a.activation_key = c.activation_key
        AND a.vip_store_id = c.vip_store_id;


COMMIT; /* analytics_base.customer_lifetime_value_monthly_agg */

--  Success
--UPDATE stg.meta_table_dependency_watermark
--SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
--        (SELECT MAX(dt.max_meta_update_datetime) AS new_high_watermark_datetime
--            FROM (
--                SELECT MAX(meta_update_datetime) AS max_meta_update_datetime
--                FROM analytics_base.customer_lifetime_value_monthly_cust
--                UNION ALL
--                SELECT MAX(meta_update_datetime) AS max_meta_update_datetime
--                FROM analytics_base.customer_lifetime_value_monthly_agg
--                ) AS dt
--            )
--        ),
--    meta_update_datetime = CURRENT_TIMESTAMP()
--WHERE table_name = $target_table;
-- SELECT * FROM stg.meta_table_dependency_watermark WHERE table_name = $target_table;
