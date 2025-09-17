SET target_table = 'analytics_base.customer_lifetime_value_monthly_eu';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
ALTER SESSION SET QUERY_TAG = $target_table;

SET initial_warehouse = CURRENT_WAREHOUSE();
USE WAREHOUSE IDENTIFIER ('da_wh_adhoc_large'); /* Always use Large Warehouse for this transform */

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
SET is_full_refresh = TRUE; test
*/

MERGE INTO stg.meta_table_dependency_watermark AS w
USING (
    SELECT
        $target_table AS table_name,
        t.dependent_table_name,
        IFF(t.dependent_table_name IS NOT NULL, t.new_high_watermark_datetime, (
            SELECT MAX(dt.max_meta_update_datetime) AS new_high_watermark_datetime
            FROM (
                SELECT MAX(meta_update_datetime) AS max_meta_update_datetime
                FROM analytics_base.customer_lifetime_value_monthly_cust_eu
                UNION ALL
                SELECT MAX(meta_update_datetime) AS max_meta_update_datetime
                FROM analytics_base.customer_lifetime_value_monthly_agg_eu
                ) AS dt
        )) AS new_high_watermark_datetime
    FROM (
        SELECT -- For self table
            NULL AS dependent_table_name,
            NULL AS new_high_watermark_datetime
        UNION
        SELECT
            'edw_prod.analytics_base.finance_sales_ops_stg' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM analytics_base.finance_sales_ops_stg
        UNION
        SELECT
            'edw_prod.stg.fact_activation' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM stg.fact_activation
        UNION
        SELECT
            'edw_prod.stg.dim_customer' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM stg.dim_customer
        UNION
        SELECT
            'edw_prod.stg.dim_customer_detail_history' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM stg.dim_customer_detail_history
        UNION
        SELECT
            'edw_prod.stg.dim_order_membership_classification' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM stg.dim_order_membership_classification
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.membership' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.membership_skip' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_skip
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.membership_snooze_period' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_snooze_period
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.session' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.session
        ) AS t
    ORDER BY COALESCE(t.dependent_table_name, '')
    ) AS s
    ON w.table_name = s.table_name
    AND w.dependent_table_name IS NOT DISTINCT FROM s.dependent_table_name
WHEN NOT MATCHED THEN
    INSERT (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
        )
    VALUES (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01', -- current high_watermark_datetime
        s.new_high_watermark_datetime
        )
WHEN MATCHED AND w.new_high_watermark_datetime IS DISTINCT FROM s.new_high_watermark_datetime THEN
    UPDATE
    SET w.new_high_watermark_datetime = s.new_high_watermark_datetime,
        w.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_edw_analytics_base_finance_sales_ops_stg = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.analytics_base.finance_sales_ops_stg'));
SET wm_edw_stg_fact_activation = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_activation'));
SET wm_edw_stg_dim_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer'));
SET wm_edw_stg_dim_customer_detail_history = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_customer_detail_history'));
SET wm_edw_stg_dim_order_membership_classification = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_order_membership_classification'));
SET wm_lake_consolidated_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership'));
SET wm_lake_consolidated_ultra_merchant_membership_skip = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_skip'));
SET wm_lake_consolidated_ultra_merchant_membership_snooze_period = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_snooze_period'));
SET wm_lake_consolidated_ultra_merchant_session = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.session'));

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
    WHERE currency_object = 'local'
        AND date_object = 'placed'
        AND NOT is_deleted
    UNION ALL
    SELECT fa.customer_id as customer_id, CAST(fa.activation_local_datetime AS DATE) AS date
    FROM data_model.fact_activation as fa JOIN stg.dim_store  as ds
    on fa.store_id=ds.store_id WHERE ds.store_region in ('EU')
    ) AS src
WHERE $is_full_refresh = TRUE
GROUP BY src.customer_id
ORDER BY src.customer_id;

-- Incremental Refresh
INSERT INTO _customer_lifetime__customer_prebase (customer_id, min_month_date)
SELECT incr.customer_id, DATE_TRUNC(MONTH, MIN(incr.date)) AS min_month_date
FROM (
    SELECT customer_id, month_date AS date
    FROM analytics_base.customer_lifetime_value_monthly_cust_eu
    WHERE meta_update_datetime > $wm_self
    UNION ALL
    SELECT customer_id, month_date AS date
    FROM analytics_base.customer_lifetime_value_monthly_agg_eu
    WHERE meta_update_datetime > $wm_self
    UNION ALL
    SELECT fso.customer_id, fso.date
    FROM analytics_base.finance_sales_ops_stg AS fso
        LEFT JOIN stg.dim_order_membership_classification AS omc
            ON omc.order_membership_classification_key = fso.order_membership_classification_key
    WHERE (fso.meta_update_datetime > $wm_edw_analytics_base_finance_sales_ops_stg
            OR omc.meta_update_datetime > $wm_edw_stg_dim_order_membership_classification)
        AND fso.currency_object = 'local'
        AND fso.date_object = 'placed'
    UNION ALL
    SELECT fa.customer_id as customer_id, CAST(fa.activation_local_datetime AS DATE) AS date
    FROM data_model.fact_activation as fa JOIN stg.dim_store  as ds
    on fa.store_id=ds.store_id WHERE ds.store_region in ('EU')
    and fa.meta_update_datetime > $wm_edw_stg_fact_activation
    UNION ALL /* Select all customers and the minimum change date from updated rows in dependent tables */
    SELECT src.customer_id, MIN(src.date) AS date
    FROM (
        SELECT customer_id, CAST(meta_update_datetime AS DATE) AS date
        FROM data_model.dim_customer
        WHERE meta_update_datetime > $wm_edw_stg_dim_customer
        UNION ALL
        SELECT customer_id, CAST(meta_update_datetime AS DATE) AS date
        FROM data_model.dim_customer
        WHERE meta_update_datetime > $wm_edw_stg_dim_customer_detail_history
        UNION ALL
        SELECT m.customer_id, CAST(m.meta_update_datetime AS DATE) AS date
        FROM lake_consolidated.ultra_merchant.membership AS m
        WHERE m.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership
        UNION ALL
        SELECT m.customer_id, CAST(p.date_period_start AS DATE) AS date
        FROM lake_consolidated.ultra_merchant.membership_skip AS ms
            JOIN lake_consolidated.ultra_merchant.membership AS m
                ON m.membership_id = ms.membership_id
            JOIN lake_consolidated.ultra_merchant.period AS p
                ON p.period_id = ms.period_id
        WHERE ms.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership_skip
        UNION ALL
        SELECT m.customer_id, CAST(p.date_period_start AS DATE) AS date
        FROM lake_consolidated.ultra_merchant.membership_snooze_period AS msp
            JOIN lake_consolidated.ultra_merchant.membership AS m
                ON m.membership_id = msp.membership_id
            JOIN lake_consolidated.ultra_merchant.period AS p
                ON p.period_id = msp.period_id
        WHERE msp.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership_snooze_period
        UNION ALL
        SELECT s.customer_id, CAST(s.meta_update_datetime AS DATE) AS date
        FROM lake_consolidated.ultra_merchant.session AS s
        WHERE s.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_session
        ) AS src
    GROUP BY src.customer_id
    ) AS incr
WHERE NOT $is_full_refresh
GROUP BY incr.customer_id
ORDER BY incr.customer_id;
-- SELECT * FROM _customer_lifetime__customer_prebase;

SET entering_new_month = (SELECT IFF(DAYOFMONTH($execution_start_time::DATE) = 2, TRUE, FALSE));

/* Get distinct activation keys for customer in prebase going forward from their min_month_date */
CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_base AS
SELECT base.customer_id, base.activation_key, base.vip_store_id, base.month_date, MIN(mc.guest_cohort_month_date) AS guest_cohort_month_date
FROM (
    SELECT fso.customer_id, fso.activation_key, fso.vip_store_id, MIN(DATE_TRUNC(MONTH, fso.date)) AS month_date
    FROM analytics_base.finance_sales_ops_stg AS fso
        JOIN _customer_lifetime__customer_prebase AS clcp
            ON fso.customer_id = clcp.customer_id
            AND fso.date >= clcp.min_month_date
    WHERE fso.date_object = 'placed'
        AND fso.currency_object = 'local'
        AND fso.is_deleted = FALSE
    GROUP BY fso.customer_id, fso.activation_key, fso.vip_store_id

    UNION ALL

    SELECT fa.customer_id, fa.activation_key, fa.sub_store_id as vip_store_id, fa.vip_cohort_month_date AS month_date
    FROM data_model.fact_activation AS fa
        JOIN _customer_lifetime__customer_prebase AS clcp
            ON fa.customer_id = clcp.customer_id
            AND fa.vip_cohort_month_date >= clcp.min_month_date
        JOIN stg.dim_store  as ds
            ON fa.store_id=ds.store_id
            WHERE ds.store_region in ('EU')
    ) AS base
    LEFT JOIN analytics_base.customer_lifetime_value_monthly_cust_eu AS mc
        ON mc.customer_id = base.customer_id
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
FROM analytics_base.customer_lifetime_value_monthly_cust_eu AS mc
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
    fa.vip_cohort_month_date,
    fa.store_id,
    fa.sub_store_id,
    fa.cancellation_local_datetime,
    ROW_NUMBER() OVER (PARTITION BY stg.customer_id ORDER BY fa.activation_sequence_number, fa.activation_local_datetime) AS row_num
FROM (SELECT DISTINCT customer_id FROM _customer_lifetime__customer_base) AS stg
    JOIN data_model.fact_activation AS fa
      ON fa.customer_id = stg.customer_id
    JOIN stg.dim_store  as ds
      ON fa.store_id=ds.store_id
      WHERE ds.store_region in ('EU')
ORDER BY stg.customer_id;
-- SELECT * FROM _customer_lifetime__activation;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_month (
    month_date DATE,
    customer_id NUMBER(38, 0),
    activation_key NUMBER(38, 0),
    gender VARCHAR(75),
    store_id NUMBER(38, 0),
    vip_store_id NUMBER(38, 0),
    guest_cohort_month_date DATE,
    vip_cohort_month_date DATE,
    is_bop_vip BOOLEAN,
    vip_cohort_rank NUMBER(38, 0)
    );

/* Create customer records going till current month starting from their min month */
INSERT INTO _customer_lifetime__customer_month (
    month_date,
    customer_id,
    activation_key,
    gender,
    store_id,
    vip_store_id,
    guest_cohort_month_date,
    vip_cohort_month_date,
    is_bop_vip,
    vip_cohort_rank
    )
SELECT
    d.month_date,
    cb.customer_id,
    COALESCE(a.activation_key, -1) AS activation_key,
    c.gender,
    IFF(a.store_id IS NOT NULL AND a.store_id != -1, a.store_id, cb.vip_store_id) AS store_id,
    IFF(a.sub_store_id IS NOT NULL AND a.sub_store_id != -1, a.sub_store_id, cb.vip_store_id) AS vip_store_id_calc,
    IFF(a.activation_key != -1, '1900-01-01', COALESCE(cb.month_date, '1900-01-01')) AS guest_cohort_month_date,
    COALESCE(a.vip_cohort_month_date, '1900-01-01') AS vip_cohort_month_date,
    IFF(a.vip_cohort_month_date < d.month_date and a.cancellation_local_datetime::DATE >= d.month_date, TRUE, FALSE) AS is_bop_vip,
    ROW_NUMBER() OVER (PARTITION BY d.month_date, cb.customer_id, vip_store_id_calc ORDER BY COALESCE(a.vip_cohort_month_date, '1900-01-01') DESC) AS vip_cohort_rank
FROM _customer_lifetime__customer_base AS cb
    JOIN (SELECT DISTINCT month_date FROM stg.dim_date WHERE month_date < $execution_start_time::DATE AND is_current) AS d
        ON d.month_date >= cb.month_date
    JOIN data_model.dim_customer AS c
        ON c.customer_id = cb.customer_id
    LEFT JOIN _customer_lifetime__activation AS a
        ON a.activation_key = cb.activation_key
    LEFT JOIN _customer_lifetime__activation AS a1
        ON a1.customer_id = cb.customer_id
        AND a1.row_num = 1
        AND a.activation_key != -1
    LEFT JOIN data_model.dim_customer_detail_history AS cdh
        ON cdh.customer_id = cb.customer_id
        AND cdh.effective_start_datetime < d.month_date
        AND cdh.effective_end_datetime >= d.month_date;
-- SELECT * FROM _customer_lifetime__customer_month;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_skip AS
SELECT DISTINCT cm.month_date, cm.customer_id, cm.activation_key, TRUE AS is_skip
FROM _customer_lifetime__customer_month AS cm
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = cm.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_skip AS ms
        ON ms.membership_id = m.membership_id
    JOIN lake_consolidated.ultra_merchant.period AS p
        ON p.period_id = ms.period_id
WHERE cm.month_date = p.date_period_start
    AND cm.vip_cohort_rank = 1;
-- SELECT * FROM _customer_lifetime__customer_skip;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_snooze AS
SELECT DISTINCT cm.month_date, cm.customer_id, cm.activation_key, TRUE AS is_snooze
FROM _customer_lifetime__customer_month AS cm
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = cm.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_snooze_period AS msp
        ON msp.membership_id = m.membership_id
        AND (msp.membership_period_id IS NOT NULL OR msp.membership_billing_id IS NOT NULL)
    JOIN lake_consolidated.ultra_merchant.period AS p
        ON p.period_id = msp.period_id
WHERE cm.month_date = p.date_period_start
    AND cm.vip_cohort_rank = 1;
-- SELECT * FROM _customer_lifetime__customer_snooze;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_login AS
SELECT DISTINCT cm.month_date, cm.customer_id, cm.activation_key, TRUE AS is_login
FROM _customer_lifetime__customer_month AS cm
    JOIN lake_consolidated.ultra_merchant.session AS s
        ON s.customer_id = cm.customer_id
WHERE cm.month_date = DATE_TRUNC(MONTH, s.date_added)
    AND cm.vip_cohort_rank = 1;
-- SELECT * FROM _customer_lifetime__customer_login;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__customer_billing AS
SELECT
    cm.month_date,
    cm.customer_id,
    cm.activation_key
FROM _customer_lifetime__customer_month AS cm
    JOIN data_model.fact_order AS o
        ON o.customer_id = cm.customer_id
        AND o.activation_key = cm.activation_key
        AND DATE_TRUNC(MONTH, o.order_local_datetime::DATE) = cm.month_date
    JOIN data_model.dim_order_sales_channel AS osc
        ON osc.order_sales_channel_key = o.order_sales_channel_key
        AND osc.order_classification_l2 IN ('Credit Billing', 'Token Billing')
    JOIN data_model.dim_order_status AS os
        ON os.order_status_key = o.order_status_key
GROUP BY cm.month_date, cm.customer_id, cm.activation_key;
-- SELECT * FROM _customer_lifetime__customer_billing;


CREATE OR REPLACE TEMP TABLE _customer_lifetime__vip_store_id AS
SELECT COALESCE(b.store_id, a.store_id) as vip_store_id,
       a.store_id
FROM data_model.dim_store AS a
         LEFT JOIN data_model.dim_store AS b
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
    SUM(fso.product_gross_revenue) AS product_gross_revenue,
    SUM(fso.product_net_revenue) AS product_net_revenue,
    SUM(fso.product_margin_pre_return) AS product_margin_pre_return,
    SUM(fso.product_gross_profit) AS product_gross_profit,
    SUM(fso.cash_gross_revenue) AS cash_gross_revenue,
    SUM(fso.cash_net_revenue) AS cash_net_revenue,
    SUM(fso.cash_gross_profit) AS cash_gross_profit
FROM _customer_lifetime__customer_month AS cm
    JOIN analytics_base.finance_sales_ops_stg AS fso
        ON fso.customer_id = cm.customer_id
        AND fso.activation_key = cm.activation_key
        AND cm.vip_store_id = fso.vip_store_id
        AND DATE_TRUNC(MONTH, fso.date) = cm.month_date
    JOIN stg.dim_store AS st
        ON st.store_id = fso.store_id
    JOIN stg.dim_order_membership_classification AS omc
        ON omc.order_membership_classification_key = fso.order_membership_classification_key
WHERE fso.date_object = 'placed'
    AND fso.currency_object = 'local'
    AND NOT fso.is_deleted
    AND fso.date < CURRENT_DATE
GROUP BY
    cm.month_date,
    cm.customer_id,
    cm.activation_key,
    cm.vip_store_id;
-- SELECT * FROM _customer_lifetime__ltv_store_type;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__ltv AS
SELECT
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    SUM(product_gross_revenue) AS product_gross_revenue,
    SUM(product_net_revenue) AS product_net_revenue,
    SUM(product_margin_pre_return) AS product_margin_pre_return,
    SUM(product_gross_profit) AS product_gross_profit,
    SUM(cash_gross_revenue) AS cash_gross_revenue,
    SUM(cash_net_revenue) AS cash_net_revenue,
    SUM(cash_gross_profit) AS cash_gross_profit
FROM _customer_lifetime__ltv_store_type st
GROUP BY month_date, customer_id, activation_key,vip_store_id;
-- SELECT noncash_credit_expired_amount, * FROM _customer_lifetime__ltv WHERE customer_id IN (24249541, 28319266, 32605993, 37857325);

CREATE OR REPLACE TEMP TABLE _customer_lifetime__agg_data AS
SELECT
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    IFNULL(product_gross_revenue, 0) AS product_gross_revenue,
    IFNULL(product_net_revenue, 0) AS product_net_revenue,
    IFNULL(product_margin_pre_return, 0) AS product_margin_pre_return,
    IFNULL(product_gross_profit, 0) AS product_gross_profit,
    IFNULL(cash_gross_revenue, 0) AS cash_gross_revenue,
    IFNULL(cash_net_revenue, 0) AS cash_net_revenue,
    IFNULL(cash_gross_profit, 0) AS cash_gross_profit
FROM _customer_lifetime__ltv;
-- SELECT * FROM _customer_lifetime__agg_data;

CREATE OR REPLACE TEMP TABLE _customer_lifetime__monthly_agg_stg AS
SELECT
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    product_gross_revenue,
    product_net_revenue,
    product_margin_pre_return,
    product_gross_profit,
    cash_gross_revenue,
    cash_net_revenue,
    cash_gross_profit,
    HASH (
        month_date,
        customer_id,
        activation_key,
        vip_store_id,
        product_gross_revenue,
        product_net_revenue,
        product_margin_pre_return,
        product_gross_profit,
        cash_gross_revenue,
        cash_net_revenue,
        cash_gross_profit
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
    COALESCE(cm.gender, 'Unknown') AS gender,
    COALESCE(cm.store_id, -1) AS store_id,
    COALESCE(cm.vip_store_id, -1) AS vip_store_id,
    COALESCE(cm.guest_cohort_month_date, '1900-01-01') AS guest_cohort_month_date,
    COALESCE(cm.vip_cohort_month_date, '1900-01-01') AS vip_cohort_month_date,
    COALESCE(cm.is_bop_vip, FALSE) AS is_bop_vip
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
    LEFT JOIN _customer_lifetime__customer_billing AS cb
        ON cb.month_date = cm.month_date
        AND cb.customer_id = cm.customer_id
        AND cb.activation_key = cm.activation_key
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
    cd.vip_cohort_month_date
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
    clvmc.vip_store_id
FROM analytics_base.customer_lifetime_value_monthly_cust_eu AS clvmc
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
/*
UPDATE _customer_lifetime__cumulative AS stg
SET
    stg.cumulative_product_gross_profit = stg.cumulative_product_gross_profit + clm.last_month_cumulative_product_gross_profit,
    stg.cumulative_cash_gross_profit = stg.cumulative_cash_gross_profit + clm.last_month_cumulative_cash_gross_profit
FROM _customer_lifetime__cumulative_last_month AS clm
WHERE clm.customer_id = stg.customer_id
    AND clm.activation_key = stg.activation_key
    AND clm.vip_store_id = stg.vip_store_id;
*/
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
    'ltv' AS source
FROM analytics_base.customer_lifetime_value_monthly_cust_eu
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
    source
FROM _customer_lifetime__cumulative_all_customers
WHERE activation_key != -1
UNION ALL
SELECT
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    source
FROM _customer_lifetime__cumulative_all_customers
WHERE activation_key = -1;
-- SELECT * FROM _customer_lifetime__decile_calc;

/* If a customer's decile did not change, remove them from the result set as we don't need to process them again
DELETE FROM _customer_lifetime__decile_calc
WHERE current_cumulative_cash_gross_profit_decile = cumulative_cash_gross_profit_decile
    AND current_cumulative_product_gross_profit_decile = cumulative_product_gross_profit_decile
    AND source = 'ltv';
-- SELECT COUNT(1) FROM _customer_lifetime__decile_calc;
*/

CREATE OR REPLACE TEMP TABLE _customer_lifetime__monthly_cust_stg AS
SELECT
    month_date,
    customer_id,
    activation_key,
    gender,
    store_id,
    vip_store_id,
    guest_cohort_month_date,
    vip_cohort_month_date,
    is_bop_vip,
    HASH (
        month_date,
        customer_id,
        activation_key,
        gender,
        store_id,
        guest_cohort_month_date,
        vip_cohort_month_date,
        is_bop_vip
        ) AS meta_row_hash,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM (
    SELECT
        cd.month_date,
        cd.customer_id,
        cd.activation_key,
        cd.gender,
        cd.store_id,
        cd.vip_store_id,
        cd.guest_cohort_month_date,
        cd.vip_cohort_month_date,
        cd.is_bop_vip,
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
        mc.activation_key,
        mc.gender,
        mc.store_id,
        mc.vip_store_id,
        mc.guest_cohort_month_date,
        mc.vip_cohort_month_date,
        mc.is_bop_vip,
        dc.source
    FROM analytics_base.customer_lifetime_value_monthly_cust_eu AS mc
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
AND fso.meta_update_datetime > $wm_edw_analytics_base_finance_sales_ops_stg;

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
            AND fso.currency_object = 'local'
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
UPDATE analytics_base.customer_lifetime_value_monthly_cust_eu AS t
SET
    --t.month_date = s.month_date
    --t.customer_id = s.customer_id
    --t.activation_key = s.activation_key
    t.gender = s.gender,
    t.store_id = s.store_id,
    t.guest_cohort_month_date = s.guest_cohort_month_date,
    t.vip_cohort_month_date = s.vip_cohort_month_date,
    t.is_bop_vip = s.is_bop_vip,
    t.meta_row_hash = s.meta_row_hash,
    --t.meta_create_datetime = s.meta_create_datetime,
    t.meta_update_datetime = s.meta_update_datetime
FROM _customer_lifetime__monthly_cust_stg AS s
WHERE s.month_date = t.month_date
    AND s.customer_id = t.customer_id
    AND s.activation_key = t.activation_key
    AND s.meta_row_hash != t.meta_row_hash
    AND s.vip_store_id = t.vip_store_id;

/* For new customers, insert their values into analytics_base.customer_lifetime_value_monthly_cust */
INSERT INTO analytics_base.customer_lifetime_value_monthly_cust_eu (
    month_date,
    customer_id,
    activation_key,
    gender,
    store_id,
    vip_store_id,
    guest_cohort_month_date,
    vip_cohort_month_date,
    is_bop_vip,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    s.month_date,
    s.customer_id,
    s.activation_key,
    s.gender,
    s.store_id,
    s.vip_store_id,
    s.guest_cohort_month_date,
    s.vip_cohort_month_date,
    s.is_bop_vip,
    s.meta_row_hash,
    s.meta_create_datetime,
    s.meta_update_datetime
FROM _customer_lifetime__monthly_cust_stg AS s
    LEFT JOIN analytics_base.customer_lifetime_value_monthly_cust_eu AS t
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
DELETE FROM analytics_base.customer_lifetime_value_monthly_cust_eu AS t
WHERE $is_full_refresh
    AND NOT EXISTS (
        SELECT TRUE AS is_exists
        FROM _customer_lifetime__customer_month AS cm
        WHERE cm.month_date = t.month_date
            AND cm.customer_id = t.customer_id
            AND cm.activation_key = t.activation_key
            AND cm.vip_store_id = t.vip_store_id
        );

DELETE FROM analytics_base.customer_lifetime_value_monthly_cust_eu AS t
WHERE NOT $is_full_refresh
    AND EXISTS (
        SELECT TRUE AS is_exists
        FROM stg.fact_activation AS fa
        WHERE fa.activation_key = t.activation_key
            AND fa.meta_update_datetime > $wm_edw_stg_fact_activation
            AND fa.is_deleted
        );

DELETE FROM analytics_base.customer_lifetime_value_monthly_cust_eu AS t
WHERE NOT $is_full_refresh
AND EXISTS (
                SELECT TRUE AS is_exists
                FROM _customer_lifetime__fso_orphan AS cm
                WHERE cm.month_date = t.month_date
                AND cm.customer_id = t.customer_id
                AND cm.activation_key = t.activation_key
                AND cm.vip_store_id = t.vip_store_id

        );
COMMIT; /* analytics_base.customer_lifetime_value_monthly_cust */

BEGIN TRANSACTION; /* analytics_base.customer_lifetime_value_monthly_agg */

/* For customers that had an aggregation change, update their values on analytics_base.customer_lifetime_value_monthly_agg */
UPDATE analytics_base.customer_lifetime_value_monthly_agg_eu AS t
SET
    --t.month_date = s.month_date
    --t.customer_id = s.customer_id
    --t.activation_key = s.activation_key
    t.product_gross_revenue = s.product_gross_revenue,
    t.product_net_revenue = s.product_net_revenue,
    t.product_margin_pre_return = s.product_margin_pre_return,
    t.product_gross_profit = s.product_gross_profit,
    t.cash_gross_revenue = s.cash_gross_revenue,
    t.cash_net_revenue = s.cash_net_revenue,
    t.cash_gross_profit = s.cash_gross_profit,
    t.meta_row_hash = s.meta_row_hash,
    --t.meta_create_datetime = s.meta_create_datetime,
    t.meta_update_datetime = s.meta_update_datetime
FROM _customer_lifetime__monthly_agg_stg AS s
WHERE s.month_date = t.month_date
    AND s.customer_id = t.customer_id
    AND s.activation_key = t.activation_key
    AND s.meta_row_hash != t.meta_row_hash
    AND s.vip_store_id = t.vip_store_id;

/* For new customers, insert their values into analytics_base.customer_lifetime_value_monthly_agg */
INSERT INTO analytics_base.customer_lifetime_value_monthly_agg_eu (
    month_date,
    customer_id,
    activation_key,
    vip_store_id,
    product_gross_revenue,
    product_net_revenue,
    product_margin_pre_return,
    product_gross_profit,
    cash_gross_revenue,
    cash_net_revenue,
    cash_gross_profit,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    s.month_date,
    s.customer_id,
    s.activation_key,
    s.vip_store_id,
    s.product_gross_revenue,
    s.product_net_revenue,
    s.product_margin_pre_return,
    s.product_gross_profit,
    s.cash_gross_revenue,
    s.cash_net_revenue,
    s.cash_gross_profit,
    s.meta_row_hash,
    s.meta_create_datetime,
    s.meta_update_datetime
FROM _customer_lifetime__monthly_agg_stg AS s
    LEFT JOIN analytics_base.customer_lifetime_value_monthly_agg_eu AS t
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
DELETE FROM analytics_base.customer_lifetime_value_monthly_agg_eu AS t
WHERE $is_full_refresh
    AND NOT EXISTS (
        SELECT TRUE AS is_exists
        FROM _customer_lifetime__monthly_agg_stg AS cm
        WHERE cm.month_date = t.month_date
            AND cm.customer_id = t.customer_id
            AND cm.activation_key = t.activation_key
            AND cm.vip_store_id = t.vip_store_id
        );

DELETE FROM analytics_base.customer_lifetime_value_monthly_agg_eu AS t
WHERE NOT $is_full_refresh
    AND EXISTS (
        SELECT TRUE AS is_exists
        FROM stg.fact_activation AS fa
        WHERE fa.activation_key = t.activation_key
            AND fa.meta_update_datetime > $wm_edw_stg_fact_activation
            AND fa.is_deleted
        );

DELETE FROM analytics_base.customer_lifetime_value_monthly_agg_eu AS t
WHERE NOT $is_full_refresh
AND EXISTS (
                SELECT TRUE AS is_exists
                FROM _customer_lifetime__fso_orphan AS cm
                WHERE cm.month_date = t.month_date
                AND cm.customer_id = t.customer_id
                AND cm.activation_key = t.activation_key
                AND cm.vip_store_id = t.vip_store_id

        );

COMMIT; /* analytics_base.customer_lifetime_value_monthly_agg */

--  Success
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
        (SELECT MAX(dt.max_meta_update_datetime) AS new_high_watermark_datetime
            FROM (
                SELECT MAX(meta_update_datetime) AS max_meta_update_datetime
                FROM analytics_base.customer_lifetime_value_monthly_cust_eu
                UNION ALL
                SELECT MAX(meta_update_datetime) AS max_meta_update_datetime
                FROM analytics_base.customer_lifetime_value_monthly_agg_eu
                ) AS dt
            )
        ),
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
-- SELECT * FROM stg.meta_table_dependency_watermark WHERE table_name = $target_table;
