SET target_table = 'stg.lkp_order_classification';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);


MERGE INTO stg.meta_table_dependency_watermark AS w
USING (
    SELECT
        $target_table AS table_name,
        t.dependent_table_name,
        IFF(t.dependent_table_name IS NOT NULL, t.new_high_watermark_datetime, (
            SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime
            FROM stg.lkp_order_classification
        )) AS new_high_watermark_datetime
    FROM (
        SELECT -- For self table
            NULL AS dependent_table_name,
            NULL AS new_high_watermark_datetime
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant."ORDER"' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant."ORDER"
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.order_cancel' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.order_cancel
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.order_classification' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.order_classification
        UNION ALL
        SELECT
            'edw_prod.stg.lkp_membership_event' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM stg.lkp_membership_event
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.order_line' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.order_line
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.order_detail' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.order_detail
        UNION ALL
        SELECT
            'edw_prod.reference.test_customer' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM reference.test_customer
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.reship' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.reship
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.exchange' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.exchange
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.membership_billing' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_billing
        UNION ALL
        SELECT
            'lake_consolidated.ultra_merchant.gift_order' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.gift_order
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
SET wm_lake_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));
SET wm_lake_ultra_merchant_order_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_classification'));
SET wm_edw_reference_test_customer = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.test_customer'));
SET wm_edw_stg_lkp_membership_event = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.lkp_membership_event'));
SET wm_lake_ultra_merchant_order_line = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line'));
SET wm_lake_ultra_merchant_order_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_detail'));
SET wm_lake_ultra_merchant_reship = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.reship'));
SET wm_lake_ultra_merchant_exchange = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.exchange'));
SET wm_lake_ultra_merchant_membership_billing = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_billing'));
SET wm_lake_ultra_merchant_gift_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_order'));

--	Order Base Table
CREATE OR REPLACE TEMP TABLE _lkp_order_classification__base (order_id INT, meta_original_order_id INT);

-- Full Refresh
INSERT INTO _lkp_order_classification__base (order_id, meta_original_order_id)
SELECT o.order_id, meta_original_order_id AS meta_original_order_id
FROM lake_consolidated.ultra_merchant."ORDER" AS o
WHERE $is_full_refresh = TRUE
ORDER BY o.order_id;


-- Incremental Refresh
INSERT INTO _lkp_order_classification__base (order_id, meta_original_order_id)
SELECT DISTINCT incr.order_id, -1 AS meta_original_order_id
FROM (
    /* updated rows from self table */
    SELECT order_id
    FROM stg.lkp_order_classification
    WHERE meta_update_datetime > $wm_self
    UNION ALL
    /* new rows */
    SELECT order_id
    FROM lake_consolidated.ultra_merchant."ORDER"
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_order
    UNION ALL
    /* rows from dependent tables */
    SELECT order_id
    FROM lake_consolidated.ultra_merchant.order_classification
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_order_classification
    UNION ALL
    SELECT order_id
    FROM lake_consolidated.ultra_merchant.gift_order
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_gift_order
    UNION ALL
    SELECT o.order_id
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
        JOIN reference.test_customer AS tc
            ON tc.customer_id = o.customer_id
    WHERE tc.meta_update_datetime > $wm_edw_reference_test_customer
    UNION ALL
    SELECT o.order_id
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
        JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
            ON gfto.order_id = o.order_id
        JOIN reference.test_customer AS tc
            ON tc.customer_id = gfto.sender_customer_id
    WHERE tc.meta_update_datetime > $wm_edw_reference_test_customer
    UNION ALL
    SELECT o.order_id
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
        JOIN stg.lkp_membership_event AS lme
            ON lme.customer_id = o.customer_id
    WHERE lme.meta_update_datetime > $wm_edw_stg_lkp_membership_event
    UNION ALL
    SELECT o.order_id
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
        JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
            ON gfto.order_id = o.order_id
        JOIN stg.lkp_membership_event AS lme
            ON lme.customer_id = gfto.sender_customer_id
    WHERE lme.meta_update_datetime > $wm_edw_stg_lkp_membership_event
    UNION ALL
    SELECT order_id
    FROM lake_consolidated.ultra_merchant.order_line
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_order_line
    UNION ALL
    SELECT order_id
    FROM lake_consolidated.ultra_merchant.order_detail
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_order_detail
    UNION ALL
    SELECT order_id
    FROM lake_consolidated.ultra_merchant.membership_billing
    WHERE membership_billing_source_id IN (5,6)
    AND meta_update_datetime > $wm_lake_ultra_merchant_membership_billing
    UNION ALL
    SELECT order_id
    FROM lake_consolidated.ultra_merchant.gift_order
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_gift_order
    UNION ALL
    SELECT rshp.order_id
    FROM ( /* Need to capture two fields in same table */
        SELECT reship_order_id AS order_id
        FROM lake_consolidated.ultra_merchant.reship
        WHERE meta_update_datetime > $wm_lake_ultra_merchant_reship
        UNION ALL
        SELECT original_order_id AS order_id
        FROM lake_consolidated.ultra_merchant.reship
        WHERE meta_update_datetime > $wm_lake_ultra_merchant_reship
        ) AS rshp
    UNION ALL
    SELECT exch.order_id
    FROM ( /* Need to capture two fields in same table */
        SELECT exchange_order_id AS order_id
        FROM lake_consolidated.ultra_merchant.exchange
        WHERE meta_update_datetime > $wm_lake_ultra_merchant_exchange
        UNION ALL
        SELECT original_order_id AS order_id
        FROM lake_consolidated.ultra_merchant.exchange
        WHERE meta_update_datetime > $wm_lake_ultra_merchant_exchange
        ) AS exch
    UNION ALL
    SELECT order_id
    FROM excp.lkp_order_classification
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.order_id;
-- SELECT * FROM _lkp_order_classification__base;


/* if a child order from a split order is getting updated, we want to update the master order as well */
INSERT INTO _lkp_order_classification__base (order_id, meta_original_order_id)
SELECT DISTINCT o.master_order_id AS order_id,
                -1 AS meta_original_order_id
FROM _lkp_order_classification__base AS base
    JOIN lake_consolidated.ultra_merchant. "ORDER" AS o
        ON o.order_id = base.order_id
        AND o.master_order_id IS NOT NULL
WHERE $is_full_refresh = FALSE
AND NOT EXISTS (
    SELECT b.order_id
    FROM _lkp_order_classification__base AS b
    WHERE b.order_id = o.master_order_id
);

/* if a master got picked up to get updated, we want to update all the child orders it is tied to */
INSERT INTO _lkp_order_classification__base (order_id, meta_original_order_id)
SELECT DISTINCT o.order_id AS order_id,
                -1 AS meta_original_order_id
FROM _lkp_order_classification__base AS base
    JOIN lake_consolidated.ultra_merchant. "ORDER" AS o
        ON o.master_order_id = base.order_id
WHERE $is_full_refresh = FALSE
AND NOT EXISTS (
    SELECT b.order_id
    FROM _lkp_order_classification__base AS b
    WHERE b.order_id = o.order_id
);

UPDATE _lkp_order_classification__base b
SET b.meta_original_order_id = o.meta_original_order_id
FROM lake_consolidated.ultra_merchant."ORDER" o
WHERE b.order_id = o.order_id AND
      $is_full_refresh = FALSE;


-- migrated (historical) orders
CREATE OR REPLACE TEMP TABLE _lkp_order_classification__migrated_orders AS
SELECT oc.order_id
FROM _lkp_order_classification__base AS base
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = base.order_id
WHERE oc.order_type_id = 4; /* 'Historical' */

CREATE OR REPLACE TEMP TABLE _lkp_order_classification__order_base AS
SELECT
    o.order_id,
    o.meta_original_order_id,
    o.master_order_id,
    COALESCE(gfto.sender_customer_id, o.customer_id) AS customer_id,
    o.session_id,
    o.datetime_added,
    CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(
        IFF(mo.order_id IS NOT NULL, o.date_placed, o.datetime_added),
        'America/Los_Angeles')) AS order_local_datetime,
    o.payment_statuscode,
    COALESCE(o.datetime_shipped, o.date_shipped) AS datetime_shipped,
    o.processing_statuscode
FROM _lkp_order_classification__base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = o.store_id
    LEFT JOIN _lkp_order_classification__migrated_orders AS mo
        ON mo.order_id = o.order_id
    LEFT JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
        ON gfto.order_id = base.order_id
ORDER BY o.order_id;
-- SELECT * FROM _lkp_order_classification__order_base;

-- activating pre-2015 orders
CREATE OR REPLACE TEMP TABLE _lkp_order_classification__is_activating_pre_2015 (order_id number, is_activating boolean);
/*
-- only run for the historical load (WHERE MIN(high_watermark_datetime) < '2015-06-01')
--
-- we look at two signals:  membership datetime_activated, or a mod log record indicating level change to VIP
-- if membership datetime_activated = mod log datetime_added, we treat this as the same signal
-- for a given signal, row_num tells us which order is placed first
-- occasionally, an order will pick up two signals.  this will be handled in a subsequent step.
*/
INSERT INTO _lkp_order_classification__is_activating_pre_2015 (order_id, is_activating)
SELECT DISTINCT
    pre.order_id,
    pre.is_activating
FROM (
    SELECT
        o.order_id,
        1 AS is_activating,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id, COALESCE(mlog.datetime_added, m.datetime_activated)
            ORDER BY o.datetime_added) AS row_num
    FROM _lkp_order_classification__order_base AS base
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.order_id = base.order_id
        JOIN lake_consolidated.ultra_merchant.membership AS m
            ON m.customer_id = o.customer_id
        LEFT JOIN lake_consolidated.ultra_merchant.modification_log AS mlog
            ON mlog.object_id = m.meta_original_membership_id
            AND LOWER(mlog.object) = 'membership'
            AND LOWER(mlog.field) = 'membership_level_id'
            /* look for a mod log record indicating level change to VIP within 60 minutes of order datetime_added */
            AND DATEDIFF(second, o.datetime_added, mlog.datetime_added) BETWEEN -600 AND 3600
            /* old value was non-VIP */
            AND mlog.old_value IN (
                SELECT CAST(ml.membership_level_id AS varchar)
                FROM lake_consolidated.ultra_merchant.membership_level AS ml
                WHERE LOWER(ml.label) IN
                      ('member', 'purchasing member', 'purchasing member (previous active subscription)')
                )
            /* new value was VIP */
            AND mlog.new_value IN (
                SELECT CAST(ml.membership_level_id AS varchar)
                FROM lake_consolidated.ultra_merchant.membership_level AS ml
                WHERE LOWER(ml.label) IN ('classic vip', 'diamond elite vip', 'elite vip', 'vip')
                )
    WHERE o.datetime_added < '2015-01-01'
        AND /* either membership datetime_activated or membership level change was within 60 minutes of order datetime_added */
            ( /* there is a mod log record indicating membership level change to VIP within 60 minutes of order */
            mlog.modification_log_id IS NOT NULL
            OR /*membership datetime_activated is within 60 minutes of order */
            DATEDIFF(second, o.datetime_added, m.datetime_activated) BETWEEN -600 AND 3600
            )
        AND o.processing_statuscode NOT IN (2200, 2201, 2202, 2205) /* order is not cancelled */
    ) AS pre
WHERE pre.row_num = 1; /* only one order_id */

-- activating post-2015 orders
CREATE OR REPLACE TEMP TABLE _lkp_order_classification__is_activating_post_2015 AS
SELECT
    oc.order_id,
    COALESCE(MAX(IFF(oc.order_type_id = 23, 1, 0)), 0) AS is_membership_conversion_order,
    COALESCE(MAX(IFF(oc.order_type_id = 33, 1, 0)), 0) AS is_first_online_retail_order
FROM _lkp_order_classification__order_base AS base
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = base.order_id
WHERE order_type_id IN (23, 33)
GROUP BY oc.order_id;

CREATE OR REPLACE TEMP TABLE _lkp_order_classification__is_activating AS
SELECT
    base.order_id,
    base.master_order_id,
    CASE
        WHEN (post.is_membership_conversion_order = 1 OR post.is_first_online_retail_order = 1)
            --AND o.processing_statuscode NOT IN (2200, 2201, 2202, 2205) /* order is not cancelled */
            AND base.datetime_added >= '2015-01-01'
            THEN 1
        WHEN base.datetime_added >= '2015-01-01' THEN 0 /* only if the above doesn't match */
        ELSE COALESCE(pre.is_activating, 0) /* non-activating and activating pre-2015 */
    END AS is_activating
FROM _lkp_order_classification__order_base AS base
    LEFT JOIN _lkp_order_classification__is_activating_post_2015 AS post
        ON post.order_id = base.order_id
    LEFT JOIN _lkp_order_classification__is_activating_pre_2015 AS pre
        ON pre.order_id = base.order_id;

/* if split orders have an activating child order, then we are setting the master to activating */
UPDATE _lkp_order_classification__is_activating AS t
SET t.is_activating = s.is_activating
FROM (
        SELECT DISTINCT master_order_id AS order_id, is_activating
        FROM _lkp_order_classification__is_activating
        WHERE master_order_id IS NOT NULL
        AND is_activating = 1
     ) AS s
WHERE t.order_id = s.order_id;

CREATE OR REPLACE TEMP TABLE _lkp_order_classification__is_guest (
    order_id number,
    is_guest boolean,
    is_vip_membership_trial boolean
    );
INSERT INTO _lkp_order_classification__is_guest (
    order_id,
    is_guest,
    is_vip_membership_trial
    )
SELECT
    base.order_id,
    CASE
        WHEN ia.is_activating = 1 THEN 0
        WHEN LOWER(act.membership_type_detail) IN ('classic') THEN 1
        WHEN LOWER(act.membership_type_detail) IN ('monthly', 'annual') THEN 0
        WHEN act.membership_event_type = 'Activation' THEN 0
        ELSE 1 /* all anonymous orders need to be fall under E-Comm (according to legacy ecomm logic) */
    END AS is_guest,
    CASE WHEN act.membership_event_type = 'Free Trial Activation' THEN 1 ELSE 0 END AS is_vip_membership_trial
 FROM _lkp_order_classification__order_base AS base
   LEFT JOIN _lkp_order_classification__is_activating AS ia
        ON ia.order_id = base.order_id
    LEFT JOIN (
        SELECT
            ob.order_id,
            lme.membership_type_detail,
            lme.membership_event_type,
            ROW_NUMBER() OVER (PARTITION BY ob.order_id ORDER BY lme.event_local_datetime DESC) AS rnk
        FROM _lkp_order_classification__order_base AS ob
        JOIN stg.lkp_membership_event AS lme
            ON lme.customer_id = ob.customer_id
            AND lme.event_local_datetime <= ob.order_local_datetime
            AND IFNULL(lme.is_deleted, FALSE) = FALSE
        ) AS act
        ON act.order_id = base.order_id
        AND act.rnk = 1;


-- update online retail orders to is_guest
UPDATE _lkp_order_classification__is_guest AS g
SET g.is_guest = 1
FROM lake_consolidated.ultra_merchant.order_classification AS oc
WHERE oc.order_id = g.order_id
    AND oc.order_type_id IN (33, 32);

CREATE OR REPLACE TEMP TABLE _lkp_order_classification__mobile_app_store AS
SELECT sc.store_id
FROM lake_consolidated.ultra_merchant.store_classification AS sc
    JOIN lake_consolidated.ultra_merchant.store_type AS st
        ON st.store_type_id = sc.store_type_id
WHERE st.store_type_id = 8;

-- get original_order_ids
CREATE OR REPLACE TEMP TABLE _lkp_order_classification__original_order (
    order_id number,
    original_order_id number,
    is_exchange boolean,
    is_reship boolean,
    is_activating boolean,
    is_guest boolean,
    is_vip_membership_trial boolean
    );
INSERT INTO _lkp_order_classification__original_order (
    order_id,
    original_order_id,
    is_exchange,
    is_reship,
    is_activating,
    is_guest,
    is_vip_membership_trial
    )
SELECT
    base.order_id,
    r.original_order_id,
    FALSE AS is_exchange,
    TRUE AS is_reship,
    COALESCE(ia.is_activating, oc.is_activating, FALSE) AS is_activating,
    COALESCE(ig.is_guest, oc.is_guest, TRUE) AS is_guest,
    COALESCE(ig.is_vip_membership_trial, oc.is_vip_membership_trial, FALSE) AS is_vip_membership_trial
FROM _lkp_order_classification__order_base AS base
    JOIN lake_consolidated.ultra_merchant.reship AS r
        ON r.reship_order_id = base.order_id
    LEFT JOIN _lkp_order_classification__is_activating AS ia
        ON ia.order_id = r.original_order_id
    LEFT JOIN _lkp_order_classification__is_guest AS ig
        ON ig.order_id = r.original_order_id
    LEFT JOIN stg.lkp_order_classification AS oc
        ON oc.order_id = r.original_order_id
UNION ALL
SELECT
    base.order_id,
    e.original_order_id,
    TRUE AS is_exchange,
    FALSE AS is_reship,
    COALESCE(ia.is_activating, oc.is_activating, FALSE) AS is_activating,
    COALESCE(ig.is_guest, oc.is_guest, TRUE) AS is_guest,
    COALESCE(ig.is_vip_membership_trial, oc.is_vip_membership_trial, FALSE) AS is_vip_membership_trial
FROM _lkp_order_classification__order_base AS base
    JOIN lake_consolidated.ultra_merchant.exchange AS e
        ON e.exchange_order_id = base.order_id
    LEFT JOIN _lkp_order_classification__is_activating AS ia
        ON ia.order_id = e.original_order_id
    LEFT JOIN _lkp_order_classification__is_guest AS ig
        ON ig.order_id = e.original_order_id
    LEFT JOIN stg.lkp_order_classification AS oc
        ON oc.order_id = e.original_order_id;
-- SELECT * FROM _lkp_order_classification__original_order;
-- SELECT order_id, COUNT(1) FROM _lkp_order_classification__original_order GROUP BY 1 HAVING COUNT(1) > 1;

/* For split orders that were exchanged, we are going to assign them as guest/activating according to the master */
UPDATE _lkp_order_classification__original_order AS t
SET t.is_activating = s.is_activating,
    t.is_guest = s.is_guest
FROM (SELECT
            oo.order_id,
            COALESCE(ia.is_activating, oc.is_activating, FALSE) AS is_activating,
            COALESCE(ig.is_guest, oc.is_guest, TRUE) AS is_guest
      FROM _lkp_order_classification__original_order AS oo
               JOIN _lkp_order_classification__order_base AS ob
                    ON ob.order_id = oo.original_order_id
               LEFT JOIN _lkp_order_classification__is_activating AS ia
                    ON ia.order_id = ob.master_order_id
               LEFT JOIN _lkp_order_classification__is_guest AS ig
                    ON ig.order_id = ob.master_order_id
               LEFT JOIN stg.lkp_order_classification AS oc
                    ON oc.order_id = ob.master_order_id
      WHERE ob.master_order_id IS NOT NULL
      ) AS s
WHERE s.order_id = t.order_id
AND NOT EQUAL_NULL(s.is_activating,t.is_activating);

-- get order types
CREATE OR REPLACE TEMP TABLE _lkp_order_classification__sales_channel (
    order_id number,
    is_billing_order boolean,
    is_membership_fee boolean,
    is_exchange boolean,
    is_reship boolean,
    is_retail_order boolean,
    is_border_free_order boolean,
    is_ps_order boolean,
    is_retail_ship_only_order boolean,
    is_test_order boolean,
    is_preorder boolean,
    is_custom_order boolean,
    is_product_seeding_order boolean,
    is_bops_order boolean,
    is_selected_order_type boolean,
    is_legacy_credit boolean,
    is_membership_token boolean,
    is_gift_certificate boolean,
    is_mobile_app_order boolean,
    is_discreet_packaging boolean,
    is_amazon_order boolean,
    is_bill_me_now_online boolean,
    is_bill_me_now_gms boolean,
    is_membership_gift boolean,
    is_dropship boolean,
    is_third_party boolean,
    is_warehouse_outlet_order boolean,
    order_sales_channel varchar,
    order_classification_name varchar
    );
INSERT INTO _lkp_order_classification__sales_channel (
    order_id,
    is_billing_order,
    is_membership_fee,
    is_exchange,
    is_reship,
    is_retail_order,
    is_border_free_order,
    is_ps_order,
    is_retail_ship_only_order,
    is_test_order,
    is_preorder,
    is_custom_order,
    is_product_seeding_order,
    is_bops_order,
    is_selected_order_type,
    is_legacy_credit,
    is_membership_token,
    is_gift_certificate,
    is_mobile_app_order,
    is_discreet_packaging,
    is_amazon_order,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_membership_gift,
    is_dropship,
    is_third_party,
    is_warehouse_outlet_order,
    order_sales_channel,
    order_classification_name
    )
SELECT
    o.order_id,
    MAX(IFF(oc.order_type_id IN (10,39), 1, 0)) AS is_billing_order,
    MAX(IFF(oc.order_type_id = 9, 1, 0)) AS is_membership_fee,
    MAX(IFF(oc.order_type_id IN (11, 26), 1, 0)) AS is_exchange,
    MAX(IFF(oc.order_type_id = 6, 1, 0)) AS is_reship,
    MAX(IFF(oc.order_type_id = 19, 1, 0)) AS is_retail_order,
    MAX(IFF(oc.order_type_id = 32, 1, 0)) AS is_border_free_order,
    MAX(IFF(oc.order_type_id IN (25, 26), 1, 0)) AS is_ps_order,
    MAX(IFF(oc.order_type_id = 19 AND o.store_id = 52, 1, 0)) AS is_retail_ship_only_order,
    MAX(IFF(o.processing_statuscode = 2335 OR IFF(tc.customer_id IS NOT NULL, TRUE, FALSE), 1, 0)) AS is_test_order,
    MAX(IFF(oc.order_type_id = 36, 1, 0)) AS is_preorder,
    MAX(IFF(oc.order_type_id = 43, 1, 0)) AS is_custom_order,
    MAX(IFF(oc.order_type_id = 35, 1, 0)) AS is_product_seeding_order,
    MAX(IFF(oc.order_type_id = 40, 1, 0)) AS is_bops_order,
    MAX(IFF(oc.order_type_id IS NOT NULL, 1, 0)) AS is_selected_order_type,
    MAX(IFF(oc.order_type_id = 10 AND o.store_id = 55 AND subtotal = 9.95, 1, 0)) AS is_legacy_credit,
    MAX(IFF(oc.order_type_id = 39, 1, 0)) AS is_membership_token,
    MAX(IFF(ol.order_id IS NOT NULL, 1, 0)) AS is_gift_certificate,
    MAX(IFF(mas.store_id IS NOT NULL, 1, 0)) AS is_mobile_app_order,
    MAX(IFF(od.value IS NOT NULL, 1, 0)) AS is_discreet_packaging,
    MAX(IFF(oc.order_type_id IN (48,49), 1, 0)) AS is_amazon_order,
    MAX(IFF(mb.membership_billing_source_id = 5, 1, 0)) AS is_bill_me_now_online,
    MAX(IFF(mb.membership_billing_source_id = 6, 1, 0)) AS is_bill_me_now_gms,
    MAX(IFF(oc.order_type_id =50 OR (ol.order_id IS NOT NULL AND goc.order_id IS NOT NULL), 1, 0))  AS is_membership_gift,
    MAX(IFF(oc.order_type_id = 46, 1, 0)) AS is_dropship,
    MAX(IFF(oc.order_type_id = 51, 1, 0)) AS is_third_party,
    MAX(IFF(o.order_source_id = 9,1,0)) AS is_warehouse_outlet_order,
    CASE
        WHEN is_billing_order = 1 THEN 'Billing Order'
        WHEN is_membership_fee = 1 THEN 'Billing Order'
        WHEN is_gift_certificate = 1 THEN 'Billing Order'
        WHEN is_retail_order = 1 THEN 'Retail Order'
        WHEN is_mobile_app_order = 1 THEN 'Mobile App Order'
        ELSE 'Web Order'
    END AS order_sales_channel,
    CASE
        WHEN is_legacy_credit = 1 THEN 'Legacy Credit'
        WHEN is_membership_token = 1 THEN 'Token Billing'
        WHEN is_gift_certificate = 1 THEN 'Gift Certificate'
        WHEN is_billing_order = 1 THEN 'Credit Billing'
        WHEN is_exchange = 1 THEN 'Exchange'
        WHEN is_reship = 1 THEN 'Reship'
        WHEN is_membership_fee = 1 THEN 'Membership Fee'
        ELSE 'Product Order'
    END AS order_classification_name
FROM _lkp_order_classification__order_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
        ON o.order_id = base.order_id
    LEFT JOIN reference.test_customer tc
        ON o.customer_id = tc.customer_id
    LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = o.order_id
        AND oc.order_type_id IN (6, 9, 10, 11, 19, 25, 26, 32, 35, 36, 39, 40, 43, 46, 48, 49, 50, 51)
    LEFT JOIN lake_consolidated.ultra_merchant.order_line ol
        ON ol.order_id = o.order_id
        AND product_type_id IN (5, 20)
    LEFT JOIN lake_consolidated.ultra_merchant.order_detail od
        ON od.order_id = o.order_id AND
           LOWER(od.name) = 'discreet_packaging' AND
           LOWER(od.value) = 'true'
    LEFT JOIN lake_consolidated.ultra_merchant.membership_billing AS mb
        ON mb.order_id = o.order_id
            AND mb.membership_billing_source_id IN (5,6)
    LEFT JOIN _lkp_order_classification__mobile_app_store AS mas
        ON mas.store_id = o.store_id
    LEFT JOIN lake_consolidated.ultra_merchant.gift_order goc
        ON goc.order_id = o.order_id
GROUP BY o.order_id;
-- SELECT * FROM _lkp_order_classification__sales_channel;

/* BOPS logic for exchange orders. If original order was BOPS, then flag exchange order as BOPS */
UPDATE _lkp_order_classification__sales_channel as sc
SET sc.is_bops_order = TRUE
FROM (
    SELECT sc.order_id
    FROM _lkp_order_classification__sales_channel AS sc
    JOIN lake_consolidated.ultra_merchant.exchange AS e
        ON e.exchange_order_id = sc.order_id
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = e.original_order_id
    WHERE oc.order_type_id = 40
     ) as bops
WHERE bops.order_id = sc.order_id
    AND sc.is_bops_order = FALSE;

CREATE OR REPLACE TEMP TABLE _lkp_order_classification__order_status AS
SELECT DISTINCT
    o.order_id,
    sc.is_test_order,
    o.processing_statuscode AS order_processing_status_code,
    COALESCE(dopr.order_processing_status_key, -1) AS order_processing_status_key,
    dopr.order_processing_status,
    o.payment_statuscode AS order_payment_status_code,
    COALESCE(dopy.order_payment_status_key, -1) AS order_payment_status_key,
    dopy.order_payment_status,
    base.order_local_datetime,
    COALESCE(o.datetime_shipped, o.date_shipped) AS shipped_hq_datetime_ltz
FROM _lkp_order_classification__order_base AS base
    JOIN lake_consolidated.ultra_merchant."ORDER" AS o
		ON o.order_id = base.order_id
    LEFT JOIN stg.dim_order_processing_status AS dopr
		ON dopr.order_processing_status_code = o.processing_statuscode
		AND base.order_local_datetime BETWEEN dopr.effective_start_datetime and dopr.effective_end_datetime
    LEFT JOIN stg.dim_order_payment_status AS dopy
		ON dopy.order_payment_status_code = o.payment_statuscode
		AND base.order_local_datetime BETWEEN dopy.effective_start_datetime and dopy.effective_end_datetime
    LEFT JOIN _lkp_order_classification__sales_channel sc
        ON sc.order_id = base.order_id
ORDER BY o.order_id;

/* Assigning pre-order and dropship order status to the MASTER order */
CREATE OR REPLACE TEMP TABLE _lkp_order_classification__order_type AS
SELECT
    os.order_id,
    ot.label AS order_type
FROM _lkp_order_classification__order_status  AS os
    JOIN _lkp_order_classification__order_base AS ob
        ON ob.master_order_id = os.order_id
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = ob.order_id
    JOIN lake_consolidated.ultra_merchant.order_type AS ot
        ON ot.order_type_id = oc.order_type_id
WHERE ot.label IN ('Dropship Fulfillment', 'Preorder');


CREATE OR REPLACE TEMP TABLE _lkp_order_classification__pre_stg AS
SELECT DISTINCT
    base.order_id,
    base.meta_original_order_id,
    base.master_order_id,
    base.customer_id,
    base.session_id,
    base.order_local_datetime,
    CASE
        WHEN os.is_test_order = 1 THEN 'Test Order'
        WHEN ot.order_type = 'Dropship Fulfillment' THEN 'DropShip Split'
        WHEN os.shipped_hq_datetime_ltz IS NOT NULL THEN 'Success'
        WHEN os.order_payment_status IN ('Failed Authorization', 'Authorization Expired') THEN 'Failure'
        WHEN os.order_processing_status = 'Placement Failed' THEN 'Failure'
        WHEN os.order_processing_status IN ('Cancelled', 'Cancelled (Incomplete Auth Redirect)') THEN 'Cancelled'
        WHEN os.order_processing_status IN ('Placed', 'FulFillment (Batching)', 'FulFillment (In Progress)', 'Ready For Pickup', 'Hold (Manual Review - Group 1)')
            OR (sc.is_preorder = True AND os.order_processing_status ='Hold (Preorder)') THEN 'Pending'
        WHEN os.order_processing_status LIKE 'Hold%' THEN 'On Hold'
        WHEN os.order_processing_status IN ('Authorizing Payment', 'Initializing') THEN 'On Hold'
        WHEN os.order_processing_status IN ('Shipped', 'Complete') THEN 'Success'
        WHEN ot.order_type = 'Preorder' THEN 'Pre-Order Split'
        WHEN os.order_processing_status = 'Split (For BOPS)' THEN 'BOPS Split'
        WHEN os.order_processing_status = 'FulFillment (Failed Inventory Check)' THEN 'Cancelled'
        ELSE 'Unclassified'
        END AS order_status,
    IFF((sc.is_exchange OR sc.is_reship), COALESCE(oo.is_activating, ia.is_activating), ia.is_activating) AS is_activating,
    IFF((sc.is_exchange OR sc.is_reship), COALESCE(oo.is_guest, ig.is_guest), ig.is_guest) AS is_guest,
    IFF((sc.is_exchange OR sc.is_reship), COALESCE(oo.is_vip_membership_trial, ig.is_vip_membership_trial), ig.is_vip_membership_trial) AS is_vip_membership_trial,
    sc.is_billing_order,
    sc.is_membership_fee,
    sc.is_exchange,
    sc.is_reship,
    sc.is_retail_order,
    sc.is_border_free_order,
    sc.is_ps_order,
    sc.is_retail_ship_only_order,
    sc.is_test_order,
    sc.is_preorder,
    sc.is_custom_order,
    sc.is_product_seeding_order,
    sc.is_bops_order,
    sc.is_selected_order_type,
    sc.is_discreet_packaging,
    sc.is_amazon_order,
    sc.is_bill_me_now_online,
    sc.is_bill_me_now_gms,
    sc.is_membership_gift,
    sc.is_dropship,
    sc.is_third_party,
    sc.is_warehouse_outlet_order,
    sc.order_sales_channel,
    sc.order_classification_name,
    oo.original_order_id
FROM _lkp_order_classification__order_base AS base
    LEFT JOIN _lkp_order_classification__order_status AS os
        ON os.order_id = base.order_id
    LEFT JOIN _lkp_order_classification__is_activating AS ia
        ON ia.order_id = base.order_id
    LEFT JOIN _lkp_order_classification__is_guest AS ig
        ON ig.order_id = base.order_id
    LEFT JOIN _lkp_order_classification__sales_channel AS sc
        ON sc.order_id = base.order_id
    LEFT JOIN lake_consolidated.ultra_merchant.box AS box
        ON box.order_id = base.order_id
    LEFT JOIN _lkp_order_classification__original_order AS oo
        ON oo.order_id = base.order_id
    LEFT JOIN _lkp_order_classification__order_type AS ot
        ON ot.order_id = base.order_id
ORDER BY base.order_id;
-- SELECT * FROM _lkp_order_classification__pre_stg;

/* if the split drop ship CHILD order is third party, we're going to mark the master order as third pary */
UPDATE _lkp_order_classification__pre_stg AS t
SET t.is_third_party = TRUE
FROM (
        SELECT DISTINCT s.master_order_id AS order_id
        FROM _lkp_order_classification__pre_stg AS s
        WHERE s.is_third_party = TRUE
          AND s.master_order_id IS NOT NULL
    ) AS s
WHERE s.order_id = t.order_id;

/* for dropship split orders, we need to assign an order_status to the MASTER order.
   Order statuses are only tied to child orders in source
   We are creating a mapping table that ranks order_status values and assigning the highest ranking order status to the master
 */

CREATE OR REPLACE TEMP TABLE _order_status_rank (order_status_rank INT, order_status VARCHAR(25));
INSERT INTO _order_status_rank (order_status_rank, order_status)
VALUES (1,'Success'), (2, 'Pending'), (3, 'On Hold'), (4, 'Failure'), (5, 'Cancelled'), (6, 'Test Order'), (7, 'Unclassified');

UPDATE _lkp_order_classification__pre_stg AS t
SET t.order_status = s.order_status
FROM (SELECT DISTINCT ps.master_order_id AS order_id,  /* Assigning an order_status to the master order_id */
                      ps.order_status
      FROM _lkp_order_classification__pre_stg AS ps
               JOIN _lkp_order_classification__order_type ot
                    ON ps.master_order_id = ot.order_id
                    AND ot.order_type = 'Dropship Fulfillment'
               JOIN _order_status_rank AS osr
                    ON osr.order_status = ps.order_status
      QUALIFY RANK() OVER (PARTITION BY ps.master_order_id ORDER BY osr.order_status_rank) = 1

      UNION ALL
      /* Assigning the child orders to DropShip Split order status */
      SELECT a.order_id, b.order_status
      FROM _lkp_order_classification__order_base AS a
               JOIN _lkp_order_classification__pre_stg AS b
                    ON a.master_order_id = b.order_id
                    AND b.order_status = 'DropShip Split'
      ) AS s
WHERE t.order_id = s.order_id;

/*
Glitchy source data can have master split BOPS tie to different processing status codes.
Setting these to BOPS Split order_status
*/
UPDATE _lkp_order_classification__pre_stg AS target
SET target.order_status = source.order_status
FROM
(
    SELECT stg.order_id, os.order_status
    FROM _lkp_order_classification__pre_stg AS stg
    JOIN lake_consolidated.ultra_merchant.order_classification AS oc
        ON oc.order_id = stg.order_id
    CROSS JOIN (
        SELECT order_status
        FROM stg.dim_order_status
        WHERE order_status = 'BOPS Split'
        ) AS os
    WHERE stg.is_bops_order = TRUE
        AND oc.order_type_id = 37 /* Master Split */
        AND stg.order_status <> 'BOPS Split'
) AS source
WHERE source.order_id = target.order_id;

CREATE OR REPLACE TEMP TABLE _lkp_order_classification__stg AS
SELECT
    order_id,
    meta_original_order_id,
    customer_id,
    session_id,
    order_local_datetime,
    order_status,
    is_activating,
    is_guest,
    is_vip_membership_trial,
    is_billing_order,
    is_membership_fee,
    is_exchange,
    is_reship,
    is_retail_order,
    is_border_free_order,
    is_ps_order,
    is_retail_ship_only_order,
    is_test_order,
    is_preorder,
    is_custom_order,
    is_product_seeding_order,
    is_bops_order,
    is_selected_order_type,
    is_discreet_packaging,
    is_amazon_order,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_membership_gift,
    is_dropship,
    is_third_party,
    is_warehouse_outlet_order,
    order_sales_channel,
    order_classification_name,
    original_order_id,
    HASH(
        order_id,
        meta_original_order_id,
        customer_id,
        session_id,
        order_local_datetime,
        order_status,
        is_activating,
        is_guest,
        is_vip_membership_trial,
        is_billing_order,
        is_membership_fee,
        is_exchange,
        is_reship,
        is_retail_order,
        is_border_free_order,
        is_ps_order,
        is_retail_ship_only_order,
        is_test_order,
        is_preorder,
        is_custom_order,
        is_product_seeding_order,
        is_bops_order,
        is_selected_order_type,
        is_discreet_packaging,
        is_amazon_order,
        is_bill_me_now_online,
        is_bill_me_now_gms,
        is_membership_gift,
        is_dropship,
        is_third_party,
        is_warehouse_outlet_order,
        order_sales_channel,
        order_classification_name,
        original_order_id
    ) AS meta_row_hash,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _lkp_order_classification__pre_stg
ORDER BY order_id;
-- SELECT * FROM _lkp_order_classification__stg;
-- SELECT COUNT(1) FROM _lkp_order_classification__stg;
-- SELECT order_id, COUNT(1) FROM _lkp_order_classification__stg GROUP BY 1 HAVING COUNT(1) > 1;

UPDATE stg.lkp_order_classification AS t
SET --t.order_id = s.order_id,
    t.meta_original_order_id = s.meta_original_order_id,
    t.customer_id = s.customer_id,
    t.session_id = s.session_id,
    t.order_local_datetime = s.order_local_datetime,
    t.order_status = s.order_status,
    t.is_activating = s.is_activating,
    t.is_guest = s.is_guest,
    t.is_vip_membership_trial = s.is_vip_membership_trial,
    t.is_billing_order = s.is_billing_order,
    t.is_membership_fee = s.is_membership_fee,
    t.is_exchange = s.is_exchange,
    t.is_reship = s.is_reship,
    t.is_retail_order = s.is_retail_order,
    t.is_border_free_order = s.is_border_free_order,
    t.is_ps_order = s.is_ps_order,
    t.is_retail_ship_only_order = s.is_retail_ship_only_order,
    t.is_test_order = s.is_test_order,
    t.is_preorder = s.is_preorder,
    t.is_custom_order = s.is_custom_order,
    t.is_product_seeding_order = s.is_product_seeding_order,
    t.is_bops_order = s.is_bops_order,
    t.is_selected_order_type = s.is_selected_order_type,
    t.is_discreet_packaging = s.is_discreet_packaging,
    t.is_amazon_order = s.is_amazon_order,
    t.is_bill_me_now_online = s.is_bill_me_now_online,
    t.is_bill_me_now_gms = s.is_bill_me_now_gms,
    t.is_membership_gift = s.is_membership_gift,
    t.is_dropship = s.is_dropship,
    t.is_third_party = s.is_third_party,
    t.is_warehouse_outlet_order = s.is_warehouse_outlet_order,
    t.order_sales_channel = s.order_sales_channel,
    t.order_classification_name = s.order_classification_name,
    t.original_order_id = s.original_order_id,
    t.meta_row_hash = s.meta_row_hash,
    --t.meta_create_datetime = s.meta_create_datetime,
    t.meta_update_datetime = s.meta_update_datetime
FROM _lkp_order_classification__stg AS s
WHERE t.order_id = s.order_id
    AND t.meta_row_hash != s.meta_row_hash;
    --AND NOT EQUAL_NULL(s.meta_row_hash, s.meta_row_curr_hash);

INSERT INTO stg.lkp_order_classification (
    order_id,
    meta_original_order_id,
    customer_id,
    session_id,
    order_local_datetime,
    order_status,
    is_activating,
    is_guest,
    is_vip_membership_trial,
    is_billing_order,
    is_membership_fee,
    is_exchange,
    is_reship,
    is_retail_order,
    is_border_free_order,
    is_ps_order,
    is_retail_ship_only_order,
    is_test_order,
    is_preorder,
    is_custom_order,
    is_product_seeding_order,
    is_bops_order,
    is_selected_order_type,
    is_discreet_packaging,
    is_amazon_order,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_membership_gift,
    is_dropship,
    is_third_party,
    is_warehouse_outlet_order,
    order_sales_channel,
    order_classification_name,
    original_order_id,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    order_id,
    meta_original_order_id,
    customer_id,
    session_id,
    order_local_datetime,
    order_status,
    is_activating,
    is_guest,
    is_vip_membership_trial,
    is_billing_order,
    is_membership_fee,
    is_exchange,
    is_reship,
    is_retail_order,
    is_border_free_order,
    is_ps_order,
    is_retail_ship_only_order,
    is_test_order,
    is_preorder,
    is_custom_order,
    is_product_seeding_order,
    is_bops_order,
    is_selected_order_type,
    is_discreet_packaging,
    is_amazon_order,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_membership_gift,
    is_dropship,
    is_third_party,
    is_warehouse_outlet_order,
    order_sales_channel,
    order_classification_name,
    original_order_id,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
FROM _lkp_order_classification__stg AS s
WHERE order_id NOT IN (SELECT order_id FROM stg.lkp_order_classification)
ORDER BY order_id;

--  Success
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
        (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime FROM stg.lkp_order_classification)),
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
-- SELECT * FROM stg.meta_table_dependency_watermark WHERE table_name = $target_table;
