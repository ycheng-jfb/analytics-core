SET target_table = 'stg.lkp_membership_event_type';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

/*
-- Initial load
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

MERGE INTO stg.meta_table_dependency_watermark AS w
USING (
    SELECT
        $target_table AS table_name,
        t.dependent_table_name,
        IFF(t.dependent_table_name IS NOT NULL, t.new_high_watermark_datetime, (
            SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime
            FROM stg.lkp_membership_event_type
        )) AS new_high_watermark_datetime
    FROM (
        SELECT -- For self table
            NULL AS dependent_table_name,
            NULL AS new_high_watermark_datetime
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.membership' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership
        UNION
        SELECT
            'lake_consolidated.ultra_merchant."ORDER"' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant."ORDER"
        UNION
        SELECT
            'lake_consolidated.ultra_merchant_history.membership' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant_history.membership
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.customer' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.customer
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.customer_link' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.customer_link
        UNION
         SELECT
            'lake_consolidated.ultra_merchant.membership_brand_activation' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_brand_activation
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.membership_plan_membership_brand' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_plan_membership_brand
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.membership_brand_signup' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_brand_signup
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.membership_passive_cancels' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_passive_cancels
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.modification_log' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.modification_log
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.statuscode_modification_log' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.statuscode_modification_log
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.membership_level_group_modification_log' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_level_group_modification_log
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.gift_order' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.gift_order
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.membership_trial' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.membership_trial
        UNION
        SELECT
            'lake_consolidated.ultra_merchant.order_classification' AS dependent_table_name,
            MAX(meta_update_datetime) AS new_high_watermark_datetime
        FROM lake_consolidated.ultra_merchant.order_classification
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
SET wm_lake_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership'));
SET wm_lake_history_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.membership'));
SET wm_lake_ultra_merchant_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer'));
SET wm_lake_ultra_merchant_customer_link = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer_link'));
SET wm_lake_ultra_merchant_modification_log = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.modification_log'));
SET wm_lake_ultra_merchant_statuscode_modification_log = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.statuscode_modification_log'));
SET wm_lake_ultra_merchant_membership_brand_activation = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_brand_activation'));
SET wm_lake_ultra_merchant_membership_brand_signup = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_brand_signup'));
SET wm_lake_ultra_merchant_membership_plan_membership_brand = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_plan_membership_brand'));
SET wm_lake_ultra_merchant_membership_passive_cancels = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_passive_cancels'));
SET wm_lake_ultra_merchant_membership_level_group_modification_log = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_level_group_modification_log'));
SET wm_lake_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));
SET wm_lake_ultra_merchant_gift_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_order'));
SET wm_lake_ultra_merchant_membership_trial = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_trial'));
SET wm_lake_ultra_merchant_order_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_classification'));

/*
SELECT
    $wm_self,
    $wm_lake_ultra_merchant_membership,
    $wm_lake_ultra_merchant_customer,
    $wm_lake_ultra_merchant_customer_link,
    $wm_lake_ultra_merchant_modification_log,
    $wm_lake_ultra_merchant_statuscode_modification_log,
    $wm_lake_ultra_merchant_membership_passive_cancels,
    $wm_lake_ultra_merchant_membership_level_group_modification_log;
*/
--	Customer Base Table
CREATE OR REPLACE TEMP TABLE _lkp_membership_event_type__base (customer_id INT, membership_id INT);

-- Full Refresh
INSERT INTO _lkp_membership_event_type__base (customer_id)
SELECT DISTINCT m.customer_id
FROM lake_consolidated.ultra_merchant.membership AS m
WHERE $is_full_refresh = TRUE
ORDER BY m.customer_id;

-- Incremental Refresh
INSERT INTO _lkp_membership_event_type__base (customer_id)
SELECT DISTINCT m.customer_id
FROM (
    /* Check for dependency table updates */
    SELECT customer_id
    FROM lake_consolidated.ultra_merchant.membership
    WHERE meta_update_datetime > $wm_lake_ultra_merchant_membership
    UNION ALL
    SELECT o.customer_id
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
        LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
            ON oc.order_id = o.order_id
    WHERE o.meta_update_datetime > $wm_lake_ultra_merchant_order
        OR oc.meta_update_datetime > $wm_lake_ultra_merchant_order_classification
    UNION ALL
    SELECT gfto.sender_customer_id AS customer_id
    FROM lake_consolidated.ultra_merchant.gift_order AS gfto
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.order_id = gfto.order_id
    WHERE o.meta_update_datetime > $wm_lake_ultra_merchant_order
        OR gfto.meta_update_datetime > $wm_lake_ultra_merchant_gift_order
    UNION ALL
    SELECT gfto.recipient_customer_id AS customer_id
    FROM lake_consolidated.ultra_merchant.gift_order AS gfto
    WHERE gfto.recipient_customer_id IS NOT NULL
    AND gfto.meta_update_datetime > $wm_lake_ultra_merchant_gift_order
    UNION ALL
    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership_trial AS mt
        JOIN lake_consolidated.ultra_merchant.membership AS m
            ON m.membership_id = mt.membership_id
    WHERE mt.meta_update_datetime > $wm_lake_ultra_merchant_membership_trial
    UNION ALL
    SELECT customer_id
    FROM lake_consolidated.ultra_merchant_history.membership
    WHERE meta_update_datetime > $wm_lake_history_ultra_merchant_membership
    UNION ALL
    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership AS m
        LEFT JOIN lake_consolidated.ultra_merchant.customer AS c
            ON c.customer_id = m.customer_id
            AND c.statuscode = 1502
        LEFT JOIN lake_consolidated.ultra_merchant.customer_link AS cl
            ON cl.current_customer_id = m.customer_id
            AND DATEDIFF(SECOND, cl.datetime_added, m.datetime_added) BETWEEN -15 AND 15
        LEFT JOIN lake_consolidated.ultra_merchant.modification_log AS ml
            ON ml.object_id = m.meta_original_membership_id
        LEFT JOIN lake_consolidated.ultra_merchant.statuscode_modification_log AS sml
            ON sml.object_id = m.meta_original_membership_id
            AND LOWER(sml.object) = 'membership'
	        AND sml.to_statuscode IN (3940, 3941, 3930)
        LEFT JOIN lake_consolidated.ultra_merchant.membership_passive_cancels AS mpc
            ON mpc.membership_id = m.membership_id
        LEFT JOIN lake_consolidated.ultra_merchant.membership_level_group_modification_log AS mlgml
            ON mlgml.membership_id = m.membership_id
            AND mlgml.passive_downgrade = 1
	        AND DATEDIFF(SECOND, mlgml.datetime_added, ml.datetime_added) BETWEEN -10 AND 10
    WHERE (c.meta_update_datetime > $wm_lake_ultra_merchant_customer
        OR cl.meta_update_datetime > $wm_lake_ultra_merchant_customer_link
        OR ml.meta_update_datetime > $wm_lake_ultra_merchant_modification_log
        OR sml.meta_update_datetime > $wm_lake_ultra_merchant_statuscode_modification_log
        OR mpc.meta_update_datetime > $wm_lake_ultra_merchant_membership_passive_cancels
        OR mlgml.meta_update_datetime > $wm_lake_ultra_merchant_membership_level_group_modification_log)
    UNION ALL
    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership_brand_signup AS mbs
        JOIN lake_consolidated.ultra_merchant.membership AS m
            ON m.membership_id = mbs.membership_id
        JOIN lake_consolidated.ultra_merchant.membership_plan_membership_brand AS mpmb
            ON mbs.membership_brand_id = mpmb.membership_brand_id
    WHERE mbs.meta_update_datetime > $wm_lake_ultra_merchant_membership_brand_signup
    OR mpmb.meta_update_datetime > $wm_lake_ultra_merchant_membership_plan_membership_brand
    UNION ALL
    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership_brand_activation AS mba
        JOIN lake_consolidated.ultra_merchant.membership AS m
            ON m.membership_id = mba.membership_id
        JOIN lake_consolidated.ultra_merchant.membership_plan_membership_brand AS mpmb
            ON mpmb.membership_brand_id = mba.membership_brand_id
    WHERE mba.meta_update_datetime > $wm_lake_ultra_merchant_membership_brand_activation
    OR mpmb.meta_update_datetime > $wm_lake_ultra_merchant_membership_plan_membership_brand
    UNION ALL
    -- excp table to process records manuall
    SELECT customer_id
    FROM excp.lkp_membership_event_type
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS m
WHERE NOT $is_full_refresh
ORDER BY m.customer_id;

-- if there's a reactivated lead in the base table, then we also want to refresh the deactivated lead account
INSERT INTO _lkp_membership_event_type__base (customer_id)
SELECT DISTINCT cl.original_customer_id
FROM _lkp_membership_event_type__base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.customer_link AS cl
        ON cl.current_customer_id = m.customer_id
WHERE $is_full_refresh = FALSE
AND cl.original_customer_id NOT IN (SELECT customer_id FROM _lkp_membership_event_type__base)
AND DATEDIFF(SECOND, cl.datetime_added, m.datetime_added) BETWEEN -15 AND 15;

-- Switch warehouse if the delta is more than medium warehouse can handle
SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', CURRENT_WAREHOUSE()) FROM _lkp_membership_event_type__base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

-- Add membership_id to Base Table
UPDATE _lkp_membership_event_type__base AS base
SET base.membership_id = m.membership_id
FROM lake_consolidated.ultra_merchant.membership AS m
WHERE m.customer_id = base.customer_id;
-- SELECT * FROM _lkp_membership_event_type__base;

-- Temporary fix to eliminate overlapping effective dates in lake_history.membership table
CREATE OR REPLACE TEMP TABLE _lake_history_ultra_merchant_membership AS
SELECT DISTINCT
    m.membership_id,
    m.customer_id,
    m.store_id,
    m.membership_plan_id,
    m.membership_type_id,
    m.effective_start_datetime,
    LEAD(DATEADD(MILLISECOND, -1, m.effective_start_datetime), 1, '9999-12-31') OVER (PARTITION BY m.customer_id ORDER BY m.effective_start_datetime) AS effective_end_datetime
FROM _lkp_membership_event_type__base AS base
    JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.membership_id = base.membership_id;
-- SELECT * FROM _lake_history_ultra_merchant_membership;

-- Registrations and email signups
CREATE OR REPLACE TEMP TABLE _lkp_membership_event_type__registrations
(
    customer_id INT,
    store_id INT,
    membership_event_type VARCHAR(30),
    membership_type_detail VARCHAR(30),
    event_datetime TIMESTAMP_TZ
);
INSERT INTO _lkp_membership_event_type__registrations
(
     customer_id,
     store_id,
     membership_event_type,
     membership_type_detail,
     event_datetime
 )
SELECT DISTINCT
	COALESCE(cl.current_customer_id, m.customer_id) AS customer_id,
	COALESCE(fix.store_id, mpmb.store_id,m.store_id) AS store_id,
    CASE WHEN c.customer_id IS NULL THEN 'Registration'
	    ELSE 'Email Signup'
	    END AS membership_event_type,
	CASE WHEN membership_event_type = 'Email Signup' THEN NULL
        WHEN cl.current_customer_id IS NULL THEN 'Regular'
        ELSE 'Reactivated'
	    END AS membership_type_detail,
    public.udf_to_timestamp_tz(m.datetime_added, 'America/Los_Angeles') AS event_datetime
FROM _lkp_membership_event_type__base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.membership_id = base.membership_id
    LEFT JOIN lake_consolidated.ultra_merchant.membership_brand_signup AS mbs
        ON m.membership_signup_id = mbs.membership_signup_id
    LEFT JOIN lake_consolidated.ultra_merchant.membership_plan_membership_brand AS mpmb
        ON mpmb.membership_brand_id = mbs.membership_brand_id
    LEFT JOIN lake_consolidated.ultra_merchant.customer AS c
        ON c.customer_id = m.customer_id
        AND c.statuscode = 1502
    LEFT JOIN lake_consolidated.ultra_merchant.customer_link AS cl
        ON cl.current_customer_id = m.customer_id
        AND DATEDIFF(SECOND, cl.datetime_added, m.datetime_added) BETWEEN -15 AND 15
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = m.store_id
    LEFT JOIN reference.yitty_fl_lead_fix_20240304 fix
        ON base.customer_id = fix.customer_id;

-- adding deactivated leads
INSERT INTO _lkp_membership_event_type__registrations
(
     customer_id,
     store_id,
     membership_event_type,
     membership_type_detail,
     event_datetime
 )
SELECT
    base.customer_id,
    r.store_id,
    'Deactivated Lead' AS membership_event_type,
    'Deactivated' AS membership_type_detail,
    public.udf_to_timestamp_tz(cl.datetime_added, 'America/Los_Angeles') AS event_datetime
FROM _lkp_membership_event_type__registrations AS r
    JOIN lake_consolidated.ultra_merchant.customer_link AS cl
        ON cl.current_customer_id = r.customer_id
    JOIN _lkp_membership_event_type__base AS base
        ON base.customer_id = cl.original_customer_id
WHERE r.membership_type_detail = 'Reactivated';

-- Grabbing activations from membership_brand_activation so that it is attributed to the correct store_id
CREATE OR REPLACE TEMP TABLE _lkp_membership_event_type__brand_activations AS
SELECT
    m.customer_id,
    m.membership_id,
    mpmb.store_id,
    'Activation' AS membership_event_type,
    INITCAP(mp.period_type) AS membership_type_detail,
    mba.datetime_activated,
    public.udf_to_timestamp_tz( mba.datetime_activated, 'America/Los_Angeles') AS event_datetime
FROM _lkp_membership_event_type__base AS base
JOIN lake_consolidated.ultra_merchant.membership_brand_activation AS mba
    ON mba.membership_id = base.membership_id
JOIN lake_consolidated.ultra_merchant.membership AS m
    ON m.membership_id = mba.membership_id
JOIN lake_consolidated.ultra_merchant.membership_plan_membership_brand AS mpmb
    ON mpmb.membership_brand_id = mba.membership_brand_id
JOIN lake_consolidated.ultra_merchant.membership_plan AS mp
    ON mp.membership_plan_id = mpmb.membership_plan_id
LEFT JOIN reference.store_timezone AS stz
    ON stz.store_id = m.store_id;
-- SELECT * FROM _lkp_membership_event_type__brand_activations

-- Getting Gift Order Free Trial Activations (3831) and Full activations from a free trial state (3839)
CREATE OR REPLACE TEMP TABLE _lkp_membership_event_type__membership_trial AS
SELECT
    base.customer_id,
    base.membership_id,
    CASE
        WHEN sml.to_statuscode = 3831 THEN 'Free Trial Activation'
        WHEN sml.to_statuscode = 3839 THEN 'Activation'
        WHEN sml.to_statuscode = 3835 THEN 'Free Trial Downgrade'
    END AS membership_event_type,
    'Free Trial' AS membership_type_detail,
    mh.store_id,
    sml.datetime_added,
    public.udf_to_timestamp_tz(sml.datetime_added, 'America/Los_Angeles') AS event_datetime
FROM _lkp_membership_event_type__base AS base
    JOIN lake_consolidated.ultra_merchant.membership_trial AS mt
        ON mt.membership_id = base.membership_id
    JOIN lake_consolidated.ultra_merchant.gift_order AS gfto
        ON gfto.order_id = mt.order_id
    JOIN lake_consolidated.ultra_merchant.statuscode_modification_log AS sml
        ON sml.object_id = mt.meta_original_membership_trial_id
        AND sml.object = 'membership_trial'
    LEFT JOIN _lake_history_ultra_merchant_membership AS mh
        ON mh.membership_id = base.membership_id
        AND public.udf_to_timestamp_tz(sml.datetime_added, 'America/Los_Angeles') BETWEEN mh.effective_start_datetime AND mh.effective_end_datetime
WHERE sml.to_statuscode IN (3831,3835,3839);

/* if store_id is NULL when wrapping from ultra_merchant_history.membership, we will default it to their registration store_id */
UPDATE _lkp_membership_event_type__membership_trial AS mt
SET mt.store_id = r.store_id
FROM (SELECT DISTINCT customer_id, store_id
      FROM _lkp_membership_event_type__registrations) AS r
WHERE mt.customer_id = r.customer_id
    AND mt.store_id IS NULL;


-- Statuscode Switches
CREATE OR REPLACE TEMP TABLE _lkp_membership_event_type__statuscode_switches AS
SELECT
    m.customer_id,
	COALESCE(mh.store_id,r.store_id) AS store_id,
    CASE WHEN m.customer_id  IN (50362353130,87518041030) THEN 'Failed Activation' /* Do not allow Activation for Border Free customer */
        WHEN sml.to_statuscode IN (3940, 3941) THEN 'Cancellation'
        WHEN sml.to_statuscode = 3930 THEN 'Activation'
        ELSE 'Unknown'
	    END AS membership_event_type,
    CASE WHEN sml.to_statuscode IN (3940, 3941) THEN 'Hard'
        WHEN sml.to_statuscode = 3930
            AND UPPER(ds.store_brand_abbr) = 'SX'
            AND m.membership_type_id = 2 THEN 'Annual'
        WHEN sml.to_statuscode = 3930 THEN INITCAP(mp.period_type)
        ELSE 'Regular'
        END AS membership_type_detail,
    public.udf_to_timestamp_tz(sml.datetime_added, 'America/Los_Angeles') AS event_datetime
FROM _lkp_membership_event_type__base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.membership_id = base.membership_id
    JOIN lake_consolidated.ultra_merchant.statuscode_modification_log AS sml
        ON sml.object_id = m.meta_original_membership_id
    LEFT JOIN _lake_history_ultra_merchant_membership AS mh
        ON mh.membership_id = m.membership_id
        AND public.udf_to_timestamp_tz(sml.datetime_added, 'America/Los_Angeles') BETWEEN mh.effective_start_datetime AND mh.effective_end_datetime
LEFT JOIN (SELECT DISTINCT customer_id, store_id FROM _lkp_membership_event_type__registrations WHERE membership_event_type = 'Registration') AS r
        ON r.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_plan AS mp
        ON COALESCE(mh.membership_plan_id, m.membership_plan_id) = mp.membership_plan_id
    JOIN stg.dim_store AS ds
        ON ds.store_id = COALESCE(mh.store_id,r.store_id)
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = m.store_id
    LEFT JOIN _lkp_membership_event_type__brand_activations AS ba
        ON ba.membership_id = m.membership_id
        AND sml.to_statuscode = 3930
        AND DATEDIFF(SECOND, ba.datetime_activated, sml.datetime_added) BETWEEN -10 AND 10
    /* free trial activations behave like full vip activations in statuscode modlog.  We want to exclude these and not count these as full activations */
    LEFT JOIN (SELECT DISTINCT membership_id, datetime_added
               FROM _lkp_membership_event_type__membership_trial
               WHERE membership_event_type = 'Free Trial Activation') AS mt
        ON mt.membership_id = m.membership_id
        AND sml.to_statuscode = 3930
        AND DATEDIFF(SECOND, mt.datetime_added, sml.datetime_added) BETWEEN -10 AND 10
WHERE LOWER(sml.object) = 'membership'
	AND sml.to_statuscode IN (3940, 3941, 3930)
	AND ba.membership_id IS NULL
    AND mt.membership_id IS NULL;
-- SELECT * FROM _lkp_membership_event_type__statuscode_switches

-- Passive Cancels
CREATE OR REPLACE TEMP TABLE _lkp_membership_event_type__passive_cancels AS
SELECT
    m.customer_id,
	COALESCE(mh.store_id, r.store_id) AS store_id,
	'Cancellation' AS membership_event_type,
	'Passive' AS membership_type_detail,
	public.udf_to_timestamp_tz(mpc.datetime_added, 'America/Los_Angeles') AS event_datetime
FROM _lkp_membership_event_type__base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.membership_id = base.membership_id
    JOIN lake_consolidated.ultra_merchant.membership_passive_cancels AS mpc
        ON mpc.membership_id = m.membership_id
    LEFT JOIN _lake_history_ultra_merchant_membership AS mh
        ON mh.membership_id = base.membership_id
        AND public.udf_to_timestamp_tz(mpc.datetime_added, 'America/Los_Angeles') BETWEEN mh.effective_start_datetime AND mh.effective_end_datetime
    LEFT JOIN (SELECT DISTINCT customer_id, store_id FROM _lkp_membership_event_type__registrations WHERE membership_event_type = 'Registration') AS r
        ON r.customer_id = base.customer_id
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = m.store_id
WHERE LOWER(mpc.downgrade_status) = 'downgraded';
-- SELECT * FROM _lkp_membership_event_type__passive_cancels

-- Modlog switches
CREATE OR REPLACE TEMP TABLE _lkp_membership_event_type__modlog_switches AS
SELECT
	m.customer_id,
	COALESCE(mh.store_id,r.store_id) AS store_id,
	CASE WHEN m.customer_id  IN (50362353130,87518041030) THEN 'Failed Activation' /* Do not allow Activation for Border Free customer */
        WHEN mlev.label = 'Member' THEN 'Failed Activation'
	    WHEN mlev.label = 'Purchasing Member (Previous Active Subscription)' THEN 'Cancellation'
        WHEN mlev.label = 'Purchasing Member' AND mlev_old.label IN ('VIP', 'Elite VIP') THEN 'Cancellation'
        WHEN mlev.label = 'Purchasing Member' AND mlev_old.label = 'Member' THEN 'Guest Purchasing Member'
        WHEN mlev.label IN ('Diamond Elite VIP', 'Elite VIP', 'VIP', 'Classic VIP') THEN 'Activation'
	    ELSE 'Unknown'
        END AS membership_event_type,
	CASE WHEN membership_event_type ='Activation' AND mlev.label='Classic VIP' THEN 'Classic'
		WHEN membership_event_type = 'Activation' AND UPPER(ds.store_brand_abbr) = 'SX'
            AND m.membership_type_id = 2 THEN 'Annual'
	    WHEN membership_event_type = 'Activation' THEN INITCAP(mp.period_type)
	    WHEN membership_event_type = 'Cancellation' THEN 'Soft'
	     WHEN membership_event_type = 'Guest Purchasing Member' AND
	         c.email ILIKE ANY ('%retail.fabletics.com','%retail.savagex.com') THEN 'Unidentified'
	    WHEN membership_event_type = 'Guest Purchasing Member' THEN 'Regular'
	    ELSE 'Unknown'
        END AS membership_type_detail,
	public.udf_to_timestamp_tz(mlog.datetime_added, 'America/Los_Angeles') AS event_datetime
FROM _lkp_membership_event_type__base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.membership_id = base.membership_id
    JOIN lake_consolidated.ultra_merchant.modification_log AS mlog
        ON mlog.object_id = m.meta_original_membership_id
    LEFT JOIN lake_consolidated.ultra_merchant.customer AS c
        ON c.customer_id = m.customer_id
    LEFT JOIN _lake_history_ultra_merchant_membership AS mh
        ON mh.membership_id = base.membership_id
        AND public.udf_to_timestamp_tz(mlog.datetime_added, 'America/Los_Angeles') BETWEEN mh.effective_start_datetime AND mh.effective_end_datetime
    LEFT JOIN (SELECT DISTINCT customer_id, store_id FROM _lkp_membership_event_type__registrations WHERE membership_event_type = 'Registration') AS r
        ON r.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_plan AS mp
        ON mp.membership_plan_id = m.membership_plan_id
    JOIN lake_consolidated.ultra_merchant.membership_level AS mlev
        ON mlev.membership_level_id = TRY_TO_NUMBER(mlog.new_value)
    JOIN lake_consolidated.ultra_merchant.membership_level AS mlev_old
        ON mlev_old.membership_level_id = TRY_TO_NUMBER(mlog.old_value)
    JOIN stg.dim_store AS ds
        ON ds.store_id = m.store_id
    /* Ensure that no duplicate VIP signals are being captured for the same datetime within 10 sec of each other, take statuscode_modification log by default */
    LEFT JOIN lake_consolidated.ultra_merchant.statuscode_modification_log AS smlog
        ON smlog.object_id = m.meta_original_membership_id
        AND DATEDIFF(SECOND, smlog.datetime_added, mlog.datetime_added) BETWEEN -10 AND 10
        AND smlog.to_statuscode = 3930 -- removes double-counting of activations
        AND LOWER(mlev.label) = 'vip'
    LEFT JOIN (SELECT DISTINCT membership_id, datetime_activated FROM _lkp_membership_event_type__brand_activations) AS ba -- removes double-counting of activations
        ON ba.membership_id = m.membership_id
        AND DATEDIFF(SECOND, ba.datetime_activated, mlog.datetime_added) BETWEEN -10 AND 10
        AND LOWER(mlev.label) = 'vip'
    /* free trial downgrades will cause a modlog change to "purchasing member (previous active subscription)".  We don't want to double count and call these cancellations */
    LEFT JOIN (SELECT DISTINCT membership_id, datetime_added
               FROM _lkp_membership_event_type__membership_trial
               WHERE membership_event_type = 'Free Trial Downgrade') AS mt
        ON mt.membership_id = m.membership_id
        AND DATEDIFF(SECOND, mt.datetime_added, mlog.datetime_added) BETWEEN -10 AND 10
        AND mlev.label = 'Purchasing Member (Previous Active Subscription)'
    /* When one time downgraded happened, modification_log level also changed to Purchasing Member (Previous Active Subscription), only take 1 cancel signal. Default is Passive Cancel */
    LEFT JOIN lake_consolidated.ultra_merchant.membership_level_group_modification_log AS mlgml
        ON mlgml.membership_id = m.membership_id
        AND mlgml.passive_downgrade = 1
        AND DATEDIFF(SECOND, mlgml.datetime_added, mlog.datetime_added) BETWEEN -10 AND 10
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = m.store_id
WHERE LOWER(mlog.object) = 'membership'
	AND LOWER(mlog.field) = 'membership_level_id'
	AND LOWER(mlev.label) IN ('purchasing member', 'purchasing member (previous active subscription)','member','classic vip','diamond elite vip','elite vip','vip')
	-- Take the records that are NOT matching from statuscode_modification_log or membership_level_group_modification_log table
	AND smlog.to_statuscode IS NULL
	AND mlgml.datetime_added IS NULL
	AND ba.datetime_activated IS NULL
    AND mt.datetime_added IS NULL;
-- SELECT * FROM _lkp_membership_event_type__modlog_switches

CREATE OR REPLACE TEMP TABLE _lkp_membership_event_type__rank AS
SELECT
    customer_id,
    store_id,
    membership_event_type,
    membership_type_detail,
    event_datetime,
    COALESCE(DATEADD(MILLISECOND, -1, LEAD(event_datetime) OVER (PARTITION BY customer_id ORDER BY event_datetime)), '9999-12-31') AS event_end_datetime,
    event_type
FROM (
    SELECT
        customer_id,
        store_id,
        membership_event_type,
        membership_type_detail,
        event_datetime,
        'registrations' AS event_type
    FROM _lkp_membership_event_type__registrations
    UNION ALL
    SELECT
        customer_id,
        store_id,
        membership_event_type,
        membership_type_detail,
        event_datetime,
        'brand_activations' AS event_type
    FROM _lkp_membership_event_type__brand_activations
    UNION ALL
    SELECT
        customer_id,
        store_id,
        membership_event_type,
        membership_type_detail,
        event_datetime,
        'free_trial_events' AS event_type
    FROM _lkp_membership_event_type__membership_trial
    UNION ALL
    SELECT
        customer_id,
        store_id,
        membership_event_type,
        membership_type_detail,
        event_datetime,
        'statuscode_switches' AS event_type
    FROM _lkp_membership_event_type__statuscode_switches
    UNION ALL
    SELECT
        customer_id,
        store_id,
        membership_event_type,
        membership_type_detail,
        event_datetime,
        'passive_cancels' AS event_type
    FROM _lkp_membership_event_type__passive_cancels
    UNION ALL
    SELECT
        customer_id,
        store_id,
        membership_event_type,
        membership_type_detail,
        event_datetime,
        'modlog_switches' AS event_type
    FROM _lkp_membership_event_type__modlog_switches
    ) AS r;
-- SELECT * FROM _lkp_membership_event_type__rank;

-- For free trial, we want membership_type_detail to be "Free Trial" tied to ALL membership_event_type records
UPDATE _lkp_membership_event_type__rank AS r
    SET r.membership_type_detail = mt.membership_type_detail
    FROM (SELECT DISTINCT customer_id, membership_type_detail FROM _lkp_membership_event_type__membership_trial) AS mt
    WHERE mt.customer_id = r.customer_id;

-- Ignore signals for a set of customers on a specific date
DELETE FROM _lkp_membership_event_type__rank AS r
USING reference.upgraded_passive_cancel_customers_20210930 AS upg
WHERE r.customer_id = upg.customer_id
    AND r.event_datetime::DATE = '2021-09-30';

-- Ignore signals for a set of customers and transactions prior to and including a specific date
DELETE FROM _lkp_membership_event_type__rank AS r
USING (
    SELECT r.customer_id
    FROM _lkp_membership_event_type__rank AS r
        JOIN reference.upgraded_passive_cancel_customers_20210930 AS upg
            ON upg.customer_id = r.customer_id
    WHERE r.membership_event_type = 'Cancellation'
        AND r.membership_type_detail = 'Passive'
        AND r.event_datetime::DATE = '2021-07-07'
    ) AS pc
WHERE r.customer_id = pc.customer_id
    AND r.membership_event_type = 'Cancellation'
    AND r.membership_type_detail = 'Passive'
    AND r.event_datetime::DATE BETWEEN '2021-01-01' AND '2021-07-07';
-- SELECT * FROM _lkp_membership_event_type__rank;

-- ignore passive cancellations on pending customers
DELETE
FROM _lkp_membership_event_type__rank r
    USING reference.pending_passive_cancel_customers_20230201 AS upg
WHERE upg.customer_id = r.customer_id
  AND r.membership_event_type = 'Cancellation'
  AND r.membership_type_detail = 'Passive'
  AND r.event_datetime::DATE >= '2023-02-03';

-- Ignore cancellation signals if there are billing orders within the cancellation period
DELETE FROM _lkp_membership_event_type__rank AS r
USING (
    SELECT o.customer_id, o.datetime_local_transaction
    FROM lake_consolidated.ultra_merchant."ORDER" AS o
        JOIN lake_consolidated.ultra_merchant.order_classification AS oc
            ON oc.order_id = o.order_id
    WHERE oc.order_type_id IN (9,10,39)
    ) AS o
WHERE r.membership_event_type = 'Cancellation'
    AND r.membership_type_detail = 'Passive'
    AND o.customer_id = r.customer_id
    AND o.datetime_local_transaction BETWEEN r.event_datetime AND r.event_end_datetime;
-- SELECT * FROM _lkp_membership_event_type__rank;

CREATE OR REPLACE TEMP TABLE _lkp_membership_event_type__pre_stg AS
SELECT -- Pre-2016 data
    a.customer_id,
    a.store_id,
    a.membership_event_type,
    a.membership_type_detail ||
        IFF(a.membership_event_type = 'Cancellation' AND a.membership_type_detail = 'Regular', ' (Pre-2016)', '') AS membership_type_detail,
    CONVERT_TIMEZONE(stz.time_zone, a.event_datetime) AS event_local_datetime,
    FALSE AS is_deleted /* not used */
FROM reference.fact_membership_event_archive AS a
    JOIN _lkp_membership_event_type__base AS base
        ON base.customer_id = a.customer_id
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = a.store_id
WHERE a.event_datetime < '2016-01-01'
UNION ALL
SELECT
    r.customer_id,
    r.store_id,
    r.membership_event_type,
    r.membership_type_detail,
    CONVERT_TIMEZONE(stz.time_zone, r.event_datetime) AS event_local_datetime,
    FALSE AS is_deleted /* not used */
FROM _lkp_membership_event_type__rank AS r
    LEFT JOIN reference.store_timezone AS stz
        ON stz.store_id = r.store_id
WHERE r.event_datetime >= '2016-01-01';

INSERT INTO _lkp_membership_event_type__pre_stg
WITH _product_orders AS
    (
        SELECT base.customer_id,
               o.order_id,
               CONVERT_TIMEZONE(stz.time_zone, o.datetime_added) AS order_local_datetime
        FROM _lkp_membership_event_type__base AS base
            JOIN lake_consolidated.ultra_merchant."ORDER" AS o
                ON o.customer_id = base.customer_id
            JOIN lake_consolidated.ultra_merchant.statuscode AS sc
                ON sc.statuscode = o.processing_statuscode
            LEFT JOIN reference.store_timezone AS stz
                ON stz.store_id = o.store_id
            LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
                ON oc.order_id = o.order_id
                AND oc.order_type_id IN (23, 11, 26, 6, 9, 10, 39) -- excluding activating orders, exchanges, reships, credit/token billings
        WHERE oc.order_id IS NULL
        AND (o.date_shipped IS NOT NULL OR sc.label IN ('Shipped', 'Complete'))
        AND CONVERT_TIMEZONE(stz.time_zone, o.datetime_added) >= '2016-01-01'
    )
SELECT
    customer_id,
    store_id,
    'Guest Purchasing Member' AS membership_event_type,
    'Previous VIP' AS membership_type_detail,
    order_local_datetime AS event_local_datetime,
    FALSE AS is_deleted
FROM (
    SELECT
        po.customer_id,
        met.store_id,
        met.membership_event_type,
        met.event_local_datetime,
        po.order_local_datetime,
        ROW_NUMBER() OVER (PARTITION BY po.order_id ORDER BY met.event_local_datetime DESC) AS rnk
    FROM _product_orders AS po
        JOIN _lkp_membership_event_type__pre_stg AS met
            ON met.customer_id = po.customer_id
            AND met.event_local_datetime <= po.order_local_datetime
    QUALIFY rnk = 1
)
WHERE membership_event_type = 'Cancellation'
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, event_local_datetime ORDER BY order_local_datetime ASC) = 1;


CREATE OR REPLACE TEMP TABLE _lkp_membership_event_type__stg AS
SELECT
    customer_id,
    CAST(-1 AS NUMERIC(38,0)) AS meta_original_customer_id,
    store_id,
    membership_event_type,
    membership_type_detail,
    event_local_datetime,
    is_deleted,
    ''::VARCHAR AS meta_data_quality
FROM (
    SELECT
        customer_id,
        CASE WHEN store_id = 41 THEN 26 ELSE store_id END AS store_id,
        membership_event_type,
        membership_type_detail,
        event_local_datetime,
        is_deleted,
        ROW_NUMBER() OVER (PARTITION BY customer_id, event_local_datetime
            ORDER BY membership_event_type, membership_type_detail) AS row_num
    FROM _lkp_membership_event_type__pre_stg
    ) AS s
WHERE s.row_num = 1;
-- SELECT * FROM _lkp_membership_event_type__stg;

-- Delete the orphans
INSERT INTO _lkp_membership_event_type__stg (
    customer_id,
    meta_original_customer_id,
    store_id,
    membership_event_type,
    membership_type_detail,
    event_local_datetime,
    is_deleted
    )
SELECT
    t.customer_id,
    -1 AS meta_original_customer_id,
    CASE WHEN t.store_id = 41 THEN 26 ELSE t.store_id END AS store_id,
    t.membership_event_type,
    t.membership_type_detail,
    t.event_local_datetime,
    TRUE AS is_deleted
FROM stg.lkp_membership_event_type AS t
    JOIN (SELECT DISTINCT customer_id FROM _lkp_membership_event_type__stg) AS f
        ON f.customer_id = t.customer_id
WHERE NOT NVL(t.is_deleted, FALSE)
    AND NOT EXISTS (
        SELECT 1
        FROM _lkp_membership_event_type__stg AS s
        WHERE s.customer_id = t.customer_id
        AND s.event_local_datetime::timestamp_ntz = t.event_local_datetime::timestamp_ntz
        );
-- SELECT COUNT(1) FROM _lkp_membership_event_type__stg;

-- Delete records with wrong concatanation
INSERT INTO _lkp_membership_event_type__stg (
    customer_id,
    meta_original_customer_id,
    store_id,
    membership_event_type,
    membership_type_detail,
    event_local_datetime,
    is_deleted
    )
SELECT
    t.customer_id,
    -1,
    t.store_id,
    t.membership_event_type,
    t.membership_type_detail,
    t.event_local_datetime,
    TRUE AS is_deleted
FROM stg.lkp_membership_event_type t
        JOIN (SELECT DISTINCT stg.UDF_UNCONCAT_BRAND(customer_id) meta_original_customer_id from _lkp_membership_event_type__stg) f
            ON t.meta_original_customer_id = f.meta_original_customer_id
         JOIN stg.dim_store ds
              ON t.store_id = ds.store_id
WHERE RIGHT(t.customer_id, 2) <> ds.company_id
  AND t.is_deleted = FALSE
  AND $is_full_refresh = TRUE
      AND NOT EXISTS (
        SELECT 1
        FROM _lkp_membership_event_type__stg AS s
        WHERE s.customer_id = t.customer_id
        AND s.event_local_datetime::timestamp_ntz = t.event_local_datetime::timestamp_ntz
        );

UPDATE _lkp_membership_event_type__stg s
SET s.meta_original_customer_id = c.meta_original_customer_id
FROM lake_consolidated.ultra_merchant.customer c
WHERE s.customer_id = c.customer_id;

UPDATE _lkp_membership_event_type__stg
SET meta_original_customer_id = stg.UDF_UNCONCAT_BRAND(customer_id)
WHERE IFNULL(meta_original_customer_id, -1) = -1;


CREATE OR REPLACE TEMPORARY TABLE _lkp_membership_event_type_excp
(
    customer_id          INT,
    event_local_datetime TIMESTAMP_TZ(3),
    meta_data_quality    VARCHAR(10),
    excp_message         VARCHAR
);

CREATE OR REPLACE TEMPORARY TABLE _lkp_membership_event_type_delta
(
    customer_id               NUMBER(38, 0)   NOT NULL,
    meta_original_customer_id NUMBER(38, 0),
    store_id                  NUMBER(38, 0),
    membership_event_type     VARCHAR(50),
    membership_type_detail    VARCHAR(20),
    event_local_datetime      TIMESTAMP_TZ(3) NOT NULL,
    is_deleted                BOOLEAN,
    meta_row_hash             NUMBER(38, 0),
    meta_create_datetime      TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(3),
    meta_update_datetime      TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(3)
);


-- Identifying duplicate business keys
INSERT INTO _lkp_membership_event_type_excp
SELECT customer_id,
       event_local_datetime,
       'error'                                     AS meta_data_quality,
       'Duplicate business key values are present' AS excp_message
FROM _lkp_membership_event_type__stg
GROUP BY customer_id, event_local_datetime
HAVING COUNT(*) > 1
;


UPDATE excp.lkp_membership_event_type e
SET e.meta_is_current_excp = FALSE
WHERE e.meta_is_current_excp;

-------create table script
INSERT INTO excp.lkp_membership_event_type
(customer_id,
 meta_original_customer_id,
 store_id,
 membership_event_type,
 membership_type_detail,
 event_local_datetime,
 is_deleted,
 meta_create_datetime,
 meta_update_datetime,
 meta_data_quality,
 excp_message,
 meta_is_current_excp)
SELECT s.customer_id,
       s.meta_original_customer_id,
       s.store_id,
       s.membership_event_type,
       s.membership_type_detail,
       s.event_local_datetime,
       s.is_deleted,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime,
       e.meta_data_quality,
       e.excp_message,
       TRUE                  AS meta_is_current_excp
FROM _lkp_membership_event_type__stg s
         JOIN _lkp_membership_event_type_excp e
              ON s.customer_id = e.customer_id
                  AND s.event_local_datetime::TIMESTAMP_NTZ = e.event_local_datetime::TIMESTAMP_NTZ;


UPDATE _lkp_membership_event_type__stg s
SET s.meta_data_quality = e.meta_data_quality
FROM _lkp_membership_event_type_excp e
WHERE s.customer_id = e.customer_id
  AND s.event_local_datetime::TIMESTAMP_NTZ = e.event_local_datetime::TIMESTAMP_NTZ
  AND e.meta_data_quality = 'error';


INSERT INTO _lkp_membership_event_type_delta
SELECT s.customer_id                                             AS customer_id,
       s.meta_original_customer_id                               AS meta_original_customer_id,
       s.store_id                                                AS store_id,
       s.membership_event_type                                   AS membership_event_type,
       s.membership_type_detail                                  AS membership_type_detail,
       s.event_local_datetime                                    AS event_local_datetime,
       s.is_deleted                                              AS is_deleted,
       HASH(s.customer_id, s.meta_original_customer_id, s.store_id, s.membership_event_type, s.membership_type_detail,
            s.event_local_datetime::TIMESTAMP_NTZ, s.is_deleted) AS meta_row_hash,
       $execution_start_time                                     AS meta_create_datetime,
       $execution_start_time                                     AS meta_update_datetime
FROM _lkp_membership_event_type__stg s

WHERE s.meta_data_quality IS DISTINCT FROM 'error';


MERGE INTO stg.lkp_membership_event_type t
    USING _lkp_membership_event_type_delta s
    ON EQUAL_NULL(t.customer_id, s.customer_id)
        AND EQUAL_NULL(s.event_local_datetime::TIMESTAMP_NTZ, t.event_local_datetime::TIMESTAMP_NTZ)
    WHEN NOT MATCHED THEN
        INSERT (
                customer_id,
                meta_original_customer_id,
                store_id,
                membership_event_type,
                membership_type_detail,
                event_local_datetime,
                is_deleted,
                meta_row_hash,
                meta_create_datetime,
                meta_update_datetime
            )
            VALUES (customer_id,
                    meta_original_customer_id,
                    store_id,
                    membership_event_type,
                    membership_type_detail,
                    event_local_datetime,
                    is_deleted,
                    meta_row_hash,
                    meta_create_datetime,
                    meta_update_datetime)
    WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN
        UPDATE
            SET --t.customer_id = s.customer_id,
                t.meta_original_customer_id = s.meta_original_customer_id,
                t.store_id = s.store_id,
                t.membership_event_type = s.membership_event_type,
                t.membership_type_detail = s.membership_type_detail,
                --t.event_local_datetime = s.event_local_datetime,
                t.is_deleted = s.is_deleted,
                t.meta_row_hash = s.meta_row_hash,
                --t.meta_create_datetime = s.meta_create_datetime,
                t.meta_update_datetime = s.meta_update_datetime;

--  Success
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
                                  (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime
                                   FROM stg.lkp_membership_event_type)),
    meta_update_datetime    = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
-- SELECT * FROM stg.meta_table_dependency_watermark WHERE table_name = $target_table;
