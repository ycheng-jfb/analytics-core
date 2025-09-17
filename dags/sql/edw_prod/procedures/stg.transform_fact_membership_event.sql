SET target_table = 'stg.fact_membership_event';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

-- Switch warehouse if performing a full refresh
SET initial_warehouse = CURRENT_WAREHOUSE();
SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', CURRENT_WAREHOUSE());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

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


-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_edw_stg_lkp_membership_state = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.lkp_membership_state'));
SET wm_edw_stg_lkp_order_classification = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.lkp_order_classification'));
SET wm_lake_consolidated_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership'));
SET wm_lake_consolidated_ultra_merchant_membership_signup = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_signup'));
SET wm_lake_consolidated_ultra_merchant_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer'));
SET wm_lake_consolidated_ultra_merchant_customer_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer_detail'));
SET wm_lake_consolidated_ultra_merchant_customer_link = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer_link'));

/*
SELECT
    $wm_self,
    $wm_edw_stg_lkp_membership_state,
    $wm_edw_stg_lkp_order_classification,
    $wm_lake_consolidated_ultra_merchant_membership,
    $wm_lake_consolidated_ultra_merchant_membership_signup,
    $wm_lake_consolidated_ultra_merchant_customer,
    $wm_lake_consolidated_ultra_merchant_customer_detail,
    $wm_lake_consolidated_ultra_merchant_customer_link;
*/

CREATE OR REPLACE TEMP TABLE _fact_membership_event__customer_base (customer_id INT);

-- Full Refresh
INSERT INTO _fact_membership_event__customer_base (customer_id)
SELECT DISTINCT lme.customer_id
FROM stg.lkp_membership_state AS lme
WHERE $is_full_refresh  /* SET is_full_refresh = TRUE; */
    AND lme.customer_id <> -1
ORDER BY lme.customer_id;

-- Incremental Refresh
INSERT INTO _fact_membership_event__customer_base (customer_id)
SELECT DISTINCT incr.customer_id
FROM (
    /* Self-check for manual updates */
    SELECT fme.customer_id
    FROM stg.fact_membership_event AS fme
    WHERE fme.meta_update_datetime > $wm_self
    UNION ALL
    /* Check for dependency table updates */
    SELECT lme.customer_id
    FROM stg.lkp_membership_state AS lme
    WHERE lme.meta_update_datetime > $wm_edw_stg_lkp_membership_state
    UNION ALL
    SELECT loc.customer_id
    FROM stg.lkp_order_classification AS loc
    WHERE loc.meta_update_datetime > $wm_edw_stg_lkp_order_classification
    UNION ALL
    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership AS m
        LEFT JOIN lake_consolidated.ultra_merchant.membership_signup AS ms
            ON ms.membership_signup_id = m.membership_signup_id
    WHERE (m.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership
        OR ms.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership_signup)
    UNION ALL
    SELECT customer_id
    FROM lake_consolidated.ultra_merchant.customer
    WHERE meta_update_datetime > $wm_lake_consolidated_ultra_merchant_customer
    UNION ALL
    SELECT customer_id
    FROM lake_consolidated.ultra_merchant.customer_detail
    WHERE meta_update_datetime > $wm_lake_consolidated_ultra_merchant_customer_detail
    AND name = 'isScrubs'
    UNION ALL
    SELECT current_customer_id AS customer_id
    FROM lake_consolidated.ultra_merchant.customer_link
    WHERE meta_update_datetime > $wm_lake_consolidated_ultra_merchant_customer_link
    UNION ALL
    /* Previously errored rows */
    SELECT customer_id
    FROM excp.fact_membership_event
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.customer_id;
-- SELECT * FROM _fact_membership_event__customer_base;

--	Find the Registration session
CREATE OR REPLACE TEMP TABLE _fact_membership_event__lead_registration AS
SELECT
    base.customer_id,
    cl.original_customer_id AS original_linked_customer_id,
    MIN(ms.session_id) AS session_id /* For 12 customers multiple entries in membership */
FROM _fact_membership_event__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
        ON m.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_signup AS ms
        ON ms.membership_signup_id = m.membership_signup_id
    LEFT JOIN lake_consolidated.ultra_merchant.customer_link AS cl
        ON cl.current_customer_id = m.customer_id
        AND DATEDIFF(SECOND, cl.datetime_added, m.datetime_added) BETWEEN -15 AND 15
GROUP BY
    base.customer_id,
    cl.original_customer_id,
    m.membership_id
-- Add qualify clause due to several customer_ids assigned to more than one membership_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY base.customer_id ORDER BY m.membership_id DESC) = 1;
-- SELECT customer_id, COUNT(1) FROM _fact_membership_event__lead_registration GROUP BY 1 HAVING COUNT(1) > 1;
-- SELECT original_linked_customer_id, COUNT(1) FROM _fact_membership_event__lead_registration GROUP BY 1 HAVING COUNT(1) > 1;

--  Get all membership events
CREATE OR REPLACE TEMP TABLE _fact_membership_event__membership_events AS
SELECT
    lms.customer_id,
    lms.meta_original_customer_id,
    lms.store_id,
    lms.membership_event_type_key,
    lms.membership_event_type,
    lms.membership_type_detail,
    lms.event_local_datetime,
    lms.membership_state,
    lms.is_deleted,
    lms.is_ignored_activation,
    lms.is_ignored_cancellation,
    lms.is_hard_cancellation_from_ecom,
      LEAD(lms.event_local_datetime) OVER (PARTITION BY lms.customer_id ORDER BY lms.event_local_datetime) AS next_event_local_datetime
FROM _fact_membership_event__customer_base AS base
    JOIN stg.lkp_membership_state AS lms
        ON lms.customer_id = base.customer_id
WHERE lms.customer_id <> -1
    AND NOT NVL(lms.is_deleted, FALSE);
-- Get associated orders
-- Find activating success orders within a minute duration
CREATE OR REPLACE TEMP TABLE _fact_membership_event__activating_success_orders AS
SELECT
	base.customer_id,
	oc.order_id,
    oc.session_id,
	oc.order_local_datetime,
	oc.is_activating
FROM stg.lkp_order_classification AS oc
    JOIN _fact_membership_event__customer_base AS base
        ON base.customer_id = oc.customer_id
WHERE oc.is_activating = TRUE
	AND oc.is_guest = FALSE
    AND LOWER(oc.order_status) IN ('success', 'pending')
    AND LOWER(oc.order_classification_name) = 'product order';

--	Find the Activating Orders for activating events
CREATE OR REPLACE TEMP TABLE _fact_membership_event__activating_event_orders AS
SELECT
    me.customer_id AS customer_id,
    me.event_local_datetime,
    aso.order_id AS order_id,
    aso.session_id AS session_id,
    ROW_NUMBER() OVER (PARTITION BY me.customer_id, me.event_local_datetime ORDER BY ABS(DATEDIFF(milliseconds, aso.order_local_datetime, me.event_local_datetime))) AS row_nbr
FROM _fact_membership_event__membership_events AS me
    JOIN stg.dim_membership_event_type AS dmet
        ON dmet.membership_event_type_key = me.membership_event_type_key
    JOIN _fact_membership_event__activating_success_orders AS aso
        ON aso.customer_id = me.customer_id
        AND DATEDIFF(SECOND, aso.order_local_datetime, me.event_local_datetime) BETWEEN -900 AND 3600
WHERE NOT me.is_ignored_activation
QUALIFY row_nbr = 1;

-- Find Ecomm orders
CREATE OR REPLACE TEMP TABLE _fact_membership_event__guest_orders AS
SELECT
    oc.customer_id,
    oc.order_id,
    oc.session_id,
    oc.order_local_datetime
FROM stg.lkp_order_classification AS oc
    JOIN _fact_membership_event__customer_base AS base
        ON base.customer_id = oc.customer_id
WHERE oc.is_guest = 1
    AND LOWER(oc.order_classification_name) = 'product order';

--	Find the ecomm Orders for ecomm events
CREATE OR REPLACE TEMP TABLE _fact_membership_event__guest_event_orders AS
SELECT
    me.customer_id,
    me.event_local_datetime,
    gs.order_id AS order_id,
    gs.session_id AS session_id,
    ROW_NUMBER() OVER (PARTITION BY me.customer_id, me.event_local_datetime ORDER BY ABS(DATEDIFF(milliseconds, gs.order_local_datetime, me.event_local_datetime))) AS row_nbr
FROM _fact_membership_event__membership_events AS me
    JOIN stg.dim_membership_event_type AS dmet
        ON dmet.membership_event_type_key = me.membership_event_type_key
    JOIN _fact_membership_event__guest_orders AS gs
        ON gs.customer_id = me.customer_id
        AND DATEDIFF(SECOND, gs.order_local_datetime, me.event_local_datetime) BETWEEN -900 AND 3600
WHERE dmet.membership_event_type = 'Guest Purchasing Member'
QUALIFY row_nbr = 1;

--	Find the Orders for Failed activation events
CREATE OR REPLACE TEMP TABLE _fact_membership_event__failed_activation_orders AS
SELECT
    me.customer_id AS customer_id,
    me.event_local_datetime,
    oc.order_id AS order_id,
    oc.session_id AS session_id,
    ROW_NUMBER() OVER (PARTITION BY me.customer_id, me.event_local_datetime ORDER BY ABS(DATEDIFF(milliseconds, oc.order_local_datetime, me.event_local_datetime))) AS row_nbr
FROM _fact_membership_event__membership_events AS me
    JOIN stg.dim_membership_event_type AS dmet
        ON dmet.membership_event_type_key = me.membership_event_type_key
    JOIN stg.lkp_order_classification AS oc
        ON oc.customer_id = me.customer_id
        AND DATEDIFF(SECOND, oc.order_local_datetime, me.event_local_datetime) BETWEEN -900 AND 3600
        AND LOWER(oc.order_status) IN ('cancelled', 'failure')
WHERE dmet.membership_event_type = 'Failed Activation'
QUALIFY row_nbr = 1;

CREATE OR REPLACE TEMP TABLE _fact_membership_event__activation_date AS
SELECT
    me.customer_id,
	me.event_local_datetime AS activation_start_local_datetime,
	COALESCE(DATEADD(MILLISECOND, -1, me.next_event_local_datetime), '9999-12-31') AS activation_end_local_datetime
FROM _fact_membership_event__membership_events AS me
    JOIN stg.dim_membership_event_type AS dmet
        ON dmet.membership_event_type_key = me.membership_event_type_key
WHERE dmet.membership_event_type_key = 3;

CREATE OR REPLACE TEMP TABLE _fact_membership_event__is_scrubs AS
SELECT base.customer_id
FROM _fact_membership_event__customer_base AS base
         JOIN  lake_consolidated.ultra_merchant.customer_detail AS cd
              ON cd.customer_id = base.customer_id
         JOIN  lake_consolidated.ultra_merchant.customer c
              ON c.customer_id = base.customer_id
         LEFT JOIN  lake_consolidated.ultra_merchant.membership m
                   ON m.customer_id = c.customer_id
WHERE cd.name = 'isScrubs'
  AND cd.value = 1
  AND CAST(cd.datetime_added AS DATE) = CAST(COALESCE(m.datetime_added, c.datetime_added) AS DATE)
  AND base.customer_id <> 99704791520 -- DA-34021
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cd.customer_id ORDER BY cd.datetime_added DESC) = 1;

CREATE OR REPLACE TEMP TABLE _fact_membership_event__pre_stg AS
SELECT
    me.customer_id,
    me.meta_original_customer_id,
	me.store_id,
	CASE WHEN dmet.membership_event_type IN ('Activation') THEN aeo.order_id
	    WHEN dmet.membership_event_type IN ('Guest Purchasing Member') THEN geo.order_id
	    WHEN dmet.membership_event_type IN ('Failed Activation') THEN fao.order_id
        ELSE NULL
    END AS order_id,
	CASE WHEN dmet.membership_event_type IN ('Registration') THEN lr.session_id
        WHEN dmet.membership_event_type IN ('Activation') THEN aeo.session_id
	    WHEN dmet.membership_event_type IN ('Guest Purchasing Member') THEN geo.session_id
	    WHEN dmet.membership_event_type IN ('Failed Activation') THEN fao.session_id
        ELSE NULL
    END AS session_id,
	me.membership_event_type_key AS membership_event_type_key,
	me.membership_event_type AS membership_event_type,
	me.membership_type_detail AS membership_type_detail,
	me.membership_state,
	me.event_local_datetime AS event_start_local_datetime,
    COALESCE(ad.activation_start_local_datetime, '1900-01-01') AS recent_activation_local_datetime,
    me.is_hard_cancellation_from_ecom,
    IFF(iss.customer_id IS NOT NULL AND st.store_brand = 'Fabletics', TRUE, FALSE) AS is_scrubs_customer,
    IFF(tc.customer_id IS NOT NULL, TRUE, FALSE) AS is_test_customer,
	me.is_deleted
FROM _fact_membership_event__membership_events AS me
    JOIN stg.dim_membership_event_type AS dmet
        ON dmet.membership_event_type_key = me.membership_event_type_key
    LEFT JOIN _fact_membership_event__lead_registration AS lr
        ON lr.customer_id = me.customer_id
    LEFT JOIN (SELECT DISTINCT original_linked_customer_id FROM _fact_membership_event__lead_registration WHERE original_linked_customer_id IS NOT NULL) AS olc
        ON olc.original_linked_customer_id = me.customer_id
    LEFT JOIN _fact_membership_event__activation_date AS ad
        ON ad.customer_id = me.customer_id
        AND me.event_local_datetime BETWEEN ad.activation_start_local_datetime AND ad.activation_end_local_datetime
    LEFT JOIN _fact_membership_event__activating_event_orders AS aeo
        ON aeo.customer_id = me.customer_id
        AND aeo.event_local_datetime = me.event_local_datetime
    LEFT JOIN _fact_membership_event__failed_activation_orders AS fao
        ON fao.customer_id = me.customer_id
        AND fao.event_local_datetime = me.event_local_datetime
    LEFT JOIN _fact_membership_event__guest_event_orders AS geo
        ON geo.customer_id = me.customer_id
        AND geo.event_local_datetime = me.event_local_datetime
    LEFT JOIN reference.test_customer AS tc
        ON tc.customer_id = me.customer_id
    LEFT JOIN _fact_membership_event__is_scrubs AS iss
        ON iss.customer_id = me.customer_id
    LEFT JOIN stg.dim_store AS st
        ON st.store_id = me.store_id
WHERE NOT me.is_ignored_activation
    AND NOT me.is_ignored_cancellation;
-- SELECT * FROM _fact_membership_event__pre_stg;

/* Updating session_id for free trial activations to equal the session_id tied to their registration since it happened during the same session */
UPDATE _fact_membership_event__pre_stg AS t
SET t.session_id = s.session_id
FROM (
        SELECT customer_id, session_id, event_start_local_datetime
        FROM _fact_membership_event__pre_stg
        WHERE membership_event_type = 'Registration'
        AND membership_type_detail = 'Free Trial'
     ) AS s
WHERE s.customer_id = t.customer_id
    AND t.membership_event_type = 'Free Trial Activation'
    AND t.membership_type_detail = 'Free Trial'
    AND DATEDIFF(SECOND, s.event_start_local_datetime, t.event_start_local_datetime) <= 5;

-- Special categorization for identifying hard cancellation from Ecom
UPDATE _fact_membership_event__pre_stg AS upd
SET
    upd.membership_event_type_key = 7,
	upd.membership_event_type = (SELECT membership_event_type FROM stg.dim_membership_event_type WHERE membership_event_type_key = 7)
WHERE is_hard_cancellation_from_ecom;

CREATE OR REPLACE TEMP TABLE _fact_membership_event__stg AS
SELECT
    customer_id,
    meta_original_customer_id,
    store_id,
    order_id,
	session_id,
	membership_event_type_key,
    membership_event_type,
    membership_type_detail,
    membership_state,
    event_start_local_datetime,
    COALESCE(DATEADD(MILLISECOND, -1, LEAD(event_start_local_datetime) OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime)),'9999-12-31') AS event_end_local_datetime,
	recent_activation_local_datetime,
	is_scrubs_customer,
    CASE
	    WHEN is_deleted THEN FALSE
	    WHEN COALESCE(LEAD(event_start_local_datetime)
	        OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime), '9999-12-31') = '9999-12-31' THEN TRUE
	    ELSE FALSE
	END AS is_current,
    is_deleted,
    is_test_customer
FROM (
    SELECT
        customer_id,
        meta_original_customer_id,
        store_id,
        order_id,
        session_id,
        membership_event_type_key,
        membership_event_type,
        membership_type_detail,
        membership_state,
        event_start_local_datetime,
        recent_activation_local_datetime,
        is_scrubs_customer,
        is_deleted,
        is_test_customer
    FROM _fact_membership_event__pre_stg
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, event_start_local_datetime
        ORDER BY membership_event_type, membership_type_detail) = 1
    ) AS s;

-- Delete the orphans
INSERT INTO _fact_membership_event__stg (
    customer_id,
    meta_original_customer_id,
    store_id,
    order_id,
	session_id,
	membership_event_type_key,
    membership_event_type,
    membership_type_detail,
    membership_state,
    event_start_local_datetime,
    event_end_local_datetime,
	recent_activation_local_datetime,
    is_scrubs_customer,
    is_current,
    is_deleted,
    is_test_customer
    )
SELECT
    t.customer_id,
    t.meta_original_customer_id,
    t.store_id,
    t.order_id,
	t.session_id,
	t.membership_event_type_key,
    t.membership_event_type,
    t.membership_type_detail,
    membership_state,
    t.event_start_local_datetime,
    t.event_end_local_datetime,
    t.recent_activation_local_datetime,
    t.is_scrubs_customer,
    FALSE AS is_current,
    TRUE AS is_deleted,
    t.is_test_customer
FROM stg.fact_membership_event AS t
    JOIN _fact_membership_event__customer_base AS base
        ON base.customer_id = t.customer_id
WHERE NOT NVL(t.is_deleted, FALSE)
    AND NOT EXISTS (
        SELECT 1
        FROM _fact_membership_event__stg AS s
        WHERE s.customer_id = t.customer_id
            AND s.event_start_local_datetime = t.event_start_local_datetime
        );

INSERT INTO stg.fact_membership_event_stg (
	customer_id,
    meta_original_customer_id,
	store_id,
	order_id,
	session_id,
	membership_event_type_key,
    membership_event_type,
	membership_type_detail,
	membership_state,
	event_start_local_datetime,
	event_end_local_datetime,
	recent_activation_local_datetime,
    is_scrubs_customer,
	is_current,
    is_deleted,
    is_test_customer,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    customer_id,
    meta_original_customer_id,
	store_id,
	order_id,
	session_id,
	membership_event_type_key,
    membership_event_type,
	membership_type_detail,
	membership_state,
	event_start_local_datetime,
	event_end_local_datetime,
	recent_activation_local_datetime,
	is_scrubs_customer,
	is_current,
    is_deleted,
    is_test_customer,
    $execution_start_time AS meta_create_datetime,
	$execution_start_time AS meta_update_datetime
FROM _fact_membership_event__stg
ORDER BY
    customer_id,
    event_start_local_datetime;
