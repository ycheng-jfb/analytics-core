SET target_table = 'stg.fact_customer_action';
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
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_lake_consolidated_ultra_merchant_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer'));
SET wm_lake_consolidated_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));
SET wm_lake_consolidated_ultra_merchant_order_classification = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_classification'));
SET wm_lake_consolidated_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership'));
SET wm_lake_consolidated_ultra_merchant_membership_period = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_period'));
SET wm_lake_consolidated_ultra_merchant_membership_skip = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_skip'));
SET wm_lake_consolidated_ultra_merchant_period = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.period'));
SET wm_lake_consolidated_ultra_merchant_session = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.session'));
SET wm_edw_stg_fact_membership_event = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_membership_event'));

/*
SELECT
    $wm_self,
    $wm_lake_consolidated_ultra_merchant_customer,
    $wm_lake_consolidated_ultra_merchant_order,
    $wm_lake_consolidated_ultra_merchant_order_classification,
    $wm_lake_consolidated_ultra_merchant_membership,
    $wm_lake_consolidated_ultra_merchant_membership_period,
    $wm_lake_consolidated_ultra_merchant_membership_skip,
    $wm_lake_consolidated_ultra_merchant_period,
    $wm_lake_consolidated_ultra_merchant_session,
    $wm_edw_stg_fact_membership_event
 */

CREATE OR REPLACE TEMP TABLE _fact_customer_action__customer_base (customer_id INT);

-- Full Refresh
INSERT INTO _fact_customer_action__customer_base (customer_id)
SELECT DISTINCT customer_id
FROM lake_consolidated.ultra_merchant.customer
WHERE $is_full_refresh  /* SET is_full_refresh = TRUE; */
ORDER BY customer_id;

-- Incremental Refresh
INSERT INTO _fact_customer_action__customer_base (customer_id)
SELECT DISTINCT incr.customer_id
FROM (
    /* Self-check for manual updates */
    SELECT fca.customer_id
    FROM stg.fact_customer_action AS fca
    WHERE fca.meta_update_datetime > $wm_self

    UNION ALL

    /* Check for dependency table updates */
    SELECT c.customer_id
    FROM lake_consolidated.ultra_merchant.customer AS c
        LEFT JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.customer_id = c.customer_id
        LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
            ON oc.order_id = o.order_id
            AND oc.order_type_id IN (9,10,35,39)
    WHERE (c.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_customer
        OR o.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_order
        OR oc.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_order_classification)

    UNION ALL

    SELECT fme.customer_id
    FROM stg.fact_membership_event AS fme
        JOIN stg.dim_membership_event_type AS dmet
            ON dmet.membership_event_type_key = fme.membership_event_type_key
	WHERE dmet.membership_event_type = 'Cancellation'
		AND fme.meta_update_datetime > $wm_edw_stg_fact_membership_event
        AND NOT NVL(fme.is_deleted, FALSE)

    UNION ALL

    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership AS m
        LEFT JOIN lake_consolidated.ultra_merchant.membership_period AS mp
            ON mp.membership_id = m.membership_id
    WHERE (m.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership
        OR mp.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership_period)

    UNION ALL

    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership AS m
        LEFT JOIN lake_consolidated.ultra_merchant.membership_skip AS ms
            ON ms.membership_id = m.membership_id
        LEFT JOIN lake_consolidated.ultra_merchant.period AS p
            ON p.period_id = ms.period_id
            AND LOWER(p.type) = 'monthly'
	    LEFT JOIN lake_consolidated.ultra_merchant.session AS s
            ON s.session_id = ms.session_id
    WHERE (ms.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership_skip
        OR p.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_period
        OR s.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_session)

    UNION ALL

    /* Previously errored rows */
    SELECT customer_id
    FROM excp.fact_customer_action
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.customer_id;
-- SELECT * FROM _fact_customer_action__customer_base;

CREATE OR REPLACE TEMPORARY TABLE _fact_customer_action__stg (
	customer_id INT,
	customer_action_type VARCHAR(50),
	store_id INT,
	order_id INT,
    meta_original_order_id INT,
	customer_action_local_datetime DATETIME,
	customer_action_period_date DATETIME
    );

INSERT INTO _fact_customer_action__stg (
	customer_id,
	customer_action_type,
	store_id,
	order_id,
    meta_original_order_id,
	customer_action_local_datetime,
	customer_action_period_date
    )
SELECT DISTINCT
    customer_id,
	customer_action_type,
	store_id,
	order_id,
	meta_original_order_id,
	customer_action_local_datetime,
	customer_action_period_date
FROM (
    SELECT
        o.customer_id,
        CASE WHEN oc.order_type_id IN (9,39) THEN 'Billed Membership Fee'
            WHEN oc.order_type_id = 10 THEN 'Billed Credit'
            WHEN (o.subtotal - o.discount - o.credit) <= 0 THEN 'Placed Order - Non-Cash'
            ELSE 'Placed Order - Cash'
        END AS customer_action_type,
        NULL AS store_id,
        o.order_id,
        o.meta_original_order_id,
        o.datetime_added AS customer_action_local_datetime,
        o.datetime_added AS customer_action_period_date
    FROM _fact_customer_action__customer_base AS base
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.customer_id = base.customer_id
        LEFT JOIN lake_consolidated.ultra_merchant.order_classification AS oc
            ON oc.order_id = o.order_id
            AND oc.order_type_id IN (9,10,35,39)

    UNION ALL

	-- "Billed Credit - Failed Processing"
	SELECT
	    m.customer_id,
		'Billed Credit - Failed Processing' AS customer_action_type,
		NULL AS store_id,
		-1 AS order_id,
	    -1 AS meta_original_order_id,
		mp.datetime_modified AS customer_action_local_datetime,
		mp.datetime_added AS customer_action_period_date
	FROM _fact_customer_action__customer_base AS base
        JOIN lake_consolidated.ultra_merchant.membership AS m
	        ON m.customer_id = base.customer_id
        JOIN lake_consolidated.ultra_merchant.membership_period AS mp
            ON mp.membership_id = m.membership_id
	WHERE mp.statuscode = 3952  -- Status: Error During Processing
		AND mp.credit_order_id IS NULL  -- valid order_ids are already processed in the above code part from ods.um.order - no need to process again

	UNION ALL

	SELECT
		fme.customer_id,
		IFF(fme.membership_type_detail = 'Regular', 'Cancelled Membership', 'Passive Cancelled Membership') AS customer_action_type,
		NULL AS store_id,
		-1 AS order_id, --Not Applicable
	    -1 AS meta_original_order_id,
		CONVERT_TIMEZONE('America/Los_Angeles',fme.event_start_local_datetime)::TIMESTAMP_NTZ(3) AS customer_action_local_datetime,
		CONVERT_TIMEZONE('America/Los_Angeles',fme.event_start_local_datetime)::TIMESTAMP_NTZ(3) AS customer_action_period_date
	FROM _fact_customer_action__customer_base AS base
        JOIN stg.fact_membership_event AS fme
	        ON fme.customer_id = base.customer_id
        JOIN stg.dim_membership_event_type AS dmet
            ON dmet.membership_event_type_key = fme.membership_event_type_key
	WHERE dmet.membership_event_type = 'Cancellation'
		AND NOT NVL(fme.is_deleted, FALSE)

    UNION ALL

	SELECT
		m.customer_id,
		'Skipped Month' AS customer_action_type,
		s.store_id AS store_id,
		-1 as order_id, --Not Applicable
	    -1 AS meta_original_order_id,
		ms.datetime_added AS customer_action_local_datetime,
		p.date_period_start AS customer_action_period_date
	FROM _fact_customer_action__customer_base AS base
        JOIN lake_consolidated.ultra_merchant.membership AS m
	        ON m.customer_id = base.customer_id
        JOIN lake_consolidated.ultra_merchant.membership_skip AS ms
            ON ms.membership_id = m.membership_id
        JOIN lake_consolidated.ultra_merchant.period AS p
            ON p.period_id = ms.period_id
            AND LOWER(p.type) = 'monthly'
	    LEFT JOIN lake_consolidated.ultra_merchant.session AS s
            ON s.session_id = ms.session_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY m.customer_id, p.date_period_start ORDER BY ms.datetime_added) = 1
    ) AS stg;
-- SELECT * FROM _fact_customer_action__stg WHERE customer_id = 860148409;

INSERT INTO stg.fact_customer_action_stg (
	customer_id,
    meta_original_customer_id,
	customer_action_type,
	order_id,
    meta_original_order_id,
	store_id,
	customer_action_local_datetime,
	customer_action_period_date,
    customer_action_period_local_date,
	event_count,
	meta_create_datetime,
	meta_update_datetime
    )
SELECT
    customer_id,
    meta_original_customer_id,
	customer_action_type,
	order_id,
	meta_original_order_id,
	store_id,
	customer_action_local_datetime,
	customer_action_period_date,
    customer_action_period_local_date,
	1 AS event_count,
	$execution_start_time AS meta_create_datetime,
	$execution_start_time AS meta_update_datetime
FROM (
    SELECT
        stg.customer_id,
        c.meta_original_customer_id,
        stg.customer_action_type,
        stg.order_id,
        stg.meta_original_order_id,
        COALESCE(stg.store_id, c.store_id) AS store_id,
        CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(stg.customer_action_local_datetime, 'America/Los_Angeles')) AS customer_action_local_datetime,
        DATE_TRUNC('month', public.udf_to_timestamp_tz(stg.customer_action_period_date, 'America/Los_Angeles'))::DATE AS customer_action_period_date,
        DATE_TRUNC('month', CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(stg.customer_action_period_date, 'America/Los_Angeles')))::DATE AS customer_action_period_local_date
    FROM _fact_customer_action__stg AS stg
        JOIN lake_consolidated.ultra_merchant.customer AS c
            ON c.customer_id = stg.customer_id
        LEFT JOIN reference.store_timezone AS stz
            ON stz.store_id = COALESCE(stg.store_id, c.store_id)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.customer_id, stg.order_id, stg.customer_action_local_datetime ORDER BY stg.customer_action_period_date DESC) = 1
    ) AS stg;
