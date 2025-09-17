SET target_table = 'stg.fact_activation_loyalty_tier';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table,NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));


SET wm_edw_stg_fact_membership_event = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_membership_event'));
SET wm_edw_stg_fact_activation = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_activation'));
SET wm_lake_consolidated_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership'));
SET wm_lake_consolidated_ultra_merchant_membership_reward_signal = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_reward_signal'));
SET wm_lake_consolidated_ultra_merchant_history_membership_reward_tier = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.membership_reward_tier'));
/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

CREATE OR REPLACE TEMP TABLE _fact_activation_loyalty_tier__customer_base (customer_id INT);

INSERT INTO _fact_activation_loyalty_tier__customer_base (customer_id)
-- Full Refresh
SELECT DISTINCT fme.customer_id
FROM stg.fact_membership_event AS fme
WHERE $is_full_refresh
UNION ALL
-- Incremental Refresh
SELECT DISTINCT incr.customer_id
FROM (
    SELECT customer_id
    FROM stg.fact_membership_event
    WHERE meta_update_datetime > $wm_edw_stg_fact_membership_event

    UNION ALL

    SELECT customer_id
    FROM stg.fact_activation
    WHERE meta_update_datetime > $wm_edw_stg_fact_activation

    UNION ALL

    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership AS m
        LEFT JOIN lake_consolidated.ultra_merchant.membership_reward_signal AS mrs
            ON mrs.membership_id = m.membership_id
    WHERE (m.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership
        OR mrs.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_membership_reward_signal)

    UNION ALL

    SELECT m.customer_id
    FROM lake_consolidated.ultra_merchant.membership AS m
        JOIN lake_consolidated.ultra_merchant.membership_reward_signal AS mrs
            ON mrs.membership_id = m.membership_id
        JOIN lake_consolidated.ultra_merchant_history.membership_reward_tier AS mrt
            ON mrt.membership_reward_tier_id = mrs.membership_reward_tier_id
    WHERE mrt.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_history_membership_reward_tier

    UNION ALL

    SELECT customer_id
    FROM excp.fact_membership_event
    WHERE meta_is_current_excp
    AND meta_data_quality = 'error'
) as incr
WHERE NOT $is_full_refresh;

CREATE OR REPLACE TEMP TABLE _fact_activation_loyalty_tier__membership_event_base AS
SELECT
    fme.customer_id,
    fme.meta_original_customer_id,
    fme.membership_event_key,
    fa.activation_key,
    fme.store_id,
    fme.order_id,
    fme.session_id,
    fme.membership_event_type,
    fme.membership_type_detail,
    fme.event_start_local_datetime,
    fme.event_end_local_datetime,
    fme.recent_activation_local_datetime,
    LEAD(fme.membership_event_type)
         OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime) AS next_fme_event,
    LEAD(fme.event_start_local_datetime)
         OVER (PARTITION BY fme.customer_id ORDER BY fme.event_start_local_datetime) AS next_fme_event_datetime,
    IFF(fme.membership_event_type ilike '%Failed Activation%', TRUE, FALSE) AS is_ignore
FROM _fact_activation_loyalty_tier__customer_base AS base
    JOIN stg.fact_membership_event AS fme
         ON fme.customer_id = base.customer_id
    LEFT JOIN stg.fact_activation AS fa
         ON fa.membership_event_key = fme.membership_event_key
         AND NOT NVL(fa.is_deleted, FALSE)
WHERE NOT NVL(fme.is_deleted, FALSE)
    AND fme.membership_event_type IN ('Activation', 'Cancellation', 'Failed Activation from Lead', 'Failed Activation from Guest', 'Failed Activation from Classic VIP', 'Failed Activation from Cancel');

-- Ignoring activations that occurred before a failed activation (because that technically is a failed activation)
-- Doing in beginning so we don't wrap reward signals against a failed activation
UPDATE _fact_activation_loyalty_tier__membership_event_base
SET is_ignore = TRUE
WHERE membership_event_type = 'Activation'
    AND next_fme_event ilike '%Failed Activation%'
    AND DATEDIFF(DAY, event_start_local_datetime::DATE, next_fme_event_datetime::DATE) = 0;

CREATE OR REPLACE TEMP TABLE _fact_activation_loyalty_tier__reward_signal_base AS
SELECT
    base.customer_id,
    mrs.membership_reward_tier_id,
    CONVERT_TIMEZONE(stz.time_zone, mrs.datetime_added::TIMESTAMP_TZ(3)) AS datetime_added,
    mrs.membership_tier_points,
    mrt.label AS membership_reward_tier,
    mrt.required_points_earned
FROM _fact_activation_loyalty_tier__customer_base AS base
    JOIN lake_consolidated.ultra_merchant.membership AS m
         ON m.customer_id = base.customer_id
    JOIN lake_consolidated.ultra_merchant.membership_reward_signal AS mrs
         ON mrs.membership_id = m.membership_id
    JOIN reference.store_timezone AS stz
         ON stz.store_id = m.store_id
    LEFT JOIN lake_consolidated.ultra_merchant_history.membership_reward_tier AS mrt
         ON mrt.membership_reward_tier_id = mrs.membership_reward_tier_id
         AND mrs.datetime_added BETWEEN mrt.effective_start_datetime AND mrt.effective_end_datetime
WHERE mrs.membership_reward_tier_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY mrs.membership_id, mrs.datetime_added ORDER BY mrs.membership_tier_points DESC) = 1;

CREATE OR REPLACE TEMP TABLE _fact_activation_loyalty_tier__reward_signals AS
SELECT
    customer_id,
    meta_original_customer_id,
    membership_event_key,
    activation_key,
    store_id,
    order_id,
    session_id,
    membership_event_loyalty_tier_type,
    membership_type_detail,
    membership_reward_tier,
    event_local_datetime,
    recent_activation_local_datetime,
    membership_tier_points,
    required_points_earned,
    membership_reward_tier_id,
    is_ignore,
    LAG(membership_event_loyalty_tier_type) OVER (PARTITION BY customer_id ORDER BY event_local_datetime) AS prior_event,
    LAG(membership_reward_tier) OVER (PARTITION BY customer_id ORDER BY event_local_datetime) AS prior_reward_tier,
    LAG(event_local_datetime) OVER (PARTITION BY customer_id ORDER BY event_local_datetime) AS prior_event_datetime
FROM (
    SELECT
        base.customer_id,
        base.meta_original_customer_id,
        base.membership_event_key,
        base.activation_key,
        base.store_id,
        base.order_id,
        base.session_id,
        base.membership_event_type AS membership_event_loyalty_tier_type,
        base.membership_type_detail,
        CASE
            WHEN base.membership_event_type = 'Activation'
                AND base.membership_type_detail <> 'Classic'
                AND mrs.membership_reward_tier IS NULL
                AND mlt.store_id IS NOT NULL
                THEN mlt.membership_reward_tier
            WHEN base.membership_event_type = 'Activation'
                AND base.membership_type_detail <> 'Classic'
                AND mrs.membership_reward_tier IS NULL
                THEN 'VIP'
            ELSE mrs.membership_reward_tier
        END AS membership_reward_tier,
        base.event_start_local_datetime AS event_local_datetime,
        base.recent_activation_local_datetime,
        mrs.membership_tier_points,
        mrs.required_points_earned,
        mrs.membership_reward_tier_id,
        base.is_ignore
    FROM _fact_activation_loyalty_tier__membership_event_base AS base
        LEFT JOIN _fact_activation_loyalty_tier__reward_signal_base AS mrs
            ON mrs.customer_id = base.customer_id
            AND mrs.datetime_added::date = base.event_start_local_datetime::date
            AND abs(datediff(second, mrs.datetime_added,base.event_start_local_datetime)) <= 120
            AND base.membership_event_type = 'Activation'
            AND base.membership_type_detail <> 'Classic'
            AND NOT base.is_ignore
        LEFT JOIN reference.membership_loyalty_tier AS mlt
            ON mlt.store_id = base.store_id
            AND base.event_start_local_datetime::date >= mlt.launch_date
    QUALIFY ROW_NUMBER() OVER(PARTITION BY base.customer_id, base.event_start_local_datetime ORDER BY mrs.datetime_added DESC) = 1
    UNION ALL
    SELECT
        base.customer_id,
        base.meta_original_customer_id,
        base.membership_event_key,
        base.activation_key,
        base.store_id,
        base.order_id,
        base.session_id,
        'VIP Loyalty Tier Change' AS membership_event_loyalty_tier_type,
        base.membership_type_detail,
        mrs.membership_reward_tier,
        mrs.datetime_added AS event_local_datetime,
        base.recent_activation_local_datetime,
        mrs.membership_tier_points,
        mrs.required_points_earned,
        mrs.membership_reward_tier_id,
        base.is_ignore
    FROM _fact_activation_loyalty_tier__membership_event_base AS base
        JOIN _fact_activation_loyalty_tier__reward_signal_base AS mrs
            ON mrs.customer_id = base.customer_id
            AND mrs.datetime_added > base.event_start_local_datetime
            AND mrs.datetime_added < base.event_end_local_datetime
    WHERE base.membership_event_type = 'Activation'
        AND base.membership_type_detail <> 'Classic'
        AND NOT base.is_ignore
    UNION ALL
    SELECT
        base.customer_id,
        base.meta_original_customer_id,
        base.membership_event_key,
        base.activation_key,
        base.store_id,
        base.order_id,
        base.session_id,
        'Loyalty Program Migration' AS membership_event_loyalty_tier_type,
        base.membership_type_detail,
        lpme.membership_reward_tier,
        lpme.event_local_datetime,
        base.recent_activation_local_datetime,
        NULL AS membership_tier_points,
        NULL AS required_points_earned,
        lpme.membership_reward_tier_id,
        base.is_ignore
    FROM _fact_activation_loyalty_tier__membership_event_base AS base
        JOIN reference.loyalty_program_migration_events as lpme
            ON lpme.customer_id = base.customer_id
            AND lpme.event_local_datetime > base.event_start_local_datetime
            AND lpme.event_local_datetime < base.event_end_local_datetime
    WHERE base.membership_event_type = 'Activation'
        AND base.membership_type_detail <> 'Classic'
        AND NOT base.is_ignore
    ) AS a;

UPDATE _fact_activation_loyalty_tier__reward_signals
SET is_ignore = TRUE
WHERE membership_event_loyalty_tier_type = 'VIP Loyalty Tier Change'
    AND prior_event = 'Activation'
    and DATEDIFF(SECOND,prior_event_datetime, event_local_datetime) <= 120
    and EQUAL_NULL(prior_reward_tier, membership_reward_tier);

CREATE OR REPLACE TEMP TABLE _fact_activation_loyalty_tier__pre_stg AS
SELECT
    rs.customer_id,
    rs.meta_original_customer_id,
    rs.membership_event_key,
    rs.activation_key,
    rs.store_id,
    rs.order_id,
    rs.session_id,
    rs.membership_event_loyalty_tier_type,
    rs.membership_type_detail,
    rs.membership_reward_tier,
    rs.event_local_datetime AS event_start_local_datetime,
    LEAD(DATEADD(MILLISECOND, -1, rs.event_local_datetime), 1, '9999-12-31') OVER (PARTITION BY rs.customer_id ORDER BY rs.event_local_datetime) AS event_end_local_datetime,
    rs.recent_activation_local_datetime,
    rs.membership_tier_points,
    rs.required_points_earned,
    rs.membership_reward_tier_id,
    IFF(tc.customer_id IS NOT NULL, TRUE, FALSE) AS is_test_customer
FROM _fact_activation_loyalty_tier__reward_signals AS rs
    LEFT JOIN reference.test_customer AS tc
        ON tc.customer_id = rs.customer_id
WHERE NOT rs.is_ignore;

CREATE OR REPLACE TEMP TABLE _fact_activation_loyalty_tier__stg AS
SELECT /* History with no matching values in underlying table */
    falt.customer_id,
    falt.meta_original_customer_id,
    falt.membership_event_key,
    falt.activation_key,
    falt.store_id,
    falt.order_id,
    falt.session_id,
    falt.membership_event_loyalty_tier_type,
    falt.membership_type_detail,
    falt.membership_reward_tier,
    falt.event_start_local_datetime,
    falt.event_end_local_datetime,
    falt.recent_activation_local_datetime,
    falt.membership_tier_points,
    falt.required_points_earned,
    falt.membership_reward_tier_id,
    FALSE AS is_current,
    TRUE AS is_deleted,
    falt.is_test_customer,
    falt.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.fact_activation_loyalty_tier AS falt
    JOIN _fact_activation_loyalty_tier__customer_base AS base
        ON base.customer_id = falt.customer_id
WHERE NOT NVL(falt.is_deleted, FALSE)
    AND NOT EXISTS (
        SELECT 1
        FROM _fact_activation_loyalty_tier__pre_stg AS pstg
        WHERE pstg.customer_id = falt.customer_id
            AND pstg.event_start_local_datetime = falt.event_start_local_datetime
        )
UNION ALL
SELECT
    customer_id,
    meta_original_customer_id,
    membership_event_key,
    activation_key,
    store_id,
    order_id,
    session_id,
    membership_event_loyalty_tier_type,
    membership_type_detail,
    membership_reward_tier,
    event_start_local_datetime,
    event_end_local_datetime,
    recent_activation_local_datetime,
    membership_tier_points,
    required_points_earned,
    membership_reward_tier_id,
    IFF(event_end_local_datetime = '9999-12-31', TRUE, FALSE) is_current,
    FALSE AS is_deleted,
    is_test_customer,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_activation_loyalty_tier__pre_stg
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, event_start_local_datetime ORDER BY event_end_local_datetime DESC) = 1;

INSERT INTO stg.fact_activation_loyalty_tier_stg (
	customer_id,
    meta_original_customer_id,
	membership_event_key,
    activation_key,
    store_id,
	order_id,
	session_id,
	membership_event_loyalty_tier_type,
    membership_type_detail,
    membership_reward_tier,
    event_start_local_datetime,
	event_end_local_datetime,
	recent_activation_local_datetime,
	membership_tier_points,
	required_points_earned,
    membership_reward_tier_id,
    is_current,
    is_deleted,
    is_test_customer,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
	customer_id,
	meta_original_customer_id,
	membership_event_key,
    activation_key,
    store_id,
	order_id,
	session_id,
	membership_event_loyalty_tier_type,
    membership_type_detail,
    membership_reward_tier,
    event_start_local_datetime,
	event_end_local_datetime,
	recent_activation_local_datetime,
	membership_tier_points,
	required_points_earned,
	membership_reward_tier_id,
    is_current,
    is_deleted,
    is_test_customer,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_activation_loyalty_tier__stg
ORDER BY
    customer_id,
    event_start_local_datetime;
