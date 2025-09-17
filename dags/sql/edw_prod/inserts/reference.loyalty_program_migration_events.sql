SET execution_start_time = current_timestamp;
TRUNCATE TABLE reference.loyalty_program_migration_events;

INSERT INTO reference.loyalty_program_migration_events
SELECT
    customer_id,
    store_id,
    membership_reward_tier_id,
    membership_reward_tier as old_membership_reward_tier,
    current_membership_reward_tier as membership_reward_tier,
    IFF(event_start_local_datetime::date = launch_date::date,
        DATEADD(MILLISECOND, 1, event_start_local_datetime),
        launch_date::timestamp_tz)
    as event_local_datetime,
    $execution_start_time as meta_create_datetime,
    $execution_start_time as meta_udpate_datetime
FROM (SELECT falt.customer_id,
             falt.store_id,
             falt.membership_event_loyalty_tier_type,
             falt.membership_reward_tier_id,
             falt.membership_reward_tier,
             mrt.label AS current_membership_reward_tier,
             falt.event_start_local_datetime,
             mlt.launch_date
      FROM stg.fact_activation_loyalty_tier AS falt
               JOIN reference.membership_loyalty_tier AS mlt ON mlt.store_id = falt.store_id
               LEFT JOIN lake_consolidated.ultra_merchant.membership_reward_tier AS mrt
                         ON mrt.membership_reward_tier_id = falt.membership_reward_tier_id
      WHERE event_start_local_datetime::date <= mlt.launch_date
        AND is_deleted = FALSE
        AND is_test_customer = FALSE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_start_local_datetime DESC) = 1
      )
WHERE membership_event_loyalty_tier_type <> 'Cancellation'
AND NOT equal_null(membership_reward_tier, current_membership_reward_tier);
