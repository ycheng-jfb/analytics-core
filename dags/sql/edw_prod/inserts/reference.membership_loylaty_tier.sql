/*
 Brands are launching new membership loyalty tier levels.  New vips were given a "VIP" tier and would default to that value if they were missing a record
 in ultra_merchant.membership_reward_signal.  This table stores store_id, the new tier launch date and the base reward tier level given to new vips
 */
SET execution_start_time = current_timestamp;
TRUNCATE TABLE reference.membership_loyalty_tier;
INSERT INTO reference.membership_loyalty_tier
(
    store_id,
    launch_date,
    membership_reward_tier,
    meta_create_datetime,
    meta_update_datetime
)
VALUES
    (52, '2024-01-08', 'Gold', $execution_start_time, $execution_start_time),
    (241, '2024-01-08', 'Gold', $execution_start_time, $execution_start_time),
    (65, '2023-11-09', 'Gold', $execution_start_time, $execution_start_time);

