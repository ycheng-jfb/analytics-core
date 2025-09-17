CREATE OR REPLACE VIEW data_model_fl.fact_activation_loyalty_tier
(
    activation_loyalty_tier_key,
    customer_id,
    membership_event_key,
    activation_key,
    store_id,
    order_id,
    session_id,
    membership_event_loyalty_tier_type,
    membership_type_detail,
    membership_reward_tier,
    membership_tier_points,
    event_start_local_datetime,
    event_end_local_datetime,
    recent_activation_local_datetime,
    required_points_earned,
    is_current,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    activation_loyalty_tier_key,
    stg.udf_unconcat_brand(customer_id) AS customer_id,
    membership_event_key,
    activation_key,
    store_id,
    stg.udf_unconcat_brand(order_id) AS order_id,
    stg.udf_unconcat_brand(session_id) AS session_id,
    membership_event_loyalty_tier_type,
    membership_type_detail,
    membership_reward_tier,
    membership_tier_points,
    event_start_local_datetime,
    event_end_local_datetime,
    recent_activation_local_datetime,
    required_points_earned,
    is_current,
    meta_create_datetime,
    meta_update_datetime
FROM stg.fact_activation_loyalty_tier
WHERE NOT is_deleted
    AND NOT NVL(is_test_customer, FALSE)
    AND substring(customer_id, -2) = '20';
