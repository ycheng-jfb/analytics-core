CREATE TRANSIENT TABLE IF NOT EXISTS month_end.loyalty_points (
	store_id NUMBER(38,0),
	membership_reward_plan_id NUMBER(38,0),
	membership_reward_tier_id NUMBER(38,0),
	membership_reward_tier VARCHAR(16777216),
	membership_reward_transaction_id NUMBER(38,0),
	membership_id NUMBER(38,0),
	customer_id NUMBER(38,0),
	membership_reward_transaction_type_id NUMBER(38,0),
	object_id NUMBER(38,0),
	points NUMBER(38,0),
	datetime_added TIMESTAMP_NTZ(9),
	date_added DATE,
	meta_create_datetime TIMESTAMP_LTZ(9),
	meta_update_datetime TIMESTAMP_LTZ(9)
);
