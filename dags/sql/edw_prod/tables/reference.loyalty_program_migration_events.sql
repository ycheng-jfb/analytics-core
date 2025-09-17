CREATE OR REPLACE TABLE reference.loyalty_program_migration_events (
	customer_id NUMBER(38,0),
	store_id NUMBER(38,0),
	membership_reward_tier_id NUMBER(38,0),
	old_membership_reward_tier VARCHAR(50),
	membership_reward_tier VARCHAR(50),
	event_local_datetime TIMESTAMP_TZ(9),
	meta_create_datetime TIMESTAMP_LTZ(9),
	meta_update_datetime TIMESTAMP_LTZ(9)
);
