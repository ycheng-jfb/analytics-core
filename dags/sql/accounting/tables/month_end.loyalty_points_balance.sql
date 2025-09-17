CREATE TRANSIENT TABLE IF NOT EXISTS month_end.loyalty_points_balance (
	mrt_key NUMBER(38,0),
	store_id NUMBER(38,0),
	membership_id NUMBER(38,0),
	membership_reward_plan_id NUMBER(38,0),
	membership_reward_tier_id NUMBER(38,0),
	membership_reward_tier VARCHAR(16777216),
	points NUMBER(38,0),
	fixed_points NUMBER(38,0),
	membership_reward_transaction_type_id NUMBER(38,0),
	datetime_added TIMESTAMP_NTZ(9),
	raw_balance NUMBER(38,0),
	fixed_balance NUMBER(38,0),
	rnk NUMBER(38,0)
);
