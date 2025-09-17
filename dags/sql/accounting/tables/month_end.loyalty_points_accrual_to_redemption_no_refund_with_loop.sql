CREATE TRANSIENT TABLE IF NOT EXISTS month_end.loyalty_points_accrual_to_redemption_no_refund_with_loop (
	store_set VARCHAR(50),
	store VARCHAR(50),
	store_abbreviation VARCHAR(25),
	month_issued DATE,
	membership_reward_tier VARCHAR(55),
	redeemed VARCHAR(55),
	gender VARCHAR(5),
	points NUMBER(38,0)
);
