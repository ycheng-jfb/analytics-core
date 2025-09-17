CREATE TABLE reference.membership_loyalty_tier (
    store_id INT,
	launch_date DATE,
	membership_reward_tier VARCHAR(30),
	meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3)
);

