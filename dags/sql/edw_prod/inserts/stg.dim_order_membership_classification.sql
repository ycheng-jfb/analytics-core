TRUNCATE TABLE stg.dim_order_membership_classification;

INSERT INTO stg.dim_order_membership_classification
	(
		order_membership_classification_key,
		is_activating,
		is_guest,
		membership_order_type_l1,
		membership_order_type_l2,
		membership_order_type_l3,
		membership_order_type_l4,
        first_repeat_order_type,
		is_vip,
        is_repeat_customer,
	    is_reactivated_vip,
        is_never_vip,
	    is_vip_membership_trial,
        effective_start_datetime,
		effective_end_datetime,
		is_current,
		meta_create_datetime,
		meta_update_datetime
	)
VALUES
    (1,1,1,'NonActivating', 'Guest', 'First Guest', 'First Guest', 'First', 0, 0, 0, 1, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (2,1,0,'Activating VIP', 'Activating VIP', 'Activating VIP', 'New VIP', 'First', 1, 0, 0, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (3,0,1,'NonActivating', 'Guest', 'Repeat Guest', 'Cancelled VIP', 'Repeat', 0, 1, 0, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (4,0,0,'NonActivating', 'Repeat VIP', 'Repeat VIP', 'Repeat VIP', 'Repeat', 1, 1, 0, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (5,1,0,'Activating VIP', 'Activating VIP', 'Activating VIP', 'Reactivated VIP', 'First', 1, 0, 1, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (6,0,1,'NonActivating', 'Guest', 'Repeat Guest', 'Repeat Guest Never VIP', 'Repeat', 0, 1, 0, 1, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (7,0,1,'NonActivating', 'Guest', 'Free Trial VIP', 'Free Trial VIP', 'Repeat', 0, 1, 0, 1, 1, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp);

/*
ALTER TABLE stg.dim_order_membership_classification ADD COLUMN membership_order_type_l4 VARCHAR(50) NULL;
ALTER TABLE stg.dim_order_membership_classification ADD COLUMN is_reactivated_vip BOOLEAN NULL;
ALTER TABLE stg.dim_order_membership_classification ADD COLUMN is_never_vip BOOLEAN NULL;

UPDATE stg.dim_order_membership_classification
SET membership_order_type_l4 = CASE
    WHEN order_membership_classification_key = 1 THEN 'First Guest'
    WHEN order_membership_classification_key = 2 THEN 'New VIP'
    WHEN order_membership_classification_key = 3 THEN 'Cancelled VIP'
    WHEN order_membership_classification_key = 4 THEN 'Repeat VIP'
    WHEN order_membership_classification_key = 5 THEN 'Reactivated VIP'
    WHEN order_membership_classification_key = 6 THEN 'Repeat Guest Never VIP'
    ELSE '<Unknown>' END,
    is_reactivated_vip = CASE
    WHEN is_reactivated_vip = 1 THEN FALSE
    WHEN is_reactivated_vip = 2 THEN FALSE
    WHEN is_reactivated_vip = 3 THEN FALSE
    WHEN is_reactivated_vip = 4 THEN FALSE
    WHEN is_reactivated_vip = 5 THEN TRUE
    WHEN is_reactivated_vip = 6 THEN FALSE
    ELSE FALSE END,
    is_never_vip = CASE
    WHEN is_never_vip = 1 THEN TRUE
    WHEN is_never_vip = 2 THEN FALSE
    WHEN is_never_vip = 3 THEN FALSE
    WHEN is_never_vip = 4 THEN FALSE
    WHEN is_never_vip = 5 THEN FALSE
    WHEN is_never_vip = 6 THEN TRUE
    ELSE FALSE END,
    meta_update_datetime = current_timestamp;

ALTER TABLE stg.dim_order_membership_classification ALTER membership_order_type_l4 NOT NULL;
ALTER TABLE stg.dim_order_membership_classification ALTER is_reactivated_vip NOT NULL;
ALTER TABLE stg.dim_order_membership_classification ALTER is_never_vip NOT NULL;
*/
