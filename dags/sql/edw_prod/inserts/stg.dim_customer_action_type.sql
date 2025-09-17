TRUNCATE TABLE stg.dim_customer_action_type;

INSERT INTO stg.dim_customer_action_type
	(
        customer_action_type_key,
		customer_action_type,
		effective_start_datetime,
		effective_end_datetime,
		is_current,
		meta_create_datetime,
		meta_update_datetime
	)
VALUES
    (-1, 'Unknown', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (1, 'Placed Order - Cash', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (2, 'Placed Order - Non-Cash', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (3, 'Billed Credit', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (4, 'Billed Credit - Failed Processing', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (5, 'Skipped Month', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (6, 'Cancelled Membership', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (7, 'Passive Cancelled Membership', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (8, 'Billed Membership Fee', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (9, 'Billed Membership Fee - Failed Processing', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp);
