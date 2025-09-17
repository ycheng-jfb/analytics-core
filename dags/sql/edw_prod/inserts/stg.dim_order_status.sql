TRUNCATE TABLE stg.dim_order_status;

INSERT INTO stg.dim_order_status
	(
        order_status_key,
		order_status,
		effective_start_datetime,
		effective_end_datetime,
		is_current,
		meta_create_datetime,
		meta_update_datetime
	)
VALUES
    (-1, 'Unknown', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (1, 'Success', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (2, 'Pending', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (3, 'Cancelled', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (4, 'Failure', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (5, 'Pre-Order Split', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (6, 'BOPS Split', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (7, 'On Hold', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (8, 'DropShip Split', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp);
