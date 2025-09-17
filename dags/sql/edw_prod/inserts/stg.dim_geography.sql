TRUNCATE TABLE stg.dim_geography;

INSERT INTO stg.dim_geography
	(
        geography_key,
		sub_region,
		region,
		effective_start_datetime,
		effective_end_datetime,
		is_current,
		meta_create_datetime,
		meta_update_datetime
	)
VALUES
    (-1, 'Unknown', 'Unknown', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (1, 'US', 'NA', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (2, 'CA', 'NA', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (3, 'EU', 'EU', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (4, 'UK', 'EU', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp);
