INSERT INTO EDW_PROD.stg.dim_membership_event_type
        (
        membership_event_type_key,
                membership_event_type,
                effective_start_datetime,
                effective_end_datetime,
                is_current,
                meta_create_datetime,
                meta_update_datetime
        )
VALUES
        (-1, 'Unknown', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (1, 'Email Signup', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (2, 'Registration', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (3, 'Activation', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (4, 'Failed Activation', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (5, 'Cancellation', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (6, 'Guest Purchasing Member', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (7, 'Hard Cancellation from E-Comm', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (8, 'Free Trial Activation', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (9, 'Free Trial Downgrade', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    (10, 'Deactivated Lead', '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp);