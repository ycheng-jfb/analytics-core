TRUNCATE TABLE stg.dim_customer_action_category;

INSERT INTO stg.dim_customer_action_category
(
    customer_action_category,
    is_cash_purchase,
    is_non_cash_purchase,
    is_merch_purchase,
    is_purchase,
    is_skip,
    is_credit_billing,
    is_failed_billing,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
)
VALUES
    ('Billed Credit - Failed / Purchased Order - Cash', 1, 0, 1, 1, 0, 0, 1, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Billed Credit - Failed / Purchased Order - Cash / Purchased Order - Non Cash', 1, 1, 1, 1, 0, 0, 1, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Billed Credit - Failed / Purchased Order - Non Cash', 0, 1, 1, 1, 0, 0, 1, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Billed Credit - Pending', 0, 0, 0, 0, 0, 0, 1, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Billed Credit - Succeeded', 1, 0, 0, 1, 0, 1, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Billed Credit - Succeeded / Purchased Order - Cash', 1, 0, 1, 1, 0, 1, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Billed Credit - Succeeded / Purchased Order - Cash / Purchased Order - Non Cash', 1, 1, 1, 1, 0, 1, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Billed Credit - Succeeded / Purchased Order - Non Cash', 1, 1, 1, 1, 0, 1, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Cancelled Membership', 0, 0, 0, 0, 0, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Billed Credit - Failed', 0, 0, 0, 0, 0, 0, 1, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('No Action', 0, 0, 0, 0, 0, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Other', 0, 0, 0, 0, 0, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Other - Cash', 1, 0, 0, 1, 0, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Purchased Order - Cash', 1, 0, 1, 1, 0, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Purchased Order - Cash / Purchased Order - Non Cash', 1, 1, 1, 1, 0, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Purchased Order - Non Cash', 0, 1, 1, 1, 0, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Skipped Month', 0, 0, 0, 0, 1, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Skipped Month / Purchased Order - Cash', 1, 0, 1, 1, 1, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Skipped Month / Purchased Order - Cash / Purchased Order - Non Cash', 1, 1, 1, 1, 1, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp),
    ('Skipped Month / Purchased Order - Non Cash', 0, 1, 1, 1, 1, 0, 0, '1900-01-01', '9999-12-31', 1, current_timestamp, current_timestamp);
