CREATE OR REPLACE TABLE stg.lkp_membership_cancel_method
(
        membership_event_key int,
        customer_id int,
        meta_original_customer_id int,
        cancellation_local_datetime timestamp_tz,
        cancel_method varchar,
        cancel_reason varchar,
        meta_row_hash int,
        meta_create_datetime timestamp_ltz,
        meta_update_datetime timestamp_ltz,
        PRIMARY KEY(membership_event_key)
);
