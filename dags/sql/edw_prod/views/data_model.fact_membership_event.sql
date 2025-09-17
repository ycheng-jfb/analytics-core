CREATE OR REPLACE VIEW data_model.fact_membership_event (
    membership_event_key,
    customer_id,
    store_id,
    order_id,
    session_id,
    membership_event_type_key,
    membership_event_type,
    membership_type_detail,
    membership_state,
    event_start_local_datetime,
    event_end_local_datetime,
    recent_activation_local_datetime,
    is_scrubs_customer,
    is_current,
    meta_create_datetime,
    meta_update_datetime
    )
AS
SELECT
    fme.membership_event_key,
    fme.customer_id,
    fme.store_id,
    fme.order_id,
    fme.session_id,
    fme.membership_event_type_key,
    fme.membership_event_type,
    fme.membership_type_detail,
    fme.membership_state,
    fme.event_start_local_datetime,
    fme.event_end_local_datetime,
    fme.recent_activation_local_datetime,
    fme.is_scrubs_customer,
    fme.is_current,
    fme.meta_create_datetime,
    fme.meta_update_datetime
FROM stg.fact_membership_event AS fme
    LEFT JOIN stg.dim_store AS ds
        ON fme.store_id = ds.store_id
WHERE NOT fme.is_deleted
    AND NOT NVL(fme.is_test_customer, FALSE)
    AND ds.store_brand NOT IN ('Legacy')
    AND fme.membership_event_type not in ('Hard Cancellation from E-Comm');
