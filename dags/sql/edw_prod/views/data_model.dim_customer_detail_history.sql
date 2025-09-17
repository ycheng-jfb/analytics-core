CREATE OR REPLACE VIEW data_model.dim_customer_detail_history (
    customer_detail_history_key,
    customer_id,
    membership_id,
    membership_event_type,
    membership_type_detail,
    membership_plan_id,
    membership_price,
    membership_type,
    is_test_customer,
    is_opt_out,
    is_sms_opt_out,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
    )
AS
SELECT
    dcdh.customer_detail_history_key,
    dcdh.customer_id,
    dcdh.membership_id,
    dcdh.membership_event_type,
    dcdh.membership_type_detail,
    dcdh.membership_plan_id,
    dcdh.membership_price,
    dcdh.membership_type,
    dcdh.is_test_customer,
    dcdh.is_opt_out,
    dcdh.is_sms_opt_out,
    dcdh.effective_start_datetime,
    dcdh.effective_end_datetime,
    dcdh.is_current,
    dcdh.meta_create_datetime,
    dcdh.meta_update_datetime
FROM stg.dim_customer_detail_history AS dcdh
WHERE NOT dcdh.is_test_customer
    AND NOT NVL(dcdh.is_deleted, FALSE);
