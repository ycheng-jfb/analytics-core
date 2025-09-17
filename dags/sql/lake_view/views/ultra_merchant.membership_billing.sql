CREATE OR REPLACE VIEW LAKE_VIEW.ULTRA_MERCHANT.MEMBERSHIP_BILLING AS
SELECT
    membership_billing_id,
    membership_id,
    membership_plan_id,
    membership_type_id,
    membership_billing_source_id,
    membership_trial_id,
    activating_order_id,
    order_id,
    period_id,
    period_type,
    error_message,
    datetime_added,
    datetime_modified,
    datetime_activated,
    date_due,
    date_period_start,
    date_period_end,
    statuscode,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.membership_billing s
WHERE NOT exists(
        SELECT
            1
        FROM lake_archive.ultra_merchant_cdc.membership_billing__del cd
        WHERE cd.membership_billing_id = s.membership_billing_id
    );
