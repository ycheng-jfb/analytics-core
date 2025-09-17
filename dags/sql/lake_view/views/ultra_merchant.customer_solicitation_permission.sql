CREATE OR REPLACE VIEW LAKE_VIEW.ULTRA_MERCHANT.CUSTOMER_SOLICITATION_PERMISSION AS
SELECT
    customer_solicitation_permission_id,
    customer_id,
    type,
    object,
    object_id,
    ip,
    datetime_added,
    datetime_modified,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.customer_solicitation_permission s
WHERE NOT exists(
        SELECT
            1
        FROM lake_archive.ultra_merchant_cdc.customer_solicitation_permission__del cd
        WHERE cd.customer_solicitation_permission_id = s.customer_solicitation_permission_id
    );
