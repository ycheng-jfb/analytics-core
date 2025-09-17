CREATE OR REPLACE VIEW LAKE_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL AS
SELECT
    customer_detail_id,
    customer_id,
    name,
    value,
    datetime_added,
    datetime_modified,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.customer_detail s
WHERE NOT exists(
        SELECT
            1
        FROM lake_archive.ultra_merchant_cdc.customer_detail__del cd
        WHERE cd.customer_detail_id = s.customer_detail_id
    );
