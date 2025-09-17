CREATE OR REPLACE VIEW LAKE_VIEW.ULTRA_MERCHANT.CUSTOMER_ADDRESS AS
SELECT
    customer_address_id,
    customer_id,
    address_id,
    datetime_added,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.customer_address s
WHERE NOT exists(
        SELECT
            1
        FROM lake_archive.ultra_merchant_cdc.customer_address__del cd
        WHERE cd.customer_address_id = s.customer_address_id
    );
