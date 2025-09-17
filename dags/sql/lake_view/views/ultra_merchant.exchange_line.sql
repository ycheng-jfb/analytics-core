CREATE OR REPLACE VIEW LAKE_VIEW.ULTRA_MERCHANT.EXCHANGE_LINE AS
SELECT
    exchange_line_id,
    exchange_id,
    rma_product_id,
    original_product_id,
    exchange_product_id,
    price_difference,
    price,
    datetime_added,
    datetime_modified,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.exchange_line s
WHERE NOT exists(
        SELECT
            1
        FROM lake_archive.ultra_merchant_cdc.exchange_line__del cd
        WHERE cd.exchange_line_id = s.exchange_line_id
    );
