CREATE OR REPLACE VIEW LAKE_VIEW.ULTRA_MERCHANT.ITEM_PREALLOCATION AS
SELECT
    item_id,
    warehouse_id,
    quantity,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.item_preallocation s
WHERE NOT exists(
        SELECT
            1
        FROM lake_archive.ultra_merchant_cdc.item_preallocation__del cd
        WHERE cd.item_id = s.item_id
    );
