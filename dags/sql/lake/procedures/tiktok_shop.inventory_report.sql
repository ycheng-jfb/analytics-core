SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _inventory AS
SELECT
    *
FROM
    lake.tiktok_shop.inventory o
    WHERE meta_update_datetime > $low_watermark_ltz;

CREATE OR REPLACE TEMP TABLE _inventory_sku AS
SELECT
shop_name                                                                   shop_name,
product_id                                                                  product_id,
s.value:id                                                                  sku_id,
s.value:seller_sku                                                          seller_sku,
s.value:total_available_inventory_distribution:in_shop_inventory:quantity   in_shop_inventory,
s.value:total_available_quantity                                            total_available_quantity,
s.value:total_committed_quantity                                            total_committed_quantity,
s.value:warehouse_inventory                                                 warehouse_inventory
FROM
    _inventory i,
    LATERAL FLATTEN(INPUT => i.SKUS) s;

CREATE OR REPLACE TEMP TABLE _inventory_warehouse_ids AS
select product_id, sku_id,
 ARRAY_AGG(w.value:warehouse_id) WITHIN GROUP(ORDER BY NULL) AS warehouse_ids
from _inventory_sku o,
lateral flatten(INPUT => o.warehouse_inventory) w
group by product_id, sku_id;

CREATE OR REPLACE TEMP TABLE _inventory_final AS
SELECT
i_sku.shop_name,
i_sku.product_id,
i_sku.sku_id,
i_sku.seller_sku,
i_sku.in_shop_inventory,
i_sku.total_available_quantity,
i_sku.total_committed_quantity,
iw.warehouse_ids
FROM _inventory_sku i_sku
    JOIN _inventory_warehouse_ids iw
        ON i_sku.product_id = iw.product_id and i_sku.sku_id = iw.sku_id;

MERGE INTO lake.tiktok_shop.inventory_report AS t
USING (
    SELECT *,
           hash(*) meta_row_hash,
           current_timestamp() meta_create_datetime,
           current_timestamp() meta_update_datetime
    FROM _inventory_final
    ) s
ON equal_null(t.product_id, s.product_id)
    AND equal_null(t.sku_id, s.sku_id)
    AND equal_null(t.shop_name, s.shop_name)
WHEN NOT MATCHED
    THEN INSERT (shop_name,product_id,sku_id,seller_sku,in_shop_inventory,total_available_quantity,total_committed_quantity,warehouse_ids,meta_row_hash,meta_create_datetime,meta_update_datetime)
         VALUES (shop_name,product_id,sku_id,seller_sku,in_shop_inventory,total_available_quantity,total_committed_quantity,warehouse_ids,meta_row_hash,meta_create_datetime,meta_update_datetime)
WHEN MATCHED AND t.meta_row_hash!=s.meta_row_hash
    THEN UPDATE
    SET
        t.seller_sku=s.seller_sku,
        t.in_shop_inventory=s.in_shop_inventory,
        t.total_available_quantity=s.total_available_quantity,
        t.total_committed_quantity=s.total_committed_quantity,
        t.warehouse_ids=s.warehouse_ids,
        t.meta_row_hash=s.meta_row_hash,
        t.meta_update_datetime=s.meta_update_datetime;
