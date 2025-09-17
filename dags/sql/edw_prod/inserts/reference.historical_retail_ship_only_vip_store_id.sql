--TRUNCATE TABLE reference.historical_retail_ship_only_vip_store_id;

CREATE OR REPLACE TEMP TABLE _historical_retail_ship_only_vip_store_id AS
    SELECT
        o.order_id,
        o.meta_original_order_id,
        h.store_id
    FROM work.dragan.historical_retail_ship_only_vip_store_id AS h
        JOIN lake_consolidated.ultra_merchant."ORDER" AS o
            ON o.meta_original_order_id = h.order_id;

MERGE INTO reference.historical_retail_ship_only_vip_store_id AS tgt
USING  _historical_retail_ship_only_vip_store_id AS src
    ON tgt.order_id = src.order_id
WHEN NOT MATCHED THEN
    INSERT (order_id, meta_original_order_id, store_id)
    VALUES (order_id, meta_original_order_id, store_id);

UPDATE reference.historical_retail_ship_only_vip_store_id as hvs
SET hvs.store_id = la.store_id
FROM lake_archive.reference.historical_retail_ship_only_vip_store_id AS la
WHERE hvs.store_id = 144
AND la.order_id = hvs.meta_original_order_id;
