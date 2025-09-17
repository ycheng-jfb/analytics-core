--TRUNCATE TABLE reference.historical_retail_ship_only_vip_store_id;

MERGE INTO reference.historical_retail_ship_only_vip_store_id AS tgt
USING work.dragan.historical_retail_ship_only_vip_store_id AS src
    ON tgt.order_id = src.order_id
WHEN NOT MATCHED THEN
    INSERT (order_id, store_id)
    VALUES (order_id, store_id);
