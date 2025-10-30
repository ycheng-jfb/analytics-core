-- 需要更新的数据和插入的数据
-- dim_product_price_history_temp
MERGE INTO EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY_TEMP tgt
USING (
    select 
     t2.product_id
    ,t2.master_product_id
    ,t2.store_id
    ,t2.vip_unit_price
    ,t2.retail_unit_price
    ,t2.sale_price
    ,t2.meta_create_datetime as effective_start_datetime
    -- ,'9999-12-31' effective_end_datetime
    -- ,1 as is_current
    -- ,current_date as meta_create_datetime
    -- ,current_date as meta_update_datetime
    ,t1.product_price_history_key
    from EDW_PROD.NEW_STG.DIM_PRODUCT t2
    left join EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY t1 on t1.product_id = t2.product_id and t1.store_id = t2.store_id and t1.is_current = 1 
    where t1.vip_unit_price <> t2.vip_unit_price or t1.retail_unit_price <> t2.retail_unit_price or t1.product_price_history_key is null
) src
    ON tgt.product_id = src.product_id and tgt.store_id = src.store_id
WHEN MATCHED THEN UPDATE SET
    tgt.product_id = src.product_id,
    tgt.master_product_id = src.master_product_id,
    tgt.store_id = src.store_id,
    tgt.vip_unit_price = src.vip_unit_price,
    tgt.retail_unit_price = src.retail_unit_price,
    tgt.sale_price = src.sale_price,
    tgt.effective_start_datetime = src.effective_start_datetime,
    tgt.product_price_history_key = src.product_price_history_key
WHEN NOT MATCHED THEN                -- 当目标表里没有匹配的数据
    INSERT (
        product_id,
        master_product_id,
        store_id,
        vip_unit_price,
        retail_unit_price,
        sale_price,
        effective_start_datetime,
        product_price_history_key
        )
        VALUES(
        src.product_id,
        src.master_product_id,
        src.store_id,
        src.vip_unit_price,
        src.retail_unit_price,
        src.sale_price,
        src.effective_start_datetime,
        src.product_price_history_key
        )
;