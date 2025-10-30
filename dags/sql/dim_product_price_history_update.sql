-- 跟新历史数据状态 dim_product_price_history_update
MERGE INTO EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY tgt
USING EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY_TEMP src
    ON tgt.product_price_history_key = src.product_price_history_key and src.product_price_history_key is not null
WHEN MATCHED THEN UPDATE SET
    tgt.effective_end_datetime = current_date,
    tgt.is_current = 0,
    tgt.meta_update_datetime=current_date
;