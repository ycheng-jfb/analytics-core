MERGE INTO EDW_PROD.NEW_STG.DIM_SKU tgt
USING (
  SELECT distinct
      SKU_KEY,
      sku,
      product_sku,
      base_sku,
      meta_create_datetime,
      meta_update_datetime
  FROM EDW_PROD.stg.dim_sku
) src ON tgt.SKU_KEY = src.SKU_KEY
WHEN MATCHED THEN UPDATE SET
    tgt.SKU_KEY = src.SKU_KEY,
    tgt.sku = src.sku,
    tgt.product_sku = src.product_sku,
    tgt.base_sku = src.base_sku,
    tgt.meta_create_datetime = src.meta_create_datetime,
    tgt.meta_update_datetime = src.meta_update_datetime
WHEN NOT MATCHED THEN                
    INSERT (
        SKU_KEY,
        sku,
        product_sku,
        base_sku,
        meta_create_datetime,
        meta_update_datetime
        )
        VALUES(
        src.SKU_KEY,
        src.sku,
        src.product_sku,
        src.base_sku,
        src.meta_create_datetime,
        src.meta_update_datetime
        )
;

-- create or replace TABLE EDW_PROD.NEW_STG.DIM_SKU (
--        SKU_KEY NUMBER(38,0) autoincrement start 70000000 increment 1 order,
--        SKU VARCHAR(30) NOT NULL,
--        PRODUCT_SKU VARCHAR(30),
--        BASE_SKU VARCHAR(30),
--        META_CREATE_DATETIME TIMESTAMP_LTZ(9),
--        META_UPDATE_DATETIME TIMESTAMP_LTZ(9),
--        primary key (SKU),
--        unique (SKU_KEY)
-- )

-- dim_sku
-- sku 有多条数据，属于异常数据，待上游处理解决
MERGE INTO EDW_PROD.NEW_STG.DIM_SKU tgt
USING (
   select 
    sku
   ,product_sku
   ,base_sku
   ,meta_create_datetime  meta_create_datetime
   ,CURRENT_TIMESTAMP::TIMESTAMP_LTZ(9) meta_update_datetime
   ,ROW_NUMBER() over(partition by sku order by meta_create_datetime) rk
   from EDW_PROD.NEW_STG.DIM_PRODUCT
   where sku is not null and product_sku is not null
    QUALIFY rk = 1
) src ON tgt.SKU = src.SKU
WHEN MATCHED THEN UPDATE SET
    tgt.sku = src.sku,
    tgt.product_sku = src.product_sku,
    tgt.base_sku = src.base_sku,
    tgt.meta_update_datetime = src.meta_update_datetime
WHEN NOT MATCHED THEN               
    INSERT (
        sku,
        product_sku,
        base_sku,
        meta_create_datetime,
        meta_update_datetime
        )
        VALUES(
        src.sku,
        src.product_sku,
        src.base_sku,
        src.meta_create_datetime,
        src.meta_update_datetime
        )
;