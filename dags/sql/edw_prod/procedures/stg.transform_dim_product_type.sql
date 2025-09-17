SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'stg.dim_product_type';

SET wm_lake_consolidated_ultra_merchant_product_type = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.product_type'));


CREATE OR REPLACE TEMPORARY TABLE _product_type_base AS
SELECT product_type_id
FROM lake_consolidated.ultra_merchant.product_type
WHERE meta_update_datetime > $wm_lake_consolidated_ultra_merchant_product_type
UNION
SELECT product_type_id
FROM excp.dim_product_type
WHERE meta_is_current_excp
AND meta_data_quality = 'error';

INSERT INTO stg.dim_product_type_stg
(
   product_type_id
  ,product_type_name
  ,is_free
  ,source_is_free
  ,meta_create_datetime
  ,meta_update_datetime
)
SELECT
   PT.product_type_id
  ,PT.label
  ,CASE
        WHEN pt.PRODUCT_TYPE_ID = 11
            THEN 0
        WHEN PT.is_free = 1
            THEN 1
        WHEN PT.is_free = 0
            THEN 0
        ELSE NULL
    END AS is_free
    ,pt.is_free as source_is_free
  ,$execution_start_time
  ,$execution_start_time
FROM lake_consolidated.ultra_merchant.product_type PT
JOIN _product_type_base PTB
    ON PTB.product_type_id = PT.product_type_id;
