SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'stg.dim_order_product_source';

SET wm_lake_ultra_merchant_order_product_source = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.order_product_source'));

CREATE OR REPLACE TEMP TABLE _order_product_source_base AS
SELECT lower(trim(value)) AS order_product_source_name
FROM lake_consolidated.ultra_merchant.order_product_source
WHERE meta_update_datetime > $wm_lake_ultra_merchant_order_product_source
UNION
SELECT order_product_source_name
FROM excp.dim_order_product_source
WHERE meta_is_current_excp AND meta_data_quality = 'error';


INSERT INTO stg.dim_order_product_source_stg
(
	order_product_source_name
	,meta_create_datetime
	,meta_update_datetime
)
SELECT order_product_source_name
	,$execution_start_time
	,$execution_start_time
FROM _order_product_source_base;
