SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'stg.dim_return_reason';

SET wm_lake_ultra_merchant_return_reason = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.return_reason'));

CREATE OR REPLACE TEMPORARY TABLE _return_reason_base AS
SELECT return_reason_id
FROM lake_consolidated.ultra_merchant.return_reason
WHERE meta_update_datetime > $wm_lake_ultra_merchant_return_reason
UNION
SELECT return_reason_id
FROM excp.dim_return_reason
WHERE meta_is_current_excp
AND meta_data_quality = 'error';

INSERT INTO stg.dim_return_reason_stg
(
   return_reason_id
  ,return_reason
  ,meta_create_datetime
  ,meta_update_datetime
)
SELECT
   PT.return_reason_id
  ,PT.label
  ,$execution_start_time
  ,$execution_start_time
FROM lake_consolidated.ultra_merchant.return_reason PT
JOIN _return_reason_base PTB
    ON PTB.return_reason_id = PT.return_reason_id;
