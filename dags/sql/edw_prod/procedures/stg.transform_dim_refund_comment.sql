SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'stg.dim_refund_comment';

SET wm_lake_ultra_merchant_refund = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.refund'));

CREATE OR REPLACE TEMPORARY TABLE _refund_comment AS
SELECT LOWER(TRIM(reason_comment)) AS refund_comment
FROM lake_consolidated.ultra_merchant.refund
WHERE LOWER(TRIM(reason_comment)) <> 'unknown'
    AND meta_update_datetime > $wm_lake_ultra_merchant_refund
UNION
SELECT refund_comment
FROM excp.dim_refund_comment
WHERE meta_is_current_excp
AND meta_data_quality = 'error';

INSERT INTO stg.dim_refund_comment_stg
(
    refund_comment,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    refund_comment,
    $execution_start_time,
    $execution_start_time
FROM _refund_comment;
