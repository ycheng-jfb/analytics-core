SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _refund_reason_base AS
WITH reasons AS
(
    SELECT DISTINCT label AS refund_reason
    FROM lake_consolidated.ultra_merchant.refund_reason
    UNION
    SELECT refund_reason
    FROM excp.dim_refund_reason
    WHERE meta_is_current_excp
    AND meta_data_quality = 'error'
)
SELECT refund_reason
FROM reasons;

INSERT INTO stg.dim_refund_reason_stg
(
    refund_reason,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    refund_reason,
    $execution_start_time,
    $execution_start_time
FROM _refund_reason_base;
