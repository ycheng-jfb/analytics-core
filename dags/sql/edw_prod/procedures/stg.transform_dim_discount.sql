SET target_table = 'stg.dim_discount';
SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);
--SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table,NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), True, False));

SET wm_lake_consolidated_ultra_merchant_discount = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.discount'));

-- Reset for full refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = current_timestamp()
WHERE table_name = $target_table;


CREATE OR REPLACE TEMP TABLE _dim_discount__base (discount_id INT);

INSERT INTO _dim_discount__base (discount_id)
-- Full Refresh
SELECT DISTINCT discount_id
FROM lake_consolidated.ultra_merchant.discount
WHERE $is_full_refresh
UNION ALL
-- Incremental Refresh
SELECT DISTINCT incr.discount_id
FROM (
    SELECT d.discount_id
    FROM lake_consolidated.ultra_merchant.discount AS d
    WHERE d.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_discount
    UNION ALL
    -- previously errored rows
    SELECT discount_id
    FROM excp.dim_discount
    WHERE meta_is_current_excp
    AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh;

-- Populate the current refresh data
INSERT INTO stg.dim_discount_stg (
    discount_id,
    meta_original_discount_id,
    discount_applied_to,
    discount_calculation_method,
    discount_label,
    discount_percentage,
    discount_rate,
    discount_date_expires,
    discount_status_code,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    d.discount_id,
    d.meta_original_discount_id,
    COALESCE(d.applied_to, 'Unknown') AS discount_applied_to,
	COALESCE(d.calculation_method, 'Unknown') AS discount_calculation_method,
	COALESCE(d.label, 'Unknown') AS discount_label,
	COALESCE(d.percentage, 0.00) AS discount_percentage,
	COALESCE(d.rate, 0.00) AS discount_rate,
	COALESCE(d.date_expires, '1900-01-01') AS discount_date_expires,
    COALESCE(d.statuscode, -1) AS discount_status_code,
	$execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_discount__base AS base
    JOIN lake_consolidated.ultra_merchant.discount AS d
        ON d.discount_id = base.discount_id;
