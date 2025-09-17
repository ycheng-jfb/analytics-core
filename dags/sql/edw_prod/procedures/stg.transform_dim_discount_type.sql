SET target_table = 'stg.dim_discount_type';
SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE,
                                  FALSE));

SET wm_lake_ultra_merchant_discount_type = (SELECT stg.udf_get_watermark($target_table,
                                                                         'lake_consolidated.ultra_merchant.discount_type'));
/*
-- Reset for full refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = current_timestamp()
WHERE table_name = $target_table;
*/

CREATE OR REPLACE TEMP TABLE _dim_discount_type__base
(
    discount_type_id INT
);

INSERT INTO _dim_discount_type__base (discount_type_id)
-- Full Refresh
SELECT DISTINCT discount_type_id
FROM lake_consolidated.ultra_merchant.discount_type
WHERE $is_full_refresh = TRUE
UNION ALL
-- Incremental Refresh
SELECT DISTINCT incr.discount_type_id
FROM (SELECT dt.discount_type_id
      FROM lake_consolidated.ultra_merchant.discount_type AS dt
      WHERE dt.meta_update_datetime > $wm_lake_ultra_merchant_discount_type
      UNION ALL
      -- previously errored rows
      SELECT discount_type_id
      FROM excp.dim_discount_type
      WHERE meta_is_current_excp
        AND meta_data_quality = 'error') AS incr
WHERE NOT $is_full_refresh;

-- Populate the current refresh data
INSERT INTO stg.dim_discount_type_stg (discount_type_id,
                                       discount_type_label,
                                       discount_type_description,
                                       meta_create_datetime,
                                       meta_update_datetime)
SELECT dt.discount_type_id,
       COALESCE(dt.label, 'Unknown')       AS discount_type_label,
       COALESCE(dt.description, 'Unknown') AS discount_type_description,
       $execution_start_time               AS meta_create_datetime,
       $execution_start_time               AS meta_update_datetime
FROM _dim_discount_type__base AS base
         JOIN lake_consolidated.ultra_merchant.discount_type AS dt
              ON dt.discount_type_id = base.discount_type_id;
