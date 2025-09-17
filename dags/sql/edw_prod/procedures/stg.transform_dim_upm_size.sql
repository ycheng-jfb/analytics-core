SET target_table = 'stg.dim_upm_size';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_lake_centric_ed_size_range = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_size_range'));
SET wm_lake_centric_er_size_range = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.er_size_range'));
SET wm_lake_centric_ed_product_size = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_product_size'));

/*
SELECT
    $wm_self,
    $wm_lake_centric_ed_size_range,
    $wm_lake_centric_er_size_range,
    $wm_lake_centric_ed_product_size;
*/

CREATE OR REPLACE TEMP TABLE _dim_upm_size__base (size_range_id INT, size_id INT);

-- Full Refresh
INSERT INTO _dim_upm_size__base (size_range_id,size_id)
SELECT DISTINCT s.id AS size_range_id, sr.ref_id AS size_id
FROM lake.centric.ed_size_range AS s
    JOIN lake.centric.er_size_range AS sr ON s.id = sr.id AND sr.attr_id = 'Sizes'
    JOIN lake.centric.ed_product_size AS p ON sr.ref_id = p.id
WHERE $is_full_refresh /* SET is_full_refresh = TRUE; */
ORDER BY size_range_id,size_id;

-- Incremental Refresh
INSERT INTO _dim_upm_size__base (size_range_id, size_id)
SELECT DISTINCT incr.size_range_id, incr.size_id
FROM (
    -- Self-check for manual updates
    SELECT size_range_id,size_id
    FROM stg.dim_upm_size
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    SELECT s.id AS size_range_id, sr.ref_id AS size_id
    FROM lake.centric.ed_size_range AS s
        JOIN lake.centric.er_size_range AS sr ON s.id = sr.id AND sr.attr_id = 'Sizes'
        JOIN lake.centric.ed_product_size AS p ON sr.ref_id = p.id
    WHERE (s.meta_update_datetime > $wm_lake_centric_ed_size_range OR
           sr.meta_update_datetime > $wm_lake_centric_er_size_range OR
           p.meta_update_datetime > $wm_lake_centric_ed_product_size)

    UNION ALL
    /* previously errored rows */
    SELECT size_range_id,size_id
    FROM excp.dim_upm_size
    WHERE meta_is_current_excp
      AND meta_data_quality = 'error') AS incr
WHERE NOT $is_full_refresh
  AND incr.size_range_id IS NOT NULL AND incr.size_id IS NOT NULL
ORDER BY incr.size_range_id,incr.size_id;

CREATE OR REPLACE TEMP TABLE _dim_upm_size__stg AS
    SELECT DISTINCT
        s.id AS size_range_id,
        sr.ref_id AS size_id,
        s.node_name AS size_range_description,
        p.node_name AS size
    FROM lake.centric.ed_size_range AS s
        JOIN lake.centric.er_size_range sr ON s.id = sr.id AND sr.attr_id = 'Sizes'
        JOIN lake.centric.ed_product_size p ON sr.ref_id = p.id;
-- SELECT * FROM _dim_upm_size__stg;
-- SELECT size_range_id,size_id, COUNT(1) FROM _dim_upm_size__stg GROUP BY 1,2 HAVING COUNT(1) > 1;

INSERT INTO stg.dim_upm_size_stg (
    size_range_id,
    size_id,
    size_range_description,
    size,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    size_range_id,
    size_id,
    size_range_description,
    size,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_upm_size__stg AS stg
ORDER BY size_range_id,size_id;
-- SELECT * FROM stg.dim_upm_size_stg;
