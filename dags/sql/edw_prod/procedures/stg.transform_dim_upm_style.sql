SET target_table = 'stg.dim_upm_style';
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
SET wm_lake_centric_ed_style = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_style'));

/*
SELECT
    $wm_self,
    $wm_lake_centric_ed_style;
*/

CREATE OR REPLACE TEMP TABLE _dim_upm_style__base (style_id INT);

-- Full Refresh
INSERT INTO _dim_upm_style__base (style_id)
SELECT DISTINCT s.id AS style_id
FROM lake.centric.ed_style AS s
WHERE $is_full_refresh /* SET is_full_refresh = TRUE; */
ORDER BY s.id;

-- Incremental Refresh
INSERT INTO _dim_upm_style__base (style_id)
SELECT DISTINCT incr.style_id
FROM (
    /* Self-check for manual updates */
    SELECT style_id
    FROM stg.dim_upm_style
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    SELECT s.id AS style_id
    FROM lake.centric.ed_style AS s
    WHERE (s.meta_update_datetime > $wm_lake_centric_ed_style)

    UNION ALL
    /* previously errored rows */
    SELECT style_id
    FROM excp.dim_upm_style
    WHERE meta_is_current_excp
      AND meta_data_quality = 'error') AS incr
WHERE NOT $is_full_refresh
  AND incr.style_id IS NOT NULL
ORDER BY incr.style_id;

CREATE OR REPLACE TEMP TABLE _dim_upm_style__stg AS
SELECT
    s.id AS style_id,
    s.the_cnl AS centric_style_id,
    s.actual_size_range,
    s.tfg_collection,
    REPLACE(SUBSTRING(s.tfg_gender, POSITION(':' IN s.tfg_gender), LENGTH(s.tfg_gender)), ':', '') AS gender,
    s.node_name AS style_name,
    s.code AS style_number_po,
    s.tfg_rank,
    s.tfg_core_fashion,
    REPLACE(SUBSTRING(s.tfg_coverage, POSITION(':' IN s.tfg_coverage), LENGTH(s.tfg_coverage)), ':',
            '') AS coverage
FROM _dim_upm_style__base AS base
    JOIN lake.centric.ed_style AS s
         ON s.id = base.style_id;
-- SELECT * FROM _dim_upm_style__stg;
-- SELECT style_id, COUNT(1) FROM _dim_upm_style__stg GROUP BY 1 HAVING COUNT(1) > 1;

INSERT INTO stg.dim_upm_style_stg (
    style_id,
    centric_style_id,
    actual_size_range,
    tfg_collection,
    gender,
    style_name,
    style_number_po,
    tfg_rank,
    tfg_core_fashion,
    coverage,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    style_id,
    centric_style_id,
    actual_size_range,
    tfg_collection,
    gender,
    style_name,
    style_number_po,
    tfg_rank,
    tfg_core_fashion,
    coverage,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_upm_style__stg AS stg
ORDER BY style_id;
-- SELECT * FROM stg.dim_upm_style_stg;
