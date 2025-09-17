SET target_table = 'stg.dim_upm_color';
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
SET wm_lake_centric_ed_colorway = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_colorway'));
SET wm_lake_centric_ed_color_specification = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_color_specification'));
SET wm_lake_centric_ed_print_design_color = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_print_design_color'));

/*
SELECT
    $wm_self,
    $wm_lake_centric_ed_colorway,
    $wm_lake_centric_ed_color_specification,
    $wm_lake_centric_ed_print_design_color;
*/

CREATE OR REPLACE TEMP TABLE _dim_upm_color__base (color_id INT);

-- Full Refresh
INSERT INTO _dim_upm_color__base (color_id)
SELECT DISTINCT colorway.id AS color_id
FROM lake.centric.ed_colorway AS colorway
WHERE $is_full_refresh /* SET is_full_refresh = TRUE; */
ORDER BY colorway.id;

-- Incremental Refresh
INSERT INTO _dim_upm_color__base (color_id)
SELECT DISTINCT incr.color_id
FROM (
    /* Self-check for manual updates */
    SELECT color_id
    FROM stg.dim_upm_color
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    SELECT colorway.id AS color_id
    FROM lake.centric.ed_colorway AS colorway
        LEFT JOIN lake.centric.ed_color_specification AS colorspec
                  ON colorway.color_specification = colorspec.id
        LEFT JOIN lake.centric.ed_print_design_color AS printspec
                  ON colorway.color_specification = printspec.id
    WHERE (colorway.meta_update_datetime > $wm_lake_centric_ed_colorway OR
           colorspec.meta_update_datetime > $wm_lake_centric_ed_color_specification OR
           printspec.meta_update_datetime > $wm_lake_centric_ed_print_design_color)

    UNION ALL
    /* previously errored rows */
    SELECT color_id
    FROM excp.dim_upm_color
    WHERE meta_is_current_excp
      AND meta_data_quality = 'error') AS incr
WHERE NOT $is_full_refresh
  AND incr.color_id IS NOT NULL
ORDER BY incr.color_id;

CREATE OR REPLACE TEMP TABLE _dim_upm_color__stg AS
    SELECT
        colorway.id AS color_id,
        colorway.the_parent_id AS style_id,
        colorway.tfg_marketing_color_name AS site_color,
        colorway.node_name AS color,
        REPLACE(SUBSTRING(colorway.tfg_color_family, POSITION(':' IN colorway.tfg_color_family),
                          LENGTH(colorway.tfg_color_family)), ':', '') AS color_family,
        colorspec.node_name AS color_value,
        printspec.node_name AS pattern
    FROM _dim_upm_color__base AS base
        JOIN lake.centric.ed_colorway AS colorway
             ON colorway.id = base.color_id
        LEFT JOIN lake.centric.ed_color_specification AS colorspec
             ON colorway.color_specification = colorspec.id
        LEFT JOIN lake.centric.ed_print_design_color AS printspec
             ON colorway.color_specification = printspec.id;
-- SELECT * FROM _dim_upm_color__stg;
-- SELECT color_id, COUNT(1) FROM _dim_upm_color__stg GROUP BY 1 HAVING COUNT(1) > 1;

INSERT INTO stg.dim_upm_color_stg (
    color_id,
    style_id,
    site_color,
    color,
    color_family,
    color_value,
    pattern,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    color_id,
    style_id,
    site_color,
    color,
    color_family,
    color_value,
    pattern,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_upm_color__stg AS stg
ORDER BY color_id;
-- SELECT * FROM stg.dim_upm_color_stg;
