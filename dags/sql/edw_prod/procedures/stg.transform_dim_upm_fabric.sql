SET target_table = 'stg.dim_upm_fabric';
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
SET wm_lake_centric_er_style = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.er_style'));
SET wm_lake_centric_ed_material = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_material'));

/*
SELECT
    $wm_self,
    $wm_lake_centric_er_style,
    $wm_lake_centric_ed_material;
*/

CREATE OR REPLACE TEMP TABLE _dim_upm_fabric__base (style_id INT);

-- Full Refresh
INSERT INTO _dim_upm_fabric__base (style_id)
SELECT DISTINCT s.id AS style_id
FROM lake.centric.er_style AS s
WHERE $is_full_refresh /* SET is_full_refresh = TRUE; */
ORDER BY s.id;

-- Incremental Refresh
INSERT INTO _dim_upm_fabric__base (style_id)
SELECT DISTINCT incr.style_id
FROM (
    /* Self-check for manual updates */
    SELECT style_id
    FROM stg.dim_upm_fabric
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    SELECT s.id AS style_id
    FROM lake.centric.er_style AS s
        LEFT JOIN lake.centric.ed_material AS mat
            ON mat.id = s.ref_id
    WHERE s.attr_id = 'BOMMainMaterials'
        AND (s.meta_update_datetime > $wm_lake_centric_er_style OR
           mat.meta_update_datetime > $wm_lake_centric_ed_material)

    UNION ALL
    /* previously errored rows */
    SELECT style_id
    FROM excp.dim_upm_fabric
    WHERE meta_is_current_excp
      AND meta_data_quality = 'error') AS incr
WHERE NOT $is_full_refresh
  AND incr.style_id IS NOT NULL
ORDER BY incr.style_id;

CREATE OR REPLACE TEMP TABLE _dim_upm_fabric__stg AS
    SELECT
        base.style_id,
        mat.id AS fabric_id,
        mat.node_name AS fabric,
        REPLACE(SUBSTRING(mat.tfg_fabric_category, POSITION(':' IN mat.tfg_fabric_category),
                          LENGTH(mat.tfg_fabric_category)), ':', '') AS fabric_family,
        REPLACE(SUBSTRING(mat.tfg_material_opacity, POSITION(':' IN mat.tfg_material_opacity),
                          LENGTH(mat.tfg_material_opacity)), ':', '') AS opacity
    FROM _dim_upm_fabric__base AS base
        JOIN ( /* Using DISTINCT eliminates duplicates due to multiple map_keys assigned to the same id */
            SELECT DISTINCT id AS style_id, ref_id
            FROM lake.centric.er_style
            WHERE attr_id = 'BOMMainMaterials'
            ) AS s
            ON s.style_id = base.style_id
        LEFT JOIN lake.centric.ed_material AS mat
            ON mat.id = s.ref_id;
-- SELECT * FROM _dim_upm_fabric__stg;
-- SELECT style_id, fabric_id, COUNT(1) FROM _dim_upm_fabric__stg GROUP BY 1, 2 HAVING COUNT(1) > 1;

INSERT INTO stg.dim_upm_fabric_stg (
    style_id,
    fabric_id,
    fabric,
    fabric_family,
    opacity,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    style_id,
    fabric_id,
    fabric,
    fabric_family,
    opacity,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_upm_fabric__stg AS stg
ORDER BY style_id;
-- SELECT * FROM stg.dim_upm_fabric_stg;
