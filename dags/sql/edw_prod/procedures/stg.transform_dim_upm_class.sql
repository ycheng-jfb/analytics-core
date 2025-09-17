SET target_table = 'stg.dim_upm_class';
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
SET wm_lake_centric_ed_classifier0 = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_classifier0'));
SET wm_lake_centric_ed_classifier1 = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_classifier1'));
SET wm_lake_centric_ed_classifier2 = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_classifier2'));
SET wm_lake_centric_ed_classifier3 = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_classifier3'));

/*
SELECT
    $wm_self,
    $wm_lake_centric_ed_style,
    $wm_lake_centric_ed_classifier0,
    $wm_lake_centric_ed_classifier1,
    $wm_lake_centric_ed_classifier2,
    $wm_lake_centric_ed_classifier3;
*/

CREATE OR REPLACE TEMP TABLE _dim_upm_class__base (style_id INT);

-- Full Refresh
INSERT INTO _dim_upm_class__base (style_id)
SELECT DISTINCT style.id AS style_id
FROM lake.centric.ed_style AS style
WHERE $is_full_refresh /* SET is_full_refresh = TRUE; */
ORDER BY style.id;

-- Incremental Refresh
INSERT INTO _dim_upm_class__base (style_id)
SELECT DISTINCT incr.style_id
FROM (
    /* Self-check for manual updates */
    SELECT style_id
    FROM stg.dim_upm_class
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    SELECT style.id AS style_id
    FROM lake.centric.ed_style AS style
        LEFT JOIN lake.centric.ed_classifier0 AS dept
            ON style.tfg_classifier0 = dept.id
        LEFT JOIN lake.centric.ed_classifier1 AS subdept
            ON style.tfg_classifier1 = subdept.id
        LEFT JOIN lake.centric.ed_classifier2 AS class
            ON style.tfg_classifier2 = class.id
        LEFT JOIN lake.centric.ed_classifier3 AS subclass
            ON style.classifier3 = subclass.id
    WHERE (style.meta_update_datetime > $wm_lake_centric_ed_style OR
        dept.meta_update_datetime > $wm_lake_centric_ed_classifier0 OR
        subdept.meta_update_datetime > $wm_lake_centric_ed_classifier1 OR
        class.meta_update_datetime > $wm_lake_centric_ed_classifier2 OR
        subclass.meta_update_datetime > $wm_lake_centric_ed_classifier3)

    UNION ALL
    /* previously errored rows */
    SELECT style_id
    FROM excp.dim_upm_class
    WHERE meta_is_current_excp
      AND meta_data_quality = 'error') AS incr
WHERE NOT $is_full_refresh
  AND incr.style_id IS NOT NULL
ORDER BY incr.style_id;

CREATE OR REPLACE TEMP TABLE _dim_upm_class__stg AS
    SELECT
        style.id AS style_id,
        COALESCE(dept.id, -1) AS dept_id,
        dept.node_name AS department,
        COALESCE(subdept.id, -1) AS subdept_id,
        subdept.node_name AS subdepartment,
        COALESCE(class.id, -1) AS class_id,
        class.node_name AS class,
        COALESCE(subclass.id, -1) AS subclass_id,
        subclass.node_name AS subclass
    FROM _dim_upm_class__base AS base
        JOIN lake.centric.ed_style AS style
             ON style.id = base.style_id
        LEFT JOIN lake.centric.ed_classifier0 AS dept
             ON style.tfg_classifier0 = dept.id
        LEFT JOIN lake.centric.ed_classifier1 AS subdept
             ON style.tfg_classifier1 = subdept.id
        LEFT JOIN lake.centric.ed_classifier2 AS class
             ON style.tfg_classifier2 = class.id
        LEFT JOIN lake.centric.ed_classifier3 AS subclass
             ON style.classifier3 = subclass.id;
-- SELECT * FROM _dim_upm_class__stg;
-- SELECT style_id, COUNT(1) FROM _dim_upm_class__stg GROUP BY 1 HAVING COUNT(1) > 1;

INSERT INTO stg.dim_upm_class_stg (
    style_id,
    dept_id,
    department,
    subdept_id,
    subdepartment,
    class_id,
    class,
    subclass_id,
    subclass,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    style_id,
    dept_id,
    department,
    subdept_id,
    subdepartment,
    class_id,
    class,
    subclass_id,
    subclass,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_upm_class__stg AS stg
ORDER BY style_id;
-- SELECT * FROM stg.dim_upm_class_stg;
