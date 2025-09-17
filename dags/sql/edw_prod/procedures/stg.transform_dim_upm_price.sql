SET target_table = 'stg.dim_upm_price';
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
SET wm_lake_centric_ed_price = (SELECT stg.udf_get_watermark($target_table, 'lake.centric.ed_price'));

/*
SELECT
    $wm_self,
    $wm_lake_centric_ed_price;
*/

CREATE OR REPLACE TEMP TABLE _dim_upm_price__base (price_id INT);

-- Full Refresh
INSERT INTO _dim_upm_price__base (price_id)
SELECT DISTINCT p.id AS price_id
FROM lake.centric.ed_price AS p
WHERE $is_full_refresh /* SET is_full_refresh = TRUE; */
ORDER BY p.id;

-- Incremental Refresh
INSERT INTO _dim_upm_price__base (price_id)
SELECT DISTINCT incr.price_id
FROM (
    /* Self-check for manual updates */
    SELECT price_id
    FROM stg.dim_upm_price
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    SELECT p.id AS price_id
    FROM lake.centric.ed_price AS p
    WHERE (p.meta_update_datetime > $wm_lake_centric_ed_price)

    UNION ALL
    /* previously errored rows */
    SELECT price_id
    FROM excp.dim_upm_price
    WHERE meta_is_current_excp
      AND meta_data_quality = 'error') AS incr
WHERE NOT $is_full_refresh
  AND incr.price_id IS NOT NULL
ORDER BY incr.price_id;

CREATE OR REPLACE TEMP TABLE _dim_upm_price__stg AS
    SELECT
        p.id AS price_id,
        p.colorway AS color_id,
        p.tfg_msrp,
        p.tfg_vip_price
    FROM _dim_upm_price__base AS base
        JOIN lake.centric.ed_price AS p
             ON p.id = base.price_id;
-- SELECT * FROM _dim_upm_price__stg;
-- SELECT price_id, COUNT(1) FROM _dim_upm_price__stg GROUP BY 1 HAVING COUNT(1) > 1;

INSERT INTO stg.dim_upm_price_stg (
    price_id,
    color_id,
    tfg_msrp,
    tfg_vip_price,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    price_id,
    color_id,
    tfg_msrp,
    tfg_vip_price,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_upm_price__stg AS stg
ORDER BY price_id;
-- SELECT * FROM stg.dim_upm_price_stg;
