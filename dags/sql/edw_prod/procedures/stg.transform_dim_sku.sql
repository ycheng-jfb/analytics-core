SET target_table = 'stg.dim_sku';
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
SET wm_lake_ultra_merchant_item = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.item'));
SET wm_lake_ultra_warehouse_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.item'));

/*
SELECT
    $wm_self,
    $wm_lake_ultra_merchant_item,
    $wm_lake_ultra_warehouse_item;
*/

CREATE OR REPLACE TEMP TABLE _dim_sku__item_number_base (item_number VARCHAR(30));

-- Full Refresh
INSERT INTO _dim_sku__item_number_base (item_number)
SELECT DISTINCT TRIM(i.item_number) AS item_number
FROM (
    SELECT i1.item_number
    FROM lake_consolidated.ultra_merchant.item AS i1
    UNION ALL
    SELECT i2.item_number
    FROM lake.ultra_warehouse.item AS i2
    ) AS i
WHERE $is_full_refresh = TRUE
ORDER BY TRIM(i.item_number);

-- Incremental Refresh
INSERT INTO _dim_sku__item_number_base (item_number)
SELECT DISTINCT TRIM(incr.item_number) AS item_number
FROM (
    /* Self-check for manual updates */
    SELECT ds.sku AS item_number
    FROM stg.dim_sku AS ds
    WHERE ds.meta_update_datetime > $wm_self
    UNION ALL
    /* Check for dependency table updates */
    SELECT i.item_number
    FROM lake_consolidated.ultra_merchant.item AS i
    WHERE i.meta_update_datetime > $wm_lake_ultra_merchant_item
    UNION ALL
    SELECT i.item_number
    FROM lake.ultra_warehouse.item AS i
    WHERE i.hvr_change_time > $wm_lake_ultra_warehouse_item
    UNION ALL
    /* Previously errored rows */
    SELECT sku AS item_number
    FROM excp.dim_sku
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY TRIM(incr.item_number);
-- SELECT * FROM _dim_sku__item_number_base;

CREATE OR REPLACE TEMP TABLE _dim_sku__stg AS
WITH
cte_sku_structure AS (
    SELECT
        1         AS	parse_id,
        'JustFab' AS	sku_structure,
        6         AS	base,
        9         AS	product
    UNION ALL
    SELECT 2, 'ShoeDazzle', 10, 15
    UNION ALL
    SELECT 3, 'Merlin', 9, 14
    UNION ALL
    SELECT 4, 'FabKids', 6, 9
    UNION ALL
    SELECT 5, 'JustFabAlt', 6, 10
    UNION ALL
    SELECT 6, 'PaintFab', 6, 11
    UNION ALL
    SELECT 7, 'JustFabShort', 5, 8
    UNION ALL
    SELECT 8, 'JustFabOther', 3, 8
    UNION ALL
    SELECT 9, 'JustFab19', 9, 13
    UNION ALL
    SELECT 10, 'JustFabOther19',7,13
    UNION ALL
    SELECT 11, 'JustFab21',10,15
	UNION ALL
	SELECT 12, 'JustFab19', 9, 14
    UNION ALL
    SELECT 13, 'JustFabOther21', 9, 15
    UNION ALL
    SELECT 14, 'Unknown', 7, 12
    ),
--Identify which structure a SKU belongs to based ON length and hyphen position
cte_sku AS (
    SELECT
        i.item_number AS item_number,
        CASE
            WHEN LEN(i.item_number) = 12 AND SUBSTRING(i.item_number, 7, 1) = '-' THEN 1
            WHEN LEN(i.item_number) = 20 AND SUBSTRING(i.item_number, 4, 1) = '-' THEN 2
            WHEN LEN(i.item_number) = 15 AND SUBSTRING(i.item_number, 4, 1) = '-' THEN 2
            WHEN LEN(i.item_number) = 20 AND SUBSTRING(i.item_number, 10, 1) = '-' AND SUBSTRING(i.item_number, 16, 1) = '-' THEN 13
            WHEN LEN(i.item_number) = 20 AND SUBSTRING(i.item_number, 10, 1) = '-' THEN 3
            WHEN LEN(i.item_number) = 13 AND SUBSTRING(i.item_number, 7, 1) = '-' AND SUBSTRING(i.item_number, 10, 1) = '-' THEN 4
            WHEN LEN(i.item_number) = 13 AND SUBSTRING(i.item_number, 7, 1) = '-' THEN 5
            WHEN LEN(i.item_number) = 14 AND SUBSTRING(i.item_number, 7, 1) = '-' THEN 6
            WHEN LEN(i.item_number) = 11 AND SUBSTRING(i.item_number, 6, 1) = '-' THEN 7
            WHEN LEN(i.item_number) = 11 AND SUBSTRING(i.item_number, 4, 1) = '-' THEN 8
			WHEN LEN(i.item_number) = 19 AND SUBSTRING(i.item_number, 10, 1) = '-' AND SUBSTRING(i.item_number, 15, 1) = '-' THEN 12
            WHEN LEN(i.item_number) = 19 AND SUBSTRING(i.item_number, 10, 1) = '-' THEN 9
            WHEN LEN(i.item_number) = 19 AND SUBSTRING(i.item_number, 8, 1) = '-' THEN 10
            WHEN LEN(i.item_number) = 21 AND SUBSTRING(i.item_number, 11, 1) = '-' THEN 11
			WHEN LEN(i.item_number) = 21 AND SUBSTRING(i.item_number, 10, 1) = '-' THEN 13
            WHEN LEN(i.item_number) = 18 AND SUBSTRING(i.item_number, 8, 1) = '-' AND SUBSTRING(i.item_number, 13, 1) = '-' THEN 14
            ELSE -1 END AS parse_id
    FROM _dim_sku__item_number_base AS i
    ),
dist AS (
    SELECT DISTINCT
        COALESCE(cte_sku.item_number, 'Unknown') AS sku,
        COALESCE(SUBSTRING(cte_sku.item_number, 1, cte_sku_structure.product), cte_sku.item_number, 'Unknown') AS product_sku,
        COALESCE(SUBSTRING(cte_sku.item_number, 1, cte_sku_structure.base), cte_sku.item_number, 'Unknown') AS base_sku
    FROM cte_sku
        LEFT JOIN cte_sku_structure
            ON cte_sku_structure.parse_id = cte_sku.parse_id
    )
SELECT
    sku,
    product_sku,
    base_sku,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM dist;

INSERT INTO stg.dim_sku_stg (
    sku,
    product_sku,
    base_sku,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    sku,
    product_sku,
    base_sku,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_sku__stg
ORDER BY sku;
