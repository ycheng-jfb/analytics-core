SET target_table = 'stg.dim_product_price_history';
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
SET wm_lake_history_ultra_merchant_product = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.product'));
SET wm_lake_history_ultra_merchant_pricing_option = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.pricing_option'));

/*
SELECT
    $wm_lake_history_ultra_merchant_product,
    $wm_lake_history_ultra_merchant_pricing_option;
*/

CREATE OR REPLACE TEMP TABLE _dim_product_price_history__product_base (product_id INT, current_effective_start_datetime TIMESTAMP_LTZ(3));

-- Full Refresh
INSERT INTO _dim_product_price_history__product_base (product_id)
SELECT DISTINCT p.product_id
FROM lake_consolidated.ultra_merchant_history.product AS p
WHERE $is_full_refresh = TRUE
ORDER BY p.product_id;

-- Incremental Refresh
INSERT INTO _dim_product_price_history__product_base (product_id)
SELECT DISTINCT incr.product_id
FROM (SELECT p.product_id
      FROM lake_consolidated.ultra_merchant_history.product AS p
      WHERE p.meta_update_datetime > $wm_lake_history_ultra_merchant_product
        AND p.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      SELECT p.product_id
      FROM lake_consolidated.ultra_merchant_history.product AS p
               JOIN lake_consolidated.ultra_merchant_history.product AS mp
                    ON mp.product_id = p.master_product_id
      WHERE mp.meta_update_datetime > $wm_lake_history_ultra_merchant_product
        AND mp.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      SELECT p.product_id
      FROM lake_consolidated.ultra_merchant_history.product AS p
               JOIN lake_consolidated.ultra_merchant_history.product AS mp
                    ON mp.product_id = p.master_product_id
               LEFT JOIN lake_consolidated.ultra_merchant_history.pricing_option AS po
                         ON po.pricing_id = mp.default_pricing_id
      WHERE po.meta_update_datetime > $wm_lake_history_ultra_merchant_pricing_option
        AND po.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      SELECT p.product_id
      FROM lake_consolidated.ultra_merchant_history.product AS p
               LEFT JOIN lake_consolidated.ultra_merchant_history.pricing_option AS po
                         ON po.pricing_id = p.default_pricing_id
      WHERE po.meta_update_datetime > $wm_lake_history_ultra_merchant_pricing_option
        AND po.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      -- previously errored rows
      SELECT product_id
      FROM excp.dim_product_price_history
      WHERE meta_is_current_excp
        AND meta_data_quality = 'error') AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.product_id;
-- SELECT count(*) FROM _dim_product_price_history__product_base;

-- Exclude product_ids from the base table to prevent meta original product ids with multiple concatenation
DELETE
FROM _dim_product_price_history__product_base
WHERE product_id IN (SELECT CONCAT(meta_original_product_id, meta_company_id)
                     FROM lake_consolidated.reference.product_exclusion_list);

UPDATE _dim_product_price_history__product_base AS base
SET base.current_effective_start_datetime = ph.effective_start_datetime
FROM (
        SELECT product_id, effective_start_datetime
        FROM stg.dim_product_price_history WHERE is_current
    ) AS ph
WHERE base.product_id = ph.product_id
AND $is_full_refresh = FALSE ;

UPDATE _dim_product_price_history__product_base AS base
SET base.current_effective_start_datetime = '1900-01-01'
WHERE base.current_effective_start_datetime IS NULL;

CREATE OR REPLACE TEMP TABLE _dim_product_price_history__products_simplified AS
SELECT meta_original_product_id,
       meta_original_master_product_id,
       product_id,
       master_product_id,
       default_store_id,
       default_pricing_id,
       sale_pricing_id,
       retail_unit_price,
       warehouse_unit_price,
       hvr_change_op,
       datetime_modified
FROM (SELECT meta_original_product_id,
             meta_original_master_product_id,
             p.product_id,
             master_product_id,
             default_store_id,
             default_pricing_id,
             sale_pricing_id,
             retail_unit_price,
             warehouse_unit_price,
             hvr_change_op,
             datetime_modified,
             HASH(
                 data_source_id,
                 meta_company_id,
                 meta_original_product_id,
                 meta_original_master_product_id,
                 p.product_id,
                 master_product_id,
                 default_store_id,
                 default_pricing_id,
                 sale_pricing_id,
                 retail_unit_price,
                 warehouse_unit_price,
                 hvr_change_op
                 )                                                         AS meta_row_hash,
             LAG(meta_row_hash)
                 OVER (PARTITION BY p.product_id ORDER BY datetime_modified) AS prev_meta_row_hash
      FROM lake_consolidated.ultra_merchant_history.product p
      JOIN _dim_product_price_history__product_base base
      ON p.product_id = base.product_id
      WHERE p.product_id IN (SELECT product_id
                           FROM lake_consolidated.ultra_merchant_history.product
                           GROUP BY product_id
                           HAVING COUNT(product_id) >= 150))
WHERE NOT EQUAL_NULL(prev_meta_row_hash, meta_row_hash)
ORDER BY datetime_modified;

CREATE OR REPLACE TEMP TABLE _dim_product_price_history__products_simplified_reduced AS
SELECT meta_original_product_id,
       meta_original_master_product_id,
       product_id,
       master_product_id,
       default_store_id,
       default_pricing_id,
       sale_pricing_id,
       retail_unit_price,
       warehouse_unit_price,
       hvr_change_op,
       datetime_modified
FROM _dim_product_price_history__products_simplified
WHERE product_id IN (SELECT product_id
                     FROM _dim_product_price_history__products_simplified
                     GROUP BY product_id
                     HAVING COUNT(*) > 500)
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, DATE_TRUNC(HOUR, datetime_modified) ORDER BY datetime_modified DESC) = 1;

CREATE OR REPLACE TEMP TABLE _dim_product_price_history__product AS
SELECT product_id,
       master_product_id,
       meta_original_product_id,
       meta_original_master_product_id,
       default_store_id,
       default_pricing_id,
       retail_unit_price,
       warehouse_unit_price,
       sale_pricing_id,
       hvr_change_op,
       datetime_modified                                              AS effective_start_datetime,
       LEAD(DATEADD(MILLISECOND, -1, datetime_modified), 1, '9999-12-31')
            OVER (PARTITION BY product_id ORDER BY datetime_modified) AS effective_end_datetime
FROM _dim_product_price_history__products_simplified
WHERE product_id NOT IN (SELECT product_id FROM _dim_product_price_history__products_simplified_reduced)
UNION

SELECT product_id,
       master_product_id,
       meta_original_product_id,
       meta_original_master_product_id,
       default_store_id,
       default_pricing_id,
       retail_unit_price,
       warehouse_unit_price,
       sale_pricing_id,
       hvr_change_op,
       datetime_modified                                              AS effective_start_datetime,
       LEAD(DATEADD(MILLISECOND, -1, datetime_modified), 1, '9999-12-31')
            OVER (PARTITION BY product_id ORDER BY datetime_modified) AS effective_end_datetime
FROM _dim_product_price_history__products_simplified_reduced

UNION
SELECT p.product_id,
       p.master_product_id,
       p.meta_original_product_id,
       p.meta_original_master_product_id,
       p.default_store_id,
       p.default_pricing_id,
       p.retail_unit_price,
       p.warehouse_unit_price,
       p.sale_pricing_id,
       p.hvr_change_op,
       datetime_modified                                              AS effective_start_datetime,
       LEAD(DATEADD(MILLISECOND, -1, datetime_modified), 1, '9999-12-31')
            OVER (PARTITION BY p.product_id ORDER BY datetime_modified) AS effective_end_datetime
FROM lake_consolidated.ultra_merchant_history.product p JOIN _dim_product_price_history__product_base BASE
ON p.product_id = base.product_id
WHERE p.product_id IN (SELECT product_id
                     FROM lake_consolidated.ultra_merchant_history.product
                     GROUP BY product_id
                     HAVING COUNT(product_id) < 150);

CREATE OR REPLACE TEMP TABLE _dim_product_price_history__action_datetime AS
SELECT DISTINCT
    product_id,
    effective_start_datetime as action_datetime
FROM (
     SELECT
        base.product_id,
        p.effective_start_datetime
    FROM _dim_product_price_history__product_base AS base
        JOIN _dim_product_price_history__product AS p
            ON p.product_id = base.product_id
    WHERE NOT p.effective_end_datetime < base.current_effective_start_datetime

    UNION ALL

    SELECT
        base.product_id,
        mp.effective_start_datetime
    FROM _dim_product_price_history__product_base AS base
        JOIN _dim_product_price_history__product AS p
            ON p.product_id = base.product_id
        JOIN _dim_product_price_history__product AS mp
            ON mp.product_id = p.master_product_id
    WHERE NOT mp.effective_end_datetime < base.current_effective_start_datetime

    UNION ALL

    SELECT
        base.product_id,
        po.effective_start_datetime
    FROM _dim_product_price_history__product_base AS base
        JOIN _dim_product_price_history__product AS p
            ON p.product_id = base.product_id
        JOIN lake_consolidated.ultra_merchant_history.pricing_option AS po
            ON po.pricing_id = p.default_pricing_id
    WHERE NOT po.effective_end_datetime < base.current_effective_start_datetime

    UNION ALL

    SELECT
        base.product_id,
        po.effective_start_datetime
    FROM _dim_product_price_history__product_base AS base
        JOIN _dim_product_price_history__product AS p
            ON p.product_id = base.product_id
        JOIN _dim_product_price_history__product AS mp
            ON mp.product_id = p.master_product_id
        JOIN lake_consolidated.ultra_merchant_history.pricing_option AS po
            ON po.pricing_id = mp.default_pricing_id
    WHERE NOT po.effective_end_datetime < base.current_effective_start_datetime
    ) AS dt;
-- SELECT * FROM _dim_product_price_history__action_datetime;

CREATE OR REPLACE TEMP TABLE _dim_product_price_history__data AS
SELECT
    p.product_id AS product_id,
	COALESCE(p.master_product_id, -1) AS master_product_id,
	p.meta_original_product_id,
    COALESCE(p.meta_original_master_product_id,LEFT(p.master_product_id,LEN(p.master_product_id)-2), -1) AS meta_original_master_product_id,
	COALESCE(mp.default_store_id, -1) AS store_id,
    COALESCE(po.unit_price, 0.00) AS vip_unit_price,
    COALESCE(mp.retail_unit_price, p.retail_unit_price, 0.00) AS retail_unit_price,
    COALESCE(mp.warehouse_unit_price, p.warehouse_unit_price, 0) AS warehouse_unit_price,
    COALESCE(pro.unit_price, 0.00) AS sale_price,
    IFF(p.hvr_change_op = 0, TRUE, FALSE) AS is_deleted,
    base.action_datetime AS meta_event_datetime
FROM _dim_product_price_history__action_datetime AS base
    JOIN _dim_product_price_history__product AS p
        ON p.product_id = base.product_id
        AND base.action_datetime BETWEEN p.effective_start_datetime AND p.effective_end_datetime
    JOIN _dim_product_price_history__product AS mp
        ON mp.product_id = COALESCE(p.master_product_id, p.product_id)
        AND base.action_datetime BETWEEN mp.effective_start_datetime AND mp.effective_end_datetime
    LEFT JOIN lake_consolidated.ultra_merchant_history.pricing_option AS po
        ON po.pricing_id = mp.default_pricing_id
        AND base.action_datetime BETWEEN po.effective_start_datetime AND po.effective_end_datetime
    LEFT JOIN lake_consolidated.ultra_merchant_history.pricing_option AS pro
        On pro.pricing_id = mp.sale_pricing_id
        AND base.action_datetime BETWEEN pro.effective_start_datetime AND pro.effective_end_datetime;
-- SELECT * FROM _dim_product_price_history__data;

CREATE OR REPLACE TEMP TABLE _dim_product_price_history__stg AS
/*
-- Delete history records with no matching values in underlying table (Only performed during full refresh)
SELECT
    base.product_id,
    master_product_id,
    store_id,
    vip_unit_price,
    retail_unit_price,
    TRUE AS is_deleted,
    dpph.meta_event_datetime,
    dpph.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.dim_product_price_history AS dpph
    JOIN _dim_product_price_history__product_base AS base
        ON dpph.product_id = base.product_id
WHERE $is_full_refresh
    AND dpph.is_current
    AND NOT dpph.is_deleted
    AND NOT EXISTS (
        SELECT 1
        FROM _dim_product_price_history__data AS p
        WHERE p.product_id = dpph.product_id
        AND p.meta_event_datetime = dpph.meta_event_datetime
        )
UNION ALL
*/
SELECT
    product_id,
    master_product_id,
    meta_original_product_id,
    meta_original_master_product_id,
    store_id,
    vip_unit_price,
    retail_unit_price,
    warehouse_unit_price,
    sale_price,
    is_deleted,
    meta_event_datetime,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM (
    SELECT
        product_id,
        master_product_id,
        meta_original_product_id,
        meta_original_master_product_id,
        store_id,
        vip_unit_price,
        retail_unit_price,
        warehouse_unit_price,
        sale_price,
        is_deleted,
        meta_event_datetime,
        HASH (
            product_id,
            master_product_id,
            meta_original_product_id,
            meta_original_master_product_id,
            store_id,
            vip_unit_price,
            retail_unit_price,
            warehouse_unit_price,
            sale_price,
            is_deleted
            ) AS meta_row_hash,
        LAG(meta_row_hash) OVER (PARTITION BY product_id ORDER BY meta_event_datetime) AS prev_meta_row_hash
    FROM _dim_product_price_history__data
    ) AS data
WHERE NOT EQUAL_NULL(prev_meta_row_hash, meta_row_hash);
-- SELECT * FROM _dim_product_price_history__stg WHERE product_id = 12189823;

-- Populate the current refresh data
INSERT INTO stg.dim_product_price_history_stg (
    product_id,
    master_product_id,
    meta_original_product_id,
    meta_original_master_product_id,
    store_id,
    vip_unit_price,
    retail_unit_price,
    warehouse_unit_price,
    sale_price,
    is_deleted,
    meta_event_datetime,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    product_id,
    master_product_id,
    meta_original_product_id,
    meta_original_master_product_id,
    store_id,
    vip_unit_price,
    retail_unit_price,
    warehouse_unit_price,
    sale_price,
    is_deleted,
    meta_event_datetime,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_product_price_history__stg
ORDER BY
    product_id,
    meta_event_datetime;

-- Whenever a full refresh (using '1900-01-01') is performed, we will truncate the existing table.  This is
-- because the Snowflake SCD operator cannot process historical data prior to the current row.  We truncate
-- at the end of the transform to prevent the table from being empty longer than necessary during processing.
DELETE
FROM stg.dim_product_price_history
WHERE $is_full_refresh = TRUE
  AND product_price_history_key <> -1;
