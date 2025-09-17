SET target_table = 'stg.dim_related_product';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET wm_lake_ultra_merchant_product = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.product'));
SET wm_lake_ultra_merchant_store_group = (SELECT stg.udf_get_watermark($target_table,'lake_consolidated.ultra_merchant.store_group'));

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

CREATE OR REPLACE TEMP TABLE _dim_related_product__master_product_base (master_product_id INT) CLUSTER BY (master_product_id);

INSERT INTO _dim_related_product__master_product_base (master_product_id)
-- Full Refresh
SELECT DISTINCT mp.product_id AS master_product_id
FROM lake_consolidated.ultra_merchant.product AS p
    JOIN lake_consolidated.ultra_merchant.product AS mp
        ON mp.product_id = COALESCE(p.master_product_id, p.product_id)
WHERE $is_full_refresh = TRUE
UNION ALL
-- Incremental Refresh
SELECT DISTINCT incr.master_product_id
FROM (
    SELECT mp.product_id AS master_product_id
    FROM lake_consolidated.ultra_merchant.product AS p
        JOIN lake_consolidated.ultra_merchant.product AS mp
            ON mp.product_id = COALESCE(p.master_product_id, p.product_id)
        LEFT JOIN lake_consolidated.ultra_merchant.store_group AS sg
            ON sg.store_group_id = mp.store_group_id
    WHERE mp.meta_update_datetime > $wm_lake_ultra_merchant_product
        OR sg.meta_update_datetime > $wm_lake_ultra_merchant_store_group
    UNION ALL
    SELECT master_product_id
    FROM excp.dim_product
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh;

CREATE OR REPLACE TEMP TABLE _dim_related_product__data AS
SELECT
    base.master_product_id,
    mp.meta_original_product_id AS meta_original_master_product_id,
    COALESCE(sg.master_store_group_id, sg.store_group_id) AS master_store_group_id,
    mp.store_group_id,
    mp.product_type_id,
    mp.default_store_id,
    mp.default_warehouse_id,
    mp.default_product_category_id,
    mp.alias,
    mp.group_code,
    mp.label,
    CONCAT_WS('|',
        IFF(mp.product_type_id = 14, mp.label, COALESCE(mp.group_code, mp.label))::VARCHAR(100),
        mp.store_group_id::VARCHAR(10),
        mp.default_product_category_id::VARCHAR(10)
        ) AS related_group_code,
    mp.date_expected::TIMESTAMP_LTZ(3) AS date_expected,
    mp.meta_company_id
FROM _dim_related_product__master_product_base AS base
    JOIN lake_consolidated.ultra_merchant.product AS mp
        ON mp.product_id = base.master_product_id
    JOIN lake_consolidated.ultra_merchant.store_group AS sg
        ON sg.store_group_id = mp.store_group_id
WHERE NOT mp.store_group_id < 9; /* filter out legacy stores */
-- SELECT * FROM _dim_related_product__data;

CREATE OR REPLACE TEMP TABLE _dim_related_product__stg AS
-- Delete history records with no matching values in underlying table (Only performed during full refresh)
SELECT
    drp.master_product_id,
    drp.meta_original_master_product_id,
    drp.master_store_group_id,
    drp.store_group_id,
    drp.product_type_id,
    drp.default_store_id,
    drp.default_warehouse_id,
    drp.default_product_category_id,
    drp.alias,
    drp.group_code,
    drp.label,
    drp.related_group_code,
    drp.date_expected,
    drp.company_id,
    TRUE AS is_deleted,
    drp.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.dim_related_product AS drp
    JOIN _dim_related_product__master_product_base AS base
        ON base.master_product_id = drp.master_product_id
WHERE $is_full_refresh
    AND drp.is_current
    AND NOT drp.is_deleted
    AND NOT EXISTS (
        SELECT 1
        FROM _dim_related_product__data AS data
        WHERE data.master_product_id = drp.master_product_id
        )
UNION ALL
-- Insert unknown record if one is not detected
SELECT
    -1 AS master_product_id,
    -1 AS meta_original_master_product_id,
    -1 AS master_store_group_id,
    -1 AS store_group_id,
    -1 AS product_type_id,
    -1 AS default_store_id,
    -1 AS default_warehouse_id,
    -1 AS default_product_category_id,
    'Unknown' AS alias,
    'Unknown' AS group_code,
    'Unknown' AS label,
    'Unknown' AS related_group_code,
    '1900-01-01' AS date_expected,
    40 AS company_id,
    FALSE AS is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
WHERE $is_full_refresh
    OR NOT EXISTS (SELECT 1 FROM stg.dim_related_product WHERE master_product_id = -1)
UNION ALL
SELECT DISTINCT /* Eliminate duplicates */
    dat.master_product_id,
    dat.meta_original_master_product_id,
    dat.master_store_group_id,
    dat.store_group_id,
    dat.product_type_id,
    dat.default_store_id,
    dat.default_warehouse_id,
    dat.default_product_category_id,
    dat.alias,
    dat.group_code,
    dat.label,
    dat.related_group_code,
    dat.date_expected,
    dat.meta_company_id AS company_id,
    FALSE AS is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_related_product__data AS dat;
-- SELECT * FROM _dim_related_product__stg;
-- SELECT master_product_id, COUNT(1) FROM _dim_related_product__stg GROUP BY 1 HAVING COUNT(1) > 1;

-- Delete duplicates caused by dbsplit
INSERT INTO _dim_related_product__stg(master_product_id,
                                      meta_original_master_product_id,
                                      master_store_group_id,
                                      store_group_id,
                                      product_type_id,
                                      default_store_id,
                                      default_warehouse_id,
                                      default_product_category_id,
                                      alias,
                                      group_code,
                                      label,
                                      related_group_code,
                                      date_expected,
                                      company_id,
                                      is_deleted,
                                      meta_create_datetime,
                                      meta_update_datetime)
SELECT master_product_id,
       meta_original_master_product_id,
       master_store_group_id,
       store_group_id,
       product_type_id,
       default_store_id,
       default_warehouse_id,
       default_product_category_id,
       alias,
       group_code,
       label,
       related_group_code,
       date_expected,
       company_id,
       TRUE,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM stg.dim_related_product drp
         JOIN lake_consolidated.reference.product_exclusion_list pel
              ON drp.master_product_id = CONCAT(pel.meta_original_product_id, meta_company_id)
                  AND NOT drp.is_deleted
                  AND NOT EXISTS(
                      SELECT 1
                      FROM _dim_related_product__data AS data
                      WHERE data.master_product_id = drp.master_product_id
                      )
WHERE $is_full_refresh = TRUE;

INSERT INTO stg.dim_related_product_stg (
    master_product_id,
    meta_original_master_product_id,
    master_store_group_id,
    store_group_id,
    product_type_id,
    default_store_id,
    default_warehouse_id,
    default_product_category_id,
    alias,
    group_code,
    label,
    related_group_code,
    date_expected,
    company_id,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    master_product_id,
    meta_original_master_product_id,
    master_store_group_id,
    store_group_id,
    product_type_id,
    default_store_id,
    default_warehouse_id,
    default_product_category_id,
    alias,
    group_code,
    label,
    related_group_code,
    date_expected,
    company_id,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_related_product__stg
ORDER BY
    master_product_id;
