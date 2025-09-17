SET target_table = 'stg.dim_bundle_component_history';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table,NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/
SET wm_lake_history_ultra_merchant_product_bundle_component = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.product_bundle_component'));
SET wm_lake_history_ultra_merchant_product = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.product'));
SET wm_lake_history_ultra_merchant_product_category = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.product_category'));
SET wm_lake_history_ultra_merchant_pricing_option = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.pricing_option'));
SET wm_lake_ultra_merchant_product_tag = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.product_tag'));

-- The following creates a reference to the non-historical version of the source tables
-- which are necessary for including in the edm_edw_load.py file.
CREATE OR REPLACE TEMP TABLE _fake_table_reference AS
    SELECT 1 AS id FROM lake_consolidated.ultra_merchant.product_bundle_component WHERE 1 = 0
    UNION ALL
    SELECT 1 AS id FROM lake_consolidated.ultra_merchant.product WHERE 1 = 0
    UNION ALL
    SELECT 1 AS id FROM lake_consolidated.ultra_merchant.product_category WHERE 1 = 0
    UNION ALL
    SELECT 1 AS id FROM lake_consolidated.ultra_merchant.pricing_option WHERE 1 = 0;

CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__product_bundle_component_base (product_bundle_component_id INT, current_effective_start_datetime TIMESTAMP_LTZ(3)) CLUSTER BY (product_bundle_component_id);

INSERT INTO _dim_bundle_component_history__product_bundle_component_base (product_bundle_component_id)
-- Full Refresh
SELECT DISTINCT pbc.product_bundle_component_id
FROM lake_consolidated.ultra_merchant_history.product_bundle_component AS pbc
WHERE $is_full_refresh = TRUE;

-- Incremental Refresh
INSERT INTO _dim_bundle_component_history__product_bundle_component_base (product_bundle_component_id)
SELECT DISTINCT incr.product_bundle_component_id
FROM (SELECT pbc.product_bundle_component_id
      FROM lake_consolidated.ultra_merchant_history.product_bundle_component AS pbc
      WHERE pbc.meta_update_datetime > $wm_lake_history_ultra_merchant_product_bundle_component
        AND pbc.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      SELECT pbc.product_bundle_component_id
      FROM lake_consolidated.ultra_merchant_history.product_bundle_component AS pbc
               JOIN lake_consolidated.ultra_merchant_history.product AS bp
                    ON bp.product_id = pbc.bundle_product_id
      WHERE bp.meta_update_datetime > $wm_lake_history_ultra_merchant_product
        AND bp.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      SELECT pbc.product_bundle_component_id
      FROM lake_consolidated.ultra_merchant_history.product_bundle_component AS pbc
               JOIN lake_consolidated.ultra_merchant_history.product AS cp
                    ON cp.product_id = pbc.component_product_id
      WHERE cp.meta_update_datetime > $wm_lake_history_ultra_merchant_product
        AND cp.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      SELECT pbc.product_bundle_component_id
      FROM lake_consolidated.ultra_merchant_history.product_bundle_component AS pbc
               JOIN lake_consolidated.ultra_merchant_history.product AS bp
                    ON bp.product_id = pbc.bundle_product_id
               JOIN lake_consolidated.ultra_merchant_history.product_category AS bpc
                    ON bpc.product_category_id = bp.default_product_category_id
      WHERE bpc.meta_update_datetime > $wm_lake_history_ultra_merchant_product_category
        AND bp.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      SELECT pbc.product_bundle_component_id
      FROM lake_consolidated.ultra_merchant_history.product_bundle_component AS pbc
               JOIN lake_consolidated.ultra_merchant_history.product AS cp
                    ON cp.product_id = pbc.component_product_id
               JOIN lake_consolidated.ultra_merchant_history.product_category AS cpc
                    ON cpc.product_category_id = cp.default_product_category_id
      WHERE cpc.meta_update_datetime > $wm_lake_history_ultra_merchant_product_category
        AND cp.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      SELECT pbc.product_bundle_component_id
      FROM lake_consolidated.ultra_merchant_history.product_bundle_component AS pbc
               JOIN lake_consolidated.ultra_merchant_history.product AS bp
                    ON bp.product_id = pbc.bundle_product_id
               JOIN lake_consolidated.ultra_merchant_history.pricing_option AS bpo
                    ON bpo.pricing_id = bp.default_pricing_id
      WHERE bpo.meta_update_datetime > $wm_lake_history_ultra_merchant_pricing_option
        AND bpo.effective_end_datetime = '9999-12-31 00:00:00.000 -08:00'

      UNION ALL

      SELECT pbc.product_bundle_component_id
      FROM lake_consolidated.ultra_merchant_history.product_bundle_component AS pbc
               JOIN lake_consolidated.ultra_merchant_history.product AS cp
                    ON cp.product_id = pbc.component_product_id
               JOIN lake_consolidated.ultra_merchant.product_tag AS cpt
                  ON cpt.product_id = cp.product_id
      WHERE cpt.meta_update_datetime > $wm_lake_ultra_merchant_product_tag

      UNION ALL
-- previously errored rows
      SELECT product_bundle_component_id
      FROM excp.dim_bundle_component_history
      WHERE meta_is_current_excp
        AND meta_data_quality = 'error') AS incr
WHERE NOT $is_full_refresh;

UPDATE _dim_bundle_component_history__product_bundle_component_base AS base
SET base.current_effective_start_datetime = dbch.effective_start_datetime
FROM (SELECT product_bundle_component_id, effective_start_datetime FROM stg.dim_bundle_component_history WHERE is_current) AS dbch
WHERE base.product_bundle_component_id = dbch.product_bundle_component_id
AND $is_full_refresh = FALSE;

UPDATE _dim_bundle_component_history__product_bundle_component_base AS base
SET base.current_effective_start_datetime = '1900-01-01'
WHERE base.current_effective_start_datetime IS NULL;
-- SELECT * FROM _dim_bundle_component_history__product_bundle_component_base;

-- Collect all the raw historical data first to make JOINs easier
CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__product_bundle_component_hist AS
SELECT DISTINCT
    base.product_bundle_component_id,
    pbc.meta_original_product_bundle_component_id,
    pbc.bundle_product_id,
    pbc.component_product_id,
    pbc.price_contribution_percentage,
    pbc.is_free,
    CASE WHEN pbc.hvr_change_op = 0 THEN TRUE ELSE FALSE END AS meta_row_is_deleted,
    pbc.effective_start_datetime,
    pbc.effective_end_datetime
FROM _dim_bundle_component_history__product_bundle_component_base AS base
    JOIN lake_consolidated.ultra_merchant_history.product_bundle_component AS pbc
        ON pbc.product_bundle_component_id = base.product_bundle_component_id
WHERE NOT pbc.effective_end_datetime < base.current_effective_start_datetime;

CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__product_list AS
SELECT DISTINCT pbc.product_id
FROM (
    SELECT bundle_product_id AS product_id
    FROM _dim_bundle_component_history__product_bundle_component_hist
    UNION ALL
    SELECT component_product_id AS product_id
    FROM _dim_bundle_component_history__product_bundle_component_hist
    ) AS pbc;

CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__product_hist_simplified AS
SELECT product_id,
       default_product_category_id,
       default_pricing_id,
       label,
       alias,
       group_code,
       retail_unit_price,
       effective_start_datetime
FROM (SELECT p.product_id,
             p.default_product_category_id,
             p.default_pricing_id,
             p.label,
             p.alias,
             p.group_code,
             p.retail_unit_price,
             p.effective_start_datetime,
             HASH(
                     p.data_source_id,
                     p.meta_company_id,
                     p.product_id,
                     p.default_product_category_id,
                     p.default_pricing_id,
                     p.label,
                     p.alias,
                     p.group_code,
                     p.retail_unit_price
                 )                                                                    AS meta_row_hash,
             LAG(meta_row_hash)
                 OVER (PARTITION BY p.product_id ORDER BY p.effective_start_datetime) AS prev_meta_row_hash
      FROM _dim_bundle_component_history__product_list AS pl
               JOIN lake_consolidated.ultra_merchant_history.product AS p
                    ON p.product_id = pl.product_id
      WHERE p.product_id IN (SELECT product_id
                             FROM lake_consolidated.ultra_merchant_history.product
                             GROUP BY product_id
                             HAVING COUNT(product_id) >= 150))
WHERE NOT EQUAL_NULL(prev_meta_row_hash, meta_row_hash)
ORDER BY product_id, effective_start_datetime;

CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__product_hist CLUSTER BY (product_id, effective_start_datetime) AS
SELECT product_id,
       default_product_category_id,
       default_pricing_id,
       label,
       alias,
       group_code,
       retail_unit_price,
       effective_start_datetime,
       LEAD(DATEADD(MILLISECOND, -1, effective_start_datetime), 1, '9999-12-31')
            OVER (PARTITION BY product_id ORDER BY effective_start_datetime) AS effective_end_datetime
FROM _dim_bundle_component_history__product_hist_simplified
UNION
SELECT p.product_id,
       p.default_product_category_id,
       p.default_pricing_id,
       p.label,
       p.alias,
       p.group_code,
       p.retail_unit_price,
       p.effective_start_datetime,
       p.effective_end_datetime
FROM _dim_bundle_component_history__product_list AS pl
         JOIN lake_consolidated.ultra_merchant_history.product AS p
              ON p.product_id = pl.product_id
WHERE p.product_id IN (SELECT product_id
                       FROM lake_consolidated.ultra_merchant_history.product
                       GROUP BY product_id
                       HAVING COUNT(product_id) < 150);

CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__product_category_hist AS
SELECT DISTINCT
    pc.product_category_id,
    pc.label,
    pc.effective_start_datetime,
    pc.effective_end_datetime
FROM (SELECT DISTINCT default_product_category_id FROM _dim_bundle_component_history__product_hist) AS p
    JOIN lake_consolidated.ultra_merchant_history.product_category AS pc
        ON pc.product_category_id = p.default_product_category_id;

CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__pricing_option_hist AS
SELECT
    po.pricing_option_id,
	po.pricing_id,
	po.unit_price,
    po.effective_start_datetime,
    po.effective_end_datetime
FROM (SELECT DISTINCT default_pricing_id FROM _dim_bundle_component_history__product_hist) AS p
    JOIN lake_consolidated.ultra_merchant_history.pricing_option AS po
        ON po.pricing_id = p.default_pricing_id;

CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__tag_color AS
SELECT
	pt.product_id,
	LISTAGG(DISTINCT t.label, ',') WITHIN GROUP (ORDER BY t.label ASC) AS tag_color_list
FROM (SELECT DISTINCT component_product_id FROM _dim_bundle_component_history__product_bundle_component_hist) AS pbc
    JOIN lake_consolidated.ultra_merchant.product_tag AS pt
        ON pt.product_id = pbc.component_product_id
    JOIN lake_consolidated.ultra_merchant.tag AS t
        ON t.tag_id = pt.tag_id
    JOIN lake_consolidated.ultra_merchant.tag AS tp
        ON tp.tag_id = t.parent_tag_id
WHERE t.tag_category_id = 11
    AND tp.label ILIKE 'Color%%'
GROUP BY pt.product_id;

CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__alias_color AS
SELECT
	p.product_id,
	LISTAGG(DISTINCT p.color, ',') WITHIN GROUP (ORDER BY p.color ASC) AS alias_color_list,
    p.effective_start_datetime,
    p.effective_end_datetime
FROM (SELECT DISTINCT component_product_id FROM _dim_bundle_component_history__product_bundle_component_hist) AS pbc
    JOIN (
        SELECT
            product_id,
            TRIM(SUBSTRING(TRIM(alias), CHARINDEX('(', TRIM(alias), 1) + 1, CASE
                WHEN (CHARINDEX('(', alias, 0) = 0 AND CHARINDEX(')', alias, 0) = 0) THEN 0
                WHEN LENGTH(TRIM(alias)) - CHARINDEX(')', REVERSE(TRIM(alias)), 0) - CHARINDEX('(', TRIM(alias), 0) > 0
                    THEN LENGTH(TRIM(alias)) - CHARINDEX(')', REVERSE(TRIM(alias)), 0) - CHARINDEX('(', TRIM(alias), 0)
                ELSE 0 END)) AS color,
            effective_start_datetime,
            effective_end_datetime
        FROM _dim_bundle_component_history__product_hist
        ) AS p
        ON p.product_id = pbc.component_product_id
GROUP BY
    p.product_id,
    p.effective_start_datetime,
    p.effective_end_datetime;

-- Action Dates (base table combining effective_start_dates of all hist tables)
CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__action_datetime AS
SELECT DISTINCT
    product_bundle_component_id,
    effective_start_datetime AS action_datetime
FROM (
    SELECT
        base.product_bundle_component_id,
        pbc.effective_start_datetime
    FROM _dim_bundle_component_history__product_bundle_component_base AS base
        JOIN _dim_bundle_component_history__product_bundle_component_hist AS pbc
            ON pbc.product_bundle_component_id = base.product_bundle_component_id
    WHERE NOT pbc.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
    SELECT
        base.product_bundle_component_id,
        bp.effective_start_datetime
    FROM _dim_bundle_component_history__product_bundle_component_base AS base
        JOIN _dim_bundle_component_history__product_bundle_component_hist AS pbc
            ON pbc.product_bundle_component_id = base.product_bundle_component_id
        JOIN _dim_bundle_component_history__product_hist AS bp
            ON bp.product_id = pbc.bundle_product_id
    WHERE NOT bp.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
    SELECT
        base.product_bundle_component_id,
        cp.effective_start_datetime
    FROM _dim_bundle_component_history__product_bundle_component_base AS base
        JOIN _dim_bundle_component_history__product_bundle_component_hist AS pbc
            ON pbc.product_bundle_component_id = base.product_bundle_component_id
        JOIN _dim_bundle_component_history__product_hist AS cp
            ON cp.product_id = pbc.component_product_id
    WHERE NOT cp.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
    SELECT
        base.product_bundle_component_id,
        bpc.effective_start_datetime
    FROM _dim_bundle_component_history__product_bundle_component_base AS base
        JOIN _dim_bundle_component_history__product_bundle_component_hist AS pbc
            ON pbc.product_bundle_component_id = base.product_bundle_component_id
        JOIN _dim_bundle_component_history__product_hist AS bp
            ON bp.product_id = pbc.bundle_product_id
        JOIN _dim_bundle_component_history__product_category_hist AS bpc
            ON bpc.product_category_id = bp.default_product_category_id
    WHERE NOT bpc.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
    SELECT
        base.product_bundle_component_id,
        cpc.effective_start_datetime
    FROM _dim_bundle_component_history__product_bundle_component_base AS base
        JOIN _dim_bundle_component_history__product_bundle_component_hist AS pbc
            ON pbc.product_bundle_component_id = base.product_bundle_component_id
        JOIN _dim_bundle_component_history__product_hist AS cp
            ON cp.product_id = pbc.component_product_id
        JOIN _dim_bundle_component_history__product_category_hist AS cpc
            ON cpc.product_category_id = cp.default_product_category_id
    WHERE NOT cpc.effective_end_datetime < base.current_effective_start_datetime
    UNION ALL
    SELECT
        pbc.product_bundle_component_id,
        bpo.effective_start_datetime
    FROM _dim_bundle_component_history__product_bundle_component_base AS base
        JOIN _dim_bundle_component_history__product_bundle_component_hist AS pbc
            ON pbc.product_bundle_component_id = base.product_bundle_component_id
        JOIN _dim_bundle_component_history__product_hist AS bp
            ON bp.product_id = pbc.bundle_product_id
        JOIN _dim_bundle_component_history__pricing_option_hist AS bpo
            ON bpo.pricing_id = bp.default_pricing_id
    WHERE NOT bpo.effective_end_datetime < base.current_effective_start_datetime
    ) AS dt;
-- SELECT * FROM _dim_bundle_component_history__action_datetime;

CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__data AS
SELECT
    pbc.product_bundle_component_id,
    pbc.meta_original_product_bundle_component_id,
    pbc.bundle_product_id,
    pbc.component_product_id AS bundle_component_product_id,
    bp.label AS bundle_name,
    bp.alias AS bundle_alias,
    bpc.label AS bundle_default_product_category,
    cpc.label AS bundle_component_default_product_category,
    COALESCE(tc.tag_color_list, ac.alias_color_list) AS bundle_component_color,
    cp.label AS bundle_component_name,
    cp.group_code AS bundle_component_group_code,
    COALESCE(pbc.price_contribution_percentage, 0.00) AS bundle_price_contribution_percent,
    COALESCE(bpo.unit_price * pbc.price_contribution_percentage, 0.00) AS bundle_component_vip_unit_price,
    COALESCE(bp.retail_unit_price * pbc.price_contribution_percentage, 0.00) AS bundle_component_retail_unit_price,
    COALESCE(pbc.is_free, FALSE) AS bundle_is_free,
    COALESCE(pbc.meta_row_is_deleted, FALSE) AS is_deleted,
    base.action_datetime AS meta_event_datetime
FROM _dim_bundle_component_history__action_datetime AS base
    LEFT JOIN _dim_bundle_component_history__product_bundle_component_hist AS pbc
        ON pbc.product_bundle_component_id = base.product_bundle_component_id
        AND base.action_datetime BETWEEN pbc.effective_start_datetime AND pbc.effective_end_datetime
    LEFT JOIN _dim_bundle_component_history__product_hist AS bp
        ON bp.product_id = pbc.bundle_product_id
        AND base.action_datetime BETWEEN bp.effective_start_datetime AND bp.effective_end_datetime
    LEFT JOIN _dim_bundle_component_history__product_hist AS cp
        ON cp.product_id = pbc.component_product_id
        AND base.action_datetime BETWEEN cp.effective_start_datetime AND cp.effective_end_datetime
    LEFT JOIN _dim_bundle_component_history__product_category_hist AS bpc
        ON bpc.product_category_id = bp.default_product_category_id
        AND base.action_datetime BETWEEN bpc.effective_start_datetime AND bpc.effective_end_datetime
    LEFT JOIN _dim_bundle_component_history__product_category_hist AS cpc
        ON cpc.product_category_id = cp.default_product_category_id
        AND base.action_datetime BETWEEN cpc.effective_start_datetime AND cpc.effective_end_datetime
    LEFT JOIN _dim_bundle_component_history__pricing_option_hist AS bpo
        ON bpo.pricing_id = bp.default_pricing_id
        AND base.action_datetime BETWEEN bpo.effective_start_datetime AND bpo.effective_end_datetime
    LEFT JOIN _dim_bundle_component_history__tag_color AS tc
        ON tc.product_id = cp.product_id
    LEFT JOIN _dim_bundle_component_history__alias_color AS ac
        ON ac.product_id = cp.product_id
        AND base.action_datetime BETWEEN ac.effective_start_datetime AND ac.effective_end_datetime;
-- SELECT * FROM _dim_bundle_component_history__data;

CREATE OR REPLACE TEMP TABLE _dim_bundle_component_history__stg AS
/*
-- Delete history records with no matching values in underlying table (Only performed during full refresh)
SELECT
    dbch.product_bundle_component_id,
    dbch.bundle_product_id,
    dbch.bundle_component_product_id,
    dbch.bundle_name,
    dbch.bundle_alias,
    dbch.bundle_default_product_category,
    dbch.bundle_component_default_product_category,
    dbch.bundle_component_color,
    dbch.bundle_component_name,
    dbch.bundle_component_group_code,
    dbch.bundle_price_contribution_percent,
    dbch.bundle_component_vip_unit_price,
    dbch.bundle_component_retail_unit_price,
    dbch.bundle_is_free,
    TRUE AS is_deleted,
    dbch.meta_event_datetime,
    dbch.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.dim_bundle_component_history AS dbch
    JOIN _dim_bundle_component_history__product_bundle_component_base AS base
        ON base.product_bundle_component_id = dbch.product_bundle_component_id
WHERE $is_full_refresh
    AND dbch.is_current
    AND NOT dbch.is_deleted
    AND NOT EXISTS (
        SELECT 1
        FROM _dim_bundle_component_history__data AS data
        WHERE data.product_bundle_component_id = dbch.product_bundle_component_id
            AND data.meta_event_datetime = dbch.meta_event_datetime
        )
UNION ALL
*/
-- Insert processed data eliminating unchanged consecutive rows
SELECT
    product_bundle_component_id,
    meta_original_product_bundle_component_id,
    bundle_product_id,
    bundle_component_product_id,
    bundle_name,
    bundle_alias,
    bundle_default_product_category,
    bundle_component_default_product_category,
    bundle_component_color,
    bundle_component_name,
    bundle_component_group_code,
    bundle_price_contribution_percent,
    bundle_component_vip_unit_price,
    bundle_component_retail_unit_price,
    bundle_is_free,
    is_deleted,
    meta_event_datetime,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM (
    SELECT
        product_bundle_component_id,
        meta_original_product_bundle_component_id,
        bundle_product_id,
        bundle_component_product_id,
        bundle_name,
        bundle_alias,
        bundle_default_product_category,
        bundle_component_default_product_category,
        bundle_component_color,
        bundle_component_name,
        bundle_component_group_code,
        bundle_price_contribution_percent,
        bundle_component_vip_unit_price,
        bundle_component_retail_unit_price,
        bundle_is_free,
        is_deleted,
        HASH (
            product_bundle_component_id,
            meta_original_product_bundle_component_id,
            bundle_product_id,
            bundle_component_product_id,
            bundle_name,
            bundle_alias,
            bundle_default_product_category,
            bundle_component_default_product_category,
            bundle_component_color,
            bundle_component_name,
            bundle_component_group_code,
            bundle_price_contribution_percent,
            bundle_component_vip_unit_price,
            bundle_component_retail_unit_price,
            bundle_is_free,
            is_deleted
            ) AS meta_row_hash,
        LAG(meta_row_hash) OVER (PARTITION BY product_bundle_component_id ORDER BY meta_event_datetime) AS prev_meta_row_hash,
        meta_event_datetime::TIMESTAMP_LTZ(3) AS meta_event_datetime
    FROM _dim_bundle_component_history__data
    ) AS data
WHERE NOT EQUAL_NULL(prev_meta_row_hash, meta_row_hash);
-- SELECT * FROM _dim_bundle_component_history__stg;
-- SELECT * FROM _dim_bundle_component_history__action_datetime;
-- SELECT * FROM _dim_bundle_component_history__data;
-- SELECT COUNT(1) FROM _dim_bundle_component_history__stg;
-- SELECT product_bundle_component_id, meta_event_datetime, COUNT(1) FROM _dim_bundle_component_history__stg GROUP BY 1, 2 HAVING COUNT(1) > 1;

INSERT INTO stg.dim_bundle_component_history_stg (
    product_bundle_component_id,
    meta_original_product_bundle_component_id,
    bundle_product_id,
    bundle_component_product_id,
    bundle_name,
    bundle_alias,
    bundle_default_product_category,
    bundle_component_default_product_category,
    bundle_component_color,
    bundle_component_name,
    bundle_component_group_code,
    bundle_price_contribution_percent,
    bundle_component_vip_unit_price,
    bundle_component_retail_unit_price,
    bundle_is_free,
    is_deleted,
    meta_event_datetime,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    product_bundle_component_id,
    meta_original_product_bundle_component_id,
    bundle_product_id,
    bundle_component_product_id,
    bundle_name,
    bundle_alias,
    bundle_default_product_category,
    bundle_component_default_product_category,
    bundle_component_color,
    bundle_component_name,
    bundle_component_group_code,
    bundle_price_contribution_percent,
    bundle_component_vip_unit_price,
    bundle_component_retail_unit_price,
    bundle_is_free,
    is_deleted,
    meta_event_datetime,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_bundle_component_history__stg
ORDER BY
    product_bundle_component_id,
    meta_event_datetime;

-- Whenever a full refresh (using '1900-01-01') is performed, we will truncate the existing table.  This is
-- because the Snowflake SCD operator cannot process historical data prior to the current row.  We truncate
-- at the end of the transform to prevent the table from being empty longer than necessary during processing.
DELETE
FROM stg.dim_bundle_component_history
WHERE $is_full_refresh = TRUE
  AND bundle_component_history_key <> -1
