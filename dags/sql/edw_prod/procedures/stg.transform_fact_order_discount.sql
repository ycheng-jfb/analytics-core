SET target_table = 'stg.fact_order_discount';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table,NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

SET wm_lake_ultra_merchant_order_discount = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_discount'));
SET wm_edw_stg_fact_order = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order'));
SET wm_edw_stg_dim_promo_history = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_promo_history'));


/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

CREATE OR REPLACE TEMP TABLE _fact_order_discount__base (order_discount_id INT);

INSERT INTO _fact_order_discount__base (order_discount_id)
-- Full Refresh
SELECT DISTINCT order_discount_id
FROM lake_consolidated.ultra_merchant.order_discount
WHERE $is_full_refresh = TRUE
UNION ALL
-- Incremental Refresh
SELECT DISTINCT incr.order_discount_id
FROM (
    SELECT od.order_discount_id
    FROM lake_consolidated.ultra_merchant.order_discount AS od
        LEFT JOIN stg.fact_order AS fo
            ON fo.order_id = od.order_id
        LEFT JOIN stg.dim_promo_history AS dph
            ON dph.promo_id = od.promo_id
    WHERE (od.meta_update_datetime > $wm_lake_ultra_merchant_order_discount
        OR fo.meta_update_datetime > $wm_edw_stg_fact_order
        OR dph.meta_update_datetime > $wm_edw_stg_dim_promo_history)
    UNION ALL /* previously errored rows */
    SELECT order_discount_id
    FROM excp.fact_order_discount
    WHERE meta_is_current_excp
    AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh;

-- Process deletions
CREATE OR REPLACE TEMP TABLE _fact_order_discount__deleted AS
SELECT
    order_discount_id,
    meta_original_order_discount_id,
    meta_event_datetime,
    ROW_NUMBER() OVER (PARTITION BY order_discount_id ORDER BY meta_event_datetime DESC) AS row_num
FROM (
    /*-- Uncomment the following once the cdc delete table is available
    SELECT
        order_discount_id,
        datetime_modified AS meta_event_datetime
    FROM lake_archive.ultra_merchant_cdc.order_discount__del
    WHERE meta_update_datetime > stg.udf_get_watermark($target_table, 'lake_archive.ultra_merchant_cdc.order_discount__del')
    UNION ALL
    */
    SELECT /* Current data with no matching values in underlying table */
        fod.order_discount_id,
        fod.meta_original_order_discount_id,
        $execution_start_time AS meta_event_datetime
    FROM stg.fact_order_discount AS fod
    WHERE $is_full_refresh /* Only check for deletes during full refresh */
        AND NOT EXISTS (
            SELECT 1
            FROM lake_consolidated.ultra_merchant.order_discount AS od
            WHERE od.order_discount_id = fod.order_discount_id
            )
    ) AS del
QUALIFY row_num = 1;

CREATE OR REPLACE TEMP TABLE _fact_order_discount__stg AS
SELECT /* History with no matching values in underlying table */
    del.order_discount_id,
    del.meta_original_order_discount_id,
    curr.order_id,
    curr.promo_history_key,
    curr.promo_id,
    curr.discount_id,
    curr.discount_type_id,
    curr.order_discount_applied_to,
    curr.order_discount_local_amount,
    TRUE AS is_deleted,
    curr.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.fact_order_discount AS curr
    JOIN _fact_order_discount__deleted AS del
        ON del.order_discount_id = curr.order_discount_id
--WHERE curr.is_current
UNION ALL
SELECT
    od.order_discount_id,
    od.meta_original_order_discount_id,
    COALESCE(od.order_id, -1) AS order_id,
	COALESCE(dph.promo_history_key, -1) AS promo_history_key,
	od.promo_id,
    COALESCE(od.discount_id, -1) AS discount_id,
	COALESCE(od.discount_type_id, -1) AS discount_type_id,
	COALESCE(od.applied_to, 'Unknown') AS order_discount_applied_to,
	COALESCE(od.amount, 0.00) AS order_discount_local_amount,
    COALESCE(fo.is_deleted, FALSE) AS is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_order_discount__base AS base
    JOIN lake_consolidated.ultra_merchant.order_discount AS od
        ON od.order_discount_id = base.order_discount_id
    LEFT JOIN stg.fact_order AS fo
        ON fo.order_id = od.order_id
    LEFT JOIN stg.dim_promo_history AS dph
        ON dph.promo_id = od.promo_id
        AND fo.order_local_datetime BETWEEN dph.effective_start_datetime AND dph.effective_end_datetime;

INSERT INTO stg.fact_order_discount_stg (
    order_discount_id,
    meta_original_order_discount_id,
    order_id,
    promo_history_key,
    promo_id,
    discount_id,
    discount_type_id,
    order_discount_applied_to,
    order_discount_local_amount,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    order_discount_id,
    meta_original_order_discount_id,
    order_id,
    promo_history_key,
    promo_id,
    discount_id,
    discount_type_id,
    order_discount_applied_to,
    order_discount_local_amount,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_order_discount__stg
ORDER BY
    order_discount_id;
