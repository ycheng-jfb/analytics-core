SET target_table = 'stg.fact_order_line_discount';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

/*
-- Initial load / full refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_lake_ultra_merchant_order_line_discount = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line_discount'));
SET wm_lake_ultra_merchant_order_line = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_line'));
SET wm_edw_stg_fact_order_line = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order_line'));
SET wm_edw_stg_dim_promo_history = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_promo_history'));
SET wm_lake_ultra_merchant_address = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.address'));

/*
SELECT
    $wm_lake_ultra_merchant_order_line_discount,
    $wm_edw_stg_fact_order_line,
    $wm_edw_stg_dim_promo_history;
 */

CREATE OR REPLACE TEMP TABLE _fact_order_line_discount__base (order_line_discount_id INT);

-- Full Refresh  /* SET is_full_refresh = TRUE; */
INSERT INTO _fact_order_line_discount__base (order_line_discount_id)
SELECT order_line_discount_id
FROM lake_consolidated.ultra_merchant.order_line_discount
WHERE $is_full_refresh = TRUE
ORDER BY order_line_discount_id;

-- Incremental Refresh
INSERT INTO _fact_order_line_discount__base (order_line_discount_id)
SELECT DISTINCT incr.order_line_discount_id
FROM (
    SELECT old.order_line_discount_id
    FROM lake_consolidated.ultra_merchant.order_line_discount AS old
        LEFT JOIN stg.fact_order_line AS fol
            ON fol.order_line_id = old.order_line_id
        LEFT JOIN stg.dim_promo_history AS dph
            ON dph.promo_id = old.promo_id
        LEFT JOIN lake_consolidated.ultra_merchant.order_line AS ol
            ON ol.order_line_id = old.order_line_id
            AND ol.product_type_id = 14
        LEFT JOIN lake_consolidated.ultra_merchant.address a
            ON a.address_id =  fol.shipping_address_id
    WHERE (old.meta_update_datetime > $wm_lake_ultra_merchant_order_line_discount
        OR fol.meta_update_datetime > $wm_edw_stg_fact_order_line
        OR dph.meta_update_datetime > $wm_edw_stg_dim_promo_history
        OR ol.meta_update_datetime > $wm_lake_ultra_merchant_order_line
        OR a.meta_update_datetime > $wm_lake_ultra_merchant_address)
    UNION ALL /* previously errored rows */
    SELECT order_line_discount_id
    FROM excp.fact_order_line_discount
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.order_line_discount_id;

-- Process deletions
CREATE OR REPLACE TEMP TABLE _fact_order_line_discount__deleted AS
SELECT
    order_line_discount_key,
    order_line_discount_id,
    meta_original_order_line_discount_id,
    meta_event_datetime
FROM (
    /*-- Uncomment this block when watermarking is fixed so that we can query from delete cdc table
    SELECT
        order_line_discount_id,
        datetime_modified AS meta_event_datetime
    FROM lake_archive.ultra_merchant_cdc.order_line_discount__del
    WHERE meta_create_datetime > stg.udf_get_watermark($target_table, 'lake_archive.ultra_merchant_cdc.order_line_discount__del')
    UNION ALL
    */
    SELECT /* Current data with no matching values in underlying table */
        fold.order_line_discount_key,
        fold.order_line_discount_id,
        fold.meta_original_order_line_discount_id,
        $execution_start_time AS meta_event_datetime
    FROM stg.fact_order_line_discount AS fold
    WHERE $is_full_refresh /* Only check for deletes during full refresh */
        AND NOT fold.is_deleted
        AND fold.order_line_discount_id <> -1
        AND NOT EXISTS (
            SELECT 1
            FROM lake_consolidated.ultra_merchant.order_line_discount AS old
            WHERE old.order_line_discount_id = fold.order_line_discount_id
            )
    UNION ALL
    SELECT /* Doing the same for bundle components where the order_line_discount_id for bundle doesn't exist */
        fold.order_line_discount_key,
        fold.order_line_discount_id,
        fold.meta_original_order_line_discount_id,
        $execution_start_time AS meta_event_datetime
    FROM stg.fact_order_line_discount AS fold
    WHERE $is_full_refresh /* Only check for deletes during full refresh */
        AND NOT fold.is_deleted
        AND fold.order_line_discount_id = -1
        AND NOT EXISTS (
            SELECT 1
            FROM lake_consolidated.ultra_merchant.order_line_discount AS old
            WHERE old.order_line_discount_id = fold.bundle_order_line_discount_id
            )
    ) AS del
ORDER BY order_line_discount_id;


-- SELECT * FROM _fact_order_line_discount__deleted;

CREATE OR REPLACE TEMP TABLE _fact_order_line_discount__disc AS
SELECT
    old.order_line_discount_id,
    old.meta_original_order_line_discount_id,
    old.order_line_id,
	old.order_id,
	old.promo_id,
    old.discount_id,
	old.amount,
	old.indirect_discount
FROM _fact_order_line_discount__base AS base
    JOIN lake_consolidated.ultra_merchant.order_line_discount AS old
        ON old.order_line_discount_id = base.order_line_discount_id
ORDER BY old.order_line_id;
-- SELECT * FROM _fact_order_line_discount__disc;

/*
   We want to distribute outfit discounts across the components (product_type_id 15)
   instead of it being tied to the bundle (product_type_id 14)
 */
CREATE OR REPLACE TEMP TABLE _fact_order_line_discount__bundle AS
SELECT
    old.order_line_discount_id,
    old.order_line_id,
	old.order_id,
	old.promo_id,
    old.discount_id,
	old.amount,
	old.indirect_discount,
	ol.purchase_unit_price AS bundle_price
FROM _fact_order_line_discount__disc AS old
    JOIN lake_consolidated.ultra_merchant.order_line AS ol
        ON ol.order_line_id = old.order_line_id
WHERE ol.product_type_id = 14;

CREATE OR REPLACE TEMP TABLE _fact_order_line_discount__bundle_components AS
SELECT
    b.order_line_discount_id AS bundle_order_line_discount_id,
    fol.order_line_id,
	b.order_id,
	b.promo_id,
    b.discount_id,
    IFNULL((fol.price_offered_local_amount / NULLIF(fol.bundle_price_offered_local_amount, 0)) * b.amount, 0) AS amount,
	b.indirect_discount,
	fol.order_local_datetime,
	fol.shipping_address_id,
	fol.is_deleted
FROM _fact_order_line_discount__bundle AS b
    JOIN stg.fact_order_line AS fol
        ON fol.bundle_order_line_id = b.order_line_id
     JOIN stg.dim_product_type AS pt
        ON pt.product_type_key = fol.product_type_key
WHERE pt.product_type_name = 'Bundle Component';

CREATE OR REPLACE TEMP TABLE _fact_order_line_discount__stg AS
SELECT /* History with no matching values in underlying table */
    del.order_line_discount_id,
    del.meta_original_order_line_discount_id,
    curr.order_line_id,
    curr.order_id,
    COALESCE(curr.bundle_order_line_discount_id, -1) AS bundle_order_line_discount_id,
    curr.promo_history_key,
    curr.promo_id,
    curr.discount_id,
    curr.order_line_discount_local_amount,
    curr.is_indirect_discount,
    TRUE AS is_deleted,
    curr.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.fact_order_line_discount AS curr
    JOIN _fact_order_line_discount__deleted AS del
        ON del.order_line_discount_key = curr.order_line_discount_key
UNION ALL /* deleting records tying to bundle order_line_id (product type id 14) */
SELECT
    curr.order_line_discount_id,
    curr.meta_original_order_line_discount_id,
    curr.order_line_id,
    curr.order_id,
    COALESCE(curr.bundle_order_line_discount_id, -1) AS bundle_order_line_discount_id,
    curr.promo_history_key,
    curr.promo_id,
    curr.discount_id,
    curr.order_line_discount_local_amount,
    curr.is_indirect_discount,
    TRUE AS is_deleted,
    curr.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.fact_order_line_discount AS curr
    JOIN _fact_order_line_discount__bundle AS b
        ON b.order_line_discount_id = curr.order_line_discount_id
--WHERE curr.is_current
UNION ALL
SELECT
    old.order_line_discount_id,
    old.meta_original_order_line_discount_id,
    COALESCE(old.order_line_id, -1) AS order_line_id,
	COALESCE(old.order_id, -1) AS order_id,
	-1 AS bundle_order_line_discount_id,
	COALESCE(dph.promo_history_key, -1) AS promo_history_key,
	old.promo_id,
    COALESCE(old.discount_id, 0) AS discount_id,
	COALESCE(old.amount, 0.00) / (1 + COALESCE(vrh.rate, 0)) AS order_line_discount_local_amount,
	COALESCE(TO_BOOLEAN(old.indirect_discount), FALSE) AS is_indirect_discount,
	COALESCE(fol.is_deleted, FALSE) AS is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_order_line_discount__disc AS old
    LEFT JOIN stg.fact_order_line AS fol
        ON fol.order_line_id = old.order_line_id
    LEFT JOIN lake_consolidated.ultra_merchant.address AS a
		ON a.address_id = fol.shipping_address_id
    LEFT JOIN reference.vat_rate_history AS vrh
        ON vrh.country_code = COALESCE(REPLACE(UPPER(a.country_code), 'UK', 'GB'), 'Unknown')
        AND fol.order_local_datetime::DATE BETWEEN vrh.start_date AND vrh.expires_date
    LEFT JOIN stg.dim_promo_history AS dph
        ON dph.promo_id = old.promo_id
        AND fol.order_local_datetime BETWEEN dph.effective_start_datetime AND dph.effective_end_datetime
    LEFT JOIN _fact_order_line_discount__bundle AS b
        ON b.order_line_discount_id = old.order_line_discount_id
WHERE b.order_line_discount_id IS NULL /* don't want bundles to flow into table */
UNION ALL
SELECT
    -1 AS order_line_discount_id,
    -1 AS meta_original_order_line_discount_id,
    COALESCE(bc.order_line_id, -1) AS order_line_id,
	COALESCE(bc.order_id, -1) AS order_id,
	COALESCE(bc.bundle_order_line_discount_id, -1) AS bundle_order_line_discount_id,
	COALESCE(dph.promo_history_key, -1) AS promo_history_key,
	bc.promo_id,
    COALESCE(bc.discount_id, 0) AS discount_id,
	COALESCE(bc.amount, 0.00) / (1 + COALESCE(vrh.rate, 0)) AS order_line_discount_local_amount,
	COALESCE(TO_BOOLEAN(bc.indirect_discount), FALSE) AS is_indirect_discount,
	COALESCE(bc.is_deleted, FALSE) AS is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_order_line_discount__bundle_components AS bc
    LEFT JOIN lake_consolidated.ultra_merchant.address AS a
		ON a.address_id = bc.shipping_address_id
    LEFT JOIN reference.vat_rate_history AS vrh
        ON vrh.country_code = COALESCE(REPLACE(UPPER(a.country_code), 'UK', 'GB'), 'Unknown')
        AND bc.order_local_datetime::DATE BETWEEN vrh.start_date AND vrh.expires_date
    LEFT JOIN stg.dim_promo_history AS dph
        ON dph.promo_id = bc.promo_id
        AND bc.order_local_datetime BETWEEN dph.effective_start_datetime AND dph.effective_end_datetime;

-- SELECT * FROM _fact_order_line_discount__stg;
-- SELECT COUNT(1) FROM _fact_order_line_discount__stg;
-- SELECT order_line_discount_id, COUNT(1) FROM _fact_order_line_discount__stg GROUP BY 1 HAVING COUNT(1) > 1;

INSERT INTO stg.fact_order_line_discount_stg (
    order_line_discount_id,
    meta_original_order_line_discount_id,
    order_line_id,
    order_id,
    bundle_order_line_discount_id,
    promo_history_key,
    promo_id,
    discount_id,
    order_line_discount_local_amount,
    is_indirect_discount,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    order_line_discount_id,
    meta_original_order_line_discount_id,
    order_line_id,
    order_id,
    bundle_order_line_discount_id,
    promo_history_key,
    promo_id,
    discount_id,
    order_line_discount_local_amount,
    is_indirect_discount,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
FROM _fact_order_line_discount__stg
ORDER BY
    order_line_discount_id;
