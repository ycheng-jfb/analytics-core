SET target_table = 'stg.dim_product';
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
SET wm_lake_consolidated_ultra_merchant_product = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.product'));
SET wm_lake_consolidated_ultra_rollup_products_in_stock_product_feed = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_rollup.products_in_stock_product_feed'));
SET wm_lake_consolidated_ultra_merchant_product_tag = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.product_tag'));
SET wm_lake_consolidated_ultra_merchant_item = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.item'));
SET wm_lake_consolidated_ultra_merchant_pricing_option = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.pricing_option'));
SET wm_lake_consolidated_ultra_merchant_product_endowment = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.product_endowment'));
SET wm_edw_stg_dim_sku = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_sku'));
SET wm_lake_ultra_warehouse_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.item'));

/*
SELECT
    $wm_self,
    $wm_lake_consolidated_ultra_merchant_product,
    $wm_lake_consolidated_ultra_merchant_product_tag,
    $wm_lake_consolidated_ultra_merchant_item,
    $wm_lake_consolidated_ultra_merchant_pricing_option,
    $wm_edw_stg_dim_sku;
*/

CREATE OR REPLACE TEMP TABLE _dim_product__product_base (product_id INT);

-- Full Refresh
INSERT INTO _dim_product__product_base (product_id)
SELECT p.product_id
FROM lake_consolidated.ultra_merchant.product AS p
WHERE $is_full_refresh  /* SET is_full_refresh = TRUE; */
ORDER BY p.product_id;

-- Incremental Refresh
INSERT INTO _dim_product__product_base (product_id)
SELECT DISTINCT incr.product_id
FROM (
    /* Self-check for manual updates */
    SELECT dp.product_id
    FROM stg.dim_product AS dp
    WHERE dp.meta_update_datetime > $wm_self
    UNION ALL
    /* Check for dependency table updates */
    SELECT p.product_id
    FROM lake_consolidated.ultra_merchant.product AS p
    WHERE p.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_product
    UNION ALL
    -- updated image_links
    SELECT pispf.product_id
    FROM lake_consolidated.ultra_rollup.products_in_stock_product_feed AS pispf
    WHERE pispf.meta_update_datetime > $wm_lake_consolidated_ultra_rollup_products_in_stock_product_feed
    UNION ALL
    SELECT pispf.master_product_id AS product_id
    FROM lake_consolidated.ultra_rollup.products_in_stock_product_feed AS pispf
    WHERE pispf.meta_update_datetime > $wm_lake_consolidated_ultra_rollup_products_in_stock_product_feed
    UNION ALL
    -- updated product_tags
    SELECT pt.product_id
    FROM lake_consolidated.ultra_merchant.product_tag AS pt
    WHERE pt.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_product_tag
    UNION ALL
    --updated product_skus
    SELECT p.product_id
    FROM lake_consolidated.ultra_merchant.product AS p
        JOIN lake_consolidated.ultra_merchant.item AS i
            ON i.item_id = p.item_id
        LEFT JOIN stg.dim_sku AS dsku
            ON dsku.sku = TRIM(i.item_number)
        LEFT JOIN lake.ultra_warehouse.item AS iw
            ON TRIM(i.item_number) = TRIM(iw.item_number)
    WHERE (
        i.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_item
        OR dsku.meta_update_datetime > $wm_edw_stg_dim_sku
        OR iw.hvr_change_time > $wm_lake_ultra_warehouse_item
        )
    UNION ALL
    SELECT p.product_id
    FROM lake_consolidated.ultra_merchant.product AS p
    JOIN lake_consolidated.ultra_merchant.pricing_option AS sp
        ON sp.pricing_id = p.sale_pricing_id
    WHERE sp.meta_update_datetime > $wm_lake_consolidated_ultra_merchant_pricing_option
    UNION ALL
    SELECT product_id
    FROM lake_consolidated.ultra_merchant.product_endowment
    WHERE product_id IS NOT NULL
    AND meta_update_datetime > $wm_lake_consolidated_ultra_merchant_product_endowment
    UNION ALL
    -- previously errored rows
    SELECT product_id
    FROM excp.dim_product
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.product_id;
-- SELECT * FROM _dim_product__product_base;

-- Adding master level product_ids
INSERT INTO _dim_product__product_base (product_id)
SELECT DISTINCT product_id
FROM (SELECT p.master_product_id AS product_id
      FROM _dim_product__product_base AS mbase
           JOIN lake_consolidated.ultra_merchant.product p
                ON mbase.product_id = p.product_id
      WHERE NOT EXISTS(
          SELECT 1
          FROM _dim_product__product_base AS base
          WHERE base.product_id = p.master_product_id)
        AND NOT $is_full_refresh);

-- Getting child level product_id (only needed for delta refresh)
CREATE OR REPLACE TEMP TABLE _dim_product__child_product_base (product_id) AS
SELECT p.product_id
FROM _dim_product__product_base base
    JOIN lake_consolidated.ultra_merchant.product AS p
        ON p.master_product_id = base.product_id
WHERE NOT $is_full_refresh
ORDER BY p.product_id;

-- Add child level product_ids
INSERT INTO _dim_product__product_base (product_id)
SELECT cbase.product_id
FROM _dim_product__child_product_base AS cbase
WHERE NOT EXISTS (
    SELECT 1
    FROM _dim_product__product_base AS base
    WHERE base.product_id = cbase.product_id
    );

CREATE OR REPLACE temp TABLE _tags AS
SELECT DISTINCT
	spct.label AS tag,
	pt.product_id AS product_id,
	COALESCE(t3.label, t2.label) AS value
FROM reference.store_product_category_tag AS spct
	JOIN lake_consolidated.ultra_merchant.tag AS t1
	    ON t1.tag_id = spct.tag_id
	LEFT JOIN lake_consolidated.ultra_merchant.tag AS t2
	    ON t1.tag_id = t2.parent_tag_id
		AND	t2.active = 1
	LEFT JOIN lake_consolidated.ultra_merchant.tag AS t3
	    ON t2.tag_id = t3.parent_tag_id
		AND	t3.active = 1
	JOIN lake_consolidated.ultra_merchant.product_tag AS pt
	    ON COALESCE(t3.tag_id, t2.tag_id) = pt.tag_id
WHERE t1.active = 1
	AND COALESCE(t3.label, t2.label) <> 'Special Occasion'
ORDER BY
	pt.product_id,
	spct.label;

DELETE FROM _tags
WHERE product_id IN (SELECT product_id FROM _tags WHERE value = 'Flat Boots')
	AND	value = 'Heeled Boots';

CREATE OR REPLACE temp TABLE _product_tags (
	product_id int NOT NULL,
	tag varchar(32) NULL,
	value varchar(800) NULL
	);

-- Join the tags together into a single value per product
INSERT INTO _product_tags (
	product_id,
	tag,
	value
	)
SELECT
	c1.product_id AS product_id,
	c1.tag AS tag,
	LISTAGG(c1.value,',') WITHIN GROUP (ORDER BY c1.value ASC) AS value
FROM _tags AS c1
GROUP BY
	c1.product_id,
	c1.tag

UNION ALL

-- Heel Height and Occasion
SELECT
    t.product_id,
    t.label,
    LISTAGG(DISTINCT t.value,',') WITHIN GROUP (ORDER BY t.value ASC) AS value
FROM (
    SELECT
        pt.product_id,
        t2.label,
        COALESCE(t3.label, t2.label) AS value
    FROM lake_consolidated.ultra_merchant.tag t1
        LEFT JOIN lake_consolidated.ultra_merchant.tag t2
            ON t2.parent_tag_id = t1.tag_id
            AND t2.active = 1
        LEFT JOIN lake_consolidated.ultra_merchant.tag t3
            ON t3.parent_tag_id = t2.tag_id
            AND t3.active = 1
        JOIN lake_consolidated.ultra_merchant.product_tag pt
            ON COALESCE(t3.tag_id, t2.tag_id) = pt.tag_id
    WHERE t1.active = 1
        AND t2.label IN ('Heel Height', 'Occasion')
    ) AS t
GROUP BY
    t.product_id,
    t.label;

-------------------------
-- Product Personality --
-------------------------
CREATE OR REPLACE temp TABLE _product_personality AS
SELECT
	pp.product_id,
	pp.product_personality
FROM
(
	SELECT
		p2.product_id,
		t.label AS product_personality,
		ROW_NUMBER() OVER (PARTITION BY p2.product_id ORDER BY pt.datetime_added DESC) AS row_num
	FROM lake_consolidated.ultra_merchant.product p1
        JOIN lake_consolidated.ultra_merchant.product p2 ON COALESCE(p1.master_product_id, p1.product_id) = p2.product_id
        JOIN lake_consolidated.ultra_merchant.product_tag pt ON p2.product_id = pt.product_id
        JOIN lake_consolidated.ultra_merchant.tag t ON t.tag_id = pt.tag_id
	WHERE t.parent_tag_id like  '1079%' -- Personality Tags
) AS pp
WHERE pp.row_num = 1;

/* YITTY product tag */
CREATE OR REPLACE TEMP TABLE _dim_product__yty_shapewear AS
SELECT
    pb.product_id
FROM _dim_product__product_base AS pb
    JOIN lake_consolidated.ultra_merchant.product_tag AS pt
        ON pt.product_id = pb.product_id
WHERE pt.tag_id = 877620;

/* need to assign child product_ids with YTY shapewear */
INSERT INTO _dim_product__yty_shapewear
(product_id)
SELECT p.product_id
FROM _dim_product__yty_shapewear AS yty
    JOIN lake_consolidated.ultra_merchant.product AS p
        ON yty.product_id = p.master_product_id
WHERE NOT EXISTS
    (
        SELECT 1
        FROM _dim_product__yty_shapewear AS ys
        WHERE ys.product_id = p.product_id
    );

------------------
-- Product Size --
------------------
/*
-- The following is the logic used in ecom production.  We are currently determining is_plus_size in the view.
CREATE OR REPLACE temp TABLE _product_size AS
SELECT
	p.product_id,
    COALESCE(pov.label, p.label) AS size,
    CASE
        WHEN LEFT(COALESCE(pov.label, p.label), 2) IN ('1X', '2X', '3X', '4X') THEN TRUE
        WHEN LEFT(COALESCE(pov.label, p.label), 3) IN ('16W', '18W', '20W', '22W', '24W') THEN TRUE
        ELSE FALSE END AS is_plus_size
FROM lake_consolidated.ultra_merchant.product AS p
    JOIN lake_consolidated.ultra_merchant.product_instance_product_option_value AS pipov
		ON pipov.product_id = p.product_id
	JOIN lake_consolidated.ultra_merchant.product_option_value AS pov
		ON pov.product_option_value_id = pipov.product_option_value_id
	JOIN lake_consolidated.ultra_merchant.product_option AS po
		ON po.product_option_id = pov.product_option_id
		AND po.alias = 'size';
*/

---------------
-- Image URL --
---------------

CREATE OR REPLACE TEMP TABLE _ultra_rollup_fk_images AS
SELECT product_id, image_link AS image_url
FROM lake_consolidated.ultra_rollup.products_in_stock_product_feed
WHERE brand_name ILIKE 'FabKids%%'
UNION
SELECT master_product_id AS product_id, MAX(image_link) AS image_url
FROM lake_consolidated.ultra_rollup.products_in_stock_product_feed
WHERE brand_name ILIKE 'FabKids%%'
GROUP BY master_product_id;

----------- SHOEDAZZLE -----------
CREATE OR REPLACE temp TABLE _image_url AS
SELECT
    product_id,
    image_url,
    brand_name
FROM
(
SELECT
	p.product_id as product_id,
	'http://static3.sd-assets.com/assets/images/' || a.path || REPLACE(a.filename, '.jpg', '-mini.jpg') as image_url,
	'ShoeDazzle' as brand_name
FROM lake_consolidated.ultra_merchant.asset  a
	JOIN lake_consolidated.ultra_merchant.product p ON a.object_id = p.meta_original_product_id
	JOIN lake_consolidated.ultra_merchant.store s ON p.default_store_id = s.store_id
    JOIN lake_consolidated.ultra_merchant.store_group sg ON s.store_group_id = sg.store_group_id
  WHERE a.object = 'product'
	AND a.asset_category_id = 3 -- Limit to hero image
	AND a.statuscode = 5085 -- Active
	AND sg.label = 'ShoeDazzle'

UNION ALL

----------- JUSTFAB -----------

SELECT
	p.product_id AS product_id,
	'http://us-cdn.justfab.com/media/images/products/' || dsku.product_sku || '/' || dsku.product_sku || '-1_48x70.jpg' as image_url,
	'JustFab' as brand_name
FROM lake_consolidated.ultra_merchant.product p
	JOIN lake_consolidated.ultra_merchant.product pb ON COALESCE(p.master_product_id, p.product_id) = pb.product_id
	LEFT JOIN lake_consolidated.ultra_merchant.item i ON COALESCE(pb.item_id, p.item_id) = i.item_id
	JOIN lake_consolidated.ultra_merchant.store s ON COALESCE(pb.default_store_id, p.default_store_id) = s.store_id
    JOIN lake_consolidated.ultra_merchant.store_group sg ON sg.store_group_id = s.store_group_id
	JOIN stg.dim_sku dsku ON TRIM(i.item_number) = dsku.sku
WHERE sg.label ILIKE 'JustFab%%'

UNION ALL

----------- FABLETICS -----------

SELECT DISTINCT
	p.product_id as product_id,
	'http://fabletics-us-cdn.justfab.com/media/images/products/' || psku.product_sku || '/' || psku.product_sku || '-1_271x407.jpg' as image_url,
	'Fabletics' as brand_name
FROM lake_consolidated.ultra_merchant.product p
	JOIN (
	    SELECT
			p.product_id,
			CASE WHEN LTRIM(RTRIM(pb.alias)) LIKE '%% %%' THEN SUBSTRING(LTRIM(RTRIM(pb.alias)), 0, CHARINDEX(' ', LTRIM(RTRIM(pb.alias))))
			    ELSE LTRIM(RTRIM(pb.alias)) END as product_sku
        FROM lake_consolidated.ultra_merchant.product p
            JOIN lake_consolidated.ultra_merchant.product pb ON COALESCE(p.master_product_id, p.product_id) = pb.product_id
            JOIN lake_consolidated.ultra_merchant.store s ON COALESCE(pb.default_store_id, p.default_store_id) = s.store_id
            JOIN lake_consolidated.ultra_merchant.store_group sg ON s.store_group_id = sg.store_group_id
        WHERE sg.label ILIKE 'Fabletics%%'
		) psku
	    ON p.product_id = psku.product_id

UNION ALL

----------- FABKIDS -----------

SELECT
	p.product_id as product_id,
	'http://fabkids-us-cdn.justfab.com/media/images/products/' || psku.product_sku || '/' || psku.product_sku || '_main' || psku.outfit || 'thumb.jpg' as image_url,
	'FabKids' as brand_name
FROM lake_consolidated.ultra_merchant.product p
	JOIN (
	    SELECT
			p.product_id,
			CASE WHEN LTRIM(RTRIM(pb.alias)) LIKE '%% %%' THEN SUBSTRING(LTRIM(RTRIM(pb.alias)), 0, CHARINDEX(' ', LTRIM(RTRIM(pb.alias))))
                --WHEN product_sku IS NOT NULL THEN product_sku
                ELSE LTRIM(RTRIM(pb.alias)) END as product_sku,
			IFF(LTRIM(RTRIM(COALESCE(pb.group_code,CAST(pb.product_id AS VARCHAR(50))))) = 'outfit', '_old_', '_') as outfit
        FROM lake_consolidated.ultra_merchant.product p
            JOIN lake_consolidated.ultra_merchant.product pb ON COALESCE(p.master_product_id, p.product_id) = pb.product_id
            JOIN lake_consolidated.ultra_merchant.store s ON COALESCE(pb.default_store_id, p.default_store_id) = s.store_id
            JOIN lake_consolidated.ultra_merchant.store_group sg ON s.store_group_id = sg.store_group_id
        WHERE sg.label ILIKE 'FabKids%%'
            AND p.product_id NOT IN (SELECT product_id FROM _ultra_rollup_fk_images)
		) psku
	    ON p.product_id = psku.product_id

UNION ALL

SELECT product_id,
       image_url,
       'FabKids' AS brand_name
FROM _ultra_rollup_fk_images

) AS x;

CREATE OR REPLACE TEMP TABLE _dim_product__item AS
SELECT
    p.product_id,
    mp.product_id AS master_product_id,
    TRIM(i.item_number) AS item_number,
    dsku.product_sku,
    dsku.base_sku,
    TRIM(iw.wms_class) AS wms_class,
    COALESCE(p.membership_brand_id, mp.membership_brand_id) AS membership_brand_id
FROM _dim_product__product_base AS base
    JOIN lake_consolidated.ultra_merchant.product AS p
        ON p.product_id = base.product_id
    JOIN lake_consolidated.ultra_merchant.product AS mp
        ON mp.product_id = COALESCE(p.master_product_id, p.product_id) /* The attributes in lake_consolidated.ultra_merchant.product are assigned to the master_product_id */
    LEFT JOIN lake_consolidated.ultra_merchant.item AS i
        ON i.item_id = COALESCE(mp.item_id, p.item_id)
    LEFT JOIN lake.ultra_warehouse.item AS iw
        ON TRIM(i.item_number) = TRIM(iw.item_number)
    LEFT JOIN stg.dim_sku AS dsku
        ON TRIM(i.item_number) = dsku.sku
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.product_id ORDER BY iw.item_id) = 1
ORDER BY p.product_id;
-- SELECT * FROM _dim_product__item;

-- bounceback endowment eligibility
CREATE OR REPLACE TEMP TABLE _dim_product__endowment_eligibility AS
SELECT DISTINCT
    base.product_id
FROM _dim_product__product_base AS base
    JOIN lake_consolidated.ultra_merchant.product AS p
        ON p.product_id = base.product_id
    JOIN lake_consolidated.ultra_merchant.product AS mp
        ON mp.product_id = COALESCE(p.master_product_id, p.product_id)
    JOIN stg.dim_store AS st
        ON st.store_id = COALESCE(mp.default_store_id, p.default_store_id)
    LEFT JOIN lake_consolidated.ultra_merchant.product_endowment AS pe
        ON pe.product_id = mp.product_id
        AND pe.product_id IS NOT NULL
        AND pe.is_endowment_eligible = 0
        AND pe.hvr_is_deleted = 0
WHERE pe.product_id IS NULL
    AND st.store_group_id IN (
                SELECT store_group_id
                FROM lake_consolidated.ultra_merchant.product_endowment
                WHERE product_id IS NULL AND is_endowment_eligible = 1
    );

-----------------
-- Stage Table --
-----------------
CREATE OR REPLACE TEMP TABLE _dim_product__stg AS
SELECT
    p.product_id AS product_id,
    IFNULL(p.master_product_id, -1) AS master_product_id,
    p.meta_original_product_id AS meta_original_product_id,
    IFNULL(p.meta_original_master_product_id, -1) AS meta_original_master_product_id,
    IFNULL(ds.store_id, -1) AS store_id,
    IFNULL(TRIM(i.item_number), 'Unknown') AS sku,
    IFNULL(i.product_sku, 'Unknown') AS product_sku,
    IFNULL(i.base_sku, 'Unknown') AS base_sku,
    IFNULL(mp.label, 'Unknown') AS product_name,
    IFNULL(mp.alias, 'Unknown') AS product_alias,
    IFNULL(pt.label, 'Unknown') AS product_type,
    IFNULL(b.label, 'Unknown') AS brand,
    IFNULL(RTRIM(LTRIM(CASE
        WHEN mp.short_description LIKE '%%:%%' AND mp.short_description ILIKE '%%manu%%'
            THEN LTRIM(SUBSTRING(mp.short_description, CHARINDEX(':', mp.short_description) + 1, LEN(mp.short_description)))
        WHEN LEN(mp.short_description) > 20 THEN ''
        ELSE '' END)), 'Unknown') AS manufacturer,
    IFNULL(mp.date_expected::DATE, '1900-01-01') AS current_showroom_date,
    IFNULL(c1.product_category_id, -1) AS product_category_id,
    IFNULL(c1.label, 'Unknown') AS product_category,
    IFF(i.membership_brand_id = 2 OR yty.product_id IS NOT NULL, 'Shapewear - ', '') ||
    CASE WHEN c4.label IS NULL THEN
    CASE
        WHEN c3.label = 'Scrubs' THEN c3.label || ' ' || c1.label
        WHEN c3.label IS NULL THEN IFNULL(c1.label, 'Unknown') ELSE IFNULL(c2.label, 'Unknown') END
    ELSE
    CASE
        WHEN c3.label = 'Scrubs' THEN c3.label || ' ' || c1.label
        WHEN c4.label IS NULL THEN IFNULL(c2.label, 'Unknown') ELSE IFNULL(c3.label, 'Unknown') END
    END
        AS department,
    CASE WHEN c4.label IS NULL THEN
    CASE
        WHEN c3.label = 'Scrubs' THEN IFNULL(c2.label,'Unknown')
        WHEN c3.label IS NOT NULL THEN IFNULL(c1.label, 'Unknown')
        WHEN style.value IS NOT NULL THEN IFNULL(style.value, 'Unknown')
        ELSE 'Unknown' END
    ELSE
    CASE
        WHEN c3.label = 'Scrubs' THEN IFNULL(c2.label,'Unknown')
        WHEN c4.label IS NOT NULL THEN IFNULL(c2.label, 'Unknown')
        WHEN style.value IS NOT NULL THEN IFNULL(style.value, 'Unknown')
        ELSE 'Unknown' END
    END
        AS category,
    CASE WHEN c3.label IS NOT NULL THEN IFNULL(style.value, 'Unknown') ELSE 'Unknown' END AS subcategory,
    IFNULL(style.value, 'Unknown') AS class,
    TRIM(CASE
        WHEN mp.alias IS NULL THEN 'Unknown'
        ELSE REPLACE(SUBSTRING(mp.alias, CHARINDEX('(', mp.alias) + 1, LEN(mp.alias) - CHARINDEX('(', mp.alias)),
                  ')', '') END) AS color,
    CASE
        WHEN p.master_product_id IS NULL THEN IFNULL(size.value, 'Unknown')
        ELSE IFNULL(p.label, 'Unknown') END AS size,
    IFNULL(material.value, 'Unknown') AS material,
    IFNULL(heel.value, 'Unknown') AS heel_height,
    IFNULL(occ.value, 'Unknown') AS occasion,
    COALESCE(po.unit_price, 0.00) AS vip_unit_price,
    COALESCE(mp.retail_unit_price, p.retail_unit_price, 0.00) AS retail_unit_price,
    IFNULL(mp.short_description, 'Unknown') AS short_description,
    IFNULL(mp.medium_description, 'Unknown') AS medium_description,
    IFNULL(mp.long_description, 'Unknown') AS long_description,
    CASE WHEN mp.active = 1 THEN 1 WHEN mp.active = 0 THEN 0 ELSE NULL END AS is_active,
    CASE WHEN pt.is_free = 1 THEN 1 WHEN pt.is_free = 0 THEN 0 ELSE NULL END AS is_free,
    REPLACE(CASE
        WHEN sg.label ILIKE 'Shoe%%' THEN COALESCE(url_sd.image_url, url.image_url,
            'http://static2.sd-assets.com/static/shared/logo-62586a7e70f6ef6f0bd692ab6e819e89.png')
        WHEN sg.label ILIKE 'Just%%' THEN COALESCE(url_sd.image_url, url.image_url,
        'http://us-cdn.justfab.com/media/images/boutique/logo_0812.png')
        WHEN sg.label ILIKE 'Fabletics%%' THEN COALESCE(url_sd.image_url, url.image_url,
            'http://fabletics-us-cdn.justfab.com/media/images/logo.png')
        WHEN sg.label ILIKE 'FabKids%%' THEN COALESCE(url_sd.image_url, url.image_url,
            'http://fabkids-us-cdn.justfab.com/media/images/brand/fabkids_logo.png')
        ELSE 'Unknown' END, ' ', '') AS image_url,
    p.statuscode AS product_status_code,
    IFNULL(sc.label, 'Unknown') AS product_status,
    IFNULL(TRIM(i.wms_class), 'Unknown') AS wms_class,
    COALESCE(pos.unit_price, 0.00) AS sale_price,
    COALESCE(mp.group_code, 'Unknown') AS group_code,
    CASE
        WHEN p.product_type_id = 14 THEN 'Bundle Product ID'
        WHEN p.is_master = 1 THEN 'Master Product ID'
        WHEN p.product_id = mp.product_id THEN 'No Size-Master Product ID'
        ELSE 'Sized Product ID' END AS product_id_object_type,
    mp.datetime_modified AS master_product_last_update_datetime,
    CASE /* Use a default value for Fabletics */
        WHEN ds.alias = 'Fabletics' THEN COALESCE(i.membership_brand_id, mb.membership_brand_id)
        ELSE i.membership_brand_id END AS membership_brand_id,
    IFF(ee.product_id IS NOT NULL, TRUE, FALSE) AS is_endowment_eligible,
    IFF(mp.is_warehouse_product = 1, TRUE, FALSE) AS is_warehouse_product,
    mp.warehouse_unit_price AS warehouse_unit_price,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_product__item AS i
    JOIN lake_consolidated.ultra_merchant.product AS p
        ON p.product_id = i.product_id
    JOIN lake_consolidated.ultra_merchant.product AS mp
        ON mp.product_id = i.master_product_id
    JOIN lake_consolidated.ultra_merchant.statuscode AS sc
        ON sc.statuscode = p.statuscode
    LEFT JOIN lake_consolidated.ultra_merchant.product_type pt
        ON mp.product_type_id = pt.product_type_id
    LEFT JOIN lake_consolidated.ultra_merchant.brand b
        ON mp.brand_id = b.brand_id
    LEFT JOIN lake_consolidated.ultra_merchant.pricing_option po
        ON mp.default_pricing_id = po.pricing_id
    LEFT JOIN lake_consolidated.ultra_merchant.product_category c1
        ON mp.default_product_category_id = c1.product_category_id
    LEFT JOIN lake_consolidated.ultra_merchant.product_category c2
        ON c1.parent_product_category_id = c2.product_category_id
    LEFT JOIN lake_consolidated.ultra_merchant.product_category c3
        ON c2.parent_product_category_id = c3.product_category_id
    LEFT JOIN lake_consolidated.ultra_merchant.product_category c4
        ON c3.parent_product_category_id = c4.product_category_id
-- Need to break out the tags to prevent dupes
    LEFT JOIN _product_tags AS color
        ON color.product_id = mp.product_id AND color.tag = 'Color'
    LEFT JOIN _product_tags AS style
        ON style.product_id = mp.product_id AND style.tag = 'Style'
    LEFT JOIN _product_tags AS size
        ON size.product_id = mp.product_id AND size.tag = 'Size'
    LEFT JOIN _product_tags AS material
        ON material.product_id = mp.product_id AND material.tag = 'Material'
    LEFT JOIN _product_tags heel
        ON p.product_id = heel.product_id AND heel.tag = 'Heel Height'
    LEFT JOIN _product_tags occ
        ON p.product_id = occ.product_id AND occ.tag = 'Occasion'
    LEFT JOIN _product_personality pp
        ON mp.product_id = pp.product_id
    LEFT JOIN _dim_product__yty_shapewear AS yty
        ON yty.product_id = p.product_id
-- Dimension joins
    LEFT JOIN lake_consolidated.ultra_merchant.store ds
        ON mp.default_store_id = ds.store_id
    LEFT JOIN lake_consolidated.ultra_merchant.store_group sg
        ON ds.store_group_id = sg.store_group_id
    LEFT JOIN lake_consolidated.ultra_merchant.membership_brand AS mb
        ON mb.store_group_id = ds.store_group_id
        AND mb.label = ds.alias
    LEFT JOIN _image_url url_sd
        ON COALESCE(p.master_product_id, p.product_id) = url_sd.product_id AND url_sd.brand_name = 'ShoeDazzle'
    LEFT JOIN _image_url url
        ON p.product_id = url.product_id AND url.brand_name <> 'ShoeDazzle'
    LEFT JOIN lake_consolidated.ultra_merchant.pricing_option pos
        ON pos.pricing_id = mp.sale_pricing_id
    LEFT JOIN _dim_product__endowment_eligibility AS ee
        ON ee.product_id = i.product_id
ORDER BY p.product_id;
-- SELECT * FROM _dim_product__stg where membership_brand_id = 2;

INSERT INTO stg.dim_product_stg (
    product_id,
    master_product_id,
    meta_original_product_id,
    meta_original_master_product_id,
    store_id,
    sku,
    product_sku,
    base_sku,
    product_name,
    product_alias,
    product_type,
    brand,
    manufacturer,
    current_showroom_date,
    product_category_id,
    product_category,
    department,
    category,
    subcategory,
    class,
    color,
    size,
    material,
    heel_height,
    occasion,
    vip_unit_price,
    retail_unit_price,
    short_description,
    medium_description,
    long_description,
    is_active,
    is_free,
    image_url,
    product_status_code,
    product_status,
    wms_class,
    sale_price,
    group_code,
    product_id_object_type,
    master_product_last_update_datetime,
    membership_brand_id,
    is_endowment_eligible,
    is_warehouse_product,
    warehouse_unit_price,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    product_id,
    master_product_id,
    meta_original_product_id,
    meta_original_master_product_id,
    store_id,
    sku,
    product_sku,
    base_sku,
    product_name,
    product_alias,
    product_type,
    brand,
    manufacturer,
    current_showroom_date,
    product_category_id,
    product_category,
    department,
    category,
    subcategory,
    class,
    color,
    size,
    material,
    heel_height,
    occasion,
    vip_unit_price,
    retail_unit_price,
    short_description,
    medium_description,
    long_description,
    is_active,
    is_free,
    image_url,
    product_status_code,
    product_status,
    wms_class,
    sale_price,
    group_code,
    product_id_object_type,
    master_product_last_update_datetime,
    membership_brand_id,
    is_endowment_eligible,
    is_warehouse_product,
    warehouse_unit_price,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_product__stg
ORDER BY product_id;
