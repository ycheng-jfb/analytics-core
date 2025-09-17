SET start_date = DATEADD('year', -3, DATE_TRUNC('year', CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _order_detail AS
SELECT olp.business_unit,
       olp.region,
       olp.country,
       olp.order_date,
       olp.order_id,
       olp.order_line_id,
       olp.order_type,
       olp.product_sku,
       (CASE
            WHEN mdp.department = 'APPAREL – FABKIDS' THEN 'FK ON JF APPAREL'
            WHEN mdp.department = 'FOOTWEAR – FABKIDS' THEN 'FK ON JF FOOTWEAR'
            WHEN (mdp.department = 'FOOTWEAR' OR mdp.department LIKE '%APPAREL%' OR mdp.department LIKE '%SHOE%')
                THEN mdp.department || '-' || mdp.subcategory
            ELSE mdp.department END) AS product_info,
       mdp.department,
       olp.total_product_revenue,
       olp.total_cogs,
       olp.total_qty_sold,
       (CASE
            WHEN olp.bundle_product_id IS NOT NULL THEN 'bundle'
            ELSE 'not bundle' END)   AS is_bundle_flag
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
JOIN reporting_prod.gfb.merch_dim_product mdp
    ON mdp.business_unit = olp.business_unit
        AND mdp.region = olp.region
        AND mdp.country = olp.country
        AND mdp.product_sku = olp.product_sku
WHERE olp.order_classification = 'product order'
  AND olp.order_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _order_detail_product AS
SELECT DISTINCT order_id, product_info FROM _order_detail;

CREATE OR REPLACE TEMPORARY TABLE _order_detail_department AS
SELECT DISTINCT order_id, department FROM _order_detail;

CREATE OR REPLACE TEMPORARY TABLE _order_product_info_product_base AS
SELECT od.order_id                                                                    AS order_id_for_join,
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-ANKLE BOOTS' THEN 1 ELSE 0 END)      AS "FOOTWEAR-ANKLE BOOTS FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-FLAT BOOTS' THEN 1 ELSE 0 END)       AS "FOOTWEAR-FLAT BOOTS FLAG",
       SUM(CASE WHEN od.product_info = 'GIRLS SHOES-SHOES' THEN 1 ELSE 0 END)         AS "GIRLS SHOES-SHOES FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-HEELED SANDALS' THEN 1 ELSE 0 END)   AS "FOOTWEAR-HEELED SANDALS FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-HEELED BOOTS' THEN 1 ELSE 0 END)     AS "FOOTWEAR-HEELED BOOTS FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-WEDGES' THEN 1 ELSE 0 END)           AS "FOOTWEAR-WEDGES FLAG",
       SUM(CASE WHEN od.product_info = 'HANDBAGS' THEN 1 ELSE 0 END)                  AS "HANDBAGS FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-PUMPS' THEN 1 ELSE 0 END)            AS "FOOTWEAR-PUMPS FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-BOOTIES' THEN 1 ELSE 0 END)          AS "FOOTWEAR-BOOTIES FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-SANDALS-DRESSY' THEN 1 ELSE 0 END)   AS "FOOTWEAR-SANDALS-DRESSY FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-FLAT SANDALS' THEN 1 ELSE 0 END)     AS "FOOTWEAR-FLAT SANDALS FLAG",
       SUM(CASE WHEN od.product_info = 'GIRLS APPAREL-BOTTOM' THEN 1 ELSE 0 END)      AS "GIRLS APPAREL-BOTTOM FLAG",
       SUM(CASE WHEN od.product_info = 'APPAREL-TOPS' THEN 1 ELSE 0 END)              AS "APPAREL-TOPS FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-FLATS' THEN 1 ELSE 0 END)            AS "FOOTWEAR-FLATS FLAG",
       SUM(CASE WHEN od.product_info = 'APPAREL-JACKETS' THEN 1 ELSE 0 END)           AS "APPAREL-JACKETS FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-SNEAKERS' THEN 1 ELSE 0 END)         AS "FOOTWEAR-SNEAKERS FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-SANDALS-FLAT' THEN 1 ELSE 0 END)     AS "FOOTWEAR-SANDALS-FLAT FLAG",
       SUM(CASE WHEN od.product_info = 'APPAREL-BOTTOMS' THEN 1 ELSE 0 END)           AS "APPAREL-BOTTOMS FLAG",
       SUM(CASE WHEN od.product_info = 'APPAREL-DENIM' THEN 1 ELSE 0 END)             AS "APPAREL-DENIM FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-COLD WEATHER' THEN 1 ELSE 0 END)     AS "FOOTWEAR-COLD WEATHER FLAG",
       SUM(CASE WHEN od.product_info = 'GIRLS APPAREL-DRESS' THEN 1 ELSE 0 END)       AS "GIRLS APPAREL-DRESS FLAG",
       SUM(CASE WHEN od.product_info = 'APPAREL-SWEATERS' THEN 1 ELSE 0 END)          AS "APPAREL-SWEATERS FLAG",
       SUM(CASE WHEN od.product_info = 'ACCESSORIES' THEN 1 ELSE 0 END)               AS "ACCESSORIES FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-FUZZIES' THEN 1 ELSE 0 END)          AS "FOOTWEAR-FUZZIES FLAG",
       SUM(CASE WHEN od.product_info = 'GIRLS APPAREL-TOP' THEN 1 ELSE 0 END)         AS "GIRLS APPAREL-TOP FLAG",
       SUM(CASE WHEN od.product_info = 'JEWELRY' THEN 1 ELSE 0 END)                   AS "JEWELRY FLAG",
       SUM(CASE WHEN od.product_info = 'FOOTWEAR-STREET' THEN 1 ELSE 0 END)           AS "FOOTWEAR-STREET FLAG",
       SUM(CASE WHEN od.product_info = 'APPAREL-ACTIVE' THEN 1 ELSE 0 END)            AS "APPAREL-ACTIVE FLAG",
       SUM(CASE WHEN od.product_info = 'BOYS SHOES-SHOES' THEN 1 ELSE 0 END)          AS "BOYS SHOES-SHOES FLAG",
       SUM(CASE WHEN od.product_info = 'GIRLS ACCESSORY' THEN 1 ELSE 0 END)           AS "GIRLS ACCESSORY FLAG",
       SUM(CASE WHEN od.product_info = 'BOYS APPAREL-TOP' THEN 1 ELSE 0 END)          AS "BOYS APPAREL-TOP FLAG",
       SUM(CASE WHEN od.product_info = 'BOYS APPAREL-BOTTOM' THEN 1 ELSE 0 END)       AS "BOYS APPAREL-BOTTOM FLAG",
       SUM(CASE WHEN od.product_info = 'GIRLS APPAREL-OUTERWEAR' THEN 1 ELSE 0 END)   AS "GIRLS APPAREL-OUTERWEAR FLAG",
       SUM(CASE WHEN od.product_info = 'APPAREL-SWIM' THEN 1 ELSE 0 END)              AS "APPAREL-SWIM FLAG",
       SUM(CASE WHEN od.product_info = 'FK ON JF FOOTWEAR' THEN 1 ELSE 0 END)         AS "FK ON JF FOOTWEAR FLAG",
       SUM(CASE WHEN od.product_info = 'BOYS ACCESSORY' THEN 1 ELSE 0 END)            AS "BOYS ACCESSORY FLAG",
       SUM(CASE WHEN od.product_info = 'GIRLS APPAREL-SWIMWEAR' THEN 1 ELSE 0 END)    AS "GIRLS APPAREL-SWIMWEAR FLAG",
       SUM(CASE WHEN od.product_info = 'BOYS APPAREL-OUTERWEAR' THEN 1 ELSE 0 END)    AS "BOYS APPAREL-OUTERWEAR FLAG",
       SUM(CASE WHEN od.product_info = 'BOYS APPAREL-SWIMWEAR' THEN 1 ELSE 0 END)     AS "BOYS APPAREL-SWIMWEAR FLAG",
       SUM(CASE WHEN od.product_info = 'FK ON JF APPAREL' THEN 1 ELSE 0 END)          AS "FK ON JF APPAREL FLAG",
       SUM(CASE WHEN od.product_info = 'GIRLS APPAREL-OUTER/OTHER' THEN 1 ELSE 0 END) AS "GIRLS APPAREL-OUTER/OTHER FLAG"
FROM _order_detail_product od
GROUP BY od.order_id;

CREATE OR REPLACE TEMPORARY TABLE _order_product_info_department_base AS
SELECT od.order_id                                                     AS order_id_for_join,
       SUM(CASE WHEN od.department LIKE '%APPAREL%' THEN 1 ELSE 0 END) AS "APPAREL FLAG",
       SUM(CASE
               WHEN od.department LIKE '%FOOTWEAR%' OR od.department LIKE '%SHOE%' THEN 1
               ELSE 0 END)                                             AS "FOOTWEAR FLAG"
FROM _order_detail_department od
GROUP BY od.order_id;

CREATE OR REPLACE TEMPORARY TABLE _order_product_info AS
SELECT a.order_id_for_join,
       a."FOOTWEAR-ANKLE BOOTS FLAG",
       a."FOOTWEAR-FLAT BOOTS FLAG",
       a."GIRLS SHOES-SHOES FLAG",
       a."FOOTWEAR-HEELED SANDALS FLAG",
       a."FOOTWEAR-HEELED BOOTS FLAG",
       a."FOOTWEAR-WEDGES FLAG",
       a."HANDBAGS FLAG",
       a."FOOTWEAR-PUMPS FLAG",
       a."FOOTWEAR-BOOTIES FLAG",
       a."FOOTWEAR-SANDALS-DRESSY FLAG",
       a."FOOTWEAR-FLAT SANDALS FLAG",
       a."GIRLS APPAREL-BOTTOM FLAG",
       a."APPAREL-TOPS FLAG",
       a."FOOTWEAR-FLATS FLAG",
       a."APPAREL-JACKETS FLAG",
       a."FOOTWEAR-SNEAKERS FLAG",
       a."FOOTWEAR-SANDALS-FLAT FLAG",
       a."APPAREL-BOTTOMS FLAG",
       a."APPAREL-DENIM FLAG",
       a."FOOTWEAR-COLD WEATHER FLAG",
       a."GIRLS APPAREL-DRESS FLAG",
       a."APPAREL-SWEATERS FLAG",
       a."ACCESSORIES FLAG",
       a."FOOTWEAR-FUZZIES FLAG",
       a."GIRLS APPAREL-TOP FLAG",
       a."JEWELRY FLAG",
       a."FOOTWEAR-STREET FLAG",
       a."APPAREL-ACTIVE FLAG",
       a."BOYS SHOES-SHOES FLAG",
       a."GIRLS ACCESSORY FLAG",
       a."BOYS APPAREL-TOP FLAG",
       a."BOYS APPAREL-BOTTOM FLAG",
       a."GIRLS APPAREL-OUTERWEAR FLAG",
       a."APPAREL-SWIM FLAG",
       a."FK ON JF FOOTWEAR FLAG",
       a."BOYS ACCESSORY FLAG",
       a."GIRLS APPAREL-SWIMWEAR FLAG",
       a."BOYS APPAREL-OUTERWEAR FLAG",
       a."BOYS APPAREL-SWIMWEAR FLAG",
       a."FK ON JF APPAREL FLAG",
       a."GIRLS APPAREL-OUTER/OTHER FLAG",
       b."APPAREL FLAG",
       b."FOOTWEAR FLAG"
FROM _order_product_info_product_base a
JOIN _order_product_info_department_base b
    ON a.order_id_for_join = b.order_id_for_join;

CREATE OR REPLACE TEMPORARY TABLE _order_metrics AS
SELECT od.business_unit,
       od.region,
       od.country,
       od.order_date,
       od.order_id,
       od.order_type,
       SUM(od.total_qty_sold)        AS total_qty_sold,
       SUM(od.total_cogs)            AS total_cogs,
       SUM(od.total_product_revenue) AS total_product_revenue
FROM _order_detail od
GROUP BY od.business_unit,
         od.region,
         od.country,
         od.order_date,
         od.order_id,
         od.order_type;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb028_merch_basket_analysis AS
SELECT om.business_unit,
       om.region,
       om.country,
       om.order_date,
       om.order_id,
       om.order_type,
       (CASE WHEN opi."FOOTWEAR-ANKLE BOOTS FLAG" = 1 THEN 'Y' ELSE 'N' END)      AS "FOOTWEAR-ANKLE BOOTS FLAG",
       (CASE WHEN opi."FOOTWEAR-FLAT BOOTS FLAG" = 1 THEN 'Y' ELSE 'N' END)       AS "FOOTWEAR-FLAT BOOTS FLAG",
       (CASE WHEN opi."GIRLS SHOES-SHOES FLAG" = 1 THEN 'Y' ELSE 'N' END)         AS "GIRLS SHOES-SHOES FLAG",
       (CASE WHEN opi."FOOTWEAR-HEELED SANDALS FLAG" = 1 THEN 'Y' ELSE 'N' END)   AS "FOOTWEAR-HEELED SANDALS FLAG",
       (CASE WHEN opi."FOOTWEAR-HEELED BOOTS FLAG" = 1 THEN 'Y' ELSE 'N' END)     AS "FOOTWEAR-HEELED BOOTS FLAG",
       (CASE WHEN opi."FOOTWEAR-WEDGES FLAG" = 1 THEN 'Y' ELSE 'N' END)           AS "FOOTWEAR-WEDGES FLAG",
       (CASE WHEN opi."HANDBAGS FLAG" = 1 THEN 'Y' ELSE 'N' END)                  AS "HANDBAGS FLAG",
       (CASE WHEN opi."FOOTWEAR-PUMPS FLAG" = 1 THEN 'Y' ELSE 'N' END)            AS "FOOTWEAR-PUMPS FLAG",
       (CASE WHEN opi."FOOTWEAR-BOOTIES FLAG" = 1 THEN 'Y' ELSE 'N' END)          AS "FOOTWEAR-BOOTIES FLAG",
       (CASE WHEN opi."FOOTWEAR-SANDALS-DRESSY FLAG" = 1 THEN 'Y' ELSE 'N' END)   AS "FOOTWEAR-SANDALS-DRESSY FLAG",
       (CASE WHEN opi."FOOTWEAR-FLAT SANDALS FLAG" = 1 THEN 'Y' ELSE 'N' END)     AS "FOOTWEAR-FLAT SANDALS FLAG",
       (CASE WHEN opi."GIRLS APPAREL-BOTTOM FLAG" = 1 THEN 'Y' ELSE 'N' END)      AS "GIRLS APPAREL-BOTTOM FLAG",
       (CASE WHEN opi."APPAREL-TOPS FLAG" = 1 THEN 'Y' ELSE 'N' END)              AS "APPAREL-TOPS FLAG",
       (CASE WHEN opi."FOOTWEAR-FLATS FLAG" = 1 THEN 'Y' ELSE 'N' END)            AS "FOOTWEAR-FLATS FLAG",
       (CASE WHEN opi."APPAREL-JACKETS FLAG" = 1 THEN 'Y' ELSE 'N' END)           AS "APPAREL-JACKETS FLAG",
       (CASE WHEN opi."FOOTWEAR-SNEAKERS FLAG" = 1 THEN 'Y' ELSE 'N' END)         AS "FOOTWEAR-SNEAKERS FLAG",
       (CASE WHEN opi."FOOTWEAR-SANDALS-FLAT FLAG" = 1 THEN 'Y' ELSE 'N' END)     AS "FOOTWEAR-SANDALS-FLAT FLAG",
       (CASE WHEN opi."APPAREL-BOTTOMS FLAG" = 1 THEN 'Y' ELSE 'N' END)           AS "APPAREL-BOTTOMS FLAG",
       (CASE WHEN opi."APPAREL-DENIM FLAG" = 1 THEN 'Y' ELSE 'N' END)             AS "APPAREL-DENIM FLAG",
       (CASE WHEN opi."FOOTWEAR-COLD WEATHER FLAG" = 1 THEN 'Y' ELSE 'N' END)     AS "FOOTWEAR-COLD WEATHER FLAG",
       (CASE WHEN opi."GIRLS APPAREL-DRESS FLAG" = 1 THEN 'Y' ELSE 'N' END)       AS "GIRLS APPAREL-DRESS FLAG",
       (CASE WHEN opi."APPAREL-SWEATERS FLAG" = 1 THEN 'Y' ELSE 'N' END)          AS "APPAREL-SWEATERS FLAG",
       (CASE WHEN opi."ACCESSORIES FLAG" = 1 THEN 'Y' ELSE 'N' END)               AS "ACCESSORIES FLAG",
       (CASE WHEN opi."FOOTWEAR-FUZZIES FLAG" = 1 THEN 'Y' ELSE 'N' END)          AS "FOOTWEAR-FUZZIES FLAG",
       (CASE WHEN opi."GIRLS APPAREL-TOP FLAG" = 1 THEN 'Y' ELSE 'N' END)         AS "GIRLS APPAREL-TOP FLAG",
       (CASE WHEN opi."JEWELRY FLAG" = 1 THEN 'Y' ELSE 'N' END)                   AS "JEWELRY FLAG",
       (CASE WHEN opi."FOOTWEAR-STREET FLAG" = 1 THEN 'Y' ELSE 'N' END)           AS "FOOTWEAR-STREET FLAG",
       (CASE WHEN opi."APPAREL-ACTIVE FLAG" = 1 THEN 'Y' ELSE 'N' END)            AS "APPAREL-ACTIVE FLAG",
       (CASE WHEN opi."BOYS SHOES-SHOES FLAG" = 1 THEN 'Y' ELSE 'N' END)          AS "BOYS SHOES-SHOES FLAG",
       (CASE WHEN opi."GIRLS ACCESSORY FLAG" = 1 THEN 'Y' ELSE 'N' END)           AS "GIRLS ACCESSORY FLAG",
       (CASE WHEN opi."BOYS APPAREL-TOP FLAG" = 1 THEN 'Y' ELSE 'N' END)          AS "BOYS APPAREL-TOP FLAG",
       (CASE WHEN opi."BOYS APPAREL-BOTTOM FLAG" = 1 THEN 'Y' ELSE 'N' END)       AS "BOYS APPAREL-BOTTOM FLAG",
       (CASE WHEN opi."GIRLS APPAREL-OUTERWEAR FLAG" = 1 THEN 'Y' ELSE 'N' END)   AS "GIRLS APPAREL-OUTERWEAR FLAG",
       (CASE WHEN opi."APPAREL-SWIM FLAG" = 1 THEN 'Y' ELSE 'N' END)              AS "APPAREL-SWIM FLAG",
       (CASE WHEN opi."FK ON JF FOOTWEAR FLAG" = 1 THEN 'Y' ELSE 'N' END)         AS "FK ON JF FOOTWEAR FLAG",
       (CASE WHEN opi."BOYS ACCESSORY FLAG" = 1 THEN 'Y' ELSE 'N' END)            AS "BOYS ACCESSORY FLAG",
       (CASE WHEN opi."GIRLS APPAREL-SWIMWEAR FLAG" = 1 THEN 'Y' ELSE 'N' END)    AS "GIRLS APPAREL-SWIMWEAR FLAG",
       (CASE WHEN opi."BOYS APPAREL-OUTERWEAR FLAG" = 1 THEN 'Y' ELSE 'N' END)    AS "BOYS APPAREL-OUTERWEAR FLAG",
       (CASE WHEN opi."BOYS APPAREL-SWIMWEAR FLAG" = 1 THEN 'Y' ELSE 'N' END)     AS "BOYS APPAREL-SWIMWEAR FLAG",
       (CASE WHEN opi."FK ON JF APPAREL FLAG" = 1 THEN 'Y' ELSE 'N' END)          AS "FK ON JF APPAREL FLAG",
       (CASE WHEN opi."GIRLS APPAREL-OUTER/OTHER FLAG" = 1 THEN 'Y' ELSE 'N' END) AS "GIRLS APPAREL-OUTER/OTHER FLAG",
       (CASE WHEN opi."APPAREL FLAG" = 1 THEN 'Y' ELSE 'N' END)                   AS "APPAREL FLAG",
       (CASE WHEN opi."FOOTWEAR FLAG" = 1 THEN 'Y' ELSE 'N' END)                  AS "FOOTWEAR FLAG",
       (CASE
            WHEN om.total_qty_sold = 1 THEN 'Orders With Single Item'
            WHEN om.total_qty_sold > 1 THEN 'Orders With 2+ Items' END)           AS order_qty_filter,
       om.total_qty_sold,
       om.total_cogs,
       om.total_product_revenue
FROM _order_metrics om
JOIN _order_product_info opi
    ON opi.order_id_for_join = om.order_id;
