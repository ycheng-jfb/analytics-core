SET target_table = 'stg.dim_universal_product';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE,
                                  FALSE));

-- Initial Load / Full Refresh
-- UPDATE stg.meta_table_dependency_watermark
-- SET high_watermark_datetime = '1900-01-01',
-- meta_update_datetime = CURRENT_TIMESTAMP()
-- WHERE table_name = $target_table;
-- SET is_full_refresh = TRUE;


-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_lake_view_centric_ed_colorway = (SELECT stg.udf_get_watermark($target_table, 'lake_view.centric.ed_colorway'));
SET wm_lake_view_centric_ed_sku = (SELECT stg.udf_get_watermark($target_table, 'lake_view.centric.ed_sku'));


CREATE OR REPLACE TEMP TABLE _dim_universal_product_base
(
    centric_style_id INT,
    centric_color_id INT
);

-- Full Refresh
INSERT INTO _dim_universal_product_base(centric_style_id, centric_color_id)
SELECT DISTINCT tfg_style_ref, id
FROM lake_view.centric.ed_colorway
WHERE $is_full_refresh = TRUE;

-- Incremental Refresh
INSERT INTO _dim_universal_product_base(centric_style_id, centric_color_id)
SELECT DISTINCT tfg_style_ref,
                id
FROM (SELECT tfg_style_ref, id
      FROM lake_view.centric.ed_colorway
      WHERE meta_update_datetime > $wm_lake_view_centric_ed_colorway
        AND NOT $is_full_refresh
      UNION ALL
      SELECT ec.tfg_style_ref, ec.id
      FROM lake_view.centric.ed_colorway ec
               JOIN lake_view.centric.ed_sku es
                    ON ec.id = es.realized_color
                        AND ec.tfg_style_ref = es.style_001
      WHERE es.meta_update_datetime > $wm_lake_view_centric_ed_sku)
WHERE NOT $is_full_refresh;


CREATE OR REPLACE TEMP TABLE _dim_most_recent_szn AS
WITH _max_szn AS (SELECT a.node_name                                                                  AS color_name,
                         a.id                                                                         AS centric_color_id,
                         a.tfg_marketing_color_name                                                   AS site_color,
                         SUBSTRING(a.tfg_color1, CHARINDEX(':', a.tfg_color1) + 1, LEN(a.tfg_color1)) AS color_family,
                         CASE
                             WHEN a.tfg_color_standard LIKE ('PRINT%') THEN NULL
                             ELSE a.tfg_color_standard END                                            AS color_value,
                         CASE
                             WHEN a.tfg_color_standard LIKE ('PRINT%') THEN
                                 SUBSTRING(a.tfg_color_standard, CHARINDEX(':', a.tfg_color_standard) + 1,
                                           LEN(a.tfg_color_standard))
                             ELSE NULL END                                                            AS print,
                         a.code                                                                       AS style_number_po,
                         a.tfg_style_ref                                                              AS centric_style_id,
                         MAX(TRY_TO_DATE(a.tfg_showroom3::VARCHAR, 'YYYY-MM'))                        AS last_showroom,
                         CASE
                             WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(a.tfg_showroom3::VARCHAR, 'YYYY-MM'))) IN (1, 2, 3)
                                 THEN CONCAT('Spring ',
                                             DATE_PART(YEAR, MAX(TRY_TO_DATE(a.tfg_showroom3::VARCHAR, 'YYYY-MM'))))
                             WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(a.tfg_showroom3::VARCHAR, 'YYYY-MM'))) IN (4, 5, 6)
                                 THEN CONCAT('Summer ',
                                             DATE_PART(YEAR, MAX(TRY_TO_DATE(a.tfg_showroom3::VARCHAR, 'YYYY-MM'))))
                             WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(a.tfg_showroom3::VARCHAR, 'YYYY-MM'))) IN (7, 8, 9)
                                 THEN CONCAT('Fall ',
                                             DATE_PART(YEAR, MAX(TRY_TO_DATE(a.tfg_showroom3::VARCHAR, 'YYYY-MM'))))
                             ELSE CONCAT('Winter ',
                                         DATE_PART(YEAR, MAX(TRY_TO_DATE(a.tfg_showroom3::VARCHAR, 'YYYY-MM'))))
                             END                                                                      AS season
                  FROM lake_view.centric.ed_colorway a
                           JOIN _dim_universal_product_base b
                                ON a.id = b.centric_color_id AND a.tfg_style_ref = b.centric_style_id
                  GROUP BY a.node_name,
                           a.id,
                           a.tfg_marketing_color_name,
                           color_family,
                           color_value,
                           print,
                           a.code,
                           a.tfg_style_ref
                  ORDER BY a.node_name,
                           a.id)
SELECT centric_color_id,
       centric_style_id,
       last_showroom,
       season
FROM _max_szn
;

CREATE OR REPLACE TEMP TABLE _intermed AS
SELECT e.node_name                            AS sku,
       a.last_showroom,
       a.season,
       e.style_001                            AS centric_style_id,
       e.tfg_style_sku_segment                AS style_sku,
       b.code                                 AS style_number_po,
       SUBSTRING(b.tfg_gender, CHARINDEX(':', b.tfg_gender) + 1,
                 LEN(b.tfg_gender))           AS tfg_gender,
       SUBSTRING(b.tfg_core_fashion, CHARINDEX(':', b.tfg_core_fashion) + 1,
                 LEN(b.tfg_core_fashion))     AS tfg_core_fashion,
       SUBSTRING(b.tfg_rank, CHARINDEX(':', b.tfg_rank) + 1,
                 LEN(b.tfg_rank))             AS tfg_rank,
       SUBSTRING(b.tfg_coverage, CHARINDEX(':', b.tfg_coverage) + 1,
                 LEN(b.tfg_coverage))         AS tfg_coverage,
       b.tfg_collection,
       e.realized_color                       AS centric_color_id,
       e.tfg_color_sku_segment                AS color_sku,
       e.tfg_colorway_sku                     AS colorway_sku,
       c.node_name                            AS color_name,
       c.tfg_marketing_color_name             AS site_color_name,
       e.realized_size                        AS centric_size_id,
       e.tfg_size_sku_segment                 AS size_sku,
       x.node_name                            AS size,
       f.node_name                            AS department,
       g.node_name                            AS subdepartment,
       h.node_name                            AS class,
       i.node_name                            AS subclass,
       j.node_name                            AS category,
       k.node_name                            AS subcategory,
       d.id                                   AS centric_material_id,
       d.node_name                            AS material_name,
       SUBSTRING(d.tfg_fabric_category, CHARINDEX(':', d.tfg_fabric_category) + 1,
                 LEN(d.tfg_fabric_category))  AS tfg_fabric_category,
       SUBSTRING(d.tfg_material_opacity, CHARINDEX(':', d.tfg_material_opacity) + 1,
                 LEN(d.tfg_material_opacity)) AS tfg_material_opacity
FROM _dim_most_recent_szn a
         JOIN lake_view.centric.ed_style b
              ON a.centric_style_id = b.id
         LEFT JOIN lake_view.centric.ed_material d
                   ON d.id = b.tfg_first_main_material
         JOIN lake_view.centric.ed_colorway c
              ON c.id = a.centric_color_id
         JOIN lake_view.centric.ed_sku e
              ON e.realized_color = a.centric_color_id AND e.style_001 = a.centric_style_id
         JOIN lake_view.centric.ed_product_size x
              ON x.id = e.realized_size
         LEFT JOIN lake_view.centric.ed_classifier1 f
                   ON b.tfg_classifier1 = f.id
         LEFT JOIN lake_view.centric.ed_classifier0 g
                   ON b.tfg_classifier0 = g.id
         LEFT JOIN lake_view.centric.ed_classifier2 h
                   ON b.tfg_classifier2 = h.id
         LEFT JOIN lake_view.centric.ed_classifier3 i
                   ON b.classifier3 = i.id
         LEFT JOIN lake_view.centric.ed_category1 j
                   ON b.category1 = j.id
         LEFT JOIN lake_view.centric.ed_category2 k
                   ON b.category2 = k.id
;

CREATE OR REPLACE TEMP TABLE _upm_tier_1 AS
SELECT t.sku,
       t.last_showroom,
       t.season,
       t.centric_style_id,
       t.style_sku,
       t.style_number_po,
       t.tfg_gender,
       t.tfg_core_fashion,
       t.tfg_rank,
       t.tfg_coverage,
       t.centric_color_id,
       t.color_sku,
       t.colorway_sku,
       t.color_name,
       t.site_color_name,
       t.centric_size_id,
       t.size_sku,
       t.size,
       t.department,
       t.subdepartment,
       t.class,
       t.subclass,
       t.category,
       t.subcategory,
       t.centric_material_id,
       t.material_name,
       t.tfg_fabric_category,
       t.tfg_material_opacity,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM _intermed t
         JOIN (SELECT sku, MAX(last_showroom) AS maxdate
               FROM _intermed
               GROUP BY sku) tm
              ON t.sku = tm.sku AND t.last_showroom = tm.maxdate
ORDER BY t.sku;

CREATE OR REPLACE TEMP TABLE _gfb AS
SELECT product_sku,
       description,
       latest_launch_date,
       sale_price,
       current_vip_retail,
       classification     AS gfb_class,
       subcategory        AS gfb_subcategory,
       subclass           AS gfb_subclass,
       direct_non_direct,
       eu_landed_cost,
       heel_height,
       latest_launch_date AS gfb_latest_launch_date,
       landing_cost       AS avg_landing_cost,
       na_landed_cost,
       current_vip_retail AS gfb_vip_retail,
       reorder_status,
       shared,
       size_13_15,
       ww_wc,
       current_showroom   AS gfb_latest_showroom,
       msrp               AS gfb_msrp,
       current_vip_retail AS gfb_vip
FROM reporting_prod.gfb.merch_dim_product
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_sku ORDER BY latest_launch_date DESC, current_showroom DESC) = 1;
-- fbl
CREATE OR REPLACE TEMP TABLE _fbl AS
SELECT sku,
       factory,
       buy_timing,
       IFF(UPPER(buy_timing) = 'CHASE', 'Y', 'N')             AS is_chase,
       ca_ttl_blended_ldp_dollar                              AS ca_blended_ldp,
       color_application                                      AS solid_print_colorblocked,
       eco_system,
       IFF(UPPER(editorial_features) = 'EDITORIAL', 'Y', 'N') AS is_editorial,
       fabric                                                 AS fabric_shop,
       fit_block                                              AS block_fit,
       inseams_construction                                   AS inseam_length,
       sku_status,
       style_status,
       ca_product_id                                          AS ca_product_tag_id,
       us_product_id                                          AS us_product_tag_id,
       us_product_id,
       us_ttl_blended_ldp_dollar                              AS us_blended_ldp,
       us_vip_dollar                                          AS fbl_vip,
       go_live_date,
       ca_ttl_buy_units,
       us_ttl_buy_units,
       camo,
       lined_unlined                                          AS mesh_nonmesh,
       style_status                                           AS fbl_style_status,
       style_plus_color,
       eco_system                                             AS fbl_ecosystem,
       dollar49_shop,
       icon,
       studio,
       train,
       current_showroom                                       AS _fbl_showroom,
       class                                                  AS fbl_class,
       subclass                                               AS fbl_subclass,
       category                                               AS fbl_category,
       us_msrp_dollar                                         AS fbl_msrp
FROM lake.excel.fl_items_ubt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY go_live_date DESC, meta_update_datetime DESC, meta_create_datetime DESC) =
            1;

-- sxf
CREATE OR REPLACE TEMP TABLE _sxf AS
SELECT sm.color_sku_po,
       sm.style_name,
       COALESCE(sm.po_color, sa.color_value)                                                               AS color,
       REPLACE(SUBSTRING(sm.size_scale, POSITION(': ' IN sm.size_scale), LENGTH(sm.size_scale)), ': ',
               '')                                                                                         AS size_scale,
       sa.bright,
       sm.bundle_retail,
       sm.category_type,
       sm.color_roll_up,
       sa.color_tone,
       sm.fabric_grouping,
       sm.first_showroom,
       sm.first_vendor,
       sa.heat,
       sm.po_color,
       sm.inv_aged_days,
       sm.inv_aged_months,
       sm.inv_aged_status,
       sm.last_received_date,
       sm.latest_qty,
       sm.latest_showroom,
       sm.latest_showroom                                                                                  AS sxf_latest_showroom,
       sm.msrp                                                                                             AS sxf_msrp,
       sm.persona,
       sm.site_name,
       sm.vip_price                                                                                        AS sxf_vip,
       sm.sr_weighted_est_cost_eu,
       sm.sr_weighted_est_cost_us,
       sm.sr_weighted_est_landed_cost_eu,
       sm.sr_weighted_est_landed_cost_us,
       sm.first_received_date,
       sm.gbp_vip,
       sm.euro_vip,
       sm.gbp_msrp,
       sm.euro_msrp,
       sm.sr_weighted_est_cmt_eu,
       sm.sr_weighted_est_cmt_us,
       sm.sr_weighted_est_duty_eu,
       sm.sr_weighted_est_duty_us,
       sm.sr_weighted_est_freight_eu,
       sm.sr_weighted_est_freight_us,
       sm.sr_weighted_est_primary_tariff_eu,
       sm.sr_weighted_est_primary_tariff_us,
       sm.sr_weighted_est_uk_tariff_eu,
       sm.sr_weighted_est_us_tariff_us,
       sm.category                                                                                         AS sxf_category,
       sm.subcategory                                                                                      AS sxf_subcategory,
       sm.style_number_po                                                                                  AS sxf_style_number_po
FROM reporting_prod.sxf.style_master sm
    LEFT JOIN reporting_prod.sxf.refined_style_anatomy sa
        ON sm.color_sku_po = sa.color_sku
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sm.style_name, COALESCE(sm.po_color, sa.color_value), sm.size_scale ORDER BY sm.latest_showroom DESC) =
            1;

CREATE OR REPLACE TEMP TABLE _price_sub AS
WITH _reg_price AS (SELECT node_name,
                           colorway,
                           tfg_style_ref,
                           pr.tfg_msrp,
                           pr.tfg_total_landed_cost,
                           pr.tfg_vip_price,
                           pr.tfg_price AS regular_vendor_price
                    FROM lake_view.centric.ed_price pr
                    WHERE node_name LIKE ('% - US - %')
                      AND node_name NOT ILIKE ('%plus%')),
     _plus_price AS (SELECT node_name,
                            colorway,
                            tfg_style_ref,
                            pr.tfg_msrp,
                            pr.tfg_total_landed_cost,
                            pr.tfg_vip_price,
                            pr.tfg_price AS plus_vendor_price
                     FROM lake_view.centric.ed_price pr
                     WHERE node_name LIKE ('% - US - %')
                       AND node_name ILIKE ('%plus%'))
SELECT node_name,
       pricing.centric_color_id,
       pricing.centric_style_id,
       tfg_vip_price,
       tfg_msrp,
       tfg_total_landed_cost,
       regular_vendor_price,
       plus_vendor_price
FROM (SELECT rp.node_name,
             rp.colorway                                                                          AS centric_color_id,
             rp.tfg_style_ref                                                                     AS centric_style_id,
             rp.tfg_vip_price,
             rp.tfg_msrp,
             rp.tfg_total_landed_cost,
             rp.regular_vendor_price,
             pp.plus_vendor_price,
             ROW_NUMBER() OVER (PARTITION BY rp.colorway, rp.tfg_style_ref ORDER BY rp.node_name) AS rn
      FROM _reg_price rp
               LEFT JOIN _plus_price pp
                         ON rp.colorway = pp.colorway AND rp.tfg_style_ref = pp.tfg_style_ref
                             AND rp.tfg_vip_price = pp.tfg_vip_price AND rp.tfg_msrp = pp.tfg_msrp
                             AND rp.tfg_total_landed_cost = pp.tfg_total_landed_cost
      ORDER BY rp.node_name, rp.colorway, rp.tfg_style_ref) pricing
         JOIN _dim_universal_product_base base
              ON pricing.centric_color_id = base.centric_color_id AND pricing.centric_style_id = base.centric_style_id
WHERE rn = 1;

CREATE OR REPLACE TEMP TABLE _true_max AS
SELECT a.sku,
       last_showroom,
       season,
       a.centric_style_id,
       style_sku,
       style_number_po,
       tfg_gender,
       tfg_core_fashion,
       tfg_rank,
       tfg_coverage,
       a.centric_color_id,
       color_sku,
       colorway_sku,
       color_name,
       site_color_name,
       centric_size_id,
       size_sku,
       size,
       department,
       subdepartment,
       class,
       subclass,
       category,
       subcategory,
       centric_material_id,
       material_name,
       tfg_fabric_category,
       tfg_material_opacity,
       g.description,
       latest_launch_date,
       sale_price,
       current_vip_retail,
       gfb_class,
       gfb_subcategory,
       gfb_subclass,
       direct_non_direct,
       eu_landed_cost,
       heel_height,
       gfb_latest_launch_date,
       avg_landing_cost,
       na_landed_cost,
       gfb_vip,
       gfb_msrp,
       reorder_status,
       shared,
       size_13_15,
       ww_wc,
       fbl.factory,
       buy_timing,
       is_chase,
       ca_blended_ldp,
       solid_print_colorblocked,
       eco_system,
       is_editorial,
       fabric_shop,
       block_fit,
       inseam_length,
       sku_status,
       style_status,
       ca_product_tag_id,
       us_product_tag_id,
       us_product_id,
       us_blended_ldp,
       fbl_vip,
       go_live_date,
       fbl_class,
       fbl_subclass,
       fbl_category,
       fbl_msrp,
       sxf.style_name,
       color,
       size_scale,
       bright,
       bundle_retail,
       category_type,
       color_roll_up,
       color_tone,
       first_showroom,
       first_vendor,
       heat,
       inv_aged_days,
       inv_aged_months,
       inv_aged_status,
       last_received_date,
       latest_qty,
       latest_showroom,
       sxf_msrp,
       persona,
       site_name,
       sxf_vip,
       sxf_category,
       sxf_subcategory,
       sxf_style_number_po,
       pr.tfg_msrp,
       pr.tfg_total_landed_cost,
       pr.tfg_vip_price,
       pr.regular_vendor_price,
       pr.plus_vendor_price,
       meta_create_datetime,
       meta_update_datetime
FROM _upm_tier_1 a
         LEFT JOIN _gfb g
                   ON a.colorway_sku = g.product_sku
         LEFT JOIN _fbl fbl
                   ON fbl.sku = a.colorway_sku
         LEFT JOIN _sxf sxf
                   ON sxf.color_sku_po = a.colorway_sku
         LEFT JOIN _price_sub pr
                   ON pr.centric_color_id = a.centric_color_id AND pr.centric_style_id = a.centric_style_id
ORDER BY sku;

CREATE OR REPLACE TEMP TABLE _fbl_skus AS
SELECT DISTINCT a.item_number
FROM lake_view.ultra_warehouse.item a
         JOIN _fbl b ON
        LEFT(item_number, CHARINDEX('-', item_number, CHARINDEX('-', item_number) + 1) - 1) = b.sku
ORDER BY item_number ASC;

CREATE OR REPLACE TEMP TABLE _gfb_skus AS
SELECT DISTINCT a.item_number
FROM lake_view.ultra_warehouse.item a
         JOIN _gfb b ON
        LEFT(item_number, CHARINDEX('-', item_number, CHARINDEX('-', item_number) + 1) - 1) = b.product_sku
ORDER BY item_number ASC;

CREATE OR REPLACE TEMP TABLE _sxf_skus AS
SELECT DISTINCT a.item_number
FROM lake_view.ultra_warehouse.item a
         JOIN _sxf b ON
        LEFT(item_number, CHARINDEX('-', item_number, CHARINDEX('-', item_number) + 1) - 1) = b.color_sku_po
ORDER BY item_number ASC;


-- Tier 1: Present in all of Centric, UBT and UltraMerchant, data dependency on Centric for most recent record
CREATE OR REPLACE TEMP TABLE _in_cent_and_sxf AS
SELECT DISTINCT item_number
FROM lake_view.ultra_warehouse.item
WHERE (item_number IN (SELECT node_name FROM lake_view.centric.ed_sku)
    AND
       item_number IN (SELECT item_number FROM _sxf_skus));

CREATE OR REPLACE TEMP TABLE _in_cent_and_fbl AS
SELECT DISTINCT item_number
FROM lake_view.ultra_warehouse.item
WHERE (item_number IN (SELECT node_name FROM lake_view.centric.ed_sku)
    AND
       item_number IN (SELECT item_number FROM _fbl_skus));

CREATE OR REPLACE TEMP TABLE _in_cent_and_gfb AS
SELECT DISTINCT item_number
FROM lake_view.ultra_warehouse.item
WHERE (item_number IN (SELECT node_name FROM lake_view.centric.ed_sku)
    AND
       item_number IN (SELECT item_number FROM _gfb_skus));

CREATE OR REPLACE TEMP TABLE _tier_1 AS
SELECT DISTINCT item_number
FROM (SELECT item_number
      FROM _in_cent_and_sxf
      UNION
      SELECT item_number
      FROM _in_cent_and_fbl
      UNION
      SELECT item_number
      FROM _in_cent_and_gfb);

-- Tier 2: Data in UBT and UltraMerchant, not in Centric, data dependency on UBT for most recent record
CREATE OR REPLACE TEMP TABLE _in_sxf_not_centric AS
SELECT DISTINCT item_number
FROM lake_view.ultra_warehouse.item
WHERE (item_number NOT IN (SELECT node_name FROM lake_view.centric.ed_sku)
    AND
       item_number IN (SELECT item_number FROM _sxf_skus));

CREATE OR REPLACE TEMP TABLE _in_fbl_not_centric AS
SELECT DISTINCT item_number
FROM lake_view.ultra_warehouse.item
WHERE (item_number NOT IN (SELECT node_name FROM lake_view.centric.ed_sku)
    AND
       item_number IN (SELECT item_number FROM _fbl_skus));

CREATE OR REPLACE TEMP TABLE _in_gfb_not_centric AS
SELECT DISTINCT item_number
FROM lake_view.ultra_warehouse.item
WHERE (item_number NOT IN (SELECT node_name FROM lake_view.centric.ed_sku)
    AND
       item_number IN (SELECT item_number FROM _gfb_skus));

CREATE OR REPLACE TEMP TABLE _tier_2 AS
SELECT DISTINCT item_number
FROM (SELECT item_number
      FROM _in_sxf_not_centric
      UNION
      SELECT item_number
      FROM _in_fbl_not_centric
      UNION
      SELECT item_number
      FROM _in_gfb_not_centric);

-- Tier 3: Data in UltraMerchant only (Not in Centric and not in UBT)
CREATE OR REPLACE TEMP TABLE _any_ubt AS
SELECT DISTINCT a.item_number
FROM lake_view.ultra_warehouse.item a
         JOIN
     (SELECT sku AS color_sku
      FROM _fbl
      UNION
      SELECT color_sku_po
      FROM _sxf
      UNION
      SELECT product_sku
      FROM _gfb) b ON
             LEFT(item_number, CHARINDEX('-', item_number, CHARINDEX('-', item_number) + 1) - 1) = b.color_sku;

CREATE OR REPLACE TEMP TABLE _centric_or_ubt AS
SELECT DISTINCT item_number
FROM lake_view.ultra_warehouse.item
WHERE (item_number IN (SELECT DISTINCT node_name FROM lake_view.centric.ed_sku)
    OR
       item_number IN (SELECT DISTINCT item_number FROM _any_ubt));

CREATE OR REPLACE TEMP TABLE _tier_3 AS
SELECT DISTINCT item_number
FROM lake_view.ultra_warehouse.item
WHERE item_number NOT IN (SELECT item_number FROM _centric_or_ubt);

CREATE OR REPLACE TEMP TABLE _tier2_gfb AS
SELECT a.item_number                                                                                AS sku,
       CASE
           WHEN a.item_number IN (SELECT i.item_number
                                  FROM lake_view.ultra_warehouse.item i
                                           JOIN lake_view.ultra_warehouse.company c
                                                ON i.company_id = c.company_id
                                  WHERE c.company_code NOT IN ('FABLETICS', 'SAVAGEX'))
               THEN b.gfb_latest_showroom END                                                       AS last_showroom,
       CASE
           WHEN DATE_PART(MONTH, MAX(gfb_latest_showroom)) IN (1, 2, 3)
               THEN CONCAT('Spring ', DATE_PART(YEAR, MAX(gfb_latest_showroom)))
           WHEN DATE_PART(MONTH, MAX(gfb_latest_showroom)) IN (4, 5, 6)
               THEN CONCAT('Summer ', DATE_PART(YEAR, MAX(gfb_latest_showroom)))
           WHEN DATE_PART(MONTH, MAX(gfb_latest_showroom)) IN (7, 8, 9)
               THEN CONCAT('Fall ', DATE_PART(YEAR, MAX(gfb_latest_showroom)))
           ELSE CONCAT('Winter ', DATE_PART(YEAR, MAX(gfb_latest_showroom))) END                    AS season,
       NULL                                                                                         AS centric_style_id,
       SUBSTRING(item_number, 0, CHARINDEX('-', item_number) - 1)                                   AS style_sku,
       NULL                                                                                         AS style_number_po,
       NULL                                                                                         AS tfg_gender,
       NULL                                                                                         AS tfg_core_fashion,
       NULL                                                                                         AS tfg_rank,
       NULL                                                                                         AS tfg_coverage,
       NULL                                                                                         AS centric_color_id,
       LEFT(item_number, CHARINDEX('-', item_number,
                                   CHARINDEX('-', item_number) + 1) - 1)                            AS color_sku,
       LEFT(RIGHT(item_number, LEN(item_number) - CHARINDEX('-', item_number)),
            CHARINDEX('-', RIGHT(item_number, LEN(item_number) - CHARINDEX('-', item_number))) - 1) AS colorway_sku,
       NULL                                                                                         AS color_name,
       NULL                                                                                         AS site_color_name,
       NULL                                                                                         AS centric_size_id,
       SUBSTRING(item_number, LENGTH(SUBSTR(item_number,
                                            LEN(item_number) - CHARINDEX('-', item_number))) + 5)   AS size_sku,
       NULL                                                                                         AS size,
       NULL                                                                                         AS department,
       NULL                                                                                         AS subdepartment,
       NULL                                                                                         AS class,
       NULL                                                                                         AS subclass,
       NULL                                                                                         AS category,
       NULL                                                                                         AS subcategory,
       NULL                                                                                         AS centric_material_id,
       NULL                                                                                         AS material_name,
       NULL                                                                                         AS tfg_fabric_category,
       NULL                                                                                         AS tfg_material_opacity,
       b.description,
       latest_launch_date,
       sale_price,
       current_vip_retail,
       b.gfb_class,
       b.gfb_subcategory,
       b.gfb_subclass,
       direct_non_direct,
       eu_landed_cost,
       heel_height,
       gfb_latest_launch_date,
       avg_landing_cost,
       na_landed_cost,
       gfb_vip,
       gfb_msrp,
       reorder_status,
       shared,
       size_13_15,
       ww_wc,
       NULL                                                                                         AS factory,
       NULL                                                                                         AS buy_timing,
       NULL                                                                                         AS is_chase,
       NULL                                                                                         AS ca_blended_ldp,
       NULL                                                                                         AS solid_print_colorblocked,
       NULL                                                                                         AS eco_system,
       NULL                                                                                         AS is_editorial,
       NULL                                                                                         AS fabric_shop,
       NULL                                                                                         AS block_fit,
       NULL                                                                                         AS inseam_length,
       NULL                                                                                         AS sku_status,
       NULL                                                                                         AS style_status,
       NULL                                                                                         AS ca_product_tag_id,
       NULL                                                                                         AS us_product_tag_id,
       NULL                                                                                         AS us_product_id,
       NULL                                                                                         AS us_blended_ldp,
       NULL                                                                                         AS fbl_vip,
       NULL                                                                                         AS fbl_msrp,
       NULL                                                                                         AS fbl_class,
       NULL                                                                                         AS fbl_subclass,
       NULL                                                                                         AS fbl_category,
       NULL                                                                                         AS go_live_date,
       NULL                                                                                         AS style_name,
       NULL                                                                                         AS color,
       NULL                                                                                         AS size_scale,
       NULL                                                                                         AS bright,
       NULL                                                                                         AS bundle_retail,
       NULL                                                                                         AS category_type,
       NULL                                                                                         AS color_roll_up,
       NULL                                                                                         AS color_tone,
       NULL                                                                                         AS first_showroom,
       NULL                                                                                         AS first_vendor,
       NULL                                                                                         AS heat,
       NULL                                                                                         AS inv_aged_days,
       NULL                                                                                         AS inv_aged_months,
       NULL                                                                                         AS inv_aged_status,
       NULL                                                                                         AS last_received_date,
       NULL                                                                                         AS latest_qty,
       NULL                                                                                         AS latest_showroom,
       NULL                                                                                         AS sxf_msrp,
       NULL                                                                                         AS persona,
       NULL                                                                                         AS site_name,
       NULL                                                                                         AS sxf_vip,
       NULL                                                                                         AS sxf_category,
       NULL                                                                                         AS sxf_subcategory,
       NULL                                                                                         AS sxf_style_number_po,
       NULL                                                                                         AS tfg_msrp,
       NULL                                                                                         AS tfg_total_landed_cost,
       NULL                                                                                         AS tfg_vip_price,
       NULL                                                                                         AS regular_vendor_price,
       NULL                                                                                         AS plus_vendor_price
FROM _tier_2 a
         JOIN _gfb b ON
        LEFT(a.item_number, CHARINDEX('-', a.item_number, CHARINDEX('-', a.item_number) + 1) - 1) = b.product_sku
GROUP BY item_number, gfb_latest_showroom, description, latest_launch_date, sale_price, current_vip_retail, gfb_class,
         gfb_subcategory, gfb_subclass, direct_non_direct, eu_landed_cost, heel_height, gfb_latest_launch_date,
         avg_landing_cost, na_landed_cost, gfb_vip, gfb_msrp, reorder_status, shared, size_13_15, ww_wc
;

CREATE OR REPLACE TEMP TABLE _tier2_fbl AS
SELECT a.item_number                                                                                     AS sku,
       CASE
           WHEN a.item_number IN (SELECT i.item_number
                                  FROM lake_view.ultra_warehouse.item i
                                           JOIN lake_view.ultra_warehouse.company c
                                                ON i.company_id = c.company_id
                                  WHERE c.company_code = 'FABLETICS')
               THEN _fbl_showroom END                                                                    AS last_showroom,
       CASE
           WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(_fbl_showroom::VARCHAR, 'YYYY-MM-DD'))) IN (1, 2, 3)
               THEN CONCAT('Spring ', DATE_PART(YEAR, MAX(TRY_TO_DATE(_fbl_showroom::VARCHAR, 'YYYY-MM-DD'))))
           WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(_fbl_showroom::VARCHAR, 'YYYY-MM-DD'))) IN (4, 5, 6)
               THEN CONCAT('Summer ', DATE_PART(YEAR, MAX(TRY_TO_DATE(_fbl_showroom::VARCHAR, 'YYYY-MM-DD'))))
           WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(_fbl_showroom::VARCHAR, 'YYYY-MM-DD'))) IN (7, 8, 9)
               THEN CONCAT('Fall ', DATE_PART(YEAR, MAX(TRY_TO_DATE(_fbl_showroom::VARCHAR, 'YYYY-MM-DD'))))
           ELSE CONCAT('Winter ', DATE_PART(YEAR,
                                            MAX(TRY_TO_DATE(_fbl_showroom::VARCHAR, 'YYYY-MM-DD')))) END AS season,
       NULL                                                                                              AS centric_style_id,
       SUBSTRING(item_number, 0, CHARINDEX('-', item_number) - 1)                                        AS style_sku,
       NULL                                                                                              AS style_number_po,
       NULL                                                                                              AS tfg_gender,
       NULL                                                                                              AS tfg_core_fashion,
       NULL                                                                                              AS tfg_rank,
       NULL                                                                                              AS tfg_coverage,
       NULL                                                                                              AS centric_color_id,
       LEFT(item_number, CHARINDEX('-', item_number, CHARINDEX('-', item_number) + 1) - 1)               AS color_sku,
       LEFT(RIGHT(item_number, LEN(item_number) - CHARINDEX('-', item_number)),
            CHARINDEX('-', RIGHT(item_number, LEN(item_number) - CHARINDEX('-', item_number))) -
            1)                                                                                           AS colorway_sku,
       NULL                                                                                              AS color_name,
       NULL                                                                                              AS site_color_name,
       NULL                                                                                              AS centric_size_id,
       SUBSTRING(item_number, LENGTH(SUBSTR(item_number, LEN(item_number) -
                                                         CHARINDEX('-', item_number))) + 5)              AS size_sku,
       NULL                                                                                              AS size,
       NULL                                                                                              AS department,
       NULL                                                                                              AS subdepartment,
       NULL                                                                                              AS class,
       NULL                                                                                              AS subclass,
       NULL                                                                                              AS category,
       NULL                                                                                              AS subcategory,
       NULL                                                                                              AS centric_material_id,
       NULL                                                                                              AS material_name,
       NULL                                                                                              AS tfg_fabric_category,
       NULL                                                                                              AS tfg_material_opacity,
       NULL                                                                                              AS description,
       NULL                                                                                              AS latest_launch_date,
       NULL                                                                                              AS sale_price,
       NULL                                                                                              AS current_vip_retail,
       NULL                                                                                              AS gfb_class,
       NULL                                                                                              AS gfb_subcategory,
       NULL                                                                                              AS gfb_subclass,
       NULL                                                                                              AS direct_non_direct,
       NULL                                                                                              AS eu_landed_cost,
       NULL                                                                                              AS heel_height,
       NULL                                                                                              AS gfb_latest_launch_date,
       NULL                                                                                              AS avg_landing_cost,
       NULL                                                                                              AS na_landed_cost,
       NULL                                                                                              AS gfb_vip,
       NULL                                                                                              AS gfb_msrp,
       NULL                                                                                              AS reorder_status,
       NULL                                                                                              AS shared,
       NULL                                                                                              AS size_13_15,
       NULL                                                                                              AS ww_wc,
       b.factory,
       b.buy_timing,
       b.is_chase,
       b.ca_blended_ldp,
       b.solid_print_colorblocked,
       b.eco_system,
       b.is_editorial,
       b.fabric_shop,
       b.block_fit,
       b.inseam_length,
       b.sku_status,
       b.style_status,
       b.ca_product_tag_id,
       b.us_product_tag_id,
       b.us_product_id,
       b.us_blended_ldp,
       b.fbl_vip,
       b.fbl_msrp,
       fbl_class,
       fbl_subclass,
       fbl_category,
       b.go_live_date,
       NULL                                                                                              AS style_name,
       NULL                                                                                              AS color,
       NULL                                                                                              AS size_scale,
       NULL                                                                                              AS bright,
       NULL                                                                                              AS bundle_retail,
       NULL                                                                                              AS category_type,
       NULL                                                                                              AS color_roll_up,
       NULL                                                                                              AS color_tone,
       NULL                                                                                              AS first_showroom,
       NULL                                                                                              AS first_vendor,
       NULL                                                                                              AS heat,
       NULL                                                                                              AS inv_aged_days,
       NULL                                                                                              AS inv_aged_months,
       NULL                                                                                              AS inv_aged_status,
       NULL                                                                                              AS last_received_date,
       NULL                                                                                              AS latest_qty,
       NULL                                                                                              AS latest_showroom,
       NULL                                                                                              AS sxf_msrp,
       NULL                                                                                              AS persona,
       NULL                                                                                              AS site_name,
       NULL                                                                                              AS sxf_vip,
       NULL                                                                                              AS sxf_category,
       NULL                                                                                              AS sxf_subcategory,
       NULL                                                                                              AS sxf_style_number_po,
       NULL                                                                                              AS tfg_msrp,
       NULL                                                                                              AS tfg_total_landed_cost,
       NULL                                                                                              AS tfg_vip_price,
       NULL                                                                                              AS regular_vendor_price,
       NULL                                                                                              AS plus_vendor_price
FROM _tier_2 a
         JOIN _fbl b
              ON LEFT(a.item_number, CHARINDEX('-', a.item_number, CHARINDEX('-', a.item_number) + 1) - 1) = b.sku
GROUP BY item_number, _fbl_showroom, factory, buy_timing, is_chase, ca_blended_ldp, us_blended_ldp,
         solid_print_colorblocked,
         eco_system, is_editorial, fabric_shop, block_fit, inseam_length, sku_status, style_status, ca_product_tag_id,
         us_product_tag_id, us_product_id, b.fbl_vip, b.fbl_msrp, fbl_class, fbl_subclass, fbl_category, go_live_date;


CREATE OR REPLACE TEMP TABLE _tier2_sxf AS
SELECT a.item_number                                                                                           AS sku,
       CASE
           WHEN a.item_number IN (SELECT i.item_number
                                  FROM lake_view.ultra_warehouse.item i
                                           JOIN lake_view.ultra_warehouse.company c
                                                ON i.company_id = c.company_id
                                  WHERE c.company_code = 'SAVAGEX')
               THEN sxf_latest_showroom END                                                                    AS last_showroom,
       CASE
           WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(sxf_latest_showroom::VARCHAR, 'YYYY-MM-DD'))) IN (1, 2, 3)
               THEN CONCAT('Spring ', DATE_PART(YEAR, MAX(TRY_TO_DATE(sxf_latest_showroom::VARCHAR, 'YYYY-MM-DD'))))
           WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(sxf_latest_showroom::VARCHAR, 'YYYY-MM-DD'))) IN (4, 5, 6)
               THEN CONCAT('Summer ', DATE_PART(YEAR, MAX(TRY_TO_DATE(sxf_latest_showroom::VARCHAR, 'YYYY-MM-DD'))))
           WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(sxf_latest_showroom::VARCHAR, 'YYYY-MM-DD'))) IN (7, 8, 9)
               THEN CONCAT('Fall ', DATE_PART(YEAR, MAX(TRY_TO_DATE(sxf_latest_showroom::VARCHAR, 'YYYY-MM-DD'))))
           ELSE CONCAT('Winter ', DATE_PART(YEAR,
                                            MAX(TRY_TO_DATE(sxf_latest_showroom::VARCHAR, 'YYYY-MM-DD')))) END AS season,
       NULL                                                                                                    AS centric_style_id,
       SUBSTRING(item_number, 0, CHARINDEX('-', sku) - 1)                                                      AS style_sku,
       NULL                                                                                                    AS style_number_po,
       NULL                                                                                                    AS tfg_gender,
       NULL                                                                                                    AS tfg_core_fashion,
       NULL                                                                                                    AS tfg_rank,
       NULL                                                                                                    AS tfg_coverage,
       NULL                                                                                                    AS centric_color_id,
       LEFT(sku, CHARINDEX('-', sku, CHARINDEX('-', sku) + 1) - 1)                                             AS color_sku,
       LEFT(RIGHT(sku, LEN(sku) - CHARINDEX('-', sku)), CHARINDEX('-', RIGHT(sku, LEN(sku) -
                                                                                  CHARINDEX('-', sku))) -
                                                        1)                                                     AS colorway_sku,
       NULL                                                                                                    AS color_name,
       NULL                                                                                                    AS site_color_name,
       NULL                                                                                                    AS centric_size_id,
       SUBSTRING(sku, LENGTH(SUBSTR(sku, LEN(sku) - CHARINDEX('-', sku))) +
                      5)                                                                                       AS size_sku,
       NULL                                                                                                    AS size,
       NULL                                                                                                    AS department,
       NULL                                                                                                    AS subdepartment,
       NULL                                                                                                    AS class,
       NULL                                                                                                    AS subclass,
       NULL                                                                                                    AS category,
       NULL                                                                                                    AS subcategory,
       NULL                                                                                                    AS centric_material_id,
       NULL                                                                                                    AS material_name,
       NULL                                                                                                    AS tfg_fabric_category,
       NULL                                                                                                    AS tfg_material_opacity,
       NULL                                                                                                    AS description,
       NULL                                                                                                    AS latest_launch_date,
       NULL                                                                                                    AS sale_price,
       NULL                                                                                                    AS current_vip_retail,
       NULL                                                                                                    AS gfb_class,
       NULL                                                                                                    AS gfb_subcategory,
       NULL                                                                                                    AS gfb_subclass,
       NULL                                                                                                    AS direct_non_direct,
       NULL                                                                                                    AS eu_landed_cost,
       NULL                                                                                                    AS heel_height,
       NULL                                                                                                    AS gfb_latest_launch_date,
       NULL                                                                                                    AS avg_landing_cost,
       NULL                                                                                                    AS na_landed_cost,
       NULL                                                                                                    AS gfb_vip,
       NULL                                                                                                    AS gfb_msrp,
       NULL                                                                                                    AS reorder_status,
       NULL                                                                                                    AS shared,
       NULL                                                                                                    AS size_13_15,
       NULL                                                                                                    AS ww_wc,
       NULL                                                                                                    AS factory,
       NULL                                                                                                    AS buy_timing,
       NULL                                                                                                    AS is_chase,
       NULL                                                                                                    AS ca_blended_ldp,
       NULL                                                                                                    AS solid_print_colorblocked,
       NULL                                                                                                    AS eco_system,
       NULL                                                                                                    AS is_editorial,
       NULL                                                                                                    AS fabric_shop,
       NULL                                                                                                    AS block_fit,
       NULL                                                                                                    AS inseam_length,
       NULL                                                                                                    AS sku_status,
       NULL                                                                                                    AS style_status,
       NULL                                                                                                    AS ca_product_tag_id,
       NULL                                                                                                    AS us_product_tag_id,
       NULL                                                                                                    AS us_product_id,
       NULL                                                                                                    AS us_blended_ldp,
       NULL                                                                                                    AS fbl_vip,
       NULL                                                                                                    AS fbl_msrp,
       NULL                                                                                                    AS fbl_class,
       NULL                                                                                                    AS fbl_subclass,
       NULL                                                                                                    AS fbl_category,
       NULL                                                                                                    AS go_live_date,
       --
       b.style_name,
       b.color,
       b.size_scale,
       b.bright,
       b.bundle_retail,
       b.category_type,
       b.color_roll_up,
       b.color_tone,
       b.first_showroom,
       b.first_vendor,
       b.heat,
       b.inv_aged_days,
       b.inv_aged_months,
       b.inv_aged_status,
       b.last_received_date,
       b.latest_qty,
       b.latest_showroom,
       b.sxf_msrp,
       b.persona,
       b.site_name,
       b.sxf_vip,
       sxf_category,
       sxf_subcategory,
       sxf_style_number_po,
       NULL                                                                                                    AS tfg_msrp,
       NULL                                                                                                    AS tfg_total_landed_cost,
       NULL                                                                                                    AS tfg_vip_price,
       NULL                                                                                                    AS regular_vendor_price,
       NULL                                                                                                    AS plus_vendor_price
FROM _tier_2 a
         JOIN _sxf b
              ON LEFT(a.item_number, CHARINDEX('-', a.item_number, CHARINDEX('-', a.item_number) + 1) - 1) =
                 b.color_sku_po
GROUP BY item_number, sxf_latest_showroom, style_name, color, size_scale, bright, bundle_retail, category_type,
         color_roll_up, color_tone, first_showroom, first_vendor, heat, inv_aged_days, inv_aged_days, inv_aged_months,
         inv_aged_status, last_received_date, latest_qty, latest_showroom, sxf_msrp, persona, site_name, sxf_vip,
         sxf_category, sxf_subcategory, sxf_style_number_po;

CREATE OR REPLACE TEMP TABLE _tier_2_upm AS
SELECT *,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM _tier2_fbl
UNION
SELECT *,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM _tier2_gfb
UNION
SELECT *,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM _tier2_sxf;

CREATE OR REPLACE TEMP TABLE _um_po_detail AS
SELECT a.item_number AS sku,
       a.description,
       a.short_description,
       a.generic_description,
       a.class,
       a.wms_class,
       a.consumable_item_number,
       a.type_code_id,
       a.class_type_code_id,
       a.datetime_added,
       a.datetime_modified,
       a.company_id,
       a.is_hazardous,
       a.plm_maps_to_item_number,
       a.plm_category,
       a.plm_sub_category,
       a.plm_gender,
       a.division,
       a.department,
       a.sub_department,
       a.category,
       a.sub_category,
       a.color_code,
       a.size,
       a.gender,
       a.hash,
       b.po_id,
       b.po_dtl_id,
       b.division_id,
       b.show_room,
       b.date_launch,
       b.po_number,
       b.color,
       b.gp_description,
       b.qty,
       b.vendor_id,
       b.xfd,
       b.fc_delivery,
       b.delivery,
       b.inco_term,
       b.freight,
       b.duty,
       b.cmt,
       b.date_create,
       b.date_update,
       b.cost,
       b.region_id,
       b.freight_method,
       b.shipping_port,
       b.wms_code,
       b.style_name,
       b.style_type,
       b.vend_name,
       b.brand,
       b.po_status_id,
       b.po_status_text,
       b.line_status,
       b.country_origin,
       b.hts_code,
       b.duty_percentage,
       b.plm_style,
       b.fabric_category,
       b.material_content,
       b.material,
       b.hts_update,
       b.buyer,
       b.style_part,
       b.component,
       b.bom_description,
       b.warehouse_id,
       b.mv2_planstyleoid,
       b.dimension,
       b.warehouse_id_origin,
       b.style_rank,
       b.primary_tariff,
       b.uk_tariff,
       b.us_tariff,
       b.avg_volume,
       b.landed_cost,
       b.row_number,
       b.official_factory_name,
       b.original_end_ship_window_date,
       b.po_type,
       b.qty_ghosted,
       b.uk_hts_code,
       b.source_vendor_name,
       b.source_vendor_id,
       b.style_direct,
       b.is_retail,
       b.sku_origin_po,
       b.uk_duty_percentage,
       b.factory_city,
       b.factory_state,
       b.factory_zip_code,
       b.factory_country,
       b.ca_hts_code,
       b.ca_duty_percentage,
       b.commission,
       b.inspection,
       b.sc_category,
       b.mx_duty_percentage,
       b.mx_hts_code,
       b.mx_tariff,
       b.split_po,
       b.user_create,
       b.user_update,
       b.case_pack_size,
       b.num_case_pack,
       b.booking,
       b.top_approval,
       b.transportation,
       b.size_name,
       b.scale,
       b.direct,
       b.style_subclass,
       b.vendor_type,
       b.payment_terms,
       b.msrp_eu,
       b.vip_eu,
       b.msrp_gb,
       b.vip_gb,
       b.msrp_us,
       b.vip_us,
       b.avg_weight,
       b.actual_volume,
       b.actual_weight,
       b.original_showroom,
       b.original_showroom_locked,
       b.is_cancelled,
       b.actual_landed_cost,
       b.reporting_landed_cost
FROM lake_view.ultra_warehouse.item a
         LEFT JOIN reporting_prod.gsc.po_detail_dataset b ON a.item_number = b.sku
WHERE a.item_number IN (SELECT DISTINCT item_number FROM _tier_3)
ORDER BY a.item_number ASC;

CREATE OR REPLACE TEMP TABLE _tier_3_upm AS
SELECT sku,
       MAX(TRY_TO_DATE(show_room::VARCHAR, 'YYYY-MM')) OVER (PARTITION BY sku)                          AS last_showroom,
       CASE
           WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(show_room::VARCHAR, 'YYYY-MM'))) IN (1, 2, 3)
               THEN CONCAT('Spring ', DATE_PART(YEAR, MAX(TRY_TO_DATE(show_room::VARCHAR, 'YYYY-MM'))))
           WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(show_room::VARCHAR, 'YYYY-MM'))) IN (4, 5, 6)
               THEN CONCAT('Summer ', DATE_PART(YEAR, MAX(TRY_TO_DATE(show_room::VARCHAR, 'YYYY-MM'))))
           WHEN DATE_PART(MONTH, MAX(TRY_TO_DATE(show_room::VARCHAR, 'YYYY-MM'))) IN (7, 8, 9)
               THEN CONCAT('Fall ', DATE_PART(YEAR, MAX(TRY_TO_DATE(show_room::VARCHAR, 'YYYY-MM'))))
           ELSE CONCAT('Winter ', DATE_PART(YEAR, MAX(TRY_TO_DATE(show_room::VARCHAR, 'YYYY-MM')))) END AS season,
       NULL                                                                                             AS centric_style_id,
       SUBSTRING(sku, 0, CHARINDEX('-', sku) - 1)                                                       AS style_sku,
       NULL                                                                                             AS style_number_po,
       gender                                                                                           AS tfg_gender,
       NULL                                                                                             AS tfg_core_fashion,
       style_rank                                                                                       AS tfg_rank,
       NULL                                                                                             AS tfg_coverage,
       NULL                                                                                             AS centric_color_id,
       LEFT(sku, CHARINDEX('-', sku, CHARINDEX('-', sku) + 1) - 1)                                      AS color_sku,
       LEFT(RIGHT(sku, LEN(sku) - CHARINDEX('-', sku)),
            CHARINDEX('-', RIGHT(sku, LEN(sku) - CHARINDEX('-', sku))) - 1)                             AS colorway_sku,
       color                                                                                            AS color_name,
       color                                                                                            AS site_color_name,
       NULL                                                                                             AS centric_size_id,
       SUBSTRING(sku, LENGTH(SUBSTR(sku, LEN(sku) - CHARINDEX('-', sku))) + 5)                          AS size_sku,
       size,
       department,
       sub_department                                                                                   AS subdepartment,
       class,
       style_subclass                                                                                   AS subclass,
       category,
       sub_category                                                                                     AS subcategory,
       NULL                                                                                             AS centric_material_id,
       material                                                                                         AS material_name,
       fabric_category                                                                                  AS tfg_fabric_category,
       NULL                                                                                             AS tfg_material_opacity,
       NULL                                                                                             AS description,
       NULL                                                                                             AS latest_launch_date,
       NULL                                                                                             AS sale_price,
       NULL                                                                                             AS current_vip_retail,
       NULL                                                                                             AS gfb_class,
       NULL                                                                                             AS gfb_subcategory,
       NULL                                                                                             AS gfb_subclass,
       NULL                                                                                             AS direct_non_direct,
       NULL                                                                                             AS eu_landed_cost,
       NULL                                                                                             AS heel_height,
       NULL                                                                                             AS gfb_latest_launch_date,
       NULL                                                                                             AS avg_landing_cost,
       NULL                                                                                             AS na_landed_cost,
       NULL                                                                                             AS gfb_vip,
       NULL                                                                                             AS gfb_msrp,
       NULL                                                                                             AS reorder_status,
       NULL                                                                                             AS shared,
       NULL                                                                                             AS size_13_15,
       NULL                                                                                             AS ww_wc,
       NULL                                                                                             AS factory,
       NULL                                                                                             AS buy_timing,
       NULL                                                                                             AS is_chase,
       NULL                                                                                             AS ca_blended_ldp,
       NULL                                                                                             AS solid_print_colorblocked,
       NULL                                                                                             AS eco_system,
       NULL                                                                                             AS is_editorial,
       NULL                                                                                             AS fabric_shop,
       NULL                                                                                             AS block_fit,
       NULL                                                                                             AS inseam_length,
       NULL                                                                                             AS sku_status,
       NULL                                                                                             AS style_status,
       NULL                                                                                             AS ca_product_tag_id,
       NULL                                                                                             AS us_product_tag_id,
       NULL                                                                                             AS us_product_id,
       NULL                                                                                             AS us_blended_ldp,
       NULL                                                                                             AS fbl_vip,
       NULL                                                                                             AS fbl_msrp,
       NULL                                                                                             AS fbl_class,
       NULL                                                                                             AS fbl_subclass,
       NULL                                                                                             AS fbl_category,
       NULL                                                                                             AS go_live_date,
       NULL                                                                                             AS style_name,
       NULL                                                                                             AS color,
       NULL                                                                                             AS size_scale,
       NULL                                                                                             AS bright,
       NULL                                                                                             AS bundle_retail,
       NULL                                                                                             AS category_type,
       NULL                                                                                             AS color_roll_up,
       NULL                                                                                             AS color_tone,
       NULL                                                                                             AS first_showroom,
       NULL                                                                                             AS first_vendor,
       NULL                                                                                             AS heat,
       NULL                                                                                             AS inv_aged_days,
       NULL                                                                                             AS inv_aged_months,
       NULL                                                                                             AS inv_aged_status,
       NULL                                                                                             AS last_received_date,
       NULL                                                                                             AS latest_qty,
       NULL                                                                                             AS latest_showroom,
       NULL                                                                                             AS sxf_msrp,
       NULL                                                                                             AS persona,
       NULL                                                                                             AS site_name,
       NULL                                                                                             AS sxf_vip,
       NULL                                                                                             AS sxf_category,
       NULL                                                                                             AS sxf_subcategory,
       NULL                                                                                             AS sxf_style_number_po,
       NULL                                                                                             AS tfg_msrp,
       NULL                                                                                             AS tfg_total_landed_cost,
       NULL                                                                                             AS tfg_vip_price,
       NULL                                                                                             AS regular_vendor_price,
       NULL                                                                                             AS plus_vendor_price,
       $execution_start_time                                                                            AS meta_create_datetime,
       $execution_start_time                                                                            AS meta_update_datetime
FROM _um_po_detail
GROUP BY sku, show_room, gender, style_rank, color_sku, colorway_sku, color, size_sku, size, department, subdepartment,
         class, subclass, category, subcategory, centric_material_id, material_name, tfg_fabric_category,
         tfg_material_opacity
ORDER BY 1 DESC;

CREATE OR REPLACE TEMP TABLE _upm_tier_2 AS
SELECT *
FROM (SELECT *
      FROM _tier_2_upm
      UNION
      SELECT *
      FROM _tier_3_upm)
ORDER BY sku ASC;

CREATE OR REPLACE TEMP TABLE _true_max_2 AS
SELECT *
FROM _true_max
UNION
SELECT sku,
       last_showroom,
       season,
       centric_style_id,
       style_sku,
       style_number_po,
       tfg_gender,
       tfg_core_fashion,
       tfg_rank,
       tfg_coverage,
       centric_color_id,
       color_sku,
       colorway_sku,
       color_name,
       site_color_name,
       centric_size_id,
       size_sku,
       size,
       department,
       subdepartment,
       class,
       subclass,
       category,
       subcategory,
       centric_material_id,
       material_name,
       tfg_fabric_category,
       tfg_material_opacity,
       description,
       latest_launch_date,
       sale_price,
       current_vip_retail,
       gfb_class,
       gfb_subcategory,
       gfb_subclass,
       direct_non_direct,
       eu_landed_cost,
       heel_height,
       gfb_latest_launch_date,
       avg_landing_cost,
       na_landed_cost,
       gfb_vip,
       gfb_msrp,
       reorder_status,
       shared,
       size_13_15,
       ww_wc,
       factory,
       buy_timing,
       is_chase,
       ca_blended_ldp,
       solid_print_colorblocked,
       eco_system,
       is_editorial,
       fabric_shop,
       block_fit,
       inseam_length,
       sku_status,
       style_status,
       ca_product_tag_id,
       us_product_tag_id,
       us_product_id,
       us_blended_ldp,
       fbl_vip,
       go_live_date,
       fbl_class,
       fbl_subclass,
       fbl_category,
       fbl_msrp,
       style_name,
       color,
       size_scale,
       bright,
       bundle_retail,
       category_type,
       color_roll_up,
       color_tone,
       first_showroom,
       first_vendor,
       heat,
       inv_aged_days,
       inv_aged_months,
       inv_aged_status,
       last_received_date,
       latest_qty,
       latest_showroom,
       sxf_msrp,
       persona,
       site_name,
       sxf_vip,
       sxf_category,
       sxf_subcategory,
       sxf_style_number_po,
       tfg_msrp,
       tfg_total_landed_cost,
       tfg_vip_price,
       regular_vendor_price,
       plus_vendor_price,
       meta_create_datetime,
       meta_update_datetime
FROM _upm_tier_2;

-- initial load
CREATE OR REPLACE TEMP TABLE _universal_product_base AS
SELECT          sku,
                last_showroom,
                season,
                centric_style_id,
                style_sku,
                style_number_po,
                tfg_gender,
                tfg_core_fashion,
                tfg_rank,
                tfg_coverage,
                centric_color_id,
                color_sku,
                colorway_sku,
                color_name,
                site_color_name,
                centric_size_id,
                size_sku,
                size,
                department,
                subdepartment,
                class,
                subclass,
                category,
                subcategory,
                centric_material_id,
                material_name,
                tfg_fabric_category,
                tfg_material_opacity,
                description,
                latest_launch_date,
                sale_price,
                current_vip_retail,
                gfb_class,
                gfb_subcategory,
                gfb_subclass,
                direct_non_direct,
                eu_landed_cost,
                heel_height,
                gfb_latest_launch_date,
                avg_landing_cost,
                na_landed_cost,
                gfb_vip,
                gfb_msrp,
                reorder_status,
                shared,
                size_13_15,
                ww_wc,
                factory,
                buy_timing,
                is_chase,
                ca_blended_ldp,
                solid_print_colorblocked,
                eco_system,
                is_editorial,
                fabric_shop,
                block_fit,
                inseam_length,
                sku_status,
                style_status,
                ca_product_tag_id,
                us_product_tag_id,
                us_product_id,
                us_blended_ldp,
                fbl_vip,
                go_live_date,
                fbl_class,
                fbl_subclass,
                fbl_category,
                fbl_msrp,
                style_name,
                color,
                size_scale,
                bright,
                bundle_retail,
                category_type,
                color_roll_up,
                color_tone,
                first_showroom,
                first_vendor,
                heat,
                inv_aged_days,
                inv_aged_months,
                inv_aged_status,
                last_received_date,
                latest_qty,
                latest_showroom,
                sxf_msrp,
                persona,
                site_name,
                sxf_vip,
                sxf_category,
                sxf_subcategory,
                sxf_style_number_po,
                tfg_msrp,
                tfg_total_landed_cost,
                tfg_vip_price,
                regular_vendor_price,
                plus_vendor_price,
                meta_create_datetime,
                meta_update_datetime
FROM _true_max_2
WHERE $is_full_refresh = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sku, COALESCE(centric_style_id, centric_color_id, centric_size_id) ORDER BY last_showroom DESC) =
            1
UNION ALL
SELECT          sku,
                last_showroom,
                season,
                centric_style_id,
                style_sku,
                style_number_po,
                tfg_gender,
                tfg_core_fashion,
                tfg_rank,
                tfg_coverage,
                centric_color_id,
                color_sku,
                colorway_sku,
                color_name,
                site_color_name,
                centric_size_id,
                size_sku,
                size,
                department,
                subdepartment,
                class,
                subclass,
                category,
                subcategory,
                centric_material_id,
                material_name,
                tfg_fabric_category,
                tfg_material_opacity,
                description,
                latest_launch_date,
                sale_price,
                current_vip_retail,
                gfb_class,
                gfb_subcategory,
                gfb_subclass,
                direct_non_direct,
                eu_landed_cost,
                heel_height,
                gfb_latest_launch_date,
                avg_landing_cost,
                na_landed_cost,
                gfb_vip,
                gfb_msrp,
                reorder_status,
                shared,
                size_13_15,
                ww_wc,
                factory,
                buy_timing,
                is_chase,
                ca_blended_ldp,
                solid_print_colorblocked,
                eco_system,
                is_editorial,
                fabric_shop,
                block_fit,
                inseam_length,
                sku_status,
                style_status,
                ca_product_tag_id,
                us_product_tag_id,
                us_product_id,
                us_blended_ldp,
                fbl_vip,
                go_live_date,
                fbl_class,
                fbl_subclass,
                fbl_category,
                fbl_msrp,
                style_name,
                color,
                size_scale,
                bright,
                bundle_retail,
                category_type,
                color_roll_up,
                color_tone,
                first_showroom,
                first_vendor,
                heat,
                inv_aged_days,
                inv_aged_months,
                inv_aged_status,
                last_received_date,
                latest_qty,
                latest_showroom,
                sxf_msrp,
                persona,
                site_name,
                sxf_vip,
                sxf_category,
                sxf_subcategory,
                sxf_style_number_po,
                tfg_msrp,
                tfg_total_landed_cost,
                tfg_vip_price,
                regular_vendor_price,
                plus_vendor_price,
                meta_create_datetime,
                meta_update_datetime
FROM _true_max_2
WHERE NOT $is_full_refresh
  AND (sku NOT IN (SELECT DISTINCT sku FROM stg.dim_universal_product)
    OR sku IN (SELECT DISTINCT a.sku
               FROM stg.dim_universal_product a
                        JOIN _true_max_2 b ON a.sku = b.sku
               WHERE b.meta_update_datetime
                         > $wm_lake_view_centric_ed_colorway))
ORDER BY sku;

CREATE OR REPLACE TEMP TABLE _upm_po_temp AS
SELECT a.*,
       b.color              AS po_color,
       i.category           AS po_category,
       i.sub_category       AS po_subcategory,
       b.class              AS po_class,
       b.style_subclass     AS po_subclass,
       b.department         AS po_department,
       b.sub_department     AS po_subdepartment,
       b.date_launch        AS po_date_launch,
       b.msrp_us            AS po_msrp,
       b.vip_us             AS po_vip,
       b.actual_landed_cost AS po_landed_cost
FROM _universal_product_base a
         LEFT JOIN lake_view.ultra_warehouse.item i
                   ON i.item_number = a.sku
         LEFT JOIN reporting_prod.gsc.po_detail_dataset b
                   ON i.item_number = b.sku
    QUALIFY
            ROW_NUMBER() OVER (PARTITION BY a.sku, COALESCE(a.centric_style_id, a.centric_color_id, a.centric_size_id) ORDER BY last_showroom DESC) =
            1
ORDER BY a.sku ASC;

CREATE OR REPLACE TEMP TABLE _universal_product_stg AS
SELECT base.sku,
       base.last_showroom::DATE                                                   AS last_showroom,
       base.season,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN 1
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) THEN 2
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) THEN 3 END AS upm_tier,
       c.company_code                                                             AS brand,
       base.centric_style_id::INTEGER                                             AS centric_style_id,
       base.style_sku,
       base.style_number_po,
       base.tfg_gender,
       base.tfg_core_fashion,
       base.tfg_rank,
       base.tfg_coverage,
       base.centric_color_id::INTEGER                                             AS centric_color_id,
       base.color_sku,
       base.colorway_sku,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.color_name
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND c.company_code IN ('SAVAGEX') THEN base.color
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code IN ('FABLETICS') THEN po.color
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX', 'SENSA') THEN po.color
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) THEN po.color
           END                                                                    AS color_name,
       base.site_color_name,
       base.centric_size_id::INTEGER                                              AS centric_size_id,
       base.size_sku,
       base.size,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.category
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND c.company_code IN ('SAVAGEX')
               THEN base.sxf_category
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND c.company_code IN ('FABLETICS')
               THEN base.fbl_category
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX', 'SENSA') THEN po.category
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) THEN po.category
           ELSE NULL END                                                          AS category,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.subcategory
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND c.company_code = 'SAVAGEX'
               THEN base.sxf_subcategory
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'FABLETICS'
               THEN po.po_subcategory
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX', 'SENSA') THEN base.gfb_subcategory
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) THEN po.po_subcategory
           ELSE NULL END                                                          AS subcategory,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.department
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'SAVAGEX' THEN po.department
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'FABLETICS' THEN po.department
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX', 'SENSA') THEN po.department
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) THEN po.department
           ELSE NULL END                                                          AS department,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.subdepartment
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'SAVAGEX'
               THEN po.po_subdepartment
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'FABLETICS'
               THEN po.po_subdepartment
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX', 'SENSA') THEN po.po_subdepartment
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) THEN po.po_subdepartment
           ELSE NULL END                                                          AS subdepartment,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.class
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'SAVAGEX' THEN po.class
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'FABLETICS' THEN base.fbl_class
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX', 'SENSA') THEN base.gfb_class
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) THEN po.class
           ELSE NULL END                                                          AS class,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.subclass
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'SAVAGEX' THEN po.po_subclass
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'FABLETICS'
               THEN base.fbl_subclass
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX', 'SENSA') THEN base.gfb_subclass
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) THEN po.po_subclass
           ELSE NULL END                                                          AS subclass,
       base.centric_material_id::INTEGER                                          AS centric_material_id,
       base.material_name,
       base.tfg_fabric_category,
       base.tfg_material_opacity,
       base.description,
       base.direct_non_direct,
       base.heel_height,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.latest_showroom::DATE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'SAVAGEX'
               THEN po.po_date_launch::DATE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND c.company_code = 'FABLETICS'
               THEN po.po_date_launch::DATE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX', 'SENSA') THEN base.gfb_latest_launch_date::DATE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _upm_po_temp) THEN po.po_date_launch::DATE
           ELSE NULL END                                                          AS latest_launch_date,
       base.avg_landing_cost,
       base.reorder_status,
       base.shared,
       base.size_13_15,
       base.ww_wc,
       base.factory,
       base.buy_timing,
       base.is_chase,
       base.ca_blended_ldp,
       base.solid_print_colorblocked,
       base.eco_system,
       base.is_editorial,
       base.fabric_shop,
       base.block_fit,
       base.inseam_length,
       base.sku_status,
       base.style_status,
       base.ca_product_tag_id,
       base.us_product_tag_id,
       base.us_product_id,
       base.us_blended_ldp,
       base.go_live_date,
       base.style_name,
       base.size_scale,
       base.bright,
       base.bundle_retail,
       base.category_type,
       base.color_roll_up,
       base.color_tone,
       base.first_showroom,
       base.first_vendor,
       base.heat,
       base.inv_aged_days,
       base.inv_aged_months,
       base.inv_aged_status,
       base.last_received_date,
       base.latest_qty,
       base.persona,
       base.site_name,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.tfg_msrp
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND c.company_code = 'SAVAGEX' THEN base.sxf_msrp
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND c.company_code = 'FABLETICS'
               THEN base.fbl_msrp
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX') THEN base.gfb_msrp
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _um_po_detail) THEN po.po_msrp
           ELSE NULL END                                                          AS msrp,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.tfg_vip_price
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND c.company_code = 'SAVAGEX' THEN base.sxf_vip
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND c.company_code = 'FABLETICS'
               THEN base.fbl_vip
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX') THEN base.gfb_vip
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _um_po_detail) THEN po.po_vip
           ELSE NULL END                                                          AS vip_price,
       CASE
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_1) THEN base.tfg_total_landed_cost
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND c.company_code = 'SAVAGEX'
               THEN po.po_landed_cost
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND c.company_code = 'FABLETICS'
               THEN po.po_landed_cost
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_2) AND
                c.company_code NOT IN ('FABLETICS', 'SAVAGEX') THEN base.na_landed_cost
           WHEN base.sku IN (SELECT DISTINCT item_number FROM _tier_3) AND
                base.sku IN (SELECT DISTINCT sku FROM _um_po_detail) THEN po.po_landed_cost
           ELSE NULL END                                                          AS landed_cost,
       base.regular_vendor_price,
       base.plus_vendor_price,
       base.meta_create_datetime,
       base.meta_update_datetime
FROM _universal_product_base base
         LEFT JOIN lake_view.ultra_warehouse.item i ON base.sku = i.item_number
         LEFT JOIN lake_view.ultra_warehouse.company c ON c.company_id = i.company_id
         LEFT JOIN _upm_po_temp po ON po.sku = base.sku
WHERE c.company_code != 'SENSA';

-- load
INSERT INTO stg.dim_universal_product_stg(sku,
                                          last_showroom,
                                          season,
                                          upm_tier,
                                          brand,
                                          centric_style_id,
                                          style_sku,
                                          style_number_po,
                                          tfg_gender,
                                          tfg_core_fashion,
                                          tfg_rank,
                                          tfg_coverage,
                                          centric_color_id,
                                          color_sku,
                                          colorway_sku,
                                          color_name,
                                          site_color_name,
                                          centric_size_id,
                                          size_sku,
                                          size,
                                          category,
                                          subcategory,
                                          department,
                                          subdepartment,
                                          class,
                                          subclass,
                                          centric_material_id,
                                          material_name,
                                          tfg_fabric_category,
                                          tfg_material_opacity,
                                          description,
                                          direct_non_direct,
                                          heel_height,
                                          latest_launch_date,
                                          avg_landing_cost,
                                          reorder_status,
                                          shared,
                                          size_13_15,
                                          ww_wc,
                                          factory,
                                          buy_timing,
                                          is_chase,
                                          ca_blended_ldp,
                                          solid_print_colorblocked,
                                          eco_system,
                                          is_editorial,
                                          fabric_shop,
                                          block_fit,
                                          inseam_length,
                                          sku_status,
                                          style_status,
                                          ca_product_tag_id,
                                          us_product_tag_id,
                                          us_product_id,
                                          us_blended_ldp,
                                          go_live_date,
                                          style_name,
                                          size_scale,
                                          bright,
                                          bundle_retail,
                                          category_type,
                                          color_roll_up,
                                          color_tone,
                                          first_showroom,
                                          first_vendor,
                                          heat,
                                          inv_aged_days,
                                          inv_aged_months,
                                          inv_aged_status,
                                          last_received_date,
                                          latest_qty,
                                          persona,
                                          site_name,
                                          msrp,
                                          vip_price,
                                          landed_cost,
                                          regular_vendor_price,
                                          plus_vendor_price,
                                          meta_create_datetime,
                                          meta_update_datetime)
SELECT DISTINCT sku,
                last_showroom,
                season,
                upm_tier,
                brand,
                centric_style_id,
                style_sku,
                style_number_po,
                tfg_gender,
                tfg_core_fashion,
                tfg_rank,
                tfg_coverage,
                centric_color_id,
                color_sku,
                colorway_sku,
                color_name,
                site_color_name,
                centric_size_id,
                size_sku,
                size,
                category,
                subcategory,
                department,
                subdepartment,
                class,
                subclass,
                centric_material_id,
                material_name,
                tfg_fabric_category,
                tfg_material_opacity,
                description,
                direct_non_direct,
                heel_height,
                latest_launch_date,
                avg_landing_cost,
                reorder_status,
                shared,
                size_13_15,
                ww_wc,
                factory,
                buy_timing,
                is_chase,
                ca_blended_ldp,
                solid_print_colorblocked,
                eco_system,
                is_editorial,
                fabric_shop,
                block_fit,
                inseam_length,
                sku_status,
                style_status,
                ca_product_tag_id,
                us_product_tag_id,
                us_product_id,
                us_blended_ldp,
                go_live_date,
                style_name,
                size_scale,
                bright,
                bundle_retail,
                category_type,
                color_roll_up,
                color_tone,
                first_showroom,
                first_vendor,
                heat,
                inv_aged_days,
                inv_aged_months,
                inv_aged_status,
                last_received_date,
                latest_qty,
                persona,
                site_name,
                msrp,
                vip_price,
                landed_cost,
                regular_vendor_price,
                plus_vendor_price,
                meta_create_datetime,
                meta_update_datetime
FROM _universal_product_stg
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY sku ORDER BY last_showroom DESC) =1
ORDER BY sku;
