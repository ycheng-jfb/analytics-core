CREATE OR REPLACE TEMPORARY TABLE _bundles AS
SELECT DISTINCT business_unit,
    region,
    country,
    bundle_product_id,
    fk_type,
    bundle_name
FROM gfb.gfb_dim_bundle;

CREATE OR REPLACE TEMPORARY TABLE _actual_bundle_product_sku AS
SELECT DISTINCT bundle_product_id,
                product_sku
FROM gfb.gfb_dim_bundle;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb077_bundle_sales AS
SELECT ol.business_unit,
       ol.region,
       ol.country,
       ol.order_date,
       ol.bundle_product_id,
       b.bundle_name,
       b.fk_type,
       ol.product_sku,
       COALESCE(mdp.fk_site_name, mdp.style_name) AS site_name,
       mdp.color,
       IFF(abps.product_sku IS NOT NULL, 1, 0)    AS actual_bundle_style_name_flag,
       ol.bundle_order_line_id,
       ol.total_qty_sold                          AS unit_sales
FROM gfb.gfb_order_line_data_set_place_date ol
     JOIN gfb.merch_dim_product mdp
          ON ol.business_unit = mdp.business_unit
              AND ol.region = mdp.region
              AND ol.country = mdp.country
              AND ol.product_sku = mdp.product_sku
     JOIN _bundles b
          ON ol.business_unit = b.business_unit
              AND ol.bundle_product_id = b.bundle_product_id
              AND ol.region = b.region
              AND ol.country = b.country
     LEFT JOIN _actual_bundle_product_sku abps
          ON ol.product_sku = abps.product_sku
              AND ol.bundle_product_id = abps.bundle_product_id
WHERE order_classification = 'product order'
  AND ol.bundle_product_id IS NOT NULL
  AND ol.order_date >= '2024-01-01';
