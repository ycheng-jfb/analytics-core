SET start_date = DATEADD(YEAR, -1, DATE_TRUNC(YEAR, CURRENT_DATE()));

CREATE OR REPLACE TRANSIENT TABLE gfb.third_party_brands_sales AS
SELECT dp.sub_brand AS third_party_brand,
       dp.country,
       ol.product_sku,
       dp.msrp,
       ol.order_id,
       ol.order_line_id,
       ol.order_date,
       ol.total_product_revenue_with_tariff,
       ol.total_cogs,
       ol.total_qty_sold,
       gtpwp.wholesale_price
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product dp
              ON ol.business_unit = dp.business_unit
                  AND ol.region = dp.region
                  AND ol.country = dp.country
                  AND ol.product_sku = dp.product_sku
                  AND dp.sub_brand NOT IN ('JFB', 'DREAM PAIRS', 'DREAM PAIRS KIDS', 'BRUNO MARC', 'NORTIV8')
         LEFT JOIN lake_view.sharepoint.gfb_third_party_wholesale_price gtpwp
                   ON gtpwp.product_sku = ol.product_sku
WHERE ol.order_classification = 'product order'
  AND ol.order_date >= $start_date;
