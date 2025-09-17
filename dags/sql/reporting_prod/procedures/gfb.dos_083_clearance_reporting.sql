SET start_date = DATEADD(YEAR, -3, DATE_TRUNC(YEAR, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _two_for_one_skus AS
SELECT DISTINCT LEFT(dm.product_alias, POSITION(' ' IN dm.product_alias) - 1)                AS product_sku
              , s.store_country
              , UPPER(s.store_brand)                                                         AS business_unit
              , FIRST_VALUE(datetime_start::DATE)
                            OVER (PARTITION BY dm.product_alias,s.store_brand,s.store_country ORDER BY sort ASC) AS datetime_start
              , FIRST_VALUE(datetime_end::DATE)
                            OVER (PARTITION BY dm.product_alias,s.store_brand,s.store_country ORDER BY sort ASC)  AS datetime_end
FROM lake_jfb.ultra_merchant_history.featured_product fp
         JOIN (SELECT DISTINCT coalesce(product_id,master_product_id) AS product_id,
                               product_alias AS product_alias,
                               store_id
               FROM edw_prod.data_model_jfb.dim_product dp
               WHERE UPPER(dp.product_name) NOT LIKE '%DO NOT USE%'
                 AND UPPER(dp.image_url) NOT LIKE '%/OLD_%'
                 AND UPPER(dp.image_url) NOT LIKE '%DELETE%'
                 AND UPPER(dp.image_url) NOT LIKE '%/TEST%') dm
              ON fp.product_id = dm.product_id
         JOIN gfb.vw_store s
              ON dm.store_id = s.store_id
WHERE featured_product_location_id IN (23921,34821,23924,34851,34836,34841);

CREATE OR REPLACE TEMPORARY TABLE _order_info AS
SELECT fol.business_unit                                                    AS store_brand_name
     , fol.region                                                           AS store_region_abbr
     , fol.country                                                          AS country
     , fol.order_date                                                       AS "Placed Date"
     , fol.clearance_flag                                                   AS "Is_Clearance"
     , CASE
           WHEN fol.two_for_one_applied > 0 THEN '2 for 1'
           WHEN fol.bundle_product_id = 16819356 THEN '3 for 1'
           WHEN fol.two_for_one_applied = 0 AND fol.tokens_applied > 0 THEN 'other tokens'
           ELSE fol.clearance_flag END                                      AS two_for_one_purchase
     , COALESCE(fol.clearance_price, '0')                                   AS sale_price
     , fol.product_sku
     , CASE WHEN tfo.product_sku IS NOT NULL THEN '2 for 1'
            WHEN fol.bundle_product_id = 16819356 THEN '3 for 1'
            ELSE NULL END                                                   AS two_for_one_flag
     , CURRENT_DATE()                                                       AS "Last Update"
     , fol.promo_code_1
     , fol.promo_code_2
     , CASE
            WHEN fol.business_unit='Fabkids' AND (fol.promo_code_1='70OFFMARKDOWN' OR fol.promo_code_2='70OFFMARKDOWN') THEN '70'
            WHEN fol.promo_code_1='60OFFMARKDOWN' OR fol.promo_code_2='60OFFMARKDOWN' THEN '60'
            WHEN fol.promo_code_1='50OFFMARKDOWN' OR fol.promo_code_2='50OFFMARKDOWN' THEN '50'
            WHEN fol.promo_code_1='40OFFMARKDOWN' OR fol.promo_code_2='40OFFMARKDOWN' THEN '40'
            WHEN fol.promo_code_1='30OFFMARKDOWN' OR fol.promo_code_2='30OFFMARKDOWN' THEN '30'
            WHEN fol.promo_code_1='20OFFMARKDOWN' OR fol.promo_code_2='20OFFMARKDOWN' THEN '20'
            END                                                             AS markdown_bucket
     , SUM(fol.total_qty_sold)                                              AS "Total Sale Units"
     , SUM(fol.total_discount)                                              AS discount_total
     , SUM(fol.total_product_revenue_with_tariff)                           AS "Sales $"
     , SUM(fol.total_cogs)                                                  AS "Cost"
     , SUM(fol.total_product_revenue_with_tariff - fol.total_cogs)          AS "Product Gross Margin"
     , SUM(fol.total_cash_credit_amount + fol.total_non_cash_credit_amount) AS credit_redeemed_amount
     , SUM(CASE
               WHEN fol.two_for_one_applied > 0
                   THEN fol.total_qty_sold
               ELSE 0 END)                                                  AS two_for_one_qty_sold
     , SUM(CASE
               WHEN fol.two_for_one_applied > 0
                   THEN fol.total_product_revenue_with_tariff
               ELSE 0 END)                                                  AS two_for_one_product_revenue
     , SUM(CASE
               WHEN fol.two_for_one_applied > 0
                   THEN fol.total_cogs
               ELSE 0 END)                                                  AS two_for_one_cogs
     , SUM(CASE
               WHEN fol.two_for_one_applied > 0
                   THEN fol.total_product_revenue_with_tariff
               ELSE 0 END)                                                  AS two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN fol.two_for_one_applied > 0 THEN fol.total_discount
               ELSE 0 END)                                                  AS two_for_one_discount
     , SUM(CASE
               WHEN fol.two_for_one_applied > 0 THEN fol.total_cash_credit_amount + fol.total_non_cash_credit_amount
               ELSE 0 END)                                                  AS two_for_one_credit_redeemed
FROM gfb.gfb_order_line_data_set_place_date fol
         LEFT JOIN _two_for_one_skus tfo
                   ON tfo.product_sku = fol.product_sku
                       AND tfo.business_unit = fol.business_unit
                       AND tfo.store_country = fol.country
                       AND tfo.datetime_start <= fol.order_date
                       AND tfo.datetime_end > fol.order_date
WHERE fol.order_classification = 'product order'
  AND fol.order_date >= $start_date
  AND fol.order_type IN ('vip repeat', 'ecom')
GROUP BY fol.business_unit
       , fol.region
       , fol.country
       , fol.order_date
       , fol.clearance_flag
       , (CASE
           WHEN fol.two_for_one_applied > 0 THEN '2 for 1'
           WHEN fol.bundle_product_id = 16819356 THEN '3 for 1'
           WHEN fol.two_for_one_applied = 0 AND fol.tokens_applied > 0 THEN 'other tokens'
           ELSE fol.clearance_flag END)
       , COALESCE(fol.clearance_price, '0')
       , fol.product_sku
       , fol.promo_code_1
       , fol.promo_code_2
       , (CASE WHEN tfo.product_sku IS NOT NULL THEN '2 for 1'
            WHEN fol.bundle_product_id = 16819356 THEN '3 for 1'
            ELSE NULL END);

CREATE OR REPLACE TEMPORARY TABLE _sales AS
SELECT oi.*
     , prod_info.department
     , prod_info.subcategory
     , (CASE
            WHEN DATEDIFF(MONTH, prod_info.latest_launch_date, DATE_TRUNC(MONTH, CURRENT_DATE())) < 0
                THEN 'FUTURE SHOWROOM'
            WHEN DATEDIFF(MONTH, prod_info.latest_launch_date, DATE_TRUNC(MONTH, CURRENT_DATE())) < 9 THEN 'CURRENT'
            WHEN DATEDIFF(MONTH, prod_info.latest_launch_date, DATE_TRUNC(MONTH, CURRENT_DATE())) < 15 THEN 'TIER 1'
            WHEN DATEDIFF(MONTH, prod_info.latest_launch_date, DATE_TRUNC(MONTH, CURRENT_DATE())) < 24 THEN 'TIER 2'
            WHEN DATEDIFF(MONTH, prod_info.latest_launch_date, DATE_TRUNC(MONTH, CURRENT_DATE())) >= 24 THEN 'TIER 3'
    END) AS tier
     , prod_info.style_name
     , prod_info.master_color
     , prod_info.latest_launch_date
     , prod_info.current_vip_retail
     , prod_info.shared
     , prod_info.business_unit
     , prod_info.region
FROM _order_info oi
         JOIN gfb.merch_dim_product prod_info
              ON prod_info.product_sku = oi.product_sku
                  AND LOWER(prod_info.business_unit) = LOWER(oi.store_brand_name)
                  AND LOWER(prod_info.region) = LOWER(oi.store_region_abbr)
                  AND LOWER(prod_info.country) = LOWER(oi.country);


CREATE OR REPLACE TEMPORARY TABLE _inventory AS
SELECT id.business_unit
     , id.region
     , id.country
     , id.product_sku
     , id.inventory_date
     , id.clearance_price
     , IFF(tfo.product_sku IS NOT NULL, '2 for 1', NULL) AS two_for_one_flag

     , SUM(id.qty_available_to_sell)                     AS ty_eop_qty_onhand
FROM gfb.gfb_inventory_data_set id
         LEFT JOIN _two_for_one_skus tfo
                   ON tfo.product_sku = id.product_sku
                       AND tfo.business_unit = id.business_unit
                       AND tfo.store_country = id.country
                       AND tfo.datetime_start <= id.inventory_date
                       AND tfo.datetime_end > id.inventory_date
WHERE id.inventory_date >= $start_date
GROUP BY id.business_unit
       , id.region
       , id.country
       , id.product_sku
       , id.inventory_date
       , id.clearance_price
       , IFF(tfo.product_sku IS NOT NULL, '2 for 1', NULL);


CREATE OR REPLACE TRANSIENT TABLE gfb.dos_083_clearance_reporting AS
SELECT DISTINCT (CASE
                     WHEN COALESCE(s.region, i.region) = 'EU' THEN COALESCE(s.business_unit, i.business_unit) || ' EU'
                     ELSE COALESCE(s.business_unit, i.business_unit) || ' ' ||
                          COALESCE(s.country, i.country) END)                                                  AS store_name
              , prod_info.sub_brand
              , COALESCE(s."Placed Date", i.inventory_date)                                                    AS "Placed Date"
              , CASE
                    WHEN s.markdown_bucket IS NOT NULL THEN s.markdown_bucket
                    ELSE s."Is_Clearance" END                                                                  AS "Is_Clearance"
              , s.two_for_one_purchase
              , s.sale_price
              , COALESCE(s.two_for_one_flag, i.two_for_one_flag)                                               AS two_for_one_flag
              , i.clearance_price
              , prod_info.department
              , prod_info.subcategory
              , prod_info.product_sku
              , (CASE
                     WHEN DATEDIFF(MONTH, prod_info.latest_launch_date, DATE_TRUNC(MONTH, CURRENT_DATE())) < 0
                         THEN 'FUTURE SHOWROOM'
                     WHEN DATEDIFF(MONTH, prod_info.latest_launch_date, DATE_TRUNC(MONTH, CURRENT_DATE())) < 9
                         THEN 'CURRENT'
                     WHEN DATEDIFF(MONTH, prod_info.latest_launch_date, DATE_TRUNC(MONTH, CURRENT_DATE())) < 15
                         THEN 'TIER 1'
                     WHEN DATEDIFF(MONTH, prod_info.latest_launch_date, DATE_TRUNC(MONTH, CURRENT_DATE())) < 24
                         THEN 'TIER 2'
                     WHEN DATEDIFF(MONTH, prod_info.latest_launch_date, DATE_TRUNC(MONTH, CURRENT_DATE())) >= 24
                         THEN 'TIER 3'
    END)                                                                                                       AS tier
              , prod_info.style_name                                                                           AS style
              , prod_info.master_color
              , prod_info.latest_launch_date
              , prod_info.current_vip_retail
              , prod_info.shared
              , CURRENT_DATE()                                                                                 AS "Last Update"
              , s.promo_code_1
              , s.promo_code_2

              , s."Total Sale Units"
              , s.discount_total
              , s."Sales $"
              , s."Cost"
              , s."Product Gross Margin"
              , s.credit_redeemed_amount
              , s.two_for_one_qty_sold
              , s.two_for_one_discount
              , s.two_for_one_product_revenue
              , s.two_for_one_cogs
              , s.two_for_one_credit_redeemed
              , COUNT(s.product_sku)
                      OVER (PARTITION BY s.business_unit, s.region, s.country, s."Placed Date", s.product_sku) AS coef
              , (COALESCE(i.ty_eop_qty_onhand, 0) / IFF(coef = 0, 1, coef))                                    AS eop_inventory
FROM _sales s
         FULL JOIN _inventory i ON i.business_unit = s.business_unit
    AND i.region = s.region
    AND i.country = s.country
    AND i.product_sku = s.product_sku
    AND i.inventory_date = s."Placed Date"
         JOIN gfb.merch_dim_product prod_info
              ON prod_info.product_sku = COALESCE(s.product_sku, i.product_sku)
                  AND prod_info.business_unit = COALESCE(s.business_unit, i.business_unit)
                  AND prod_info.region = COALESCE(s.region, i.region)
                  AND prod_info.country = COALESCE(s.country, i.country);
