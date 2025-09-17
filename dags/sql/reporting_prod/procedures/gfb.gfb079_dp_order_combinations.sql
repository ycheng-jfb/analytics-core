SET start_date = DATE_TRUNC(MONTH, DATEADD(MONTH, -24, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _dream_pairs_sales AS
SELECT DISTINCT ol.business_unit,
                mdp.sub_brand,
                ol.region,
                ol.country,
                ol.order_id,
                ol.order_date
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
  AND mdp.sub_brand = 'DREAM PAIRS'
  AND ol.order_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _dream_pairs_kids_sales AS
SELECT DISTINCT ol.business_unit,
                mdp.sub_brand,
                ol.region,
                ol.country,
                ol.order_id,
                ol.order_date
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
  AND mdp.sub_brand = 'DREAM PAIRS KIDS'
  AND ol.order_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _bruno_marc_sales AS
SELECT DISTINCT ol.business_unit,
                mdp.sub_brand,
                ol.region,
                ol.country,
                ol.order_id,
                ol.order_date
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
  AND mdp.sub_brand = 'BRUNO MARC'
  AND ol.order_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _nortiv8_sales AS
SELECT DISTINCT ol.business_unit,
                mdp.sub_brand,
                ol.region,
                ol.country,
                ol.order_id,
                ol.order_date
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
  AND mdp.sub_brand = 'NORTIV8'
  AND ol.order_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _justfab_sales AS
SELECT DISTINCT ol.business_unit,
                mdp.sub_brand,
                ol.region,
                ol.country,
                ol.order_id,
                ol.order_date
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
  AND mdp.sub_brand IN ('JFB')
  AND ol.business_unit = 'JUSTFAB'
  AND ol.order_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _mens_sales AS
SELECT DISTINCT ol.business_unit,
                ol.region,
                ol.country,
                ol.order_id,
                ol.order_date
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
  AND mdp.gender = 'MENS'
  AND ol.business_unit = 'JUSTFAB'
  AND ol.order_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _womens_sales AS
SELECT DISTINCT ol.business_unit,
                ol.region,
                ol.country,
                ol.order_id,
                ol.order_date
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
  AND mdp.gender = 'WOMENS'
  AND ol.business_unit = 'JUSTFAB'
  AND ol.order_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _fabkids_sales AS
SELECT DISTINCT ol.business_unit,
                ol.region,
                ol.country,
                ol.order_id,
                ol.order_date
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
  AND mdp.gender IN ('BOYS', 'GIRLS')
  AND ol.business_unit = 'JUSTFAB'
  AND mdp.sub_brand = 'JFB'
  AND ol.order_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _dreampairs_gender_sales AS
SELECT DISTINCT ol.business_unit,
                ol.region,
                ol.country,
                ol.order_id,
                ol.order_date
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
  AND mdp.gender IN ('BOYS', 'GIRLS')
  AND mdp.sub_brand = 'DREAM PAIRS KIDS'
  AND ol.business_unit = 'JUSTFAB'
  AND ol.order_date >= $start_date;

CREATE OR REPLACE TEMPORARY TABLE _all_sales AS
SELECT ol.business_unit,
       ol.region,
       ol.country,
       ol.order_id,
       ol.order_date,
       SUM(total_qty_sold)                    AS total_qty_sold,
       SUM(total_product_revenue_with_tariff) AS total_product_revenue_with_tariff,
       SUM(CASE
               WHEN order_type = 'vip activating'
                   THEN total_qty_sold
               ELSE 0 END)                    AS activating_qty_sold,
       SUM(CASE
               WHEN order_type != 'vip activating'
                   THEN total_qty_sold
               ELSE 0 END)                    AS repeat_qty_sold,
       SUM(CASE
               WHEN order_type = 'vip activating'
                   THEN total_product_revenue_with_tariff
               ELSE 0 END)                    AS activating_product_revenue_with_tariff,
       SUM(CASE
               WHEN order_type != 'vip activating'
                   THEN total_product_revenue_with_tariff
               ELSE 0 END)                    AS repeat_product_revenue_with_tariff,
       SUM(tokens_applied)                    AS total_token_count,
       SUM(CASE
               WHEN order_type != 'vip activating'
                   THEN tokens_applied
               ELSE 0 END)                    AS repeat_token_count,
       SUM(CASE
               WHEN order_type = 'vip activating'
                   THEN tokens_applied
               ELSE 0 END)                    AS activating_token_count
FROM gfb.gfb_order_line_data_set_place_date ol
         JOIN gfb.merch_dim_product mdp
              ON ol.business_unit = mdp.business_unit
                  AND ol.region = mdp.region
                  AND ol.country = mdp.country
                  AND ol.product_sku = mdp.product_sku
WHERE ol.order_classification = 'product order'
  AND ol.business_unit = 'JUSTFAB'
  AND ol.order_date >= $start_date
GROUP BY ol.business_unit,
         mdp.sub_brand,
         ol.region,
         ol.country,
         ol.order_id,
         ol.order_date;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb079_dp_order_combinations AS
SELECT ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(CASE WHEN js.sub_brand IS NOT NULL THEN 'JFB' END,
                                                     CASE WHEN dps.sub_brand IS NOT NULL THEN 'Dream Pairs' END,
                                                     CASE WHEN dpks.sub_brand IS NOT NULL THEN 'Dream Pairs Kids' END,
                                                     CASE WHEN bms.sub_brand IS NOT NULL THEN 'Bruno Marc' END,
                                                     CASE WHEN ns.sub_brand IS NOT NULL THEN 'Nortiv8' END,
                                                     CASE
                                                         WHEN js.sub_brand IS NULL AND dps.sub_brand IS NULL AND
                                                              dpks.sub_brand IS NULL AND bms.sub_brand IS NULL AND
                                                              ns.sub_brand IS NULL THEN 'JFB' END)),
                       ' + ')                      AS order_combinations,
       ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(CASE WHEN ms.business_unit IS NOT NULL THEN 'MENS' END,
                                                     CASE WHEN ws.business_unit IS NOT NULL THEN 'WOMENS' END,
                                                     CASE
                                                         WHEN dpgs.business_unit IS NOT NULL
                                                             THEN 'Dream Pairs Kids' END,
                                                     CASE WHEN fs.business_unit IS NOT NULL THEN 'FABKIDS' END)),
                       ' + ')                      AS gender_combinations,
       sa.business_unit,
       sa.region,
       sa.country,
       sa.order_id                                 AS order_id,
       sa.order_date,
       SUM(total_qty_sold)                         AS total_qty_sold,
       SUM(total_product_revenue_with_tariff)      AS total_product_revenue_with_tariff,
       SUM(activating_qty_sold)                    AS activating_qty_sold,
       SUM(repeat_qty_sold)                        AS repeat_qty_sold,
       SUM(activating_product_revenue_with_tariff) AS activating_product_revenue_with_tariff,
       SUM(repeat_product_revenue_with_tariff)     AS repeat_product_revenue_with_tariff,
       SUM(total_token_count)                      AS total_token_count,
       SUM(activating_token_count)                 AS activating_token_count,
       SUM(repeat_token_count)                     AS repeat_token_count
FROM _all_sales sa
         LEFT JOIN _dream_pairs_sales dps
                   ON sa.order_id = dps.order_id
         LEFT JOIN _justfab_sales js
                   ON sa.order_id = js.order_id
         LEFT JOIN _dream_pairs_kids_sales dpks
                   ON sa.order_id = dpks.order_id
         LEFT JOIN _bruno_marc_sales bms
                   ON sa.order_id = bms.order_id
         LEFT JOIN _nortiv8_sales ns
                   ON sa.order_id = ns.order_id
         LEFT JOIN _mens_sales ms
                   ON sa.order_id = ms.order_id
         LEFT JOIN _womens_sales ws
                   ON sa.order_id = ws.order_id
         LEFT JOIN _dreampairs_gender_sales dpgs
                   ON sa.order_id = dpgs.order_id
         LEFT JOIN _fabkids_sales fs
                   ON sa.order_id = fs.order_id
GROUP BY order_combinations,
         gender_combinations,
         sa.business_unit,
         sa.region,
         sa.country,
         sa.order_id,
         sa.order_date;

