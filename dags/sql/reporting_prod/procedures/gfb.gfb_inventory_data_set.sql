SET today = CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _remove_gwp AS
SELECT DISTINCT UPPER(st.store_brand) AS business_unit
              , st.store_country
              , sku
              , product_type
FROM edw_prod.data_model_jfb.dim_product dp
         JOIN edw_prod.data_model_jfb.dim_store st
              ON st.store_id = dp.store_id
WHERE product_type LIKE '%GWP%';

CREATE OR REPLACE TEMPORARY TABLE _product_size AS
SELECT DISTINCT UPPER(st.store_brand)                                                                                                AS business_unit
              , UPPER(st.store_region)                                                                                               AS region
              , dp.sku
              , LOWER(FIRST_VALUE(dp.size)
                                  OVER (PARTITION BY st.store_brand, st.store_region, dp.sku ORDER BY dp.meta_update_datetime DESC)) AS dp_size
FROM edw_prod.data_model_jfb.dim_product dp
         JOIN gfb.vw_store st
              ON st.store_id = dp.store_id;


CREATE OR REPLACE TEMPORARY TABLE _product_sale_price_hist AS
SELECT DISTINCT st.store_brand,
                st.store_country,
                st.store_id,
                dp.product_sku,
                FIRST_VALUE(a.sale_price)
                            OVER (PARTITION BY st.store_brand,st.store_country,dp.product_sku,a.effective_start_datetime,a.effective_end_datetime ORDER BY a.effective_start_datetime) AS sale_price,
                a.effective_start_datetime,
                a.effective_end_datetime
FROM (SELECT DISTINCT product_id
                    , store_id
                    , sale_price
                    , effective_start_datetime::DATE AS effective_start_datetime
                    , effective_end_datetime::DATE   AS effective_end_datetime
      FROM edw_prod.data_model_jfb.dim_product_price_history
      WHERE sale_price > 0
        AND effective_start_datetime::DATE != '1900-01-01') a
         JOIN
     (SELECT DISTINCT business_unit,
                      country,
                      master_product_id,
                      product_sku
      FROM gfb.merch_dim_product mdp) dp
     ON dp.master_product_id = a.product_id
         JOIN gfb.vw_store st
              ON st.store_id = a.store_id;


CREATE OR REPLACE TEMPORARY TABLE _main_brand AS
SELECT DISTINCT sku AS sku
              , CASE
                    WHEN FIRST_VALUE(UPPER(brand)) OVER (PARTITION BY sku ORDER BY show_room DESC) != 'JUSTFAB' AND
                         division_id = 'SHOEDAZZLE' THEN 'SHOEDAZZLE'
                    WHEN FIRST_VALUE(UPPER(brand)) OVER (PARTITION BY sku ORDER BY show_room DESC) IS NULL AND
                         division_id = 'SHOEDAZZLE' THEN 'SHOEDAZZLE'
                    WHEN FIRST_VALUE(UPPER(brand)) OVER (PARTITION BY sku ORDER BY show_room DESC) != 'SHOEDAZZLE' AND
                         division_id = 'JUSTFAB' THEN 'JUSTFAB'
                    WHEN FIRST_VALUE(UPPER(brand)) OVER (PARTITION BY sku ORDER BY show_room DESC) IS NULL AND
                         division_id = 'JUSTFAB' THEN 'JUSTFAB'
                    WHEN division_id = 'FABKIDS' THEN 'FABKIDS'
                    ELSE FIRST_VALUE(UPPER(brand)) OVER (PARTITION BY sku ORDER BY show_room DESC)
    END             AS brand
FROM gsc.po_detail_dataset
WHERE UPPER(division_id) IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS');


CREATE OR REPLACE TRANSIENT TABLE gfb.gfb_inventory_data_set AS
SELECT UPPER(CASE
                 WHEN (CASE
                           WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                           WHEN id.warehouse_id = 154 THEN 'NA'
                           WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                           WHEN id.warehouse_id = 421 THEN 'NA'
                           WHEN id.warehouse_id = 221 THEN 'EU'
                           WHEN id.warehouse_id = 366 THEN 'EU'
                           WHEN id.warehouse_id = 466 THEN 'NA'
                           WHEN id.warehouse_id = 601 THEN 'NA'
                           WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                           WHEN id.warehouse_id = 245 THEN 'NA'
                           WHEN id.warehouse_id = 405 THEN 'EU'
                     END) = 'EU' THEN 'JUSTFAB' -- this is for FK product in JFEU
                 WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                 ELSE id.brand END)                                         AS business_unit
     , CASE
           WHEN id.warehouse_id IN ('221', '366') THEN 'JUSTFAB'
           WHEN mb.brand IS NOT NULL THEN mb.brand
           ELSE UPPER(CASE
                          WHEN (CASE
                                    WHEN id.warehouse_id = 107 THEN 'NA'
                                    WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                    WHEN id.warehouse_id = 154 THEN 'NA'
                                    WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                    WHEN id.warehouse_id = 421 THEN 'NA'
                                    WHEN id.warehouse_id = 221 THEN 'EU'
                                    WHEN id.warehouse_id = 366 THEN 'EU'
                                    WHEN id.warehouse_id = 466 THEN 'NA'
                                    WHEN id.warehouse_id = 601 THEN 'NA'
                                    WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                    WHEN id.warehouse_id = 245 THEN 'NA'
                                    WHEN id.warehouse_id = 405 THEN 'EU'
                              END) = 'EU' THEN 'JUSTFAB' -- this is for FK product in JFEU
                          WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                          ELSE id.brand END)
    END                                                                     AS main_brand
     , (CASE
            WHEN id.warehouse_id IN (109, 215) THEN 'NA'
            WHEN id.warehouse_id = 154 THEN 'NA'
            WHEN id.warehouse_id IN (231, 242) THEN 'NA'
            WHEN id.warehouse_id = 421 THEN 'NA'
            WHEN id.warehouse_id = 221 THEN 'EU'
            WHEN id.warehouse_id = 366 THEN 'EU'
            WHEN id.warehouse_id = 466 THEN 'NA'
            WHEN id.warehouse_id = 601 THEN 'NA'
            WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
            WHEN id.warehouse_id = 245 THEN 'NA'
            WHEN id.warehouse_id = 405 THEN 'EU'
    END)                                                                    AS region
     , (CASE
            WHEN id.warehouse_id = 107 THEN 'US'
            WHEN id.warehouse_id IN (109, 215) THEN 'CA'
            WHEN id.warehouse_id = 154 THEN 'US'
            WHEN id.warehouse_id IN (231, 242) THEN 'US'
            WHEN id.warehouse_id = 421 THEN 'US'
            WHEN id.warehouse_id = 221 THEN 'FR'
            WHEN id.warehouse_id = 366 THEN 'UK'
            WHEN id.warehouse_id = 466 THEN 'US'
            WHEN id.warehouse_id = 601 THEN 'US'
            WHEN id.warehouse_id IN (284, 44, 346) THEN 'US'
            WHEN id.warehouse_id = 245 THEN 'US'
            WHEN id.warehouse_id = 405 THEN 'FR'
    END)                                                                    AS country
     , (CASE
            WHEN id.warehouse_id = 107 THEN 'KENTUCKY'
            WHEN id.warehouse_id IN (109, 215) THEN 'CANADA'
            WHEN id.warehouse_id = 154 THEN 'KENTUCKY'
            WHEN id.warehouse_id IN (231, 242) THEN 'PERRIS'
            WHEN id.warehouse_id = 421 THEN 'KENTUCKY'
            WHEN id.warehouse_id = 221 THEN 'NETHERLANDS'
            WHEN id.warehouse_id = 366 THEN 'UK'
            WHEN id.warehouse_id = 466 THEN 'TIJUANA'
            WHEN id.warehouse_id = 601 THEN 'MIRACLE MILES'
            WHEN id.warehouse_id IN (284, 44, 346) THEN 'NEW JERSEY'
            WHEN id.warehouse_id = 245 THEN 'WINIT'
            WHEN id.warehouse_id = 405 THEN 'NETHERLANDS'
    END)                                                                    AS warehouse
     , id.warehouse_id
     , local_date                                                           AS inventory_date
     , id.product_sku
     , id.sku
     , ps.dp_size
     , COALESCE(mcsl.clearance_group, CAST(psph.sale_price AS VARCHAR(20))) AS clearance_price

     , SUM(id.onhand_quantity)                                              AS qty_onhand
     , SUM(id.available_to_sell_quantity)                                   AS qty_available_to_sell
     , SUM(id.open_to_buy_quantity)                                         AS qty_open_to_buy
     , SUM(id.ghost_quantity)                                               AS qty_ghost
     , SUM(id.manual_stock_reserve_quantity)                                AS qty_manual_stock_reserve
     , SUM(id.reserve_quantity)                                             AS qty_reserve
     , SUM(id.pick_staging_quantity)                                        AS qty_pick_staging
     , SUM(id.replen_quantity)                                              AS qty_replen
     , SUM(id.receipt_inspection_quantity)                                  AS qty_ri
     , SUM(id.special_pick_quantity)                                        AS qty_special_pick_reserve
     , SUM(id.staging_quantity)                                             AS qty_staging
     , SUM(id.intransit_quantity)                                           AS qty_intransit
FROM edw_prod.data_model_jfb.fact_inventory_history id
         LEFT JOIN _main_brand mb
                   ON id.sku = mb.sku
         LEFT JOIN _product_size ps
                   ON ps.business_unit = UPPER(CASE
                                                   WHEN (CASE
                                                             WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                                             WHEN id.warehouse_id = 154 THEN 'NA'
                                                             WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                                             WHEN id.warehouse_id = 421 THEN 'NA'
                                                             WHEN id.warehouse_id = 221 THEN 'EU'
                                                             WHEN id.warehouse_id = 366 THEN 'EU'
                                                             WHEN id.warehouse_id = 466 THEN 'NA'
                                                             WHEN id.warehouse_id = 601 THEN 'NA'
                                                             WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                                             WHEN id.warehouse_id = 245 THEN 'NA'
                                                             WHEN id.warehouse_id = 405 THEN 'EU'
                                                       END) = 'EU' THEN 'JUSTFAB' -- this is for FK product in JFEU
                                                   WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                                                   ELSE id.brand END)
                       AND ps.region = (CASE
                                            WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                            WHEN id.warehouse_id = 154 THEN 'NA'
                                            WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                            WHEN id.warehouse_id = 421 THEN 'NA'
                                            WHEN id.warehouse_id = 221 THEN 'EU'
                                            WHEN id.warehouse_id = 366 THEN 'EU'
                                            WHEN id.warehouse_id = 466 THEN 'NA'
                                            WHEN id.warehouse_id = 601 THEN 'NA'
                                            WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                            WHEN id.warehouse_id = 245 THEN 'NA'
                                            WHEN id.warehouse_id = 405 THEN 'EU'
                           END)
                       AND ps.sku = id.sku
         LEFT JOIN lake_view.sharepoint.gfb_merch_clearance_sku_list mcsl
                   ON LOWER(mcsl.business_unit) = LOWER(CASE
                                                            WHEN (CASE
                                                                      WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                                                      WHEN id.warehouse_id = 154 THEN 'NA'
                                                                      WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                                                      WHEN id.warehouse_id = 421 THEN 'NA'
                                                                      WHEN id.warehouse_id = 221 THEN 'EU'
                                                                      WHEN id.warehouse_id = 366 THEN 'EU'
                                                                      WHEN id.warehouse_id = 466 THEN 'NA'
                                                                      WHEN id.warehouse_id = 601 THEN 'NA'
                                                                      WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                                                      WHEN id.warehouse_id = 245 THEN 'NA'
                                                                      WHEN id.warehouse_id = 405 THEN 'EU'
                                                                END) = 'EU'
                                                                THEN 'JUSTFAB' -- this is for FK product in JFEU
                                                            WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                                                            ELSE id.brand END)
                       AND LOWER(mcsl.region) = LOWER(CASE
                                                          WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                                          WHEN id.warehouse_id = 154 THEN 'NA'
                                                          WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                                          WHEN id.warehouse_id = 421 THEN 'NA'
                                                          WHEN id.warehouse_id = 221 THEN 'EU'
                                                          WHEN id.warehouse_id = 366 THEN 'EU'
                                                          WHEN id.warehouse_id = 466 THEN 'NA'
                                                          WHEN id.warehouse_id = 601 THEN 'NA'
                                                          WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                                          WHEN id.warehouse_id = 245 THEN 'NA'
                                                          WHEN id.warehouse_id = 405 THEN 'EU'
                           END)
                       AND LOWER(mcsl.sku) = LOWER(id.product_sku)
                       AND local_date BETWEEN mcsl.start_date AND COALESCE(mcsl.end_date, CURRENT_DATE())
         LEFT JOIN _product_sale_price_hist psph
                   ON LOWER(psph.store_brand) = LOWER(CASE
                                                          WHEN (CASE
                                                                    WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                                                    WHEN id.warehouse_id = 154 THEN 'NA'
                                                                    WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                                                    WHEN id.warehouse_id = 421 THEN 'NA'
                                                                    WHEN id.warehouse_id = 221 THEN 'EU'
                                                                    WHEN id.warehouse_id = 366 THEN 'EU'
                                                                    WHEN id.warehouse_id = 466 THEN 'NA'
                                                                    WHEN id.warehouse_id = 601 THEN 'NA'
                                                                    WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                                                    WHEN id.warehouse_id = 245 THEN 'NA'
                                                                    WHEN id.warehouse_id = 405 THEN 'EU'
                                                              END) = 'EU'
                                                              THEN 'JUSTFAB' -- this is for FK product in JFEU
                                                          WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                                                          ELSE id.brand END)
                       AND LOWER(psph.store_country) = LOWER(CASE
                                                                 WHEN id.warehouse_id = 107 THEN 'US'
                                                                 WHEN id.warehouse_id IN (109, 215) THEN 'CA'
                                                                 WHEN id.warehouse_id = 154 THEN 'US'
                                                                 WHEN id.warehouse_id IN (231, 242) THEN 'US'
                                                                 WHEN id.warehouse_id = 421 THEN 'US'
                                                                 WHEN id.warehouse_id = 221 THEN 'FR'
                                                                 WHEN id.warehouse_id = 366 THEN 'UK'
                                                                 WHEN id.warehouse_id = 466 THEN 'US'
                                                                 WHEN id.warehouse_id = 601 THEN 'US'
                                                                 WHEN id.warehouse_id IN (284, 44, 346) THEN 'US'
                                                                 WHEN id.warehouse_id = 245 THEN 'US'
                                                                 WHEN id.warehouse_id = 405 THEN 'FR'
                           END)
                       AND LOWER(psph.product_sku) = LOWER(id.product_sku)
                       AND local_date >= psph.effective_start_datetime AND local_date < psph.effective_end_datetime
         LEFT JOIN _remove_gwp rg
                   ON UPPER(rg.business_unit) = UPPER(CASE
                                                          WHEN (CASE
                                                                    WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                                                    WHEN id.warehouse_id = 154 THEN 'NA'
                                                                    WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                                                    WHEN id.warehouse_id = 421 THEN 'NA'
                                                                    WHEN id.warehouse_id = 221 THEN 'EU'
                                                                    WHEN id.warehouse_id = 366 THEN 'EU'
                                                                    WHEN id.warehouse_id = 466 THEN 'NA'
                                                                    WHEN id.warehouse_id = 601 THEN 'NA'
                                                                    WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                                                    WHEN id.warehouse_id = 245 THEN 'NA'
                                                                    WHEN id.warehouse_id = 405 THEN 'EU'
                                                              END) = 'EU'
                                                              THEN 'JUSTFAB' -- this is for FK product in JFEU
                                                          WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                                                          ELSE id.brand END)
                       AND rg.store_country = (CASE
                                                   WHEN id.warehouse_id = 107 THEN 'US'
                                                   WHEN id.warehouse_id IN (109, 215) THEN 'CA'
                                                   WHEN id.warehouse_id = 154 THEN 'US'
                                                   WHEN id.warehouse_id IN (231, 242) THEN 'US'
                                                   WHEN id.warehouse_id = 421 THEN 'US'
                                                   WHEN id.warehouse_id = 221 THEN 'FR'
                                                   WHEN id.warehouse_id = 366 THEN 'UK'
                                                   WHEN id.warehouse_id = 466 THEN 'US'
                                                   WHEN id.warehouse_id = 601 THEN 'US'
                                                   WHEN id.warehouse_id IN (284, 44, 346) THEN 'US'
                                                   WHEN id.warehouse_id = 245 THEN 'US'
                                                   WHEN id.warehouse_id = 405 THEN 'FR'
                           END)
                       AND rg.sku = id.sku
WHERE id.brand IN ('SHOE DAZZLE', 'JUSTFAB', 'FABKIDS','SHOEDAZZLE')
  AND (CASE
           WHEN id.warehouse_id = 107 THEN 'NA'
           WHEN id.warehouse_id IN (109, 215) THEN 'NA'
           WHEN id.warehouse_id = 154 THEN 'NA'
           WHEN id.warehouse_id IN (231, 242) THEN 'NA'
           WHEN id.warehouse_id = 421 THEN 'NA'
           WHEN id.warehouse_id = 221 THEN 'EU'
           WHEN id.warehouse_id = 366 THEN 'EU'
           WHEN id.warehouse_id = 466 THEN 'NA'
           WHEN id.warehouse_id = 601 THEN 'NA'
           WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
           WHEN id.warehouse_id = 245 THEN 'NA'
           WHEN id.warehouse_id = 405 THEN 'EU'
    END) IS NOT NULL
  AND rg.product_type IS NULL
GROUP BY UPPER(CASE
                   WHEN (CASE
                             WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                             WHEN id.warehouse_id = 154 THEN 'NA'
                             WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                             WHEN id.warehouse_id = 421 THEN 'NA'
                             WHEN id.warehouse_id = 221 THEN 'EU'
                             WHEN id.warehouse_id = 366 THEN 'EU'
                             WHEN id.warehouse_id = 466 THEN 'NA'
                             WHEN id.warehouse_id = 601 THEN 'NA'
                             WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                             WHEN id.warehouse_id = 245 THEN 'NA'
                             WHEN id.warehouse_id = 405 THEN 'EU'
                       END) = 'EU' THEN 'JUSTFAB' -- this is for FK product in JFEU
                   WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                   ELSE id.brand END)
       , CASE
             WHEN id.warehouse_id IN ('221', '366') THEN 'JUSTFAB'
             WHEN mb.brand IS NOT NULL THEN mb.brand
             ELSE UPPER(CASE
                            WHEN (CASE
                                      WHEN id.warehouse_id = 107 THEN 'NA'
                                      WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                      WHEN id.warehouse_id = 154 THEN 'NA'
                                      WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                      WHEN id.warehouse_id = 421 THEN 'NA'
                                      WHEN id.warehouse_id = 221 THEN 'EU'
                                      WHEN id.warehouse_id = 366 THEN 'EU'
                                      WHEN id.warehouse_id = 466 THEN 'NA'
                                      WHEN id.warehouse_id = 601 THEN 'NA'
                                      WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                      WHEN id.warehouse_id = 245 THEN 'NA'
                                      WHEN id.warehouse_id = 405 THEN 'EU'
                                END) = 'EU' THEN 'JUSTFAB' -- this is for FK product in JFEU
                            WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                            ELSE id.brand END)
    END
       , (CASE
              WHEN id.warehouse_id = 107 THEN 'NA'
              WHEN id.warehouse_id IN (109, 215) THEN 'NA'
              WHEN id.warehouse_id = 154 THEN 'NA'
              WHEN id.warehouse_id IN (231, 242) THEN 'NA'
              WHEN id.warehouse_id = 421 THEN 'NA'
              WHEN id.warehouse_id = 221 THEN 'EU'
              WHEN id.warehouse_id = 366 THEN 'EU'
              WHEN id.warehouse_id = 466 THEN 'NA'
              WHEN id.warehouse_id = 601 THEN 'NA'
              WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
              WHEN id.warehouse_id = 245 THEN 'NA'
              WHEN id.warehouse_id = 405 THEN 'EU'
    END)
       , (CASE
              WHEN id.warehouse_id = 107 THEN 'US'
              WHEN id.warehouse_id IN (109, 215) THEN 'CA'
              WHEN id.warehouse_id = 154 THEN 'US'
              WHEN id.warehouse_id IN (231, 242) THEN 'US'
              WHEN id.warehouse_id = 421 THEN 'US'
              WHEN id.warehouse_id = 221 THEN 'FR'
              WHEN id.warehouse_id = 366 THEN 'UK'
              WHEN id.warehouse_id = 466 THEN 'US'
              WHEN id.warehouse_id = 601 THEN 'US'
              WHEN id.warehouse_id IN (284, 44, 346) THEN 'US'
              WHEN id.warehouse_id = 245 THEN 'US'
              WHEN id.warehouse_id = 405 THEN 'FR'
    END)
       , (CASE
              WHEN id.warehouse_id = 107 THEN 'KENTUCKY'
              WHEN id.warehouse_id IN (109, 215) THEN 'CANADA'
              WHEN id.warehouse_id = 154 THEN 'KENTUCKY'
              WHEN id.warehouse_id IN (231, 242) THEN 'PERRIS'
              WHEN id.warehouse_id = 421 THEN 'KENTUCKY'
              WHEN id.warehouse_id = 221 THEN 'NETHERLANDS'
              WHEN id.warehouse_id = 366 THEN 'UK'
              WHEN id.warehouse_id = 466 THEN 'TIJUANA'
              WHEN id.warehouse_id = 601 THEN 'MIRACLE MILES'
              WHEN id.warehouse_id IN (284, 44, 346) THEN 'NEW JERSEY'
              WHEN id.warehouse_id = 245 THEN 'WINIT'
              WHEN id.warehouse_id = 405 THEN 'NETHERLANDS'
    END)
       , id.warehouse_id
       , local_date
       , id.product_sku
       , id.sku
       , ps.dp_size
       , COALESCE(mcsl.clearance_group, CAST(psph.sale_price AS VARCHAR(20)));

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb_inventory_data_set_current AS
SELECT UPPER(CASE
                 WHEN (CASE
                           WHEN id.warehouse_id = 107 THEN 'NA'
                           WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                           WHEN id.warehouse_id = 154 THEN 'NA'
                           WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                           WHEN id.warehouse_id = 421 THEN 'NA'
                           WHEN id.warehouse_id = 221 THEN 'EU'
                           WHEN id.warehouse_id = 366 THEN 'EU'
                           WHEN id.warehouse_id = 466 THEN 'NA'
                           WHEN id.warehouse_id = 601 THEN 'NA'
                           WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                           WHEN id.warehouse_id = 245 THEN 'NA'
                           WHEN id.warehouse_id = 405 THEN 'EU'
                     END) = 'EU' THEN 'JUSTFAB' -- this is for FK product in JFEU
                 WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                 ELSE id.brand END)                                         AS business_unit
     , CASE
           WHEN id.warehouse_id IN ('221', '366') THEN 'JUSTFAB'
           WHEN mb.brand IS NOT NULL THEN mb.brand
           ELSE UPPER(CASE
                          WHEN (CASE
                                    WHEN id.warehouse_id = 107 THEN 'NA'
                                    WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                    WHEN id.warehouse_id = 154 THEN 'NA'
                                    WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                    WHEN id.warehouse_id = 421 THEN 'NA'
                                    WHEN id.warehouse_id = 221 THEN 'EU'
                                    WHEN id.warehouse_id = 366 THEN 'EU'
                                    WHEN id.warehouse_id = 466 THEN 'NA'
                                    WHEN id.warehouse_id = 601 THEN 'NA'
                                    WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                    WHEN id.warehouse_id = 245 THEN 'NA'
                                    WHEN id.warehouse_id = 405 THEN 'EU'
                              END) = 'EU' THEN 'JUSTFAB' -- this is for FK product in JFEU
                          WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                          ELSE id.brand END)
    END                                                                     AS main_brand
     , (CASE
            WHEN id.warehouse_id = 107 THEN 'NA'
            WHEN id.warehouse_id IN (109, 215) THEN 'NA'
            WHEN id.warehouse_id = 154 THEN 'NA'
            WHEN id.warehouse_id IN (231, 242) THEN 'NA'
            WHEN id.warehouse_id = 421 THEN 'NA'
            WHEN id.warehouse_id = 221 THEN 'EU'
            WHEN id.warehouse_id = 366 THEN 'EU'
            WHEN id.warehouse_id = 466 THEN 'NA'
            WHEN id.warehouse_id = 601 THEN 'NA'
            WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
            WHEN id.warehouse_id = 245 THEN 'NA'
            WHEN id.warehouse_id = 405 THEN 'EU'
    END)                                                                    AS region
     , (CASE
            WHEN id.warehouse_id = 107 THEN 'US'
            WHEN id.warehouse_id IN (109, 215) THEN 'CA'
            WHEN id.warehouse_id = 154 THEN 'US'
            WHEN id.warehouse_id IN (231, 242) THEN 'US'
            WHEN id.warehouse_id = 421 THEN 'US'
            WHEN id.warehouse_id = 221 THEN 'FR'
            WHEN id.warehouse_id = 366 THEN 'UK'
            WHEN id.warehouse_id = 466 THEN 'US'
            WHEN id.warehouse_id = 601 THEN 'US'
            WHEN id.warehouse_id IN (284, 44, 346) THEN 'US'
            WHEN id.warehouse_id = 245 THEN 'US'
            WHEN id.warehouse_id = 405 THEN 'FR'
    END)                                                                    AS country
     , (CASE
            WHEN id.warehouse_id = 107 THEN 'KENTUCKY'
            WHEN id.warehouse_id IN (109, 215) THEN 'CANADA'
            WHEN id.warehouse_id = 154 THEN 'KENTUCKY'
            WHEN id.warehouse_id IN (231, 242) THEN 'PERRIS'
            WHEN id.warehouse_id = 421 THEN 'KENTUCKY'
            WHEN id.warehouse_id = 221 THEN 'NETHERLANDS'
            WHEN id.warehouse_id = 366 THEN 'UK'
            WHEN id.warehouse_id = 466 THEN 'TIJUANA'
            WHEN id.warehouse_id = 601 THEN 'MIRACLE MILES'
            WHEN id.warehouse_id IN (284, 44, 346) THEN 'NEW JERSEY'
            WHEN id.warehouse_id = 245 THEN 'WINIT'
            WHEN id.warehouse_id = 405 THEN 'NETHERLANDS'
    END)                                                                    AS warehouse
     , id.warehouse_id
     , id.product_sku
     , id.sku
     , ps.dp_size
     , COALESCE(mcsl.clearance_group, CAST(psph.sale_price AS VARCHAR(20))) AS clearance_price

     , SUM(id.onhand_quantity)                                              AS qty_onhand
     , SUM(id.available_to_sell_quantity)                                   AS qty_available_to_sell
     , SUM(id.open_to_buy_quantity)                                         AS qty_open_to_buy
     , SUM(id.ghost_quantity)                                               AS qty_ghost
     , SUM(id.manual_stock_reserve_quantity)                                AS qty_manual_stock_reserve
     , SUM(id.reserve_quantity)                                             AS qty_reserve
     , SUM(id.pick_staging_quantity)                                        AS qty_pick_staging
     , SUM(id.replen_quantity)                                              AS qty_replen
     , SUM(id.receipt_inspection_quantity)                                  AS qty_ri
     , SUM(id.special_pick_quantity)                                        AS qty_special_pick_reserve
     , SUM(id.staging_quantity)                                             AS qty_staging
     , SUM(id.intransit_quantity)                                           AS qty_intransit
FROM edw_prod.data_model_jfb.fact_inventory id
         LEFT JOIN _main_brand mb
                   ON mb.sku = id.sku
         LEFT JOIN _product_size ps
                   ON ps.business_unit = UPPER(CASE
                                                   WHEN (CASE
                                                             WHEN id.warehouse_id = 107 THEN 'NA'
                                                             WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                                             WHEN id.warehouse_id = 154 THEN 'NA'
                                                             WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                                             WHEN id.warehouse_id = 421 THEN 'NA'
                                                             WHEN id.warehouse_id = 221 THEN 'EU'
                                                             WHEN id.warehouse_id = 366 THEN 'EU'
                                                             WHEN id.warehouse_id = 466 THEN 'NA'
                                                             WHEN id.warehouse_id = 601 THEN 'NA'
                                                             WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                                             WHEN id.warehouse_id = 245 THEN 'NA'
                                                             WHEN id.warehouse_id = 405 THEN 'EU'
                                                       END) = 'EU' THEN 'JUSTFAB' -- this is for FK product in JFEU
                                                   WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                                                   ELSE id.brand END)
                       AND ps.region = (CASE
                                            WHEN id.warehouse_id = 107 THEN 'NA'
                                            WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                            WHEN id.warehouse_id = 154 THEN 'NA'
                                            WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                            WHEN id.warehouse_id = 421 THEN 'NA'
                                            WHEN id.warehouse_id = 221 THEN 'EU'
                                            WHEN id.warehouse_id = 366 THEN 'EU'
                                            WHEN id.warehouse_id = 466 THEN 'NA'
                                            WHEN id.warehouse_id = 601 THEN 'NA'
                                            WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                            WHEN id.warehouse_id = 245 THEN 'NA'
                                            WHEN id.warehouse_id = 405 THEN 'EU'
                           END)
                       AND ps.sku = id.sku
         LEFT JOIN lake_view.sharepoint.gfb_merch_clearance_sku_list mcsl
                   ON LOWER(mcsl.business_unit) = LOWER(CASE
                                                            WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                                                            ELSE id.brand END)
                       AND LOWER(mcsl.region) = LOWER(CASE
                                                          WHEN id.warehouse_id = 107 THEN 'NA'
                                                          WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                                          WHEN id.warehouse_id = 154 THEN 'NA'
                                                          WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                                          WHEN id.warehouse_id = 421 THEN 'NA'
                                                          WHEN id.warehouse_id = 221 THEN 'EU'
                                                          WHEN id.warehouse_id = 366 THEN 'EU'
                                                          WHEN id.warehouse_id = 466 THEN 'NA'
                                                          WHEN id.warehouse_id = 601 THEN 'NA'
                                                          WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                                          WHEN id.warehouse_id = 245 THEN 'NA'
                                                          WHEN id.warehouse_id = 405 THEN 'EU'
                           END)
                       AND LOWER(mcsl.sku) = LOWER(id.product_sku)
                       AND
                      ($today BETWEEN mcsl.start_date AND COALESCE(mcsl.end_date, CURRENT_DATE()))
         LEFT JOIN _product_sale_price_hist psph
                   ON LOWER(psph.store_brand) = LOWER(CASE
                                                          WHEN (CASE
                                                                    WHEN id.warehouse_id = 107 THEN 'NA'
                                                                    WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                                                    WHEN id.warehouse_id = 154 THEN 'NA'
                                                                    WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                                                    WHEN id.warehouse_id = 421 THEN 'NA'
                                                                    WHEN id.warehouse_id = 221 THEN 'EU'
                                                                    WHEN id.warehouse_id = 366 THEN 'EU'
                                                                    WHEN id.warehouse_id = 466 THEN 'NA'
                                                                    WHEN id.warehouse_id = 601 THEN 'NA'
                                                                    WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                                                    WHEN id.warehouse_id = 245 THEN 'NA'
                                                                    WHEN id.warehouse_id = 405 THEN 'EU'
                                                              END) = 'EU'
                                                              THEN 'JUSTFAB' -- this is for FK product in JFEU
                                                          WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                                                          ELSE id.brand END)
                       AND LOWER(psph.store_country) = LOWER(CASE
                                                                 WHEN id.warehouse_id = 107 THEN 'US'
                                                                 WHEN id.warehouse_id IN (109, 215) THEN 'CA'
                                                                 WHEN id.warehouse_id = 154 THEN 'US'
                                                                 WHEN id.warehouse_id IN (231, 242) THEN 'US'
                                                                 WHEN id.warehouse_id = 421 THEN 'US'
                                                                 WHEN id.warehouse_id = 221 THEN 'FR'
                                                                 WHEN id.warehouse_id = 366 THEN 'UK'
                                                                 WHEN id.warehouse_id = 466 THEN 'US'
                                                                 WHEN id.warehouse_id = 601 THEN 'US'
                                                                 WHEN id.warehouse_id IN (284, 44, 346) THEN 'US'
                                                                 WHEN id.warehouse_id = 245 THEN 'US'
                                                                 WHEN id.warehouse_id = 405 THEN 'FR'
                           END)
                       AND LOWER(psph.product_sku) = LOWER(id.product_sku)
                       AND $today >= psph.effective_start_datetime AND $today < psph.effective_end_datetime
         LEFT JOIN _remove_gwp rg
                   ON UPPER(rg.business_unit) = UPPER(CASE
                                                          WHEN (CASE
                                                                    WHEN id.warehouse_id = 107 THEN 'NA'
                                                                    WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                                                    WHEN id.warehouse_id = 154 THEN 'NA'
                                                                    WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                                                    WHEN id.warehouse_id = 421 THEN 'NA'
                                                                    WHEN id.warehouse_id = 221 THEN 'EU'
                                                                    WHEN id.warehouse_id = 366 THEN 'EU'
                                                                    WHEN id.warehouse_id = 466 THEN 'NA'
                                                                    WHEN id.warehouse_id = 601 THEN 'NA'
                                                                    WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                                                    WHEN id.warehouse_id = 245 THEN 'NA'
                                                                    WHEN id.warehouse_id = 405 THEN 'EU'
                                                              END) = 'EU'
                                                              THEN 'JUSTFAB' -- this is for FK product in JFEU
                                                          WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                                                          ELSE id.brand END)
                       AND rg.store_country = (CASE
                                                   WHEN id.warehouse_id = 107 THEN 'US'
                                                   WHEN id.warehouse_id IN (109, 215) THEN 'CA'
                                                   WHEN id.warehouse_id = 154 THEN 'US'
                                                   WHEN id.warehouse_id IN (231, 242) THEN 'US'
                                                   WHEN id.warehouse_id = 421 THEN 'US'
                                                   WHEN id.warehouse_id = 221 THEN 'FR'
                                                   WHEN id.warehouse_id = 366 THEN 'UK'
                                                   WHEN id.warehouse_id = 466 THEN 'US'
                                                   WHEN id.warehouse_id = 601 THEN 'US'
                                                   WHEN id.warehouse_id IN (284, 44, 346) THEN 'US'
                                                   WHEN id.warehouse_id = 245 THEN 'US'
                                                   WHEN id.warehouse_id = 405 THEN 'FR'
                           END)
                       AND rg.sku = id.sku
WHERE id.brand IN ('SHOE DAZZLE', 'JUSTFAB', 'FABKIDS','SHOEDAZZLE')
  AND (CASE
           WHEN id.warehouse_id = 107 THEN 'NA'
           WHEN id.warehouse_id IN (109, 215) THEN 'NA'
           WHEN id.warehouse_id = 154 THEN 'NA'
           WHEN id.warehouse_id IN (231, 242) THEN 'NA'
           WHEN id.warehouse_id = 421 THEN 'NA'
           WHEN id.warehouse_id = 221 THEN 'EU'
           WHEN id.warehouse_id = 366 THEN 'EU'
           WHEN id.warehouse_id = 466 THEN 'NA'
           WHEN id.warehouse_id = 601 THEN 'NA'
           WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
           WHEN id.warehouse_id = 245 THEN 'NA'
           WHEN id.warehouse_id = 405 THEN 'EU'
    END) IS NOT NULL
  AND rg.product_type IS NULL
GROUP BY UPPER(CASE
                   WHEN (CASE
                             WHEN id.warehouse_id = 107 THEN 'NA'
                             WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                             WHEN id.warehouse_id = 154 THEN 'NA'
                             WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                             WHEN id.warehouse_id = 421 THEN 'NA'
                             WHEN id.warehouse_id = 221 THEN 'EU'
                             WHEN id.warehouse_id = 366 THEN 'EU'
                             WHEN id.warehouse_id = 466 THEN 'NA'
                             WHEN id.warehouse_id = 601 THEN 'NA'
                             WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                             WHEN id.warehouse_id = 245 THEN 'NA'
                             WHEN id.warehouse_id = 405 THEN 'EU'
                       END) = 'EU' THEN 'JUSTFAB'
                   WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                   ELSE id.brand END)
       , CASE
             WHEN id.warehouse_id IN ('221', '366') THEN 'JUSTFAB'
             WHEN mb.brand IS NOT NULL THEN mb.brand
             ELSE UPPER(CASE
                            WHEN (CASE
                                      WHEN id.warehouse_id = 107 THEN 'NA'
                                      WHEN id.warehouse_id IN (109, 215) THEN 'NA'
                                      WHEN id.warehouse_id = 154 THEN 'NA'
                                      WHEN id.warehouse_id IN (231, 242) THEN 'NA'
                                      WHEN id.warehouse_id = 421 THEN 'NA'
                                      WHEN id.warehouse_id = 221 THEN 'EU'
                                      WHEN id.warehouse_id = 366 THEN 'EU'
                                      WHEN id.warehouse_id = 466 THEN 'NA'
                                      WHEN id.warehouse_id = 601 THEN 'NA'
                                      WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
                                      WHEN id.warehouse_id = 245 THEN 'NA'
                                      WHEN id.warehouse_id = 405 THEN 'EU'
                                END) = 'EU' THEN 'JUSTFAB' -- this is for FK product in JFEU
                            WHEN id.brand IN ('SHOE DAZZLE','SHOEDAZZLE') THEN 'SHOEDAZZLE'
                            ELSE id.brand END)
    END
       , (CASE
              WHEN id.warehouse_id = 107 THEN 'NA'
              WHEN id.warehouse_id IN (109, 215) THEN 'NA'
              WHEN id.warehouse_id = 154 THEN 'NA'
              WHEN id.warehouse_id IN (231, 242) THEN 'NA'
              WHEN id.warehouse_id = 421 THEN 'NA'
              WHEN id.warehouse_id = 221 THEN 'EU'
              WHEN id.warehouse_id = 366 THEN 'EU'
              WHEN id.warehouse_id = 466 THEN 'NA'
              WHEN id.warehouse_id = 601 THEN 'NA'
              WHEN id.warehouse_id IN (284, 44, 346) THEN 'NA'
              WHEN id.warehouse_id = 245 THEN 'NA'
              WHEN id.warehouse_id = 405 THEN 'EU'
    END)
       , (CASE
              WHEN id.warehouse_id = 107 THEN 'US'
              WHEN id.warehouse_id IN (109, 215) THEN 'CA'
              WHEN id.warehouse_id = 154 THEN 'US'
              WHEN id.warehouse_id IN (231, 242) THEN 'US'
              WHEN id.warehouse_id = 421 THEN 'US'
              WHEN id.warehouse_id = 221 THEN 'FR'
              WHEN id.warehouse_id = 366 THEN 'UK'
              WHEN id.warehouse_id = 466 THEN 'US'
              WHEN id.warehouse_id = 601 THEN 'US'
              WHEN id.warehouse_id IN (284, 44, 346) THEN 'US'
              WHEN id.warehouse_id = 245 THEN 'US'
              WHEN id.warehouse_id = 405 THEN 'FR'
    END)
       , (CASE
              WHEN id.warehouse_id = 107 THEN 'KENTUCKY'
              WHEN id.warehouse_id IN (109, 215) THEN 'CANADA'
              WHEN id.warehouse_id = 154 THEN 'KENTUCKY'
              WHEN id.warehouse_id IN (231, 242) THEN 'PERRIS'
              WHEN id.warehouse_id = 421 THEN 'KENTUCKY'
              WHEN id.warehouse_id = 221 THEN 'NETHERLANDS'
              WHEN id.warehouse_id = 366 THEN 'UK'
              WHEN id.warehouse_id = 466 THEN 'TIJUANA'
              WHEN id.warehouse_id = 601 THEN 'MIRACLE MILES'
              WHEN id.warehouse_id IN (284, 44, 346) THEN 'NEW JERSEY'
              WHEN id.warehouse_id = 245 THEN 'WINIT'
              WHEN id.warehouse_id = 405 THEN 'NETHERLANDS'
    END)
       , id.warehouse_id
       , id.product_sku
       , id.sku
       , ps.dp_size
       , COALESCE(mcsl.clearance_group, CAST(psph.sale_price AS VARCHAR(20)));
