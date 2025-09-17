CREATE OR REPLACE VIEW reference.store_warehouse AS
SELECT DISTINCT sw.store_id,
                ds.store_type,
                ds.store_name,
                ds.store_brand,
                ds.store_country                    AS country,
                ds.store_region                     AS region,
                dw.warehouse_id,
                IFF(ds.store_type = 'Retail', 1, 0) AS is_retail
FROM lake_consolidated.ultra_merchant.store_warehouse_map sw
         JOIN data_model.dim_store ds
              ON ds.store_id = sw.store_id
         JOIN data_model.dim_warehouse dw
              ON sw.warehouse_id = dw.warehouse_id
                  AND dw.is_active = 'true'
                  AND dw.is_consignment = 'false'
                  AND dw.warehouse_type = 'We Ship'
                  AND sw.store_id NOT IN
                      (36, 37, 41, 42, 50, 51, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 71, 72, 73, 74, 75, 76, 77,
                       78, 79, 141)

UNION

SELECT wm.store_id,
       ds.store_type,
       ds.store_name,
       ds.store_brand,
       ds.store_country                    AS country,
       ds.store_region                     AS region,
       wm.warehouse_id,
       IFF(ds.store_type = 'Retail', 1, 0) AS is_retail
FROM reference.store_warehouse_missing wm
         JOIN data_model.dim_store ds
              ON ds.store_id = wm.store_id;
