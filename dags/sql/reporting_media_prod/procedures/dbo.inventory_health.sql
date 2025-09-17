--TO DO - make warehouse selection more dynamic - currently manually list NA online inventory warehouse IDs by region
--FILTERS: FL US, online-only
--MAKE SURE TO SET MEMBERSHIP_STATUS PROPERLY FOR USE CASE - currently including ('lead','prospect','vip')


--comment this out before pushing to git!!!
--USE ROLE TFG_MEDIA_ADMIN;

--set parameters
set min_date = '2013-08-30';

--pull all ubt records
--in following queries, make sure we are only pulling records for most recent showroom for product attributes
--note: MPID may change over time & impact join to site CVR data
CREATE
    OR REPLACE TEMPORARY TABLE _showroom_ubt AS
SELECT DISTINCT ubt.*
FROM LAKE_VIEW.EXCEL.FL_MERCH_ITEMS_UBT ubt;

--get min showroom to fill in NULLs in following data where showroom starts after min_date
--creates row numbers for each sku partition based on showroom and selects only the first row for each sku
CREATE
    OR REPLACE TEMPORARY TABLE _min_showroom_ubt AS
SELECT DISTINCT ubt.*
FROM (SELECT *,
             ROW_NUMBER() OVER (
                 PARTITION BY sku
                 ORDER BY
                     current_showroom
                 ) AS row_num
      FROM LAKE_VIEW.EXCEL.FL_MERCH_ITEMS_UBT) ubt
WHERE row_num = 1;

--available to sell quantity by date & SKU (size-level)
--this will first join to ubt on the go_live_date, then fill in values in between go live dates
--so, if there is a product with 2 showrooms with go live dates '2023-01-15' and '2023-02-15', this will show the Feb showroom from 1/15-2/15 and the March showroom from 2/15-present
-- includes only NA + EU online
CREATE
    OR REPLACE TEMPORARY TABLE _avail_to_sell AS
with _base_inv as (SELECT TO_DATE(fih.local_date)                                AS date,
                          fih.sku,
                          UPPER(
                              TRIM(
                                  SUBSTRING(
                                      fih.sku,
                                      1,
                                      CHARINDEX(
                                          '-',
                                          fih.sku,
                                          CHARINDEX('-', fih.sku) + 1
                                      ) - 1
                                  )
                              )
                          )                                                      as product_sku,
                          ubt.CURRENT_NAME,
                          ubt.color,
                          ubt.current_showroom,
                          ubt.GO_LIVE_DATE,
                          CASE
                              WHEN ubt.gender IS NULL THEN NULL
                              WHEN UPPER(
                                       TRIM(ubt.gender)
                                   ) ilike 'MEN%' THEN 'MEN'
                              ELSE 'WOMEN' END                                   AS gender,
                          CASE
                              when ubt.category ilike 'Dress%' then 'ONEPIECES'
                              when ubt.category ilike 'Bra%' then 'BRA'
                              when ubt.category ilike '%Jacket%' then 'JACKET'
                              when ubt.category ilike '%Bottom%' then 'BOTTOM'
                              when ubt.category ilike '%Top%' then 'TOP'
                              else ubt.category end                              as category,
                          ubt.sub_brand,
                          ubt.image_url,
                          ubt.item_status,
                          ubt.NA_TOTAL_UNITS                                     as na_ecomm_buy,
                          ubt.ITEM_RANK,
                          ubt.BUY_TIMING,
                          ubt.CLASS,
                          ubt.FABRIC,
                          i.size                                                 as size_extended,
                          --clean up size names
                          CASE
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike any ('OS%', 'ONE SIZE', 'ONESIZE') THEN 'OS'
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%2XL%' THEN 'XXL'
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%3XL%' THEN 'XXXL'
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%XXXL%' THEN 'XXXL'
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%XXL/1X%' THEN 'XXL/1X' -- added
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike any ('%XXL%', 'SIZE XXL%') THEN 'XXL/1X' --'XXL/XXXL'?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike any ('%XL%', 'SIZE XL%') THEN 'XL' --'XL/1X'? and 'XL/XXL'?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%XXS%' THEN 'XXS' --'XXS/XS'?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike any ('%XS%', 'SIZE XS%') THEN 'XS' --'XS/S'?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%S/M%' THEN 'S/M' --added
                              WHEN UPPER(
                                       TRIM(I.SIZE)
                                   ) ilike '%M/L%' THEN 'M/L' --added
                              WHEN UPPER(
                                       TRIM(I.SIZE)
                                   ) ilike '%L/XL%' THEN 'L/XL' --added
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike any ('%S%', '%SM%', 'SIZE S%') THEN 'S' --added S -- 'SM/MD'?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike any ('%M%', '%MD%', 'SIZE M%') THEN 'M' --added MD
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike any ('%L%', '%LG%', 'SIZE L%') THEN 'L' --added LG  --'L/XL'?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%X3%' THEN '3X'
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%1X%' THEN 'XXL/1X' --1X/2X?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%2X%' THEN '2X' --2X/3X?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%3X%' THEN '3X' --3X/4X?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%4X%' THEN '4X' --4X/5X?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%5X%' THEN '5X' --5X/6X?
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike '%6X%' THEN '6X'
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) ilike 'Unknown' THEN 'OS'
                              WHEN UPPER(
                                       TRIM(I.size)
                                   ) in ('2T', '3T') THEN 'OTHER'
                              ELSE UPPER(
                                  TRIM(I.size)
                                   ) end                                         AS size,
                          CASE
                              when i.description ilike '%Short' then 'Short'
                              when i.description ilike '%Tall' then 'Tall'
                              when i.description ilike '%Reg' then 'Regular' end as inseam,
                          CASE
                              WHEN fih.warehouse_id in (107, 154, 421, 231, 465, 466) THEN 'US'
                              WHEN fih.warehouse_id = 109 THEN 'CA'
                              WHEN lw.region_id = 8 THEN w.country_code
                              WHEN (
                                          UPPER(
                                              TRIM(lw.label)
                                          ) ilike '%UK'
                                      and fih.local_date >= '2021-03-24'
                                  ) THEN 'UK'
                              WHEN fih.warehouse_id in (108, 221, 366) THEN 'EU'
                              WHEN lw.region_id = 11 THEN 'MX' END               AS country_region,
                          IFF(lw.region_id = 8, 'Retail', 'Online')              AS store_type,
                          SUM(fih.available_to_sell_quantity)                    AS available_to_sell_quantity,
                          SUM(
                              fih.open_to_buy_quantity + fih.intransit_quantity
                          )                                                      as OTB_incl_IT_inventory
                   FROM edw_prod.data_model_fl.fact_inventory_history fih
                            LEFT JOIN edw_prod.data_model_fl.dim_warehouse w ON fih.warehouse_id = w.warehouse_id
                            JOIN lake_view.ultra_warehouse.item i ON i.item_id = fih.item_id
                            JOIN lake_view.ultra_warehouse.company c ON c.company_id = i.company_id
                            JOIN lake_view.ultra_warehouse.warehouse lw on fih.warehouse_id = lw.warehouse_id
                            LEFT JOIN _showroom_ubt ubt on ubt.sku = fih.product_sku
                       and ubt.GO_LIVE_DATE = TO_DATE(fih.local_date)
                   WHERE fih.local_date >= $min_date
                     AND UPPER(
                             TRIM(c.label)
                         ) IN ('FABLETICS', 'YITTY') --this is where to add yitty
                   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
                   HAVING country_region in ('US', 'CA', 'MX', 'EU', 'UK') --8.22.24 Christina added in EU Warehouses
                      AND store_type <> 'Retail'),
--forward fill NULLs for ubt attributes
--this will fill in the records between go live dates
-- last_value window function takes the most recent date from the _base_inv CTE (so the date from fih) and forward fills in the attributes for each sku partition based on the most recent non-null values for that sku
     -- do we want to use current_showroom instead??
     _ffill as (select date,
                       sku,
                       product_sku,
                       LAST_VALUE(current_name IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS current_name,
                       LAST_VALUE(color IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS color,
                       LAST_VALUE(current_showroom IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS current_showroom,
                       LAST_VALUE(go_live_date IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS go_live_date,
                       LAST_VALUE(gender IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS gender,
                       LAST_VALUE(category IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS category,
                       LAST_VALUE(sub_brand IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS sub_brand,
                       LAST_VALUE(image_url IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS image_url,
                       LAST_VALUE(item_status IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS item_status,
                       LAST_VALUE(na_ecomm_buy IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS na_ecomm_buy,
                       LAST_VALUE(ITEM_RANK IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS item_rank,
                       LAST_VALUE(BUY_TIMING IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS buy_timing,
                       LAST_VALUE(CLASS IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS class,
                       LAST_VALUE(FABRIC IGNORE NULLS) OVER (
                           PARTITION BY product_sku
                           ORDER BY
                               date ROWS BETWEEN UNBOUNDED PRECEDING
                               AND CURRENT ROW
                           ) AS fabric,
                       size_extended,
                       size,
                       inseam,
                       available_to_sell_quantity,
                       OTB_incl_IT_inventory
                FROM _base_inv b)
--for first days of product records, pull min ubt attributes to fill in NULLs
--this will fill in records before the first go live date
select a.date,
       a.sku,
       a.product_sku,
       coalesce(a.current_name, b.CURRENT_NAME)                         as current_name,
       coalesce(a.color, b.COLOR)                                       as color,
       coalesce(
           a.current_showroom, b.CURRENT_SHOWROOM
       )                                                                as current_showroom,
       coalesce(a.go_live_date, b.GO_LIVE_DATE)                         as go_live_date,
       IFF(coalesce(a.gender, b.GENDER) ILIKE 'MEN%', 'MENS', 'WOMENS') as gender,
       CASE
           when coalesce(a.category, b.CATEGORY) ilike 'Dress%' then 'ONEPIECES'
           when coalesce(a.category, b.CATEGORY) ilike 'Bra%' then 'BRA'
           when coalesce(a.category, b.CATEGORY) ilike '%Jacket%' then 'JACKET'
           when coalesce(a.category, b.CATEGORY) ilike '%Bottom%' then 'BOTTOM'
           when coalesce(a.category, b.CATEGORY) ilike '%Top%' then 'TOP'
           else coalesce(a.category, b.CATEGORY) end                    as category,
       coalesce(a.sub_brand, b.SUB_BRAND)                               as sub_brand,
       coalesce(a.image_url, b.image_url)                               as image_url,
       coalesce(a.item_status, b.ITEM_STATUS)                           as item_status,
       coalesce(
           a.na_ecomm_buy, b.NA_TOTAL_UNITS
       )                                                                as na_ecomm_buy,
       coalesce(a.item_rank, b.ITEM_RANK)                               as item_rank,
       coalesce(a.buy_timing, b.BUY_TIMING)                             as buy_timing,
       coalesce(a.class, b.CLASS)                                       as class,
       coalesce(a.fabric, b.BUY_TIMING)                                 as fabric,
       a.size_extended,
       a.size,
       a.inseam,
       a.available_to_sell_quantity,
       a.OTB_incl_IT_inventory
from _ffill a
         join _min_showroom_ubt b on a.product_sku = b.SKU;

--unit sales by date and sku
-- includes only NA + EU online
CREATE
    OR REPLACE TEMPORARY TABLE _daily_sales AS
SELECT TO_DATE(fol.order_local_datetime) AS date,
       dp.sku,
       dp.product_sku,
       sum(
           IFF(mc.membership_order_type_l2 = 'Activating VIP', fol.item_quantity, 0)
       )                                 AS activating_unit_sales,
       sum(
           IFF(mc.membership_order_type_l2 != 'Activating VIP', 0, fol.item_quantity)
       )                                 AS non_activating_unit_sales,
       sum(fol.item_quantity)            as unit_sales,
       sum(
           IFF(fol.TOKEN_COUNT = 0
                   and mc.membership_order_type_l2 <> 'Activating VIP', fol.item_quantity, 0)
       )                                 AS cash_unit_sales,
       sum(
           IFF(fol.bundle_product_id = -1, 0, fol.item_quantity)
       )                                 AS bundle_unit_sales,
       sum(
           IFF(mc.membership_order_type_l2 = 'Activating VIP'
                   AND fol.bundle_product_id <> -1, fol.item_quantity, 0)
       )                                 AS activating_bundle_unit_sales,
       sum(
           IFF(mc.membership_order_type_l2 <> 'Activating VIP'
                   AND fol.bundle_product_id <> -1, fol.item_quantity, 0)
       )                                 AS non_activating_bundle_unit_sales
FROM edw_prod.data_model_fl.fact_order_line fol
         JOIN edw_prod.data_model_fl.fact_order fo ON fol.order_id = fo.order_id
         JOIN edw_prod.data_model_fl.dim_product dp ON dp.product_id = fol.product_id --and dp.meta_is_current = 1
         JOIN edw_prod.data_model_fl.dim_store ds ON fol.store_id = ds.store_id -- and meta_is_current = 1
         JOIN edw_prod.data_model_fl.dim_product_type dpt
              ON fol.product_type_key = dpt.product_type_key --and dpt.meta_is_current = 1
         JOIN edw_prod.data_model_fl.DIM_ORDER_MEMBERSHIP_CLASSIFICATION mc
              on mc.order_membership_classification_key = fol.order_membership_classification_key
         JOIN edw_prod.data_model_fl.DIM_ORDER_sales_channel doc
              ON fol.order_sales_channel_key = doc.order_sales_channel_key
         JOIN edw_prod.data_model_fl.DIM_ORDER_status dos ON fol.order_status_key = dos.order_status_key
         JOIN edw_prod.data_model_fl.DIM_ORDER_processing_status ops
              ON ops.order_processing_status_key = fo.order_processing_status_key
         JOIN edw_prod.data_model_fl.dim_order_line_status ols ON fol.order_line_status_key = ols.order_line_status_key
             LEFT JOIN _showroom_ubt ubt on ubt.sku = dp.product_sku
WHERE UPPER(ubt.sub_brand) IN ('FABLETICS','YITTY','SCRUBS') -- 8.22.24 Christina updated to filter on UBT instead of dim store
  AND ds.store_region in ('NA', 'EU') --8.22.24 Christina added in EU
  AND ds.store_type <> 'Retail'
  AND dpt.is_free = 'false'
  AND dpt.product_type_name IN ('Bundle Component', 'Normal')
  AND doc.order_classification_L1 = 'Product Order'
  AND dos.order_status IN ('Success', 'Pending')
  AND date >= $min_date
  AND ols.order_line_status <> 'Cancelled'
GROUP BY 1,
         2,
         3;

--daily discount amounts by product sku
-- includes only NA + EU online
CREATE
    OR REPLACE TEMPORARY TABLE _daily_discounts AS
SELECT TO_DATE(fol.order_local_datetime) AS date,
       dp.product_sku,
       sum(
           fol.product_discount_local_amount
       )                                 as product_discount_local_amount,
       sum(
           fol.PRODUCT_SUBTOTAL_LOCAL_AMOUNT
       )                                 as product_subtotal_local_amount,
       sum(
           fol.product_discount_local_amount
       ) / sum(
           fol.PRODUCT_SUBTOTAL_LOCAL_AMOUNT
           )                             as total_cash_discount,
       AVG(
           fol.product_discount_local_amount / fol.PRODUCT_SUBTOTAL_LOCAL_AMOUNT
       )                                 as avg_cash_discount
FROM edw_prod.data_model_fl.fact_order_line fol
         JOIN edw_prod.data_model_fl.fact_order fo ON fol.order_id = fo.order_id
         JOIN edw_prod.data_model_fl.dim_product dp ON dp.product_id = fol.product_id --and dp.meta_is_current = 1
         JOIN edw_prod.data_model_fl.dim_store ds ON fol.store_id = ds.store_id -- and meta_is_current = 1
         JOIN edw_prod.data_model_fl.dim_product_type dpt
              ON fol.product_type_key = dpt.product_type_key --and dpt.meta_is_current = 1
         JOIN edw_prod.data_model_fl.DIM_ORDER_MEMBERSHIP_CLASSIFICATION mc
              on mc.order_membership_classification_key = fol.order_membership_classification_key
         JOIN edw_prod.data_model_fl.DIM_ORDER_sales_channel doc
              ON fol.order_sales_channel_key = doc.order_sales_channel_key
         JOIN edw_prod.data_model_fl.DIM_ORDER_status dos ON fol.order_status_key = dos.order_status_key
         JOIN edw_prod.data_model_fl.DIM_ORDER_processing_status ops
              ON ops.order_processing_status_key = fo.order_processing_status_key
         JOIN edw_prod.data_model_fl.dim_order_line_status ols ON fol.order_line_status_key = ols.order_line_status_key
    LEFT JOIN _showroom_ubt ubt on ubt.sku = dp.PRODUCT_SKU
WHERE UPPER(ubt.sub_brand) IN ('FABLETICS','YITTY','SCRUBS') -- 8.22.24 Christina updated to filter on UBT instead of dim store
  AND ds.store_region in ('NA', 'EU') --8.22.24 Christina added in EU
  AND ds.store_type <> 'Retail'
  AND dpt.is_free = 'false'
  AND dpt.product_type_name IN ('Bundle Component', 'Normal')
  AND doc.order_classification_L1 = 'Product Order'
  AND dos.order_status IN ('Success', 'Pending')
  AND date >= $min_date
  AND ols.order_line_status <> 'Cancelled'
  AND fol.TOKEN_COUNT = 0
  AND mc.membership_order_type_l2 <> 'Activating VIP'
  AND fol.PRODUCT_SUBTOTAL_LOCAL_AMOUNT > 0
GROUP BY 1,
         2;

--grid views, pdp views, add to cart, orders by product sku
--this table may not have reliable data further back than 08/22
--reporting_prod.fabletics.pcvr_session_historical  is owned by Omar
CREATE
    OR REPLACE TEMPORARY TABLE _all_cvr AS
select
    --registration_session --can be used later if filtering down to lead to VIP same session
    product_id              as master_product_id,
    session_start_date      as date,
    sum(list_view_total)    as grid_views,
    --note logic for FL does not double-count if clicked back into same view - should revisit this
    sum(ss_pdp_view)        as pdp_views,
    --same session metrics to more cleanly tie back to sessions started as leads/prospects
    sum(ss_product_added)   as add_to_carts,
    sum(ss_product_ordered) as total_orders,
    --activating
    sum(
        IFF(lower(membership_status) <> 'vip', list_view_total, 0)
    )                       as activating_grid_views,
    --note logic for FL does not double-count if clicked back into same view - should revisit this
    sum(
        IFF(lower(membership_status) <> 'vip', ss_pdp_view, 0)
    )                       as activating_pdp_views,
    --same session metrics to more cleanly tie back to sessions started as leads/prospects
    sum(
        IFF(lower(membership_status) <> 'vip', ss_product_added, 0)
    )                       as activating_add_to_carts,
    sum(
        IFF(is_activating = 'true'
                and lower(membership_status) <> 'vip', ss_product_ordered, 0)
    )                       as activating_orders,
    --non activating
    sum(
        IFF(lower(membership_status) = 'vip', list_view_total, 0)
    )                       as non_activating_grid_views,
    --note logic for FL does not double-count if clicked back into same view - should revisit this
    sum(
        IFF(lower(membership_status) = 'vip', ss_pdp_view, 0)
    )                       as non_activating_pdp_views,
    --same session metrics to more cleanly tie back to sessions started as leads/prospects
    sum(
        IFF(lower(membership_status) = 'vip', ss_product_added, 0)
    )                       as non_activating_add_to_carts,
    sum(
        IFF(is_activating = 'false'
                and lower(membership_status) = 'vip', ss_product_ordered, 0)
    )                       as non_activating_orders
from reporting_prod.fabletics.pcvr_session_historical
where date >= $min_date
  and lower(membership_status) in ('lead', 'prospect', 'vip')
  and product_type <> 'Bundle' --exclude bundles for now for simplicity
  and platform <> 'Mobile App'
group by 1,
         2;

--inventory & sales rolled up to product sku level
--already filtered to FL online
CREATE
    OR REPLACE TEMPORARY TABLE _inv_agg AS
with iaw as (select ats.date,
                    to_varchar(ats.product_sku)         as product_sku,
                    MAX(ats.current_name)               as current_name,
                    MAX(ats.color)                      as color,
                    MAX(ats.current_showroom)           as current_showroom,
                    MAX(ats.GO_LIVE_DATE)               as go_live_date,
                    MAX(ats.gender)                     as gender,
                    MAX(ats.category)                   as category,
                    MAX(ats.sub_brand)                  as sub_brand,
                    MAX(ats.image_url)                  as image_url,
                    MAX(ats.item_status)                as item_status,
                    MAX(ats.na_ecomm_buy)               as na_ecomm_buy,
                    MAX(ats.item_rank)                  as item_rank,
                    MAX(ats.buy_timing)                 as buy_timing,
                    MAX(ats.class)                      as class,
                    MAX(ats.fabric)                     as fabric,
                    SUM(ats.available_to_sell_quantity) as available_to_sell_quantity,
                    SUM(
                        IFF(ats.size = 'S', ats.available_to_sell_quantity, 0)
                    )                                   as available_to_sell_quantity_small,
                    SUM(
                        IFF(ats.size = 'M', ats.available_to_sell_quantity, 0)
                    )                                   as available_to_sell_quantity_med,
                    SUM(
                        IFF(ats.size = 'L', ats.available_to_sell_quantity, 0)
                    )                                   as available_to_sell_quantity_large,
                    SUM(ds.unit_sales)                  as unit_sales,
                    SUM(ds.bundle_unit_sales)           as bundle_unit_sales,
                    SUM(ds.cash_unit_sales)             as cash_unit_sales,
                    SUM(
                        ds.activating_bundle_unit_sales
                    )                                   as activating_bundle_unit_sales,
                    SUM(
                        ds.non_activating_bundle_unit_sales
                    )                                   as non_activating_bundle_unit_sales,
                    SUM(ds.activating_unit_sales)       as activating_unit_sales,
                    SUM(ds.non_activating_unit_sales)   as non_activating_unit_sales,
                    SUM(
                        IFF(ats.size = 'S', ds.unit_sales, 0)
                    )                                   as unit_sales_small,
                    SUM(
                        IFF(ats.size = 'M', ds.unit_sales, 0)
                    )                                   as unit_sales_med,
                    SUM(
                        IFF(ats.size = 'L', ds.unit_sales, 0)
                    )                                   as unit_sales_large
             from _avail_to_sell ats
                      full outer join _daily_sales ds on ats.date = ds.date
                 and ats.sku = ds.sku
             group by 1,
                      2),
     iaw2 as (select *,
                     IFF(available_to_sell_quantity_small = 0, 1, 0) AS small_broken_flag,
                     IFF(available_to_sell_quantity_med = 0, 1, 0)   AS med_broken_flag,
                     IFF(available_to_sell_quantity_large = 0, 1, 0) AS large_broken_flag
              from iaw)
select *,
       IFF(small_broken_flag > 0
               or med_broken_flag > 0
               or large_broken_flag > 0, 1, 0) AS size_broken_flag
from iaw2;

--product sku to MPID mapping - note there seem to be one to many product sku to MPID in some edge cases
--Lacey Asplund helped me with this - reach out to her if you have questions here
-- NA + EU Online only
CREATE
    OR REPLACE TEMPORARY TABLE _prod_mapping AS
with store AS (SELECT *
               FROM edw_prod.data_model.dim_store
               WHERE store_brand = 'Fabletics'
                 and STORE_COUNTRY = 'US'
                 and store_type = 'Online') --Consolidating dim product data down to 1 record per mpid
    ,
     dp1 AS (
         --First part is for singular products
         --udf_unconcat_brand removes the last 2 numbers from MPID (brand ID)
         SELECT DISTINCT edw_prod.stg.udf_unconcat_brand(master_product_id) as master_product_id,
                         d.product_sku,
                         product_name,
                         product_type,
                         -- a row numbers is assigned to each row, partitioned by MPID and ordered by product_sku
                         ROW_NUMBER() OVER (
                             PARTITION BY edw_prod.stg.udf_unconcat_brand(master_product_id)
                             ORDER BY
                                 d.product_sku
                             )                                              AS rn
         FROM edw_prod.data_model.dim_product d
                  JOIN store ON d.store_id = store.store_id
         WHERE master_product_id <> -1
           AND product_id_object_type <> 'No Size-Master Product ID'
           AND d.product_sku <> 'Unknown'
           AND lower(product_status) = 'active'
           -- 2.8.24 removed is_active condition because found skus where product_status is active but is_active is false
--     AND is_active = 'true'
           AND LEN(d.product_sku) > 5
         UNION
         -- This part is for one size products that where the product_id is the master_product_id record
         SELECT DISTINCT edw_prod.stg.udf_unconcat_brand(product_id) AS master_product_id,
                         dp.product_sku,
                         product_name,
                         product_type,
                         ROW_NUMBER() OVER (
                             PARTITION BY edw_prod.stg.udf_unconcat_brand(product_id)
                             ORDER BY
                                 dp.product_sku
                             )                                       AS rn
         FROM edw_prod.data_model.dim_product dp
                  JOIN store ON dp.store_id = store.store_id
         WHERE product_id_object_type = 'No Size-Master Product ID'
           AND lower(product_status) = 'active'
           -- 2.8.24 removed is_active condition because found skus where product_status is active but is_active is false
--     AND is_active = 'true'
           AND (
                     dp.product_sku = SPLIT_PART(product_alias, ' ', 1)
                 or product_alias not ilike ('do not use%')
             ))
select product_sku,
       max(master_product_id) as master_product_id
from dp1
         full outer join _showroom_ubt ubt on dp1.product_sku = ubt.sku
where product_name NOT ILIKE '%membership%'
  AND product_type NOT ILIKE '%bundle%'
  AND product_name NOT ILIKE '%insert%'
  AND product_type NOT ILIKE '%insert%'
group by 1;

--join CVR data with product sku
CREATE
    OR REPLACE TEMPORARY TABLE _cvr_with_sku AS
select a.*,
       m.product_sku
from _all_cvr a
         join _prod_mapping m on to_varchar(a.master_product_id) = to_varchar(m.master_product_id);

--SOME MPIDS IN CVR AND NOT IN MAPPING due to additional filtering in mapping, so we do inner join
--filters are set in mapping

--join inventory aggregates to cvr data
CREATE
    OR REPLACE TEMPORARY TABLE _all_agg AS
WITH  aaw1 AS (select a.*,
                     i.current_name,
                     i.color,
                     i.current_showroom,
                     --showroom category for visual that shows % of unit sales from current showroom, core or other showrooms by month
                     CASE
                         WHEN LOWER(i.item_status) ILIKE '%core%' THEN 'core'
                         WHEN DATE_PART(MONTH, a.date) = DATE_PART(MONTH, i.current_showroom)
                             AND DATE_PART(YEAR, a.date) = DATE_PART(YEAR, i.current_showroom) THEN 'current showroom'
                         WHEN (
                                          DATE_PART(MONTH, a.date) = 1
                                      AND DATE_PART(MONTH, i.current_showroom) = 12
                                      AND DATE_PART(YEAR, a.date) = DATE_PART(YEAR, i.current_showroom) + 1
                                  )
                             OR (
                                          DATE_PART(MONTH, a.date) = DATE_PART(MONTH, i.current_showroom) + 1
                                      AND DATE_PART(YEAR, a.date) = DATE_PART(YEAR, i.current_showroom)
                                  ) THEN 'previous showroom'
                         WHEN (
                                          DATE_PART(MONTH, a.date) = 12
                                      AND DATE_PART(MONTH, i.current_showroom) = 1
                                      AND DATE_PART(YEAR, a.date) + 1 = DATE_PART(YEAR, i.current_showroom)
                                  )
                             OR (
                                              DATE_PART(MONTH, a.date) + 1 = DATE_PART(MONTH, i.current_showroom)
                                      AND DATE_PART(YEAR, a.date) = DATE_PART(YEAR, i.current_showroom)
                                  ) THEN 'following showroom'
                         ELSE 'other' END                                             AS showroom_category,
                     i.go_live_date,
                     i.gender,
                     i.category,
                     i.sub_brand,
                     i.image_url,
                     i.item_status,
                     i.na_ecomm_buy,
                     --clean up the rank field from the ubt
                     CASE
                         WHEN i.item_rank LIKE 'A%' THEN 'A'
                         WHEN i.item_rank LIKE 'B%' THEN 'B'
                         WHEN i.item_rank LIKE 'CORE%' THEN 'CORE'
                         WHEN i.item_rank LIKE 'CUT%' THEN null
                         WHEN i.item_rank LIKE 'C%' THEN 'C' END                      AS item_rank,
                     i.buy_timing,
                     i.class,
                     i.fabric,
                     (a.date - i.current_showroom)                                    as days_since_showroom_start,
                     --coalesce used to replace nulls with 0
                     COALESCE(i.small_broken_flag, 0)                                 as small_broken_flag,
                     COALESCE(i.med_broken_flag, 0)                                   as med_broken_flag,
                     COALESCE(i.large_broken_flag, 0)                                 as large_broken_flag,
                     COALESCE(i.size_broken_flag, 0)                                  as size_broken_flag,
                     COALESCE(i.available_to_sell_quantity, 0)                        as available_to_sell_quantity,
                     COALESCE(
                         i.available_to_sell_quantity_small,
                         0
                     )                                                                as available_to_sell_quantity_small,
                     COALESCE(
                         i.available_to_sell_quantity_med,
                         0
                     )                                                                as available_to_sell_quantity_med,
                     COALESCE(
                         i.available_to_sell_quantity_large,
                         0
                     )                                                                as available_to_sell_quantity_large,
                     COALESCE(i.unit_sales, 0)                                        as unit_sales,
                     COALESCE(i.cash_unit_sales, 0)                                   as cash_unit_sales,
                     COALESCE(i.bundle_unit_sales, 0)                                 as bundle_unit_sales,
                     COALESCE(
                         i.activating_bundle_unit_sales,
                         0
                     )                                                                as activating_bundle_unit_sales,
                     COALESCE(
                         i.non_activating_bundle_unit_sales,
                         0
                     )                                                                as non_activating_bundle_unit_sales,
                     COALESCE(i.activating_unit_sales, 0)                             as activating_unit_sales,
                     COALESCE(i.non_activating_unit_sales, 0)                         as non_activating_unit_sales,
                     COALESCE(i.unit_sales_small, 0)                                  as unit_sales_small,
                     COALESCE(i.unit_sales_med, 0)                                    as unit_sales_med,
                     COALESCE(i.unit_sales_large, 0)                                  as unit_sales_large,
                     a.total_orders / NULLIF(a.grid_views, 0)                         as spi,
                     a.activating_orders / NULLIF(a.activating_grid_views, 0)         as activating_spi,
                     a.non_activating_orders / NULLIF(a.non_activating_grid_views, 0) as non_activating_spi,
                     a.pdp_views / NULLIF(a.grid_views, 0)                            as grid_to_pdp,
                     a.total_orders / NULLIF(a.pdp_views, 0)                          as spv,
                     a.activating_orders / NULLIF(a.activating_pdp_views, 0)          as activating_spv,
                     a.non_activating_orders / NULLIF(a.non_activating_pdp_views, 0)  as non_activating_spv,
                     a.add_to_carts / NULLIF(a.pdp_views, 0)                          as pdp_to_atc,
                     SUM(a.total_orders) OVER (
                         PARTITION BY a.master_product_id
                         ORDER BY
                             a.date ROWS BETWEEN 29 PRECEDING
                             AND CURRENT ROW
                         )                                                            AS total_orders_30_day_sum,
                     AVG(a.total_orders) OVER (
                         PARTITION BY a.master_product_id
                         ORDER BY
                             a.date ROWS BETWEEN 29 PRECEDING
                             AND CURRENT ROW
                         )                                                            AS total_orders_30_day_avg,
                     AVG(a.grid_views) OVER (
                         PARTITION BY a.master_product_id
                         ORDER BY
                             a.date ROWS BETWEEN 29 PRECEDING
                             AND CURRENT ROW
                         )                                                            AS grid_views_30_day_avg,
                     --calculated release date to show the date a product sku first has grid and pdp views as well as available inventory
                     MIN(
                         CASE
                             WHEN (
                                              grid_views > 5
                                          OR pdp_views > 5
                                      )
                                 AND available_to_sell_quantity > 0 THEN a.date END
                     ) OVER (PARTITION BY master_product_id)                          AS calculated_release_date
              from _cvr_with_sku a
                       join _inv_agg i on to_varchar(a.product_sku) = to_varchar(i.product_sku)
                  and a.date = i.date),
    --2.8.24 removed class exception for scrubs
--               where lower(i.class) not ilike '%scrub%')
     aaw as (select *,
                    MIN(
                        CASE
                            WHEN date > calculated_release_date
                                AND size_broken_flag = 1 THEN date END
                    ) OVER (PARTITION BY master_product_id) AS first_broken_date,
                    MIN(
                        CASE
                            WHEN date > go_live_date
                                AND size_broken_flag = 1 THEN date END
                    ) OVER (
                            PARTITION BY master_product_id, current_showroom
                            )                               AS first_broken_date_showroom,
                    DATE_TRUNC(
                        'MONTH', calculated_release_date
                    )                                       AS calculated_release_year_month,
                    DATE_TRUNC('MONTH', date)               AS date_year_month
             from aaw1)
SELECT *,
       DATEDIFF(
           DAY, current_showroom, first_broken_date
       )                                                                       AS days_since_current_showroom_to_first_broken,
       DATEDIFF(
           DAY, calculated_release_date, first_broken_date
       )                                                                       AS days_since_release_to_first_broken,
       DATEDIFF(
           DAY, go_live_date, first_broken_date_showroom
       )                                                                       AS days_since_go_live_to_break,
       (date - calculated_release_date)                                        as days_since_release,
       CASE WHEN calculated_release_year_month = date_year_month THEN 'M1' END AS M1_flag,
       IFF((date - calculated_release_date) <= 30, '1', '0')                   AS new_product_flag,
       AVG(spi) OVER (
           PARTITION BY master_product_id
           ORDER BY
               date ROWS BETWEEN 29 PRECEDING
               AND CURRENT ROW
           )                                                                   AS spi_30_day_avg,
       AVG(spv) OVER (
           PARTITION BY master_product_id
           ORDER BY
               date ROWS BETWEEN 29 PRECEDING
               AND CURRENT ROW
           )                                                                   AS spv_30_day_avg,
       AVG(pdp_to_atc) OVER (
           PARTITION BY master_product_id
           ORDER BY
               date ROWS BETWEEN 29 PRECEDING
               AND CURRENT ROW
           )             AS pdp_to_atc_30_day_avg,
       IFF(size_broken_flag > 0, NULL, AVG(
           CASE WHEN size_broken_flag = 0 THEN grid_views END
                                       ) OVER (
               PARTITION BY master_product_id
               ORDER BY
                   date ROWS BETWEEN 29 PRECEDING
                   AND CURRENT ROW
               ))        AS pre_break_grid_views_30_day_avg,
       IFF(size_broken_flag > 0, NULL, AVG(
           CASE WHEN size_broken_flag = 0 THEN spi END
                                       ) OVER (
               PARTITION BY master_product_id
               ORDER BY
                   date ROWS BETWEEN 29 PRECEDING
                   AND CURRENT ROW
               ))        AS pre_break_spi_30_day_avg,
       IFF(size_broken_flag > 0, NULL, AVG(
           CASE WHEN size_broken_flag = 0 THEN spv END
                                       ) OVER (
               PARTITION BY master_product_id
               ORDER BY
                   date ROWS BETWEEN 29 PRECEDING
                   AND CURRENT ROW
               ))                                            AS     pre_break_spv_30_day_avg,
       IFF(size_broken_flag > 0, NULL, AVG(
           CASE WHEN size_broken_flag = 0 THEN pdp_to_atc END
                                       ) OVER (
               PARTITION BY master_product_id
               ORDER BY
                   date ROWS BETWEEN 29 PRECEDING
                   AND CURRENT ROW
               ))                                    AS             pre_break_pdp_to_atc_30_day_avg,
       IFF(size_broken_flag > 0, NULL, grid_views)   AS             pre_break_grid_views,
       IFF(size_broken_flag > 0, NULL, total_orders) AS             pre_break_orders,
       IFF(size_broken_flag > 0, NULL, spi)          AS             pre_break_spi,
       IFF(size_broken_flag > 0, NULL, spv)          AS             pre_break_spv,
       IFF(size_broken_flag > 0, NULL, pdp_to_atc) pre_break_pdp_to_atc
FROM aaw
order by date,
         master_product_id;

--join ad spend by sku
CREATE
    OR REPLACE TEMPORARY TABLE _all_agg_with_ad_spend AS
WITH ads AS (select date,
                    sku,
                    sum(total_spend) as ad_spend
             from reporting_media_prod.dbo.media_spend_by_sku
             where store_brand_name = 'Fabletics'
               and region = 'NA'
             group by 1,
                      2
             having ad_spend > 0)
select a.*,
       b.ad_spend
from _all_agg a
         left join ads b on a.date = b.date
    and a.product_sku = b.sku;

--add in bop inventory on the first of the month for each product sku
CREATE
    OR REPLACE TEMPORARY TABLE _all_agg_with_bop AS
with bop as (SELECT date,
                    PRODUCT_SKU,
                    AVAILABLE_TO_SELL_QUANTITY as bop_inventory
             from _all_agg_with_ad_spend
             where date = DATE_TRUNC('MONTH', date :: DATE)
             order by PRODUCT_SKU,
                      date)
select a.*,
       b.bop_inventory
from _all_agg_with_ad_spend a
         left join bop b on a.product_sku = b.PRODUCT_SKU
    and a.date = b.DATE;

--join daily avg cash discounts
CREATE
    OR REPLACE TEMPORARY TABLE _all_agg_with_disc AS
SELECT a.*,
       b.avg_cash_discount,
       b.product_discount_local_amount,
       b.product_subtotal_local_amount,
       b.total_cash_discount
FROM _all_agg_with_bop a
         LEFT JOIN _daily_discounts b on a.product_sku = b.PRODUCT_SKU
    and a.date = b.DATE;

--write into tables
--only include rows by product after its calculated release date (date of first grid views or pdp views - defined earlier in script)
CREATE
OR REPLACE TABLE REPORTING_BASE_PROD.FABLETICS.INVENTORY_HEALTH AS
SELECT *,
       sum(unit_sales) over (
           partition by product_sku
           order by
               date
           ) as running_sum_unit_sales
FROM _all_agg_with_disc
WHERE date >= calculated_release_date
  and date >= $min_date
  and date < DATE(
    TO_CHAR(
        DATE_TRUNC(
            'MONTH',
            CURRENT_DATE()
        ),
        'YYYY-MM-01'
    )
             )
  and current_showroom >= $min_date;

--STILL NEED TO REVIEW THE FOLLOWING CODE FOR TABLEAU DASH--


--used for dashboard filtering
--don't really use this - can probably delete if you don't want to use it for anything
CREATE
    OR REPLACE TABLE REPORTING_MEDIA_PROD.dbo.AGG_GRID_VIEWS AS
SELECT MASTER_PRODUCT_ID,
       PRODUCT_SKU,
       SUM(
           IFNULL(PRE_BREAK_GRID_VIEWS, 0)
       )     AS total_pre_break_grid_views,
       PERCENT_RANK() OVER (
           ORDER BY
               SUM(
                   IFNULL(PRE_BREAK_GRID_VIEWS, 0)
               ) ASC
           ) AS percentile_total_pre_break_grid_views
FROM REPORTING_BASE_PROD.FABLETICS.INVENTORY_HEALTH
where NEW_PRODUCT_FLAG = 1
  and current_showroom < current_date()
GROUP BY 1,
         2
HAVING total_pre_break_grid_views > 0
ORDER BY 3 ASC;

--used for showroom quality summary
--create table to flag which products may have 2 successive showrooms - to flag this may skew this output for fashion products
CREATE
    OR REPLACE TEMPORARY TABLE _succ_srs AS
WITH product_showrooms AS (SELECT DISTINCT PRODUCT_SKU,
                                           current_showroom
                           FROM REPORTING_BASE_PROD.FABLETICS.INVENTORY_HEALTH)
SELECT ps1.product_sku,
       ps1.current_showroom,
       IFF(ps2.product_sku IS NOT NULL, 1, 0) AS has_showroom_one_month_apart
FROM product_showrooms ps1
         LEFT JOIN product_showrooms ps2 ON ps1.product_sku = ps2.product_sku
    AND (
                                                        DATEDIFF(
                                                            MONTH, ps1.current_showroom, ps2.current_showroom
                                                        ) = 1
                                                    OR DATEDIFF(
                                                           MONTH, ps1.current_showroom, ps2.current_showroom
                                                       ) = -1
                                                )
ORDER BY PRODUCT_SKU,
         CURRENT_SHOWROOM;

--set parameters for recommendation classification
set target_inv = 200;
set low_days_to_break = 21;
set high_days_to_break = 40;
--this is where we update the sell-through targets
set a_fashion_target_str = 0.96;
set b_fashion_target_str = 0.91;
set c_fashion_target_str = 0.89;
--this is where we update the range around the targets that is used to calculate the buy recommendation
set str_range = 0.05;
-- set seasonal_target_str = 0.55;

--write to table and create calculated fields for dashboard
CREATE
    OR REPLACE TABLE REPORTING_BASE_PROD.FABLETICS.SHOWROOM_QUALITY AS
WITH a AS (SELECT MASTER_PRODUCT_ID,
                  PRODUCT_SKU,
                  CATEGORY,
                  CURRENT_SHOWROOM,
                  ITEM_STATUS,
                  GENDER,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date < current_showroom, unit_sales, 0)
                  )                                                                         AS pre_cycle_unit_sales,
                  SUM(
                      IFF(date >= current_showroom
                                                    AND date <= DATEADD('day', 30, current_showroom), unit_sales, 0)
                  )                                                                         AS m1_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom), unit_sales, 0)
                  )                                                                         AS total_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date < current_showroom, cash_unit_sales, 0)
                  )                                                                         AS pre_cycle_cash_unit_sales,
                  SUM(
                      IFF(date >= current_showroom
                                                    AND date <= DATEADD('day', 30, current_showroom), cash_unit_sales, 0)
                  )                                                                         AS m1_cash_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom), cash_unit_sales, 0)
                  )                                                                         AS total_cash_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date < current_showroom, activating_unit_sales, 0)
                  )                                                                         AS pre_cycle_activating_unit_sales,
                  SUM(
                      IFF(date >= current_showroom
                                                    AND date <= DATEADD('day', 30, current_showroom), activating_unit_sales, 0)
                  )                                                                         AS m1_activating_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom), activating_unit_sales, 0)
                  )                                                                         AS total_activating_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date < current_showroom, non_activating_unit_sales, 0)
                  )                                                                         AS pre_cycle_non_activating_unit_sales,
                  SUM(
                      IFF(date >= current_showroom
                                                    AND date <= DATEADD('day', 30, current_showroom), non_activating_unit_sales, 0)
                  )                                                                         AS m1_non_activating_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom), non_activating_unit_sales, 0)
                  )                                                                         AS total_non_activating_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date < current_showroom, bundle_unit_sales, 0)
                  )                                                                         AS pre_cycle_bundle_unit_sales,
                  SUM(
                      IFF(date >= current_showroom
                                                    AND date <= DATEADD('day', 30, current_showroom), bundle_unit_sales, 0)
                  )                                                                         AS m1_bundle_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom), bundle_unit_sales, 0)
                  )                                                                         AS total_bundle_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date < current_showroom, activating_bundle_unit_sales, 0)
                  )                                                                         AS pre_cycle_activating_bundle_unit_sales,
                  SUM(
                      IFF(date >= current_showroom
                                                    AND date <= DATEADD('day', 30, current_showroom), activating_bundle_unit_sales, 0)
                  )                                                                         AS m1_activating_bundle_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom), activating_bundle_unit_sales, 0)
                  )                                                                         AS total_activating_bundle_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date < current_showroom, non_activating_bundle_unit_sales, 0)
                  )                                                                         AS pre_cycle_non_activating_bundle_unit_sales,
                  SUM(
                      IFF(date >= current_showroom
                                                    AND date <= DATEADD('day', 30, current_showroom), non_activating_bundle_unit_sales, 0)
                  )                                                                         AS m1_non_activating_bundle_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom), non_activating_bundle_unit_sales, 0)
                  )                                                                         AS total_non_activating_bundle_unit_sales,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom)
                                                    AND SIZE_BROKEN_FLAG = 0
                                                    AND GRID_VIEWS <> 0, TOTAL_ORDERS, 0)
                  ) / SUM(
                      CASE
                          WHEN date >= DATEADD('day', -15, current_showroom)
                              AND date <= DATEADD('day', 30, current_showroom)
                              AND SIZE_BROKEN_FLAG = 0
                              AND GRID_VIEWS <> 0 THEN GRID_VIEWS END
                      )                                                                     AS eom_pre_break_spi,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom)
                                                    AND SIZE_BROKEN_FLAG = 0
                                                    AND ACTIVATING_GRID_VIEWS <> 0, ACTIVATING_ORDERS, 0)
                  ) / SUM(
                      CASE
                          WHEN date >= DATEADD('day', -15, current_showroom)
                              AND date <= DATEADD('day', 30, current_showroom)
                              AND SIZE_BROKEN_FLAG = 0
                              AND ACTIVATING_GRID_VIEWS <> 0 THEN ACTIVATING_GRID_VIEWS END
                      )                                                                     AS eom_activating_pre_break_spi,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom)
                                                    AND SIZE_BROKEN_FLAG = 0
                                                    AND NON_ACTIVATING_GRID_VIEWS <> 0, NON_ACTIVATING_ORDERS, 0)
                  ) / SUM(
                      CASE
                          WHEN date >= DATEADD('day', -15, current_showroom)
                              AND date <= DATEADD('day', 30, current_showroom)
                              AND SIZE_BROKEN_FLAG = 0
                              AND NON_ACTIVATING_GRID_VIEWS <> 0 THEN NON_ACTIVATING_GRID_VIEWS END
                      )                                                                     AS eom_non_activating_pre_break_spi,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom)
                                                    AND SIZE_BROKEN_FLAG = 0
                                                    AND PDP_VIEWS <> 0, TOTAL_ORDERS, 0)
                  ) / SUM(
                      CASE
                          WHEN date >= DATEADD('day', -15, current_showroom)
                              AND date <= DATEADD('day', 30, current_showroom)
                              AND SIZE_BROKEN_FLAG = 0
                              AND PDP_VIEWS <> 0 THEN PDP_VIEWS END
                      )                                                                     AS eom_pre_break_spv,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom)
                                                    AND SIZE_BROKEN_FLAG = 0
                                                    AND ACTIVATING_PDP_VIEWS <> 0, ACTIVATING_ORDERS, 0)
                  ) / SUM(
                      CASE
                          WHEN date >= DATEADD('day', -15, current_showroom)
                              AND date <= DATEADD('day', 30, current_showroom)
                              AND SIZE_BROKEN_FLAG = 0
                              AND ACTIVATING_PDP_VIEWS <> 0 THEN ACTIVATING_PDP_VIEWS END
                      )                                                                     AS eom_activating_pre_break_spv,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom)
                                                    AND SIZE_BROKEN_FLAG = 0
                                                    AND NON_ACTIVATING_PDP_VIEWS <> 0, NON_ACTIVATING_ORDERS, 0)
                  ) / SUM(
                      CASE
                          WHEN date >= DATEADD('day', -15, current_showroom)
                              AND date <= DATEADD('day', 30, current_showroom)
                              AND SIZE_BROKEN_FLAG = 0
                              AND NON_ACTIVATING_PDP_VIEWS <> 0 THEN NON_ACTIVATING_PDP_VIEWS END
                      )                                                                     AS eom_non_activating_pre_break_spv,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND GRID_VIEWS <> 0, TOTAL_ORDERS, 0)
                  )                                                                         as cycle_total_orders,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND GRID_VIEWS <> 0, GRID_VIEWS, 0)
                  )                                                                         as cycle_grid_views,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND ACTIVATING_GRID_VIEWS <> 0, ACTIVATING_ORDERS, 0)
                  )                                                                         as cycle_activating_orders,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND ACTIVATING_GRID_VIEWS <> 0, ACTIVATING_GRID_VIEWS, 0)
                  )                                                                         as cycle_activating_grid_views,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND ACTIVATING_GRID_VIEWS <> 0, ACTIVATING_PDP_VIEWS, 0)
                  )                                                                         as cycle_activating_pdp_views,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND GRID_VIEWS <> 0, NON_ACTIVATING_ORDERS, 0)
                  )                                                                         as cycle_non_activating_orders,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND GRID_VIEWS <> 0, NON_ACTIVATING_GRID_VIEWS, 0)
                  )                                                                         as cycle_non_activating_grid_views,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND GRID_VIEWS <> 0, NON_ACTIVATING_PDP_VIEWS, 0)
                  )                                                                         as cycle_non_activating_pdp_views,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND GRID_VIEWS <> 0, TOTAL_ORDERS, 0)
                  ) / SUM(
                      CASE
                          WHEN date >= CURRENT_SHOWROOM
                              AND date <= DATEADD('day', 5, current_showroom)
                              AND GRID_VIEWS <> 0 THEN GRID_VIEWS END
                      )                                                                     AS cycle_spi,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND ACTIVATING_GRID_VIEWS <> 0, ACTIVATING_ORDERS, 0)
                  ) / SUM(
                      CASE
                          WHEN date >= CURRENT_SHOWROOM
                              AND date <= DATEADD('day', 5, current_showroom)
                              AND ACTIVATING_GRID_VIEWS <> 0 THEN ACTIVATING_GRID_VIEWS END
                      )                                                                     AS cycle_activating_spi,
                  SUM(
                      IFF(date >= CURRENT_SHOWROOM
                                                    AND date <= DATEADD('day', 5, current_showroom)
                                                    AND NON_ACTIVATING_GRID_VIEWS <> 0, NON_ACTIVATING_ORDERS, 0)
                  ) / SUM(
                      CASE
                          WHEN date >= CURRENT_SHOWROOM
                              AND date <= DATEADD('day', 5, current_showroom)
                              AND NON_ACTIVATING_GRID_VIEWS <> 0 THEN NON_ACTIVATING_GRID_VIEWS END
                      )                                                                     AS cycle_non_activating_spi,
                  MAX(NA_ECOMM_BUY)                                                         as na_ecomm_buy,
                  MAX(BUY_TIMING)                                                           as buy_timing,
                  MAX(ITEM_RANK)                                                            as original_item_rank_ubt,
                  MAX(CURRENT_NAME)                                                         as current_name,
                  MAX(GO_LIVE_DATE)                                                         as go_live_date,
                  MAX(IMAGE_URL)                                                            as image_url,
                  MAX(
                      IFF(date = TO_CHAR(
                          LAST_DAY(CURRENT_SHOWROOM),
                          'YYYY-MM-DD'
                                 ), AVAILABLE_TO_SELL_QUANTITY, 0)
                  )                                                                         AS eom_inventory,
                  --calculated bop inventory = EOM inventory + pre cycle and m1 unit sales
                  MAX(
                      IFF(date = TO_CHAR(
                          LAST_DAY(CURRENT_SHOWROOM),
                          'YYYY-MM-DD'
                                 ), AVAILABLE_TO_SELL_QUANTITY, 0)
                  ) --eom inv
                      + SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= TO_CHAR(
                              LAST_DAY(CURRENT_SHOWROOM),
                              'YYYY-MM-DD'
                                                                ), unit_sales, 0)
                        ) --m1 unit sales
                                                                                            as calculated_bop_inventory,
                  MAX(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    and date <= current_showroom, AVAILABLE_TO_SELL_QUANTITY, 0)
                  )                                                                         AS start_pre_cycle_inventory,
                  MAX(
                      IFF(date = CURRENT_SHOWROOM, AVAILABLE_TO_SELL_QUANTITY, 0)
                  )                                                                         AS start_cycle_inventory,
                  MAX(
                      days_since_release_to_first_broken
                  )                                                                         AS days_to_break,
                  SUM(
                      CASE
                          WHEN date >= current_showroom
                              AND date <= DATEADD('day', 30, current_showroom) THEN product_discount_local_amount END
                  ) / SUM(
                      CASE
                          WHEN date >= current_showroom
                              AND date <= DATEADD('day', 30, current_showroom) THEN product_subtotal_local_amount END
                      )                                                                     AS avg_cash_discount,
                  SUM(
                      IFF(date >= DATEADD('day', -15, current_showroom)
                                                    AND date <= DATEADD('day', 30, current_showroom), ad_spend, 0)
                  )                                                                         AS ad_spend,
                  CASE
                      WHEN LOWER(
                               MAX(ITEM_RANK)
                           ) = 'a'
                          AND LOWER(ITEM_STATUS) = 'fashion' THEN $a_fashion_target_str
                      WHEN LOWER(
                               MAX(ITEM_RANK)
                           ) = 'b'
                          AND LOWER(ITEM_STATUS) = 'fashion' THEN $b_fashion_target_str
                      WHEN LOWER(
                               MAX(ITEM_RANK)
                           ) = 'c'
                          AND LOWER(ITEM_STATUS) = 'fashion' THEN $c_fashion_target_str END as m1_sell_through_target
           FROM REPORTING_BASE_PROD.FABLETICS.INVENTORY_HEALTH
           GROUP BY 1,
                    2,
                    3,
                    4,
                    5,
                    6),
     n AS (SELECT a.*,
                  CASE
                      WHEN a.calculated_bop_inventory <> 0
                          THEN a.total_unit_sales / a.calculated_bop_inventory END as total_sell_through,
                  --below is used as a flag for the low sell-through dashboard for Adam
                  --this feeds into REPORTING_MEDIA_PROD.dbo.sell_through_from_first_sr
                  IFF(na_ecomm_buy = 0
                                            OR (
                                  start_pre_cycle_inventory = 0
                              AND start_cycle_inventory = 0
                          )
                                            OR calculated_bop_inventory = 0, 1, 0) AS exception_flag_low_sell_thru_dash
           FROM a),
     b AS (SELECT *,
                  CASE
                      WHEN days_to_break <= $low_days_to_break THEN 'low'
                      WHEN days_to_break <= $high_days_to_break THEN 'expected'
                      ELSE 'high' END                              AS days_to_break_class,
                  IFF(eom_inventory <= $target_inv, 'low', 'high') AS inventory_class,
                  CASE
                      WHEN lower(item_status) ilike '%core%' THEN 'NA'
                      WHEN lower(item_status) ilike '%seasonal%' THEN 'NA'
                      WHEN lower(item_status) = 'fashion'
                          and total_sell_through < (
                              m1_sell_through_target - $str_range
                              ) THEN 'low'
                      WHEN lower(item_status) = 'fashion'
                          and total_sell_through > (
                              m1_sell_through_target + $str_range
                              ) THEN 'high'
                      ELSE 'expected' END                             AS sell_through_class,
                  --group jackets, onepieces and tops together in category_map to be used to calculate minimum unit sales threshold
                  IFF(CATEGORY = 'JACKET'
                                            OR CATEGORY = 'ONEPIECES', 'TOP', CATEGORY) AS CATEGORY_MAP,
                  --use this quarter calculation in the minimum units sales threshold calculation as well
                  --keep Nov separate
                  IFF(MONTH(CURRENT_SHOWROOM) = 11, 'NOV', TO_VARCHAR(
                      QUARTER(CURRENT_SHOWROOM)
                                                           )) AS showroom_quarter
           FROM n),
     c as (
         --calculate min sales threshold between bought too much and should not have bought using bottom 15th percentile among showroom quarter and category
         --group tops, onepieces, jackets to create larger group for percentile calc - they have most similar avg sales by product and showroom month
         SELECT gender,
                showroom_quarter,
                category_map,
                PERCENTILE_CONT(0.15) WITHIN GROUP (
                    ORDER BY
                    TOTAL_UNIT_SALES
                    ) AS MIN_UNIT_SALES_THRESHOLD
         FROM b
         WHERE total_unit_sales > 0
           AND ITEM_STATUS in ('FASHION', 'SEASONAL FASHION')
         GROUP BY 1,
                  2,
                  3),
     d AS (SELECT b.*,
                  c.MIN_UNIT_SALES_THRESHOLD,
                  --create buy recommendation based on sell-through and days to break
                  --flag exceptions where fashion skus have records in 2 successive showrooms
                  --this is built for fashion only
                  --when adding in seasonal, you will need to decide the logic to include here
                  --for seasonal, remove the exception for successive showrooms. you can do this by adding in item_status = 'fashion' into the case statement for that exception
                  --also flag products that started the month with 0 inventory. these will not be given a fair change to sell-through by EOM.
                  CASE
                      WHEN ss.has_showroom_one_month_apart = 1
                          THEN 'data may be skewed - product is in 2 successive showrooms'
                      WHEN na_ecomm_buy = 0
                          OR (
                                       start_pre_cycle_inventory = 0
                                   AND start_cycle_inventory = 0
                               )
                          OR calculated_bop_inventory = 0 THEN 'starting inventory or buy = 0'
                      WHEN sell_through_class = 'high'
                          OR (
                                       sell_through_class = 'expected'
                                   and days_to_break <= 21
                               ) THEN 'bought too little'
                      WHEN sell_through_class = 'expected' THEN 'bought well'
                      WHEN (
                                  sell_through_class = 'low'
                              and total_unit_sales >= MIN_UNIT_SALES_THRESHOLD
                          ) THEN 'bought too much'
                      WHEN (
                                  sell_through_class = 'low'
                              and total_unit_sales < MIN_UNIT_SALES_THRESHOLD
                          ) THEN 'should not have bought'
                      ELSE 'NULL' END AS buy_recommendation
           FROM b
                    LEFT JOIN _succ_srs ss ON b.PRODUCT_SKU = ss.product_sku
               AND b.CURRENT_SHOWROOM = ss.current_showroom
                    LEFT JOIN c ON b.showroom_quarter = c.showroom_quarter
               AND b.CATEGORY_MAP = c.CATEGORY_MAP
               AND b.GENDER = c.GENDER
           WHERE DATEDIFF(
                     DAY,
                     b.current_showroom,
                     CURRENT_DATE()
                 ) < (365 * 2))
SELECT DISTINCT *
FROM d
where CATEGORY not in (
                       'SWIMWEAR', 'SHOES', 'UNDERWEAR',
                       'PANTIES'
    )
  and current_showroom < DATE(
    TO_CHAR(
        DATE_TRUNC(
            'MONTH',
            CURRENT_DATE()
        ),
        'YYYY-MM-01'
    )
                         )
  and (
            lower(ITEM_STATUS) like '%seasonal%'
        or lower(ITEM_STATUS) like '%fashion%'
    );

--table to show first showroom by product sku
CREATE
    OR REPLACE TEMPORARY TABLE _first_sr AS
select product_sku,
       min(CURRENT_SHOWROOM) as first_showroom
from REPORTING_BASE_PROD.FABLETICS.SHOWROOM_QUALITY
group by 1
order by 1;

--table to filter products down to their first showroom only
--used to show sell-through by end of the first month that product was live
--goal is to flag really low sell-through products. in this case, less than 25% by EOM1
CREATE
    OR REPLACE TABLE REPORTING_MEDIA_PROD.dbo.sell_through_from_first_sr AS
select b.*,
       IFF(b.TOTAL_SELL_THROUGH <= 0.25, 1, 0) as less_than_25_pct_sell_thru
from _first_sr a
         left join REPORTING_BASE_PROD.FABLETICS.SHOWROOM_QUALITY b on a.product_sku = b.PRODUCT_SKU
    and a.first_showroom = b.CURRENT_SHOWROOM
where b.exception_flag_low_sell_thru_dash = 0
order by 1;

--to match cvr metrics to psource dashboard when QA'ing, must apply following filters:
--date range, definition = same session, country = us, membership status = lead, prospect, bundle component = false


