CREATE OR REPLACE TEMPORARY TABLE _max_showroom_ubt AS
    (SELECT DISTINCT ubt.*,
                     CASE
                         WHEN sub_brand = 'Yitty' THEN CONCAT(
                                 'https://fabletics-us-cdn.justfab.com/media/images/products/' ||
                                 ubt.sku || '/' || 'yitty_' ||
                                 ubt.sku || '-1_271x407.jpg'
                             )
                         ELSE CONCAT(
                                 'https://fabletics-us-cdn.justfab.com/media/images/products/' ||
                                 ubt.sku || '/' || ubt.sku ||
                                 '-1_271x407.jpg'
                             ) END AS image_url1
     FROM LAKE_VIEW.EXCEL.FL_MERCH_ITEMS_UBT ubt
              JOIN (SELECT sku,
                           MAX(current_showroom) AS max_current_showroom
                    FROM LAKE_VIEW.EXCEL.FL_MERCH_ITEMS_UBT
                    WHERE current_showroom <= CURRENT_DATE()
                    GROUP BY sku) AS ms ON ubt.sku = ms.sku
         AND ubt.current_showroom = ms.max_current_showroom);

CREATE OR REPLACE TEMPORARY TABLE _fabletics_warehouses as
select distinct sw.warehouse_id
from edw_prod.reference.store_warehouse sw
where STORE_BRAND = 'Fabletics';

create or replace temp table _valid_product as
select distinct dip.product_sku,
       dip.sku
from edw_prod.DATA_MODEL.fact_order_line fol
         join edw_prod.data_model.dim_item_price dip on fol.item_price_key = dip.item_price_key
         join edw_prod.DATA_MODEL.fact_order fo
              on fol.order_id = fo.order_id
         Join edw_prod.DATA_MODEL.dim_store ds on fol.store_id = ds.store_id
         join edw_prod.DATA_MODEL.dim_product_type dpt on fol.product_type_key = dpt.product_type_key
         join edw_prod.DATA_MODEL.DIM_ORDER_MEMBERSHIP_CLASSIFICATION mc
              on mc.order_membership_classification_key = fol.order_membership_classification_key
         join edw_prod.DATA_MODEL.DIM_ORDER_sales_channel doc on fol.order_sales_channel_key = doc.order_sales_channel_key
         Join edw_prod.DATA_MODEL.DIM_ORDER_status dos on fol.order_status_key = dos.order_status_key
         JOIN edw_prod.DATA_MODEL.DIM_ORDER_processing_status ops
              ON ops.order_processing_status_key = fo.order_processing_status_key
         JOIN edw_prod.DATA_MODEL.dim_order_line_status ols
              ON fol.order_line_status_key = ols.order_line_status_key
WHERE ds.store_brand in ('Fabletics', 'Yitty')
  and ds.store_country = 'US'
  and dpt.is_free ilike 'false'
  and doc.order_classification_L1 = 'Product Order'
  and dos.order_status IN ('Success', 'Pending')
  and ols.order_line_status != 'Cancelled'
  and dpt.product_type_name in ('Bundle Component', 'Normal')
  and fol.order_local_datetime::date >= '2017-01-01';

CREATE OR REPLACE TEMPORARY TABLE _inventory AS
SELECT sku,
       SUM(available_to_sell_quantity) AS available_inventory
FROM edw_prod.data_model.fact_inventory_history fih
         JOIN _fabletics_warehouses w ON fih.warehouse_id = w.warehouse_id
WHERE local_date = current_date - 1
GROUP BY sku;

CREATE OR REPLACE TEMPORARY TABLE _default_sku AS
SELECT product_sku,
       MAX(sku) AS sku
FROM edw_prod.data_model.dim_item_price
GROUP BY product_sku;

CREATE OR REPLACE TEMP TABLE _retail_unit_price AS
SELECT DISTINCT sku,
                retail_unit_price,
                description,
                size,
                is_plus_size
FROM (SELECT dip.sku           AS sku,
             dip.retail_unit_price,
             short_description AS description,
             size,
             is_plus_size,
             ROW_NUMBER() OVER (
                 PARTITION BY dip.sku
                 ORDER BY
                     dp.current_showroom_date DESC
                 )                rn
      FROM edw_prod.data_model.dim_item_price dip
               JOIN edw_prod.data_model.dim_product dp
                    ON dip.product_id = dp.product_id
               JOIN edw_prod.data_model.dim_store ds ON ds.store_id = dp.store_id
      WHERE store_brand IN ('Fabletics', 'Yitty')
        AND store_region = 'NA'
        AND product_status = 'Active')
WHERE rn = 1;

INSERT INTO _retail_unit_price (sku, retail_unit_price, description,
                                size, is_plus_size)
SELECT DISTINCT sku,
                retail_unit_price,
                description,
                size,
                is_plus_size
FROM (SELECT dip.sku           AS sku,
             dip.retail_unit_price,
             short_description AS description,
             dp.size,
             dp.is_plus_size,
             ROW_NUMBER() OVER (
                 PARTITION BY dip.sku
                 ORDER BY
                     dp.current_showroom_date DESC
                 )                rn
      FROM edw_prod.data_model.dim_item_price dip
               JOIN edw_prod.data_model.dim_product dp
                    ON dip.product_id = dp.product_id
               JOIN edw_prod.data_model.dim_store ds ON ds.store_id = dp.store_id
               LEFT JOIN _retail_unit_price p ON p.sku = dip.sku
      WHERE store_brand IN ('Fabletics', 'Yitty')
        AND store_region = 'NA'
        AND p.sku IS NULL)
WHERE rn = 1;

CREATE OR REPLACE TEMPORARY TABLE _product_feed AS
SELECT DISTINCT dip.sku                                                        AS ID,
                dip.product_sku                                                AS parent_id,
                'Placeholder!'                                                AS gtin,                      --oq edit
                initcap(ubt.current_name)                                     AS title,                     --oq edit
                r.description                                                 AS description,
                ubt.sub_brand                                                 AS brand,
                links.product_link                AS link,
                ubt.image_url1                                                 AS image_link,
                coalesce(
                    iff(available_inventory >= 100, 1, 0),
                    0
                    )                                                         AS is_available,
                r.retail_unit_price                                           AS price,
                NULL                                                          AS sale_price,
                NULL                                                          AS sale_start_date,
                NULL                                                          AS sale_end_date,
                CONCAT('cat-', case
                                   when ubt.gender ilike 'men%' then 'Men'
                                   else 'Women' end)                          AS category_level1_id,
                case when ubt.gender ilike 'men%' then 'Men' else 'Women' end AS category_level1_title,
                CONCAT('cat-', case when ubt.gender ilike 'men%' then 'Men' else 'Women' end, '-',
                       case
                           when lower(ubt.category) ILIKE 'dress%'
                               then 'Onepieces'
                           when lower(ubt.category) ILIKE 'bra%'
                               then 'Bra'
                           when lower(ubt.category) ILIKE '%jacket%'
                               then 'Jacket'
                           when lower(ubt.category) ILIKE '%bottom%'
                               then 'Bottom'
                           when lower(ubt.category) ILIKE '%top%'
                               then 'Top'
                           else initcap(ubt.category) end)                    AS category_level2_id,
                case
                    when lower(ubt.category) ILIKE 'dress%' then 'Onepieces'
                    when lower(ubt.category) ILIKE 'bra%' then 'Bra'
                    when lower(ubt.category) ILIKE '%jacket%' then 'Jacket'
                    when lower(ubt.category) ILIKE '%bottom%' then 'Bottom'
                    when lower(ubt.category) ILIKE '%top%' then 'Top'
                    else initcap(ubt.category) end                            as category_level2_title,     --clean UBT categories
                NULL                                                          AS arrival_date,
                'en_US'                                                       AS language_code,             --oq - placeholder for now. todo: add other languages as SF expands
                case when ubt.color is null then null else 'color' end        AS variant_attribute_1_name,
                ubt.color                                                     AS variant_attribute_1_value,
                color_family                                                  AS variant_attribute_1_group,
                NULL                                                          AS variant_attribute_1_swatch_url,
                'size'                                                        AS variant_attribute_2_name,
                CASE
                    WHEN r.size ILIKE 'XXXL%' THEN 'XXXL'
                    WHEN r.size ILIKE 'XXL%' THEN 'XXL'
                    WHEN r.size LIKE 'XL%' THEN 'XL'
                    WHEN r.size LIKE 'XS%' THEN 'XS'
                    WHEN r.size LIKE 'M%' THEN 'M'
                    WHEN r.size LIKE 'S%' THEN 'S'
                    WHEN r.size LIKE 'L%' THEN 'L'
                    WHEN r.size ILIKE 'XXS%' THEN 'XXS'
                    WHEN r.size ILIKE '1X%' THEN '1X'
                    WHEN r.size ILIKE '2X%' THEN '2X'
                    WHEN r.size ILIKE '3X%' THEN '3X'
                    WHEN r.size ILIKE '4X%' THEN '4X'
                    WHEN r.size ILIKE '5X%' THEN '5X'
                    WHEN r.size ILIKE '6X%' THEN '6X'
                    WHEN r.size ILIKE '%XXL' THEN 'XXL'
                    WHEN r.size LIKE '%XL' THEN 'XL'
                    WHEN r.size LIKE '%XS' THEN 'XS'
                    WHEN r.size LIKE '%M' THEN 'M'
                    WHEN r.size LIKE '%S' THEN 'S'
                    WHEN r.size LIKE '%L' THEN 'L'
                    WHEN r.size ILIKE '%XXS' THEN 'XXS'
                    WHEN r.size ILIKE '%1X' THEN '1X'
                    WHEN r.size ILIKE '%2X' THEN '2X'
                    WHEN r.size ILIKE '%3X' THEN '3X'
                    WHEN r.size ILIKE '%4X' THEN '4X'
                    WHEN r.size ILIKE '%5X' THEN '5X'
                    WHEN r.size ILIKE '%6X' THEN '6X' --WHEN dp.category ILIKE 'Shoes' THEN dp.size
                    ELSE 'other' END                                          AS variant_attribute_2_value,
                iff(
                        r.is_plus_size = 'FALSE', 'standard',
                        'plus_size'
                    )                                                         AS variant_attribute_2_group,
                NULL                                                          AS variant_attribute_2_swatch_url,
                case when fabric is null then 'NA' else 'material' end        AS variant_attribute_3_name,
                COALESCE(fabric, 'NA')                                        AS variant_attribute_3_value, --'NA' cleared w/ salesfloor
                COALESCE(fabric, 'NA')                                        AS variant_attribute_3_group,
                NULL                                                          AS variant_attribute_3_swatch_url,
                iff(d.sku is not null, 1, 0)                                  AS is_default
FROM edw_prod.data_model.dim_item_price dip
         JOIN edw_prod.data_model.dim_store ds on ds.store_id = dip.store_id
         JOIN _retail_unit_price r on r.sku = dip.sku
         LEFT JOIN _max_showroom_ubt ubt on ubt.sku = dip.product_sku
         LEFT JOIN _inventory inv on inv.sku = dip.sku
         LEFT JOIN _default_sku d on d.sku = dip.sku
         LEFT JOIN lake_consolidated_view.ultra_rollup.products_in_stock_product_feed links on links.PRODUCT_SKU = dip.PRODUCT_SKU
    and links.store_group_id = 16 --only pull US links!
WHERE store_brand IN ('Fabletics', 'Yitty')
  AND store_region = 'NA'
  and title is not null --cases where dim_product.sku is not found in UBT - Just filtering out for now - investigate if you think its worth the time/effort :)
  and price != 0
  and dip.product_sku in (select distinct product_sku from _valid_product)
and link is not null; --12 instances of product with price = 0

Truncate table reporting_prod.salesfloor.product_feed;
Insert into reporting_prod.salesfloor.product_feed
select * from _product_feed;
