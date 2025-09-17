ALTER SESSION SET TIMEZONE='America/Los_Angeles';

SET target_table = 'reporting_prod.salesfloor.transaction_feed';
SET current_datetime = current_timestamp();

MERGE INTO reporting_prod.public.meta_table_dependency_watermark AS t
    USING
        (
            SELECT
                'reporting_prod.salesfloor.transaction_feed' AS table_name,
                NULLIF(dependent_table_name,'reporting_prod.salesfloor.transaction_feed') AS dependent_table_name,
                high_watermark_datetime AS new_high_watermark_datetime
                FROM(
                        SELECT
                            'edw_prod.data_model.fact_order' AS dependent_table_name,
                            max(meta_update_datetime) AS high_watermark_datetime
                            FROM edw_prod.data_model.fact_order
                        UNION
                        SELECT
                            'edw_prod.data_model.fact_return_line' AS dependent_table_name,
                            max(meta_update_datetime) AS high_watermark_datetime
                            FROM edw_prod.data_model.fact_return_line
                    ) h

    ) AS s ON t.table_name = s.table_name
    AND equal_null(t.dependent_table_name, s.dependent_table_name)
WHEN MATCHED
    AND not equal_null(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
    THEN UPDATE
        SET t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = $current_datetime::timestamp_ltz(3)
WHEN NOT MATCHED
    THEN
    INSERT
        (
         table_name,
         dependent_table_name,
         high_watermark_datetime,
         new_high_watermark_datetime,
         meta_create_datetime
        )
    VALUES
        (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01'::timestamp_ltz,
        s.new_high_watermark_datetime,
        $current_datetime::timestamp_ltz(3)
        );

SET wm_edw_data_model_fact_order = reporting_prod.public.udf_get_watermark($target_table,'edw_prod.data_model.fact_order');
SET wm_edw_data_model_fact_return_line = reporting_prod.public.udf_get_watermark($target_table,'edw_prod.data_model.fact_return_line');

--customer base should pull from customer feed
CREATE OR REPLACE TEMPORARY TABLE _customer_base as
SELECT DISTINCT customer_id
FROM reporting_prod.SALESFLOOR.CUSTOMER_FEED;


CREATE OR REPLACE TEMPORARY TABLE
_max_showroom_ubt AS (
    SELECT DISTINCT ubt.*,
    CASE WHEN  sub_brand = 'Yitty'
                THEN CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.sku || '/' || 'yitty_'||ubt.sku ||'-1_271x407.jpg')
                ELSE CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.sku || '/' || ubt.sku ||'-1_271x407.jpg') END AS image_url1
              FROM LAKE_VIEW.EXCEL.FL_MERCH_ITEMS_UBT ubt
             JOIN (SELECT sku, MAX(current_showroom) AS max_current_showroom
                         FROM LAKE_VIEW.EXCEL.FL_MERCH_ITEMS_UBT
                         WHERE current_showroom <= CURRENT_DATE()
                         GROUP BY sku) AS ms
                        ON ubt.sku = ms.sku
                            AND ubt.current_showroom = ms.max_current_showroom
);

CREATE OR REPLACE TEMPORARY TABLE _transaction_feed AS
-----sale
SELECT
fo.order_id AS parent_id,
fo.order_id AS id,
'sale' AS type,
to_varchar(fo.order_local_datetime, 'yyyy-mm-dd hh:mi:ss') AS date,
fo.customer_id,
iff(store_retail_location_code = 'N/A', '0000', store_retail_location_code) AS store_id,
NULL AS fulfillment,
ds.STORE_CURRENCY as currency,
dp.sku,
ubt.color AS product_attribute_1,
CASE
   WHEN dp.size ILIKE 'XXXL%' THEN 'XXXL'
   WHEN dp.size ILIKE 'XXL%' THEN 'XXL'
   WHEN dp.size LIKE 'XL%' THEN 'XL'
   WHEN dp.size LIKE 'XS%' THEN 'XS'
   WHEN dp.size LIKE 'M%' THEN 'M'
   WHEN dp.size LIKE 'S%' THEN 'S'
   WHEN dp.size LIKE 'L%' THEN 'L'
   WHEN dp.size ILIKE 'XXS%' THEN 'XXS'
   WHEN dp.size ILIKE '1X%' THEN '1X'
   WHEN dp.size ILIKE '2X%' THEN '2X'
   WHEN dp.size ILIKE '3X%' THEN '3X'
   WHEN dp.size ILIKE '4X%' THEN '4X'
   WHEN dp.size ILIKE '5X%' THEN '5X'
   WHEN dp.size ILIKE '6X%' THEN '6X'
   WHEN dp.size ILIKE '%XXL' THEN 'XXL'
   WHEN dp.size LIKE '%XL' THEN 'XL'
   WHEN dp.size LIKE '%XS' THEN 'XS'
   WHEN dp.size LIKE '%M' THEN 'M'
   WHEN dp.size LIKE '%S' THEN 'S'
   WHEN dp.size LIKE '%L' THEN 'L'
   WHEN dp.size ILIKE '%XXS' THEN 'XXS'
   WHEN dp.size ILIKE '%1X' THEN '1X'
   WHEN dp.size ILIKE '%2X' THEN '2X'
   WHEN dp.size ILIKE '%3X' THEN '3X'
   WHEN dp.size ILIKE '%4X' THEN '4X'
   WHEN dp.size ILIKE '%5X' THEN '5X'
   WHEN dp.size ILIKE '%6X' THEN '6X'
   --WHEN dp.category ILIKE 'Shoes' THEN dp.size
   ELSE 'other'
   END  AS product_attribute_2,
price_offered_local_amount AS unit_price,
sum(fol.ITEM_QUANTITY) AS units,
NULL AS employee_id
FROM edw_prod.data_model.fact_order fo
JOIN _customer_base cb on cb.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_store ds on ds.store_id = fo.store_id
JOIN edw_prod.data_model.fact_order_line fol on fol.order_id = fo.order_id
JOIN edw_prod.data_model.dim_product dp on dp.product_id = fol.product_id
LEFT JOIN _max_showroom_ubt ubt ON ubt.sku = dp.product_sku
JOIN edw_prod.data_model.dim_currency dc on dc.currency_key = fo.currency_key
JOIN edw_prod.data_model.dim_order_status dos on dos.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_sales_channel dosc on dosc.order_sales_channel_key = fo.order_sales_channel_key
join edw_prod.data_model.dim_product_type dpt on fol.product_type_key = dpt.product_type_key
WHERE store_brand IN ('Fabletics','Yitty')
AND store_type IN ('Retail','Online','Mobile App')
AND store_region = 'NA'
AND order_status = 'Success'
AND order_classification_l1 = 'Product Order'
and dpt.product_type_name in ('Bundle Component', 'Normal')
AND fo.meta_update_datetime > $wm_edw_data_model_fact_order
group by 1,2,3,4,5,6,7,8,9,10,11,12

UNION
------ cancelled
SELECT
fo.order_id AS parent_id,
fo.order_id AS id,
'canceled' AS type,
to_varchar(fo.order_local_datetime, 'yyyy-mm-dd hh:mi:ss') AS date,
fo.customer_id,
iff(store_retail_location_code = 'N/A', '0000', store_retail_location_code) AS store_id,
NULL AS fulfillment,
ds.STORE_CURRENCY as currency,
dp.sku,
ubt.color AS product_attribute_1,
CASE
   WHEN dp.size ILIKE 'XXXL%' THEN 'XXXL'
   WHEN dp.size ILIKE 'XXL%' THEN 'XXL'
   WHEN dp.size LIKE 'XL%' THEN 'XL'
   WHEN dp.size LIKE 'XS%' THEN 'XS'
   WHEN dp.size LIKE 'M%' THEN 'M'
   WHEN dp.size LIKE 'S%' THEN 'S'
   WHEN dp.size LIKE 'L%' THEN 'L'
   WHEN dp.size ILIKE 'XXS%' THEN 'XXS'
   WHEN dp.size ILIKE '1X%' THEN '1X'
   WHEN dp.size ILIKE '2X%' THEN '2X'
   WHEN dp.size ILIKE '3X%' THEN '3X'
   WHEN dp.size ILIKE '4X%' THEN '4X'
   WHEN dp.size ILIKE '5X%' THEN '5X'
   WHEN dp.size ILIKE '6X%' THEN '6X'
   WHEN dp.size ILIKE '%XXL' THEN 'XXL'
   WHEN dp.size LIKE '%XL' THEN 'XL'
   WHEN dp.size LIKE '%XS' THEN 'XS'
   WHEN dp.size LIKE '%M' THEN 'M'
   WHEN dp.size LIKE '%S' THEN 'S'
   WHEN dp.size LIKE '%L' THEN 'L'
   WHEN dp.size ILIKE '%XXS' THEN 'XXS'
   WHEN dp.size ILIKE '%1X' THEN '1X'
   WHEN dp.size ILIKE '%2X' THEN '2X'
   WHEN dp.size ILIKE '%3X' THEN '3X'
   WHEN dp.size ILIKE '%4X' THEN '4X'
   WHEN dp.size ILIKE '%5X' THEN '5X'
   WHEN dp.size ILIKE '%6X' THEN '6X'
   WHEN dp.category ILIKE 'Shoes' THEN dp.size
   ELSE 'other'
   END  AS product_attribute_2,
price_offered_local_amount AS unit_price,
sum(fol.ITEM_QUANTITY) AS units,
NULL AS employee_id
FROM edw_prod.data_model.fact_order fo
JOIN _customer_base cb ON cb.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = fo.store_id
JOIN edw_prod.data_model.fact_order_line fol ON fol.order_id = fo.order_id
JOIN edw_prod.data_model.dim_product dp on dp.product_id = fol.product_id
LEFT JOIN _max_showroom_ubt ubt ON ubt.sku = dp.product_sku
JOIN edw_prod.data_model.dim_currency dc ON dc.currency_key = fo.currency_key
JOIN edw_prod.data_model.dim_order_status dos ON dos.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
join edw_prod.data_model.dim_product_type dpt on fol.product_type_key = dpt.product_type_key
WHERE store_brand IN ('Fabletics','Yitty')
AND store_type IN ('Retail','Online','Mobile App')
AND store_region = 'NA'
AND order_status = 'Cancelled'
AND order_classification_l1 = 'Product Order'
and dpt.product_type_name in ('Bundle Component', 'Normal')
AND fo.meta_update_datetime > $wm_edw_data_model_fact_order
group by 1,2,3,4,5,6,7,8,9,10,11,12

UNION
------ returned
SELECT
frl.order_id AS parent_id,
frl.return_id AS id,
'return' AS type,
to_varchar(frl.return_completion_local_datetime, 'yyyy-mm-dd hh:mi:ss') AS date,
frl.customer_id,
iff(store_retail_location_code = 'N/A', '0000', store_retail_location_code) AS store_id,
NULL AS fulfillment,
ds.STORE_CURRENCY as currency,
dp.sku,
ubt.color AS product_attribute_1,
CASE
   WHEN dp.size ILIKE 'XXXL%' THEN 'XXXL'
   WHEN dp.size ILIKE 'XXL%' THEN 'XXL'
   WHEN dp.size LIKE 'XL%' THEN 'XL'
   WHEN dp.size LIKE 'XS%' THEN 'XS'
   WHEN dp.size LIKE 'M%' THEN 'M'
   WHEN dp.size LIKE 'S%' THEN 'S'
   WHEN dp.size LIKE 'L%' THEN 'L'
   WHEN dp.size ILIKE 'XXS%' THEN 'XXS'
   WHEN dp.size ILIKE '1X%' THEN '1X'
   WHEN dp.size ILIKE '2X%' THEN '2X'
   WHEN dp.size ILIKE '3X%' THEN '3X'
   WHEN dp.size ILIKE '4X%' THEN '4X'
   WHEN dp.size ILIKE '5X%' THEN '5X'
   WHEN dp.size ILIKE '6X%' THEN '6X'
   WHEN dp.size ILIKE '%XXL' THEN 'XXL'
   WHEN dp.size LIKE '%XL' THEN 'XL'
   WHEN dp.size LIKE '%XS' THEN 'XS'
   WHEN dp.size LIKE '%M' THEN 'M'
   WHEN dp.size LIKE '%S' THEN 'S'
   WHEN dp.size LIKE '%L' THEN 'L'
   WHEN dp.size ILIKE '%XXS' THEN 'XXS'
   WHEN dp.size ILIKE '%1X' THEN '1X'
   WHEN dp.size ILIKE '%2X' THEN '2X'
   WHEN dp.size ILIKE '%3X' THEN '3X'
   WHEN dp.size ILIKE '%4X' THEN '4X'
   WHEN dp.size ILIKE '%5X' THEN '5X'
   WHEN dp.size ILIKE '%6X' THEN '6X'
   WHEN dp.category ILIKE 'Shoes' THEN dp.size
   ELSE 'other'
   END  AS product_attribute_2,
price_offered_local_amount AS unit_price,
sum(frl.RETURN_ITEM_QUANTITY) AS units,
NULL AS employee_id
FROM edw_prod.data_model.fact_return_line frl
JOIN _customer_base cb ON cb.customer_id = frl.customer_id
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = frl.store_id
JOIN edw_prod.data_model.fact_order fo ON fo.order_id = frl.order_id
JOIN edw_prod.data_model.fact_order_line fol ON fol.order_id = frl.order_id
JOIN edw_prod.data_model.dim_product dp on dp.product_id = fol.product_id
LEFT JOIN _max_showroom_ubt ubt ON ubt.sku = dp.product_sku
JOIN edw_prod.data_model.dim_return_status drs ON drs.return_status_key = frl.return_status_key
join edw_prod.data_model.dim_product_type dpt on fol.product_type_key = dpt.product_type_key
WHERE store_brand IN ('Fabletics','Yitty')
AND store_type IN ('Retail','Online','Mobile App')
AND store_region = 'NA'
AND return_status = 'Resolved'
and dpt.product_type_name in ('Bundle Component', 'Normal')
AND frl.meta_update_datetime > $wm_edw_data_model_fact_return_line
group by 1,2,3,4,5,6,7,8,9,10,11,12;


ALTER TABLE _transaction_feed ADD COLUMN meta_row_hash INT;


UPDATE _transaction_feed
SET meta_row_hash = HASH(parent_id, id, type, date, customer_id, store_id, fulfillment, currency,
    sku, product_attribute_1, product_attribute_2, unit_price, units, employee_id);


update reporting_prod.salesfloor.transaction_feed t
set t.parent_id = s.parent_id,
    t.id = s.id,
    t.type = s.type,
    t.date = s.date,
    t.customer_id = s.customer_id,
    t.store_id = s.store_id,
    t.fulfillment = s.fulfillment,
    t.currency = s.currency,
    t.sku = s.sku,
    t.product_attribute_1 = s.product_attribute_1,
    t.product_attribute_2 = s.product_attribute_2,
    t.unit_price = s.unit_price,
    t.units = s.units,
    t.employee_id = s.employee_id,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = $current_datetime
from _transaction_feed s
where t.meta_row_hash != s.meta_row_hash
    and equal_null(t.date, s.date)
    and equal_null(t.sku, s.sku)
    and equal_null(t.product_attribute_1, s.product_attribute_1)
    and equal_null(t.product_attribute_2, s.product_attribute_2)
;

insert into reporting_prod.salesfloor.transaction_feed(
        parent_id,
        id,
        type,
        date,
        customer_id,
        store_id,
        fulfillment,
        currency,
        sku,
        product_attribute_1,
        product_attribute_2,
        unit_price,
        units,
        employee_id,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
        )
SELECT
    s.parent_id,
    s.id,
    s.type,
    s.date,
    s.customer_id,
    s.store_id,
    s.fulfillment,
    s.currency,
    s.sku,
    s.product_attribute_1,
    s.product_attribute_2,
    s.unit_price,
    s.units,
    s.employee_id,
    s.meta_row_hash,
    $current_datetime,
    $current_datetime
FROM _transaction_feed s
LEFT JOIN reporting_prod.salesfloor.transaction_feed t ON equal_null(t.date, s.date)
                                            AND equal_null(t.sku, s.sku)
                                            AND equal_null(t.product_attribute_1, s.product_attribute_1)
                                            AND equal_null(t.product_attribute_2, s.product_attribute_2)
WHERE COALESCE(t.date, t.sku, t.product_attribute_1, t.product_attribute_2) IS NULL;

UPDATE reporting_prod.public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = $current_datetime::timestamp_ltz(3)
WHERE table_name = $target_table;
