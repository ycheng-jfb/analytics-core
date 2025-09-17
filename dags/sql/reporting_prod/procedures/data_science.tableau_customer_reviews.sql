-- Get reviews within last 3 years
CREATE OR REPLACE TEMPORARY TABLE _new_reviews_dup AS
SELECT st.STORE_BRAND, st.STORE_BRAND_ABBR, st.STORE_COUNTRY as country, st.STORE_REGION as region,
    r.review_id, r.customer_id, r.review_template_id, r.datetime_added, r.statuscode, r.order_id,
    r.product_id, r.body, r.recommended, r.datetime_added AS review_datetime, sc.label AS review_status,
    ol.ORDER_LINE_ID, dp.product_name, dp.product_sku, dp.sku, dp.color, dp.HEEL_HEIGHT, dp.CURRENT_SHOWROOM_DATE,dp.size,
    COALESCE(plus.size_type,'Unknown') as size_type, dp.department, dp.category, dp.vip_unit_price,
    dp.GROUP_CODE, po.WAREHOUSE_ID, po.PO_NUMBER, rtrim(po.VENDOR_ID) as vendor_name,
    rtrim(po.OFFICIAL_FACTORY_NAME) as official_factory_name, dp.PRODUCT_STATUS,
    frl.RETURN_LINE_ID, ol.lpn_code, po.show_room as po_show_room_date, rtrim(po.plm_style) as plm_style,
    row_number() over (partition by REVIEW_ID order by ol.ORDER_LINE_ID desc, frl.RETURN_LINE_ID desc) as order_line_dedup_rank
FROM lake_consolidated_view.ultra_merchant.review r
JOIN LAKE_CONSOLIDATED_VIEW.ultra_merchant.statuscode sc ON sc.statuscode = r.statuscode
JOIN lake_consolidated_view.ultra_merchant.customer c ON c.customer_id = r.customer_id
JOIN EDW_prod.DATA_MODEL.DIM_STORE st on st.STORE_ID = c.STORE_ID
left join LAKE_CONSOLIDATED.ULTRA_MERCHANT.order_line ol on ol.ORDER_ID=r.ORDER_ID and ol.PRODUCT_ID=r.PRODUCT_ID
left join EDW_prod.DATA_MODEL.DIM_PRODUCT DP ON DP.PRODUCT_ID = r.product_id
left JOIN LAKE_VIEW.SHAREPOINT.MED_PLUS_REG_SIZE_MAPPING plus ON lower(plus.size) = lower(dp.size) and plus.store_brand_name_abbr = st.store_brand_abbr
left join lake_view.ultra_warehouse.lpn l on ol.lpn_code = l.lpn_code
left join lake_view.ultra_warehouse.receipt re on l.receipt_id = re.receipt_id
left join lake_consolidated_view.ultra_merchant.product p on r.product_id = p.product_id
left join lake_consolidated_view.ultra_merchant.item i on p.item_id = i.item_id
left join reporting_prod.gsc.po_detail_dataset po on re.po_number = po.po_number and i.item_number = po.sku
left join edw_prod.DATA_MODEL.FACT_RETURN_LINE frl on frl.ORDER_LINE_ID = ol.ORDER_LINE_ID
WHERE r.datetime_added >= dateadd(year, -3, current_timestamp())::DATE AND st.STORE_REGION = 'NA';

-- dedup because of multiple order lines for same product_id, return, and other duplicates like in reporting_prod.gsc.po_detail_dataset
create or replace temp table _new_reviews as select * from _new_reviews_dup
where (order_line_dedup_rank is null or order_line_dedup_rank=1);

-- review answers pivoted
CREATE OR REPLACE TEMPORARY TABLE _review_answers AS
SELECT pivottable.review_id, "'Overall'" AS overall_rating, "'Quality & Value'" AS quality_value_rating,
    "'Style'" AS style_rating, "'Size Fit'" AS size_fit_rating, "'Comfort'" AS comfort_rating
FROM ( SELECT r.review_id, rfg.label AS review_question, rtfa_s.score AS review_score FROM _new_reviews r
    LEFT JOIN LAKE_CONSOLIDATED_VIEW.ultra_merchant.review_template_field rtf_s ON rtf_s.review_template_id = r.review_template_id
        AND edw_prod.STG.UDF_UNCONCAT_BRAND(rtf_s.review_template_field_group_id) IN (1, 2, 3, 4, 5)
    LEFT JOIN LAKE_CONSOLIDATED_VIEW.ultra_merchant.review_template_field_group rfg
              ON rfg.review_template_field_group_id = edw_prod.STG.UDF_UNCONCAT_BRAND(rtf_s.review_template_field_group_id)
    LEFT JOIN LAKE_CONSOLIDATED_VIEW.ultra_merchant.review_field_answer AS rfa_s ON rfa_s.review_id = r.review_id
        AND rfa_s.review_template_field_id = rtf_s.review_template_field_id
    LEFT JOIN LAKE_CONSOLIDATED_VIEW.ultra_merchant.review_template_field_answer AS rtfa_s
        ON rtfa_s.review_template_field_answer_id = rfa_s.review_template_field_answer_id
) AS src
    PIVOT(MAX(review_score) FOR review_question IN ('Comfort','Style','Quality & Value','Size Fit','Overall') ) AS pivottable;

-- Review title: set up to deal with very rare duplicates for SX
CREATE OR REPLACE TEMPORARY TABLE _review_title AS
SELECT r.review_id, rfa_s.value AS review_title,
    dense_rank() over (partition by rfa_s.REVIEW_ID, rfa_s.REVIEW_TEMPLATE_FIELD_ID order by DATETIME_MODIFIED desc) as title_dedup_rank
FROM _new_reviews r
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ultra_merchant.review_template_field rtf_s ON rtf_s.review_template_id = r.review_template_id
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ultra_merchant.review_field_answer AS rfa_s ON rfa_s.review_id = r.review_id
    AND rfa_s.review_template_field_id = rtf_s.review_template_field_id
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ultra_merchant.review_template_field_answer AS rtfa_s ON rtfa_s.review_template_field_answer_id = rfa_s.review_template_field_answer_id
WHERE rtf_s.label IN ('One-Line Review', 'Write Your Review');

-- Final output
CREATE OR REPLACE TRANSIENT TABLE REPORTING_PROD.DATA_SCIENCE.TABLEAU_CUSTOMER_REVIEWS AS
-- create or replace temporary table _test as
SELECT n.STORE_BRAND as STORE_BRAND_NAME, n.country, n.region, n.review_id, n.customer_id, n.order_id, n.product_id,
    n.product_name, n.sku, n.size, n.size_type, n.color, n.heel_height, n.CURRENT_SHOWROOM_DATE as SHOWROOM_DATE,
    n.department, n.category, n.vip_unit_price, rt.review_title, n.body AS review_text, n.recommended, n.review_status,
    p.overall_rating, p.quality_value_rating, p.style_rating, p.size_fit_rating, p.comfort_rating, n.review_datetime,
    n.order_line_id, n.GROUP_CODE, n.WAREHOUSE_ID, n.PO_NUMBER, n.po_show_room_date, n.vendor_name, n.OFFICIAL_FACTORY_NAME,
    n.plm_style, n.PRODUCT_STATUS, n.RETURN_LINE_ID, lpn_code,
    CURRENT_TIMESTAMP() AS meta_create_datetime
FROM _new_reviews n
left JOIN _review_answers p ON n.review_id = p.review_id
LEFT JOIN _review_title rt ON rt.review_id = n.review_id
where (rt.title_dedup_rank is null or rt.title_dedup_rank=1);
