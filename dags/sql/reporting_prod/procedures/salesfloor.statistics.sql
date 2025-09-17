--note- schedule this query to always run AFTER customer feed

ALTER SESSION SET TIMEZONE='America/Los_Angeles';

SET target_table = 'reporting_prod.salesfloor.statistics_feed';
SET current_datetime = current_timestamp();


MERGE INTO reporting_prod.public.meta_table_dependency_watermark AS t USING (
    SELECT
        'reporting_prod.salesfloor.statistics_feed' AS table_name,
        NULLIF(
            dependent_table_name,
            'reporting_prod.salesfloor.statistics_feed'
        ) AS dependent_table_name,
        high_watermark_datetime AS new_high_watermark_datetime
    FROM
(
            SELECT
                'reporting_prod.SALESFLOOR.CUSTOMER_FEED' AS dependent_table_name,
                max(meta_update_datetime) AS high_watermark_datetime
            FROM
                reporting_prod.SALESFLOOR.CUSTOMER_FEED
        ) h
) AS s ON t.table_name = s.table_name
AND equal_null(t.dependent_table_name, s.dependent_table_name)
WHEN MATCHED
AND not equal_null(
    t.new_high_watermark_datetime,
    s.new_high_watermark_datetime
) THEN
UPDATE
SET
    t.new_high_watermark_datetime = s.new_high_watermark_datetime,
    t.meta_update_datetime = $current_datetime::timestamp_ltz(3)
    WHEN NOT MATCHED THEN
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

SET wm_reporting_prod_salesfloor_customer_feed = reporting_prod.public.udf_get_watermark($target_table,'reporting_prod.SALESFLOOR.CUSTOMER_FEED');


CREATE OR REPLACE TEMPORARY TABLE _customer_base as
SELECT DISTINCT customer_id
FROM reporting_prod.SALESFLOOR.CUSTOMER_FEED
where META_UPDATE_DATETIME >= $wm_reporting_prod_salesfloor_customer_feed
;

CREATE OR REPLACE TEMPORARY TABLE _membership_type as
SELECT cb.customer_id
,'1' AS panel_id
,'Membership' AS panel_label
,'p1d2' AS datapoint_id
,'Membership Type' AS datapoint_label
,'plain_text' AS datapoint_type
, ifnull(membership_state, 'NA')::string AS datapoint_value
, '1' AS section_id
, 'Membership' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN edw_prod.data_model.fact_membership_event fme on cb.customer_id = fme.customer_id
AND fme.is_current = 'TRUE';

CREATE OR REPLACE TEMPORARY TABLE _loyalty_status as
SELECT cb.customer_id
,'1' AS panel_id
,'Membership' AS panel_label
,'p1d3' AS datapoint_id
,'Loyalty Status' AS datapoint_label
,'plain_text' AS datapoint_type
, ifnull(membership_reward_tier, 'None')::string AS datapoint_value
, '1' AS section_id
, 'Membership' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM  _customer_base cb
LEFT JOIN edw_prod.data_model.fact_activation_loyalty_tier falt ON cb.customer_id = falt.customer_id AND falt.is_current = 'TRUE';


CREATE OR REPLACE TEMP TABLE _membership_ids AS
SELECT DISTINCT cdh.customer_id,cdh.membership_id
FROM edw_prod.data_model.dim_customer_detail_history cdh
join _customer_base cb on cdh.customer_id = cb.customer_id
;


CREATE OR REPLACE TEMP TABLE _active_membership_tokens AS
SELECT cb.customer_id
,'1' AS panel_id
,'Membership' AS panel_label
,'p1d4' AS datapoint_id
,'Member Tokens' AS datapoint_label
,'number' AS datapoint_type
, SUM(IFF(s.label= 'Active',1,0))::string AS datapoint_value
, '1' AS section_id
, 'Membership' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
JOIN _membership_ids m ON cb.customer_id = m.customer_id
LEFT JOIN LAKE_CONSOLIDATED.ULTRA_MERCHANT.MEMBERSHIP_TOKEN sc ON m.membership_id = sc.membership_id
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STATUSCODE S ON S.STATUSCODE = SC.STATUSCODE
GROUP BY cb.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _vip_tenure as
SELECT cb.customer_id
,'1' AS panel_id
,'Membership' AS panel_label
,'p1d5' AS datapoint_id
,'VIP Tenure(Months)' AS datapoint_label
,'number' AS datapoint_type
, ifnull(iff(fme.recent_activation_local_datetime ='1900-01-01 00:00:00.000 -0800',0,DATEDIFF('MONTH', date_trunc('month',fme.recent_activation_local_datetime), date_trunc('month',current_date)) + 1), 0)::string AS datapoint_value
, '1' AS section_id
, 'Membership' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN edw_prod.data_model.fact_membership_event fme ON fme.customer_id = cb.customer_id
AND fme.is_current = 'TRUE';


CREATE OR REPLACE TEMPORARY TABLE _anniversary_date as
SELECT cb.customer_id
,'1' AS panel_id
,'Membership' AS panel_label
,'p1d6' AS datapoint_id
,'Anniversary Date' AS datapoint_label
,'date' AS datapoint_type
, ifnull(to_varchar(fme.recent_activation_local_datetime, 'yyyy-mm-dd hh:mi:ss'), 'N/A') AS datapoint_value
, '1' AS section_id
, 'Membership' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN edw_prod.data_model.fact_membership_event fme ON fme.customer_id = cb.customer_id
AND fme.is_current = 'TRUE'
and fme.RECENT_ACTIVATION_LOCAL_DATETIME is not null;


CREATE OR REPLACE TEMPORARY TABLE _lead_tenure as
SELECT cb.customer_id
,'1' AS panel_id
,'Membership' AS panel_label
,'p1d7' AS datapoint_id
,'Lead Tenure(Days)' AS datapoint_label
,'number' AS datapoint_type
, iff(membership_state = 'Lead',DATEDIFF('DAY',fme.EVENT_START_LOCAL_DATETIME,current_date),0)::string AS datapoint_value
, '1' AS section_id
, 'Membership' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN edw_prod.data_model.fact_membership_event fme ON fme.customer_id = cb.customer_id
AND fme.is_current = 'TRUE';

CREATE OR REPLACE TEMPORARY TABLE _city as
SELECT cb.customer_id
,'1' AS panel_id
,'Membership' AS panel_label
,'p1d9' AS datapoint_id
,'City' AS datapoint_label
,'plain_text' AS datapoint_type
, default_city AS datapoint_value
, '1' AS section_id
, 'Membership' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
JOIN edw_prod.data_model.dim_customer dc ON dc.customer_id = cb.customer_id;


CREATE OR REPLACE TEMP TABLE _active_store_credits AS
SELECT cb.customer_id
,'1' AS panel_id
,'Membership' AS panel_label
,'p1d8' AS datapoint_id
,'Store Credits' AS datapoint_label
,'number' AS datapoint_type
, SUM(IFF(s.label= 'Active',1,0))::string AS datapoint_value
, '1' AS section_id
, 'Membership' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STORE_CREDIT sc ON cb.customer_id = sc.customer_id
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STATUSCODE S ON S.STATUSCODE = SC.STATUSCODE
GROUP BY cb.customer_id;


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



CREATE OR REPLACE TEMPORARY TABLE _unit_count as
SELECT cb.customer_id
,ubt.category
,ubt.gender AS gender
,store_type
,ubt.sub_brand
,SUM(item_quantity) AS units
FROM _customer_base cb
JOIN edw_prod.data_model.fact_order fo ON cb.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = fo.store_id
JOIN edw_prod.data_model.fact_order_line fol ON fol.order_id = fo.order_id
JOIN edw_prod.data_model.dim_product dp ON dp.product_id = fol.product_id
LEFT JOIN _max_showroom_ubt ubt ON ubt.sku = dp.product_sku
JOIN edw_prod.data_model.dim_currency dc ON dc.currency_key = fo.currency_key
JOIN edw_prod.data_model.dim_order_status dos ON dos.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
WHERE order_classification_l1 = 'Product Order'
AND store_brand IN ('Fabletics','Yitty')
AND store_type IN ('Retail','Online','Mobile App')
AND store_region = 'NA'
AND order_status = 'Success' --todo - look into case when one line item in an order is returned/cancelled - does that change status of order?
GROUP BY cb.customer_id,ubt.category,ubt.gender,store_type,ubt.sub_brand;


CREATE OR REPLACE TEMPORARY TABLE _order_data as
SELECT  cb.customer_id
,count(order_id) as order_count
     , sum(SUBTOTAL_EXCL_TARIFF_LOCAL_AMOUNT) + sum(TARIFF_REVENUE_LOCAL_AMOUNT)- sum(PRODUCT_DISCOUNT_LOCAL_AMOUNT)+sum(SHIPPING_REVENUE_BEFORE_DISCOUNT_LOCAL_AMOUNT)-sum(SHIPPING_DISCOUNT_LOCAL_AMOUNT)-sum(NON_CASH_CREDIT_LOCAL_AMOUNT) as product_gross_revenue
--,sum(product_gross_revenue_local_amount) AS product_gross_revenue
FROM _customer_base cb
JOIN edw_prod.data_model.fact_order fo ON cb.customer_id = fo.customer_id
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = fo.store_id
JOIN edw_prod.data_model.dim_order_status dos ON dos.order_status_key = fo.order_status_key
JOIN edw_prod.data_model.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
WHERE order_classification_l1 = 'Product Order'
AND store_brand IN ('Fabletics','Yitty')
AND store_type IN ('Retail','Online','Mobile App')
AND store_region = 'NA'
AND order_status = 'Success'
GROUP BY cb.customer_id;

CREATE OR REPLACE TEMP TABLE _AOV AS
SELECT cb.customer_id
,'1' AS panel_id
,'Membership' AS panel_label
,'p1d10' AS datapoint_id
,'AOV' AS datapoint_label
,'currency' AS datapoint_type
, ifnull((SUM(product_gross_revenue)/sum(order_count)), 0)::int::string AS datapoint_value
, '1' AS section_id
, 'Membership' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN _order_data o on cb.customer_id = o.customer_id
GROUP BY cb.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _category_level_unit_count AS
SELECT cb.customer_id
,ubt.category
,SUM(iff(order_classification_l1 = 'Product Order',item_quantity,0)) AS units
,row_number() over (partition by cb.customer_id order by units desc) AS rank
FROM _customer_base cb
LEFT JOIN edw_prod.data_model.fact_order fo ON cb.customer_id = fo.customer_id
LEFT JOIN edw_prod.data_model.dim_store ds ON ds.store_id = fo.store_id
AND store_brand IN ('Fabletics','Yitty')
AND store_type IN ('Retail','Online','Mobile App')
AND store_region = 'NA'
LEFT JOIN edw_prod.data_model.fact_order_line fol ON fol.order_id = fo.order_id
LEFT JOIN edw_prod.data_model.dim_product dp ON dp.product_id = fol.product_id
LEFT JOIN _max_showroom_ubt ubt ON ubt.sku = dp.product_sku
LEFT JOIN edw_prod.data_model.dim_currency dc ON dc.currency_key = fo.currency_key
LEFT JOIN edw_prod.data_model.dim_order_status dos ON dos.order_status_key = fo.order_status_key AND order_status = 'Success'
LEFT JOIN edw_prod.data_model.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
AND order_classification_l1 = 'Product Order'
GROUP BY cb.customer_id,ubt.category ;

CREATE OR REPLACE TEMPORARY TABLE _category_final as
SELECT *
,SUM(units) over (partition by customer_id) AS total_units
,iff(total_units=0, 0, (units/total_units) *100)::string AS data_point_value
,max(rank) over (partition by customer_id) as max_rank
FROM _category_level_unit_count;

CREATE OR REPLACE TEMPORARY TABLE _missing_rank_customers as
select *
from _category_final
where max_rank in (1,2);

CREATE OR REPLACE TEMPORARY TABLE _rank_base (CATEGORY varchar(10),UNITS varchar(10),RANK varchar(10),TOTAL_UNITS varchar(10),DATA_POINT_VALUE varchar(10),MAX_RANK varchar(10));
INSERT INTO _rank_base values (null,null,1,null,null,3);
INSERT INTO _rank_base values (null,null,2,null,null,3);
INSERT INTO _rank_base values (null,null,3,null,null,3);


CREATE OR REPLACE TEMPORARY TABLE _customer_rank_base as
select *
from _rank_base
cross join (select distinct customer_id from _missing_rank_customers);

INSERT INTO _category_final
SELECT r.customer_id ,r.category ,r.units,r.rank,r.total_units,r.data_point_value,r.max_rank
FROM _customer_rank_base r
LEFT JOIN _missing_rank_customers c ON r.customer_id = c.customer_id  AND r.rank = c.rank
WHERE c.rank is null;


CREATE OR REPLACE TEMPORARY TABLE _category AS
SELECT customer_id
,'2' AS panel_id
,'Categories' AS panel_label
,'p2d' AS datapoint_id
,coalesce(category, 'NA') AS datapoint_label --oq edit - also targeting leads(haven't made a purchase) so need to be able to support
,'graph' AS datapoint_type
, zeroifnull(data_point_value)::string as data_point_value
,'2' AS section_id
,'Spend Behavior' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _category_final
WHERE rank IN (1,2,3);


CREATE OR REPLACE TEMPORARY TABLE _womens_products as
SELECT cb.customer_id
,'3' AS panel_id
,'Product Gender' AS panel_label
,'p3d' AS datapoint_id
, 'Female' AS datapoint_label
,'graph' AS datapoint_type
, zeroifnull((sum(iff(gender not like 'MEN%',units,0))/ sum(units))*100)::int::string as data_point_value
,'2' AS section_id
,'Spend Behavior' AS section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN _unit_count uc on uc.customer_id = cb.customer_id
GROUP BY cb.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _mens_products as
select cb.customer_id
,'3' as panel_id
,'Product Gender' as panel_label
,'p3d' as datapoint_id
, 'Male' as datapoint_label
,'graph' as datapoint_type
, zeroifnull((sum(iff(gender ilike 'MEN%',units,0))/ sum(units))*100)::int::string as data_point_value
,'2' as section_id
,'Spend Behavior' as section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN _unit_count uc on uc.customer_id = cb.customer_id
GROUP BY cb.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _Unknown_gender_products as
select cb.customer_id
,'3' as panel_id
,'Product Gender' as panel_label
,'p3d' as datapoint_id
,'Unknown_gender' as datapoint_label
,'graph' as datapoint_type
, zeroifnull((sum(iff(gender is null,units,0))/ sum(units))*100)::int::string as data_point_value
,'2' as section_id
,'Spend Behavior' as section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN _unit_count uc on uc.customer_id = cb.customer_id
GROUP BY cb.customer_id;


CREATE OR REPLACE TEMPORARY TABLE _fl_products as
select cb.customer_id
,'4' as panel_id
,'Product Brand' as panel_label
,'p4d' as datapoint_id
,'Fabletics' as datapoint_label
,'graph' as datapoint_type
, zeroifnull((sum(iff(sub_brand = 'Fabletics' ,units,0))/ sum(units))*100)::int::string as data_point_value
,'2' as section_id
,'Spend Behavior' as section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN _unit_count uc on uc.customer_id = cb.customer_id
GROUP BY cb.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _yitty_products as
select cb.customer_id
,'4' as panel_id
,'Product Brand' as panel_label
,'p4d' as datapoint_id
,'Yitty' as datapoint_label
,'graph' as datapoint_type
,zeroifnull((sum(iff(sub_brand = 'Yitty',units,0))/ sum(units))*100)::int::string as data_point_value
,'2' as section_id
,'Spend Behavior' as section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN _unit_count uc on uc.customer_id = cb.customer_id
GROUP BY cb.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _unknown_brand_products as
select cb.customer_id
,'4' as panel_id
,'Product Brand' as panel_label
,'p4d' as datapoint_id
,'Unknown_brand' as datapoint_label
,'graph' as datapoint_type
,zeroifnull((sum(iff(sub_brand is null ,units,0))/ sum(units))*100)::int::string as data_point_value
,'2' as section_id
,'Spend Behavior' as section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN _unit_count uc on uc.customer_id = cb.customer_id
GROUP BY cb.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _retail_orders as
select cb.customer_id
,'5' as panel_id
,'Store Type' as panel_label
,'p6d' as datapoint_id
,'Retail' as datapoint_label
,'graph' as datapoint_type
,zeroifnull((sum(iff(store_type ='Retail',units,0))/ sum(units))*100)::int::string as data_point_value
,'2' as section_id
,'Spend Behavior' as section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN _unit_count uc on uc.customer_id = cb.customer_id
GROUP BY cb.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _online_orders as
select cb.customer_id
,'5' as panel_id
,'Store Type' as panel_label
,'p6d' as datapoint_id
,'Online' as datapoint_label
,'graph' as datapoint_type
,zeroifnull((sum(iff(store_type ='Online',units,0))/ sum(units))*100)::int::string as data_point_value
,'2' as section_id
,'Spend Behavior' as section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN _unit_count uc on uc.customer_id = cb.customer_id
GROUP BY cb.customer_id;

CREATE OR REPLACE TEMPORARY TABLE _mobile_orders as
select cb.customer_id
,'5' as panel_id
,'Store Type' as panel_label
,'p6d' as datapoint_id
,'Mobile' as datapoint_label
,'graph' as datapoint_type
,zeroifnull((sum(iff(store_type ='Mobile App',units,0))/ sum(units))*100)::int::string as data_point_value
,'2' as section_id
,'Spend Behavior' as section_label
, '' AS section_sequence
, '' AS panel_sequence
, '' AS datapoint_sequence
FROM _customer_base cb
LEFT JOIN _unit_count uc on uc.customer_id = cb.customer_id
GROUP BY cb.customer_id;

CREATE OR REPLACE transient TABLE _final as
SELECT * FROM _membership_type
UNION ALL
SELECT * FROM _loyalty_status
UNION ALL
SELECT * FROM _active_membership_tokens
UNION ALL
SELECT * FROM _vip_tenure
UNION ALL
SELECT * FROM _anniversary_date
UNION ALL
SELECT * FROM _lead_tenure
UNION ALL
SELECT * FROM _city
UNION ALL
SELECT * FROM _active_store_credits
UNION ALL
SELECT * FROM _AOV
UNION ALL
SELECT * FROM _category
UNION ALL
SELECT * FROM _womens_products
UNION ALL
SELECT * FROM _mens_products
UNION ALL
SELECT * FROM _Unknown_gender_products
UNION ALL
SELECT * FROM _fl_products
UNION ALL
SELECT * FROM _yitty_products
UNION ALL
SELECT * FROM _unknown_brand_products
UNION ALL
SELECT * FROM _retail_orders
UNION ALL
SELECT * FROM _online_orders
UNION ALL
SELECT * FROM _mobile_orders;


CREATE OR REPLACE TEMPORARY TABLE _statistics_feed as
select * from _final
order by customer_id,datapoint_id;

ALTER TABLE _statistics_feed ADD COLUMN meta_row_hash INT;

UPDATE _statistics_feed
SET meta_row_hash = HASH(CUSTOMER_ID,
PANEL_ID,
PANEL_LABEL,
DATAPOINT_ID,
DATAPOINT_LABEL,
DATAPOINT_TYPE,
DATAPOINT_VALUE,
SECTION_ID,
SECTION_LABEL,
SECTION_SEQUENCE,
PANEL_SEQUENCE,
DATAPOINT_SEQUENCE);

MERGE INTO reporting_prod.salesfloor.statistics_feed as tsf using(
    Select
        * exclude(rn)
    from
        (
            Select
                *,
                row_number() over(
                    partition by CUSTOMER_ID,
                    PANEL_ID,
                    DATAPOINT_ID,
                    SECTION_ID,
                    datapoint_label
                    order by
                        (
                            select
                                null
                        )
                ) as rn
            from
                _statistics_feed QUALIFY rn = 1
        )
) ssf on tsf.CUSTOMER_ID = ssf.CUSTOMER_ID
and tsf.PANEL_ID = ssf.PANEL_ID
and tsf.DATAPOINT_ID = ssf.DATAPOINT_ID
and tsf.SECTION_ID = ssf.SECTION_ID
AND tsf.datapoint_label = ssf.datapoint_label
WHEN MATCHED
AND tsf.meta_row_hash != ssf.meta_row_hash then
UPDATE
SET
    tsf.CUSTOMER_ID = ssf.CUSTOMER_ID,
    tsf.PANEL_ID = ssf.PANEL_ID,
    tsf.PANEL_LABEL = ssf.PANEL_LABEL,
    tsf.DATAPOINT_ID = ssf.DATAPOINT_ID,
    tsf.DATAPOINT_LABEL = ssf.DATAPOINT_LABEL,
    tsf.DATAPOINT_TYPE = ssf.DATAPOINT_TYPE,
    tsf.DATAPOINT_VALUE = ssf.DATAPOINT_VALUE,
    tsf.SECTION_ID = ssf.SECTION_ID,
    tsf.SECTION_LABEL = ssf.SECTION_LABEL,
    tsf.SECTION_SEQUENCE = ssf.SECTION_SEQUENCE,
    tsf.PANEL_SEQUENCE = ssf.PANEL_SEQUENCE,
    tsf.DATAPOINT_SEQUENCE = ssf.DATAPOINT_SEQUENCE,
    tsf.meta_row_hash = ssf.meta_row_hash,
    tsf.meta_update_datetime = $current_datetime
    WHEN NOT MATCHED THEN
INSERT
    (
        CUSTOMER_ID,
        PANEL_ID,
        PANEL_LABEL,
        DATAPOINT_ID,
        DATAPOINT_LABEL,
        DATAPOINT_TYPE,
        DATAPOINT_VALUE,
        SECTION_ID,
        SECTION_LABEL,
        SECTION_SEQUENCE,
        PANEL_SEQUENCE,
        DATAPOINT_SEQUENCE,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
    )
values
(
        ssf.CUSTOMER_ID,
        ssf.PANEL_ID,
        ssf.PANEL_LABEL,
        ssf.DATAPOINT_ID,
        ssf.DATAPOINT_LABEL,
        ssf.DATAPOINT_TYPE,
        ssf.DATAPOINT_VALUE,
        ssf.SECTION_ID,
        ssf.SECTION_LABEL,
        ssf.SECTION_SEQUENCE,
        ssf.PANEL_SEQUENCE,
        ssf.DATAPOINT_SEQUENCE,
        ssf.meta_row_hash,
        $current_datetime,
        $current_datetime
    );

UPDATE reporting_prod.public.meta_table_dependency_watermark
SET
    high_watermark_datetime = new_high_watermark_datetime,
    meta_update_datetime = $current_datetime::timestamp_ltz(3)
WHERE table_name = $target_table;
