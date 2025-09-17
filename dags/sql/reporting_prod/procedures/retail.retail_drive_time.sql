
SET max_date = (SELECT coalesce(MAX(date),'2019-01-01'::date)
FROM reporting_prod.retail.retail_drive_time);

CREATE OR REPLACE TEMPORARY TABLE _retail_open AS (
SELECT DISTINCT rlc.retail_location_id,
  rl.store_id,
  case when (try_to_date(replace(rlc.configuration_value,'/','-')) is null) then to_date(replace(rlc.configuration_value,'/','-'),'MM-DD-YYYY')
         else (replace(rlc.configuration_value,'/','-'))::date end as open_date,
         (replace(rlc.configuration_value,'/','-'))  as date,
  row_number() OVER (partition by rl.retail_location_id, rl.store_id order by rlc.datetime_modified desc) AS rnk
FROM LAKE.ULTRA_WAREHOUSE.RETAIL_LOCATION_CONFIGURATION rlc
JOIN LAKE.ULTRA_WAREHOUSE.RETAIL_LOCATION rl ON rlc.RETAIL_LOCATION_ID = rl.RETAIL_LOCATION_ID
WHERE rlc.retail_configuration_id = 27
);

CREATE OR REPLACE TEMPORARY TABLE _retail_close AS (
SELECT DISTINCT rlc.retail_location_id,rl.store_id,replace(rlc.configuration_value,'/','-')::date AS close_date,
    row_number() OVER (partition by rl.retail_location_id, rl.store_id order by rlc.datetime_modified desc) AS rnk
FROM LAKE.ULTRA_WAREHOUSE.RETAIL_LOCATION_CONFIGURATION rlc
JOIN LAKE.ULTRA_WAREHOUSE.RETAIL_LOCATION rl ON rlc.RETAIL_LOCATION_ID = rl.RETAIL_LOCATION_ID
WHERE rlc.retail_configuration_id = 28 and NVL(rlc.hvr_is_deleted, 0) = 0
);

CREATE OR REPLACE TEMPORARY TABLE _retail_open_close AS (
SELECT DISTINCT ds.store_id,
  COALESCE(open_date ,'1900-01-01') AS open_date,
  COALESCE(close_date, '9999-12-31') AS close_date
FROM edw_prod.data_model.dim_store ds
LEFT JOIN _retail_open ro ON ro.store_id = ds.store_id
    AND ro.rnk = 1
LEFT JOIN _retail_close rc ON rc.retail_location_id = ro.retail_location_id AND ro.store_id = rc.store_id
    AND rc.rnk = 1
WHERE ds.store_brand_abbr = 'FL'
    AND store_type = 'Retail'
    AND store_sub_type not in ('Varsity','Tough Mudder')
);

CREATE OR REPLACE TEMPORARY TABLE _customer_base  as (
SELECT customer_id
      ,activation_local_datetime as  activation_local_datetime
      ,cancellation_local_datetime as cancellation_local_datetime
      ,Row_number() over(partition by customer_id order by activation_local_datetime asc) as rn
FROM edw_prod.data_model.FACT_ACTIVATION
WHERE
    store_id = 52
);

CREATE OR REPLACE TEMPORARY TABLE _retail_customers  as (
SELECT DISTINCT
     fo.customer_id,order_local_datetime::date as order_date
FROM edw_prod.data_model.fact_order  AS  fo
JOIN _customer_base cb ON cb.customer_id = fo.customer_Id and cb.rn= 1
JOIN edw_prod.data_model.dim_store AS  st ON st.store_id = fo.store_id
JOIN edw_prod.data_model.dim_order_sales_channel  dosc on dosc.order_sales_channel_key = fo.order_sales_channel_key
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STORE  AS  v ON v.store_id = st.store_id AND v.code LIKE 'flretailvarsity%'
WHERE
    st.store_brand = 'Fabletics'
    AND order_classification_l1 = 'Product Order'
    AND order_sales_channel_l1 = 'Retail Order'
    AND fo.order_status_key = 1
    AND v.store_id IS NULL  -- takes out varsity
    AND order_local_datetime::date between date_trunc('year',$max_date) AND current_date()-1
);

CREATE OR REPLACE TEMPORARY TABLE _driving_distance as (
SELECT dd.full_date as  full_date
      ,cb.customer_id
      ,st.store_id
      ,concat(store_retail_location_code,' ',store_full_name) as store_name
      ,store_region AS region
      ,store_country as country
      ,store_retail_district as district
      ,rdd.duration AS  driving_time
      ,rdd.distance AS distance
      ,row_number() over (partition by cb.customer_id,full_date  order by distance) AS distance_rank
FROM edw_prod.data_model.dim_date dd
LEFT JOIN _customer_base cb on dd.full_date >= activation_local_datetime::date and rn = 1
LEFT JOIN edw_prod.data_model.dim_customer dc ON cb.customer_id = dc.customer_id
LEFT JOIN reporting_base_prod.data_science.fl_retail_driving_distance rdd ON TO_CHAR(rdd.vip_zip) = TO_CHAR
                                     ((CASE WHEN dc.QUIZ_ZIP IS NOT NULL AND dc.QUIZ_ZIP<>'Unknown' AND length(dc.QUIZ_ZIP)>=5 THEN LEFT(dc.QUIZ_ZIP,5)
                                            WHEN dc.DEFAULT_POSTAL_CODE<>'Unknown' AND LENGTH(dc.DEFAULT_POSTAL_CODE)>=5 THEN LEFT(dc.DEFAULT_POSTAL_CODE,5)
                                            ELSE dc.QUIZ_ZIP END))
LEFT JOIN edw_prod.data_model.dim_store  AS  st ON st.store_retail_zip_code = rdd.store_zip
     AND st.store_group = 'Fabletics'
LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STORE  AS  v ON v.store_id = st.store_id AND v.code LIKE 'flretailvarsity%'
LEFT JOIN _retail_open_close roc ON roc.store_id = st.store_id
                                 AND dd.full_date BETWEEN open_date AND close_date
WHERE full_date between date_trunc('year',$max_date) AND current_date()-1
and rdd.duration <= 60
AND v.store_id IS NULL
and roc.open_date is not null
);

CREATE OR REPLACE TEMPORARY TABLE _nearest_driving_distance as
SELECT * FROM _driving_distance where distance_rank = 1;

CREATE OR REPLACE TEMPORARY TABLE _final as (
SELECT DISTINCT
     dld.full_date
    ,dld.customer_id
    ,dld.driving_time
    ,dld.distance
    ,store_name
    ,region
    ,country
    ,district
    ,iff(rc.customer_id is null,0,1) as Shopped_In_Retail
    ,iff (dld.full_date >= cb.activation_local_datetime and dld.full_date < cb.cancellation_local_datetime ,1,0) AS is_bop_vip
    ,iff (dld.full_date = cb.activation_local_datetime::date,1,0 ) AS is_activated
    ,dld.distance_rank
    ,row_number() over (partition by dld.customer_id,date_trunc('month',full_date) order by distance) AS distance_rank_month
    ,row_number() over (partition by dld.customer_id,date_trunc('year',full_date) order by distance) AS distance_rank_year
FROM _nearest_driving_distance dld
LEFT JOIN _customer_base cb on dld.customer_id = cb.customer_id and dld.full_date >= cb.activation_local_datetime::date and dld.full_date <= cb.cancellation_local_datetime::date
LEFT JOIN _retail_customers rc on rc.customer_id = dld.customer_id and dld.full_date = order_date
);

CREATE OR REPLACE TEMPORARY TABLE _yearly_activated as
SELECT DISTINCT DATE_TRUNC('year',full_date) as date,customer_id
FROM _final
WHERE is_activated =1
GROUP BY date,customer_id;

CREATE OR REPLACE TEMPORARY TABLE _monthly_activated as
SELECT DISTINCT DATE_TRUNC('month',full_date) as date,customer_id
FROM _final
WHERE is_activated =1
GROUP BY date,customer_id;

CREATE OR REPLACE TEMPORARY TABLE _yearly_retail_shoppers as
SELECT DISTINCT DATE_TRUNC('year',full_date) as date,customer_id
FROM _final
WHERE Shopped_In_Retail =1
GROUP BY date,customer_id;

CREATE OR REPLACE TEMPORARY TABLE _monthly_retail_shoppers as
SELECT DISTINCT DATE_TRUNC('month',full_date) as date,customer_id
FROM _final
WHERE Shopped_In_Retail =1
GROUP BY date,customer_id;

CREATE OR REPLACE TEMPORARY TABLE _yearly_vips as
SELECT DISTINCT DATE_TRUNC('year',full_date) as date,customer_id
FROM _final
WHERE full_date = DATE_TRUNC('year',full_date) and is_bop_vip =1
GROUP BY date,customer_id;

CREATE OR REPLACE TEMPORARY TABLE _monthly_vips as
SELECT DISTINCT DATE_TRUNC('month',full_date) as date,customer_id
FROM _final
WHERE full_date = DATE_TRUNC('month',full_date) and is_bop_vip =1
GROUP BY date,customer_id;

CREATE OR REPLACE TEMPORARY TABLE _retail_drive_time_final as
SELECT 'Day' AS Grain
       ,'sheet1' as sheet
       ,full_date AS date
       ,store_name
       ,region
       ,country
       ,district
       ,driving_time
       ,DISTANCE
       ,Shopped_In_Retail
       ,is_bop_vip
       ,is_activated
       ,1 as distance_rank
       ,COUNT(DISTINCT customer_id) as DISTINCT_CUSTOMER_CNT
FROM _final
WHERE distance_rank =1
GROUP BY full_date
       ,store_name
       ,region
       ,country
       ,district
       ,driving_time
       ,DISTANCE
       ,Shopped_In_Retail
       ,is_bop_vip
       ,is_activated
UNION
SELECT 'Month' AS Grain
       ,'sheet1' as sheet
       ,DATE_TRUNC('month',full_date) AS date
       ,store_name
       ,region
       ,country
       ,district
       ,driving_time
       ,DISTANCE
       ,case when ms.customer_Id is null then 0 else 1 end as Shopped_In_Retail
       ,case when mv.customer_Id is null then 0 else 1 end as is_bop_vip
       ,case when ma.customer_Id is null then 0 else 1 end as is_activated
       ,1 as distance_rank
       ,COUNT(DISTINCT a.customer_id) as DISTINCT_CUSTOMER_CNT
FROM _final a
LEFT JOIN _monthly_vips mv on a.customer_id = mv.customer_id and mv.date = DATE_TRUNC('month',a.full_date)
LEFT JOIN _monthly_retail_shoppers ms on a.customer_id = ms.customer_id and ms.date = DATE_TRUNC('month',a.full_date)
LEFT JOIN _monthly_activated ma on a.customer_id = ma.customer_id and ma.date = DATE_TRUNC('month',a.full_date)
WHERE distance_rank_month =1
GROUP BY DATE_TRUNC('month',full_date)
       ,store_name
       ,region
       ,country
       ,district
       ,driving_time
       ,DISTANCE
       ,case when ms.customer_Id is null then 0 else 1 end
       ,case when mv.customer_Id is null then 0 else 1 end
       ,case when ma.customer_Id is null then 0 else 1 end
UNION
SELECT 'Year' AS Grain
      ,'sheet1' as sheet
      ,DATE_TRUNC('year',full_date) AS date
      ,store_name
      ,region
      ,country
      ,district
      ,driving_time
      ,distance
      ,case when ys.customer_Id is null then 0 else 1 end as Shopped_In_Retail
      ,case when yv.customer_Id is null then 0 else 1 end as is_bop_vip
      ,case when ya.customer_Id is null then 0 else 1 end as is_activated
      ,1 as distance_rank
      ,COUNT(DISTINCT a.customer_id) as DISTINCT_CUSTOMER_CNT
FROM _final a
LEFT JOIN _yearly_vips yv ON a.customer_id = yv.customer_id AND yv.date = DATE_TRUNC('year',a.full_date)
LEFT JOIN _yearly_retail_shoppers ys ON a.customer_id = ys.customer_id AND ys.date = DATE_TRUNC('year',a.full_date)
LEFT JOIN _yearly_activated ya ON a.customer_id = ya.customer_id AND ya.date = DATE_TRUNC('year',a.full_date)
WHERE distance_rank_year =1
GROUP BY
      DATE_TRUNC('year',full_date)
      ,store_name
      ,region
      ,country
      ,district
      ,driving_time
      ,distance
      ,case when ys.customer_Id is null then 0 else 1 end
      ,case when yv.customer_Id is null then 0 else 1 end
      ,case when ya.customer_Id is null then 0 else 1 end
;

DELETE FROM reporting_prod.retail.retail_drive_time
WHERE date >= DATE_TRUNC('year',$max_date);


INSERT INTO reporting_prod.retail.retail_drive_time (grain,sheet,date,store_name,region,country,district,driving_time,distance,shopped_in_retail,is_bop_vip,is_activated,distance_rank,distinct_customer_cnt)
(SELECT grain,sheet,date,store_name,region,country,district,driving_time,distance,shopped_in_retail,is_bop_vip,is_activated,distance_rank,distinct_customer_cnt
 FROM _retail_drive_time_final);
