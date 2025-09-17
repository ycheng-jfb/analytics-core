SET low_watermark = %(low_watermark)s :: TIMESTAMP_LTZ;

create or replace temp table _customer_events as
select store_brand_abbr,
       case when lower(membership_order_type_l3) = 'activating vip' then 'Purchase'
           else membership_order_type_l3 end as event_name,
       order_local_datetime,
       order_id,
       store_region,
       product_gross_revenue_local_amount as value,
       customer_id,
       store_currency
from edw_prod.data_model.fact_order o
join edw_prod.data_model.dim_order_membership_classification c on c.order_membership_classification_key = o.order_membership_classification_key
join edw_prod.data_model.dim_store ds on ds.store_id = o.store_id
where lower(ds.store_type) = 'retail'
    and lower(store_country) = 'us' and o.meta_update_datetime>dateadd('hour',-2,$low_watermark)
    and DATEDIFF(DAY, CONVERT_TIMEZONE('America/Los_Angeles', o.order_local_datetime), o.meta_update_datetime) <=7;


 create or replace temp table _user_phone as (
   select distinct u.customer_id, last_value(cp.phone IGNORE NULLs) over( partition by cp.customer_id order by cp.DATETIME_MODIFIED) as cp_phone,
     last_value(a.phone IGNORE NULLs) over( partition by a.customer_id order by a.DATETIME_MODIFIED) as a_phone
   from _customer_events u
   left join lake_consolidated_view.ultra_merchant.customer_phone cp on cp.customer_id = u.customer_id
   left join lake_consolidated_view.ultra_merchant.address a on a.customer_id = u.customer_id
 );

 create or replace temp table _conversions as
 select store_brand_abbr
 ,ce.customer_id
 ,order_id
 ,store_currency as currency
 ,order_local_datetime as event_time
 ,event_name
 ,value
 ,email
 ,case
         when coalesce(up.cp_phone,up.a_phone) is null then ''
         when contains(coalesce(up.cp_phone,up.a_phone), '+') then ltrim(regexp_replace(coalesce(up.cp_phone,up.a_phone,''), '[^0-9]+'), '0')
         when LENGTH(coalesce(up.cp_phone,up.a_phone)) = 11 AND LEFT(coalesce(up.cp_phone,up.a_phone), 1) = '1' THEN coalesce(up.cp_phone,up.a_phone)
         else '1'||ltrim(regexp_replace(coalesce(up.cp_phone,up.a_phone), '[^0-9]+'), '0') end as phone_number
 ,iff(gender not in ('F','M'), 'F',gender ) as gender
 ,FIRST_NAME
 ,LAST_NAME
 ,DEFAULT_CITY as city
 ,DEFAULT_STATE_PROVINCE as state
 ,DEFAULT_POSTAL_CODE
 ,COUNTRY_CODE as country
 from _customer_events ce
 left join edw_prod.data_model.dim_customer dc on ce.customer_id=dc.customer_id
 left join _user_phone up on ce.customer_id=up.customer_id;


 MERGE INTO reporting_media_prod.dbo.facebook_offline_conversion t
 USING (
           SELECT a.*
               FROM (
                        SELECT *,
                               hash(*) AS meta_row_hash,
                               CURRENT_TIMESTAMP AS meta_create_datetime,
                               CURRENT_TIMESTAMP AS meta_update_datetime,
                               row_number() over (partition BY order_id,event_name,event_time order by '1900-01-01' ) AS rn
                            FROM _conversions
                            WHERE event_name IS NOT NULL
                    ) a
               WHERE a.rn = 1
       ) s ON equal_null(t.event_name, s.event_name)
     AND equal_null(t.event_time, s.event_time)
     AND equal_null(t.order_id, s.order_id)
 WHEN NOT MATCHED
     THEN INSERT (
                  store_brand_abbr
                 ,customer_id
                 ,order_id
                 ,currency
                 ,event_time
                 ,event_name
                 ,value
                 ,email
                 ,phone_number
                 ,gender
                 ,FIRST_NAME
                 ,LAST_NAME
                 ,city
                 ,state
                 ,DEFAULT_POSTAL_CODE
                 ,country
                  ,meta_row_hash,
                  meta_create_datetime,
                  meta_update_datetime)
         VALUES (store_brand_abbr
                 ,customer_id
                 ,order_id
                 ,currency
                 ,event_time
                 ,event_name
                 ,value
                 ,email
                 ,phone_number
                 ,gender
                 ,FIRST_NAME
                 ,LAST_NAME
                 ,city
                 ,state
                 ,DEFAULT_POSTAL_CODE
                 ,country
                  ,meta_row_hash,
                  meta_create_datetime,
                  meta_update_datetime)
 WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
 THEN UPDATE SET
     t.store_brand_abbr = s.store_brand_abbr,
     t.customer_id=s.customer_id,
     t.order_id=s.order_id,
     t.currency = s.currency,
     t.event_name=s.event_name,
     t.value=s.value,
     t.event_time=s.event_time,
     t.email = s.email,
     t.phone_number = s.phone_number,
     t.gender = s.gender,
     t.FIRST_NAME=s.FIRST_NAME,
     t.LAST_NAME=s.LAST_NAME,
     t.city=s.city,
     t.state=s.state,
     t.DEFAULT_POSTAL_CODE=s.DEFAULT_POSTAL_CODE,
     t.country=s.country,
     t.meta_row_hash = s.meta_row_hash,
     t.meta_update_datetime = s.meta_update_datetime;
