
-- get cj conversions --
set start_date = dateadd(day,-2,current_date());

create or replace temporary table _leads as
select action_type,
       business_unit,
       store_region,
       store_country,
       companyid,
       enterpriseid,
       subscriptionid,
       cast(edw_prod.stg.udf_unconcat_brand(r.customer_id) as varchar) as orderid,
       actiontrackerid,
       convert_timezonE('UTC', registration_local_datetime)::timestamp_ntz AS eventtime,
       replace(get_ignore_case((parse_url('https:/' || uri):"parameters"), 'cjevent'), '', '') as cjevent,
       0.00 as amount,
       0.00 as discount,
       currency,
       duration,
       customerstatus
from edw_prod.data_model.fact_registration r
join lake_view.sharepoint.cj_batch_action_mapping m on m.store_id = r.store_id
    and m.action_type = 'lead'
where to_date(registration_local_datetime) >= $start_date
    and is_secondary_registration = false
    and is_retail_registration = false
    and is_fake_retail_registration = false
    and uri ilike '%cjevent%';


create or replace temporary table _vips_from_leads_3d as
select action_type,
       business_unit,
       store_region,
       store_country,
       companyid,
       enterpriseid,
       subscriptionid,
       cast(edw_prod.stg.udf_unconcat_brand(o.order_id) as varchar) as orderid,
       actiontrackerid,
       convert_timezone('UTC', order_local_datetime)::timestamp_ntz AS eventtime,
       replace(get_ignore_case((parse_url('https:/' || r.uri):"parameters"), 'cjevent'), '', '') AS cjevent,
       product_subtotal_local_amount AS amount,
       product_discount_local_amount AS discount,
       currency,
       duration,
       customerstatus
from edw_prod.data_model.fact_registration r
join edw_prod.data_model.fact_activation a on a.customer_id = r.customer_id
    and datediff(day,registration_local_datetime, activation_local_datetime) < 3
    and r.store_id = a.store_id
join edw_prod.data_model.fact_order o on o.order_id = a.order_id
join edw_prod.data_model.dim_order_status st on st.order_status_key = o.order_status_key
join lake_view.sharepoint.cj_batch_action_mapping m on m.store_id = a.store_id
    and m.action_type = 'vips from leads 3d'
where to_date(activation_local_datetime) >= $start_date
    and is_secondary_registration = false
    and is_retail_registration = false
    and is_fake_retail_registration = false
    and lower(order_status) = 'success'
    and r.uri ilike '%cjevent%';


create or replace temporary table _vips_90plus as
select action_type,
       business_unit,
       store_region,
       store_country,
       companyid,
       enterpriseid,
       subscriptionid,
       cast(edw_prod.stg.udf_unconcat_brand(o.order_id) as varchar) as orderid,
       actiontrackerid,
       convert_timezone('UTC', order_local_datetime)::timestamp_ntz AS eventtime,
       replace(get_ignore_case((parse_url('https:/' || s.uri):"parameters"), 'cjevent'), '', '') as cjevent,
       product_subtotal_local_amount as amount,
       product_discount_local_amount as discount,
       currency,
       duration,
       customerstatus
from edw_prod.data_model.fact_activation a
join reporting_base_prod.shared.session s on s.session_id = a.session_id
join edw_prod.data_model.fact_registration r on a.customer_id = r.customer_id
    and datediff(day,registration_local_datetime, activation_local_datetime) > 90
    and r.store_id = a.store_id
join edw_prod.data_model.fact_order o on o.order_id = a.order_id
join edw_prod.data_model.dim_order_status st on st.order_status_key = o.order_status_key
join lake_view.sharepoint.cj_batch_action_mapping m on m.store_id = a.store_id
    and m.action_type = 'vips 90plusd'
where to_date(activation_local_datetime) >= $start_date
    and is_secondary_registration = false
    and is_retail_registration = false
    and is_fake_retail_registration = false
    and lower(order_status) = 'success'
    and s.uri ilike '%cjevent%';

create or replace temporary table _guest as
select action_type,
       business_unit,
       store_region,
       store_country,
       companyid,
       enterpriseid,
       subscriptionid,
       cast(edw_prod.stg.udf_unconcat_brand(o.order_id) as varchar) as orderid,
       actiontrackerid,
       convert_timezone('UTC', order_local_datetime)::timestamp_ntz AS eventtime,
       replace(get_ignore_case((parse_url('https:/' || s.uri):"parameters"), 'cjevent'), '', '') AS cjevent,
       product_subtotal_local_amount as amount,
       product_discount_local_amount as discount,
       currency,
       duration,
       customerstatus
from edw_prod.data_model.fact_order o
join reporting_base_prod.shared.session s on s.session_id = o.session_id
join edw_prod.data_model.dim_order_membership_classification cl on cl.order_membership_classification_key = o.order_membership_classification_key
join edw_prod.data_model.dim_order_status st on st.order_status_key = o.order_status_key
join edw_prod.data_model.dim_order_sales_channel c on c.order_sales_channel_key = o.order_sales_channel_key
join lake_view.sharepoint.cj_batch_action_mapping m on m.store_id = o.store_id
    and m.action_type = 'guest purchase'
where to_date(order_local_datetime) >= $start_date
    and s.uri ilike '%cjevent%'
    and lower(order_status) = 'success'
    and lower(membership_order_type_l2) = 'guest'
    and lower(order_classification_l2) = 'product order';


create or replace temporary table _repeat as
select action_type,
       business_unit,
       store_region,
       store_country,
       companyid,
       enterpriseid,
       subscriptionid,
       cast(edw_prod.stg.udf_unconcat_brand(o.order_id) as varchar) as orderid,
       actiontrackerid,
       convert_timezone('UTC', order_local_datetime)::timestamp_ntz AS eventtime,
       replace(get_ignore_case((parse_url('https:/' || s.uri):"parameters"), 'cjevent'), '', '') AS cjevent,
       product_subtotal_local_amount as amount,
       product_discount_local_amount as discount,
       currency,
       duration,
       customerstatus
from edw_prod.data_model.fact_order o
join reporting_base_prod.shared.session s on s.session_id = o.session_id
join edw_prod.data_model.dim_order_membership_classification cl on cl.order_membership_classification_key = o.order_membership_classification_key
join edw_prod.data_model.dim_order_status st on st.order_status_key = o.order_status_key
join edw_prod.data_model.dim_order_sales_channel c on c.order_sales_channel_key = o.order_sales_channel_key
join lake_view.sharepoint.cj_batch_action_mapping m on m.store_id = o.store_id
    and m.action_type = 'repeat vip purchase'
where to_date(order_local_datetime) >= $start_date
    and s.uri ilike '%cjevent%'
    and lower(order_status) = 'success'
    and lower(membership_order_type_l2) = 'repeat vip'
    and lower(order_classification_l2) = 'product order';

create or replace transient table reporting_media_base_prod.dbo.cj_all_events as
-- create or replace temporary table _cj_all_events as
select * from _leads
union
select * from _vips_from_leads_3d
union
select * from _vips_90plus
union
select * from _repeat
union
select * from _guest;

create or replace temporary table _vips_same_session as
select action_type,
       business_unit,
       store_region,
       store_country,
       companyid,
       enterpriseid,
       subscriptionid,
       cast(edw_prod.stg.udf_unconcat_brand(o.order_id) as varchar) as orderid,
       actiontrackerid,
       convert_timezone('UTC', order_local_datetime)::timestamp_ntz AS eventtime,
       replace(get_ignore_case((parse_url('https:/' || s.uri):"parameters"), 'cjevent'), '', '') as cjevent,
       product_subtotal_local_amount as amount,
       product_discount_local_amount as discount,
       currency,
       duration,
       customerstatus
from edw_prod.data_model.fact_activation a
join reporting_base_prod.shared.session s on s.session_id = a.session_id
join edw_prod.data_model.fact_order o on o.order_id = a.order_id
join edw_prod.data_model.dim_order_status st on st.order_status_key = o.order_status_key
join lake_view.sharepoint.cj_batch_action_mapping m on m.store_id = a.store_id
    and m.action_type = 'vips same session'
where to_date(activation_local_datetime) >= $start_date
    and lower(order_status) = 'success'
    and s.uri ilike '%cjevent%';

insert into reporting_media_base_prod.dbo.cj_all_events
-- insert into _cj_all_events
select s.*
from _vips_same_session s
left join reporting_media_base_prod.dbo.cj_all_events c on c.orderid = s.orderid
-- left join _cj_all_events c on c.orderid = s.orderid
where c.orderid is null;
