
delete
from reporting_media_base_prod.dbo.impact_radius_ltk_all_events
where meta_update_datetime <= dateadd(day, -45, current_date());

set min_date_order = dateadd(day,-2,current_date());
set min_date_session = dateadd(day,-4,current_date());
set max_date = current_date();

------------------------------------------------------------------------------------
-- VIPS WITH LTK SESSION WITHIN 24 HOURS OF ACTIVATION --

-- get all visitor IDs with irclick ID in URI
create or replace temporary table _visitors_w_ltk_click_id as
select distinct visitor_id as visitor_id_w_ltkevent
from reporting_base_prod.shared.session s
join edw_prod.data_model.dim_store ds on ds.store_id = s.store_id
where uri ilike '%irclickid%'
    and lower(store_brand_abbr) in ('fl','yty')
    and lower(store_country) = 'us'
    and session_local_datetime::date >= $min_date_session
    and session_local_datetime::date < $max_date;

-- get all sessions from above visitors
create or replace temporary table _sessions_from_visitors_w_ltk_click_id as
select distinct visitor_id,
                session_id,
                convert_timezone('UTC', session_local_datetime)::timestamp_ntz as session_local_datetime,
                uri,
                replace(get_ignore_case((parse_url('https:/' || uri):"parameters"), 'irclickid'), '', '') as click_id
from reporting_base_prod.shared.session s
join _visitors_w_ltk_click_id v on v.visitor_id_w_ltkevent = s.visitor_id
where session_local_datetime::date >= $min_date_session
    and session_local_datetime::date < $max_date;

-- get all activating orders from above visitors
create or replace temporary table _act_orders_w_ltk_visit as
select a.customer_id,
       a.order_id,
       convert_timezone('UTC', a.source_activation_local_datetime)::timestamp_ntz as activation_local_datetime,
       s.visitor_id
from edw_prod.data_model.fact_activation a
join _sessions_from_visitors_w_ltk_click_id s on s.session_id = a.session_id
where source_activation_local_datetime::date >= $min_date_order
    and source_activation_local_datetime::date < $max_date;

-- get all ltk sessions within 24 hours for visitors with activating order
create or replace temporary table _act_orders_w_visitor_sessions as
select v.*,
       s.session_local_datetime,
       s.click_id
from _act_orders_w_ltk_visit v
join _sessions_from_visitors_w_ltk_click_id s on s.visitor_id = v.visitor_id
where click_id is not null
    and session_local_datetime < activation_local_datetime
    and datediff(hour,session_local_datetime,activation_local_datetime) < 25;

-- get latest click ID for all activating orders
create or replace temporary table _latest_click_id as
select *,
       datediff(hour,session_local_datetime,activation_local_datetime) as hours_between,
       row_number() over (partition by order_id order by hours_between) as rn
from _act_orders_w_visitor_sessions;

create or replace temporary table _final_act_orders_w_ltk_visit as
select activation_local_datetime,
       click_id,
       edw_prod.stg.udf_unconcat_brand(order_id) as order_id,
       edw_prod.stg.udf_unconcat_brand(customer_id) as customer_id
from _latest_click_id
where rn = 1;

------------------------------------------------------------------------------------
-- LTK ORDERS W/ CLICKID IN SEGMENT PAGES  --

create or replace temporary table _segment_clickid_sessions as
select distinct properties_session_id,
                regexp_substr(parse_url(properties_url)['query'],'irclickid=([^&]+)', 1, 1, 'e') as irclickid_value,
                originaltimestamp as event_datetime,
                row_number() over (partition by properties_session_id order by event_datetime asc) as rn -- first click ID in the session
from lake.segment_fl.javascript_fabletics_page
where originaltimestamp::date >= $min_date_session
  and irclickid_value is not null;

create or replace temporary table _unique_segment_clickid_sessions as
select distinct properties_session_id, irclickid_value
from _segment_clickid_sessions
where rn = 1;

create or replace temporary table _session_og_datetime as
select c.*,
       convert_timezone('UTC', s.session_local_datetime)::timestamp_ntz as session_local_datetime
from _unique_segment_clickid_sessions c
join reporting_base_prod.shared.session s on edw_prod.stg.udf_unconcat_brand(s.session_id) = c.properties_session_id;

create or replace temporary table _final_act_orders_from_segment_sessions as
select distinct convert_timezone('UTC', a.source_activation_local_datetime)::timestamp_ntz as activation_local_datetime,
                session_local_datetime,
                irclickid_value as click_id,
                order_id,
                customer_id
from _session_og_datetime s
join edw_prod.data_model_fl.fact_activation a on a.session_id = s.properties_session_id
    and s.session_local_datetime < convert_timezone('UTC', a.source_activation_local_datetime)::timestamp_ntz;


------------------------------------------------------------------------------------
-- 3D VIPS FROM LTK LEADS --

create or replace temporary table _vips_from_leads_with_ltk_click_id as
select convert_timezone('UTC', source_activation_local_datetime)::timestamp_ntz as activation_local_datetime,
       replace(get_ignore_case((parse_url('https:/' || s.uri):"parameters"), 'irclickid'), '', '') as click_id,
       edw_prod.stg.udf_unconcat_brand(a.customer_id) as customer_id,
       edw_prod.stg.udf_unconcat_brand(a.order_id) as order_id
from edw_prod.data_model.fact_activation a
join edw_prod.data_model.fact_registration r on r.customer_id = a.customer_id
    and r.is_secondary_registration = false
    and  datediff(day,registration_local_datetime,source_activation_local_datetime) < 3
join reporting_base_prod.shared.session s on s.session_id = r.session_id
where a.store_id in (52,241)
    and a.source_activation_local_datetime::date >= $min_date_order
    and a.source_activation_local_datetime::date < $max_date
    and s.uri ilike '%irclickid%';

------------------------------------------------------------------------------------

create or replace temporary table _all_ltk_vips
(
    activation_local_datetime DATETIME,
    click_id VARCHAR,
    order_id NUMBER(38,0),
    customer_id NUMBER(38,0)
);

insert into  _all_ltk_vips
select activation_local_datetime,
       click_id,
       order_id,
       customer_id
from _final_act_orders_w_ltk_visit;

insert into  _all_ltk_vips
select l.activation_local_datetime,
       l.click_id,
       l.order_id,
       l.customer_id
from _vips_from_leads_with_ltk_click_id l
left join  _all_ltk_vips c on c.order_id = l.order_id
where c.order_id is null;

insert into  _all_ltk_vips
select l.activation_local_datetime,
       l.click_id,
       l.order_id,
       l.customer_id
from _final_act_orders_from_segment_sessions l
left join  _all_ltk_vips c on c.order_id = l.order_id
where c.order_id is null;

------------------------------------------------------------------------------------

insert into reporting_media_base_prod.dbo.impact_radius_ltk_all_events
select '12340' as "CampaignId",
       '49532' as "ActionTrackerId",
       activation_local_datetime as "EventDate",
       click_id as "ClickId",
       order_id as "OrderId",
       customer_id as "CustomerId",
       'NEW' as "CustomerStatus",
       current_timestamp as meta_update_datetime
from _all_ltk_vips;
