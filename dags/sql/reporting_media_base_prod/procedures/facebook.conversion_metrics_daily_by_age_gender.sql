create or replace  temp table _ACTIONS_BY_AGE_GENDER as
select coalesce(a.account_id,b.account_id) as account_id,coalesce(a.ad_id,b.ad_id) as ad_id,coalesce(a.date,b.date) as date,coalesce(a.age,b.age) as age,coalesce(a.gender,b.gender) as gender,coalesce(a.ACTIONS_action_type,b.ACTIONS_action_type) as ACTIONS_action_type
,coalesce(a.CONVERSIONS_action_type,b.CONVERSIONS_action_type) as CONVERSIONS_action_type
,coalesce(a.CONVERSION_VALUES_action_type,b.CONVERSION_VALUES_action_type) as CONVERSION_VALUES_action_type
,coalesce(a.ACTIONS_value,b.ACTIONS_value) ACTIONS_value
,a.ACTIONS_1d_view
,a.ACTIONS_1d_click
,b.ACTIONS_7d_view
,b.ACTIONS_7d_click
,coalesce(a.CONVERSIONS_value,b.CONVERSIONS_value) CONVERSIONS_value
,a.CONVERSIONS_1d_view
,a.CONVERSIONS_1d_click
,b.CONVERSIONS_7d_view
,b.CONVERSIONS_7d_click
,coalesce(a.CONVERSION_VALUES_value,b.CONVERSION_VALUES_value) CONVERSION_VALUES_value
,a.CONVERSION_VALUES_1d_view
,a.CONVERSION_VALUES_1d_click
,b.CONVERSION_VALUES_7d_view
,b.CONVERSION_VALUES_7d_click
,coalesce(a.META_CREATE_DATETIME,b.META_CREATE_DATETIME) META_CREATE_DATETIME
,coalesce(a.META_UPDATE_DATETIME,b.META_UPDATE_DATETIME) META_UPDATE_DATETIME
from LAKE_VIEW.FACEBOOK.ACTIONS_BY_AGE_GENDER_1_d a
full join LAKE_VIEW.FACEBOOK.ACTIONS_BY_AGE_GENDER_7_d b on a.ad_id=b.ad_id and a.date=b.date
and (a.ACTIONS_action_type=b.ACTIONS_action_type or a.CONVERSIONS_action_type=b.CONVERSIONS_action_type or a.CONVERSION_VALUES_action_type = b.CONVERSION_VALUES_action_type) and a.age=b.age and a.gender=b.gender;

create or replace temporary table _action_types as
select
    ACTIONS_action_type as "action_type",
    account_id,
    ad_id,
    date,
    age,
    gender,
    ACTIONS_value as "value",
    actions_1d_click as "1d_click",
    actions_7d_click as "7d_click",
    actions_1d_view as "1d_view",
    actions_7d_view as "7d_view"
from _ACTIONS_BY_AGE_GENDER;


create or replace temporary table _conversion_types as
select
    conversions_action_type as "action_type",
    account_id,
    ad_id,
    date,
    age,
    gender,
    conversions_value as "value",
    conversions_1d_click as "1d_click",
    conversions_7d_click as "7d_click",
    conversions_1d_view as "1d_view",
    conversions_7d_view as "7d_view"
from _ACTIONS_BY_AGE_GENDER;


create or replace temporary table _conversion_values as
select
    conversion_values_action_type as "action_type",
    account_id,
    ad_id,
    date,
    age,
    gender,
    conversion_values_value as "value",
    conversion_values_1d_click as "1d_click",
    conversion_values_7d_click as "7d_click",
    conversion_values_1d_view as "1d_view",
    conversion_values_7d_view as "7d_view"
from _ACTIONS_BY_AGE_GENDER;

--------------------------------------------------------------------


create or replace temporary table _pixel_lead as
select
    account_id,
    ad_id,
    date,
    age,
    gender,
    coalesce(att."1d_click",0)as pixel_lead_click_1d,
    coalesce(att."1d_view",0) as pixel_lead_view_1d,
    coalesce(att."7d_click",0) as pixel_lead_click_7d,
    coalesce(att."7d_view",0) as pixel_lead_view_7d
from _action_types att
where "action_type" = 'offsite_conversion.fb_pixel_complete_registration';

----------------------------------------------------------------------------

create or replace temporary table _pixel_purchase as
select
    account_id,
    ad_id,
    date,
    age,
    gender,
    coalesce(att."1d_click",0)as pixel_purchase_click_1d,
    coalesce(att."1d_view",0) as pixel_purchase_view_1d,
    coalesce(att."7d_click",0) as pixel_purchase_click_7d,
    coalesce(att."7d_view",0) as pixel_purchase_view_7d
from _action_types att
where "action_type" = 'offsite_conversion.fb_pixel_purchase';


----------------------------------------------------------------------------

create or replace temporary table _pixel_purchase_new_vip as
select
    att.account_id,
    ad_id,
    date,
    age,
    gender,
    coalesce(att."1d_click",0)as pixel_purchase_click_1d,
    coalesce(att."1d_view",0) as pixel_purchase_view_1d,
    coalesce(att."7d_click",0) as pixel_purchase_click_7d,
    coalesce(att."7d_view",0) as pixel_purchase_view_7d
from _action_types att
join lake_view.sharepoint.med_facebook_conversion_mapping p on att.account_id = p.account_id
    and p.map_to_column ilike 'new vip%'
    and att.date < p.first_day_used
where "action_type" = 'offsite_conversion.fb_pixel_purchase';


insert into _pixel_purchase_new_vip
(select
    pur.account_id,
    pur.ad_id,
    pur.date,
    age,
    gender,
    pixel_purchase_click_1d,
    pixel_purchase_view_1d,
    pixel_purchase_click_7d,
    pixel_purchase_view_7d
from _pixel_purchase pur
left join lake_view.sharepoint.med_facebook_conversion_mapping p on pur.account_id = p.account_id
    and p.map_to_column ilike 'new vip%'
where p.account_id is null);

----------------------------------------------------------------------------

create or replace temporary table _pixel_new_vip as
select
    att.account_id,
    ad_id,
    date,
    age,
    gender,
    p.first_day_used,
    coalesce(att."1d_click",0)as pixel_vip_click_1d,
    coalesce(att."1d_view",0) as pixel_vip_view_1d,
    coalesce(att."7d_click",0) as pixel_vip_click_7d,
    coalesce(att."7d_view",0) as pixel_vip_view_7d
from _action_types att
cross join lake_view.sharepoint.med_facebook_conversion_mapping p where p.custom_conversion_id = replace("action_type",'offsite_conversion.custom.','')
    and "action_type" ilike '%offsite_conversion.custom%'
    and p.first_day_used <= att.date
    and p.map_to_column ilike 'new vip%'
    and p.account_id = att.account_id;

----------------------------------------------------------------------------

create or replace temporary table _pixel_subscribe as
select
    att.account_id,
    ad_id,
    date,
    age,
    gender,
    coalesce(att."1d_click",0)as pixel_subscribe_click_1d,
    coalesce(att."1d_view",0) as pixel_subscribe_view_1d,
    coalesce(att."7d_click",0) as pixel_subscribe_click_7d,
    coalesce(att."7d_view",0) as pixel_subscribe_view_7d
from _conversion_types att
where "action_type" = 'subscribe_total';

----------------------------------------------------------------------------

create or replace temporary table _pixel_subscribe_revenue as
select
    account_id,
    ad_id,
    date,
    age,
    gender,
    coalesce(att."1d_click",0)as pixel_subscribe_revenue_click_1d,
    coalesce(att."1d_view",0) as pixel_subscribe_revenue_view_1d,
    coalesce(att."7d_click",0) as pixel_subscribe_revenue_click_7d,
    coalesce(att."7d_view",0) as pixel_subscribe_revenue_view_7d
from _conversion_values att
where "action_type" = 'subscribe_total';

----------------------------------------------------------------------------

create or replace temporary table _combos as
select distinct
    account_id,
    ad_id,
    date,
    age,
    gender
from _pixel_lead
union
select distinct
    account_id,
    ad_id,
    date,
    age,
    gender
from _pixel_new_vip
union
select distinct
    account_id,
    ad_id,
    date,
    age,
    gender
from _pixel_purchase
union
select distinct
    account_id,
    ad_id,
    date,
    age,
    gender
from _pixel_purchase_new_vip
union
select distinct
    account_id,
    ad_id,
    date,
    age,
    gender
from _pixel_subscribe
union
select distinct
    account_id,
    ad_id,
    date,
    age,
    gender
from _pixel_subscribe_revenue;

----------------------------------------------------------------------------

set ios_rollout = '2021-04-19';

create or replace temporary table _all_pixel_events as
select
    p.account_id,
    p.ad_id,
    p.date,
    p.age,
    p.gender,

    coalesce(pl.pixel_lead_click_1d,0) as pixel_lead_click_1d,
    coalesce(pl.pixel_lead_view_1d,0) as pixel_lead_view_1d,
    coalesce(pl.pixel_lead_click_7d,0) as pixel_lead_click_7d,
    coalesce(pl.pixel_lead_view_7d,0) as pixel_lead_view_7d,

    coalesce(pl3.pixel_purchase_click_1d,0) as pixel_purchase_click_1d,
    coalesce(pl3.pixel_purchase_view_1d,0) as pixel_purchase_view_1d,
    coalesce(pl3.pixel_purchase_click_7d,0) as pixel_purchase_click_7d,
    coalesce(pl3.pixel_purchase_view_7d,0) as pixel_purchase_view_7d,

    -- use new vip/purchase prior to ios rollout, use purchase event after
    case when p.date <= $ios_rollout then coalesce(pl2.pixel_vip_click_1d,pl4.PIXEL_PURCHASE_CLICK_1D,0)
        else coalesce(pl3.PIXEL_purchase_CLICK_1D,0) end as pixel_vip_click_1d,
    case when p.date <= $ios_rollout then coalesce(pl2.pixel_vip_view_1d,pl4.PIXEL_PURCHASE_VIEW_1D,0)
        else coalesce(pl3.PIXEL_purchase_VIEW_1D,0) end as pixel_vip_view_1d,
    case when p.date <= $ios_rollout then coalesce(pl2.pixel_vip_click_7d,pl4.PIXEL_PURCHASE_CLICK_7D,0)
        else coalesce(pl3.PIXEL_purchase_CLICK_7D,0) end as pixel_vip_click_7d,
    case when p.date <= $ios_rollout then coalesce(pl2.pixel_vip_view_7d,pl4.PIXEL_PURCHASE_VIEW_7D,0)
        else coalesce(pl3.PIXEL_purchase_VIEW_7D,0) end as pixel_vip_view_7d,

    coalesce(pl5.pixel_subscribe_click_1d,0) as pixel_subscribe_click_1d,
    coalesce(pl5.pixel_subscribe_view_1d,0) as pixel_subscribe_view_1d,
    coalesce(pl5.pixel_subscribe_click_7d,0) as pixel_subscribe_click_7d,
    coalesce(pl5.pixel_subscribe_view_7d,0) as pixel_subscribe_view_7d,

    coalesce(pl6.pixel_subscribe_revenue_click_1d,0) as pixel_subscribe_revenue_click_1d,
    coalesce(pl6.pixel_subscribe_revenue_view_1d,0) as pixel_subscribe_revenue_view_1d,
    coalesce(pl6.pixel_subscribe_revenue_click_7d,0) as pixel_subscribe_revenue_click_7d,
    coalesce(pl6.pixel_subscribe_revenue_view_7d,0) as pixel_subscribe_revenue_view_7d

from _combos p
left join _pixel_lead pl on pl.ad_id = p.ad_id
    and pl.date = p.date
    and pl.age = p.age
    and pl.gender = p.gender
left join _pixel_purchase pl3 on pl3.ad_id = p.ad_id
    and pl3.date = p.date
    and pl3.age = p.age
    and pl3.gender = p.gender
left join _pixel_new_vip pl2 on pl2.ad_id = p.ad_id
    and pl2.date = p.date
    and pl2.age = p.age
    and pl2.gender = p.gender
left join _pixel_purchase_new_vip pl4 on pl4.ad_id = p.ad_id
    and pl4.date = p.date
    and pl4.age = p.age
    and pl4.gender = p.gender
left join _pixel_subscribe pl5 on pl5.ad_id = p.ad_id
    and pl5.date = p.date
    and pl5.age = p.age
    and pl5.gender = p.gender
left join _pixel_subscribe_revenue pl6 on pl6.ad_id = p.ad_id
    and pl6.date = p.date
    and pl6.age = p.age
    and pl6.gender = p.gender;


----------------------------------------------------------------------------

set meta_update_datetime = (select max(meta_update_datetime) from _ACTIONS_BY_AGE_GENDER);

create or replace transient table reporting_media_base_prod.facebook.conversion_metrics_daily_by_age_gender as
select
    account_id,
    ad_id,
    store_id,
    age,
    gender,
    date,

    pixel_lead_click_1d,
    pixel_lead_view_1d,
    pixel_lead_click_7d,
    pixel_lead_view_7d,

    pixel_vip_click_1d,
    pixel_vip_view_1d,
    pixel_vip_click_7d,
    pixel_vip_view_7d,

    pixel_purchase_click_1d,
    pixel_purchase_view_1d,
    pixel_purchase_click_7d,
    pixel_purchase_view_7d,

    pixel_subscribe_click_1d,
    pixel_subscribe_view_1d,
    pixel_subscribe_click_7d,
    pixel_subscribe_view_7d,

    pixel_subscribe_revenue_click_1d,
    pixel_subscribe_revenue_view_1d,
    pixel_subscribe_revenue_click_7d,
    pixel_subscribe_revenue_view_7d,

    $meta_update_datetime as pixel_conversion_meta_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime

from
(
    select
        account_id,
        ad_id,
        store_id,
        age,
        gender,
        date,

        sum(pixel_lead_click_1d)      as pixel_lead_click_1d,
        sum(pixel_lead_view_1d)       as pixel_lead_view_1d,
        sum(pixel_lead_click_7d)      as pixel_lead_click_7d,
        sum(pixel_lead_view_7d)       as pixel_lead_view_7d,

        sum(pixel_vip_click_1d)       as pixel_vip_click_1d,
        sum(pixel_vip_view_1d)        as pixel_vip_view_1d,
        sum(pixel_vip_click_7d)       as pixel_vip_click_7d,
        sum(pixel_vip_view_7d)        as pixel_vip_view_7d,

        sum(pixel_purchase_click_1d)  as pixel_purchase_click_1d,
        sum(pixel_purchase_view_1d)   as pixel_purchase_view_1d,
        sum(pixel_purchase_click_7d)  as pixel_purchase_click_7d,
        sum(pixel_purchase_view_7d)   as pixel_purchase_view_7d,

        sum(pixel_subscribe_click_1d)   as pixel_subscribe_click_1d,
        sum(pixel_subscribe_view_1d)    as pixel_subscribe_view_1d,
        sum(pixel_subscribe_click_7d)   as pixel_subscribe_click_7d,
        sum(pixel_subscribe_view_7d)    as pixel_subscribe_view_7d,

        sum(pixel_subscribe_revenue_click_1d)   as pixel_subscribe_revenue_click_1d,
        sum(pixel_subscribe_revenue_view_1d)    as pixel_subscribe_revenue_view_1d,
        sum(pixel_subscribe_revenue_click_7d)   as pixel_subscribe_revenue_click_7d,
        sum(pixel_subscribe_revenue_view_7d)    as pixel_subscribe_revenue_view_7d

    from _all_pixel_events p
    join lake_view.sharepoint.med_account_mapping_media am on am.source_id = p.account_id
        and am.source ilike '%facebook%'
    group by 1,2,3,4,5,6
);
