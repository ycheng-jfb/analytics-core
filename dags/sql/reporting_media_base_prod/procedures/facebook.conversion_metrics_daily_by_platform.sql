CREATE OR REPLACE TEMP TABLE _actions_by_platform AS
SELECT COALESCE(a.account_id, b.account_id) AS account_id,
    COALESCE(a.ad_id, b.ad_id) AS ad_id,
    COALESCE(a.date, b.date) AS DATE,
    COALESCE(a.actions_action_type,b.actions_action_type) AS actions_action_type,
    COALESCE(a.conversions_action_type,b.conversions_action_type) AS conversions_action_type,
    COALESCE(a.conversion_values_action_type,b.conversion_values_action_type) AS conversion_values_action_type,
    COALESCE(a.publisher_platform, b.publisher_platform) publisher_platform,
    COALESCE(a.platform_position, b.platform_position) platform_position,
    COALESCE(a.impression_device, b.impression_device) impression_device,
    COALESCE(a.actions_value,b.actions_value) actions_value,
    a.actions_1d_view,
    a.actions_1d_click,
    b.actions_7d_view,
    b.actions_7d_click,
    COALESCE(a.conversions_value,b.conversions_value) conversions_value,
    a.conversions_1d_view,
    a.conversions_1d_click,
    b.conversions_7d_view,
    b.conversions_7d_click,
    COALESCE(a.conversion_values_value,b.conversion_values_value) conversion_values_value,
    a.conversion_values_1d_view,
    a.conversion_values_1d_click,
    b.conversion_values_7d_view,
    b.conversion_values_7d_click,
    COALESCE(a.meta_create_datetime,b.meta_create_datetime) meta_create_datetime,
    COALESCE(a.meta_update_datetime,b.meta_update_datetime) meta_update_datetime
FROM LAKE_VIEW.FACEBOOK.ACTIONS_BY_PLATFORM_1_D a
    FULL JOIN LAKE_VIEW.FACEBOOK.ACTIONS_BY_PLATFORM_7_D b ON a.ad_id=b.ad_id AND a.date=b.date
    AND (a.actions_action_type=b.actions_action_type OR a.conversions_action_type=b.conversions_action_type
         OR a.conversion_values_action_type = b.conversion_values_action_type)
    AND a.publisher_platform = b.publisher_platform
    AND a.platform_position = b.platform_position
    AND a.impression_device = b.impression_device;

CREATE OR REPLACE TEMPORARY TABLE _action_types AS
select
    ACTIONS_action_type as "action_type",
    ACCOUNT_ID,
    AD_ID,
    date,
    publisher_platform,
    platform_position,
    impression_device,
    ACTIONS_value value,
    actions_1d_click "1d_click",
    actions_7d_click "7d_click",
    actions_1d_view "1d_view",
    actions_7d_view "7d_view"
from _actions_by_platform;

CREATE OR REPLACE TEMPORARY TABLE _conversion_types AS
select
    conversions_action_type AS "action_type",
    ACCOUNT_ID,
    AD_ID,
    date,
    publisher_platform,
    platform_position,
    impression_device,
    conversions_value as "value",
    conversions_1d_click as "1d_click",
    conversions_7d_click as "7d_click",
    conversions_1d_view as "1d_view",
    conversions_7d_view as "7d_view"
from _actions_by_platform;

CREATE OR REPLACE TEMPORARY TABLE _conversion_values AS
select
    conversion_values_action_type AS "action_type",
    ACCOUNT_ID,
    AD_ID,
    date,
    publisher_platform,
    platform_position,
    impression_device,
    conversion_values_value as "value",
    conversion_values_1d_click as "1d_click",
    conversion_values_7d_click as "7d_click",
    conversion_values_1d_view as "1d_view",
    conversion_values_7d_view as "7d_view"
from _actions_by_platform;

--------------------------------------------------------------------


create or replace temporary table _pixel_lead as
select
    account_id,
    ad_id,
    date,
    publisher_platform,
    platform_position,
    impression_device,
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
    publisher_platform,
    platform_position,
    impression_device,
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
    publisher_platform,
    platform_position,
    impression_device,
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
    publisher_platform,
    platform_position,
    impression_device,
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
    publisher_platform,
    platform_position,
    impression_device,
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
    publisher_platform,
    platform_position,
    impression_device,
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
    publisher_platform,
    platform_position,
    impression_device,
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
    publisher_platform,
    platform_position,
    impression_device
from _pixel_lead
union
select distinct
    account_id,
    ad_id,
    date,
    publisher_platform,
    platform_position,
    impression_device
from _pixel_new_vip
union
select distinct
    account_id,
    ad_id,
    date,
    publisher_platform,
    platform_position,
    impression_device
from _pixel_purchase
union
select distinct
    account_id,
    ad_id,
    date,
    publisher_platform,
    platform_position,
    impression_device
from _pixel_purchase_new_vip
union
select distinct
    account_id,
    ad_id,
    date,
    publisher_platform,
    platform_position,
    impression_device
from _pixel_subscribe
union
select distinct
    account_id,
    ad_id,
    date,
    publisher_platform,
    platform_position,
    impression_device
from _pixel_subscribe_revenue;

----------------------------------------------------------------------------

set ios_rollout = '2021-04-19';

create or replace temporary table _all_pixel_events as
select
    p.account_id,
    p.ad_id,
    p.date,
    p.publisher_platform,
    p.platform_position,
    p.impression_device,

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
    and pl.publisher_platform = p.publisher_platform
    and pl.platform_position = p.platform_position
    and pl.impression_device = p.impression_device
left join _pixel_purchase pl3 on pl3.ad_id = p.ad_id
    and pl3.date = p.date
    and pl3.publisher_platform = p.publisher_platform
    and pl3.platform_position = p.platform_position
    and pl3.impression_device = p.impression_device
left join _pixel_new_vip pl2 on pl2.ad_id = p.ad_id
    and pl2.date = p.date
    and pl2.publisher_platform = p.publisher_platform
    and pl2.platform_position = p.platform_position
    and pl2.impression_device = p.impression_device
left join _pixel_purchase_new_vip pl4 on pl4.ad_id = p.ad_id
    and pl4.date = p.date
    and pl4.publisher_platform = p.publisher_platform
    and pl4.platform_position = p.platform_position
    and pl4.impression_device = p.impression_device
left join _pixel_subscribe pl5 on pl5.ad_id = p.ad_id
    and pl5.date = p.date
    and pl5.publisher_platform = p.publisher_platform
    and pl5.platform_position = p.platform_position
    and pl5.impression_device = p.impression_device
left join _pixel_subscribe_revenue pl6 on pl6.ad_id = p.ad_id
    and pl6.date = p.date
    and pl6.publisher_platform = p.publisher_platform
    and pl6.platform_position = p.platform_position
    and pl6.impression_device = p.impression_device;

----------------------------------------------------------------------------

set meta_update_datetime = (select max(meta_update_datetime) from _actions_by_platform);

create or replace transient table reporting_media_base_prod.facebook.conversion_metrics_daily_by_platform as
select
    store_id,
    account_id,
    ad_id,
    publisher_platform,
    platform_position,
    impression_device,
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
        store_id,
        account_id,
        ad_id,
        publisher_platform,
        platform_position,
        impression_device,
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
    group by 1,2,3,4,5,6,7
);
