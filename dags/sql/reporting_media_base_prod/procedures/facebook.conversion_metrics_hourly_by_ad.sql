CREATE OR REPLACE TEMP TABLE _actions_by_hour AS
SELECT COALESCE(a.account_id, b.account_id) AS account_id,
    COALESCE(a.ad_id, b.ad_id) AS ad_id,
    COALESCE(a.date, b.date) AS DATE,
    COALESCE(a.actions_action_type,b.actions_action_type) AS actions_action_type,
    COALESCE(a.conversions_action_type,b.conversions_action_type) AS conversions_action_type,
    COALESCE(a.conversion_values_action_type,b.conversion_values_action_type) AS conversion_values_action_type,
    COALESCE(a.actions_values_action_type, b.actions_values_action_type) actions_values_action_type,
    COALESCE(a.hourly_stats_aggregated_by_advertiser_time_zone, b.hourly_stats_aggregated_by_advertiser_time_zone) hourly_stats_aggregated_by_advertiser_time_zone,
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
    COALESCE(a.actions_values_value, b.actions_values_value) actions_values_value,
    a.actions_values_1d_view,
    a.actions_values_1d_click,
    b.actions_values_7d_view,
    b.actions_values_7d_click,
    COALESCE(a.meta_create_datetime,b.meta_create_datetime) meta_create_datetime,
    COALESCE(a.meta_update_datetime,b.meta_update_datetime) meta_update_datetime
FROM LAKE_VIEW.FACEBOOK.ACTIONS_BY_HOUR_1_D a
    FULL JOIN LAKE_VIEW.FACEBOOK.ACTIONS_BY_HOUR_7_D b
ON a.ad_id=b.ad_id AND a.date=b.date
    AND (a.actions_action_type=b.actions_action_type OR a.conversions_action_type=b.conversions_action_type OR a.conversion_values_action_type = b.conversion_values_action_type OR a.actions_values_action_type=b.actions_values_action_type)
    AND a.hourly_stats_aggregated_by_advertiser_time_zone=b.hourly_stats_aggregated_by_advertiser_time_zone;


CREATE OR REPLACE TEMPORARY TABLE _action_types AS
select
    ACTIONS_action_type as "action_type",
    ACCOUNT_ID,
    AD_ID,
    DATE,
    LEFT(hourly_stats_aggregated_by_advertiser_time_zone,2) HOUR,
    ACTIONS_value value,
    actions_1d_click "1d_click",
    actions_7d_click "7d_click",
    actions_1d_view "1d_view",
    actions_7d_view "7d_view"
from _actions_by_hour;


CREATE OR REPLACE TEMPORARY TABLE _conversion_types AS
select
    conversions_action_type AS "action_type",
    ACCOUNT_ID,
    AD_ID,
    DATE,
    LEFT(hourly_stats_aggregated_by_advertiser_time_zone,2) HOUR,
    conversions_value as "value",
    conversions_1d_click as "1d_click",
    conversions_7d_click as "7d_click",
    conversions_1d_view as "1d_view",
    conversions_7d_view as "7d_view"
from _actions_by_hour;


CREATE OR REPLACE TEMPORARY TABLE _action_values AS
select
    ACTIONS_values_action_type as "action_type",
    ACCOUNT_ID,
    AD_ID,
    DATE,
    LEFT(hourly_stats_aggregated_by_advertiser_time_zone,2) HOUR,
    ACTIONS_values_value value,
    actions_values_1d_click "1d_click",
    actions_values_7d_click "7d_click",
    actions_values_1d_view "1d_view",
    actions_values_7d_view "7d_view"
from _actions_by_hour;

CREATE OR REPLACE TEMPORARY TABLE _conversion_values AS
select
    conversion_values_action_type AS "action_type",
    ACCOUNT_ID,
    AD_ID,
    DATE,
    LEFT(hourly_stats_aggregated_by_advertiser_time_zone,2) HOUR,
    conversion_values_value as "value",
    conversion_values_1d_click as "1d_click",
    conversion_values_7d_click as "7d_click",
    conversion_values_1d_view as "1d_view",
    conversion_values_7d_view as "7d_view"
from _actions_by_hour;

--------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_lead AS
SELECT
    ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR,
    COALESCE(att."1d_click",0)AS PIXEL_LEAD_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_LEAD_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_LEAD_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_LEAD_VIEW_7D
FROM _action_types att
WHERE "action_type" = 'offsite_conversion.fb_pixel_complete_registration';

----------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_purchase AS
SELECT
    ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR,
    COALESCE(att."1d_click",0)AS PIXEL_PURCHASE_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_PURCHASE_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_PURCHASE_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_PURCHASE_VIEW_7D
FROM _action_types att
WHERE "action_type" = 'offsite_conversion.fb_pixel_purchase';

--------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_purchase_new_vip AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR,
    COALESCE(att."1d_click",0)AS PIXEL_PURCHASE_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_PURCHASE_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_PURCHASE_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_PURCHASE_VIEW_7D
FROM _action_types att
JOIN lake_view.sharepoint.med_facebook_conversion_mapping p ON att.ACCOUNT_ID = p.ACCOUNT_ID
    AND p.MAP_TO_COLUMN ilike 'New VIP%'
    AND att.DATE < p.FIRST_DAY_USED
WHERE "action_type" = 'offsite_conversion.fb_pixel_purchase';


INSERT INTO _pixel_purchase_new_vip
(SELECT
    pur.ACCOUNT_ID,
    pur.AD_ID,
    pur.DATE,
    HOUR,
    PIXEL_PURCHASE_CLICK_1D,
    PIXEL_PURCHASE_VIEW_1D,
    PIXEL_PURCHASE_CLICK_7D,
    PIXEL_PURCHASE_VIEW_7D
FROM _pixel_purchase pur
LEFT JOIN lake_view.sharepoint.med_facebook_conversion_mapping p ON pur.ACCOUNT_ID = p.ACCOUNT_ID
    AND p.MAP_TO_COLUMN ilike 'New VIP%'
WHERE p.ACCOUNT_ID IS NULL);

----------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_new_vip AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_VIP_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_VIP_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_VIP_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_VIP_VIEW_7D
FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN ilike 'New VIP%'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;

----------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_subscribe AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR,
    COALESCE(att."1d_click",0)AS PIXEL_SUBSCRIBE_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_SUBSCRIBE_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_SUBSCRIBE_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_SUBSCRIBE_VIEW_7D
FROM _conversion_types att
WHERE "action_type" = 'subscribe_website';

----------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_subscribe_revenue AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR,
    COALESCE(att."1d_click",0)AS PIXEL_SUBSCRIBE_REVENUE_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_SUBSCRIBE_REVENUE_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_SUBSCRIBE_REVENUE_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_SUBSCRIBE_REVENUE_VIEW_7D
FROM _conversion_values att
WHERE "action_type" = 'subscribe_total';

----------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_purchase_revenue AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR,
    COALESCE(att."1d_click",0)AS PIXEL_PURCHASE_REVENUE_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_PURCHASE_REVENUE_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_PURCHASE_REVENUE_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_PURCHASE_REVENUE_VIEW_7D
FROM _action_values att
WHERE "action_type" = 'offsite_conversion.fb_pixel_purchase';

----------------------------------------------------------------------------

create or replace TEMPORARY table _combos AS
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR
FROM _pixel_lead
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR
FROM _pixel_new_vip
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR
FROM _pixel_purchase
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR
FROM _pixel_purchase_new_vip
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR
FROM _pixel_subscribe
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR
FROM _pixel_subscribe_revenue
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR
FROM _pixel_purchase_revenue;


----------------------------------------------------------------------------

set ios_rollout = '2021-04-19';

CREATE OR REPLACE TEMPORARY TABLE _all_pixel_events AS
select
    p.ACCOUNT_ID,
    p.AD_ID,
    p.DATE,
    p.HOUR,
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

    coalesce(pl5.PIXEL_SUBSCRIBE_CLICK_1D,0) as pixel_subscribe_click_1d,
    coalesce(pl5.PIXEL_SUBSCRIBE_VIEW_1D,0) as pixel_subscribe_view_1d,
    coalesce(pl5.PIXEL_SUBSCRIBE_CLICK_7D,0) as pixel_subscribe_click_7d,
    coalesce(pl5.PIXEL_SUBSCRIBE_VIEW_7D,0) as pixel_subscribe_view_7d,

    --use subscribe revenue prior to ios rollouts, use purchase revenue after
    case when p.date <= $ios_rollout then coalesce(rev.pixel_subscribe_revenue_CLICK_1D,0)
        else coalesce(prev.pixel_purchase_revenue_CLICK_1D,0) end as pixel_subscribe_revenue_click_1d,
    case when p.date <= $ios_rollout then coalesce(rev.pixel_subscribe_revenue_VIEW_1D,0)
        else coalesce(prev.pixel_purchase_revenue_VIEW_1D,0) end as pixel_subscribe_revenue_view_1d,
    case when p.date <= $ios_rollout then coalesce(rev.pixel_subscribe_revenue_CLICK_7D,0)
        else coalesce(prev.pixel_purchase_revenue_CLICK_7D,0) end as pixel_subscribe_revenue_click_7d,
    case when p.date <= $ios_rollout then coalesce(rev.pixel_subscribe_revenue_VIEW_7D,0)
        else coalesce(prev.pixel_purchase_revenue_VIEW_7D,0) end as pixel_subscribe_revenue_view_7d
from _combos p
LEFT JOIN _pixel_lead pl on pl.ad_id = p.AD_ID
    and pl.DATE = p.DATE
    and pl.ACCOUNT_ID = p.ACCOUNT_ID
    and pl.HOUR = p.HOUR
LEFT JOIN _pixel_purchase pl3 on pl3.ad_id = p.AD_ID
    and pl3.DATE = p.DATE
    and pl3.ACCOUNT_ID = p.ACCOUNT_ID
    and pl3.HOUR = p.HOUR
LEFT JOIN _pixel_new_vip pl2 on pl2.AD_ID = p.AD_ID
    and pl2.DATE = p.DATE
    and pl2.ACCOUNT_ID = p.ACCOUNT_ID
    and pl2.HOUR = p.HOUR
LEFT JOIN _pixel_purchase_new_vip pl4 on pl4.AD_ID = p.AD_ID
    and pl4.DATE = p.DATE
    and pl4.ACCOUNT_ID = p.ACCOUNT_ID
    and pl4.HOUR = p.HOUR
LEFT JOIN _pixel_subscribe pl5 on pl5.AD_ID = p.AD_ID
    and pl5.DATE = p.DATE
    and pl5.ACCOUNT_ID = p.ACCOUNT_ID
    and pl5.HOUR = p.HOUR
LEFT JOIN _pixel_subscribe_revenue rev on rev.AD_ID = p.AD_ID
    and rev.DATE = p.date
    and rev.ACCOUNT_ID = p.ACCOUNT_ID
    and rev.HOUR = p.HOUR
LEFT JOIN _pixel_purchase_revenue prev on prev.AD_ID = p.AD_ID
    and prev.DATE = p.date
    and prev.ACCOUNT_ID = p.ACCOUNT_ID
    and prev.HOUR = p.HOUR;


----------------------------------------------------------------------------

SET meta_update_datetime = (SELECT MAX(META_UPDATE_DATETIME) FROM _ACTIONS_BY_HOUR);

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.FACEBOOK.CONVERSION_METRICS_HOURLY_BY_AD AS
SELECT
    STORE_ID,
    ACCOUNT_ID,
    AD_ID,
    DATE,
    HOUR,
    PIXEL_LEAD_CLICK_1D,
    PIXEL_LEAD_VIEW_1D,
    PIXEL_LEAD_CLICK_7D,
    PIXEL_LEAD_VIEW_7D,
    PIXEL_VIP_CLICK_1D,
    PIXEL_VIP_VIEW_1D,
    PIXEL_VIP_CLICK_7D,
    PIXEL_VIP_VIEW_7D,
    PIXEL_PURCHASE_CLICK_1D,
    PIXEL_PURCHASE_VIEW_1D,
    PIXEL_PURCHASE_CLICK_7D,
    PIXEL_PURCHASE_VIEW_7D,
    PIXEL_SUBSCRIBE_CLICK_1D,
    PIXEL_SUBSCRIBE_VIEW_1D,
    PIXEL_SUBSCRIBE_CLICK_7D,
    PIXEL_SUBSCRIBE_VIEW_7D,
    PIXEL_SUBSCRIBE_REVENUE_CLICK_1D,
    PIXEL_SUBSCRIBE_REVENUE_VIEW_1D,
    PIXEL_SUBSCRIBE_REVENUE_CLICK_7D,
    PIXEL_SUBSCRIBE_REVENUE_VIEW_7D,

    $meta_update_datetime AS PIXEL_CONVERSION_META_UPDATE_DATETIME,
    CURRENT_TIMESTAMP()::timestamp_ltz AS META_CREATE_DATETIME,
    CURRENT_TIMESTAMP()::timestamp_ltz AS META_UPDATE_DATETIME

FROM
(
    SELECT
        STORE_ID,
        account_id,
        ad_id,
        date,
        HOUR,

        SUM(PIXEL_LEAD_CLICK_1D)      AS PIXEL_LEAD_CLICK_1D,
        SUM(PIXEL_LEAD_VIEW_1D)       AS PIXEL_LEAD_VIEW_1D,
        SUM(PIXEL_LEAD_CLICK_7D)      AS PIXEL_LEAD_CLICK_7D,
        SUM(PIXEL_LEAD_VIEW_7D)       AS PIXEL_LEAD_VIEW_7D,

        SUM(PIXEL_PURCHASE_CLICK_1D)  AS PIXEL_PURCHASE_CLICK_1D,
        SUM(PIXEL_PURCHASE_VIEW_1D)   AS PIXEL_PURCHASE_VIEW_1D,
        SUM(PIXEL_PURCHASE_CLICK_7D)  AS PIXEL_PURCHASE_CLICK_7D,
        SUM(PIXEL_PURCHASE_VIEW_7D)   AS PIXEL_PURCHASE_VIEW_7D,

        SUM(PIXEL_VIP_CLICK_1D)       AS PIXEL_VIP_CLICK_1D,
        SUM(PIXEL_VIP_VIEW_1D)        AS PIXEL_VIP_VIEW_1D,
        SUM(PIXEL_VIP_CLICK_7D)       AS PIXEL_VIP_CLICK_7D,
        SUM(PIXEL_VIP_VIEW_7D)        AS PIXEL_VIP_VIEW_7D,

        SUM(PIXEL_SUBSCRIBE_CLICK_1D)   AS PIXEL_SUBSCRIBE_CLICK_1D,
        SUM(PIXEL_SUBSCRIBE_VIEW_1D)    AS PIXEL_SUBSCRIBE_VIEW_1D,
        SUM(PIXEL_SUBSCRIBE_CLICK_7D)   AS PIXEL_SUBSCRIBE_CLICK_7D,
        SUM(PIXEL_SUBSCRIBE_VIEW_7D)    AS PIXEL_SUBSCRIBE_VIEW_7D,

        SUM(PIXEL_SUBSCRIBE_REVENUE_CLICK_1D)   AS PIXEL_SUBSCRIBE_REVENUE_CLICK_1D,
        SUM(PIXEL_SUBSCRIBE_REVENUE_VIEW_1D)    AS PIXEL_SUBSCRIBE_REVENUE_VIEW_1D,
        SUM(PIXEL_SUBSCRIBE_REVENUE_CLICK_7D)   AS PIXEL_SUBSCRIBE_REVENUE_CLICK_7D,
        SUM(PIXEL_SUBSCRIBE_REVENUE_VIEW_7D)    AS PIXEL_SUBSCRIBE_REVENUE_VIEW_7D

    from _all_pixel_events p
    join lake_view.sharepoint.med_account_mapping_media am on am.source_id = p.account_id
        and am.source ilike '%facebook%'
     GROUP BY 1,2,3,4,5
);
