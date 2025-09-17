CREATE OR REPLACE TEMP TABLE _actions_by_country AS
SELECT COALESCE(a.account_id, b.account_id) AS account_id,
    COALESCE(a.ad_id, b.ad_id) AS ad_id,
    COALESCE(a.date, b.date) AS DATE,
    COALESCE(a.country,b.country) AS country,
    COALESCE(a.actions_action_type,b.actions_action_type) AS actions_action_type,
    COALESCE(a.conversions_action_type,b.conversions_action_type) AS conversions_action_type,
    COALESCE(a.conversion_values_action_type,b.conversion_values_action_type) AS conversion_values_action_type,
    COALESCE(a.actions_values_action_type, b.actions_values_action_type) actions_values_action_type,
    COALESCE(a.video_play_action_type, b.video_play_action_type) video_play_action_type,
    COALESCE(a.video_play_value, b.video_play_value) video_play_value,
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
FROM LAKE_VIEW.FACEBOOK.ACTIONS_BY_COUNTRY_1_D a
    FULL JOIN LAKE_VIEW.FACEBOOK.ACTIONS_BY_COUNTRY_7_D b
ON a.ad_id=b.ad_id AND a.date=b.date
    AND (a.actions_action_type=b.actions_action_type OR a.conversions_action_type=b.conversions_action_type
    OR a.conversion_values_action_type = b.conversion_values_action_type
    OR a.actions_values_action_type=b.actions_values_action_type OR a.video_play_action_type=b.video_play_action_type)
    AND a.country=b.country;

CREATE OR REPLACE TEMPORARY TABLE _action_types AS
select
    ACTIONS_action_type as "action_type",
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    ACTIONS_value value,
    actions_1d_click "1d_click",
    actions_7d_click "7d_click",
    actions_1d_view "1d_view",
    actions_7d_view "7d_view"
from _actions_by_country;

CREATE OR REPLACE TEMPORARY TABLE _conversion_types AS
select
    conversions_action_type AS "action_type",
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    conversions_value as "value",
    conversions_1d_click as "1d_click",
    conversions_7d_click as "7d_click",
    conversions_1d_view as "1d_view",
    conversions_7d_view as "7d_view"
from _actions_by_country;

CREATE OR REPLACE TEMPORARY TABLE _action_values AS
select
    ACTIONS_values_action_type as "action_type",
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    ACTIONS_values_value value,
    actions_values_1d_click "1d_click",
    actions_values_7d_click "7d_click",
    actions_values_1d_view "1d_view",
    actions_values_7d_view "7d_view"
from _actions_by_country;

CREATE OR REPLACE TEMPORARY TABLE _conversion_values AS
select
    conversion_values_action_type AS "action_type",
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    conversion_values_value as "value",
    conversion_values_1d_click as "1d_click",
    conversion_values_7d_click as "7d_click",
    conversion_values_1d_view as "1d_view",
    conversion_values_7d_view as "7d_view"
from _actions_by_country;


--------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_view_content AS
SELECT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    COALESCE(att."1d_click",0)AS PIXEL_VIEW_CONTENT_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_VIEW_CONTENT_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_VIEW_CONTENT_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_VIEW_CONTENT_VIEW_7D
FROM _action_types att
WHERE "action_type" = 'offsite_conversion.fb_pixel_view_content';

--------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_landing_page_view AS
SELECT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    COALESCE(att."1d_click",0)AS PIXEL_LANDING_PAGE_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_LANDING_PAGE_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_LANDING_PAGE_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_LANDING_PAGE_VIEW_7D
FROM _action_types att
WHERE "action_type" = 'landing_page_view';

--------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_add_to_cart AS
SELECT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    COALESCE(att."1d_click",0)AS PIXEL_ADD_TO_CART_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_ADD_TO_CART_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_ADD_TO_CART_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_ADD_TO_CART_VIEW_7D
FROM _action_types att
WHERE "action_type" = 'offsite_conversion.fb_pixel_add_to_cart';

--------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_lead AS
SELECT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
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
    COUNTRY,
    DATE,
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
    COUNTRY,
    DATE,
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
    COUNTRY,
    pur.DATE,
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
    COUNTRY,
    DATE,
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
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    COALESCE(att."1d_click",0)AS PIXEL_SUBSCRIBE_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_SUBSCRIBE_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_SUBSCRIBE_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_SUBSCRIBE_VIEW_7D
FROM _conversion_types att
WHERE "action_type" = 'subscribe_website';

----------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_D1_new_vip AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_D1_VIP_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_D1_VIP_VIEW_1D

FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN ilike 'D1NewVIP%'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;


CREATE OR REPLACE TEMPORARY TABLE _pixel_M1_new_vip AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_M1_VIP_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_M1_VIP_VIEW_1D

FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN ilike 'M1NewVIP%'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;

----------------------------------------------------------------------------
-- Fabletics Men

CREATE OR REPLACE TEMPORARY TABLE _pixel_subscribe_male AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_SUBSCRIBE_MEN_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_SUBSCRIBE_MEN_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_SUBSCRIBE_MEN_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_SUBSCRIBE_MEN_VIEW_7D
FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN = 'Subscribe Male'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;


CREATE OR REPLACE TEMPORARY TABLE _pixel_complete_reg_male AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_LEAD_MEN_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_LEAD_MEN_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_LEAD_MEN_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_LEAD_MEN_VIEW_7D
FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN = 'Complete Reg Male'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;

CREATE OR REPLACE TEMPORARY TABLE _pixel_complete_reg_female AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_LEAD_WOMEN_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_LEAD_WOMEN_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_LEAD_WOMEN_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_LEAD_WOMEN_VIEW_7D
FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN = 'Complete Reg Female'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;


CREATE OR REPLACE TEMPORARY TABLE _pixel_add_to_cart_male AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_ATC_MEN_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_ATC_MEN_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_ATC_MEN_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_ATC_MEN_VIEW_7D
FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN = 'Add to Cart Male'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;


CREATE OR REPLACE TEMPORARY TABLE _pixel_add_to_cart_female AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_ATC_WOMEN_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_ATC_WOMEN_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_ATC_WOMEN_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_ATC_WOMEN_VIEW_7D
FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN = 'Add to Cart Female'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;


CREATE OR REPLACE TEMPORARY TABLE _pixel_purchase_male AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_PURCHASE_MEN_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_PURCHASE_MEN_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_PURCHASE_MEN_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_PURCHASE_MEN_VIEW_7D
FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN = 'Purchase Male'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;


CREATE OR REPLACE TEMPORARY TABLE _pixel_purchase_female AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_PURCHASE_WOMEN_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_PURCHASE_WOMEN_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_PURCHASE_WOMEN_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_PURCHASE_WOMEN_VIEW_7D
FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN = 'Purchase Female'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;


CREATE OR REPLACE TEMPORARY TABLE _pixel_view_content_male AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_VIEW_CONTENT_MEN_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_VIEW_CONTENT_MEN_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_VIEW_CONTENT_MEN_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_VIEW_CONTENT_MEN_VIEW_7D
FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN = 'View Content Male'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;


CREATE OR REPLACE TEMPORARY TABLE _pixel_view_content_female AS
SELECT
    att.ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    p.FIRST_DAY_USED,
    COALESCE(att."1d_click",0)AS PIXEL_VIEW_CONTENT_WOMEN_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_VIEW_CONTENT_WOMEN_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_VIEW_CONTENT_WOMEN_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_VIEW_CONTENT_WOMEN_VIEW_7D
FROM _action_types att
CROSS JOIN lake_view.sharepoint.med_facebook_conversion_mapping p WHERE p.custom_conversion_id = REPLACE("action_type",'offsite_conversion.custom.','')
    AND "action_type" ilike '%offsite_conversion.custom%'
    AND p.FIRST_DAY_USED <= att.DATE
    AND p.MAP_TO_COLUMN = 'View Content Female'
    AND p.ACCOUNT_ID = att.ACCOUNT_ID;


----------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _video_view_3s AS
SELECT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    COALESCE(att."1d_click",0)AS PIXEL_VIDEO_VIEW_3S_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_VIDEO_VIEW_3S_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_VIDEO_VIEW_3S_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_VIDEO_VIEW_3S_VIEW_7D
FROM _action_types att
WHERE "action_type" = 'video_view';

----------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_subscribe_revenue AS
SELECT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
    COALESCE(att."1d_click",0)AS PIXEL_SUBSCRIBE_REVENUE_CLICK_1D,
    COALESCE(att."1d_view",0) AS PIXEL_SUBSCRIBE_REVENUE_VIEW_1D,
    COALESCE(att."7d_click",0) AS PIXEL_SUBSCRIBE_REVENUE_CLICK_7D,
    COALESCE(att."7d_view",0) AS PIXEL_SUBSCRIBE_REVENUE_VIEW_7D
FROM _conversion_values att
WHERE "action_type" = 'subscribe_total';

----------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pixel_purchase_revenue AS
SELECT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE,
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
    COUNTRY,
    DATE
FROM _pixel_lead
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_new_vip
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_purchase
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_purchase_new_vip
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_subscribe
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_M1_new_vip
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_D1_new_vip
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_subscribe_male
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_complete_reg_male
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_complete_reg_female
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_add_to_cart_male
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_add_to_cart_female
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_purchase_male
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_purchase_female
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_view_content_male
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_view_content_female
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_subscribe_revenue
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_purchase_revenue
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_add_to_cart
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_landing_page_view
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _pixel_view_content
UNION
SELECT DISTINCT
    ACCOUNT_ID,
    AD_ID,
    COUNTRY,
    DATE
FROM _video_view_3s;

----------------------------------------------------------------------------

set ios_rollout = '2021-04-19';

CREATE OR REPLACE TEMPORARY TABLE _all_pixel_events AS
select
    p.ACCOUNT_ID,
    p.AD_ID,
    p.COUNTRY,
    p.DATE,

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

    -- old vip event
    coalesce(pl2.pixel_vip_click_1d,pl4.PIXEL_PURCHASE_CLICK_1D,0) as pixel_vip_click_1d_old,
    coalesce(pl2.pixel_vip_view_1d,pl4.PIXEL_PURCHASE_VIEW_1D,0) as pixel_vip_view_1d_old,
    coalesce(pl2.pixel_vip_click_7d,pl4.PIXEL_PURCHASE_CLICK_7D,0) as pixel_vip_click_7d_old,
    coalesce(pl2.pixel_vip_view_7d,pl4.PIXEL_PURCHASE_VIEW_7D,0) as pixel_vip_view_7d_old,

    coalesce(pl17.PIXEL_SUBSCRIBE_CLICK_1D,0) as pixel_subscribe_click_1d,
    coalesce(pl17.PIXEL_SUBSCRIBE_VIEW_1D,0) as pixel_subscribe_view_1d,
    coalesce(pl17.PIXEL_SUBSCRIBE_CLICK_7D,0) as pixel_subscribe_click_7d,
    coalesce(pl17.PIXEL_SUBSCRIBE_VIEW_7D,0) as pixel_subscribe_view_7d,

    coalesce(pl5.PIXEL_D1_VIP_CLICK_1D,0) as pixel_d1_vip_click_1d,
    coalesce(pl5.PIXEL_D1_VIP_VIEW_1D,0) as pixel_d1_vip_view_1d,

    coalesce(pl6.PIXEL_M1_VIP_CLICK_1D,0) as pixel_m1_vip_click_1d,
    coalesce(pl6.PIXEL_M1_VIP_VIEW_1D,0) as pixel_m1_vip_view_1d,

    coalesce(pl7.PIXEL_SUBSCRIBE_MEN_CLICK_1D,0) as pixel_subscribe_men_click_1d,
    coalesce(pl7.PIXEL_SUBSCRIBE_MEN_VIEW_1D,0) as pixel_subscribe_men_view_1d,
    coalesce(pl7.PIXEL_SUBSCRIBE_MEN_CLICK_7D,0) as pixel_subscribe_men_click_7d,
    coalesce(pl7.PIXEL_SUBSCRIBE_MEN_VIEW_7D,0) as pixel_subscribe_men_view_7d,

    coalesce(pl8.PIXEL_LEAD_MEN_CLICK_1D,0) as pixel_lead_men_click_1d,
    coalesce(pl8.PIXEL_LEAD_MEN_VIEW_1D,0) as pixel_lead_men_view_1d,
    coalesce(pl8.PIXEL_LEAD_MEN_CLICK_7D,0) as pixel_lead_men_click_7d,
    coalesce(pl8.PIXEL_LEAD_MEN_VIEW_7D,0) as pixel_lead_men_view_7d,
    coalesce(pl9.PIXEL_LEAD_WOMEN_CLICK_1D,0) as pixel_lead_women_click_1d,
    coalesce(pl9.PIXEL_LEAD_WOMEN_VIEW_1D,0) as pixel_lead_women_view_1d,
    coalesce(pl9.PIXEL_LEAD_WOMEN_CLICK_7D,0) as pixel_lead_women_click_7d,
    coalesce(pl9.PIXEL_LEAD_WOMEN_VIEW_7D,0) as pixel_lead_women_view_7d,

    coalesce(pl13.PIXEL_PURCHASE_MEN_CLICK_1D,0) as pixel_purchase_men_click_1d,
    coalesce(pl13.PIXEL_PURCHASE_MEN_VIEW_1D,0) as pixel_purchase_men_view_1d,
    coalesce(pl13.PIXEL_PURCHASE_MEN_CLICK_7D,0) as pixel_purchase_men_click_7d,
    coalesce(pl13.PIXEL_PURCHASE_MEN_VIEW_7D,0) as pixel_purchase_men_view_7d,
    coalesce(pl14.PIXEL_PURCHASE_WOMEN_CLICK_1D,0) as pixel_purchase_women_click_1d,
    coalesce(pl14.PIXEL_PURCHASE_WOMEN_VIEW_1D,0) as pixel_purchase_women_view_1d,
    coalesce(pl14.PIXEL_PURCHASE_WOMEN_CLICK_7D,0) as pixel_purchase_women_click_7d,
    coalesce(pl14.PIXEL_PURCHASE_WOMEN_VIEW_7D,0) as pixel_purchase_women_view_7d,

    coalesce(pl10.PIXEL_ATC_MEN_CLICK_1D,0) as pixel_atc_men_click_1d,
    coalesce(pl10.PIXEL_ATC_MEN_VIEW_1D,0) as pixel_atc_men_view_1d,
    coalesce(pl10.PIXEL_ATC_MEN_CLICK_7D,0) as pixel_atc_men_click_7d,
    coalesce(pl10.PIXEL_ATC_MEN_VIEW_7D,0) as pixel_atc_men_view_7d,
    coalesce(pl12.PIXEL_ATC_WOMEN_CLICK_1D,0) as pixel_atc_women_click_1d,
    coalesce(pl12.PIXEL_ATC_WOMEN_VIEW_1D,0) as pixel_atc_women_view_1d,
    coalesce(pl12.PIXEL_ATC_WOMEN_CLICK_7D,0) as pixel_atc_women_click_7d,
    coalesce(pl12.PIXEL_ATC_WOMEN_VIEW_7D,0) as pixel_atc_women_view_7d,

    coalesce(pl15.PIXEL_VIEW_CONTENT_MEN_CLICK_1D,0) as pixel_view_content_men_click_1d,
    coalesce(pl15.PIXEL_VIEW_CONTENT_MEN_VIEW_1D,0) as pixel_view_content_men_view_1d,
    coalesce(pl15.PIXEL_VIEW_CONTENT_MEN_CLICK_7D,0) as pixel_view_content_men_click_7d,
    coalesce(pl15.PIXEL_VIEW_CONTENT_MEN_VIEW_7D,0) as pixel_view_content_men_view_7d,
    coalesce(pl16.PIXEL_VIEW_CONTENT_WOMEN_CLICK_1D,0) as pixel_view_content_women_click_1d,
    coalesce(pl16.PIXEL_VIEW_CONTENT_WOMEN_VIEW_1D,0) as pixel_view_content_women_view_1d,
    coalesce(pl16.PIXEL_VIEW_CONTENT_WOMEN_CLICK_7D,0) as pixel_view_content_women_click_7d,
    coalesce(pl16.PIXEL_VIEW_CONTENT_WOMEN_VIEW_7D,0) as pixel_view_content_women_view_7d,

    --use subscribe revenue prior to ios rollouts, use purchase revenue after
    case when p.date <= $ios_rollout then coalesce(rev.pixel_subscribe_revenue_CLICK_1D,0)
        else coalesce(prev.pixel_purchase_revenue_CLICK_1D,0) end as pixel_subscribe_revenue_click_1d,
    case when p.date <= $ios_rollout then coalesce(rev.pixel_subscribe_revenue_VIEW_1D,0)
        else coalesce(prev.pixel_purchase_revenue_VIEW_1D,0) end as pixel_subscribe_revenue_view_1d,
    case when p.date <= $ios_rollout then coalesce(rev.pixel_subscribe_revenue_CLICK_7D,0)
        else coalesce(prev.pixel_purchase_revenue_CLICK_7D,0) end as pixel_subscribe_revenue_click_7d,
    case when p.date <= $ios_rollout then coalesce(rev.pixel_subscribe_revenue_VIEW_7D,0)
        else coalesce(prev.pixel_purchase_revenue_VIEW_7D,0) end as pixel_subscribe_revenue_view_7d,

    --add to cart--
    coalesce(pl18.PIXEL_ADD_TO_CART_CLICK_1D,0) as pixel_add_to_cart_click_1d,
    coalesce(pl18.PIXEL_ADD_TO_CART_VIEW_1D,0) as pixel_add_to_cart_view_1d,
    coalesce(pl18.PIXEL_ADD_TO_CART_CLICK_7D,0) as pixel_add_to_cart_click_7d,
    coalesce(pl18.PIXEL_ADD_TO_CART_VIEW_7D,0) as pixel_add_to_cart_view_7d,

    --landing page view
    coalesce(pl19.PIXEL_LANDING_PAGE_CLICK_1D,0) as pixel_landing_page_click_1d,
    coalesce(pl19.PIXEL_LANDING_PAGE_VIEW_1D,0) as pixel_landing_page_view_1d,
    coalesce(pl19.PIXEL_LANDING_PAGE_CLICK_7D,0) as pixel_landing_page_click_7d,
    coalesce(pl19.PIXEL_LANDING_PAGE_VIEW_7D,0) as pixel_landing_page_view_7d,

    --view content
    coalesce(pl20.PIXEL_VIEW_CONTENT_CLICK_1D,0) as pixel_view_content_click_1d,
    coalesce(pl20.PIXEL_VIEW_CONTENT_VIEW_1D,0) as pixel_view_content_view_1d,
    coalesce(pl20.PIXEL_VIEW_CONTENT_CLICK_7D,0) as pixel_view_content_click_7d,
    coalesce(pl20.PIXEL_VIEW_CONTENT_VIEW_7D,0) as pixel_view_content_view_7d,

    COALESCE(pl21.PIXEL_VIDEO_VIEW_3S_CLICK_1D,0)AS PIXEL_VIDEO_VIEW_3S_CLICK_1D,
    COALESCE(pl21.PIXEL_VIDEO_VIEW_3S_VIEW_1D,0) AS PIXEL_VIDEO_VIEW_3S_VIEW_1D,
    COALESCE(pl21.PIXEL_VIDEO_VIEW_3S_CLICK_7D,0) AS PIXEL_VIDEO_VIEW_3S_CLICK_7D,
    COALESCE(pl21.PIXEL_VIDEO_VIEW_3S_VIEW_7D,0) AS PIXEL_VIDEO_VIEW_3S_VIEW_7D

from _combos p
LEFT JOIN _pixel_lead pl on pl.ad_id = p.AD_ID
    and pl.date = p.date
    and pl.ACCOUNT_ID = p.ACCOUNT_ID
    and pl.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_new_vip pl2 on pl2.AD_ID = p.AD_ID
    and pl2.date = p.date
    and pl2.ACCOUNT_ID = p.ACCOUNT_ID
    and pl2.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_purchase pl3 on pl3.AD_ID = p.AD_ID
    and pl3.date = p.date
    and pl3.ACCOUNT_ID = p.ACCOUNT_ID
    and pl3.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_subscribe pl17 on pl17.AD_ID = p.AD_ID
    and pl17.date = p.date
    and pl17.ACCOUNT_ID = p.ACCOUNT_ID
    and pl17.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_purchase_new_vip pl4 on pl4.AD_ID = p.AD_ID
    and pl4.date = p.date
    and pl4.ACCOUNT_ID = p.ACCOUNT_ID
    and pl4.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_D1_new_vip pl5 on pl5.AD_ID = p.AD_ID
    and pl5.DATE = p.date
    and pl5.ACCOUNT_ID = p.ACCOUNT_ID
    and pl5.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_M1_new_vip pl6 on pl6.AD_ID = p.AD_ID
    and pl6.DATE = p.date
    and pl6.ACCOUNT_ID = p.ACCOUNT_ID
    and pl6.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_subscribe_male pl7 on pl7.AD_ID = p.AD_ID
    and pl7.DATE = p.date
    and pl7.ACCOUNT_ID = p.ACCOUNT_ID
    and pl7.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_complete_reg_male pl8 on pl8.AD_ID = p.AD_ID
    and pl8.DATE = p.date
    and pl8.ACCOUNT_ID = p.ACCOUNT_ID
    and pl8.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_complete_reg_female pl9 on pl9.AD_ID = p.AD_ID
    and pl9.DATE = p.date
    and pl9.ACCOUNT_ID = p.ACCOUNT_ID
    and pl9.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_add_to_cart_male pl10 on pl10.AD_ID = p.AD_ID
    and pl10.DATE = p.date
    and pl10.ACCOUNT_ID = p.ACCOUNT_ID
    and pl10.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_add_to_cart_female pl12 on pl12.AD_ID = p.AD_ID
    and pl12.DATE = p.date
    and pl12.ACCOUNT_ID = p.ACCOUNT_ID
    and pl12.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_purchase_male pl13 on pl13.AD_ID = p.AD_ID
    and pl13.DATE = p.date
    and pl13.ACCOUNT_ID = p.ACCOUNT_ID
    and pl13.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_purchase_female pl14 on pl14.AD_ID = p.AD_ID
    and pl14.DATE = p.date
    and pl14.ACCOUNT_ID = p.ACCOUNT_ID
    and pl14.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_view_content_male pl15 on pl15.AD_ID = p.AD_ID
    and pl15.DATE = p.date
    and pl15.ACCOUNT_ID = p.ACCOUNT_ID
    and pl15.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_view_content_female pl16 on pl16.AD_ID = p.AD_ID
    and pl16.DATE = p.date
    and pl16.ACCOUNT_ID = p.ACCOUNT_ID
    and pl16.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_subscribe_revenue rev on rev.AD_ID = p.AD_ID
    and rev.DATE = p.date
    and rev.ACCOUNT_ID = p.ACCOUNT_ID
    and rev.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_purchase_revenue prev on prev.AD_ID = p.AD_ID
    and prev.DATE = p.date
    and prev.ACCOUNT_ID = p.ACCOUNT_ID
    and prev.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_add_to_cart pl18 on pl18.AD_ID = p.AD_ID
    and pl18.DATE = p.date
    and pl18.ACCOUNT_ID = p.ACCOUNT_ID
    and pl18.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_landing_page_view pl19 on pl19.AD_ID = p.AD_ID
    and pl19.DATE = p.date
    and pl19.ACCOUNT_ID = p.ACCOUNT_ID
    and pl19.COUNTRY = p.COUNTRY
LEFT JOIN _pixel_view_content pl20 on pl20.AD_ID = p.AD_ID
    and pl20.DATE = p.date
    and pl20.ACCOUNT_ID = p.ACCOUNT_ID
    and pl20.COUNTRY = p.COUNTRY
LEFT JOIN _video_view_3s pl21 on pl21.AD_ID = p.AD_ID
    and pl21.DATE = p.date
    and pl21.ACCOUNT_ID = p.ACCOUNT_ID
    and pl21.COUNTRY = p.COUNTRY;

----------------------------------------------------------------------------
-- Country Mappings

CREATE OR REPLACE TEMPORARY TABLE _FB_Country_Mappings
    (
    store_id INT,
    start_date DATE,
    end_date DATE,
    fb_country_code VARCHAR(15),
    store_id_map_to INT
        );

INSERT INTO _FB_Country_Mappings VALUES
(26,'2010-01-01','2018-07-01','CA',41),
--(55,'2010-01-01','2079-01-01','CA',55),
(52,'2010-01-01','2018-06-01','CA',79),
--(36,'2010-01-01','2018-06-01','AT',36), -- jfde to jfat

(38,'2019-03-01','2079-01-01','DK',61), -- jfuk to jfdk
(38,'2019-03-01','2079-01-01','SE',63), -- jfuk to jfse

--(65,'2010-01-01','2018-06-01','AT',65), -- flde to flat
(139,'2019-01-01','2079-01-01','GB',133), -- sxeu to sxuk -- it's 'GB' in the FB feed
(139,'2019-01-01','2079-01-01','FR',125), -- sxeu to sxfr
(139,'2019-01-01','2079-01-01','ES',131), -- sxeu to sxes
(139,'2019-01-01','2079-01-01','DE',128), -- sxeu to sxde
(139,'2019-01-01','2079-01-01','AT',128), -- sxeu to sxat (under sxde) -- should be 12801, but doesn't exist yet
(139,'2019-01-01','2079-01-01','BE',137); -- sxeu to sxbe (under sxnl) -- should be 13701, but doesnt exist yet


----------------------------------------------------------------------------

SET high_watermark_datetime = (SELECT MAX(META_UPDATE_DATETIME) FROM _actions_by_country);

CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.facebook.conversion_metrics_daily_by_country AS
SELECT
    ACCOUNT_ID,
    AD_ID,
    STORE_ID,
    DATE,
    PIXEL_LEAD_CLICK_1D,
    PIXEL_LEAD_VIEW_1D,
    PIXEL_LEAD_CLICK_7D,
    PIXEL_LEAD_VIEW_7D,

    PIXEL_VIP_CLICK_1D,
    PIXEL_VIP_VIEW_1D,
    PIXEL_VIP_CLICK_7D,
    PIXEL_VIP_VIEW_7D,

    PIXEL_VIP_CLICK_1D_OLD,
    PIXEL_VIP_VIEW_1D_OLD,
    PIXEL_VIP_CLICK_7D_OLD,
    PIXEL_VIP_VIEW_7D_OLD,

    PIXEL_PURCHASE_CLICK_1D,
    PIXEL_PURCHASE_VIEW_1D,
    PIXEL_PURCHASE_CLICK_7D,
    PIXEL_PURCHASE_VIEW_7D,

    PIXEL_SUBSCRIBE_CLICK_1D,
    PIXEL_SUBSCRIBE_VIEW_1D,
    PIXEL_SUBSCRIBE_CLICK_7D,
    PIXEL_SUBSCRIBE_VIEW_7D,

    PIXEL_M1_VIP_CLICK_1D,
    PIXEL_M1_VIP_VIEW_1D,
    PIXEL_D1_VIP_CLICK_1D,
    PIXEL_D1_VIP_VIEW_1D,

    PIXEL_SUBSCRIBE_MEN_CLICK_1D,
    PIXEL_SUBSCRIBE_MEN_VIEW_1D,
    PIXEL_SUBSCRIBE_MEN_CLICK_7D,
    PIXEL_SUBSCRIBE_MEN_VIEW_7D,

    PIXEL_LEAD_MEN_CLICK_1D,
    PIXEL_LEAD_MEN_VIEW_1D,
    PIXEL_LEAD_MEN_CLICK_7D,
    PIXEL_LEAD_MEN_VIEW_7D,
    PIXEL_LEAD_WOMEN_CLICK_1D,
    PIXEL_LEAD_WOMEN_VIEW_1D,
    PIXEL_LEAD_WOMEN_CLICK_7D,
    PIXEL_LEAD_WOMEN_VIEW_7D,

    PIXEL_PURCHASE_MEN_CLICK_1D,
    PIXEL_PURCHASE_MEN_VIEW_1D,
    PIXEL_PURCHASE_MEN_CLICK_7D,
    PIXEL_PURCHASE_MEN_VIEW_7D,
    PIXEL_PURCHASE_WOMEN_CLICK_1D,
    PIXEL_PURCHASE_WOMEN_VIEW_1D,
    PIXEL_PURCHASE_WOMEN_CLICK_7D,
    PIXEL_PURCHASE_WOMEN_VIEW_7D,

    PIXEL_ATC_MEN_CLICK_1D,
    PIXEL_ATC_MEN_VIEW_1D,
    PIXEL_ATC_MEN_CLICK_7D,
    PIXEL_ATC_MEN_VIEW_7D,
    PIXEL_ATC_WOMEN_CLICK_1D,
    PIXEL_ATC_WOMEN_VIEW_1D,
    PIXEL_ATC_WOMEN_CLICK_7D,
    PIXEL_ATC_WOMEN_VIEW_7D,

    PIXEL_VIEW_CONTENT_MEN_CLICK_1D,
    PIXEL_VIEW_CONTENT_MEN_VIEW_1D,
    PIXEL_VIEW_CONTENT_MEN_CLICK_7D,
    PIXEL_VIEW_CONTENT_MEN_VIEW_7D,
    PIXEL_VIEW_CONTENT_WOMEN_CLICK_1D,
    PIXEL_VIEW_CONTENT_WOMEN_VIEW_1D,
    PIXEL_VIEW_CONTENT_WOMEN_CLICK_7D,
    PIXEL_VIEW_CONTENT_WOMEN_VIEW_7D,

    PIXEL_SUBSCRIBE_REVENUE_CLICK_1D,
    PIXEL_SUBSCRIBE_REVENUE_VIEW_1D,
    PIXEL_SUBSCRIBE_REVENUE_CLICK_7D,
    PIXEL_SUBSCRIBE_REVENUE_VIEW_7D,

    PIXEL_ADD_TO_CART_CLICK_1D,
    PIXEL_ADD_TO_CART_VIEW_1D,
    PIXEL_ADD_TO_CART_CLICK_7D,
    PIXEL_ADD_TO_CART_VIEW_7D,

    PIXEL_LANDING_PAGE_CLICK_1D,
    PIXEL_LANDING_PAGE_VIEW_1D,
    PIXEL_LANDING_PAGE_CLICK_7D,
    PIXEL_LANDING_PAGE_VIEW_7D,

    PIXEL_VIEW_CONTENT_CLICK_1D,
    PIXEL_VIEW_CONTENT_VIEW_1D,
    PIXEL_VIEW_CONTENT_CLICK_7D,
    PIXEL_VIEW_CONTENT_VIEW_7D,

    PIXEL_VIDEO_VIEW_3S_CLICK_1D,
    PIXEL_VIDEO_VIEW_3S_VIEW_1D,
    PIXEL_VIDEO_VIEW_3S_CLICK_7D,
    PIXEL_VIDEO_VIEW_3S_VIEW_7D,

    $high_watermark_datetime AS PIXEL_CONVERSION_META_UPDATE_DATETIME,
    CURRENT_TIMESTAMP()::timestamp_ltz AS META_CREATE_DATETIME,
    CURRENT_TIMESTAMP()::timestamp_ltz AS META_UPDATE_DATETIME

FROM
(
    SELECT
        fb.ACCOUNT_ID,
        fb.AD_ID,
        COALESCE(c.store_id_map_to,DS.STORE_ID) as STORE_ID,
        fb.DATE,

        SUM(PIXEL_LEAD_CLICK_1D)      AS PIXEL_LEAD_CLICK_1D,
        SUM(PIXEL_LEAD_VIEW_1D)       AS PIXEL_LEAD_VIEW_1D,
        SUM(PIXEL_LEAD_CLICK_7D)      AS PIXEL_LEAD_CLICK_7D,
        SUM(PIXEL_LEAD_VIEW_7D)       AS PIXEL_LEAD_VIEW_7D,

        SUM(PIXEL_VIP_CLICK_1D)       AS PIXEL_VIP_CLICK_1D,
        SUM(PIXEL_VIP_VIEW_1D)        AS PIXEL_VIP_VIEW_1D,
        SUM(PIXEL_VIP_CLICK_7D)       AS PIXEL_VIP_CLICK_7D,
        SUM(PIXEL_VIP_VIEW_7D)        AS PIXEL_VIP_VIEW_7D,

        SUM(pixel_vip_click_1d_old)       AS PIXEL_VIP_CLICK_1D_OLD,
        SUM(pixel_vip_view_1d_old)        AS PIXEL_VIP_VIEW_1D_OLD,
        SUM(pixel_vip_click_7d_old)       AS PIXEL_VIP_CLICK_7D_OLD,
        SUM(pixel_vip_view_7d_old)        AS PIXEL_VIP_VIEW_7D_OLD,

        SUM(PIXEL_PURCHASE_CLICK_1D)  AS PIXEL_PURCHASE_CLICK_1D,
        SUM(PIXEL_PURCHASE_VIEW_1D)   AS PIXEL_PURCHASE_VIEW_1D,
        SUM(PIXEL_PURCHASE_CLICK_7D)  AS PIXEL_PURCHASE_CLICK_7D,
        SUM(PIXEL_PURCHASE_VIEW_7D)   AS PIXEL_PURCHASE_VIEW_7D,

        SUM(PIXEL_SUBSCRIBE_CLICK_1D)   AS PIXEL_SUBSCRIBE_CLICK_1D,
        SUM(PIXEL_SUBSCRIBE_VIEW_1D)    AS PIXEL_SUBSCRIBE_VIEW_1D,
        SUM(PIXEL_SUBSCRIBE_CLICK_7D)   AS PIXEL_SUBSCRIBE_CLICK_7D,
        SUM(PIXEL_SUBSCRIBE_VIEW_7D)    AS PIXEL_SUBSCRIBE_VIEW_7D,

        SUM(PIXEL_M1_VIP_CLICK_1D)       AS PIXEL_M1_VIP_CLICK_1D,
        SUM(PIXEL_M1_VIP_VIEW_1D)        AS PIXEL_M1_VIP_VIEW_1D,
        SUM(PIXEL_D1_VIP_CLICK_1D)       AS PIXEL_D1_VIP_CLICK_1D,
        SUM(PIXEL_D1_VIP_VIEW_1D)        AS PIXEL_D1_VIP_VIEW_1D,

        SUM(PIXEL_SUBSCRIBE_MEN_CLICK_1D)   AS PIXEL_SUBSCRIBE_MEN_CLICK_1D,
        SUM(PIXEL_SUBSCRIBE_MEN_VIEW_1D)    AS PIXEL_SUBSCRIBE_MEN_VIEW_1D,
        SUM(PIXEL_SUBSCRIBE_MEN_CLICK_7D)   AS PIXEL_SUBSCRIBE_MEN_CLICK_7D,
        SUM(PIXEL_SUBSCRIBE_MEN_VIEW_7D)    AS PIXEL_SUBSCRIBE_MEN_VIEW_7D,

        SUM(PIXEL_LEAD_MEN_CLICK_1D)    AS PIXEL_LEAD_MEN_CLICK_1D,
        SUM(PIXEL_LEAD_MEN_VIEW_1D)     AS PIXEL_LEAD_MEN_VIEW_1D,
        SUM(PIXEL_LEAD_MEN_CLICK_7D)    AS PIXEL_LEAD_MEN_CLICK_7D,
        SUM(PIXEL_LEAD_MEN_VIEW_7D)     AS PIXEL_LEAD_MEN_VIEW_7D,
        SUM(PIXEL_LEAD_WOMEN_CLICK_1D)  AS PIXEL_LEAD_WOMEN_CLICK_1D,
        SUM(PIXEL_LEAD_WOMEN_VIEW_1D)   AS PIXEL_LEAD_WOMEN_VIEW_1D,
        SUM(PIXEL_LEAD_WOMEN_CLICK_7D)  AS PIXEL_LEAD_WOMEN_CLICK_7D,
        SUM(PIXEL_LEAD_WOMEN_VIEW_7D)   AS PIXEL_LEAD_WOMEN_VIEW_7D,

        SUM(PIXEL_PURCHASE_MEN_CLICK_1D)    AS PIXEL_PURCHASE_MEN_CLICK_1D,
        SUM(PIXEL_PURCHASE_MEN_VIEW_1D)     AS PIXEL_PURCHASE_MEN_VIEW_1D,
        SUM(PIXEL_PURCHASE_MEN_CLICK_7D)    AS PIXEL_PURCHASE_MEN_CLICK_7D,
        SUM(PIXEL_PURCHASE_MEN_VIEW_7D)     AS PIXEL_PURCHASE_MEN_VIEW_7D,
        SUM(PIXEL_PURCHASE_WOMEN_CLICK_1D)  AS PIXEL_PURCHASE_WOMEN_CLICK_1D,
        SUM(PIXEL_PURCHASE_WOMEN_VIEW_1D)   AS PIXEL_PURCHASE_WOMEN_VIEW_1D,
        SUM(PIXEL_PURCHASE_WOMEN_CLICK_7D)  AS PIXEL_PURCHASE_WOMEN_CLICK_7D,
        SUM(PIXEL_PURCHASE_WOMEN_VIEW_7D)   AS PIXEL_PURCHASE_WOMEN_VIEW_7D,

        SUM(PIXEL_ATC_MEN_CLICK_1D)     AS PIXEL_ATC_MEN_CLICK_1D,
        SUM(PIXEL_ATC_MEN_VIEW_1D)      AS PIXEL_ATC_MEN_VIEW_1D,
        SUM(PIXEL_ATC_MEN_CLICK_7D)     AS PIXEL_ATC_MEN_CLICK_7D,
        SUM(PIXEL_ATC_MEN_VIEW_7D)      AS PIXEL_ATC_MEN_VIEW_7D,
        SUM(PIXEL_ATC_WOMEN_CLICK_1D)   AS PIXEL_ATC_WOMEN_CLICK_1D,
        SUM(PIXEL_ATC_WOMEN_VIEW_1D)    AS PIXEL_ATC_WOMEN_VIEW_1D,
        SUM(PIXEL_ATC_WOMEN_CLICK_7D)   AS PIXEL_ATC_WOMEN_CLICK_7D,
        SUM(PIXEL_ATC_WOMEN_VIEW_7D)    AS PIXEL_ATC_WOMEN_VIEW_7D,

        SUM(PIXEL_VIEW_CONTENT_MEN_CLICK_1D)    AS PIXEL_VIEW_CONTENT_MEN_CLICK_1D,
        SUM(PIXEL_VIEW_CONTENT_MEN_VIEW_1D)     AS PIXEL_VIEW_CONTENT_MEN_VIEW_1D,
        SUM(PIXEL_VIEW_CONTENT_MEN_CLICK_7D)    AS PIXEL_VIEW_CONTENT_MEN_CLICK_7D,
        SUM(PIXEL_VIEW_CONTENT_MEN_VIEW_7D)     AS PIXEL_VIEW_CONTENT_MEN_VIEW_7D,
        SUM(PIXEL_VIEW_CONTENT_WOMEN_CLICK_1D)  AS PIXEL_VIEW_CONTENT_WOMEN_CLICK_1D,
        SUM(PIXEL_VIEW_CONTENT_WOMEN_VIEW_1D)   AS PIXEL_VIEW_CONTENT_WOMEN_VIEW_1D,
        SUM(PIXEL_VIEW_CONTENT_WOMEN_CLICK_7D)  AS PIXEL_VIEW_CONTENT_WOMEN_CLICK_7D,
        SUM(PIXEL_VIEW_CONTENT_WOMEN_VIEW_7D)   AS PIXEL_VIEW_CONTENT_WOMEN_VIEW_7D,

        SUM(PIXEL_SUBSCRIBE_REVENUE_CLICK_1D)   AS PIXEL_SUBSCRIBE_REVENUE_CLICK_1D,
        SUM(PIXEL_SUBSCRIBE_REVENUE_VIEW_1D)    AS PIXEL_SUBSCRIBE_REVENUE_VIEW_1D,
        SUM(PIXEL_SUBSCRIBE_REVENUE_CLICK_7D)   AS PIXEL_SUBSCRIBE_REVENUE_CLICK_7D,
        SUM(PIXEL_SUBSCRIBE_REVENUE_VIEW_7D)    AS PIXEL_SUBSCRIBE_REVENUE_VIEW_7D,

        SUM(PIXEL_ADD_TO_CART_CLICK_1D)         AS PIXEL_ADD_TO_CART_CLICK_1D,
        SUM(PIXEL_ADD_TO_CART_VIEW_1D)          AS PIXEL_ADD_TO_CART_VIEW_1D,
        SUM(PIXEL_ADD_TO_CART_CLICK_7D)         AS PIXEL_ADD_TO_CART_CLICK_7D,
        SUM(PIXEL_ADD_TO_CART_VIEW_7D)          AS PIXEL_ADD_TO_CART_VIEW_7D,

        SUM(PIXEL_LANDING_PAGE_CLICK_1D)         AS PIXEL_LANDING_PAGE_CLICK_1D,
        SUM(PIXEL_LANDING_PAGE_VIEW_1D)          AS PIXEL_LANDING_PAGE_VIEW_1D,
        SUM(PIXEL_LANDING_PAGE_CLICK_7D)         AS PIXEL_LANDING_PAGE_CLICK_7D,
        SUM(PIXEL_LANDING_PAGE_VIEW_7D)          AS PIXEL_LANDING_PAGE_VIEW_7D,

        SUM(PIXEL_VIEW_CONTENT_CLICK_1D)         AS PIXEL_VIEW_CONTENT_CLICK_1D,
        SUM(PIXEL_VIEW_CONTENT_VIEW_1D)          AS PIXEL_VIEW_CONTENT_VIEW_1D,
        SUM(PIXEL_VIEW_CONTENT_CLICK_7D)         AS PIXEL_VIEW_CONTENT_CLICK_7D,
        SUM(PIXEL_VIEW_CONTENT_VIEW_7D)          AS PIXEL_VIEW_CONTENT_VIEW_7D,

        SUM(PIXEL_VIDEO_VIEW_3S_CLICK_1D)         AS PIXEL_VIDEO_VIEW_3S_CLICK_1D,
        SUM(PIXEL_VIDEO_VIEW_3S_VIEW_1D)          AS PIXEL_VIDEO_VIEW_3S_VIEW_1D,
        SUM(PIXEL_VIDEO_VIEW_3S_CLICK_7D)         AS PIXEL_VIDEO_VIEW_3S_CLICK_7D,
        SUM(PIXEL_VIDEO_VIEW_3S_VIEW_7D)          AS PIXEL_VIDEO_VIEW_3S_VIEW_7D

    FROM _all_pixel_events fb
    JOIN lake_view.sharepoint.med_account_mapping_media GS ON GS.SOURCE_ID = fb.ACCOUNT_ID
        AND LOWER(GS.SOURCE) = 'facebook'
    JOIN edw_prod.data_model.dim_store DS ON DS.STORE_ID = GS.STORE_ID
    LEFT JOIN _FB_country_mappings c ON c.store_id = ds.STORE_ID
        and lower(c.fb_country_code) = lower(fb.COUNTRY)
        and fb.DATE >= c.start_date
        and fb.DATE < c.end_date
    GROUP BY 1,2,3,4
);
