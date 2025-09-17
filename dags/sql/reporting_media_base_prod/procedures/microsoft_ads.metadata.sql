
set latest_data_update_datetime =
        (select least(
        (select max(meta_update_datetime) from lake_view.microsoft_ads.ad_performance_daily_report),
        (select max(meta_update_datetime) from lake_view.microsoft_ads.account_history),
        (select max(meta_update_datetime) from lake_view.microsoft_ads.campaign_history),
        (select max(meta_update_datetime) from lake_view.microsoft_ads.ad_group_history),
        (select max(meta_update_datetime) from lake_view.microsoft_ads.ad_history)));

-- ids
create or replace temporary table _ids as
select distinct account_id, campaign_id, ad_group_id as adgroup_id
from lake_view.microsoft_ads.ad_performance_daily_report;

-- ad metadata
-- create or replace temporary table _ad_metadata as
-- with _ads as (
-- select distinct
--     id as ad_id,
--     editorial_status as status,
--     type as ad_type,
--     text as ad_text,
--     title_part_1 as ad_title_1,
--     title_part_2 as ad_title_2,
--     title_part_3 as ad_title_3,
--     row_number() over (partition by ad_id order by meta_update_datetime desc) as rn_ad
-- from lake_view.microsoft_ads.ad_history),
-- _label as (
-- select distinct
--     adl.ad_id,
--     array_agg(l.name)::string ad_labels
-- from lake_view.microsoft_ads.ad_label_history adl
-- join lake_view.microsoft_ads.label_history l on l.id = adl.label_id
-- group by 1)
-- select
--     ad.ad_id,
--     status,
--     ad_type,
--     ad_text,
--     ad_title_1,
--     ad_title_2,
--     ad_title_3,
--     l.ad_labels
-- from _ads ad
-- left join _label l on ad.ad_id = l.ad_id
-- where rn_ad = 1;


-- metadata
create or replace transient table reporting_media_base_prod.microsoft_ads.metadata as
select distinct

    id.account_id,
    ac.account_name,
    id.campaign_id,
    c.campaign_name,
    id.adgroup_id,
    ag.adgroup_name,
--     id.ad_id,
--     ad.status,
--     ad.ad_type,
--     ad.ad_text,
--     ad.ad_title_1,
--     ad.ad_title_2,
--     ad.ad_title_3,
--     ad.ad_labels,

    $latest_data_update_datetime as latest_data_update_datetime,
    current_timestamp()   as meta_create_datetime,
    current_timestamp()   as meta_update_datetime

from _ids id
left join (select distinct
    id as account_id,
    name as account_name,
    row_number() over (partition by account_id order by meta_update_datetime desc) as rn_account
    from lake_view.microsoft_ads.account_history) ac on ac.account_id = id.account_id and rn_account = 1
left join (
select distinct
    id as campaign_id,
    name as campaign_name,
    row_number() over (partition by campaign_id order by meta_update_datetime desc) as rn_campaign
from lake_view.microsoft_ads.campaign_history) c on c.campaign_id = id.campaign_id and rn_campaign = 1
left join (
select distinct
    id as adgroup_id,
    name as adgroup_name,
    row_number() over (partition by adgroup_id order by meta_update_datetime desc) as rn_adgroup
from lake_view.microsoft_ads.ad_group_history) ag on ag.adgroup_id = id.adgroup_id and rn_adgroup = 1;
-- left join _ad_metadata ad on ad.ad_id = id.ad_id;


