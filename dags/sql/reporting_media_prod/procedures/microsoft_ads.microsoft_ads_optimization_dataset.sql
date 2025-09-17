create or replace transient table reporting_media_prod.microsoft_ads.microsoft_ads_optimization_dataset as
with _scaffold as (
select distinct store_id, adgroup_id, date from reporting_media_base_prod.microsoft_ads.daily_media_spend
union
select distinct store_id, adgroup_id, date from reporting_media_base_prod.microsoft_ads.pixel_conversion_metrics_daily
)
select
    n.*,
    sc.date,
    spend_account_currency,
    spend_usd,
    spend_local,
    impressions,
    clicks,
    pixel_lead_30dc,
    pixel_lead_1dc,
    pixel_vip_30dc,
    pixel_vip_1dc,
    latest_spend_update_datetime,
    latest_conversion_update_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime
from _scaffold sc
join reporting_media_base_prod.microsoft_ads.vw_search_naming_convention n on sc.adgroup_id = n.adgroup_id
left join  reporting_media_base_prod.microsoft_ads.daily_media_spend s on s.store_id = sc.store_id and s.adgroup_id = sc.adgroup_id and s.date = sc.date
left join reporting_media_base_prod.microsoft_ads.pixel_conversion_metrics_daily c on s.store_id and sc.store_id and c.adgroup_id = sc.adgroup_id and c.date = sc.date;
