create or replace temporary table _combined_no_ui_cac_target as
select
    n.*,

    s.date,
    spend_account_currency,
    spend_account_currency * (1 + ifnull(tfee_pct_of_media_spend,0)) as spend_with_vendor_fees_account_currency,
    spend_usd,
    spend_usd * (1 + ifnull(tfee_pct_of_media_spend,0)) as spend_with_vendor_fees_usd,
    spend_local,
    spend_local * (1 + ifnull(tfee_pct_of_media_spend,0)) as spend_with_vendor_fees_local,
    impressions,
    clicks,

    lead,
    vip,
    vip_view_1d,
    cta,
    purchase,
    subscribe,
    vips_from_leads_30d,

    video_play_actions,
    video_watched_2s,
    video_watched_6s,

    sum(spend_account_currency) over(partition by n.ad_id order by s.date asc) as runningsum_spendacctcurrency_by_date_ad_id,
    sum(spend_usd) over(partition by n.ad_id order by s.date asc) as runningsum_spendusd_by_date_ad_id,
    sum(impressions) over(partition by n.ad_id order by s.date asc) as runningsum_impressions_by_date_ad_id,
    sum(spend_with_vendor_fees_account_currency) over(partition by n.ad_id order by s.date asc) as runningsum_spendacctcurrency_fees_by_date_ad_id,
    sum(spend_with_vendor_fees_usd) over(partition by n.ad_id order by s.date asc) as runningsum_spendusd_fees_by_date_ad_id,

    latest_spend_update_datetime,
    latest_conversion_update_datetime,
    current_timestamp()::timestamp_ltz as meta_create_datetime,
    current_timestamp()::timestamp_ltz as meta_update_datetime

from reporting_media_base_prod.tiktok.advertising_spend_metrics_daily_by_ad s
left join reporting_media_base_prod.tiktok.conversion_metrics_daily_by_ad c on s.ad_id = c.ad_id
            and s.date = c.date
left join reporting_media_base_prod.tiktok.vw_tiktok_naming_convention n on n.ad_id = s.ad_id
left join lake_view.sharepoint.med_vendor_fees_media v
    on (case when lower(v.identifier) = 'campaign' and n.campaign_name ilike '%' || lower(v.identifier_value) || '%'
            and s.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'adset' and n.adgroup_name ilike '%' || lower(v.identifier_value) || '%'
            and s.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'ad' and n.ad_name ilike '%' || lower(v.identifier_value) || '%'
            and s.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.account_id end)
    and lower(v.channel) = 'tiktok';


create or replace transient table reporting_media_prod.tiktok.tiktok_optimization_dataset as
select c.*,
    coalesce(dc.offer_promo,c.offer) as new_offer,
    ad.poster_url,
    ad.url,
    ui_cac_target,
    ui_optimization_window
from _combined_no_ui_cac_target c
left join lake_view.tiktok.ad_link ad on c.ad_id = ad.ad_id
left join reporting_media_base_prod.dbo.media_channel_ui_targets_actuals t on t.store_brand_name = c.store_brand_name
    and t.date=c.date
    and t.channel = 'tiktok'
    and t.region = c.region
left join lake_view.sharepoint.dynamic_display_card_offer dc on c.ad_id = dc.ad_id
    and c.store_brand_name = dc.store_brand
    and c.country = dc.country
    and lower(dc.channel) = 'tiktok'
    and c.date between to_date(start_date_include) and to_date(end_date_exclude) - 1;
