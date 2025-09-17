--optimization dataset
create or replace transient table reporting_media_prod.twitter.twitter_optimization_dataset as

select distinct

    n.*,
    s.date,

    s.spend_account_currency,
    s.spend_account_currency * (1 + ifnull(v.tfee_pct_of_media_spend,0)) as spend_with_vendor_fees_account_currency,
    s.spend_usd,
    s.spend_usd * (1 + ifnull(v.tfee_pct_of_media_spend,0)) as spend_with_vendor_fees_usd,
    s.spend_local,
    s.spend_local * (1 + ifnull(v.tfee_pct_of_media_spend,0)) as spend_with_vendor_fees_local,
    v.tfee_pct_of_media_spend as vendor_fees,
    s.impressions,
    s.clicks,

    count(n.unique_ad_id) over (partition by n.adgroup_id, s.date) as ad_count,
    sum(s.vips) over (partition by n.adgroup_id, s.date) as total_ad_vips,
    iff(total_ad_vips =0,(a.vips/ad_count),((s.vips/total_ad_vips)*a.vips)) as vips,
    sum(s.leads) over (partition by n.adgroup_id, s.date) as total_ad_leads,
    iff(total_ad_leads =0,(a.leads/ad_count),((s.leads/total_ad_leads)*a.leads)) as leads,






    t.ui_cac_target,

    current_timestamp()::timestamp_ltz as meta_update_datetime

from reporting_media_base_prod.twitter.spend_and_conversion_metrics_daily_by_unique_ad_id s
left join reporting_media_base_prod.twitter.vw_twitter_naming_convention n on s.unique_ad_id =  n.unique_ad_id
left join lake_view.sharepoint.med_vendor_fees_media v
    on (case when lower(v.identifier) = 'campaign' and n.campaign_name ilike '%' || lower(v.identifier_value) || '%'
            and s.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.account_id
        when lower(v.identifier) = 'adset' and n.adgroup_name ilike '%' || lower(v.identifier_value) || '%'
            and s.date between v.start_date_include and v.end_date_exclude - 1
        then cast(n.account_id as varchar) = v.account_id end)
    and lower(v.channel) = 'twitter'
left join reporting_media_base_prod.dbo.media_channel_ui_targets_actuals t on n.store_brand_name = t.store_brand_name
    and s.date=t.date
    and t.channel = 'twitter'
    and n.region = t.region
left join lake_view.twitter.twitter_spend_and_conversion_metrics_by_line_item_id a on a.line_item_id = n.adgroup_id
    and a.date = s.date;
