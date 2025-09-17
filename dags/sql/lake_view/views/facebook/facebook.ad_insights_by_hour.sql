CREATE OR REPLACE VIEW IF NOT EXISTS LAKE_VIEW.FACEBOOK.AD_INSIGHTS_BY_HOUR (
	account_id,
	account_name,
	campaign_id,
	campaign_name,
	adset_id,
	adset_name,
	ad_id,
	ad_name,
	impressions,
	outbound_clicks,
	inline_link_clicks,
	spend,
	date,
	hourly_stats_aggregated_by_advertiser_time_zone,
	meta_create_datetime,
	meta_update_datetime
) AS
SELECT account_id,
    account_name,
    campaign_id,
    campaign_name,
    adset_id,
    adset_name,
    a.ad_id,
    ad_name,
    impressions,
    value AS outbound_clicks,
    inline_link_clicks,
    spend,
    a.date,
    hourly_stats_aggregated_by_advertiser_time_zone,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_create_datetime,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_facebook_ads_30min_na_v1.ad_insights_by_hour a
LEFT JOIN lake_fivetran.med_facebook_ads_30min_na_v1.ad_insights_by_hour_outbound_clicks b
    ON a.ad_id=b.ad_id AND a.date=b.date AND a._fivetran_id=b._fivetran_id

UNION ALL

SELECT account_id,
    account_name,
    campaign_id,
    campaign_name,
    adset_id,
    adset_name,
    a.ad_id,
    ad_name,
    impressions,
    value AS outbound_clicks,
    inline_link_clicks,
    spend,
    a.date,
    hourly_stats_aggregated_by_advertiser_time_zone,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_create_datetime,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_facebook_ads_30min_eu_v1.ad_insights_by_hour a
LEFT JOIN lake_fivetran.med_facebook_ads_30min_eu_v1.ad_insights_by_hour_outbound_clicks b
    ON a.ad_id=b.ad_id AND a.date=b.date AND a._fivetran_id=b._fivetran_id

UNION ALL

SELECT account_id,
    account_name,
    campaign_id,
    campaign_name,
    adset_id,
    adset_name,
    a.ad_id,
    ad_name,
    impressions,
    value AS outbound_clicks,
    inline_link_clicks,
    spend,
    a.date,
    hourly_stats_aggregated_by_advertiser_time_zone,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_create_datetime,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_facebook_ads_30min_sxf_v1.ad_insights_by_hour a
LEFT JOIN lake_fivetran.med_facebook_ads_30min_sxf_v1.ad_insights_by_hour_outbound_clicks b
    ON a.ad_id=b.ad_id AND a.date=b.date AND a._fivetran_id=b._fivetran_id;
