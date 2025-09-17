CREATE OR REPLACE VIEW IF NOT EXISTS LAKE_VIEW.FACEBOOK.AD_INSIGHTS_BY_PLATFORM (
	account_id,
	ad_id,
	impressions,
	clicks,
	outbound_action_type,
	outbound_clicks,
	inline_link_clicks,
	spend,
	date,
	publisher_platform,
	platform_position,
	impression_device,
	video_action_type,
	video_play_impressions,
	meta_create_datetime,
	meta_update_datetime
) AS
SELECT account_id,
    a.ad_id,
    impressions,
    clicks,
    b.action_type AS outbound_action_type,
    b.value AS outbound_clicks,
    inline_link_clicks,
    spend,
    a.date,
    a.publisher_platform,
    a.platform_position,
    a.impression_device,
    c.action_type AS video_action_type,
    c.value AS video_play_impressions,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_create_datetime,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_facebook_ads_3hr_v1.ad_insights_by_platform_outbound_clicks b
FULL JOIN lake_fivetran.med_facebook_ads_3hr_v1.ad_insights_by_platform_video_play_actions c
    ON b.ad_id=c.ad_id AND b.date=c.date AND b._fivetran_id=c._fivetran_id
FULL JOIN lake_fivetran.med_facebook_ads_3hr_v1.ad_insights_by_platform a
    ON a.ad_id=coalesce(b.ad_id,c.ad_id) AND a.date=coalesce(b.date,c.date)
        AND a._fivetran_id=coalesce(b._fivetran_id,c._fivetran_id);
