create or replace view LAKE_VIEW.FACEBOOK.AD_INSIGHTS_BY_COUNTRY(
	ACCOUNT_ID,
	AD_ID,
	IMPRESSIONS,
	CLICKS,
	OUTBOUND_ACTION_TYPE,
	OUTBOUND_CLICKS,
	INLINE_LINK_CLICKS,
	SPEND,
	DATE,
	COUNTRY,
	VIDEO_ACTION_TYPE,
	VIDEO_PLAY_IMPRESSIONS,
	AVG_VIDEO_VIEW,
	VIDEO_VIEWS_2S,
	VIDEO_VIEWS_P75,
	META_CREATE_DATETIME,
	META_UPDATE_DATETIME
) AS
select a.account_id,
    a.ad_id,
    impressions,
    clicks,
    b.action_type AS outbound_action_type,
    b.value AS outbound_clicks,
    inline_link_clicks,
    spend,
    a.date,
    a.country,
    c.action_type AS video_action_type,
    c.value AS video_play_impressions,
    d.value as avg_video_view,
    e.value as video_views_2s,
    f.value as video_views_p75,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_create_datetime,
    convert_timezone('America/Los_Angeles',a._fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_facebook_ads_2hr_v1.ad_insights_by_country a
FULL JOIN lake_fivetran.med_facebook_ads_2hr_v1.ad_insights_by_country_outbound_clicks b
    ON a.ad_id = b.ad_id and a.date = b.date and a._fivetran_id = b._fivetran_id
FULL JOIN lake_fivetran.med_facebook_ads_2hr_v1.ad_insights_by_country_video_play_actions c
    ON a.ad_id=c.ad_id AND a.date=c.date AND a._fivetran_id=c._fivetran_id
FULL JOIN lake_fivetran.med_facebook_ads_6hr_v2.video_insights_by_ad g
    ON a.ad_id = g.ad_id AND a.date = g.date AND a.country = g.country
FULL JOIN lake_fivetran.med_facebook_ads_6hr_v2.video_insights_by_ad_video_avg_time_watched_actions d
    ON g.ad_id = d.ad_id AND g.date = d.date AND g._fivetran_id = d._fivetran_id
FULL JOIN lake_fivetran.med_facebook_ads_6hr_v2.video_insights_by_ad_video_continuous_2_sec_watched_actions e
    ON g.ad_id = e.ad_id AND g.date = e.date AND g._fivetran_id = e._fivetran_id
FULL JOIN lake_fivetran.med_facebook_ads_6hr_v2.video_insights_by_ad_video_p_75_watched_actions f
    ON g.ad_id = f.ad_id AND g.date = f.date AND g._fivetran_id = f._fivetran_id;
