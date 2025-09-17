CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_HARDCODED_INFLUENCER_HDYH_ANSWERS AS
SELECT Distinct INFLUENCER_NAME
	,iff(MEDIA_PARTNER_ID::VARCHAR = '0.0', NULL, MEDIA_PARTNER_ID::VARCHAR) MEDIA_PARTNER_ID
	,STORE_BRAND_ABBR
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.MED_SHAREPOINT_INFLUENCER_V1.hdyh_hardcoded_influencers_fl_hardcoded_influencers

UNION ALL

SELECT Distinct INFLUENCER_NAME
	,IFF(MEDIA_PARTNER_ID::VARCHAR = '0', NULL, MEDIA_PARTNER_ID::VARCHAR) MEDIA_PARTNER_ID
	,STORE_BRAND_ABBR
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.MED_SHAREPOINT_INFLUENCER_V1.hdyh_hardcoded_influencers_sx_hardcoded_influencers;
