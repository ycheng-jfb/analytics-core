create or replace view LAKE_VIEW.SHAREPOINT.MEDIA_SOURCE_CHANNEL_MAPPING_ADJ(
	BRAND,
	UTM_SOURCE,
	UTM_MEDIUM,
	UTM_CAMPAIGN,
	CHANNEL_UPDATE,
	SUBCHANNEL_UPDATE,
	CHANNEL_TYPE_UPDATE,
    REFERRER,
	META_UPDATE_DATETIME
) as
select 'SXF' as brand,
       utm_source,
       utm_medium,
       utm_campaign,
       channel_update,
       subchannel_update,
       channel_type_update,
       referrer,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from LAKE_FIVETRAN.WEBANALYTICS_SHAREPOINT_V1.MEDIA_SOURCE_CHANNEL_MAPPING_ADJ_SXF
where _LINE > 0
UNION ALL
select 'FL' as brand,
       utm_source,
       utm_medium,
       utm_campaign,
       channel_update,
       subchannel_update,
       channel_type_update,
       referrer,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from LAKE_FIVETRAN.WEBANALYTICS_SHAREPOINT_V1.MEDIA_SOURCE_CHANNEL_MAPPING_ADJ_FL
where _LINE > 0
UNION ALL
select 'JFB' as brand,
       utm_source,
       utm_medium,
       utm_campaign,
       channel_update,
       subchannel_update,
       channel_type_update,
       referrer,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from LAKE_FIVETRAN.WEBANALYTICS_SHAREPOINT_V1.MEDIA_SOURCE_CHANNEL_MAPPING_ADJ_JFB
where _LINE > 0;
