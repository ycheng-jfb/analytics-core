create or replace view LAKE_VIEW.MICROSOFT_ADS.CAMPAIGN_LABEL_HISTORY as
select
MODIFIED_TIME
,CAMPAIGN_ID
,LABEL_ID
,Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
from LAKE_FIVETRAN.MED_MICROSOFT_ADS_V1.CAMPAIGN_LABEL_HISTORY;
