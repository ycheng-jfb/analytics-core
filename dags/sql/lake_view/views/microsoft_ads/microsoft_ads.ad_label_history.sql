create or replace view LAKE_VIEW.MICROSOFT_ADS.AD_LABEL_HISTORY as
select
MODIFIED_TIME
,AD_ID
,LABEL_ID
,Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
from LAKE_FIVETRAN.MED_MICROSOFT_ADS_V1.AD_LABEL_HISTORY;
