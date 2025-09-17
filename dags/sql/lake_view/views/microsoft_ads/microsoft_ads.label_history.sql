create or replace view LAKE_VIEW.MICROSOFT_ADS.LABEL_HISTORY as
select
ID
,MODIFIED_TIME
,COLOR
,DESCRIPTION
,STATUS
,NAME
,Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
from LAKE_FIVETRAN.MED_MICROSOFT_ADS_V1.LABEL_HISTORY;
