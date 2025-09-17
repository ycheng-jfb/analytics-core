create or replace view lake_view.sharepoint.builder_api_targeting as
select builder_id,
       property,
       operator,
       value,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from lake_fivetran.webanalytics_sharepoint_v1.builder_targeting_targeting_20240701
