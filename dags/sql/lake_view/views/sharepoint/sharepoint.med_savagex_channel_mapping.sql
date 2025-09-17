CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_SAVAGEX_CHANNEL_MAPPING as
SELECT traffic_type,
        utm_medium,
        utm_source,
        channel,
        subchannel,
        vendor,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_acquisition_v1.savage_x_channel_mapping_sheet_1
where traffic_type is not null;
