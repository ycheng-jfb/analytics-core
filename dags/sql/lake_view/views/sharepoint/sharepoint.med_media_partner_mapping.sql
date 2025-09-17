CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_MEDIA_PARTNER_MAPPING as
SELECT channel,
        subchannel,
        vendor,
        spend_type,
        processing_lookback,
        email,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_admin_v1.media_partner_mapping_sheet_1 qualify row_number() OVER (
                PARTITION BY channel,subchannel,vendor,spend_type ORDER BY processing_lookback DESC
                ) = 1;
