CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_SMART_COEF_KEY_IN_VALUE as
SELECT brand,
        channel,
        member_segment,
        month_start::date as month_start,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_acquisition_v1.pixel_incrementality_key_in_sc_incrementality_adjustments;
