CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_ADVERTISING_CORRECTIONS as
SELECT column_name,
        value,
        name,
        source,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_acquisition_v1.advertising_corrections_sheet_1;
