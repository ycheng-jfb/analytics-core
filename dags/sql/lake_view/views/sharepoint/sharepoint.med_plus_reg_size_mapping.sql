CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_PLUS_REG_SIZE_MAPPING as
SELECT store_brand_name_abbr,
        size,
        size_type,
        sort_order,
        _fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
from lake_fivetran.med_sharepoint_acquisition_v1.reg_vs_plus_size_mapping_sheet_1;
