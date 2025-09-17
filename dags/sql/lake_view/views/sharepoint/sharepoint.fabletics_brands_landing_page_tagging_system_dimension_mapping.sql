CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.FABLETICS_BRANDS_LANDING_PAGE_TAGGING_SYSTEM_DIMENSION_MAPPING (
    COLUMN_NAME,
    DIMENSION,
    META_CREATE_DATETIME,
    META_UPDATE_DATETIME
) AS
SELECT column_name,
    dimension,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_naming_generators_v1.fabletics_brands_landing_page_tagging_system_dimension_mapping
WHERE _line>2
QUALIFY ROW_NUMBER() OVER(PARTITION BY dimension ORDER BY _fivetran_synced)=1;
