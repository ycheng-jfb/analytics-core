CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_CUSTOMER_LOOKUP as (
    select
        SEGMENT_OWNER,
        SEGMENT_NAME,
        CUSTOMER_ID,
        MEMBER_ID,
        MEMBERSHIP_TYPE,
        COUNTRY,
        DIMENSION_1,
        DIMENSION_2,
        DIMENSION_3,
        DIMENSION_4,
        DIMENSION_5,
        DIMENSION_6,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_CUSTOMER_LOOKUP_SHEET_1
);
