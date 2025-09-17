CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.GFB_MERCH_CLEARANCE_SKU_LIST as (
    select
        BUSINESS_UNIT,
        REGION,
        SKU,
        CLEARANCE_GROUP::VARCHAR as CLEARANCE_GROUP,
        START_DATE::DATE as START_DATE,
        END_DATE::DATE as END_DATE,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_MERCH_CLEARANCE_SKU_LIST_FKNA

    UNION

    select
        BUSINESS_UNIT,
        REGION,
        SKU,
        CLEARANCE_GROUP::VARCHAR as CLEARANCE_GROUP,
        START_DATE::DATE as START_DATE,
        END_DATE::DATE as END_DATE,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_MERCH_CLEARANCE_SKU_LIST_JFEU

    UNION

    select
        BUSINESS_UNIT,
        REGION,
        SKU,
        CLEARANCE_GROUP::VARCHAR as CLEARANCE_GROUP,
        START_DATE::DATE as START_DATE,
        END_DATE::DATE as END_DATE,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_MERCH_CLEARANCE_SKU_LIST_JFNA

    UNION

    select
        BUSINESS_UNIT,
        REGION,
        SKU,
        CLEARANCE_GROUP::VARCHAR as CLEARANCE_GROUP,
        START_DATE::DATE as START_DATE,
        END_DATE::DATE as END_DATE,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_OTHER_SHAREPOINT_V1.GFB_MERCH_CLEARANCE_SKU_LIST_SDNA
);
