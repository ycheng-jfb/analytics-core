CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.JFB_OUTLOOK_INPUT as (
    select
        BRAND as BUSINESS_UNIT,
        OUTLLOOK_MONTH::DATE as OUTLOOK_MONTH,
        DATE::DATE as DATE,
        MEDIA_SPEND::FLOAT as MEDIA_SPEND,
        VIP_CPA,
        CANCELS::FLOAT as CANCELS,
        CREDIT_BILLINGS,
        NEW_VIPS,
    	_fivetran_synced::TIMESTAMP_LTZ as meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ as meta_update_datetime
    from LAKE_FIVETRAN.JFB_CONFIDENTIAL_SHAREPOINT_V1.JFB_OUTLOOK_INPUT_SHEET_1
);
