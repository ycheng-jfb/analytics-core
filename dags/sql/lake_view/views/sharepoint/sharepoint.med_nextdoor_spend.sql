CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_NEXTDOOR_SPEND AS
SELECT date::date as date,
    business_unit,
    country,
    store_id as store_id,
    advertiser_id,
    spend,
    impressions,
    clicks,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM LAKE_FIVETRAN.MED_SHAREPOINT_SPEND_V1.NEXTDOOR_FABLETICS_SPEND_SHEET_1;
