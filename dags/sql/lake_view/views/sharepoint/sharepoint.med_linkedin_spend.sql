CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_LINKEDIN_SPEND as
SELECT date::date as date,
    business_unit,
    country,
    store_id,
    spend,
    impressions,
    clicks,
    scrubs_flag,
    mens_flag,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM LAKE_FIVETRAN.MED_SHAREPOINT_SPEND_V1.LINKED_IN_SPEND_FLMUS
UNION
SELECT date::date as date,
    business_unit,
    country,
    store_id,
    spend,
    impressions,
    clicks,
    scrubs_flag,
    mens_flag,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM LAKE_FIVETRAN.MED_SHAREPOINT_SPEND_V1.LINKED_IN_SPEND_FLWUS
UNION
SELECT date::date as date,
    business_unit,
    country,
    store_id,
    spend,
    impressions,
    clicks,
    scrubs_flag,
    mens_flag,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM LAKE_FIVETRAN.MED_SHAREPOINT_SPEND_V1.LINKED_IN_SPEND_SCBUS;
