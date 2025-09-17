CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_MANUAL_SPEND_ADJUSTMENTS as
SELECT spend_spread,
        effective_start_date,
        effective_end_date,
        store_brand_name,
        store_country_code,
        is_mens_flag,
        is_scrubs_flag,
        media_channel_name,
        media_subchannel_name,
        media_vendor_name,
        spend_type_code,
        spend_currency_code,
        audience_targeting_type_name,
        SUM(spend_amt) spend_amt,
        SUM(impressions_count) impressions_count,
        SUM(clicks_count) clicks_count,
        _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
        _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_admin_v1.manual_spend_adjustments_spend_additions
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,17,18;
