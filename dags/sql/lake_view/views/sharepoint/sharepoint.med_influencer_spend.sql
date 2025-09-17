CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_INFLUENCER_SPEND AS
SELECT start_date::date as start_date,
    end_date::date as end_date,
    NULL as influencer_spend,
    brand,
    country,
    channel,
    tier,
    influencer_name,
    currency,
    upfront_spend,
    product_cost_spend,
    media_partner_id,
    shared_id,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM LAKE_FIVETRAN.MED_SHAREPOINT_INFLUENCER_V1.influencer_spreadhseet_new_upfront_spends;
