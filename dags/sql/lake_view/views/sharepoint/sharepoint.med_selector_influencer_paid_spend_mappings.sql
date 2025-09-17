CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_SELECTOR_INFLUENCER_PAID_SPEND_MAPPINGS AS
select brand as business_unit_abbr,
    region,
    influencer_ambassador_first_last_name as full_name,
    name_shown_in_hdyh as hdyh,
    media_partner_id,
    shortened_name,
    name_abbreviation as name_abbr,
    active_partnership as active_in_generator,
    uniqueness_for_shortened_name as uniqueness_shortened_name,
    uniqueness_for_name_abbreviation as uniqueness_name_abbr,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
    _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
from LAKE_FIVETRAN.med_sharepoint_influencer_v1.influencer_mapping_organic_hdyh_paid_media_performance_overview_link;
