CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_ORGANIC_INFLUENCER_MAPPING AS
SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_flna
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_fleu
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_flmna
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_flmeu
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_ytyna
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_sxna
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_sxeu
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_jfna
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_jfeu
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_sdna
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_fkna
    WHERE _line > 3
        AND total::BOOLEAN = FALSE

UNION ALL

SELECT brand business_unit_abbr,
       region,
       influencer_ambassador_first_last_name full_name,
       name_shown_in_hdyh hdyh,
       media_partner_id,
       ciq_publisher_id,
       shortened_name,
       name_abbreviation name_abbr,
       active_partnership::BOOLEAN active_in_generator,
       total::BOOLEAN total,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
    FROM LAKE_FIVETRAN.med_sharepoint_influencer_v1.new_organic_influencer_mapping_for_hdyh_and_paid_media_scbna
    WHERE _line > 3
        AND total::BOOLEAN = FALSE;
