CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_SXF_CRM_SMS_DIMENSIONS AS

SELECT campaign_name,
    campaign_id,
    country,
    launch_date,
    message_type,
    sms_or_mms,
    text_to_join,
    segment,
    subsegment,
    version,
    track,
    creative_concept,
    online_or_retail,
    welcome_drip,
    offer_1,
    offer_2,
    emoji_in_subject,
    test,
    sms_length,
    gif_in_hero,
    message_objective,
    sub_department,
    promo_series,
    event,
    sms_emarsys_attentive_joint_campaign,
    landing_page,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM LAKE_FIVETRAN.MED_SHAREPOINT_ACQUISITION_V1.SXF_CRM_SMS_CAMPAIGN_GENERATOR_NA_SMS_CAMPAIGN_GENERATOR
WHERE _line > 1

UNION ALL


SELECT campaign_name,
    campaign_id,
    country,
    launch_date,
    message_type,
    sms_or_mms,
    text_to_join,
    segment,
    subsegment,
    version,
    track,
    creative_concept,
    online_or_retail,
    welcome_drip,
    offer_1,
    offer_2,
    emoji_in_subject,
    test,
    sms_length,
    gif_in_hero,
    message_objective,
    sub_department,
    promo_series,
    event,
    sms_emarsys_attentive_joint_campaign,
    landing_page,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM LAKE_FIVETRAN.MED_SHAREPOINT_ACQUISITION_V1.SXF_CRM_SMS_CAMPAIGN_GENERATOR_EU_SMS_CAMPAIGN_GENERATOR
WHERE _line > 1
;
