CREATE OR REPLACE VIEW lake_view.sharepoint.influencer_promo_codes AS
SELECT promo_code,
       include_in_reporting,
       Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM   lake_fivetran.med_sharepoint_influencer_v1.influencer_promo_codes_promo_codes;
