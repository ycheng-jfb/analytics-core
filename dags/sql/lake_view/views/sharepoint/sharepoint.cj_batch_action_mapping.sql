CREATE OR replace VIEW lake_view.sharepoint.cj_batch_action_mapping
AS
SELECT currency,
       store_region,
       actiontrackerid,
       business_unit,
       customerstatus,
       subscriptionid,
       store_id,
       companyid,
       enterpriseid,
       store_country,
       duration,
       action_type,
       Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_admin_v1.cj_batch_action_tracker_id_mapping_cj_batch_action_mapping;
