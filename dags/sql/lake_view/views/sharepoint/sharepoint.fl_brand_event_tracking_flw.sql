CREATE OR REPLACE VIEW lake_view.sharepoint.fl_brand_event_tracking_flw AS
SELECT start_date,
       event_description,
       end_date,
       event_type,
       notes,
       Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_acquisition_v1.fl_brand_event_tracking_flw;
