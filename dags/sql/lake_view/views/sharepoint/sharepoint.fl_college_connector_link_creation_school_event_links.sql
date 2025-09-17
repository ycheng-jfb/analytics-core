CREATE OR REPLACE VIEW lake_view.sharepoint.fl_college_connector_link_creation_school_event_links AS
SELECT final_link,
       event_date_mm_yy_ AS event_date,
       school_name_select_from_dropdown_ AS school_name,
       school_abbreviation_populates_automatically_ AS school_abbreviation,
       event_name_select_from_dropdown_ AS event_name,
       gateway,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_college_connectors_v1.fl_college_connector_link_creation_school_event_links;
