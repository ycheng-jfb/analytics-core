CREATE OR REPLACE VIEW lake_view.sharepoint.fl_college_connector_link_creation_university_events_mapping AS
SELECT event_name,
       school_abbreviation_must_be_unique_ AS school_abbreviation,
       school_name,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_college_connectors_v1.fl_college_connector_link_creation_university_events_mapping;
