CREATE OR REPLACE VIEW lake_view.sharepoint.fl_college_connector_link_creation_individual_connector_links AS
SELECT final_link,
       media_partner_id_individuals_only_ AS media_partner_id,
       school_name_select_from_dropdown_ AS school_name,
       school_abbreviation_populates_automatically_ AS school_abbreviation,
       connector_name_no_spaces_ AS connector_name,
       gateway,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_college_connectors_v1.fl_college_connector_link_creation_individual_connector_links;
