CREATE OR REPLACE VIEW LAKE_VIEW.GPT.OPENPATH_NAME_MISMATCHES AS
 select open_path_name,
     workday_name,
     _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
 FROM lake_fivetran.gpt_sharepoint_admin_v1.open_path_name_mismatches_name_mismatches;
