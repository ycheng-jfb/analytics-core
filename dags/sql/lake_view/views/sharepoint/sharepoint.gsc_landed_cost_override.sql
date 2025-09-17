CREATE OR REPLACE VIEW lake_view.sharepoint.gsc_landed_cost_override AS
SELECT target_table,
       po_dtl_id,
       action,
       field_name,
       field_value,
       to_timestamp_ltz(_fivetran_synced) as meta_update_datetime
FROM LAKE_FIVETRAN.GLOBAL_APPS_GSC_LANDED_COST_INGESTIONS.DATA_CORRECTION_FILE_SHEET_1
WHERE target_table <> 'Example';
