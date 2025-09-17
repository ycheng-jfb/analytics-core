CREATE OR REPLACE VIEW lake_view.sharepoint.ubt_hierarchy AS
SELECT
       PRODUCT_SEGMENT,
       CATEGORY,
       CLASS,
       SUBCLASS,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.ubt_hierarchy_final_result_1;
