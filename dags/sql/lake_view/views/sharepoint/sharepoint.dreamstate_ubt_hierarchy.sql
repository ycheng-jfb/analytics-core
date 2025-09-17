CREATE VIEW lake_view.sharepoint.dreamstate_ubt_hierarchy AS
SELECT
    product_segment,
    category,
    class,
    subclass,
    CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_inbound_sharepoint_v1.dreamstate_ubt_hierarchy_sheet_1;
