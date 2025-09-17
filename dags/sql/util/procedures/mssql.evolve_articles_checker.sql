
SET NOCOUNT ON;

SELECT
    CONCAT(hvr.db_name, '.dbo.', hvr.table_name) AS SOURCE_TABLE
FROM (
    SELECT
        db_name,
        table_name
    FROM (VALUES
        /* ULTRAWAREHOUSE */
        ('ultrawarehouse', 'address'),
        ('ultrawarehouse', 'adjustment'),
        ('ultrawarehouse', 'adjustment_item'),
        ('ultrawarehouse', 'administrator'),
        ('ultrawarehouse', 'administrator_activity_detail'),
        ('ultrawarehouse', 'administrator_warehouse'),
        ('ultrawarehouse', 'bulk_shipment'),
        ('ultrawarehouse', 'bulk_shipment_container'),
        ('ultrawarehouse', 'bulk_shipment_container_detail'),
        ('ultrawarehouse', 'carrier'),
        ('ultrawarehouse', 'carrier_billing'),
        ('ultrawarehouse', 'carrier_billing_detail'),
        ('ultrawarehouse', 'carrier_service'),
        ('ultrawarehouse', 'carrier_service_zone_cost'),
        ('ultrawarehouse', 'carrier_tracking'),
        ('ultrawarehouse', 'carrier_tracking_log'),
        ('ultrawarehouse', 'case'),
        ('ultrawarehouse', 'case_item'),
        ('ultrawarehouse', 'case_log'),
        ('ultrawarehouse', 'chargeback'),
        ('ultrawarehouse', 'chargeback_container_target'),
        ('ultrawarehouse', 'chargeback_customer_return'),
        ('ultrawarehouse', 'chargeback_detail'),
        ('ultrawarehouse', 'chargeback_fc_processing_cost'),
        ('ultrawarehouse', 'chargeback_outbound_cost'),
        ('ultrawarehouse', 'chargeback_return_cost'),
        ('ultrawarehouse', 'chargeback_vendor'),
        ('ultrawarehouse', 'code'),
        ('ultrawarehouse', 'company'),
        ('ultrawarehouse', 'company_carrier_service'),
        ('ultrawarehouse', 'company_carrier_zone'),
        ('ultrawarehouse', 'container'),
        ('ultrawarehouse', 'container_hospital'),
        ('ultrawarehouse', 'container_hospital_detail'),
        ('ultrawarehouse', 'cube'),
        ('ultrawarehouse', 'cycle_count'),
        ('ultrawarehouse', 'cycle_count_criteria'),
        ('ultrawarehouse', 'cycle_count_location'),
        ('ultrawarehouse', 'cycle_count_location_cycle_count'),
        ('ultrawarehouse', 'cycle_count_location_log'),
        ('ultrawarehouse', 'cycle_count_location_log_item'),
        ('ultrawarehouse', 'cycle_count_queue'),
        ('ultrawarehouse', 'cycle_count_sequence'),
        ('ultrawarehouse', 'cycle_count_sequence_inventory'),
        ('ultrawarehouse', 'device_log'),
        ('ultrawarehouse', 'dropship_retailer'),
        ('ultrawarehouse', 'edi_transmit_log'),
        ('ultrawarehouse', 'fulfillment'),
        ('ultrawarehouse', 'fulfillment_batch'),
        ('ultrawarehouse', 'fulfillment_file_archive'),
        ('ultrawarehouse', 'fulfillment_item'),
        ('ultrawarehouse', 'fulfillment_replen_batch'),
        ('ultrawarehouse', 'in_transit'),
        ('ultrawarehouse', 'in_transit_container'),
        ('ultrawarehouse', 'in_transit_container_case'),
        ('ultrawarehouse', 'in_transit_container_case_detail'),
        ('ultrawarehouse', 'in_transit_container_reference'),
        ('ultrawarehouse', 'in_transit_document'),
        ('ultrawarehouse', 'in_transit_milestone'),
        ('ultrawarehouse', 'in_transit_milestone_cross_reference'),
        ('ultrawarehouse', 'inventory'),
        ('ultrawarehouse', 'inventory_adjustment_rollup'),
        ('ultrawarehouse', 'inventory_fence'),
        ('ultrawarehouse', 'inventory_fence_detail'),
        ('ultrawarehouse', 'inventory_flat'),
        ('ultrawarehouse', 'inventory_location'),
        ('ultrawarehouse', 'inventory_log'),
        ('ultrawarehouse', 'inventory_log_container_item'),
        ('ultrawarehouse', 'inventory_rollup'),
        ('ultrawarehouse', 'invoice'),
        ('ultrawarehouse', 'invoice_box'),
        ('ultrawarehouse', 'invoice_box_item'),
        ('ultrawarehouse', 'invoice_item'),
        ('ultrawarehouse', 'item'),
        ('ultrawarehouse', 'item_reference'),
        ('ultrawarehouse', 'item_warehouse'),
        ('ultrawarehouse', 'item_warehouse_region'),
        ('ultrawarehouse', 'las_po'),
        ('ultrawarehouse', 'las_po_item'),
        ('ultrawarehouse', 'location'),
        ('ultrawarehouse', 'lpn'),
        ('ultrawarehouse', 'lpn_detail'),
        ('ultrawarehouse', 'lpn_log'),
        ('ultrawarehouse', 'outbound_pallet'),
        ('ultrawarehouse', 'outbound_pallet_carrier_service'),
        ('ultrawarehouse', 'outbound_pallet_detail'),
        ('ultrawarehouse', 'outbound_pallet_log'),
        ('ultrawarehouse', 'outbound_plan_log'),
        ('ultrawarehouse', 'outbound_preallocation'),
        ('ultrawarehouse', 'package'),
        ('ultrawarehouse', 'pallet'),
        ('ultrawarehouse', 'pick'),
        ('ultrawarehouse', 'pick_log'),
        ('ultrawarehouse', 'po_inventory_rollup'),
        ('ultrawarehouse', 'putaway'),
        ('ultrawarehouse', 'putaway_detail'),
        ('ultrawarehouse', 'putaway_detail_log'),
        ('ultrawarehouse', 'receipt'),
        ('ultrawarehouse', 'receipt_item'),
        ('ultrawarehouse', 'region'),
        ('ultrawarehouse', 'retail_configuration'),
        ('ultrawarehouse', 'retail_location'),
        ('ultrawarehouse', 'retail_location_configuration'),
        ('ultrawarehouse', 'warehouse'),
        ('ultrawarehouse', 'wcs_divert_log'),
        ('ultrawarehouse', 'yard_container'),
        ('ultrawarehouse', 'yard_container_case'),
        ('ultrawarehouse', 'yard_container_log'),
        ('ultrawarehouse', 'yard_container_queue'),
        ('ultrawarehouse', 'yard_po_priority'),
        ('ultrawarehouse', 'zone')

        ) AS hvr_tables(db_name, table_name)
    ) AS HVR
    LEFT JOIN (
    SELECT
        sp.name,
        'ultrawarehouse' AS db_name,
        sa.name as table_name
    FROM ultrawarehouse.dbo.sysarticles AS sa with(nolock)
    JOIN ultrawarehouse.dbo.syspublications AS sp with(nolock)
    ON sa.pubid = sp.pubid
    WHERE sp.name LIKE '%HVR%'
        AND sa.status % 2 = 1
    ) AS A
    ON A.db_name = HVR.db_name
    AND A.table_name = HVR.table_name
WHERE
    A.table_name IS NULL
    OR hvr.table_name IS NULL
ORDER BY SOURCE_TABLE ASC;
