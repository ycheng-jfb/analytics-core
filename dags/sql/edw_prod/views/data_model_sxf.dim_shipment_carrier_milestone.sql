CREATE OR REPLACE VIEW data_model_sxf.dim_shipment_carrier_milestone
(
    order_id,
    tracking_number,
    carrier_service_id,
    order_shipped_local_datetime,
    carrier_ingestion_source,
    carrier_delivered_source,
    carrier_delivered_local_datetime,
    carrier_ingestion_local_datetime,
    returned_local_datetime,
    en_route_local_datetime,
    shipment_damaged_local_datetime,
    estimated_delivery_local_datetime,
    arrived_at_terminal_local_datetime,
    shipment_acknowledged_local_datetime,
    shipment_cancelled_local_datetime,
    shipment_delayed_local_datetime,
    meta_create_datetime,
    meta_update_datetime
)
AS
SELECT meta_original_order_id,
       tracking_number,
       carrier_service_id,
       order_shipped_local_datetime,
       carrier_ingestion_source,
       carrier_delivered_source,
       carrier_delivered_local_datetime,
       carrier_ingestion_local_datetime,
       returned_local_datetime,
       en_route_local_datetime,
       shipment_damaged_local_datetime,
       estimated_delivery_local_datetime,
       arrived_at_terminal_local_datetime,
       shipment_acknowledged_local_datetime,
       shipment_cancelled_local_datetime,
       shipment_delayed_local_datetime,
       meta_create_datetime,
       meta_update_datetime
FROM dbt_edw_prod.stg.dim_shipment_carrier_milestone
WHERE SUBSTRING(order_id, -2) = '30';
