BEGIN TRANSACTION NAME gsc_narvar_consolidated_milestones;

SET last_update = (SELECT COALESCE(dateadd('HOUR',-4,max(meta_update_datetime)),'1970-01-01')
                   FROM reporting_base_prod.gsc.narvar_consolidated_milestones);

-- If we can get the ORDER ID, awesome. If not, it's ok.
CREATE OR REPLACE TEMP TABLE _get_order_ids AS
WITH latest_tracking_number AS (
SELECT DISTINCT ib.tracking_number,
                max(date_shipped) latest_date_shipped
FROM lake_view.ultra_warehouse.invoice_box ib
JOIN reporting_prod.gsc.narvar_carrier_milestones n
    ON ib.tracking_number = n.tracking_number
WHERE to_date(date_shipped) >= '2024-01-01'
GROUP BY ib.tracking_number)
SELECT distinct ib.tracking_number,
                i.foreign_order_id
FROM lake_view.ultra_warehouse.invoice_box ib
LEFT JOIN lake_view.ultra_warehouse.invoice i
    ON ib.invoice_id = i.invoice_id
JOIN latest_tracking_number l
    ON ib.tracking_number = l.tracking_number
    AND ib.date_shipped = l.latest_date_shipped;

CREATE OR REPLACE TEMP TABLE _new_tracking_num_info AS
SELECT DISTINCT CAST(COALESCE(ncm.order_id, oid.foreign_order_id) AS INTEGER) order_id,
                ncm.tracking_number,
                ncm.carrier_name,
                MIN(CASE WHEN narvar_milestone_event in ('Picked-up by Carrier / In-Transit: General') THEN milestone_datetime END) AS carrier_ingestion_datetime,
                MAX(CASE WHEN narvar_milestone_event in ('Delivered: General','Delivered to address other than recipient','Delivered to a pickup point','Picked up by customer') THEN milestone_datetime END) AS carrier_delivered_datetime,
                MAX(CASE WHEN narvar_milestone_event in ('Delivered to Sender','Refused by customer','Returned to shipper','Undeliverable: General','In Transit to Sender','Final Delivery Attempt Failed','Canceled by Shipper') THEN milestone_datetime END) AS returned_datetime,
                MAX(CASE WHEN narvar_milestone_event in ('In-Transit to Alternate Address','In-Transit to Pickup Point','Out For Delivery: General','Pickup Point Requested','Redirected to Pickup Point') THEN milestone_datetime END) AS en_route_datetime,
                MAX(CASE WHEN narvar_milestone_event in ('Carrier Delay','Delay: General','On Hold','Weather Event','Weather') THEN milestone_datetime END) AS shipment_delayed_datetime,
                MAX(CASE WHEN narvar_milestone_event in ('Damaged/Disposed ','Lost') THEN milestone_datetime END) AS shipment_damaged_datetime,
                MAX(CASE WHEN narvar_milestone_event in ('Delivery Tomorrow') THEN milestone_datetime END) AS estimated_delivery_datetime,
                MAX(CASE WHEN narvar_milestone_event in ('Drop-off at Carrier','Handoff to Local Carrier') THEN milestone_datetime END) AS arrived_at_terminal_datetime,
                MAX(CASE WHEN narvar_milestone_event in ('Just Shipped (Manifest Created): General') THEN milestone_datetime END) AS shipment_acknowledged_datetime
FROM reporting_prod.gsc.narvar_carrier_milestones ncm
LEFT JOIN _get_order_ids oid
    ON ncm.tracking_number = oid.tracking_number
WHERE meta_update_datetime >= $last_update
GROUP BY COALESCE(ncm.order_id, oid.foreign_order_id),
         ncm.tracking_number,
         carrier_name;

MERGE INTO reporting_base_prod.gsc.narvar_consolidated_milestones AS m
USING _new_tracking_num_info AS n
ON (
    m.tracking_number = n.tracking_number
    AND m.carrier_name = n.carrier_name
    AND (
        (m.order_id = n.order_id)
        OR (m.order_id IS NULL AND n.order_id IS NULL)
        )
    )
WHEN MATCHED THEN
    UPDATE SET
--         m.order_id = coalesce(n.order_id,m.order_id),
--         m.carrier_name = coalesce(n.carrier_name,m.carrier_name),
--         m.tracking_number = coalesce(n.tracking_number,m.tracking_number),
        m.carrier_ingestion_datetime = COALESCE(n.carrier_ingestion_datetime,m.carrier_ingestion_datetime),
        m.carrier_delivered_datetime = COALESCE(n.carrier_delivered_datetime,m.carrier_delivered_datetime),
        m.returned_datetime = COALESCE(n.returned_datetime,m.returned_datetime),
        m.en_route_datetime = COALESCE(n.en_route_datetime,m.en_route_datetime),
        m.shipment_delayed_datetime = COALESCE(n.shipment_delayed_datetime,m.shipment_delayed_datetime),
        m.shipment_damaged_datetime = COALESCE(n.shipment_damaged_datetime,m.shipment_damaged_datetime),
        m.estimated_delivery_datetime = COALESCE(n.estimated_delivery_datetime,m.estimated_delivery_datetime),
        m.arrived_at_terminal_datetime = COALESCE(n.arrived_at_terminal_datetime,m.arrived_at_terminal_datetime),
        m.shipment_acknowledged_datetime = COALESCE(n.shipment_acknowledged_datetime,m.shipment_acknowledged_datetime),
        m.meta_update_datetime = current_timestamp
WHEN NOT MATCHED THEN
    INSERT (
       order_id,
       tracking_number,
       carrier_name,
       carrier_ingestion_datetime,
       carrier_delivered_datetime,
       returned_datetime,
       en_route_datetime,
       shipment_delayed_datetime,
       shipment_damaged_datetime,
       estimated_delivery_datetime,
       arrived_at_terminal_datetime,
       shipment_acknowledged_datetime,
       meta_create_datetime,
       meta_update_datetime
    )
    VALUES (
        n.order_id,
        n.tracking_number,
        n.carrier_name,
        n.carrier_ingestion_datetime,
        n.carrier_delivered_datetime,
        n.returned_datetime,
        n.en_route_datetime,
        n.shipment_delayed_datetime,
        n.shipment_damaged_datetime,
        n.estimated_delivery_datetime,
        n.arrived_at_terminal_datetime,
        n.shipment_acknowledged_datetime,
        current_timestamp,
        current_timestamp
    );

COMMIT;
