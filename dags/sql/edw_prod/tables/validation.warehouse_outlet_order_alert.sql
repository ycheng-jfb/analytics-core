CREATE OR REPLACE TRANSIENT TABLE validation.warehouse_outlet_order_alert
(
    order_id INT,
    alert_type VARCHAR(30),
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);
