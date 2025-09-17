
CREATE OR REPLACE VIEW data_model.fact_ebs_bulk_shipment
(
    transaction_id,
    item_number,
    business_unit,
    item_id,
    product_id,
    facility,
    sales_order_id,
    line_number,
    from_sub_inventory,
    shipped_quantity,
    system_datetime,
    shipment_local_datetime,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    transaction_id,
    item_number,
    business_unit,
    item_id,
    product_id,
    facility,
    sales_order_id,
    line_number,
    from_sub_inventory,
    shipped_quantity,
    system_datetime,
    shipment_local_datetime,
    meta_create_datetime,
    meta_update_datetime
FROM stg.fact_ebs_bulk_shipment;
