
CREATE OR REPLACE VIEW data_model_fl.fact_ebs_bulk_shipment
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
    stg.udf_unconcat_brand(item_id) AS item_id,
    stg.udf_unconcat_brand(product_id) AS product_id,
    facility,
    sales_order_id, -- lake.ultra_warehouse.inventory_log
    line_number,
    from_sub_inventory,
    shipped_quantity,
    system_datetime,
    shipment_local_datetime,
    meta_create_datetime,
    meta_update_datetime
FROM stg.fact_ebs_bulk_shipment
WHERE substring(item_id, -2) = '20'
  AND substring(product_id, -2) = '20';
