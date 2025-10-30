truncate table EDW_PROD.NEW_STG.fact_order_shipment;

-- TOC 部分
insert into EDW_PROD.NEW_STG.fact_order_shipment
SELECT
    t3.order_code AS order_id,
    t3.from_warehouse_code AS fulfillment_airport_code,
    t3.product_service_code AS carrier_service_id,
    t3.external_logistics_code AS tracking_number,
    t4.brand_name AS item_gender,
    t4.pairs AS item_count,
    t3.delivery_time_market AS estimated_shipped_local_datetime,
    NULL AS estimated_delivered_local_datetime,
    t3.signing_time_market AS carrier_delivered_local_datetime,
    t4.outbound_weight_lb AS actual_weight,
    CASE WHEN t4.outbound_weight_lb < 1 THEN 1 ELSE 0 END AS is_sub_one_pound,
    '' AS zone_rate_shopping,
    0.0 AS base_rate,
    0.0 AS total_surcharges,
    0.0 AS net_charge_amount,
    0.0 AS discount,
    0.0 AS shipment_freight_charge_before_discount,
    0.0 AS fuel_surcharge,
    t3.is_residence AS residential_surcharge,
    '' AS address_id,
    0 AS is_split_order,
    t3.is_remote AS is_out_of_region_order,
    t3.transport_type AS order_type,
    t3.transport_type AS shipment_type,
    t3.system_create_time_market AS carrier_ingested_local_datetime,
    0 AS is_scrub,
    t4.outbound_length_in AS package_length,
    t4.outbound_width_in AS package_width,
    t4.outbound_height_in AS package_height,
    'in' AS package_dim_unit,
    true AS only_shoes,
    t3.create_time AS meta_create_datetime,
    t3.modify_time AS meta_update_datetime
FROM
    LAKE.MMT.ods_tms_plus_tms_toc_transport_df t3
JOIN
    LAKE.MMT.ods_tms_plus_tms_toc_transport_detail_df t4
    ON t3.transport_code = t4.transport_code
and t3.PT_DAY = t4.PT_DAY
where t3.PT_DAY = current_date();

