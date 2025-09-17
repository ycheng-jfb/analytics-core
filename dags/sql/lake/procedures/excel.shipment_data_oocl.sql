BEGIN;
DELETE FROM lake_stg.excel.shipment_data_oocl_stg;
COPY INTO lake_stg.excel.shipment_data_oocl_stg (business_unit, jf_traffic_mode, po, line_num, origin, por, origin_country_region, cargo_fnd, final_destination_country_region, post_advice_number, container, bl, size_type, vendor_code, start_ship_window, end_ship_window, showroom_date, trading_terms_po, transport_mode, business_type, style_color, description, hts_number, units_ordered, units_booked, units_shipped, packs_booked, unit_cost, pol, pol_country_region, pod, pod_country_region, total_item_volume_cbm, total_item_weight_kg, carrier, traffic_mode, pol_etd, pod_eta, launch_date, vessel_voyage_of_all_routes)
FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.shipment_data_oocl/v3/'
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%'
;
MERGE INTO lake.excel.shipment_data_oocl t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            business_unit,
            jf_traffic_mode,
            po,
            CAST(line_num AS NUMBER) AS line_num,
            origin,
            por,
            origin_country_region,
            cargo_fnd,
            final_destination_country_region,
            post_advice_number,
            container,
            bl,
            size_type,
            vendor_code,
            start_ship_window,
            end_ship_window,
            showroom_date,
            trading_terms_po,
            transport_mode,
            business_type,
            style_color,
            description,
            hts_number,
            units_ordered,
            units_booked,
            units_shipped,
            packs_booked,
            unit_cost,
            pol,
            pol_country_region,
            pod,
            pod_country_region,
            total_item_volume_cbm,
            total_item_weight_kg,
            carrier,
            traffic_mode,
            pol_etd,
            pod_eta,
            launch_date,
            vessel_voyage_of_all_routes,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY po, post_advice_number, container, bl, style_color, line_num ORDER BY NULL) AS rn
        FROM lake_stg.excel.shipment_data_oocl_stg
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.po, s.po)
    AND equal_null(t.post_advice_number, s.post_advice_number)
    AND equal_null(t.container, s.container)
    AND equal_null(t.bl, s.bl)
    AND equal_null(t.style_color, s.style_color)
    AND equal_null(t.line_num, s.line_num)
WHEN NOT MATCHED THEN INSERT (
    business_unit, jf_traffic_mode, po, line_num, origin, por, origin_country_region, cargo_fnd, final_destination_country_region, post_advice_number, container, bl, size_type, vendor_code, start_ship_window, end_ship_window, showroom_date, trading_terms_po, transport_mode, business_type, style_color, description, hts_number, units_ordered, units_booked, units_shipped, packs_booked, unit_cost, pol, pol_country_region, pod, pod_country_region, total_item_volume_cbm, total_item_weight_kg, carrier, meta_row_hash, meta_create_datetime, meta_update_datetime, traffic_mode, pol_etd, pod_eta, launch_date, vessel_voyage_of_all_routes
)
VALUES (
    business_unit, jf_traffic_mode, po, line_num, origin, por, origin_country_region, cargo_fnd, final_destination_country_region, post_advice_number, container, bl, size_type, vendor_code, start_ship_window, end_ship_window, showroom_date, trading_terms_po, transport_mode, business_type, style_color, description, hts_number, units_ordered, units_booked, units_shipped, packs_booked, unit_cost, pol, pol_country_region, pod, pod_country_region, total_item_volume_cbm, total_item_weight_kg, carrier, meta_row_hash, meta_create_datetime, meta_update_datetime, traffic_mode, pol_etd, pod_eta, launch_date, vessel_voyage_of_all_routes
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN UPDATE
SET t.business_unit = s.business_unit,
    t.jf_traffic_mode = s.jf_traffic_mode,
    t.po = s.po,
    t.line_num = s.line_num,
    t.origin = s.origin,
    t.por = s.por,
    t.origin_country_region = s.origin_country_region,
    t.cargo_fnd = s.cargo_fnd,
    t.final_destination_country_region = s.final_destination_country_region,
    t.post_advice_number = s.post_advice_number,
    t.container = s.container,
    t.bl = s.bl,
    t.size_type = s.size_type,
    t.vendor_code = s.vendor_code,
    t.start_ship_window = s.start_ship_window,
    t.end_ship_window = s.end_ship_window,
    t.showroom_date = s.showroom_date,
    t.trading_terms_po = s.trading_terms_po,
    t.transport_mode = s.transport_mode,
    t.business_type = s.business_type,
    t.style_color = s.style_color,
    t.description = s.description,
    t.hts_number = s.hts_number,
    t.units_ordered = s.units_ordered,
    t.units_booked = s.units_booked,
    t.units_shipped = s.units_shipped,
    t.packs_booked = s.packs_booked,
    t.unit_cost = s.unit_cost,
    t.pol = s.pol,
    t.pol_country_region = s.pol_country_region,
    t.pod = s.pod,
    t.pod_country_region = s.pod_country_region,
    t.total_item_volume_cbm = s.total_item_volume_cbm,
    t.total_item_weight_kg = s.total_item_weight_kg,
    t.carrier = s.carrier,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime,
    t.traffic_mode = s.traffic_mode,
    t.pol_etd = s.pol_etd,
    t.pod_eta = s.pod_eta,
    t.launch_date = s.launch_date,
    t.vessel_voyage_of_all_routes = s.vessel_voyage_of_all_routes
;
COMMIT;
