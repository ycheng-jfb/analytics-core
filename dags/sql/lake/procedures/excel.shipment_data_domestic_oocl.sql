BEGIN;
DELETE FROM lake_stg.excel.shipment_data_domestic_oocl_stg;
COPY INTO lake_stg.excel.shipment_data_domestic_oocl_stg (business_unit, carrier, jf_traffic_mode, traffic_mode, po, origin, por, origin_country_region, cargo_fnd, final_destination_country_region, origin_container, domestic_bl, domestic_trailer, size_type, vendor_code, start_ship_window, end_ship_window, showroom_date, trading_terms, transport_mode, business_type, style_color, destination, units_booked, units_ordered, units_shipped, pre_advice_number, post_advice_number, pol, pol_country_region, pod, total_item_volume_cbm, total_item_weight_kg, vol_cbm, gwt_pound, line_num)
FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.shipment_data_domestic_oocl/v3/'
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
MERGE INTO lake.excel.shipment_data_domestic_oocl t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            business_unit,
            carrier,
            jf_traffic_mode,
            traffic_mode,
            po,
            origin,
            por,
            origin_country_region,
            cargo_fnd,
            final_destination_country_region,
            origin_container,
            domestic_bl,
            domestic_trailer,
            size_type,
            vendor_code,
            start_ship_window,
            end_ship_window,
            showroom_date,
            trading_terms,
            transport_mode,
            business_type,
            style_color,
            destination,
            units_booked,
            units_ordered,
            units_shipped,
            pre_advice_number,
            post_advice_number,
            pol,
            pol_country_region,
            pod,
            total_item_volume_cbm,
            total_item_weight_kg,
            vol_cbm,
            gwt_pound,
            CAST(line_num AS INT) AS line_num,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY po, origin_container, domestic_bl, post_advice_number, style_color, line_num ORDER BY NULL ) AS rn
        FROM lake_stg.excel.shipment_data_domestic_oocl_stg
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.po, s.po)
    AND equal_null(t.origin_container, s.origin_container)
    AND equal_null(t.domestic_bl, s.domestic_bl)
    AND equal_null(t.post_advice_number, s.post_advice_number)
    AND equal_null(t.style_color, s.style_color)
    AND equal_null(t.line_num, s.line_num)
WHEN NOT MATCHED THEN INSERT (
    business_unit, carrier, jf_traffic_mode, traffic_mode, po, origin, por, origin_country_region, cargo_fnd, final_destination_country_region, origin_container, domestic_bl, domestic_trailer, size_type, vendor_code, start_ship_window, end_ship_window, showroom_date, trading_terms, transport_mode, business_type, style_color, destination, units_booked, units_ordered, units_shipped, pre_advice_number, post_advice_number, pol, pol_country_region, pod, total_item_volume_cbm, total_item_weight_kg, vol_cbm, gwt_pound, line_num, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    business_unit, carrier, jf_traffic_mode, traffic_mode, po, origin, por, origin_country_region, cargo_fnd, final_destination_country_region, origin_container, domestic_bl, domestic_trailer, size_type, vendor_code, start_ship_window, end_ship_window, showroom_date, trading_terms, transport_mode, business_type, style_color, destination, units_booked, units_ordered, units_shipped, pre_advice_number, post_advice_number, pol, pol_country_region, pod, total_item_volume_cbm, total_item_weight_kg, vol_cbm, gwt_pound, line_num, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN UPDATE
SET t.business_unit = s.business_unit,
    t.carrier = s.carrier,
    t.jf_traffic_mode = s.jf_traffic_mode,
    t.traffic_mode = s.traffic_mode,
    t.origin = s.origin,
    t.por = s.por,
    t.origin_country_region = s.origin_country_region,
    t.cargo_fnd = s.cargo_fnd,
    t.final_destination_country_region = s.final_destination_country_region,
    t.domestic_trailer = s.domestic_trailer,
    t.size_type = s.size_type,
    t.vendor_code = s.vendor_code,
    t.start_ship_window = s.start_ship_window,
    t.end_ship_window = s.end_ship_window,
    t.showroom_date = s.showroom_date,
    t.trading_terms = s.trading_terms,
    t.transport_mode = s.transport_mode,
    t.business_type = s.business_type,
    t.destination = s.destination,
    t.units_booked = s.units_booked,
    t.units_ordered = s.units_ordered,
    t.units_shipped = s.units_shipped,
    t.pre_advice_number = s.pre_advice_number,
    t.pol = s.pol,
    t.pol_country_region = s.pol_country_region,
    t.pod = s.pod,
    t.total_item_volume_cbm = s.total_item_volume_cbm,
    t.total_item_weight_kg = s.total_item_weight_kg,
    t.vol_cbm = s.vol_cbm,
    t.gwt_pound = s.gwt_pound,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;
COMMIT;
