BEGIN;

DELETE FROM lake_stg.gsc.booking_data_stg;

COPY INTO lake_stg.gsc.booking_data_stg (
    so, po, style_color, vendor_booking_received_date, cargo_ready_date, cargo_receive_date, fnd_eta,
    revised_fnd_date, dc_appointment_date, gate_in_date, pol_etd, pod_eta, pol, pod, fnd, traffic_mode, vol, gwt,
    units, pack, carrier_bkg_place_date, carrier_bkg_confirm_date, file_name, mixcarton_flag, line_item_num
    )
FROM (
    SELECT
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,
        metadata$filename AS file_name, $23, $24
    from '@lake_stg.public.tsos_da_int_inbound/lake/gsc.booking_data/v3/'
    )
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = ',',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%'
;


MERGE INTO lake.gsc.booking_data t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            *,
            hash(*) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER(PARTITION BY so, po, style_color ORDER BY file_name desc, units desc ) AS rn
        FROM lake_stg.gsc.booking_data_stg
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.so, s.so)
    AND equal_null(t.po, s.po)
    AND equal_null(t.style_color, s.style_color)
WHEN NOT MATCHED THEN INSERT (
    so, po, style_color, vendor_booking_received_date, cargo_ready_date, cargo_receive_date, fnd_eta, revised_fnd_date,
    dc_appointment_date, gate_in_date, pol_etd, pod_eta, pol, pod, fnd, traffic_mode, vol, gwt, units, pack,
    carrier_bkg_place_date, carrier_bkg_confirm_date, file_name, meta_row_hash, meta_create_datetime,
    meta_update_datetime, mixcarton_flag, line_item_num
)
VALUES (
    so, po, style_color, vendor_booking_received_date, cargo_ready_date, cargo_receive_date, fnd_eta, revised_fnd_date,
    dc_appointment_date, gate_in_date, pol_etd, pod_eta, pol, pod, fnd, traffic_mode, vol, gwt, units, pack,
    carrier_bkg_place_date, carrier_bkg_confirm_date, file_name, meta_row_hash, meta_create_datetime,
    meta_update_datetime, mixcarton_flag, line_item_num
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN UPDATE
SET t.so = s.so,
    t.po = s.po,
    t.style_color = s.style_color,
    t.vendor_booking_received_date = s.vendor_booking_received_date,
    t.cargo_ready_date = s.cargo_ready_date,
    t.cargo_receive_date = s.cargo_receive_date,
    t.fnd_eta = s.fnd_eta,
    t.revised_fnd_date = s.revised_fnd_date,
    t.dc_appointment_date = s.dc_appointment_date,
    t.gate_in_date = s.gate_in_date,
    t.pol_etd = s.pol_etd,
    t.pod_eta = s.pod_eta,
    t.pol = s.pol,
    t.pod = s.pod,
    t.fnd = s.fnd,
    t.traffic_mode = s.traffic_mode,
    t.vol = s.vol,
    t.gwt = s.gwt,
    t.units = s.units,
    t.pack = s.pack,
    t.carrier_bkg_place_date = s.carrier_bkg_place_date,
    t.carrier_bkg_confirm_date = s.carrier_bkg_confirm_date,
    t.file_name = s.file_name,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime,
    t.mixcarton_flag = s.mixcarton_flag,
    t.line_item_num = s.line_item_num
;

COMMIT;
