SELECT DISTINCT ship_bol,
                container_label,
                milestone_datetime,
                milestone_status::int as milestone_status
FROM (
    SELECT bill_of_lading                                   AS ship_bol,
           container_id                                     as container_label,
           TO_DATE(est_point_of_discharge_date, 'YYYYMMDD') AS "608",
           TO_DATE(est_delivery_date, 'YYYYMMDD')           AS "607",
           TO_DATE(departure_date, 'YYYYMMDD')              AS "606",
           current_date                                     as "633"
    FROM REPORTING_PROD.SPS.ASN l
    where l.meta_create_datetime > %(low_watermark)s
) a
    UNPIVOT ( milestone_datetime
    FOR milestone_status IN ( "606", "607", "608", "633" ) )
