CREATE OR REPLACE TASK util.tasks_central_dp.gsc_landed_cost_01_cbm_standrd_aggregation_daily
    WAREHOUSE =da_wh_analytics
    SCHEDULE = 'USING CRON 07 03 * * * America/Los_Angeles'
    ERROR_INTEGRATION =SNOWFLAKE_TASK_SNS_INTEGRATION
    AS
BEGIN

    USE SCHEMA reporting_base_prod.gsc;

    CREATE OR REPLACE TEMPORARY TABLE _ship AS
    SELECT o.po                                                       AS po_number,
           o.style_color                                              AS item_number,
           COALESCE(psd.bc_po_line_number, o.line_num, psd.po_dtl_id) AS po_line_number,
           NVL(o.post_advice_number, 'null')                          AS post_advice_number,
           NVL(o.container, 'null')                                   AS container,
           NVL(o.bl, 'nul')                                           AS bl,
           CASE
               WHEN UPPER(o.jf_traffic_mode) = 'AD'
                   THEN 'AIR (DEFERRED)'
               WHEN UPPER(o.jf_traffic_mode) = 'AE'
                   THEN 'AIR (EXPEDITED)'
               WHEN UPPER(o.jf_traffic_mode) = 'AS'
                   THEN 'AIR (STANDARD)'
               WHEN UPPER(o.jf_traffic_mode) = 'CP'
                   THEN 'CIP VENDOR'
               WHEN UPPER(o.jf_traffic_mode) = 'DV'
                   THEN 'DDP VENDOR'
               WHEN UPPER(o.jf_traffic_mode) = 'OC'
                   THEN 'OCEAN'
               WHEN UPPER(o.jf_traffic_mode) = 'PA'
                   THEN 'SMALL PARCEL'
               WHEN UPPER(o.jf_traffic_mode) = 'TR'
                   THEN 'TRUCK'
               ELSE UPPER(o.jf_traffic_mode)
               END                                                    AS jf_traffic_mode,
           o.carrier,
           UPPER(o.final_destination_country_region)                  AS final_destination_country_region,
           --UPPER(o.TRANSPORT_MODE) AS TRANSPORT_MODE,
           SUM(o.units_shipped)                                       AS units_shipped,
           SUM(o.total_item_volume_cbm)                               AS total_item_volume_cbm,
           SUM(o.total_item_weight_kg)                                AS total_item_weight_kg,
           psd.po_dtl_id,
           MAX(o.meta_update_datetime)                                AS high_water_mark,
           o.pod
    FROM lake_view.excel.shipment_data_oocl o
             LEFT JOIN gsc.po_skus_data psd
                       ON o.po = psd.po_number
                           AND o.style_color = psd.sku
                           AND (
                              NVL(o.line_num, 0) = NVL(psd.bc_po_line_number, 1)
                                  OR NVL(o.line_num, 0) = NVL(psd.po_line_number, 1)
                                  OR NVL(o.line_num, 0) = 0
                              )
    WHERE 1 = 1
      AND o.pol_etd >= '2023-01-01'
    --and psd.qty > 0
--and psd.is_cancelled = FALSE
--and o.meta_update_datetime > (select max(high_water_mark) from gsc.shipment_data)

    GROUP BY o.po,
             o.style_color,
             COALESCE(psd.bc_po_line_number, o.line_num, psd.po_dtl_id),
             NVL(o.post_advice_number, 'null'),
             NVL(o.container, 'null'),
             NVL(o.bl, 'nul'),
             CASE
                 WHEN UPPER(o.jf_traffic_mode) = 'AD'
                     THEN 'AIR (DEFERRED)'
                 WHEN UPPER(o.jf_traffic_mode) = 'AE'
                     THEN 'AIR (EXPEDITED)'
                 WHEN UPPER(o.jf_traffic_mode) = 'AS'
                     THEN 'AIR (STANDARD)'
                 WHEN UPPER(o.jf_traffic_mode) = 'CP'
                     THEN 'CIP VENDOR'
                 WHEN UPPER(o.jf_traffic_mode) = 'DV'
                     THEN 'DDP VENDOR'
                 WHEN UPPER(o.jf_traffic_mode) = 'OC'
                     THEN 'OCEAN'
                 WHEN UPPER(o.jf_traffic_mode) = 'PA'
                     THEN 'SMALL PARCEL'
                 WHEN UPPER(o.jf_traffic_mode) = 'TR'
                     THEN 'TRUCK'
                 ELSE UPPER(o.jf_traffic_mode)
                 END,
             o.carrier,
             UPPER(o.final_destination_country_region),
--UPPER(o.TRANSPORT_MODE),
             psd.po_dtl_id,
             o.pod

    UNION

    SELECT DISTINCT o.po                                                        AS po_number,
                    o.style_color                                               AS item_number,
                    COALESCE(psd.bc_po_line_number, o.line_num, psd.po_dtl_id)  AS po_line_number,
                    NVL(CAST(o.pre_advice_number AS VARCHAR(16777216)), 'null') AS post_advice_number,
                    NVL(o.domestic_trailer, 'null')                             AS container,
                    NVL(o.domestic_bl, 'null')                                  AS bl,
                    --UPPER(o.JF_TRAFFIC_MODE) AS JF_TRAFFIC_MODE,
                    CASE
                        WHEN UPPER(o.jf_traffic_mode) = 'AD'
                            THEN 'AIR (DEFERRED)'
                        WHEN UPPER(o.jf_traffic_mode) = 'AE'
                            THEN 'AIR (EXPEDITED)'
                        WHEN UPPER(o.jf_traffic_mode) = 'AS'
                            THEN 'AIR (STANDARD)'
                        WHEN UPPER(o.jf_traffic_mode) = 'CP'
                            THEN 'CIP VENDOR'
                        WHEN UPPER(o.jf_traffic_mode) = 'DV'
                            THEN 'DDP VENDOR'
                        WHEN UPPER(o.jf_traffic_mode) = 'OC'
                            THEN 'OCEAN'
                        WHEN UPPER(o.jf_traffic_mode) = 'PA'
                            THEN 'SMALL PARCEL'
                        WHEN UPPER(o.jf_traffic_mode) = 'TR'
                            THEN 'TRUCK'
                        ELSE UPPER(o.jf_traffic_mode)
                        END                                                     AS jf_traffic_mode,
                    o.carrier,
                    UPPER(o.final_destination_country_region)                   AS final_destination_country_region,
                    --UPPER(o.TRANSPORT_MODE) AS TRANSPORT_MODE,
                    SUM(o.units_ordered)                                        AS units_shipped,
                    SUM(o.vol_cbm)                                              AS total_item_volume_cbm,
                    SUM(o.gwt_pound)                                            AS total_item_weight_kg,
                    psd.po_dtl_id,
                    MAX(o.meta_update_datetime)                                 AS high_water_mark,
                    o.pod
    FROM lake_view.gsc.shipment_data_domestic_oocl o
             LEFT JOIN gsc.po_skus_data psd
                       ON UPPER(TRIM(psd.po_number)) = UPPER(TRIM(o.po))
                           AND UPPER(TRIM(psd.sku)) = UPPER(TRIM(o.style_color))
                           AND (
                              NVL(o.line_num, 0) = NVL(psd.bc_po_line_number, 1)
                                  OR NVL(o.line_num, 0) = NVL(psd.po_line_number, 1)
                                  OR NVL(o.line_num, 0) = 0
                              )
    WHERE 1 = 1
      AND start_ship_window >= '2023-01-01'
    --and psd.qty > 0
    -- and psd.is_cancelled = FALSE
    --and o.meta_update_datetime > (select max(high_water_mark) from gsc.shipment_data)

    GROUP BY o.po,
             o.style_color,
             COALESCE(psd.bc_po_line_number, o.line_num, psd.po_dtl_id),
             NVL(CAST(o.pre_advice_number AS VARCHAR(16777216)), 'null'),
             NVL(o.domestic_trailer, 'null'),
             NVL(o.domestic_bl, 'null'),
             CASE
                 WHEN UPPER(o.jf_traffic_mode) = 'AD'
                     THEN 'AIR (DEFERRED)'
                 WHEN UPPER(o.jf_traffic_mode) = 'AE'
                     THEN 'AIR (EXPEDITED)'
                 WHEN UPPER(o.jf_traffic_mode) = 'AS'
                     THEN 'AIR (STANDARD)'
                 WHEN UPPER(o.jf_traffic_mode) = 'CP'
                     THEN 'CIP VENDOR'
                 WHEN UPPER(o.jf_traffic_mode) = 'DV'
                     THEN 'DDP VENDOR'
                 WHEN UPPER(o.jf_traffic_mode) = 'OC'
                     THEN 'OCEAN'
                 WHEN UPPER(o.jf_traffic_mode) = 'PA'
                     THEN 'SMALL PARCEL'
                 WHEN UPPER(o.jf_traffic_mode) = 'TR'
                     THEN 'TRUCK'
                 ELSE UPPER(o.jf_traffic_mode)
                 END,
             o.carrier,
             UPPER(o.final_destination_country_region),
--UPPER(o.TRANSPORT_MODE),
             psd.po_dtl_id,
             o.pod;


    UPDATE _ship
    SET po_line_number = NVL(psd.bc_po_line_number, psd.po_dtl_id)
      , po_dtl_id      = psd.po_dtl_id
    FROM (SELECT *
          FROM (SELECT po_number,
                       sku,
                       po_dtl_id,
                       bc_po_line_number,
                       ROW_NUMBER() OVER (PARTITION BY po_number, sku ORDER BY bc_po_line_number ASC) AS rn
                FROM gsc.po_skus_data
                WHERE qty > 0
                  AND is_cancelled = FALSE) a
          WHERE rn = 1) psd
    WHERE _ship.po_number = psd.po_number
      AND _ship.item_number = psd.sku
      AND _ship.po_line_number IS NULL;

    DELETE FROM _ship WHERE NVL(po_dtl_id, -1) = -1;

    MERGE INTO gsc.shipment_data tgt
        USING _ship src
        ON tgt.post_advice_number = src.post_advice_number
            AND tgt.container = src.container
            AND tgt.bl = src.bl
            AND tgt.po_dtl_id = src.po_dtl_id
        WHEN MATCHED THEN
            UPDATE
                SET
                    tgt.po_number = src.po_number,
                    tgt.item_number = src.item_number,
                    tgt.po_line_number = src.po_line_number ,
                    tgt.post_advice_number = src.post_advice_number ,
                    tgt.container = src.container ,
                    tgt.bl = src.bl ,
                    tgt.jf_traffic_mode = src.jf_traffic_mode ,
                    tgt.carrier = src.carrier ,
                    tgt.final_destination_country_region = src.final_destination_country_region ,
--tgt.transport_mode = src.transport_mode,
                    tgt.units_shipped = src.units_shipped ,
                    tgt.total_item_volume_cbm = src.total_item_volume_cbm,
                    tgt.total_item_weight_kg = src.total_item_weight_kg ,
                    tgt.po_dtl_id = src.po_dtl_id,
                    tgt.high_water_mark = src.high_water_mark,
                    tgt.meta_update_datetime = CURRENT_TIMESTAMP(),
                    tgt.pod = src.pod
        WHEN NOT MATCHED THEN
            INSERT
                (
                 po_number,
                 item_number,
                 po_line_number,
                 post_advice_number,
                 container,
                 bl,
                 jf_traffic_mode,
                 carrier,
                 final_destination_country_region,
--transport_mode,
                 units_shipped,
                 total_item_volume_cbm,
                 total_item_weight_kg,
                 po_dtl_id,
                 high_water_mark,
                 pod
                    )
                VALUES (src.po_number,
                        src.item_number,
                        src.po_line_number,
                        src.post_advice_number,
                        src.container,
                        src.bl,
                        src.jf_traffic_mode,
                        src.carrier,
                        src.final_destination_country_region,
--src.transport_mode,
                        src.units_shipped,
                        src.total_item_volume_cbm,
                        src.total_item_weight_kg,
                        src.po_dtl_id,
                        src.high_water_mark,
                        src.pod);


END;
