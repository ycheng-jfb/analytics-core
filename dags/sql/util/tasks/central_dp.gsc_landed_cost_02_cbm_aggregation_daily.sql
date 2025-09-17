CREATE OR REPLACE TASK util.tasks_central_dp.gsc_landed_cost_02_cbm_aggregation_daily
    WAREHOUSE =da_wh_analytics
    AFTER util.tasks_central_dp.gsc_landed_cost_01_cbm_standrd_aggregation_daily
    AS BEGIN


        USE SCHEMA reporting_base_prod.gsc;

/***  24 scripts
***/

        CREATE OR REPLACE TEMPORARY TABLE _ship AS

        SELECT sd.*
        FROM gsc.shipment_data sd;


        CREATE OR REPLACE TEMPORARY TABLE _cbm
        (
            datapoint          VARCHAR(255)   DEFAULT 'null',
            container          VARCHAR(255)   DEFAULT 'null',
            bl                 VARCHAR(255)   DEFAULT 'null',
            post_advice_number VARCHAR(255)   DEFAULT 'null',
            po_number          VARCHAR(255)   DEFAULT 'null',
            item_number        VARCHAR(255)   DEFAULT 'null',
            business_unit      VARCHAR(255)   DEFAULT 'null',
            order_number       VARCHAR(255)   DEFAULT 'null',
            traffic_mode       VARCHAR(255)   DEFAULT 'null',
            invoice_number     VARCHAR(255)   DEFAULT 'null',
            cbm                NUMBER(38, 10) DEFAULT 0,
            units_shipped      BIGINT         DEFAULT 0,
            amount_usd         NUMBER(38, 12) DEFAULT 0,
            po_line_number     NUMBER(38, 0)  DEFAULT 0,
            po_dtl_id          NUMBER(38, 0)  DEFAULT 0
        );

        INSERT INTO _cbm
        (datapoint, po_number, item_number, po_line_number, po_dtl_id, post_advice_number, container, cbm,
         units_shipped)
        SELECT 'po_sku_pa_con'            AS datapoint,
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               post_advice_number         AS post_advice,
               container,

               SUM(total_item_volume_cbm) AS cbm,
               SUM(units_shipped)         AS units_shipped
        FROM _ship

        GROUP BY po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id,
                 post_advice_number,
                 container;

        INSERT INTO _cbm
            (datapoint, post_advice_number, container, cbm, units_shipped)
        SELECT 'pa_con',
               post_advice_number         AS post_advice,
               container,
               SUM(total_item_volume_cbm) AS cbm,
               SUM(units_shipped)         AS units_shipped
        FROM _ship
        WHERE UPPER(jf_traffic_mode) = 'OCEAN'
        GROUP BY post_advice_number,
                 container;

        INSERT INTO _cbm
        (datapoint, po_number, item_number, po_line_number, po_dtl_id, container, bl, cbm, units_shipped)
        SELECT 'po_sku_con_bl',
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               container,
               bl,
               SUM(total_item_volume_cbm) AS cbm,
               SUM(units_shipped)         AS units_shipped
        FROM _ship
        GROUP BY po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id,
                 container,
                 bl;

        INSERT INTO _cbm
        (datapoint, po_number, item_number, po_line_number, po_dtl_id, container, cbm, units_shipped)
        SELECT 'po_sku_con',
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               container,
               SUM(total_item_volume_cbm) AS cbm,
               SUM(units_shipped)         AS units_shipped
        FROM _ship

        GROUP BY po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id,
                 container;

        INSERT INTO _cbm
            (datapoint, container, bl, cbm, units_shipped)
        SELECT 'con_bl',
               container,
               bl,
               SUM(total_item_volume_cbm) AS cbm,
               SUM(units_shipped)         AS units_shipped
        FROM _ship
        WHERE (carrier != 'OOCL'
            AND UPPER(jf_traffic_mode) = 'OCEAN')
           OR (UPPER(final_destination_country_region) = 'MEXICO'
            AND UPPER(transport_mode) = 'AIR')

        GROUP BY container,
                 bl;

        INSERT INTO _cbm
        (datapoint, po_number, item_number, po_line_number, po_dtl_id, post_advice_number, cbm, units_shipped)
        SELECT 'po_sku_pa',
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               post_advice_number         AS post_advice,
               SUM(total_item_volume_cbm) AS cbm,
               SUM(units_shipped)         AS units_shipped
        FROM _ship
        WHERE units_shipped > 0
        GROUP BY po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id,
                 post_advice_number;

        INSERT INTO _cbm
            (datapoint, post_advice_number, cbm)
        SELECT 'pa',
               post_advice_number         AS post_advice,
               SUM(total_item_volume_cbm) AS cbm
        FROM _ship

        GROUP BY post_advice_number;

        INSERT INTO _cbm
            (datapoint, container, cbm)
        SELECT 'con',
               container,
               SUM(total_item_volume_cbm) AS cbm
        FROM _ship

        GROUP BY container;

        INSERT INTO _cbm
            (datapoint, bl, cbm)
        SELECT 'bl',
               bl,
               SUM(total_item_weight_kg) AS kg
        FROM _ship
        WHERE UPPER(jf_traffic_mode) LIKE 'AIR%'

        GROUP BY bl;

        INSERT INTO _cbm
        (datapoint, po_number, item_number, po_line_number, po_dtl_id, bl, cbm, units_shipped)
        SELECT 'po_sku_bl',
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               bl,
               SUM(total_item_weight_kg) AS kg,
               SUM(units_shipped)        AS units_shipped
        FROM _ship
        WHERE UPPER(jf_traffic_mode) LIKE 'AIR%'

        GROUP BY po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id,
                 bl;

/*******new roll up*********/
        INSERT INTO _cbm
            (datapoint, post_advice_number, bl, cbm, units_shipped)
        SELECT 'pa_bl',
               post_advice_number,
               bl,
               SUM(total_item_weight_kg) AS kg,
               SUM(units_shipped)        AS units_shipped
        FROM _ship
        WHERE UPPER(jf_traffic_mode) LIKE 'AIR%'

        GROUP BY post_advice_number,
                 bl;

        INSERT INTO _cbm
        (datapoint, po_number, item_number, po_line_number, po_dtl_id, post_advice_number, bl, cbm, units_shipped)
        SELECT 'po_sku_pa_bl',
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               post_advice_number,
               bl,
               SUM(total_item_weight_kg) AS kg,
               SUM(units_shipped)        AS units_shipped
        FROM _ship
        WHERE UPPER(jf_traffic_mode) LIKE 'AIR%'

        GROUP BY po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id,
                 post_advice_number,
                 bl;


        MERGE INTO gsc.lc_cbm_data tgt
            USING _cbm src
            ON tgt.datapoint = src.datapoint
                AND NVL(tgt.container, 'unk') = NVL(src.container, 'unk')
                AND NVL(tgt.bl, 'unk') = NVL(src.bl, 'unk')
                AND NVL(tgt.post_advice_number, 'unk') = NVL(src.post_advice_number, 'unk')
                AND NVL(tgt.po_number, 'unk') = NVL(src.po_number, 'unk')
                AND NVL(tgt.item_number, 'unk') = NVL(src.item_number, 'unk')
                AND NVL(tgt.business_unit, 'unk') = NVL(src.business_unit, 'unk')
                AND NVL(tgt.order_number, 'unk') = NVL(src.order_number, 'unk')
                AND NVL(tgt.traffic_mode, 'unk') = NVL(src.traffic_mode, 'unk')
                AND NVL(tgt.invoice_number, 'unk') = NVL(src.invoice_number, 'unk')
                AND NVL(tgt.po_line_number, 0) = NVL(src.po_line_number, 0)
                AND NVL(tgt.po_dtl_id, 0) = NVL(src.po_dtl_id, 0)
            WHEN MATCHED THEN UPDATE
                SET tgt.cbm = src.cbm,
                    tgt.units_shipped = src.units_shipped,
                    tgt.amount_usd = src.amount_usd,
                    tgt.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(9)
            WHEN NOT MATCHED THEN INSERT
                (datapoint,
                 container,
                 bl,
                 post_advice_number,
                 po_number,
                 item_number,
                 business_unit,
                 order_number,
                 traffic_mode,
                 invoice_number,
                 amount_usd,
                 cbm,
                 units_shipped,
                 po_line_number,
                 po_dtl_id
                    )
                VALUES (src.datapoint,
                        src.container,
                        src.bl,
                        src.post_advice_number,
                        src.po_number,
                        src.item_number,
                        src.business_unit,
                        src.order_number,
                        src.traffic_mode,
                        src.invoice_number,
                        src.amount_usd,
                        src.cbm,
                        src.units_shipped,
                        src.po_line_number,
                        po_dtl_id);

/***********************
scripts needing
full dataset
***********************/
        TRUNCATE TABLE _cbm;

        CREATE OR REPLACE TEMPORARY TABLE _ship AS
        SELECT DISTINCT y.po_number, y.item_number, y.po_line_number, y.po_dtl_id
        FROM gsc.yard_container_data y
                 JOIN gsc.po_skus_data psd
                      ON y.po_dtl_id = psd.po_dtl_id
        WHERE 1 = 1
        -- received_date_80_percent >= dateadd(day, -200, current_timestamp())
--and nvl(y.carrier, 'broomstick') != 'CrossBorder'
        ;

        CREATE OR REPLACE TEMPORARY TABLE _order_number AS
        SELECT DISTINCT order_number
        FROM _ship p
                 JOIN gsc.yard_container_data yc
                      ON p.po_dtl_id = yc.po_dtl_id;


        CREATE OR REPLACE TEMPORARY TABLE _trailer AS
        SELECT DISTINCT yc.trailer_id
        FROM _ship p
                 JOIN gsc.yard_container_data yc
                      ON p.po_dtl_id = yc.po_dtl_id;

--REPORTING_BASE_PROD.GSC.LC_CBM_OTR_ORDER_DATASET(
        INSERT INTO _cbm
            (datapoint, order_number, cbm)
        SELECT 'otr_order'                                                                         AS datapoint,
               order_number,
               ((SUM(NVL(a.cbm, a.avg_cbm)) / SUM(NVL(a.units_shipped, a.qty))) * SUM(a.quantity)) AS cbm
        FROM (SELECT UPPER(TRIM(yc.order_number)) AS order_number,
                     c.po_number,
                     c.item_number,
                     c.cbm,
                     a.avg_cbm * p.qty            AS avg_cbm,
                     c.units_shipped,
                     p.qty,
                     yc.quantity
              FROM _order_number o
                       JOIN gsc.yard_container_data yc
                            ON o.order_number = yc.order_number
                       JOIN gsc.po_skus_data p
                            ON p.po_dtl_id = yc.po_dtl_id
                       LEFT JOIN gsc.lc_cbm_data c
                                 ON c.po_dtl_id = p.po_dtl_id
                                     AND c.datapoint = 'po_sku_pa'
                       LEFT JOIN reporting_base_prod.gsc.lc_cbm_product_type_avg_dataset a
                                 ON
                                     CASE
                                         WHEN p.division_id = 'LINGERIE'
                                             THEN 'SAVAGEX'
                                         ELSE p.division_id
                                         END = a.business_unit
                                         AND LEFT(p.sku, 2) = a.product_type) a
        GROUP BY a.order_number;


--REPORTING_BASE_PROD.GSC.LC_CBM_OTR_PO_SKU_DATASET(
        INSERT INTO _cbm
        (datapoint, po_number, item_number, po_dtl_id, po_line_number, order_number, container, units_shipped, cbm)
        SELECT datapoint,
               po_number,
               item_number,
               po_dtl_id,
               po_line_number,
               order_number,
               trailer_id,
               units_shipped,
               AVG(cbm) AS cbm
        FROM (SELECT 'otr_po_sku'                                                                       AS datapoint,
                     yc.po_number,
                     yc.item_number,
                     po.po_dtl_id,
                     po.po_line_number,
                     UPPER(TRIM(yc.order_number))                                                       AS order_number,
                     yc.trailer_id,
                     yc.quantity                                                                        AS units_shipped,
                     IFF(COALESCE(c.units_shipped, p.qty, 0) = 0, 0,
                         (NVL(c.cbm, (a.avg_cbm * p.qty)) / NVL(c.units_shipped, p.qty)) * yc.quantity) AS cbm,
              FROM (SELECT DISTINCT po_number, item_number, po_line_number, po_dtl_id FROM _ship) po
                       JOIN gsc.yard_container_data yc
                            ON po.po_dtl_id = yc.po_dtl_id
                       JOIN gsc.po_skus_data p
                            ON po.po_dtl_id = p.po_dtl_id
                       LEFT JOIN gsc.lc_cbm_data c
                                 ON c.po_dtl_id = p.po_dtl_id
                                     AND c.datapoint = 'po_sku_pa'

                       LEFT JOIN reporting_base_prod.gsc.lc_cbm_product_type_avg_dataset a
                                 ON
                                     CASE
                                         WHEN p.division_id = 'LINGERIE'
                                             THEN 'SAVAGEX'
                                         ELSE p.division_id
                                         END = a.business_unit
                                         AND LEFT(p.sku, 2) = a.product_type
              WHERE NVL(yc.order_number, '-6.02') != '-6.02')
        GROUP BY datapoint,
                 po_number,
                 item_number,
                 po_dtl_id,
                 po_line_number,
                 order_number,
                 trailer_id,
                 units_shipped;


--REPORTING_BASE_PROD.GSC.LC_CBM_UNITS_BLECKMANN_PO_SKU_BU_BL_DATASET
        INSERT INTO _cbm
        (datapoint, po_number, item_number, po_line_number, po_dtl_id, bl, business_unit, cbm, units_shipped)
        SELECT 'bleckmann_po_sku_bu_bl' AS datapoint,
               yc.po_number,
               yc.item_number,
               yc.po_line_number,
               s.po_dtl_id,
               yc.trailer_id            AS bl,
               CASE
                   WHEN pod.division_id IN ('JUSTFAB', 'FABKIDS', 'SHOEDAZZLE') THEN 'JUSTFAB'
                   WHEN pod.division_id IN ('FABLETICS', 'YITTY') THEN 'FABLETICS'
                   WHEN pod.division_id IN ('LINGERIE') THEN 'SAVAGE X'
                   END                     business_unit,
               SUM(yc.total_unit_volume)   cbm,
               SUM(yc.quantity)            units_shipped
        FROM (SELECT DISTINCT po_number, item_number, po_line_number, po_dtl_id FROM _ship) s
                 JOIN gsc.yard_container_data yc
                      ON s.po_dtl_id = yc.po_dtl_id
                 LEFT JOIN gsc.po_skus_data pod
                           ON s.po_dtl_id = pod.po_dtl_id
        WHERE 1 = 1
          AND yc.trailer_id IN
              (SELECT DISTINCT bl FROM gsc.lc_invoice_data WHERE NVL(carrier, 'broomstick') = 'BLECKMANN')
        GROUP BY yc.po_number,
                 yc.item_number,
                 yc.po_line_number,
                 s.po_dtl_id,
                 yc.trailer_id,
                 CASE
                     WHEN pod.division_id IN ('JUSTFAB', 'FABKIDS', 'SHOEDAZZLE') THEN 'JUSTFAB'
                     WHEN pod.division_id IN ('FABLETICS', 'YITTY') THEN 'FABLETICS'
                     WHEN pod.division_id IN ('LINGERIE') THEN 'SAVAGE X'
                     END;


--REPORTING_BASE_PROD.GSC.LC_CBM_USD_BLECKMANN_OTR_BL_BU_DATASET
        INSERT INTO _cbm
            (datapoint, bl, business_unit, invoice_number, cbm, amount_usd)
        SELECT t1.datapoint,
               t1.bl,
               t1.business_unit,
               t2.invoice_number,
               t1.cbm,
               SUM(t2.amount_usd) AS amount_usd
        FROM (SELECT 'bleckmann_otr_bl_bu' AS  datapoint,
                     yc.trailer_id         AS  bl,
                     CASE
                         WHEN pod.division_id IN ('JUSTFAB', 'FABKIDS', 'SHOEDAZZLE') THEN 'JUSTFAB'
                         WHEN pod.division_id IN ('FABLETICS', 'YITTY') THEN 'FABLETICS'
                         WHEN pod.division_id IN ('LINGERIE') THEN 'SAVAGE X'
                         END                   business_unit,
                     SUM(yc.total_unit_volume) cbm
              FROM _trailer t
                       JOIN gsc.yard_container_data yc
                            ON t.trailer_id = yc.trailer_id
                       JOIN gsc.po_skus_data pod
                            ON yc.po_dtl_id = yc.po_dtl_id
              GROUP BY yc.trailer_id,
                       CASE
                           WHEN pod.division_id IN ('JUSTFAB', 'FABKIDS', 'SHOEDAZZLE') THEN 'JUSTFAB'
                           WHEN pod.division_id IN ('FABLETICS', 'YITTY') THEN 'FABLETICS'
                           WHEN pod.division_id IN ('LINGERIE') THEN 'SAVAGE X'
                           END) t1
                 JOIN lc_invoice_data t2
                      ON t1.business_unit = t2.business_unit
                          AND t1.bl = t2.bl
                          AND t2.field_name = 'BLECKMANN_BU'
        GROUP BY t1.datapoint,
                 t1.bl,
                 t1.business_unit,
                 t2.invoice_number,
                 t1.cbm;

--REPORTING_BASE_PROD.GSC.LC_BLECKMANN_PO_SKU_BU_BL_CBM_OTR_DATASET(
        INSERT INTO _cbm
        (datapoint, po_number, item_number, po_line_number, po_dtl_id, business_unit, bl, units_shipped, cbm,
         amount_usd)
        SELECT 'bleckmann_po_sku_bu_bl_cbm_otr'                                         AS datapoint,
               c.po_number,
               c.item_number,
               c.po_line_number,
               c.po_dtl_id,
               c1.business_unit,
               c.bl,
               c.units_shipped,
               c.cbm,
               ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(c1.amount_usd)) / SUM(c.units_shipped) AS amount_usd
        FROM gsc.lc_invoice_data i
                 JOIN gsc.lc_cbm_data c --p
                      ON i.bl = c.bl
                          AND c.datapoint = 'bleckmann_po_sku_bu_bl'
                 JOIN _ship s
                      ON c.po_dtl_id = s.po_dtl_id
                 LEFT JOIN gsc.lc_cbm_data c1
                           ON c.bl = c1.bl
                               AND c.business_unit = c1.business_unit
                               AND c1.datapoint = 'bleckmann_otr_bl_bu'
        WHERE i.field_name = 'OTR_FREIGHT_DATA'
        GROUP BY c.po_number,
                 c.item_number,
                 c.po_line_number,
                 c.po_dtl_id,
                 c1.business_unit,
                 c.bl,
                 c.units_shipped,
                 c.cbm
        HAVING SUM(c1.cbm) > 0;


        MERGE INTO gsc.lc_cbm_data tgt
            USING _cbm src
            ON tgt.datapoint = src.datapoint
                AND NVL(tgt.container, 'unk') = NVL(src.container, 'unk')
                AND NVL(tgt.bl, 'unk') = NVL(src.bl, 'unk')
                AND NVL(tgt.post_advice_number, 'unk') = NVL(src.post_advice_number, 'unk')
                AND NVL(tgt.po_number, 'unk') = NVL(src.po_number, 'unk')
                AND NVL(tgt.item_number, 'unk') = NVL(src.item_number, 'unk')
                AND NVL(tgt.business_unit, 'unk') = NVL(src.business_unit, 'unk')
                AND NVL(tgt.order_number, 'unk') = NVL(src.order_number, 'unk')
                AND NVL(tgt.traffic_mode, 'unk') = NVL(src.traffic_mode, 'unk')
                AND NVL(tgt.invoice_number, 'unk') = NVL(src.invoice_number, 'unk')
--and NVL(tgt.po_line_number, 0) = NVL(src.po_line_number, 0)
                AND NVL(tgt.po_dtl_id, 0) = NVL(src.po_dtl_id, 0)
            WHEN MATCHED THEN UPDATE
                SET tgt.cbm = src.cbm,
                    tgt.units_shipped = src.units_shipped,
                    tgt.amount_usd = src.amount_usd,
                    tgt.po_line_number = src.po_line_number,
                    tgt.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(9)
            WHEN NOT MATCHED THEN INSERT
                (datapoint,
                 container,
                 bl,
                 post_advice_number,
                 po_number,
                 item_number,
                 po_line_number,
                 business_unit,
                 order_number,
                 traffic_mode,
                 invoice_number,
                 amount_usd,
                 cbm,
                 units_shipped,
                 po_dtl_id
                    )
                VALUES (src.datapoint,
                        src.container,
                        src.bl,
                        src.post_advice_number,
                        src.po_number,
                        src.item_number,
                        src.po_line_number,
                        src.business_unit,
                        src.order_number,
                        src.traffic_mode,
                        src.invoice_number,
                        src.amount_usd,
                        src.cbm,
                        src.units_shipped,
                        src.po_dtl_id);

/******correct for multiple BL or PA per container*******/
        UPDATE gsc.lc_cbm_data
        SET con_cbm = c.cbm
        FROM (SELECT DISTINCT c0.container, c0.cbm
              FROM gsc.lc_cbm_data c0
                       JOIN (SELECT DISTINCT container
                             FROM lake_view.excel.shipment_data_oocl
                             WHERE pol_etd >= '2023-01-01') o
                            ON c0.container = o.container
              WHERE datapoint = 'con') c
        WHERE gsc.lc_cbm_data.container = c.container
          AND gsc.lc_cbm_data.datapoint IN ('pa_con', 'con_bl');

/***********clean null cbm/units shipped records*************/
        DELETE
        FROM gsc.lc_cbm_data
        WHERE cbm + units_shipped <= 0;


    END;
