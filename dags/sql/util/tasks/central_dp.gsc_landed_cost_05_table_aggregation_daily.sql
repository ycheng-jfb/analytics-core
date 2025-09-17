CREATE OR REPLACE TASK util.tasks_central_dp.gsc_landed_cost_05_table_aggregation_daily
    WAREHOUSE =da_wh_analytics
    AFTER util.tasks_central_dp.gsc_landed_cost_04_cost_aggregation_daily
    AS BEGIN

        USE SCHEMA reporting_base_prod.gsc;


        CREATE OR REPLACE TEMPORARY TABLE _po
        (
            po_number      VARCHAR(100),
            item_number    VARCHAR(100),
            po_dtl_id      NUMBER(38, 0),
            po_line_number NUMBER(38, 0)
        );


        INSERT INTO _po
            (po_number, item_number, po_dtl_id, po_line_number)
        /*
        select distinct po_number, sku, po_dtl_id, bc_po_line_number from gsc.po_skus_data
        where received_date_80_percent >=  '2024-01-01'
        and show_room >= '2024-01'
        ;
        */

--/*
        SELECT DISTINCT po_number, sku AS item_number, po_dtl_id, bc_po_line_number
        FROM (SELECT psd.po_number, psd.sku, po_dtl_id, bc_po_line_number
              FROM gsc.po_skus_data psd
                       LEFT JOIN "LAKE_VIEW"."GSC"."CMT_LABOR_COST" cmt
                                 ON psd.po_number = cmt.po_number
                                     AND psd.sku = cmt.item_number
              WHERE (psd.received_date_80_percent >= DATEADD('day',
                                                             CASE
                                                                 WHEN DATE_PART(DAY, CURRENT_TIMESTAMP()) = 4
                                                                     THEN -120
                                                                 ELSE -30
                                                                 END
                  , CURRENT_TIMESTAMP()))
                 OR TO_DATE(cmt.meta_create_datetime) >= TO_DATE(DATEADD(DAY, -30, CURRENT_TIMESTAMP()))

              UNION

              SELECT po_number, sku AS item_number, po_dtl_id, po_line_number
              FROM reporting_prod.gsc.landed_cost_dataset
              WHERE fully_landed = 'N'
                AND history = 'N'
                AND TO_DATE(meta_create_datetime) >= TO_DATE(DATEADD(DAY, -120, CURRENT_TIMESTAMP()))

              UNION

              SELECT psd.po_number, psd.sku, psd.po_dtl_id, psd.bc_po_line_number
              FROM reference.landed_cost_manual_refresh lcmr
                       JOIN gsc.po_skus_data psd
                            ON lcmr.po_dtl_id = psd.po_dtl_id
              WHERE lcmr.to_be_processed = TRUE);
--*/

        CREATE OR REPLACE TEMPORARY TABLE _rcv AS
        SELECT p.*, rd.received_date, rd.received_qty + rd.correction_qty AS received_qty
        FROM _po p
                 JOIN gsc.receipt_data rd
                      ON p.po_dtl_id = rd.po_dtl_id;

        CREATE OR REPLACE TEMPORARY TABLE _default AS
        SELECT psd.po_number,
               psd.sku,
               NVL(psd.bc_po_line_number, psd.po_dtl_id)                                                                                              AS po_line_number,
               po_dtl_id,
               ROW_NUMBER() OVER (PARTITION BY psd.po_number, psd.sku ORDER BY psd.po_number, psd.sku, NVL(psd.bc_po_line_number, psd.po_dtl_id) ASC) AS sorted
        FROM gsc.po_skus_data psd
        WHERE 1 = 1
          AND qty > 0
          AND NVL(is_cancelled, FALSE) = FALSE
          AND NVL(received_date_80_percent, '2000-01-01') >= '2000-01-01';

        DELETE FROM _default WHERE sorted > 1;

/*****************
screen printing
duty & freight
******************/
        CREATE OR REPLACE TEMPORARY TABLE _sp AS
        SELECT *,
               SUM(total_freight_cost) OVER (PARTITION BY po_number) AS po_freight,
               SUM(total_duty_cost) OVER (PARTITION BY po_number)    AS po_duty,
               SUM(qty) OVER (PARTITION BY po_number)                AS po_qty
        FROM (SELECT sp.po_number
                   , sp.sku
                   , d.po_dtl_id
                   , SUM(sp.total_freight_cost) AS total_freight_cost
                   , SUM(sp.total_duty_cost)    AS total_duty_cost
                   , SUM(sp.qty)                AS qty
              FROM lake_view.excel.carrier_invoice_screen_printing sp
                       LEFT JOIN _default d
                                 ON sp.po_number = d.po_number
                                     AND sp.sku = d.sku
              --where sp.po_number = '1039968'
--and upper(trim(sp.sku)) = 'SS2355259-12367-36200'
              GROUP BY sp.po_number, sp.sku, d.po_dtl_id);

/************capture errors*********************/
        INSERT INTO gsc.aggregation_errors
            (error, process_or_table, field_list, field_values)
        SELECT DISTINCT 'PO/SKU IS NOT ACTIVE'          AS error,
                        'Screen Printing Invoice'       AS table_or_process,
                        'po_dtl_id/po/sku'              AS file_list,
                        '0/' || po_number || '/' || sku AS field_values
        FROM _sp
        WHERE po_dtl_id IS NULL;

        DELETE FROM _sp WHERE po_dtl_id IS NULL;


        CREATE OR REPLACE TEMPORARY TABLE _screen AS
        SELECT DISTINCT sp.*
                      , lcd.units_received
                      , (sp.po_freight / sp.po_qty) AS screen_print_freight
                      , (sp.po_duty / sp.po_qty)    AS screen_print_duty
        FROM _sp sp
                 JOIN reporting_prod.gsc.landed_cost_dataset lcd
                      ON sp.po_dtl_id = lcd.po_dtl_id
        --where sp.po_number = '1039968'
--and sp.sku = 'SS2355259-12354-35800'
        ORDER BY sku;

        CREATE OR REPLACE TEMPORARY TABLE _cost AS
        SELECT DATE_TRUNC(MONTH, po.received_date_80_percent)                                           AS year_month_received,
--RCV.YEAR_MONTH_RECEIVED,
               po.po_number,
               po.sku                                                                                   AS item_number,
               po.po_dtl_id,
               po.po_line_number,
               CASE
                   WHEN po.brand IN ('JUSTFAB', 'SHOEDAZZLE')
                       THEN 'FAST FASHION'
                   WHEN po.brand = 'LINGERIE'
                       THEN 'SAVAGE X'
                   ELSE po.brand
                   END                                                                                  AS budget_bu,

               CASE
                   WHEN po.brand = 'FABLETICS' AND po.po_type = 'RETAIL'
                       THEN 'FL RETAIL'
                   WHEN po.brand = 'FABLETICS' AND po.po_type != 'RETAIL'
                       THEN 'FL' ||
                            CASE
                                WHEN po.region_id = 'EU' AND po.po_number LIKE '%U%'
                                    THEN 'UK'
                                ELSE po.region_id
                                END
                   WHEN po.brand = 'LINGERIE'
                       THEN 'SAVAGE X'
                   ELSE po.brand
                   END                                                                                  AS budget_business_unit,
               CASE
                   WHEN po.brand = 'FABKIDS'
                       THEN 'BOY/GIRL'
                   WHEN po.brand = 'FABLETICS'
                       THEN ''
                   ELSE po.gender
                   END                                                                                  AS budget_gender,
               CASE
                   WHEN UPPER(po.department) LIKE '%ACCESSORIE%'
                       THEN 'ACCESSORIES'
                   WHEN UPPER(po.department) LIKE '%APPAREL%'
                       THEN 'APPAREL'
                   WHEN UPPER(po.department) LIKE '%FOOTWEAR%'
                       THEN 'FOOTWEAR'
                   END                                                                                  AS budget_category,
               CASE
                   WHEN po.warehouse_id IN (107, 154, 421)
                       THEN 'KY'
                   WHEN po.warehouse_id = 231
                       THEN 'PERRIS'
                   WHEN po.warehouse_id = 109
                       THEN 'CA'
                   WHEN po.warehouse_id = 221
                       THEN 'EU'
                   WHEN po.warehouse_id = 366
                       THEN 'UK'
                   WHEN po.warehouse_id IN (465, 466)
                       THEN 'MX'
                   END                                                                                  AS budget_fc,
               CASE
                   WHEN UPPER(po.freight_method) IN ('AIR (STANDARD)', 'AIR (EXPEDITED)')
                       THEN 'AIR'
                   WHEN LEFT(UPPER(po.freight_method), 3) = 'DDP'
                       THEN 'DDP'
                   ELSE UPPER(po.freight_method)
                   END                                                                                  AS budget_shipping_mode,

               ROUND(IFNULL(co.ocean_other_cost, 0) + IFNULL(ca.air_other_cost, 0), 4)                  AS cpu_freight,
               ROUND(co.ocean_freight_cost, 4)                                                          AS cpu_ocean,
               ROUND(ca.air_freight_cost, 4)                                                            AS cpu_air,
               ROUND(tr.transload_cost, 4)                                                              AS cpu_transload,
               ROUND(NVL(otr.otr_cost, 0) + NVL(sc.screen_print_freight, 0) + NVL(ca.otr_cost, 0), 4)   AS cpu_otr,
--ROUND(NVL(OTR.OTR_COST, 0), 4) AS CPU_OTR,
               ROUND(dm.domestic_cost, 4)                                                               AS cpu_domestic,
               ROUND(pp.pier_pass_cost, 4)                                                              AS cpu_pierpass,
               rcv.received_qty                                                                         AS units_received,


               (ROUND(ZEROIFNULL(co.ocean_other_cost) + ZEROIFNULL(ca.air_other_cost), 2) +
                ROUND(ZEROIFNULL(co.ocean_freight_cost), 2) + ROUND(ZEROIFNULL(ca.air_freight_cost), 2) +
                ROUND(ZEROIFNULL(tr.transload_cost), 2) +
                ROUND(ZEROIFNULL(NVL(otr.otr_cost, 0) + NVL(sc.screen_print_freight, 0)), 2) +
                ROUND(ZEROIFNULL(dm.domestic_cost), 2) + ROUND(ZEROIFNULL(pp.pier_pass_cost), 2) +
                ROUND(ZEROIFNULL(NVL(ca.otr_cost, 0)), 2)) * rcv.received_qty
                                                                                                        AS total_actual_primary_cost,
/*
(ZEROIFNULL(CO.OCEAN_OTHER_COST) + ZEROIFNULL(CA.AIR_OTHER_COST)) + ZEROIFNULL(CO.OCEAN_FREIGHT_COST) + ZEROIFNULL(CA.AIR_FREIGHT_COST) + ZEROIFNULL(TR.TRANSLOAD_COST) + ZEROIFNULL(OTR.OTR_COST) + ZEROIFNULL(DM.DOMESTIC_COST) + ZEROIFNULL(PP.PIER_PASS_COST) * RCV.UNITS_RECEIVED
AS TOTAL_ACTUAL_PRIMARY_COST,
*/
               ROUND(ZEROIFNULL(co.ocean_other_cost) + ZEROIFNULL(ca.air_other_cost), 2) +
               ROUND(ZEROIFNULL(co.ocean_freight_cost), 2) + ROUND(ZEROIFNULL(ca.air_freight_cost), 2) +
               ROUND(ZEROIFNULL(tr.transload_cost), 2) +
               ROUND(ZEROIFNULL(NVL(otr.otr_cost, 0) + NVL(sc.screen_print_freight, 0)), 2) +
               ROUND(ZEROIFNULL(dm.domestic_cost), 2) + ROUND(ZEROIFNULL(pp.pier_pass_cost), 2) +
               ROUND(ZEROIFNULL(NVL(ca.otr_cost, 0)), 2)                                                AS actual_primary_cost_per_unit,

/*
round(ZEROIFNULL(CO.OCEAN_OTHER_COST) + ZEROIFNULL(CA.AIR_OTHER_COST) + ZEROIFNULL(CO.OCEAN_FREIGHT_COST) + ZEROIFNULL(CA.AIR_FREIGHT_COST) + ZEROIFNULL(TR.TRANSLOAD_COST) + ZEROIFNULL(OTR.OTR_COST) + ZEROIFNULL(DM.DOMESTIC_COST) + ZEROIFNULL(PP.PIER_PASS_COST), 2) AS ACTUAL_PRIMARY_COST_PER_UNIT,
*/
               IFF(po.region_id NOT IN ('MX', 'CA') AND po.warehouse_id NOT IN (366, 465, 466) AND
                   po.inco_term != 'DDP', po.duty_percentage,
                   0)                                                                                   AS duty_percentage,
               IFF(po.region_id NOT IN ('MX', 'CA') AND po.warehouse_id NOT IN (366, 465, 466) AND
                   po.inco_term != 'DDP', po.duty, 0)                                                   AS duty,
               IFF(po.region_id NOT IN ('MX', 'CA') AND po.warehouse_id NOT IN (366, 465, 466) AND
                   po.inco_term != 'DDP', po.primary_tariff, 0)                                         AS tariff_rate,
               IFF(po.region_id NOT IN ('MX', 'CA') AND po.warehouse_id NOT IN (366, 465, 466) AND
                   po.inco_term != 'DDP', (po.primary_tariff * po.cost) / po.qty,
                   0)                                                                                   AS tariff_cost_per_unit,
               po.vend_name,
               po.inco_term,
               po.qty,
               NULL                                                                                     AS first_sale_cost,
               0                                                                                        AS other_duty_cost_per_unit
                ,
               NVL(sc.screen_print_freight, 0)                                                          AS screen_print_freight
                ,
               + ROUND(ZEROIFNULL(NVL(otr.otr_cost, 0)), 2) + ROUND(ZEROIFNULL(NVL(ca.otr_cost, 0)), 2) AS otr_cost
                ,
               ROUND(NVL(ca.air_other_cost, 0), 4)                                                      AS air_other
                ,
               ROUND(IFNULL(co.ocean_other_cost, 0), 4)                                                 AS ocean_other


        FROM _po p1

/*join REPORTING_BASE_PROD.GSC.LC_RECEIPT_DATASET RCV
    on p1.po_number = rcv.po_number
    and p1.item_number = rcv.item_number*/

                 JOIN _rcv rcv
                      ON p1.po_dtl_id = rcv.po_dtl_id
                 LEFT JOIN gsc.po_skus_data po
                           ON rcv.po_dtl_id = po.po_dtl_id
                 LEFT JOIN gsc.lc_cost_data co
--LEFT JOIN REPORTING_BASE_PROD.GSC.LC_COST_OCEAN_DATASET CO
                           ON rcv.po_dtl_id = co.po_dtl_id
                               AND co.proc = 'LC_COST_OCEAN_DATASET'
                 LEFT JOIN gsc.lc_cost_data ca
--LEFT JOIN REPORTING_BASE_PROD.GSC.LC_COST_AIR_DATASET CA
                           ON rcv.po_dtl_id = ca.po_dtl_id
                               AND ca.proc = 'LC_COST_AIR_DATASET'
                 LEFT JOIN gsc.lc_cost_data dm
--LEFT JOIN REPORTING_BASE_PROD.GSC.LC_COST_DOMESTIC_DATASET DM
                           ON rcv.po_dtl_id = dm.po_dtl_id
                               AND dm.proc = 'LC_COST_DOMESTIC_DATASET'
                 LEFT JOIN gsc.lc_cost_data tr
--LEFT JOIN REPORTING_BASE_PROD.GSC.LC_COST_TRANSLOAD_DATASET TR
                           ON rcv.po_dtl_id = tr.po_dtl_id
                               AND tr.proc = 'LC_COST_TRANSLOAD_DATASET'
                 LEFT JOIN gsc.lc_cost_data otr
--LEFT JOIN REPORTING_BASE_PROD.GSC.LC_COST_OTR_DATASET OTR
                           ON rcv.po_dtl_id = otr.po_dtl_id
                               AND otr.proc = 'LC_COST_OTR_DATASET'
                 LEFT JOIN gsc.lc_cost_data pp
--LEFT JOIN REPORTING_BASE_PROD.GSC.LC_COST_PIER_PASS_DATASET PP
                           ON rcv.po_dtl_id = pp.po_dtl_id
                               AND pp.proc = 'LC_COST_PIER_PASS_DATASET'
--where rcv.year_month_received >= $meta_update_datetime

                 LEFT JOIN _screen sc
                           ON p1.po_dtl_id = sc.po_dtl_id;


/*************************
fetch duty tariff

G-Global Ocean duty
***************************/
        CREATE OR REPLACE TEMPORARY TABLE _duty_tariff_02 AS
        SELECT c.year_month_received,
               c.po_number,
               c.item_number,
               c.po_dtl_id,
               ROUND(SUM(c1.units_shipped), 0) AS d_units_shipped,
               ROUND(AVG(c1.units_shipped), 0) AS units_shipped,
               AVG(i.amount_usd)               AS amount_usd,
               ROUND(AVG(c2.units_shipped), 0) AS c2_units_shipped
        FROM _cost c
                 JOIN gsc.lc_cbm_data c1
                      ON c.po_dtl_id = c1.po_dtl_id
                          AND c1.datapoint = 'po_sku_pa_con'
                 LEFT JOIN gsc.lc_cbm_data c2
                           ON c1.container = c2.container
                               AND c1.post_advice_number = c2.post_advice_number
                               AND c2.datapoint = 'pa_con'
                 JOIN gsc.lc_invoice_data i
                      ON c1.container = i.container
                          AND c1.post_advice_number = i.post_advice
                          AND i.field_name = 'DUTY_DATA'
        GROUP BY c.year_month_received,
                 c.po_number,
                 c.item_number,
                 c.po_dtl_id

        UNION

/******G-Global Air Duty***************/
        SELECT c.year_month_received,
               c.po_number,
               c.item_number,
               c.po_dtl_id,
               ROUND(SUM(c1.units_shipped), 0) AS d_units_shipped,
               ROUND(AVG(c1.units_shipped), 0) AS units_shipped,
               AVG(i.amount_usd)               AS amount_usd,
               ROUND(AVG(c2.units_shipped), 0) AS c2_units_shipped
        FROM _cost c
                 JOIN gsc.lc_cbm_data c1
                      ON c.po_dtl_id = c1.po_dtl_id
                          AND c1.datapoint = 'po_sku_pa_bl'
                 LEFT JOIN gsc.lc_cbm_data c2
                           ON c1.bl = c2.bl
                               AND c1.post_advice_number = c2.post_advice_number
                               AND c2.datapoint = 'pa_bl'
                 JOIN gsc.lc_invoice_data i
                      ON c1.bl = i.bl
                          AND c1.post_advice_number = i.post_advice
                          AND i.field_name = 'DUTY_DATA'
        GROUP BY c.year_month_received,
                 c.po_number,
                 c.item_number,
                 c.po_dtl_id;

/************Carmichael Duty and Tariff**************/
        CREATE OR REPLACE TEMPORARY TABLE _duty_tariff_1 AS
        SELECT DISTINCT po.year_month_received,
                        po.po_number,
                        po.item_number,
                        po.po_dtl_id,
                        CASE
                            WHEN SUM(NVL(COALESCE(bd.duty_fee, bd.estimated_duty), 0)) > 0
                                THEN SUM(bd.units)
                            WHEN SUM(NVL(gg.amount_usd, 0)) > 0
                                THEN SUM(gg.units_shipped)
                            ELSE 0
                            END                                                 AS units,
/*CASE WHEN CUSTOMER_BROKER = 'OOCL' THEN NVL(BD.ADVALOREM_RATE,0) / 100 ELSE NVL(BD.ADVALOREM_RATE,0) END*/
                        CASE
                            WHEN SUM(NVL(bd.value, 0)) = 0 THEN 0
                            ELSE SUM(NVL(COALESCE(bd.duty_fee, bd.estimated_duty), 0)) /
                                 SUM(bd.value) END                              AS actual_duty_rate,
                        CASE
                            WHEN SUM(NVL(COALESCE(bd.duty_fee, bd.estimated_duty), 0)) > 0 AND SUM(bd.units) > 0
                                THEN SUM(NVL(COALESCE(bd.duty_fee, bd.estimated_duty), 0)) / SUM(bd.units)
                            WHEN SUM(NVL(gg.amount_usd, 0)) > 0 AND MAX(gg.c2_units_shipped) > 0
                                THEN SUM(NVL(gg.amount_usd, 0)) / MAX(gg.c2_units_shipped)
                            ELSE 0
                            END                                                 AS duty_cost_per_unit,

                        CASE
                            WHEN SUM(NVL(bd.value, 0)) = 0 THEN 0
                            ELSE SUM(NVL(bd.tariff_fee, 0)) / SUM(bd.value) END AS tariff_rate,
                        CASE
                            WHEN SUM(NVL(bd.units, 0)) = 0 THEN 0
                            ELSE SUM(NVL(bd.tariff_fee, 0)) / SUM(bd.units) END AS tariff_cost_per_unit,
                        CASE
                            WHEN po.vend_name LIKE '%INTAI %' OR po.vend_name LIKE '%DELTA BOGART%' THEN
                                (CASE
                                     WHEN (SUM(NVL(bd.units, 0)) = 0 OR SUM(NVL(bd.advalorem_rate, 0)) = 0) THEN 0
                                     ELSE (SUM(NVL(COALESCE(bd.duty_fee, bd.estimated_duty), 0)) /
                                           SUM(NVL(bd.advalorem_rate, 0)) / SUM(bd.units)) END)
                            ELSE NULL END                                       AS first_sale_cost,
                        CASE
                            WHEN SUM(NVL(bd.units, 0)) = 0 THEN 0
                            ELSE (SUM(NVL(bd.hmf_fee, 0)) + SUM(NVL(bd.mpf_fee, 0)) + SUM(NVL(bd.cotton_fee, 0)) +
                                  SUM(NVL(bd.other_fees, 0)) + SUM(NVL(bd.broker_fee, 0))) /
                                 SUM(bd.units) END                              AS other_duty_cost_per_unit,

        FROM _cost po --"LAKE_VIEW"."GSC"."BROKER_DUTY_TARIFF" BD
                 LEFT JOIN lake_view.gsc.broker_duty_tariff bd
                           ON UPPER(TRIM(bd.po_number)) = po.po_number
                               AND UPPER(TRIM(bd.sku_number)) = po.item_number
                               AND (
                                  NVL(bd.po_line_number, 0) = NVL(po.po_line_number, 1)
                                      OR NVL(bd.po_line_number, 0) = NVL(po.po_dtl_id, 1)
                                      OR NVL(bd.po_line_number, 0) = 0
                                  )
                 LEFT JOIN (SELECT entry_number, SUM(units) AS entry_number_units
                            FROM lake_view.gsc.broker_duty_tariff
                            GROUP BY entry_number) ship_total
                           ON bd.entry_number = ship_total.entry_number
                 LEFT JOIN _duty_tariff_02 gg --g-global data
                           ON po.po_dtl_id = gg.po_dtl_id
        WHERE po.inco_term != 'DDP'
--and po.po_dtl_id = 1043598804
        GROUP BY po.year_month_received,
                 po.po_number,
                 po.item_number,
                 po.po_dtl_id,
                 po.vend_name
        HAVING CASE
                   WHEN SUM(NVL(COALESCE(bd.duty_fee, bd.estimated_duty), 0)) > 0
                       THEN SUM(bd.units)
                   WHEN SUM(NVL(gg.amount_usd, 0)) > 0
                       THEN SUM(gg.units_shipped)
                   ELSE 0
                   END > 0;

        CREATE OR REPLACE TEMPORARY TABLE _duty_tariff AS
        SELECT year_month_received,
               po_number,
               item_number,
               po_dtl_id,
               units,
               SUM(actual_duty_rate)         AS actual_duty_rate,
               CASE
                   WHEN SUM(units) > 0
                       THEN SUM(duty_cost_per_unit * units) / SUM(units)
                   ELSE 0
                   END                       AS duty_cost_per_unit,
               SUM(tariff_rate)              AS tariff_rate,
               SUM(tariff_cost_per_unit)     AS tariff_cost_per_unit,
               SUM(first_sale_cost)          AS first_sale_cost,
               SUM(other_duty_cost_per_unit) AS other_duty_cost_per_unit
        FROM (SELECT year_month_received,
                     po_number,
                     item_number,
                     po_dtl_id,
                     CAST(units AS NUMBER(38, 12))              AS units,
                     actual_duty_rate,
                     CAST(duty_cost_per_unit AS NUMBER(38, 12)) AS duty_cost_per_unit,
                     tariff_rate,
                     tariff_cost_per_unit,
                     first_sale_cost,
                     other_duty_cost_per_unit
              FROM _duty_tariff_1)
        GROUP BY year_month_received,
                 po_number,
                 item_number,
                 po_dtl_id,
                 units;


/******************
Merlin duty cost
********************/
        CREATE OR REPLACE TEMPORARY TABLE _md_duty AS
        SELECT DISTINCT po.po_number,
                        po.sku                                 AS item_number,
                        po.po_dtl_id,
                        po.qty,
                        po.duty_percentage,
                        po.duty,
                        po.primary_tariff                      AS tariff_rate,
                        (po.primary_tariff * po.cost) / po.qty AS tariff_cost_per_unit,
                        NULL                                   AS first_sale_cost,
                        0                                      AS other_duty_cost_per_unit
        FROM _po p
                 JOIN gsc.po_skus_data po
                      ON p.po_dtl_id = po.po_dtl_id
                 LEFT JOIN _duty_tariff dt -- REPORTING_BASE_PROD.GSC.LC_DUTY_TARIFF_DATASET DT
                           ON po.po_dtl_id = dt.po_dtl_id
        WHERE (ZEROIFNULL(po.duty_percentage) > 0 OR ZEROIFNULL(po.duty) > 0 OR ZEROIFNULL(po.primary_tariff) > 0)
          AND dt.po_number IS NULL
          AND po.po_status_id != 10
          AND po.qty > 0
          AND po.region_id NOT IN ('MX', 'CA')
          AND po.warehouse_id NOT IN (366, 465, 466)
          AND po.inco_term != 'DDP';

/***************************
create staging table
****************************/

        CREATE OR REPLACE TEMPORARY TABLE _stg_landed_cost AS
        SELECT DISTINCT c.po_dtl_id,
                        c.po_line_number,
                        DATE_TRUNC(MONTH, rcv.received_date)                                        AS year_month_received,
                        UPPER(po.inco_term)                                                         AS inco_term,
                        CASE
                            WHEN UPPER(po.division_id) = 'LINGERIE' THEN 'SAVAGE X'
                            ELSE UPPER(NVL(po.brand, po.division_id)) END                           AS business_unit,
                        UPPER(po.gender)                                                            AS gender,
                        UPPER(po.department)                                                        AS category,
                        po.class,
                        UPPER(po.po_number)                                                         AS po_number,
                        po.warehouse_id_origin                                                      AS receiving_warehouse_id,
                        po.warehouse_id                                                             AS destination_warehouse_id,
                        CAST(po.show_room || '-01' AS DATE)                                         AS show_room,
                        po.sku,
                        UPPER(po.plm_style)                                                         AS plm_style,
                        UPPER(bc.budget_class)                                                      AS budget_class,
                        UPPER(po.subclass)                                                          AS style_type,
                        UPPER(po.style_type)                                                        AS style_type_new,
                        UPPER(po.factory_city)                                                      AS origin_city,
                        UPPER(po.country_origin)                                                    AS origin_country,
                        CASE
                            WHEN po.region_id = 'EU' AND po.po_number LIKE '%U%' THEN 'UK'
                            ELSE po.region_id END                                                   AS destination_country,
                        UPPER(po.vend_name)                                                         AS vend_name,
                        UPPER(po.freight_method)                                                    AS po_mode,
                        UPPER(COALESCE(po.freight_method, osr.traffic_mode, asn.traffic_mode))      AS shipping_mode,
                        po.qty                                                                      AS units_ordered,
                        COALESCE(osr.units_shipped, asn.units_shipped)                              AS units_shipped,
                        c.units_received,
                        c.total_actual_primary_cost,
                        c.actual_primary_cost_per_unit,
                        b.primary_freight                                                           AS budgeted_primary_cost,
                        ROUND(COALESCE(dt.actual_duty_rate, mdd.duty_percentage), 2)                AS actual_duty_rate,
                        b.duty_rate                                                                 AS budget_duty_rate,
                        ROUND(COALESCE(dt.tariff_rate, mdd.tariff_rate), 2)                         AS actual_tariff_rate,
                        b.tariff_rate                                                               AS budget_tariff_rate,
                        COALESCE(dt.tariff_cost_per_unit, mdd.tariff_cost_per_unit)                 AS actual_tariff_cost_per_unit,
                        ROUND(rcv.received_qty * COALESCE(dt.tariff_cost_per_unit, mdd.tariff_cost_per_unit),
                              2)                                                                    AS total_actual_tariff_cost,
                        ROUND(b.tariff_rate * b.first_cost_rate)                                    AS budget_tariff_cost_per_unit,
                        b.total_budget_tariff_cost                                                  AS total_budget_tariff_cost,
                        b.first_cost_rate                                                           AS budget_unit_cost,
                        po.cost + ROUND(IFNULL(po.inspection, 0), 2) + ROUND(IFNULL(po.commission, 0), 2) +
                        ROUND(IFNULL(cmt.cmt_cost, 0), 2)                                           AS actual_unit_cost,
                        (b.primary_freight + b.first_cost_rate + (b.duty_rate * b.first_cost_rate) +
                         (b.tariff_rate * b.first_cost_rate))                                       AS budget_auc_per_unit,
                        CASE
                            WHEN COALESCE(IFNULL(po.inspection, 0), IFNULL(po.commission, 0)) > 0 THEN po.cost
                            ELSE NULL END                                                           AS agency_cost_no_ci,
                        po.cost                                                                     AS po_cost_without_commission,
                        (ROUND(IFNULL(po.inspection, 0), 2) + ROUND(IFNULL(po.commission, 0), 2)) *
                        COALESCE(dt.actual_duty_rate, mdd.duty_percentage)                          AS agency_duty_savings_per_unit,
                        po.cost + ROUND(IFNULL(po.inspection, 0), 2) +
                        ROUND(IFNULL(po.commission, 0), 2)                                          AS agency_cost_and_po_cost,
                        (po.cost + ROUND(IFNULL(po.inspection, 0), 2) + ROUND(IFNULL(po.commission, 0), 2)) *
                        rcv.received_qty                                                            AS total_agency_cost_and_po_cost,
                        ZEROIFNULL(ROUND(COALESCE(dt.first_sale_cost, mdd.first_sale_cost), 2))     AS first_sale_program_cost,--***
                        COALESCE(ROUND(COALESCE(dt.first_sale_cost, mdd.first_sale_cost), 2),
                                 po.cost)                                                           AS first_cost_and_po_cost,---***

                        COALESCE(ROUND(COALESCE(dt.first_sale_cost, mdd.first_sale_cost), 2), po.cost) *
                        rcv.received_qty                                                            AS total_first_cost_and_po_cost,---*

                        ROUND(NVL(COALESCE(dt.duty_cost_per_unit, mdd.duty), 0), 6) +
                        NVL(screen_print_duty, 0)                                                   AS actual_duty_cost_per_unit,

                        (ROUND(COALESCE(dt.duty_cost_per_unit, mdd.duty, 0), 6) + NVL(screen_print_duty, 0)) *
                        COALESCE(dt.units, mdd.qty, rcv.received_qty, 0)                            AS total_duty_cost,

                        b.primary_freight * rcv.received_qty                                        AS total_budgeted_primary_cost,
                        b.first_cost_rate * rcv.received_qty                                        AS total_budgeted_product_cost,
                        (ZEROIFNULL(po.cost) + ZEROIFNULL(po.inspection) + ZEROIFNULL(po.commission) +
                         ZEROIFNULL(cmt.cmt_cost)) *
                        rcv.received_qty                                                            AS total_actual_product_cost,

--round((COALESCE(DT.DUTY_COST_PER_UNIT , mdd.DUTY) * RCV.UNITS_RECEIVED),4) AS TOTAL_ACTUAL_DUTY,
                        (COALESCE(dt.duty_cost_per_unit, mdd.duty, 0) + NVL(screen_print_duty, 0)) *
                        rcv.received_qty                                                            AS total_actual_duty,

                        ZEROIFNULL(COALESCE(dt.duty_cost_per_unit, mdd.duty)) + NVL(screen_print_duty, 0) +
                        actual_primary_cost_per_unit                                                AS total_primary_and_duty_cost_per_unit,

                        (ZEROIFNULL(COALESCE(dt.duty_cost_per_unit, mdd.duty)) + NVL(screen_print_duty, 0) +
                         c.actual_primary_cost_per_unit) *
                        rcv.received_qty                                                            AS total_actual_primary_and_duty,

                        ZEROIFNULL(b.duty_rate) * ZEROIFNULL(b.first_cost_rate) *
                        rcv.received_qty                                                            AS total_budgeted_duty,
                        (ZEROIFNULL(b.duty_rate) * ZEROIFNULL(b.first_cost_rate) * rcv.received_qty) +
                        (ZEROIFNULL(b.primary_freight) * rcv.received_qty)                          AS total_budgeted_primary_and_duty,


                        (ZEROIFNULL(po.cost) + ROUND(ZEROIFNULL(po.inspection), 2) +
                         ROUND(ZEROIFNULL(po.commission), 2) + ZEROIFNULL(ROUND(cmt.cmt_cost, 2)) +
                         ZEROIFNULL(ROUND((COALESCE(dt.duty_cost_per_unit, mdd.duty) + NVL(screen_print_duty, 0)), 2)) +
                         ZEROIFNULL(ROUND(COALESCE(dt.tariff_cost_per_unit, mdd.tariff_cost_per_unit), 2)) +
                         c.actual_primary_cost_per_unit) *
                        rcv.received_qty                                                            AS total_actual_landed_cost,

                        ROUND(
                                CAST(
                                        (ZEROIFNULL(po.cost) +
                                         ROUND(ZEROIFNULL(po.inspection), 2) +
                                         ROUND(ZEROIFNULL(po.commission), 2) +
                                         ROUND(ZEROIFNULL(cmt.cmt_cost), 2) +
                                         ROUND(
                                                 ZEROIFNULL(COALESCE(dt.duty_cost_per_unit, mdd.duty) +
                                                            NVL(screen_print_duty, 0)),
                                                 2) +
                                         ROUND(ZEROIFNULL(COALESCE(dt.tariff_cost_per_unit, mdd.tariff_cost_per_unit)),
                                               2) +
                                         c.actual_primary_cost_per_unit
                                            ) AS NUMBER(38, 4)),
                                2)                                                                  AS actual_landed_cost_per_unit,

                        (b.primary_freight + b.first_cost_rate + (b.duty_rate * b.first_cost_rate) +
                         (b.tariff_rate * b.first_cost_rate)) *
                        rcv.received_qty                                                            AS total_blended_budget,
                        ((ROUND(ZEROIFNULL(po.inspection), 2) + ROUND(ZEROIFNULL(po.commission), 2)) *
                         ZEROIFNULL(COALESCE(dt.actual_duty_rate, mdd.duty_percentage))) *
                        rcv.received_qty                                                            AS total_agency_program_savings,
                        CASE
                            WHEN ZEROIFNULL(dt.first_sale_cost) = 0 THEN 0
                            ELSE (((ZEROIFNULL(po.cost) + ZEROIFNULL(po.inspection) + ZEROIFNULL(po.commission) +
                                    ZEROIFNULL(cmt.cmt_cost)) * rcv.received_qty) -
                                  (ZEROIFNULL(COALESCE(dt.first_sale_cost, mdd.first_sale_cost)) * rcv.received_qty)) *
                                 ZEROIFNULL(COALESCE(dt.actual_duty_rate, mdd.duty_percentage)) END AS total_first_sale_program_savings,
                        CASE
                            WHEN po.warehouse_id IN (107, 154, 421) THEN 'KY'
                            WHEN po.warehouse_id = 231 THEN 'PERRIS'
                            WHEN po.warehouse_id = 109 THEN 'CA'
                            WHEN po.warehouse_id = 221 THEN 'EU'
                            WHEN po.warehouse_id = 366 THEN 'UK'
                            WHEN po.warehouse_id IN (465, 466) THEN 'MX'
                            END                                                                     AS fc,
                        ZEROIFNULL(ROUND(cmt.cmt_cost, 4))                                          AS cmt_cost,
                        ZEROIFNULL(c.cpu_freight)                                                   AS cpu_freight,
                        ZEROIFNULL(c.cpu_ocean)                                                     AS cpu_ocean,
                        ZEROIFNULL(c.cpu_air)                                                       AS cpu_air,
                        ZEROIFNULL(c.cpu_transload)                                                 AS cpu_transload,
                        ZEROIFNULL(c.cpu_otr)                                                       AS cpu_otr,
                        ZEROIFNULL(c.cpu_domestic)                                                  AS cpu_domestic,
                        ZEROIFNULL(c.cpu_pierpass)                                                  AS cpu_pierpass,
                        'N'                                                                         AS history,
                        e.fully_landed,
                        po.brand,
                        po.centric_department,
                        po.centric_subdepartment,
                        po.centric_category,
                        po.centric_class,
                        po.centric_subclass,
                        po.centric_style_id
                ,
                        po.freight_method
                ,
                        osr.traffic_mode                                                            AS osr_traffic_mode
                ,
                        osr.po_number                                                               AS osr_po
                ,
                        asn.traffic_mode                                                            AS asn_traffic_mode
                ,
                        asn.po_number                                                               AS asn_po
                ,
                        sc.screen_print_duty
                ,
                        sc.screen_print_freight
                ,
                        ROUND(COALESCE(dt.duty_cost_per_unit, mdd.duty, 0), 6)                      AS duty_cost_per_unit,
                        e.fully_calculated_cost,
                        e.review_required,

                        ZEROIFNULL(po.cost)                                                         AS lc_cost,
                        ROUND(ZEROIFNULL(po.inspection), 2)                                         AS lc_inspection,
                        ROUND(ZEROIFNULL(po.commission), 2)                                         AS lc_commission,
                        ROUND(ZEROIFNULL(cmt.cmt_cost), 2)                                          AS lc_cmt,
                        ROUND(ZEROIFNULL(COALESCE(dt.duty_cost_per_unit, mdd.duty) + NVL(screen_print_duty, 0)),
                              2)                                                                    AS lc_duty,
                        ROUND(ZEROIFNULL(COALESCE(dt.tariff_cost_per_unit, mdd.tariff_cost_per_unit)),
                              2)                                                                    AS lc_tariff,
                        c.actual_primary_cost_per_unit                                              AS lc_cpu,
                        po.po_type,
                        lcmr.record_type
        FROM _cost c
                 JOIN _rcv rcv
                      ON c.po_dtl_id = rcv.po_dtl_id
                 JOIN gsc.po_skus_data po
                      ON rcv.po_dtl_id = po.po_dtl_id
                 LEFT JOIN gsc.lc_asn_detail_dataset asn
                           ON rcv.po_number = asn.po_number
                               AND rcv.item_number = asn.item_number
                               AND IFF(po.freight_method ILIKE 'AIR', 'AIR', po.freight_method) =
                                   UPPER(TRIM(asn.traffic_mode))
                 LEFT JOIN gsc.lc_oocl_units_shipped osr
                           ON rcv.po_number = osr.po_number
                               AND rcv.item_number = osr.item_number
                               AND po.freight_method =
                                   CASE
                                       WHEN osr.traffic_mode = 'AIR STANDARD' OR osr.traffic_mode = 'AIR'
                                           THEN 'AIR (STANDARD)'
                                       WHEN osr.traffic_mode = 'AIR EXPRESS'
                                           THEN 'AIR (EXPEDITED)'
                                       WHEN osr.traffic_mode = 'SEA'
                                           THEN 'OCEAN'
                                       ELSE osr.traffic_mode
                                       END
                 LEFT JOIN _duty_tariff dt
                           ON rcv.po_dtl_id = dt.po_dtl_id
                 LEFT JOIN _md_duty mdd
                           ON rcv.po_dtl_id = mdd.po_dtl_id
                 LEFT JOIN gsc.lc_cmt_labor_cost_dataset cmt
                           ON rcv.po_number = cmt.po_number
                               AND rcv.item_number = cmt.item_number

                 LEFT JOIN gsc.lc_budget_class bc
                           ON c.budget_bu = bc.bu
                               AND LEFT(po.sku, 2) = bc.segment
                 LEFT JOIN gsc.lc_budget_dataset b
                           ON c.budget_business_unit = b.business_unit
                               AND c.budget_gender = IFNULL(b.gender, '')
                               AND po.inco_term = b.po_incoterms
                               AND c.budget_category = b.category
                               AND bc.budget_class = b.class
                               AND UPPER(po.country_origin) = b.origin_country
                               AND UPPER(po.region_id) = b.destination_country
                               AND c.budget_fc = b.fc
                               AND c.budget_shipping_mode = b.shipping_mode
                               AND DATE_TRUNC(MONTH, rcv.received_date) = b.budget_date

                 LEFT JOIN reporting_prod.gsc.landed_cost_error_datafeed e ---update Db before deploy
                           ON c.po_dtl_id = e.po_dtl_id
                 LEFT JOIN _screen sc
                           ON c.po_dtl_id = sc.po_dtl_id
                 LEFT JOIN reference.landed_cost_manual_refresh lcmr
                           ON c.po_dtl_id = lcmr.po_dtl_id;

/******************************
Update duty, tariff and other affected fields
******************************/

UPDATE _stg_landed_cost src
SET cpu_otr                              = src.cpu_otr + NVL(new.extra_freight_cost_per_unit, 0),
    total_actual_primary_cost            = src.total_actual_primary_cost + NVL(new.extra_freight_total_cost, 0),
    actual_primary_cost_per_unit         = src.actual_primary_cost_per_unit + NVL(new.extra_freight_cost_per_unit, 0),
    actual_duty_cost_per_unit            = src.actual_duty_cost_per_unit + NVL(new.extra_duty_cost_per_unit, 0),
    total_duty_cost                      = src.total_duty_cost + NVL(new.extra_duty_total_cost, 0),
    total_actual_duty                    = src.total_actual_duty + NVL(new.extra_duty_total_cost, 0),
    total_primary_and_duty_cost_per_unit = src.total_primary_and_duty_cost_per_unit +
                                           NVL(new.extra_duty_cost_per_unit, 0) + NVL(new.extra_freight_cost_per_unit, 0),
    total_actual_primary_and_duty        = src.total_actual_primary_and_duty + NVL(new.extra_duty_total_cost, 0)
                                            + NVL(new.extra_freight_total_cost, 0),
    duty_cost_per_unit                   = src.duty_cost_per_unit + NVL(new.extra_duty_cost_per_unit, 0),
    lc_duty                              = src.lc_duty + NVL(new.extra_duty_cost_per_unit, 0),
    actual_tariff_cost_per_unit          = CASE
                                               WHEN src.actual_tariff_cost_per_unit IS NULL AND
                                                    new.extra_tariff_cost_per_unit IS NULL
                                                   THEN NULL
                                               WHEN src.actual_tariff_cost_per_unit IS NULL AND
                                                    new.extra_tariff_cost_per_unit IS NOT NULL
                                                   THEN NVL(new.extra_tariff_cost_per_unit, 0)
                                               ELSE
                                                   src.actual_tariff_cost_per_unit +
                                                   NVL(new.extra_tariff_cost_per_unit, 0)
                                            END,
    total_actual_tariff_cost             = CASE
                                               WHEN src.total_actual_tariff_cost IS NULL AND
                                                    new.extra_tariff_total_cost IS NULL
                                                   THEN NULL
                                               WHEN src.total_actual_tariff_cost IS NULL AND
                                                    new.extra_tariff_total_cost IS NOT NULL
                                                   THEN NVL(new.extra_tariff_total_cost, 0)
                                               ELSE
                                                   src.total_actual_tariff_cost +
                                                   NVL(new.extra_tariff_total_cost, 0)
        END,
    lc_tariff                            = src.lc_tariff + NVL(new.extra_tariff_cost_per_unit, 0),
    total_actual_landed_cost             = src.total_actual_landed_cost + NVL(new.extra_duty_total_cost, 0)
        + NVL(new.extra_tariff_total_cost, 0) + NVL(new.extra_freight_total_cost, 0),
    actual_landed_cost_per_unit          = src.actual_landed_cost_per_unit +
                                           NVL(new.extra_duty_cost_per_unit, 0)
                                               + NVL(new.extra_tariff_cost_per_unit, 0) + NVL(new.extra_freight_cost_per_unit, 0),
    lc_cpu                               = src.lc_cpu + NVL(new.extra_freight_cost_per_unit, 0)
FROM reporting_base_prod.reference.po_override_lcosts_modified new
WHERE new.po_line_key = src.po_dtl_id;

UPDATE reporting_base_prod.reference.po_override_lcosts_modified po_over_mod
SET po_over_mod.year_month_received = stg.year_month_received
FROM _stg_landed_cost stg
WHERE po_over_mod.po_line_key = stg.po_dtl_id;

/******************************
Merge data
*******************************/

        MERGE INTO
--gsc.landed_cost_dataset tgt
            reporting_prod.gsc.landed_cost_dataset tgt
            USING _stg_landed_cost src
            ON tgt.year_month_received = src.year_month_received
                AND tgt.po_dtl_id = src.po_dtl_id
            WHEN MATCHED
                AND
                    ((src.record_type IS NULL AND src.year_month_received >= DATEADD('day',
                                                              CASE
                                                                  WHEN DATE_PART(DAY, CURRENT_TIMESTAMP()) = 4
                                                                      THEN -120
                                                                  ELSE -30
                                                                  END, CURRENT_TIMESTAMP()))
                        OR (src.year_month_received < DATEADD('day',
                                                              CASE
                                                                  WHEN DATE_PART(DAY, CURRENT_TIMESTAMP()) = 4
                                                                      THEN -120
                                                                  ELSE -30
                                                                  END
                            , CURRENT_TIMESTAMP())
                            AND (NOT EQUAL_NULL(tgt.cmt_cost, src.cmt_cost)))
                                OR src.record_type = 'manual') THEN
                UPDATE
                    SET
--tgt.YEAR_MONTH_RECEIVED = src.YEAR_MONTH_RECEIVED,
                        tgt.inco_term = src.inco_term,
                        tgt.business_unit = src.business_unit,
                        tgt.gender = src.gender,
                        tgt.category = src.category,
                        tgt.class = src.class,
                        tgt.po_number = src.po_number,
                        tgt.receiving_warehouse_id = src.receiving_warehouse_id,
                        tgt.destination_warehouse_id = src.destination_warehouse_id,
                        tgt.show_room = src.show_room,
                        tgt.sku = src.sku,
                        tgt.plm_style = src.plm_style,
                        tgt.budget_class = src.budget_class,
                        tgt.style_type = src.style_type,
                        tgt.style_type_new = src.style_type_new,
                        tgt.origin_city = src.origin_city,
                        tgt.origin_country = src.origin_country,
                        tgt.destination_country = src.destination_country,
                        tgt.vend_name = src.vend_name,
                        tgt.po_mode = src.po_mode,
                        tgt.shipping_mode = src.shipping_mode,
                        tgt.units_ordered = src.units_ordered,
                        tgt.units_shipped = src.units_shipped,
                        tgt.units_received = src.units_received,
                        tgt.total_actual_primary_cost = src.total_actual_primary_cost,
                        tgt.actual_primary_cost_per_unit = src.actual_primary_cost_per_unit,
                        tgt.budgeted_primary_cost = src.budgeted_primary_cost,
                        tgt.actual_duty_rate = src.actual_duty_rate,
                        tgt.budget_duty_rate = src.budget_duty_rate,
                        tgt.actual_tariff_rate = src.actual_tariff_rate,
                        tgt.budget_tariff_rate = src.budget_tariff_rate,
                        tgt.actual_tariff_cost_per_unit = src.actual_tariff_cost_per_unit,
                        tgt.total_actual_tariff_cost = src.total_actual_tariff_cost,
                        tgt.budget_tariff_cost_per_unit = src.budget_tariff_cost_per_unit,
                        tgt.total_budget_tariff_cost = src.total_budget_tariff_cost,
                        tgt.budget_unit_cost = src.budget_unit_cost,
                        tgt.actual_unit_cost = src.actual_unit_cost,
                        tgt.budget_auc_per_unit = src.budget_auc_per_unit,
                        tgt.agency_cost_no_ci = src.agency_cost_no_ci,
                        tgt.po_cost_without_commission = src.po_cost_without_commission,
                        tgt.agency_duty_savings_per_unit = src.agency_duty_savings_per_unit,
                        tgt.agency_cost_and_po_cost = src.agency_cost_and_po_cost,
                        tgt.total_agency_cost_and_po_cost = src.total_agency_cost_and_po_cost,
                        tgt.first_sale_program_cost = src.first_sale_program_cost,
                        tgt.first_cost_and_po_cost = src.first_cost_and_po_cost,
                        tgt.total_first_cost_and_po_cost = src.total_first_cost_and_po_cost,
                        tgt.actual_duty_cost_per_unit = src.actual_duty_cost_per_unit,
                        tgt.total_duty_cost = src.total_duty_cost,
                        tgt.total_budgeted_primary_cost = src.total_budgeted_primary_cost,
                        tgt.total_budgeted_product_cost = src.total_budgeted_product_cost,
                        tgt.total_actual_product_cost = src.total_actual_product_cost,
                        tgt.total_actual_duty = src.total_actual_duty,
                        tgt.total_primary_and_duty_cost_per_unit = src.total_primary_and_duty_cost_per_unit,
                        tgt.total_actual_primary_and_duty = src.total_actual_primary_and_duty,
                        tgt.total_budgeted_duty = src.total_budgeted_duty,
                        tgt.total_budgeted_primary_and_duty = src.total_budgeted_primary_and_duty,
                        tgt.total_actual_landed_cost = src.total_actual_landed_cost,
                        tgt.actual_landed_cost_per_unit = src.actual_landed_cost_per_unit,
                        tgt.total_blended_budget = src.total_blended_budget,
                        tgt.total_agency_program_savings = src.total_agency_program_savings,
                        tgt.total_first_sale_program_savings = src.total_first_sale_program_savings,
                        tgt.fc = src.fc,
                        tgt.cmt_cost = src.cmt_cost,
                        tgt.cpu_freight = src.cpu_freight,
                        tgt.cpu_ocean = src.cpu_ocean,
                        tgt.cpu_air = src.cpu_air,
                        tgt.cpu_transload = src.cpu_transload,
                        tgt.cpu_otr = src.cpu_otr,
                        tgt.cpu_domestic = src.cpu_domestic,
                        tgt.cpu_pierpass = src.cpu_pierpass,
                        tgt.history = src.history,
                        tgt.fully_landed = src.fully_landed,
                        tgt.brand = src.brand,
                        tgt.centric_department = src.centric_department,
                        tgt.centric_subdepartment = src.centric_subdepartment,
                        tgt.centric_category = src.centric_category,
                        tgt.centric_class = src.centric_class,
                        tgt.centric_subclass = src.centric_subclass,
                        tgt.centric_style_id = src.centric_style_id,
                        tgt.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(9),
                        tgt.po_line_number = src.po_line_number,
                        tgt.screen_print_duty_cpu = src.screen_print_duty,
                        tgt.screen_print_freight_cpu = src.screen_print_freight,
                        tgt.duty_cost_per_unit = src.duty_cost_per_unit,
                        tgt.fully_calculated_cost = src.fully_calculated_cost,
                        tgt.review_required = src.review_required,
                        tgt.po_type = src.po_type
            WHEN NOT MATCHED THEN INSERT
                (year_month_received,
                 inco_term,
                 business_unit,
                 gender,
                 category,
                 class,
                 po_number,
                 receiving_warehouse_id,
                 destination_warehouse_id,
                 show_room,
                 sku,
                 plm_style,
                 budget_class,
                 style_type,
                 style_type_new,
                 origin_city,
                 origin_country,
                 destination_country,
                 vend_name,
                 po_mode,
                 shipping_mode,
                 units_ordered,
                 units_shipped,
                 units_received,
                 total_actual_primary_cost,
                 actual_primary_cost_per_unit,
                 budgeted_primary_cost,
                 actual_duty_rate,
                 budget_duty_rate,
                 actual_tariff_rate,
                 budget_tariff_rate,
                 actual_tariff_cost_per_unit,
                 total_actual_tariff_cost,
                 budget_tariff_cost_per_unit,
                 total_budget_tariff_cost,
                 budget_unit_cost,
                 actual_unit_cost,
                 budget_auc_per_unit,
                 agency_cost_no_ci,
                 po_cost_without_commission,
                 agency_duty_savings_per_unit,
                 agency_cost_and_po_cost,
                 total_agency_cost_and_po_cost,
                 first_sale_program_cost,
                 first_cost_and_po_cost,
                 total_first_cost_and_po_cost,
                 actual_duty_cost_per_unit,
                 total_duty_cost,
                 total_budgeted_primary_cost,
                 total_budgeted_product_cost,
                 total_actual_product_cost,
                 total_actual_duty,
                 total_primary_and_duty_cost_per_unit,
                 total_actual_primary_and_duty,
                 total_budgeted_duty,
                 total_budgeted_primary_and_duty,
                 total_actual_landed_cost,
                 actual_landed_cost_per_unit,
                 total_blended_budget,
                 total_agency_program_savings,
                 total_first_sale_program_savings,
                 fc,
                 cmt_cost,
                 cpu_freight,
                 cpu_ocean,
                 cpu_air,
                 cpu_transload,
                 cpu_otr,
                 cpu_domestic,
                 cpu_pierpass,
                 history,
                 fully_landed,
                 brand,
                 centric_department,
                 centric_subdepartment,
                 centric_category,
                 centric_class,
                 centric_subclass,
                 centric_style_id,
                 po_line_number,
                 po_dtl_id,
                 screen_print_duty_cpu,
                 screen_print_freight_cpu,
                 duty_cost_per_unit,
                 fully_calculated_cost,
                 review_required,
                 po_type
                    )
                VALUES (src.year_month_received,
                        src.inco_term,
                        src.business_unit,
                        src.gender,
                        src.category,
                        src.class,
                        src.po_number,
                        src.receiving_warehouse_id,
                        src.destination_warehouse_id,
                        src.show_room,
                        src.sku,
                        src.plm_style,
                        src.budget_class,
                        src.style_type,
                        src.style_type_new,
                        src.origin_city,
                        src.origin_country,
                        src.destination_country,
                        src.vend_name,
                        src.po_mode,
                        src.shipping_mode,
                        src.units_ordered,
                        src.units_shipped,
                        src.units_received,
                        src.total_actual_primary_cost,
                        src.actual_primary_cost_per_unit,
                        src.budgeted_primary_cost,
                        src.actual_duty_rate,
                        src.budget_duty_rate,
                        src.actual_tariff_rate,
                        src.budget_tariff_rate,
                        src.actual_tariff_cost_per_unit,
                        src.total_actual_tariff_cost,
                        src.budget_tariff_cost_per_unit,
                        src.total_budget_tariff_cost,
                        src.budget_unit_cost,
                        src.actual_unit_cost,
                        src.budget_auc_per_unit,
                        src.agency_cost_no_ci,
                        src.po_cost_without_commission,
                        src.agency_duty_savings_per_unit,
                        src.agency_cost_and_po_cost,
                        src.total_agency_cost_and_po_cost,
                        src.first_sale_program_cost,
                        src.first_cost_and_po_cost,
                        src.total_first_cost_and_po_cost,
                        src.actual_duty_cost_per_unit,
                        src.total_duty_cost,
                        src.total_budgeted_primary_cost,
                        src.total_budgeted_product_cost,
                        src.total_actual_product_cost,
                        src.total_actual_duty,
                        src.total_primary_and_duty_cost_per_unit,
                        src.total_actual_primary_and_duty,
                        src.total_budgeted_duty,
                        src.total_budgeted_primary_and_duty,
                        src.total_actual_landed_cost,
                        src.actual_landed_cost_per_unit,
                        src.total_blended_budget,
                        src.total_agency_program_savings,
                        src.total_first_sale_program_savings,
                        src.fc,
                        src.cmt_cost,
                        src.cpu_freight,
                        src.cpu_ocean,
                        src.cpu_air,
                        src.cpu_transload,
                        src.cpu_otr,
                        src.cpu_domestic,
                        src.cpu_pierpass,
                        src.history,
                        src.fully_landed,
                        src.brand,
                        src.centric_department,
                        src.centric_subdepartment,
                        src.centric_category,
                        src.centric_class,
                        src.centric_subclass,
                        src.centric_style_id,
                        src.po_line_number,
                        src.po_dtl_id,
                        src.screen_print_duty,
                        src.screen_print_freight,
                        src.duty_cost_per_unit,
                        src.fully_calculated_cost,
                        src.review_required,
                        src.po_type);


/***********landed cost ******************/
        UPDATE gsc.po_skus_data
        SET actual_landed_cost_per_unit = lcd.actual_landed_cost_per_unit
          , reporting_landed_cost       = lcd.actual_landed_cost_per_unit
          , meta_update_datetime        = CURRENT_TIMESTAMP()
        FROM _stg_landed_cost lcd
        WHERE gsc.po_skus_data.po_dtl_id = lcd.po_dtl_id
          AND lcd.fully_landed = 'Y';


/**** update manual refresh table ****/
        UPDATE reference.landed_cost_manual_refresh lcmr
        SET to_be_processed = FALSE
        FROM _stg_landed_cost lcd
        WHERE lcmr.po_dtl_id = lcd.po_dtl_id
            END;
