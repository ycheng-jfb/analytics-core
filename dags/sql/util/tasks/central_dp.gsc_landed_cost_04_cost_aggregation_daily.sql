CREATE OR REPLACE TASK util.tasks_central_dp.gsc_landed_cost_04_cost_aggregation_daily
    WAREHOUSE =da_wh_analytics
    AFTER util.tasks_central_dp.gsc_landed_cost_03_invoice_aggregation_daily
    AS BEGIN


        USE SCHEMA reporting_base_prod.gsc;

        CREATE OR REPLACE TEMPORARY TABLE _po
        (
            po_number      VARCHAR(100),
            item_number    VARCHAR(100),
            po_line_number NUMBER(38, 0),
            po_dtl_id      NUMBER(38, 0)
        );

        INSERT INTO _po
            (po_number, item_number, po_line_number, po_dtl_id)

        /*
        select distinct psd.po_number, psd.sku, NVL(psd.bc_po_line_number, psd.po_dtl_id) as po_line_number, psd.po_dtl_id
        from gsc.po_skus_data psd
        where 1=1
        --and po_dtl_id in (4002450316)
        and psd.received_date_80_percent >= '2024-01-01'
        ;
        */

--/*
        SELECT DISTINCT a.po_number, a.sku, a.po_line_number, a.po_dtl_id
        FROM (SELECT psd.po_number, psd.sku, NVL(psd.bc_po_line_number, psd.po_dtl_id) AS po_line_number, psd.po_dtl_id
              FROM gsc.po_skus_data psd
              WHERE psd.received_date_80_percent >= DATEADD('day',
                                                            CASE
                                                                WHEN DATE_PART(DAY, CURRENT_TIMESTAMP()) = 4
                                                                    THEN -120
                                                                ELSE -30
                                                                END
                  , CURRENT_TIMESTAMP()
                                                    )

              UNION

              SELECT psd.po_number, psd.sku, NVL(psd.bc_po_line_number, psd.po_dtl_id) AS po_line_number, psd.po_dtl_id
              FROM reporting_prod.gsc.landed_cost_dataset lc
                       JOIN gsc.po_skus_data psd
                            ON lc.po_dtl_id = psd.po_dtl_id
              WHERE lc.fully_landed = 'N'
                AND lc.history = 'N'
                AND TO_DATE(lc.meta_create_datetime) >= TO_DATE(DATEADD(DAY, -120, CURRENT_TIMESTAMP()))

              UNION

              SELECT psd.po_number, psd.sku, NVL(psd.bc_po_line_number, psd.po_dtl_id) AS po_line_number, psd.po_dtl_id
              FROM reference.landed_cost_manual_refresh lcmr
                       JOIN gsc.po_skus_data psd
                            ON lcmr.po_dtl_id = psd.po_dtl_id
              WHERE lcmr.to_be_processed = TRUE) a;
--*/

        CREATE OR REPLACE TEMPORARY TABLE _cost
        (
            datapoint      VARCHAR(255) NOT NULL,
            proc           VARCHAR(255) NOT NULL,
            po_number      VARCHAR(255) NOT NULL,
            item_number    VARCHAR(255) NOT NULL,
            po_line_number NUMBER(38, 0),
            po_dtl_id      NUMBER(38, 0),
            bl             VARCHAR(255)   DEFAULT 'null',
            carrier        VARCHAR(255)   DEFAULT 'null',
            container      VARCHAR(255)   DEFAULT 'null',
            invoice_number VARCHAR(255)   DEFAULT 'null',
            post_advice    VARCHAR(255)   DEFAULT 'null',
            order_number   VARCHAR(255)   DEFAULT 'null',
            units_shipped  INT            DEFAULT 0,
            cost           NUMBER(38, 12) DEFAULT 0
        );


/*lc cost dataset */
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, cost)
        SELECT datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, SUM(cost) AS cost
        FROM (SELECT 'AIR_FREIGHT_COST'                                                                  AS datapoint,
                     'LC_COST_AIR_DATASET'                                                               AS proc,
                     p.po_number,
                     p.item_number,
                     p.po_line_number,
                     p.po_dtl_id,
                     ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
              FROM _po p
                       JOIN gsc.lc_cbm_data c
                            ON p.po_dtl_id = c.po_dtl_id
                                AND c.datapoint = 'po_sku_bl'
                       LEFT JOIN gsc.lc_cbm_data c1
                                 ON c.bl = c1.bl
                                     AND c1.datapoint = 'bl'
                       JOIN gsc.lc_invoice_data i
                            ON c.bl = i.bl
                                AND i.field_name = 'AIR_FREIGHT_DATA'
              WHERE 1 = 1
                AND i.amount_usd IS NOT NULL
              GROUP BY p.po_number, p.item_number, p.po_line_number, p.po_dtl_id
              HAVING SUM(c1.cbm) > 0
                 AND SUM(c.units_shipped) > 0) a
        GROUP BY datapoint, proc, po_number, item_number, po_line_number, po_dtl_id;

        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, cost)
        SELECT datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, SUM(cost) AS cost
        FROM (SELECT 'AIR_OTHER_COST'                                                                    AS datapoint,
                     'LC_COST_AIR_DATASET'                                                               AS proc,
                     p.po_number,
                     p.item_number,
                     p.po_line_number,
                     p.po_dtl_id,
                     ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
              FROM _po p
                       JOIN gsc.lc_cbm_data c
                            ON p.po_dtl_id = c.po_dtl_id
                                AND c.datapoint = 'po_sku_bl'
                       LEFT JOIN gsc.lc_cbm_data c1
                                 ON c.bl = c1.bl
                                     AND c1.datapoint = 'bl'
                       JOIN gsc.lc_invoice_data i
                            ON c.bl = i.bl
                                AND i.field_name = 'AIR_OTHER_DATA'
              WHERE i.amount_usd != 0
              GROUP BY p.po_number, p.item_number, p.po_line_number, p.po_dtl_id
              HAVING SUM(c1.cbm) > 0
                 AND SUM(c.units_shipped) > 0) a
        GROUP BY datapoint, proc, po_number, item_number, po_line_number, po_dtl_id;

/************Air G-Global OTR***************/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, cost)
        SELECT datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, SUM(cost) AS cost
        FROM (SELECT 'OTR_COST'                                                                          AS datapoint,
                     'LC_COST_AIR_DATASET'                                                               AS proc,
                     p.po_number,
                     p.item_number,
                     p.po_line_number,
                     p.po_dtl_id,
                     ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
              FROM _po p
                       JOIN gsc.lc_cbm_data c
                            ON p.po_dtl_id = c.po_dtl_id
                                AND c.datapoint = 'po_sku_bl'
                       LEFT JOIN gsc.lc_cbm_data c1
                                 ON c.bl = c1.bl
                                     AND c1.datapoint = 'bl'
                       JOIN gsc.lc_invoice_data i
                            ON c.bl = i.bl
                                AND i.field_name = 'AIR_OTR_DATA'
              WHERE i.amount_usd != 0
              GROUP BY p.po_number, p.item_number, p.po_line_number, p.po_dtl_id
              HAVING SUM(c1.cbm) > 0
                 AND SUM(c.units_shipped) > 0) a
        GROUP BY datapoint, proc, po_number, item_number, po_line_number, po_dtl_id;


/*lc cost air detail */
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, bl, carrier, invoice_number, cost)
        SELECT datapoint,
               proc,
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               bl,
               carrier,
               invoice_number,
               SUM(cost) AS cost
        FROM (SELECT 'AIR_FREIGHT_COST'                                                                  AS datapoint,
                     'LC_COST_AIR_DETAIL_DATASET'                                                        AS proc,
                     p.po_number,
                     p.item_number,
                     p.po_line_number,
                     p.po_dtl_id,
                     i.bl,
                     i.carrier,
                     i.invoice_number,
                     ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
              FROM _po p
                       JOIN gsc.lc_cbm_data c
                            ON p.po_dtl_id = c.po_dtl_id
                                AND c.datapoint = 'po_sku_bl'
                       LEFT JOIN gsc.lc_cbm_data c1
                                 ON c.bl = c1.bl
                                     AND c1.datapoint = 'bl'
                       JOIN gsc.lc_invoice_data i
                            ON c.bl = i.bl
                                AND i.field_name = 'AIR_FREIGHT_DETAIL'
              WHERE i.amount_usd != 0
              GROUP BY p.po_number, p.item_number, p.po_line_number, p.po_dtl_id, i.bl, i.carrier, i.invoice_number
              HAVING SUM(c1.cbm) > 0
                 AND SUM(c.units_shipped) > 0) a
        GROUP BY a.datapoint, a.proc, a.po_number, a.item_number, a.po_line_number, a.po_dtl_id, a.bl, a.carrier,
                 a.invoice_number;

        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, bl, carrier, invoice_number, cost)
        SELECT datapoint,
               proc,
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               bl,
               carrier,
               invoice_number,
               SUM(cost) AS cost
        FROM (SELECT 'AIR_OTHER_COST'                                                                    AS datapoint,
                     'LC_COST_AIR_DETAIL_DATASET'                                                        AS proc,
                     p.po_number,
                     p.item_number,
                     p.po_line_number,
                     p.po_dtl_id,
                     i.bl,
                     i.carrier,
                     i.invoice_number,
                     ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
              FROM _po p
                       JOIN gsc.lc_cbm_data c
                            ON p.po_dtl_id = c.po_dtl_id
                                AND c.datapoint = 'po_sku_bl'
                       LEFT JOIN gsc.lc_cbm_data c1
                                 ON c.bl = c1.bl
                                     AND c1.datapoint = 'bl'
                       JOIN gsc.lc_invoice_data i
                            ON c.bl = i.bl
                                AND i.field_name = 'AIR_OTHER_DETAIL'
              WHERE i.amount_usd != 0
              GROUP BY p.po_number, p.item_number, p.po_line_number, p.po_dtl_id, i.bl, i.carrier, i.invoice_number
              HAVING SUM(c1.cbm) > 0
                 AND SUM(c.units_shipped) > 0) a
        GROUP BY a.datapoint, a.proc, a.po_number, a.item_number, a.po_line_number, a.po_dtl_id, a.bl, a.carrier,
                 a.invoice_number;

/*DOMESTIC DATASET*/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, units_shipped, cost)
        SELECT 'DOMESTIC_COST'                                                                     AS datapoint,
               'LC_COST_DOMESTIC_DATASET'                                                          AS proc,
               p.po_number,
               p.item_number,
               p.po_line_number,
               p.po_dtl_id,
               SUM(c.units_shipped)                                                                AS units_shipped,
               ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
        FROM _po p
                 JOIN gsc.lc_cbm_data c
                      ON p.po_dtl_id = c.po_dtl_id
                          AND c.datapoint = 'po_sku_pa'
                 LEFT JOIN gsc.lc_cbm_data c1
                           ON c.post_advice_number = c1.post_advice_number
                               AND c1.datapoint = 'pa'
                 LEFT JOIN gsc.lc_invoice_data i
                           ON c.post_advice_number = i.post_advice
                               AND i.field_name = 'DOMESTIC_FREIGHT_DATA'
        GROUP BY p.po_number, p.item_number, p.po_line_number, p.po_dtl_id;

/*domestic detail*/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, bl, carrier, invoice_number, units_shipped,
         cost)
        SELECT 'DOMESTIC_COST'                                                                     AS datapoint,
               'LC_COST_DOMESTIC_DETAIL_DATASET'                                                   AS proc,
               p.po_number,
               p.item_number,
               p.po_line_number,
               p.po_dtl_id,
               c.post_advice_number                                                                AS bl,
               NVL(i.carrier, 'null')                                                              AS carrier,
               NVL(i.invoice_number, 'null')                                                       AS invoice_number,
               SUM(c.units_shipped)                                                                AS units_shipped,
               ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
        FROM _po p
                 JOIN gsc.lc_cbm_data c
                      ON p.po_dtl_id = c.po_dtl_id
                          AND c.datapoint = 'po_sku_pa'
                 LEFT JOIN gsc.lc_cbm_data c1
                           ON c.post_advice_number = c1.post_advice_number
                               AND c1.datapoint = 'pa'
                 LEFT JOIN gsc.lc_invoice_data i
                           ON c.post_advice_number = i.post_advice
                               AND i.field_name = 'DOMESTIC_FREIGHT_DETAIL'
        GROUP BY p.po_number, p.item_number, p.po_line_number, p.po_dtl_id, c.post_advice_number, i.carrier,
                 i.invoice_number;

/*Ocean dataset*/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, cost)
        SELECT 'OCEAN_FREIGHT_COST'                                                          AS datapoint,
               'LC_COST_OCEAN_DATASET'                                                       AS proc,
               q0.po_number,
               q0.item_number,
               q0.po_line_number,
               q0.po_dtl_id,
               COALESCE(q1.ocean_freight_cost, q2.ocean_freight_cost, q3.ocean_freight_cost) AS cost
        FROM (SELECT DISTINCT po_number,
                              item_number,
                              po_line_number,
                              po_dtl_id
              FROM _po) q0

                 LEFT JOIN
             (SELECT po_number,
                     item_number,
                     po_line_number,
                     po_dtl_id,
                     SUM(ocean_freight_cost) AS ocean_freight_cost
              FROM (SELECT p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           CASE
                               WHEN SUM(c1.con_cbm) = 0 OR SUM(c.units_shipped) = 0
                                   THEN 0
                               ELSE ((SUM(c.cbm) / SUM(c1.con_cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)
                               END
                               AS ocean_freight_cost
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_pa_con'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.post_advice_number = c1.post_advice_number
                                           AND c.container = c1.container
                                           AND c1.datapoint = 'pa_con'
                             JOIN gsc.lc_invoice_data i
                                  ON c.post_advice_number = i.post_advice
                                      AND c.container = i.container
                                      AND i.field_name = 'OCEAN_FREIGHT_DATA'
                    GROUP BY p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.con_cbm) + SUM(c.units_shipped) > 0)
              GROUP BY po_number,
                       item_number,
                       po_line_number,
                       po_dtl_id) q1
             ON q0.po_dtl_id = q1.po_dtl_id

                 LEFT JOIN

             (SELECT po_number,
                     item_number,
                     po_line_number,
                     po_dtl_id,
                     SUM(ocean_freight_cost) AS ocean_freight_cost
              FROM (SELECT p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           --((SUM(c.CBM) / SUM(c1.CBM)) * SUM(i.AMOUNT_USD)) / SUM(c.UNITS_SHIPPED) AS OCEAN_FREIGHT_COST,
                           CASE
                               WHEN SUM(c1.con_cbm) = 0 OR SUM(c.units_shipped) = 0
                                   THEN 0
                               ELSE ((SUM(c.cbm) / SUM(c1.con_cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)
                               END
                                AS ocean_freight_cost,
                           NULL AS ocean_other_cost
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_con'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.container = c1.container
                                           AND c.bl = c1.bl
                                           AND c1.datapoint = 'con_bl'
                             JOIN gsc.lc_invoice_data i
                                  ON c.container = i.container
                                      AND c.bl = i.bl
                                      AND i.field_name = 'OCEAN_FREIGHT_ALT_DATA'
                    GROUP BY p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.con_cbm) > 0
                       AND SUM(c.units_shipped) > 0)
              GROUP BY po_number,
                       item_number,
                       po_line_number,
                       po_dtl_id) q2
             ON q0.po_dtl_id = q2.po_dtl_id

                 LEFT JOIN

             (SELECT p.po_number,
                     p.item_number,
                     p.po_line_number,
                     p.po_dtl_id,
                     ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped) AS ocean_freight_cost
              FROM _po p
                       JOIN gsc.lc_cbm_data c
                            ON p.po_dtl_id = c.po_dtl_id
                                AND c.datapoint = 'po_sku_con'
                       LEFT JOIN gsc.lc_cbm_data c1
                                 ON c.container = c1.container
                                     AND c1.datapoint = 'con'
                       JOIN gsc.lc_invoice_data i
                            ON c.container = i.container
                                AND i.field_name = 'OCEAN_FREIGHT_ALT_2_DATA'
              GROUP BY p.po_number,
                       p.item_number,
                       p.po_line_number,
                       p.po_dtl_id
              HAVING SUM(c1.cbm) > 0
                 AND SUM(c.units_shipped) > 0) q3
             ON q0.po_dtl_id = q3.po_dtl_id
        WHERE 1 = 1
          AND COALESCE(q1.ocean_freight_cost, q2.ocean_freight_cost, q3.ocean_freight_cost) > 0;


        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, cost)
        SELECT 'OCEAN_OTHER_COST'                                 AS datapoint,
               'LC_COST_OCEAN_DATASET'                            AS proc,
               q0.po_number,
               q0.item_number,
               q0.po_line_number,
               q0.po_dtl_id,
               --COALESCE(Q1.PO_NUMBER,Q2.PO_NUMBER) AS PO_NUMBER,
               --COALESCE(Q1.ITEM_NUMBER,Q2.ITEM_NUMBER) AS ITEM_NUMBER,
               COALESCE(q1.ocean_other_cost, q2.ocean_other_cost) AS ocean_other_cost
        FROM (SELECT DISTINCT po_number,
                              item_number,
                              po_line_number,
                              po_dtl_id
              FROM _po) q0

                 LEFT JOIN
             (SELECT po_number,
                     item_number,
                     po_line_number,
                     po_dtl_id,
                     SUM(ocean_other_cost) AS ocean_other_cost
              FROM (SELECT p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           NULL                                                                    AS ocean_freight_cost,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped) AS ocean_other_cost
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_pa_con'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.post_advice_number = c1.post_advice_number
                                           AND c.container = c1.container
                                           AND c1.datapoint = 'pa_con'
                             JOIN gsc.lc_invoice_data i
                                  ON c.post_advice_number = i.post_advice
                                      AND c.container = i.container
                                      AND i.field_name = 'OCEAN_OTHER_DATA'

                    GROUP BY p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0)
              GROUP BY po_number,
                       item_number,
                       po_line_number,
                       po_dtl_id) q1
             ON q0.po_dtl_id = q1.po_dtl_id

                 LEFT JOIN

             (SELECT po_number,
                     item_number,
                     po_line_number,
                     po_dtl_id,
                     SUM(ocean_other_cost) AS ocean_other_cost
              FROM (SELECT p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           NULL                                                                    AS ocean_freight_cost,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped) AS ocean_other_cost
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_con_bl'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.container = c1.container
                                           AND c.bl = c1.bl
                                           AND c1.datapoint = 'con_bl'
                             JOIN gsc.lc_invoice_data i
                                  ON c.container = i.container
                                      AND c.bl = i.bl
                                      AND i.field_name = 'OCEAN_OTHER_ALT_DATA'
                    GROUP BY p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0)
              GROUP BY po_number,
                       item_number,
                       po_line_number,
                       po_dtl_id) q2
             ON q0.po_dtl_id = q2.po_dtl_id
        WHERE COALESCE(q1.ocean_other_cost, q2.ocean_other_cost) > 0;

/*OCEAN DETAIL ALT DATASET*/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, bl, carrier, container, invoice_number,
         post_advice, cost)
        SELECT 'OCEAN_FREIGHT_COST'               AS datapoint,
               'LC_COST_OCEAN_DETAIL_ALT_DATASET' AS proc,
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               bl,
               carrier,
               container,
               invoice_number,
               post_advice,
               SUM(ocean_freight_cost)            AS cost
        FROM (SELECT DISTINCT carrier,
                              invoice_number,
                              post_advice,
                              container,
                              bl,
                              po_number,
                              item_number,
                              po_line_number,
                              po_dtl_id,
                              FIRST_VALUE(ocean_freight_cost)
                                          OVER (PARTITION BY carrier, invoice_number, post_advice, container, bl, po_number, item_number ORDER BY priority ASC) AS ocean_freight_cost
              FROM (SELECT i.carrier,
                           i.invoice_number,
                           c.post_advice_number AS post_advice,
                           c.container,
                           'null'               AS bl,
                           p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) /
                           SUM(c.units_shipped) AS ocean_freight_cost,
                           1                    AS priority
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_pa_con'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.post_advice_number = c1.post_advice_number
                                           AND c.container = c1.container
                                           AND c1.datapoint = 'pa_con'
                             JOIN gsc.lc_invoice_data i
                                  ON c.post_advice_number = i.post_advice
                                      AND c.container = i.container
                                      AND i.field_name = 'OCEAN_FREIGHT_DETAIL'
                    GROUP BY i.carrier,
                             i.invoice_number,
                             c.post_advice_number,
                             c.container,
                             p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0

                    UNION

                    SELECT i.carrier,
                           i.invoice_number,
                           'null'               AS post_advice,
                           c.container,
                           c.bl,
                           p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) /
                           SUM(c.units_shipped) AS ocean_freight_cost,
                           2                    AS priority
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_con_bl'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.container = c1.container
                                           AND c.bl = c1.bl
                                           AND c1.datapoint = 'con_bl'
                             JOIN gsc.lc_invoice_data i
                                  ON c.container = i.container
                                      AND c.bl = i.bl
                                      AND i.field_name = 'OCEAN_FREIGHT_ALT_DETAIL'
                    GROUP BY i.carrier,
                             i.invoice_number,
                             c.container,
                             c.bl,
                             p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0

                    UNION

                    SELECT i.carrier,
                           i.invoice_number,
                           'null'                                                                 AS post_advice,
                           c.container,
                           'null'                                                                 AS bl,
                           p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           ((SUM(c.cbm) / SUM(c.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped) AS ocean_freight_cost,
                           3                                                                      AS priority
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_con'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.container = c1.container
                                           AND c1.datapoint = 'con'
                             JOIN gsc.lc_invoice_data i
                                  ON c.container = i.container
                                      AND i.field_name = 'OCEAN_FREIGHT_ALT_2_DETAIL'
                    GROUP BY i.carrier,
                             i.invoice_number,
                             c.container,
                             p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0))
        GROUP BY carrier,
                 invoice_number,
                 post_advice,
                 container,
                 bl,
                 po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id;

/*******ocean other cost****/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, bl, carrier, container, invoice_number,
         post_advice, cost)
        SELECT 'OCEAN_OTHER_COST'                 AS datapoint,
               'LC_COST_OCEAN_DETAIL_ALT_DATASET' AS proc,
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               bl,
               carrier,
               container,
               invoice_number,
               post_advice,
               SUM(ocean_other_cost)              AS cost
        FROM (SELECT DISTINCT carrier,
                              invoice_number,
                              post_advice,
                              container,
                              bl,
                              po_number,
                              item_number,
                              po_line_number,
                              po_dtl_id,
                              FIRST_VALUE(ocean_other_cost)
                                          OVER (PARTITION BY carrier, invoice_number, post_advice, container, bl, po_number, item_number, po_line_number ORDER BY priority ASC) AS ocean_other_cost
              FROM (SELECT i.carrier,
                           i.invoice_number,
                           c.post_advice_number                                                    AS post_advice,
                           c.container,
                           'null'                                                                  AS bl,
                           p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped) AS ocean_other_cost,
                           1                                                                       AS priority
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_pa_con'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.post_advice_number = c1.post_advice_number
                                           AND c.container = c1.container
                                           AND c1.datapoint = 'pa_con'
                             JOIN gsc.lc_invoice_data i
                                  ON c.post_advice_number = i.post_advice
                                      AND c.container = i.container
                                      AND i.field_name = 'OCEAN_OTHER_DETAIL'
                    GROUP BY i.carrier,
                             i.invoice_number,
                             c.post_advice_number,
                             c.container,
                             p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0
                    UNION

                    SELECT i.carrier,
                           i.invoice_number,
                           'null'                                                                  AS post_advice,
                           c.container,
                           c.bl,
                           p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped) AS ocean_other_cost,
                           2                                                                       AS priority
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_con_bl'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.post_advice_number = c1.post_advice_number
                                           AND c.container = c1.container
                                           AND c.bl = c1.bl
                                           AND c1.datapoint = 'con_bl'
                             JOIN gsc.lc_invoice_data i
                                  ON c.post_advice_number = i.post_advice
                                      AND c.container = i.container
                                      AND c.bl = i.bl
                                      AND i.field_name = 'OCEAN_OTHER_ALT_DETAIL'
                    GROUP BY i.carrier,
                             i.invoice_number,
                             c.container,
                             c.bl,
                             p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0))
        GROUP BY carrier,
                 invoice_number,
                 post_advice,
                 container,
                 bl,
                 po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id;

/***LC_COST_OCEAN_DETAIL_DATASET***/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, bl, carrier, container, invoice_number,
         post_advice, cost)
        SELECT 'OCEAN_FREIGHT_COST'           AS datapoint,
               'LC_COST_OCEAN_DETAIL_DATASET' AS proc,
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               bl,
               carrier,
               container,
               invoice_number,
               post_advice,
               SUM(ocean_freight_cost)        AS ocean_freight_cost
        FROM (SELECT DISTINCT carrier,
                              invoice_number,
                              FIRST_VALUE(post_advice)
                                          OVER (PARTITION BY carrier, invoice_number, po_number, item_number, po_line_number ORDER BY frt_priority ASC) AS post_advice,
                              FIRST_VALUE(container)
                                          OVER (PARTITION BY carrier, invoice_number, po_number, item_number, po_line_number ORDER BY frt_priority ASC) AS container,
                              FIRST_VALUE(bl)
                                          OVER (PARTITION BY carrier, invoice_number, po_number, item_number, po_line_number ORDER BY frt_priority ASC) AS bl,
                              po_number,
                              item_number,
                              po_line_number,
                              po_dtl_id,
                              FIRST_VALUE(ocean_freight_cost)
                                          OVER (PARTITION BY carrier, invoice_number, po_number, item_number, po_line_number ORDER BY frt_priority ASC) AS ocean_freight_cost
              FROM (SELECT i.carrier,
                           i.invoice_number,
                           c.post_advice_number AS post_advice,
                           c.container,
                           'null'               AS bl,
                           p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) /
                           SUM(c.units_shipped) AS ocean_freight_cost,
                           1                    AS frt_priority
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_pa_con'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.post_advice_number = c1.post_advice_number
                                           AND c.container = c1.container
                                           AND c1.datapoint = 'pa_con'
                             JOIN gsc.lc_invoice_data i
                                  ON c.post_advice_number = i.post_advice
                                      AND c.container = i.container
                                      AND i.field_name = 'OCEAN_FREIGHT_DETAIL'
                    GROUP BY i.carrier,
                             i.invoice_number,
                             c.post_advice_number,
                             c.container,
                             p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0

                    UNION

                    SELECT i.carrier,
                           i.invoice_number,
                           'null'               AS post_advice,
                           c.container,
                           c.bl,
                           p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) /
                           SUM(c.units_shipped) AS ocean_freight_cost,
                           2                    AS frt_priority
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_con_bl'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.container = c1.container
                                           AND c.bl = c1.bl
                                           AND c1.datapoint = 'con_bl'
                             JOIN gsc.lc_invoice_data i
                                  ON c.container = i.container
                                      AND c.bl = i.bl
                                      AND i.field_name = 'OCEAN_FREIGHT_ALT_DETAIL'
                    GROUP BY i.carrier,
                             i.invoice_number,
                             c.container,
                             c.bl,
                             p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0

                    UNION

                    SELECT i.carrier,
                           i.invoice_number,
                           'null'               AS post_advice,
                           c.container,
                           'null'               AS bl,
                           p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) /
                           SUM(c.units_shipped) AS ocean_freight_cost,
                           3                    AS frt_priority
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.container = c1.container
                                           AND c1.datapoint = 'con'
                             JOIN gsc.lc_invoice_data i
                                  ON c.container = i.container
                                      AND i.field_name = 'OCEAN_FREIGHT_ALT_2_DETAIL'
                    GROUP BY i.carrier,
                             i.invoice_number,
                             c.container,
                             p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0))
        GROUP BY carrier,
                 invoice_number,
                 post_advice,
                 container,
                 bl,
                 po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id;

/***ocean other data***/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, bl, carrier, container, invoice_number,
         post_advice, cost)
        SELECT 'OCEAN_OTHER_COST'             AS datapoint,
               'LC_COST_OCEAN_DETAIL_DATASET' AS proc,
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               bl,
               carrier,
               container,
               invoice_number,
               post_advice,
               SUM(ocean_other_cost)          AS ocean_other_cost
        FROM (SELECT DISTINCT carrier,
                              invoice_number,
                              FIRST_VALUE(post_advice)
                                          OVER (PARTITION BY carrier, invoice_number, po_number, item_number, po_line_number ORDER BY otr_priority ASC) AS post_advice,
                              FIRST_VALUE(container)
                                          OVER (PARTITION BY carrier, invoice_number, po_number, item_number, po_line_number ORDER BY otr_priority ASC) AS container,
                              FIRST_VALUE(bl)
                                          OVER (PARTITION BY carrier, invoice_number, po_number, item_number, po_line_number ORDER BY otr_priority ASC) AS bl,
                              po_number,
                              item_number,
                              po_line_number,
                              po_dtl_id,
                              FIRST_VALUE(ocean_other_cost)
                                          OVER (PARTITION BY carrier, invoice_number, po_number, item_number, po_line_number ORDER BY otr_priority ASC) AS ocean_other_cost
              FROM (SELECT i.carrier,
                           i.invoice_number,
                           c.post_advice_number                                                    AS post_advice,
                           c.container,
                           'null'                                                                  AS bl,
                           p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped) AS ocean_other_cost,
                           1                                                                       AS otr_priority
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_pa_con'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.post_advice_number = c1.post_advice_number
                                           AND c.container = c1.container
                                           AND c1.datapoint = 'pa_con'
                             JOIN gsc.lc_invoice_data i
                                  ON c.post_advice_number = i.post_advice
                                      AND c.container = i.container
                                      AND i.field_name = 'OCEAN_OTHER_DETAIL'
                    GROUP BY i.carrier,
                             i.invoice_number,
                             c.post_advice_number,
                             c.container,
                             p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0

                    UNION

                    SELECT i.carrier,
                           i.invoice_number,
                           'null'                                                                  AS post_advice,
                           c.container,
                           c.bl,
                           p.po_number,
                           p.item_number,
                           p.po_line_number,
                           p.po_dtl_id,
                           ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped) AS ocean_other_cost,
                           2                                                                       AS otr_priority
                    FROM _po p
                             JOIN gsc.lc_cbm_data c
                                  ON p.po_dtl_id = c.po_dtl_id
                                      AND c.datapoint = 'po_sku_con_bl'
                             LEFT JOIN gsc.lc_cbm_data c1
                                       ON c.container = c1.container
                                           AND c.bl = c1.bl
                                           AND c1.datapoint = 'con_bl'
                             JOIN gsc.lc_invoice_data i
                                  ON c.container = i.container
                                      AND c.bl = i.bl
                                      AND i.field_name = 'OCEAN_OTHER_ALT_DETAIL'

                    GROUP BY i.carrier,
                             i.invoice_number,
                             c.container,
                             c.bl,
                             p.po_number,
                             p.item_number,
                             p.po_line_number,
                             p.po_dtl_id
                    HAVING SUM(c1.cbm) > 0
                       AND SUM(c.units_shipped) > 0))
        GROUP BY carrier,
                 invoice_number,
                 post_advice,
                 container,
                 bl,
                 po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id;

/***lc_cost_pier_pass_dataset***/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, units_shipped, cost)
        SELECT 'PIER_PASS_COST'                                                                    AS datapoint,
               'LC_COST_PIER_PASS_DATASET'                                                         AS proc,
               p.po_number,
               p.item_number,
               p.po_line_number,
               p.po_dtl_id,
               SUM(c.units_shipped)                                                                AS units_shipped,
               ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
        FROM _po p
                 JOIN gsc.lc_cbm_data c
                      ON p.po_dtl_id = c.po_dtl_id
                          AND c.datapoint = 'po_sku_con'
                 LEFT JOIN gsc.lc_cbm_data c1
                           ON c.container = c1.container
                               AND c1.datapoint = 'con'
                 LEFT JOIN gsc.lc_invoice_data i
                           ON c.container = i.container
                               AND i.field_name = 'PEIR_PASS_FREIGHT_DATA'
        GROUP BY p.po_number,
                 p.item_number,
                 p.po_line_number,
                 p.po_dtl_id
        HAVING SUM(c1.cbm) > 0
           AND SUM(c.units_shipped) > 0;

/***LC_COST_PIER_PASS_DETAIL_DATASET***/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, carrier, container, invoice_number,
         units_shipped, cost)
        SELECT 'PIER_PASS_COST'                                                                    AS datapoint,
               'LC_COST_PIER_PASS_DETAIL_DATASET'                                                  AS proc,
               p.po_number,
               p.item_number,
               p.po_line_number,
               p.po_dtl_id,
               i.carrier,
               c.container,
               i.invoice_number,
               SUM(c.units_shipped)                                                                AS units_shipped,
               ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
        FROM _po p
                 JOIN gsc.lc_cbm_data c
                      ON p.po_dtl_id = c.po_dtl_id
                          AND c.datapoint = 'po_sku_con'
                 LEFT JOIN gsc.lc_cbm_data c1
                           ON c.container = c1.container
                               AND c1.datapoint = 'con'
                 JOIN gsc.lc_invoice_data i
                      ON c.container = i.container
                          AND i.field_name = 'PEIR_PASS_FREIGHT_DETAIL'
        GROUP BY i.carrier,
                 i.invoice_number,
                 c.container,
                 p.po_number,
                 p.item_number,
                 p.po_line_number,
                 p.po_dtl_id
        HAVING SUM(c1.cbm) > 0
           AND SUM(c.units_shipped) > 0;


        /***LC_COST_TRANSLOAD_DATASET***/ ---data error cost is off on some POs
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, units_shipped, cost)
        SELECT 'TRANSLOAD_COST'                                                                    AS datapoint,
               'LC_COST_TRANSLOAD_DATASET'                                                         AS proc,
               p.po_number,
               p.item_number,
               p.po_line_number,
               p.po_dtl_id,
               SUM(c.units_shipped)                                                                AS units_shipped,
               ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
        FROM _po p
                 JOIN gsc.lc_cbm_data c
                      ON p.po_dtl_id = c.po_dtl_id
                          AND c.datapoint = 'po_sku_con'
                 LEFT JOIN gsc.lc_cbm_data c1
                           ON c.container = c1.container
                               AND c1.datapoint = 'con'
                 LEFT JOIN gsc.lc_invoice_data i
                           ON c.container = i.container
                               AND i.field_name = 'TRANSLOAD_FREIGHT_DATA'
        --where p.po_number = 'MX135982-288462'
        --and p.item_number = 'LS2149831-4870-15050'
        GROUP BY p.po_number,
                 p.item_number,
                 p.po_line_number,
                 p.po_dtl_id
        HAVING SUM(c1.cbm) > 0
           AND SUM(c.units_shipped) > 0;

/***LC_COST_TRANSLOAD_DETAIL_DATASET***/
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, carrier, container, invoice_number,
         units_shipped, cost)
        SELECT 'TRANSLOAD_COST'                                                                    AS datapoint,
               'LC_COST_TRANSLOAD_DETAIL_DATASET'                                                  AS proc,
               p.po_number,
               p.item_number,
               p.po_line_number,
               p.po_dtl_id,
               i.carrier,
               c.container,
               i.invoice_number,
               SUM(c.units_shipped)                                                                AS units_shipped,
               ZEROIFNULL(((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped)) AS cost
        FROM _po p
                 JOIN gsc.lc_cbm_data c
                      ON p.po_dtl_id = c.po_dtl_id
                          AND c.datapoint = 'po_sku_con'
                 LEFT JOIN gsc.lc_cbm_data c1
                           ON c.container = c1.container
                               AND c1.datapoint = 'con'
                 JOIN gsc.lc_invoice_data i
                      ON c.container = i.container
                          AND i.field_name = 'TRANSLOAD_FREIGHT_DETAIL'
        GROUP BY i.carrier,
                 i.invoice_number,
                 c.container,
                 p.po_number,
                 p.item_number,
                 p.po_line_number,
                 p.po_dtl_id
        HAVING SUM(c1.cbm) > 0
           AND SUM(c.units_shipped) > 0;

--REPORTING_BASE_PROD.GSC.LC_COST_OTR_ORDER_DATASET
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, order_number, units_shipped, cost)
        SELECT 'OTR_COST'                                          AS datapoint,
               'LC_COST_OTR_ORDER_DATASET'                         AS proc,
               p.po_number,
               p.item_number,
               p.po_line_number,
               p.po_dtl_id,
               c.order_number,
               c.units_shipped,
               ((c.cbm / c1.cbm) * i.amount_usd) / c.units_shipped AS cost
        FROM _po p
                 JOIN gsc.lc_cbm_data c
                      ON p.po_dtl_id = c.po_dtl_id
                          AND c.datapoint = 'otr_po_sku'
                 JOIN gsc.lc_cbm_data c1
                      ON c.order_number = c1.order_number
                          AND c1.datapoint = 'otr_order'
                 JOIN gsc.lc_invoice_data i
                      ON c.order_number = i.bl
                          AND i.field_name = 'OTR_FREIGHT_DATA'
        WHERE c.units_shipped > 0;

--REPORTING_BASE_PROD.GSC.LC_COST_OTR_ORDER_DETAIL_DATASET
        INSERT INTO _cost
        (datapoint, proc, carrier, invoice_number, bl, po_number, item_number, po_line_number, po_dtl_id, order_number,
         units_shipped, cost)
        SELECT 'OTR_COST'                                          AS datapoint,
               'LC_COST_OTR_ORDER_DETAIL_DATASET'                  AS proc,
               i.carrier,
               i.invoice_number,
               i.bl,
               c.po_number,
               c.item_number,
               c.po_line_number,
               c.po_dtl_id,
               c.order_number,
               c.units_shipped,
               ((c.cbm / c1.cbm) * i.amount_usd) / c.units_shipped AS cost
        FROM _po p
                 JOIN gsc.lc_cbm_data c
                      ON p.po_dtl_id = c.po_dtl_id
                          AND c.datapoint = 'otr_po_sku'
                 JOIN gsc.lc_cbm_data c1
                      ON c.order_number = c1.order_number
                          AND c1.datapoint = 'otr_order'
                 JOIN gsc.lc_invoice_data i
                      ON c.order_number = i.bl
                          AND i.field_name = 'OTR_FREIGHT_DETAIL'
                          AND i.amount_usd > 0;


--REPORTING_BASE_PROD.GSC.LC_COST_OTR_DATASET
        INSERT INTO _cost
        (datapoint, proc, po_number, item_number, po_line_number, po_dtl_id, units_shipped, cost)
        SELECT 'OTR_COST'            AS datapoint,
               'LC_COST_OTR_DATASET' AS proc,
               po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               SUM(units_shipped)    AS units_shipped,
               SUM(cost)             AS cost
        FROM (SELECT c.po_number,
                     c.item_number,
                     c.po_line_number,
                     c.po_dtl_id,
                     --c.units_shipped,
                     SUM(c.units_shipped)                                   AS units_shipped,
                     (SUM(c.cost * c.units_shipped)) / SUM(c.units_shipped) AS cost
              FROM _po p
                       JOIN _cost c
                            ON p.po_dtl_id = c.po_dtl_id
              WHERE c.datapoint = 'OTR_COST'
                AND proc = 'LC_COST_OTR_ORDER_DATASET'
              GROUP BY c.po_number, c.item_number, c.po_line_number, c.po_dtl_id --yard_container
              HAVING SUM(c.units_shipped) > 0

              UNION

              SELECT p.po_number,
                     p.item_number,
                     p.po_line_number,
                     p.po_dtl_id,
                     c.units_shipped,
                     ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(i.amount_usd)) / SUM(c.units_shipped) AS cost
              FROM _po p
                       LEFT JOIN gsc.lc_cbm_data c
                                 ON p.po_dtl_id = c.po_dtl_id
                                     AND c.datapoint = 'po_sku_con_bl'
                       JOIN gsc.lc_cbm_data c1
                            ON c.bl = c1.bl
                                AND c1.datapoint = 'con_bl'
                       JOIN gsc.lc_invoice_data i
                            ON c.bl = i.bl
                                AND i.field_name = 'OTR_FREIGHT_DATA_GG'
              GROUP BY p.po_number,
                       p.item_number,
                       p.po_line_number,
                       p.po_dtl_id,
                       c.units_shipped
              HAVING SUM(c1.cbm) > 0
                 AND SUM(c.units_shipped) > 0

              UNION

--Bleckmann OTR
              SELECT p.po_number,
                     p.item_number,
                     p.po_line_number,
                     p.po_dtl_id,
                     SUM(c.units_shipped)                                                     AS units_shipped,
                     ((SUM(c.cbm) / SUM(c1.cbm)) * SUM(c1.amount_usd)) / SUM(c.units_shipped) AS cost
              FROM _po p
                       JOIN gsc.lc_cbm_data c
                            ON p.po_dtl_id = c.po_dtl_id
                                AND c.datapoint = 'bleckmann_po_sku_bu_bl'
                       JOIN gsc.lc_cbm_data c1
                            ON c.bl = c1.bl
                                AND c.business_unit = c1.business_unit
                                AND c1.datapoint = 'bleckmann_otr_bl_bu'
              GROUP BY p.po_number,
                       p.item_number,
                       p.po_line_number,
                       p.po_dtl_id
              HAVING SUM(c1.cbm) > 0
                 AND SUM(c.units_shipped) > 0) q1
        GROUP BY po_number,
                 item_number,
                 po_line_number,
                 po_dtl_id;


--REPORTING_BASE_PROD.GSC.LC_COST_OTR_DETAIL_DATASET
        INSERT INTO _cost
        (datapoint, proc, carrier, bl, invoice_number, po_number, item_number, po_line_number, po_dtl_id, units_shipped,
         cost)
        SELECT 'OTR_COST'                                           AS datapoint,
               'LC_COST_OTR_DETAIL_DATASET'                         AS proc,
               c.carrier,
               c.bl,
               c.invoice_number,
               c.po_number,
               c.item_number,
               c.po_line_number,
               c.po_dtl_id,
               units_shipped,
               SUM(c.cost * c.units_shipped) / SUM(c.units_shipped) AS cost
        FROM _po p
                 JOIN _cost c
                      ON p.po_dtl_id = c.po_dtl_id
        WHERE datapoint = 'OTR_COST'
          AND proc = 'LC_COST_OTR_ORDER_DETAIL_DATASET'
          AND c.cost > 0
        GROUP BY c.carrier,
                 c.bl,
                 c.invoice_number,
                 c.po_number,
                 c.item_number,
                 c.po_line_number,
                 c.po_dtl_id,
                 c.units_shipped
        HAVING SUM(c.units_shipped) > 0

        UNION

        SELECT 'OTR_COST'                                                 AS datapoint,
               'LC_COST_OTR_DETAIL_DATASET'                               AS proc,
               i.carrier,
               c1.bl,
               i.invoice_number,
               c.po_number,
               c.item_number,
               c.po_line_number,
               c.po_dtl_id,
               c.units_shipped,
               SUM(c.amount_usd * c.units_shipped) / SUM(c.units_shipped) AS cost
        FROM _po p
                 JOIN gsc.lc_cbm_data c
                      ON p.po_dtl_id = c.po_dtl_id
                          AND c.datapoint = 'bleckmann_po_sku_bu_bl_cbm_otr'
                 LEFT JOIN gsc.lc_cbm_data c1
                           ON c.po_dtl_id = c1.po_dtl_id
                               AND c.business_unit = c1.business_unit
                               AND c.bl = c1.bl
                               AND c1.datapoint = 'bleckmann_po_sku_bu_bl'
                 LEFT JOIN gsc.lc_cbm_data c2
                           ON c.po_dtl_id = c1.po_dtl_id
                               AND c1.bl = c2.bl
                               AND c.business_unit = c2.business_unit
                               AND c2.datapoint = 'bleckmann_otr_bl_bu'
                 JOIN gsc.lc_invoice_data i
                      ON c1.bl = i.bl
                          AND i.field_name = 'OTR_FREIGHT_DETAIL'
                          AND i.amount_usd > 0
        GROUP BY i.carrier,
                 c1.bl,
                 i.invoice_number,
                 c.po_number,
                 c.item_number,
                 c.po_line_number,
                 c.po_dtl_id,
                 c.units_shipped

        HAVING SUM(c.units_shipped) > 0;


        CREATE OR REPLACE TEMPORARY TABLE _stg_lc_cost_data AS
        SELECT po_number,
               item_number,
               po_line_number,
               po_dtl_id,
               units_shipped,
               proc,
               air_freight_cost,
               air_other_cost,
               domestic_cost,
               ocean_freight_cost,
               ocean_other_cost,
               otr_cost,
               pier_pass_cost,
               transload_cost
        FROM (SELECT c.po_number,
                     c.item_number,
                     c.po_line_number,
                     c.po_dtl_id,
                     c.units_shipped,
                     c.proc,
                     c.datapoint,
                     c.cost
              FROM _cost c
              WHERE proc IN (
                             'LC_COST_OCEAN_DATASET',
                             'LC_COST_AIR_DATASET',
                             'LC_COST_DOMESTIC_DATASET',
                             'LC_COST_TRANSLOAD_DATASET',
                             'LC_COST_OTR_DATASET',
                             'LC_COST_PIER_PASS_DATASET'
                  ))
                 PIVOT (SUM(cost) FOR datapoint IN
                (
                'AIR_FREIGHT_COST',
                'AIR_OTHER_COST',
                'DOMESTIC_COST',
                'OCEAN_FREIGHT_COST',
                'OCEAN_OTHER_COST',
                'OTR_COST',
                'PIER_PASS_COST',
                'TRANSLOAD_COST'
                )) p
                 (
                  po_number,
                  item_number,
                  po_line_number,
                  po_dtl_id,
                  units_shipped,
                  proc,
                  air_freight_cost,
                  air_other_cost,
                  domestic_cost,
                  ocean_freight_cost,
                  ocean_other_cost,
                  otr_cost,
                  pier_pass_cost,
                  transload_cost
                     );


        MERGE INTO
            gsc.lc_cost_data tgt
--reporting_base_prod.gsc.lc_cost_data tgt
            USING _stg_lc_cost_data src
            ON tgt.po_dtl_id = src.po_dtl_id
                AND tgt.proc = src.proc
            WHEN MATCHED THEN UPDATE
                SET tgt.units_shipped = src.units_shipped,
--tgt.PROC =  src.PROC,
                    tgt.air_freight_cost = src.air_freight_cost,
                    tgt.air_other_cost = src.air_other_cost,
                    tgt.domestic_cost = src.domestic_cost,
                    tgt.ocean_freight_cost = src.ocean_freight_cost,
                    tgt.ocean_other_cost = src.ocean_other_cost,
                    tgt.otr_cost = src.otr_cost,
                    tgt.pier_pass_cost = src.pier_pass_cost,
                    tgt.transload_cost = src.transload_cost,
                    tgt.meta_update_datetime = CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_LTZ(9)),
                    tgt.po_line_number = src.po_line_number
            WHEN NOT MATCHED THEN INSERT
                (
                 po_dtl_id,
                 po_number,
                 item_number,
                 po_line_number,
                 units_shipped,
                 proc,
                 air_freight_cost,
                 air_other_cost,
                 domestic_cost,
                 ocean_freight_cost,
                 ocean_other_cost,
                 otr_cost,
                 pier_pass_cost,
                 transload_cost
                    )
                VALUES (src.po_dtl_id,
                        src.po_number,
                        src.item_number,
                        src.po_line_number,
                        src.units_shipped,
                        src.proc,
                        src.air_freight_cost,
                        src.air_other_cost,
                        src.domestic_cost,
                        src.ocean_freight_cost,
                        src.ocean_other_cost,
                        src.otr_cost,
                        src.pier_pass_cost,
                        src.transload_cost);

    END;
