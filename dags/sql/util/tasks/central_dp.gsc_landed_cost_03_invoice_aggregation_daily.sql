CREATE OR REPLACE TASK util.tasks_central_dp.gsc_landed_cost_03_invoice_aggregation_daily
    WAREHOUSE =da_wh_analytics
    AFTER util.tasks_central_dp.gsc_landed_cost_02_cbm_aggregation_daily
    AS BEGIN

        /*** 32 scripts ***/

        USE SCHEMA reporting_base_prod.gsc;

        CREATE OR REPLACE TEMPORARY TABLE _invoice AS
        SELECT i.invoice_number,
               i.post_advice,
               UPPER(TRIM(i.carrier))                   AS carrier,
               UPPER(TRIM(i.container))                 AS container,
               CASE
                   WHEN UPPER(TRIM(i.traffic_mode)) IN ('OCEAN', 'SEA') OR i.traffic_mode IS NULL
                       THEN FIRST_VALUE(UPPER(TRIM(i.traffic_mode))) OVER (PARTITION BY container ORDER BY CASE
                                                                                                               WHEN UPPER(TRIM(i.carrier)) = 'OOCL'
                                                                                                                   THEN 1
                                                                                                               WHEN UPPER(TRIM(i.carrier)) = 'CMI'
                                                                                                                   THEN 2
                                                                                                               ELSE 3 END ASC)
                   ELSE UPPER(TRIM(i.traffic_mode)) END AS traffic_mode,
               UPPER(TRIM(i.bl))                        AS bl,
               i.cbm,
               i.weight,
               UPPER(TRIM(i.po_number))                    po_number,
               CASE
                   /*WHEN UPPER(TRIM(i.carrier)) IN ('BLECKMANN') AND UPPER(TRIM(i.description)) = 'CROSSDOCK'
                       THEN 'OTR' -- Takes into Account for Bleckmann Crossdock data */
                   WHEN UPPER(TRIM(i.carrier)) IN ('BLECKMANN', 'CMA', 'CMA CGM', 'CMDU', 'EVERGREEN', 'EVE', 'MSC')
                       THEN 'OCEAN FREIGHT' -- Do not change this
                   WHEN UPPER(TRIM(i.carrier)) IN ('EFL', 'RCS', 'RCS LOGISTICS') AND
                        (UPPER(TRIM(traffic_mode)) LIKE 'OCEAN%' OR UPPER(TRIM(traffic_mode)) LIKE 'CFS%' OR
                         UPPER(TRIM(traffic_mode)) LIKE 'CY%' OR UPPER(TRIM(traffic_mode)) LIKE 'SEA%')
                       THEN 'OCEAN FREIGHT'
                   WHEN UPPER(TRIM(i.carrier)) IN ('EFL', 'RCS', 'RCS LOGISTICS') AND
                        UPPER(TRIM(traffic_mode)) LIKE 'AIR%'
                       THEN 'AIR FREIGHT'
                   WHEN UPPER(TRIM(i.carrier)) = 'PIERPASS'
                       THEN 'PIER PASS'
                   WHEN UPPER(TRIM(i.carrier)) IN ('CMI', 'GOLDPOINT', 'PLC')
                       THEN 'TRANSLOAD'
                   WHEN UPPER(TRIM(i.carrier)) IN ('SWIFT', 'NTG', 'TQL')
                       THEN 'OTR'
                   WHEN UPPER(TRIM(i.carrier)) IN ('JAG')
                       THEN 'DOMESTIC'
                   WHEN UPPER(TRIM(i.carrier)) = 'OOCL'
                       THEN UPPER(TRIM(i.description))
                   WHEN UPPER(TRIM(i.carrier)) = 'G-GLOBAL' AND x.charge_bucket IS NULL
                       THEN 'UNKNOWN'
                   ELSE UPPER(TRIM(COALESCE(x.charge_bucket, i.description)))
                   END                                  AS description,
               i.amount_usd
        FROM lake_view.gsc.primary_carrier_invoice i --reporting_base_prod.gsc.primary_carrier_invoice i
                 LEFT JOIN reporting_base_prod.reference.g_global_invoice_codes x
                           ON i.charge_code IS NOT NULL
                               AND i.charge_code = x.charge_code
        WHERE 1 = 1
          AND i.invoice_date >= '2023-01-01'
--and i.meta_update_datetime >= '2024-01-01'  --dateadd(day, -120, current_date)
          AND NULLIF(i.invoice_number, '') IS NOT NULL

        UNION ALL

        SELECT cic.invoice_number,
               NULL                        AS post_advice,
               'BLECKMANN'                 AS carrier,
               cic.plates                  AS container,
               NULL                        AS traffic_mode,
               TO_VARCHAR(cic.load_number) AS bl,
               NULL                        AS cbm,
               NULL                        AS weight,
               UPPER(cic.business_unit)    AS po_number,
               'OTR'                       AS description,
               cic.total * c.exchange_rate AS amount_usd
        FROM lake_view.excel.carrier_invoice_crossdock cic
                 LEFT JOIN edw_prod.reference.currency_exchange_rate c
                           ON cic.invoice_date = TO_DATE(c.rate_datetime)
                               AND TRIM(UPPER(c.src_currency)) = 'EUR' AND TRIM(UPPER(c.dest_currency)) = 'USD'
        WHERE NULLIF(cic.invoice_number, '') IS NOT NULL;

        CREATE OR REPLACE TEMPORARY TABLE _yard AS
        SELECT DISTINCT UPPER(TRIM(order_number)) AS order_number FROM gsc.yard_container_data;

        UPDATE _invoice
        SET description = 'OTR'
        FROM _yard y
        WHERE _invoice.bl = y.order_number
          AND _invoice.carrier = 'PLC';


        CREATE OR REPLACE TEMPORARY TABLE _grouped
        (
            bl             VARCHAR(80000) DEFAULT 'null',
            carrier        VARCHAR(255)   DEFAULT 'null',
            invoice_number VARCHAR(255)   DEFAULT 'null',
            container      VARCHAR(255)   DEFAULT 'null',
            post_advice    VARCHAR(255)   DEFAULT 'null',
            field_name     VARCHAR(50)    DEFAULT 'null',
            business_unit  VARCHAR(25)    DEFAULT 'null',
            amount_usd     FLOAT          DEFAULT 0
        );

        INSERT INTO _grouped
            (bl, carrier, invoice_number, field_name, amount_usd)
        SELECT bl, carrier, invoice_number, 'AIR_FREIGHT_DETAIL', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'AIR FREIGHT'
        GROUP BY bl, carrier, invoice_number;

        INSERT INTO _grouped
            (bl, field_name, amount_usd)
        SELECT bl, 'AIR_FREIGHT_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'AIR FREIGHT'
        GROUP BY bl;

        INSERT INTO _grouped
            (bl, carrier, invoice_number, field_name, amount_usd)
        SELECT bl, carrier, invoice_number, 'AIR_OTHER_DETAIL', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description NOT IN ('AIR FREIGHT', 'TRANSLOAD', 'PIER PASS', 'DOMESTIC', 'OTR', 'UNKNOWN', 'DUTY')
        GROUP BY bl, carrier, invoice_number;


        INSERT INTO _grouped
            (bl, field_name, amount_usd)
        SELECT bl, 'AIR_OTHER_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description NOT IN
              ('AIR FREIGHT', 'TRANSLOAD', 'PIER PASS', 'DOMESTIC', 'OTR', 'UNKNOWN', 'DUTY', 'OTR/TRANSLOAD')
          AND bl IS NOT NULL
        GROUP BY bl;

        INSERT INTO _grouped
            (bl, field_name, amount_usd)
        SELECT bl, 'AIR_OTR_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'OTR/TRANSLOAD'
          AND carrier = 'G-GLOBAL'
          AND UPPER(TRIM(traffic_mode)) ILIKE '%AIR%'
          AND bl IS NOT NULL
        GROUP BY bl;

        INSERT INTO _grouped
            (post_advice, carrier, invoice_number, field_name, amount_usd)
        SELECT bl                          AS post_advice,
               carrier,
               invoice_number,
               'DOMESTIC_FREIGHT_DETAIL',
               SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'DOMESTIC'
        GROUP BY bl, carrier, invoice_number;

        INSERT INTO _grouped
            (post_advice, field_name, amount_usd)
        SELECT bl AS post_advice, 'DOMESTIC_FREIGHT_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'DOMESTIC'
          AND bl IS NOT NULL
        GROUP BY bl;

        INSERT INTO _grouped
        (bl, carrier, invoice_number, post_advice, container, field_name, amount_usd)
        SELECT bl,
               carrier,
               invoice_number,
               post_advice,
               container,
               'DUTY_DATA',
               SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'DUTY'
          AND carrier = 'G-GLOBAL'
        GROUP BY bl, carrier, invoice_number, post_advice, container;

        INSERT INTO _grouped
            (container, field_name, amount_usd)
        SELECT container, 'OCEAN_FREIGHT_ALT_2_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'OCEAN FREIGHT'
          AND carrier IN ('EFL', 'RCS', 'RCS LOGISTICS', 'CMA', 'CMA CGM', 'CMDU', 'OOCL')
          AND container IS NOT NULL
        GROUP BY container;

        INSERT INTO _grouped
            (carrier, invoice_number, container, field_name, amount_usd)
        SELECT carrier,
               invoice_number,
               container,
               'OCEAN_FREIGHT_ALT_2_DETAIL',
               SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'OCEAN FREIGHT'
          AND carrier IN ('EFL', 'RCS', 'RCS LOGISTICS', 'CMA', 'CMA CGM', 'CMDU')
        GROUP BY carrier, invoice_number, container;

        INSERT INTO _grouped
            (bl, container, field_name, amount_usd)
        SELECT bl, container, 'OCEAN_FREIGHT_ALT_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'OCEAN FREIGHT'
        GROUP BY bl, container;

        INSERT INTO _grouped
            (bl, carrier, invoice_number, container, field_name, amount_usd)
        SELECT bl,
               carrier,
               invoice_number,
               container,
               'OCEAN_FREIGHT_ALT_DETAIL',
               SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'OCEAN FREIGHT'
        GROUP BY bl, carrier, invoice_number, container;

        INSERT INTO _grouped
            (container, post_advice, field_name, amount_usd)
        SELECT container, post_advice, 'OCEAN_FREIGHT_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'OCEAN FREIGHT'
        GROUP BY container, post_advice;

        INSERT INTO _grouped
        (carrier, invoice_number, container, post_advice, field_name, amount_usd)
        SELECT carrier,
               invoice_number,
               container,
               post_advice,
               'OCEAN_FREIGHT_DETAIL',
               SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'OCEAN FREIGHT'
        GROUP BY carrier, invoice_number, container, post_advice;

        INSERT INTO _grouped
            (bl, container, field_name, amount_usd)
        SELECT bl, container, 'OCEAN_OTHER_ALT_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description NOT IN ('OCEAN FREIGHT', 'TRANSLOAD', 'PIER PASS', 'DOMESTIC', 'OTR', 'UNKNOWN', 'DUTY')
        GROUP BY bl, container;

        INSERT INTO _grouped
            (bl, carrier, invoice_number, container, field_name, amount_usd)
        SELECT bl,
               carrier,
               invoice_number,
               container,
               'OCEAN_OTHER_ALT_DETAIL',
               SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description NOT IN ('OCEAN FREIGHT', 'TRANSLOAD', 'PIER PASS', 'DOMESTIC', 'OTR', 'UNKNOWN', 'DUTY')
        GROUP BY bl, carrier, invoice_number, container;

        INSERT INTO _grouped
            (container, post_advice, field_name, amount_usd)
        SELECT container, post_advice, 'OCEAN_OTHER_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description NOT IN ('OCEAN FREIGHT', 'TRANSLOAD', 'PIER PASS', 'DOMESTIC', 'OTR', 'UNKNOWN', 'DUTY')
        GROUP BY container, post_advice;

        INSERT INTO _grouped
        (carrier, invoice_number, container, post_advice, field_name, amount_usd)
        SELECT carrier,
               invoice_number,
               container,
               post_advice,
               'OCEAN_OTHER_DETAIL',
               SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description NOT IN ('OCEAN FREIGHT', 'TRANSLOAD', 'PIER PASS', 'DOMESTIC', 'OTR', 'UNKNOWN', 'DUTY')
        GROUP BY carrier, invoice_number, container, post_advice;

        INSERT INTO _grouped
            (bl, field_name, amount_usd)
        SELECT bl, 'OTR_FREIGHT_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description IN ('OTR', 'OTR/TRANSLOAD')
        GROUP BY bl;


        INSERT INTO _grouped
            (bl, field_name, amount_usd)
        SELECT bl, 'OTR_FREIGHT_DATA_GG', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description IN ('OTR', 'OTR/TRANSLOAD')
          AND carrier = 'G-GLOBAL'
        GROUP BY bl;

        INSERT INTO _grouped
            (bl, carrier, invoice_number, field_name, amount_usd)
        SELECT bl, carrier, invoice_number, 'OTR_FREIGHT_DETAIL', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'OTR'
        GROUP BY bl, carrier, invoice_number;

        INSERT INTO _grouped
            (container, field_name, amount_usd)
        SELECT container, 'PEIR_PASS_FREIGHT_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'PIER PASS'
          AND container IS NOT NULL
        GROUP BY container;

        INSERT INTO _grouped
            (carrier, invoice_number, container, field_name, amount_usd)
        SELECT carrier, invoice_number, container, 'PEIR_PASS_FREIGHT_DETAIL', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'PIER PASS'
        GROUP BY carrier, invoice_number, container;

        INSERT INTO _grouped
            (container, field_name, amount_usd)
        SELECT container, 'TRANSLOAD_FREIGHT_DATA', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description IN ('TRANSLOAD', 'OTR/TRANSLOAD')
          AND container IS NOT NULL
        GROUP BY container;

        INSERT INTO _grouped
            (carrier, invoice_number, container, field_name, amount_usd)
        SELECT carrier, invoice_number, container, 'TRANSLOAD_FREIGHT_DETAIL', SUM(ZEROIFNULL(amount_usd)) AS amount_usd
        FROM _invoice
        WHERE description = 'TRANSLOAD'
        GROUP BY carrier, invoice_number, container;

        INSERT INTO _grouped
            (bl, carrier, invoice_number, business_unit, field_name, amount_usd)
        SELECT bl,
               carrier,
               invoice_number,
               po_number       AS business_unit,
               'BLECKMANN_BU'  AS field_name,
               SUM(amount_usd) AS amount_usd
        FROM _invoice
        WHERE carrier = 'BLECKMANN'
          AND description = 'OTR'
        GROUP BY bl, carrier, invoice_number, po_number;

        MERGE INTO gsc.lc_invoice_data tgt
            USING _grouped src
            ON NVL(tgt.bl, 'unk') = NVL(src.bl, 'unk')
                AND NVL(tgt.carrier, 'unk') = NVL(src.carrier, 'unk')
                AND NVL(tgt.invoice_number, 'unk') = NVL(src.invoice_number, 'unk')
                AND NVL(tgt.container, 'unk') = NVL(src.container, 'unk')
                AND NVL(tgt.post_advice, 'unk') = NVL(src.post_advice, 'unk')
                AND NVL(tgt.field_name, 'unk') = NVL(src.field_name, 'unk')
                AND NVL(tgt.business_unit, 'unk') = NVL(src.business_unit, 'unk')
            WHEN MATCHED THEN UPDATE
                SET tgt.amount_usd = src.amount_usd,
                    tgt.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(9)
            WHEN NOT MATCHED THEN INSERT
                (bl, carrier, invoice_number, container, post_advice, field_name, business_unit, amount_usd)
                VALUES (src.bl,
                        src.carrier,
                        src.invoice_number,
                        src.container,
                        src.post_advice,
                        src.field_name,
                        src.business_unit,
                        src.amount_usd);

        /**************Post advice update*********************/

/****find all pa_con********/
        CREATE OR REPLACE TEMPORARY TABLE _inv AS
        SELECT DISTINCT container, post_advice
        FROM lc_invoice_data
        WHERE NVL(post_advice, 'null') != 'null';

/********find con with 2 or more pa*******/
        CREATE OR REPLACE TEMPORARY TABLE _con AS
        SELECT container, COUNT(1) rn
        FROM _inv
        GROUP BY container
        HAVING COUNT(1) > 1;

/********find ocean freight of all 2 pa containers **********/
        CREATE OR REPLACE TEMPORARY TABLE _ocean_freight AS
        SELECT DISTINCT lcd.*
        FROM _inv i
                 JOIN _con c
                      ON i.container = c.container
                 JOIN lake_view.gsc.primary_carrier_invoice pci
                      ON i.container = pci.container
                          AND i.post_advice = pci.post_advice
                 JOIN lc_invoice_data lcd
                      ON i.container = lcd.container
                          AND i.post_advice = lcd.post_advice
        WHERE pci.invoice_date >= '2023-01-01'
          AND lcd.field_name = 'OCEAN_FREIGHT_DATA'
        ORDER BY container;

/**********find PAs missing data*************/
        CREATE OR REPLACE TEMPORARY TABLE _bad AS
        SELECT DISTINCT i.container, i.post_advice
        FROM _inv i
                 JOIN _con c
                      ON i.container = c.container
                 JOIN lake_view.gsc.primary_carrier_invoice pci
                      ON i.container = pci.container
                          AND i.post_advice = pci.post_advice
                 LEFT JOIN lc_invoice_data lcd
                           ON i.container = lcd.container
                               AND i.post_advice = lcd.post_advice
                               AND lcd.field_name = 'OCEAN_FREIGHT_DATA'
        WHERE 1 = 1
          AND pci.invoice_date >= '2023-01-01'
          AND NVL(lcd.field_name, 'bad') = 'bad'

        ORDER BY container;

/************merge data**********/
        CREATE OR REPLACE TEMPORARY TABLE _stg AS
        SELECT DISTINCT oc.bl,
                        oc.carrier,
                        oc.invoice_number,
                        oc.container,
                        b.post_advice,
                        oc.field_name,
                        oc.amount_usd,
                        oc.business_unit
        FROM _bad b
                 JOIN _ocean_freight oc
                      ON b.container = oc.container;

        MERGE INTO gsc.lc_invoice_data tgt
            USING _stg src
            ON NVL(tgt.bl, 'unk') = NVL(src.bl, 'unk')
                AND NVL(tgt.carrier, 'unk') = NVL(src.carrier, 'unk')
                AND NVL(tgt.invoice_number, 'unk') = NVL(src.invoice_number, 'unk')
                AND NVL(tgt.container, 'unk') = NVL(src.container, 'unk')
                AND NVL(tgt.post_advice, 'unk') = NVL(src.post_advice, 'unk')
                AND NVL(tgt.field_name, 'unk') = NVL(src.field_name, 'unk')
                AND NVL(tgt.business_unit, 'unk') = NVL(src.business_unit, 'unk')
            WHEN NOT MATCHED THEN INSERT
                (bl, carrier, invoice_number, container, post_advice, field_name, business_unit, amount_usd)
                VALUES (src.bl,
                        src.carrier,
                        src.invoice_number,
                        src.container,
                        src.post_advice,
                        src.field_name,
                        src.business_unit,
                        src.amount_usd);


        /*****************BL update****************/
/****find all bl_con********/
        CREATE OR REPLACE TEMPORARY TABLE _invb AS
        SELECT DISTINCT container, bl
        FROM lc_invoice_data
        WHERE NVL(bl, 'null') != 'null'
          AND NVL(container, 'null') != 'null';


/********find con with 2 or more pa*******/
        CREATE OR REPLACE TEMPORARY TABLE _bl AS
        SELECT container, COUNT(1) rn
        FROM _invb
        GROUP BY container
        HAVING COUNT(1) > 1;

/********find ocean freight of all 2 BL containers **********/
        CREATE OR REPLACE TEMPORARY TABLE _ocean_freightb AS
        SELECT DISTINCT lcd.*
        FROM _invb i
                 JOIN _bl b
                      ON i.container = b.container
                 JOIN lake_view.gsc.primary_carrier_invoice pci
                      ON i.container = pci.container
                          AND i.bl = pci.bl
                 JOIN lc_invoice_data lcd
                      ON i.container = lcd.container
                          AND i.bl = lcd.bl
        WHERE pci.invoice_date >= '2023-01-01'
          AND lcd.field_name = 'OCEAN_FREIGHT_ALT_DATA'
        ORDER BY container;

/**********find BLs missing data*************/
        CREATE OR REPLACE TEMPORARY TABLE _badb AS
        SELECT DISTINCT i.container, i.bl
        FROM _invb i
                 JOIN _bl b
                      ON i.container = b.container
                 JOIN lake_view.gsc.primary_carrier_invoice pci
                      ON i.container = pci.container
                          AND i.bl = pci.bl
                 LEFT JOIN lc_invoice_data lcd
                           ON i.container = lcd.container
                               AND i.bl = lcd.bl
                               AND lcd.field_name = 'OCEAN_FREIGHT_ALT_DATA'
        WHERE 1 = 1
          AND pci.invoice_date >= '2023-01-01'
          AND NVL(lcd.field_name, 'bad') = 'bad'

        ORDER BY container;

/************merge data**********/
        CREATE OR REPLACE TEMPORARY TABLE _stgb AS
        SELECT DISTINCT b.bl,
                        oc.carrier,
                        oc.invoice_number,
                        oc.container,
                        oc.post_advice,
                        oc.field_name,
                        oc.amount_usd,
                        oc.business_unit
        FROM _badb b
                 JOIN _ocean_freightb oc
                      ON b.container = oc.container;

        MERGE INTO gsc.lc_invoice_data tgt
            USING _stgb src
            ON NVL(tgt.bl, 'unk') = NVL(src.bl, 'unk')
                AND NVL(tgt.carrier, 'unk') = NVL(src.carrier, 'unk')
                AND NVL(tgt.invoice_number, 'unk') = NVL(src.invoice_number, 'unk')
                AND NVL(tgt.container, 'unk') = NVL(src.container, 'unk')
                AND NVL(tgt.post_advice, 'unk') = NVL(src.post_advice, 'unk')
                AND NVL(tgt.field_name, 'unk') = NVL(src.field_name, 'unk')
                AND NVL(tgt.business_unit, 'unk') = NVL(src.business_unit, 'unk')
            WHEN NOT MATCHED THEN INSERT
                (bl, carrier, invoice_number, container, post_advice, field_name, business_unit, amount_usd)
                VALUES (src.bl,
                        src.carrier,
                        src.invoice_number,
                        src.container,
                        src.post_advice,
                        src.field_name,
                        src.business_unit,
                        src.amount_usd);


    END;
