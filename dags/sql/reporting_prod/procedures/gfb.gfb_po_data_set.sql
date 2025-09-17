CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb_po_data_set AS
SELECT DISTINCT CASE
                    WHEN pdd.warehouse_id IN (221, 366) AND UPPER(pdd.division_id) = 'FABKIDS' THEN 'JUSTFAB'
                    ELSE UPPER(pdd.division_id)
    END                                                                                                         AS business_unit
              , (CASE
                     WHEN pdd.warehouse_id = 107 THEN 'NA'
                     WHEN pdd.warehouse_id = 109 THEN 'NA'
                     WHEN pdd.warehouse_id = 154 THEN 'NA'
                     WHEN pdd.warehouse_id = 231 THEN 'NA'
                     WHEN pdd.warehouse_id = 421 THEN 'NA'
                     WHEN pdd.warehouse_id = 221 THEN 'EU'
                     WHEN pdd.warehouse_id = 366 THEN 'EU'
                     WHEN pdd.warehouse_id = 466 THEN 'NA'
    END)                                                                                                        AS region
              , (CASE
                     WHEN pdd.warehouse_id = 107 THEN 'US'
                     WHEN pdd.warehouse_id = 109 THEN 'CA'
                     WHEN pdd.warehouse_id = 154 THEN 'US'
                     WHEN pdd.warehouse_id = 231 THEN 'US'
                     WHEN pdd.warehouse_id = 421 THEN 'US'
                     WHEN pdd.warehouse_id = 221 THEN 'FR'
                     WHEN pdd.warehouse_id = 366 THEN 'UK'
                     WHEN pdd.warehouse_id = 466 THEN 'US'
    END)                                                                                                        AS country
              , (CASE
                     WHEN pdd.warehouse_id = 107 THEN 'KENTUCKY'
                     WHEN pdd.warehouse_id = 109 THEN 'CANADA'
                     WHEN pdd.warehouse_id = 154 THEN 'KENTUCKY'
                     WHEN pdd.warehouse_id = 231 THEN 'PERRIS'
                     WHEN pdd.warehouse_id = 421 THEN 'KENTUCKY'
                     WHEN pdd.warehouse_id = 221 THEN 'NETHERLANDS'
                     WHEN pdd.warehouse_id = 366 THEN 'UK'
                     WHEN pdd.warehouse_id = 466 THEN 'TIJUANA'
    END)                                                                                                        AS warehouse
              , pdd.warehouse_id
              , ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(pdd.sku, '-'), 0, -1), '-')                                   AS product_sku
              , pdd.sku
              , pdd.po_number
              , pdd.po_type
              , DATE_FROM_PARTS(SPLIT(pdd.show_room, '-')[0], SPLIT(pdd.show_room, '-')[1],
                                1)                                                                              AS show_room

              --Date
              , pdd.fc_delivery                                                                                 AS expected_deliver_to_warehouse_date
              , rdd.receipt_datetime
              , DATE_TRUNC(DAY, rdd.receipt_datetime)                                                           AS date_received
              , FIRST_VALUE(pul."ETA on Hand")
                            OVER (PARTITION BY pul."Style-Color",pul.po ORDER BY "ETA on Hand" DESC)                    AS arrival_date
              , FIRST_VALUE(pul."fnd/eta") OVER (PARTITION BY pul."Style-Color",pul.po ORDER BY "ETA on Hand" DESC)     AS "fnd/eta"

              , pdd.total_qty                                                                                   AS po_qty
              , COALESCE(rdd.received, 0)                                                                       AS qty_receipt

              , pdd.weighted_lc_act                                                                             AS landed_cost
              , DATE_TRUNC(MONTH, pdd.date_launch)                                                              AS launch_date
              , pdd.date_launch                                                                                 AS exact_launch_date
              , FIRST_VALUE(pul.description) OVER (PARTITION BY pul."Style-Color",pul.po ORDER BY "ETA on Hand" DESC)   AS description
FROM reporting_prod.gsc.po_detail_dataset_agg pdd
         LEFT JOIN reporting_prod.gfc.receipt_detail_dataset rdd
                   ON rdd.po_number = pdd.po_number
                       AND rdd.item_number = pdd.sku
         LEFT JOIN reporting_prod.gsc.pulse_dataset pul
                   ON pul.po = pdd.po_number
                       AND pul."Style-Color" = pdd.sku
WHERE pdd.style_name != 'TBD'
  AND UPPER(pdd.region_id) IN ('US', 'CA', 'EU')
  AND UPPER(pdd.division_id) IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS');
