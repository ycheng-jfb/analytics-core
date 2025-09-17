SET start_date_ly = '2023-11-03';
SET end_date_ly = '2024-01-01';
SET start_date_ty = '2022-10-31';
SET end_date_ty = CURRENT_DATE();
SET start_date_lly = '2022-11-04';
SET end_date_lly = '2023-01-01';
SET thanksgiving_ly = '2023-11-23';
SET thanksgiving_ty = '2024-11-28';
SET thanksgiving_lly = '2022-11-24';

CREATE OR REPLACE TEMPORARY TABLE _orders AS
SELECT olp.business_unit
     , olp.region
     , (CASE
            WHEN olp.order_type = 'ecom' THEN 'Guest'
            WHEN olp.order_type = 'vip activating' THEN 'Activating VIP'
            WHEN olp.order_type = 'vip repeat' THEN 'Repeat VIP'
    END)                                                                       AS order_type
     , (CASE
            WHEN olp.region = 'NA' THEN DATE_TRUNC(HOUR, fol.order_local_datetime)
            WHEN olp.region = 'EU' THEN DATE_TRUNC(HOUR, DATEADD(HOUR, 9, fol.order_local_datetime))
    END)                                                                       AS date

     , SUM(olp.order_line_subtotal)                                            AS total_subtotal
     , SUM(olp.total_discount)                                                 AS total_discount
     , SUM(olp.total_shipping_revenue)                                         AS total_shipping_revenue
     , SUM(olp.total_product_revenue_with_tariff + olp.total_shipping_revenue) AS product_revenue
     , COUNT(DISTINCT olp.order_id)                                            AS order_count
     , SUM(olp.total_qty_sold)                                                 AS units
     , SUM(olp.total_cogs)                                                     AS total_cogs
     , SUM(olp.cash_collected_amount)                                          AS cash_collected
     , IFF(SUM(olp.total_product_revenue) = 0, 0,
           (SUM(olp.total_product_revenue) - SUM(olp.cash_collected_amount)) /
           SUM(olp.total_product_revenue))                                     AS cash_percent_order
     , SUM(total_non_cash_credit_amount + total_cash_credit_amount)            AS token_product_revenue
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         JOIN edw_prod.data_model_jfb.fact_order_line fol
              ON fol.order_line_id = olp.order_line_id
WHERE (
        (olp.order_date >= $start_date_ly AND olp.order_date < $end_date_ly)
        OR
        (olp.order_date >= $start_date_lly AND olp.order_date < $end_date_lly)
    )
  AND olp.order_classification = 'product order'
GROUP BY olp.business_unit
       , olp.region
       , (CASE
              WHEN olp.order_type = 'ecom' THEN 'Guest'
              WHEN olp.order_type = 'vip activating' THEN 'Activating VIP'
              WHEN olp.order_type = 'vip repeat' THEN 'Repeat VIP'
    END)
       , (CASE
              WHEN olp.region = 'NA' THEN DATE_TRUNC(HOUR, fol.order_local_datetime)
              WHEN olp.region = 'EU' THEN DATE_TRUNC(HOUR, DATEADD(HOUR, 9, fol.order_local_datetime))
    END);


CREATE OR REPLACE TEMPORARY TABLE _orders_today AS
SELECT UPPER(ds.store_brand)                                                                          AS business_unit
     , UPPER(ds.store_region)                                                                         AS region
     , (CASE -- matching order_membership_sales_channel_L3 in new data warehouse naming convention
            WHEN act.order_id IS NOT NULL THEN 'Activating VIP'
            WHEN rep.order_id IS NOT NULL THEN 'Repeat VIP'
            ELSE 'Guest'
    END)                                                                                              AS order_type
     , (CASE
            WHEN ds.store_region = 'NA' THEN DATE_TRUNC(HOUR, o.datetime_added)
            WHEN ds.store_region = 'EU' THEN DATE_TRUNC(HOUR, DATEADD(HOUR, 9, o.datetime_added))
    END)                                                                                                 date
     , SUM(COALESCE(o.subtotal, 0) / (1 + COALESCE(vrh.rate, 0))) * IFNULL(AVG(ocr.exchange_rate), 1) AS total_subtotal
     , SUM(COALESCE(o.discount, 0) / (1 + COALESCE(vrh.rate, 0))) * IFNULL(AVG(ocr.exchange_rate), 1) AS total_discount
     , SUM(COALESCE(o.shipping, 0) / (1 + COALESCE(vrh.rate, 0))) * IFNULL(AVG(ocr.exchange_rate), 1) AS total_shipping
     , SUM(COALESCE(o.credit, 0) / (1 + COALESCE(vrh.rate, 0))) * IFNULL(AVG(ocr.exchange_rate), 1)   AS total_credit
     , (total_subtotal - total_discount + total_shipping - total_credit)                              AS cash_collected
     , (total_subtotal - total_discount + total_shipping)                                             AS product_revenue
     , COUNT(DISTINCT COALESCE(o.master_order_id, o.order_id))                                        AS order_count
     , SUM(COALESCE(units.units, 0))                                                                  AS units
     , MAX(o.datetime_added)                                                                          AS max_refresh_time
     , NULL                                                                                           AS total_cogs
     , IFF(product_revenue = 0, 0, (product_revenue - SUM(credit)) / product_revenue)                 AS cash_percent
     , SUM(COALESCE(o.credit, 0) / (1 + COALESCE(vrh.rate, 0))) *
       IFNULL(AVG(ocr.exchange_rate), 1)                                                              AS token_product_revenue
FROM lake_jfb_view.ultra_merchant."ORDER" o
         JOIN reporting_prod.gfb.vw_store ds
              ON ds.store_id = o.store_id
         LEFT JOIN edw_prod.reference.currency_exchange_rate_by_date ocr
                   ON ocr.src_currency = ds.store_currency --used to convert eu transactions to usd
                       AND TO_DATE(ocr.rate_date_pst) = DATE_TRUNC('day', TO_DATE(o.datetime_added)) AND
                      ocr.dest_currency = 'USD'
         LEFT JOIN lake_jfb.ultra_merchant.address AS a
                   ON a.address_id = o.shipping_address_id --used to remove eu vat from backed in order values
         LEFT JOIN edw_prod.reference.vat_rate_history AS vrh
                   ON REPLACE(vrh.country_code, 'GB', 'UK') = REPLACE(a.country_code, 'GB', 'UK')
                       AND vrh.expires_date > TO_DATE(o.datetime_added)
         JOIN
     (
         SELECT DISTINCT a.order_id
         FROM lake_jfb_view.ultra_merchant.order_classification a
                  JOIN lake_jfb_view.ultra_merchant."ORDER" o
                       ON o.order_id = a.order_id
                  JOIN reporting_prod.gfb.vw_store ds
                       ON ds.store_id = o.store_id
         WHERE a.order_type_id IN (33, 34, 23, 13, 32)
           AND (
                 (
                             ds.store_region = 'NA'
                         AND (
                                         DATE_TRUNC(DAY, o.datetime_added) = CURRENT_DATE()
                                     OR
                                         DATE_TRUNC(DAY, o.datetime_added) =
                                         DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                     OR
                                         DATE_TRUNC(DAY, o.datetime_added) =
                                         DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()),
                                                 $thanksgiving_lly)
                                 )
                     )
                 OR
                 (
                             ds.store_region = 'EU'
                         AND (
                                         DATE_TRUNC(DAY, DATEADD(HOUR, 9, o.datetime_added)) = CURRENT_DATE()
                                     OR
                                         DATE_TRUNC(DAY, DATEADD(HOUR, 9, o.datetime_added)) =
                                         DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                     OR
                                         DATE_TRUNC(DAY, DATEADD(HOUR, 9, o.datetime_added)) =
                                         DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()),
                                                 $thanksgiving_lly)
                                 )
                     )
             )
     ) oc ON oc.order_id = o.order_id
         LEFT JOIN lake_jfb_view.ultra_merchant.order_classification act
                   ON act.order_id = o.order_id
                       AND act.order_type_id = 23
         LEFT JOIN lake_jfb_view.ultra_merchant.order_classification rep
                   ON rep.order_id = o.order_id
                       AND rep.order_type_id = 34
         JOIN lake_jfb_view.ultra_merchant.customer c
              ON c.customer_id = o.customer_id
         LEFT JOIN lake_jfb_view.ultra_merchant.membership m
                   ON m.customer_id = c.customer_id
         LEFT JOIN lake_jfb_view.ultra_merchant.order_classification oct
                   ON oct.order_id = o.order_id
                       AND oct.order_type_id = 3
         LEFT JOIN
     (
         SELECT ol.order_id
              , COUNT(*) AS units
         FROM lake_jfb_view.ultra_merchant.order_line ol
                  JOIN lake_jfb_view.ultra_merchant.product_type pt
                       ON pt.product_type_id = ol.product_type_id
                  JOIN lake_jfb_view.ultra_merchant."ORDER" o
                       ON o.order_id = ol.order_id
                  JOIN lake_jfb_view.ultra_merchant.statuscode sc
                       ON sc.statuscode = ol.statuscode
                  JOIN reporting_prod.gfb.vw_store ds
                       ON ds.store_id = o.store_id
         WHERE ol.product_type_id != 14
           AND pt.is_free = 0
           AND sc.label != 'Cancelled'
           AND (
                 (
                             ds.store_region = 'NA'
                         AND (
                                         DATE_TRUNC(DAY, o.datetime_added) = CURRENT_DATE()
                                     OR
                                         DATE_TRUNC(DAY, o.datetime_added) =
                                         DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                     OR
                                         DATE_TRUNC(DAY, o.datetime_added) =
                                         DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()),
                                                 $thanksgiving_lly)
                                 )
                     )
                 OR
                 (
                             ds.store_region = 'EU'
                         AND (
                                         DATE_TRUNC(DAY, DATEADD(HOUR, 9, o.datetime_added)) = CURRENT_DATE()
                                     OR
                                         DATE_TRUNC(DAY, DATEADD(HOUR, 9, o.datetime_added)) =
                                         DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                     OR
                                         DATE_TRUNC(DAY, DATEADD(HOUR, 9, o.datetime_added)) =
                                         DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()),
                                                 $thanksgiving_lly)
                                 )
                     )
             )
         GROUP BY ol.order_id
     ) units ON units.order_id = o.order_id
WHERE (
        (
                    ds.store_region = 'NA'
                AND (
                                DATE_TRUNC(DAY, o.datetime_added) =
                                DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                            OR
                                DATE_TRUNC(DAY, o.datetime_added) =
                                DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
                        )
            )
        OR
        (
                    ds.store_region = 'EU'
                AND (
                                DATE_TRUNC(DAY, DATEADD(HOUR, 9, o.datetime_added)) =
                                DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                            OR
                                DATE_TRUNC(DAY, DATEADD(HOUR, 9, o.datetime_added)) =
                                DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
                        )
            )
    )
  AND oct.order_id IS NULL
  AND c.email NOT LIKE '%@test%'
  AND c.email NOT LIKE '%@example%'
  AND c.email NOT LIKE '%@fkqa'
  AND c.email NOT LIKE '%@ibinc'
  AND o.processing_statuscode NOT IN (2200, 2205, 2202)
GROUP BY UPPER(ds.store_brand)
       , UPPER(ds.store_region)
       , (CASE -- matching order_membership_sales_channel_L3 in new data warehouse naming convention
              WHEN act.order_id IS NOT NULL THEN 'Activating VIP'
              WHEN rep.order_id IS NOT NULL THEN 'Repeat VIP'
              ELSE 'Guest'
    END)
       , (CASE
              WHEN ds.store_region = 'NA' THEN DATE_TRUNC(HOUR, o.datetime_added)
              WHEN ds.store_region = 'EU' THEN DATE_TRUNC(HOUR, DATEADD(HOUR, 9, o.datetime_added))
    END);


CREATE OR REPLACE TEMPORARY TABLE _order_final AS
SELECT o.business_unit
     , o.region
     , o.date
     , 'Not Today'                                                                  AS data_set_filter
     , NULL                                                                         AS max_refresh_time
     , SUM(o.order_count)                                                           AS total_orders
     , SUM(o.units)                                                                 AS total_qty_sold
     , SUM(o.product_revenue)                                                       AS total_product_revenue
     , SUM(o.total_subtotal)                                                        AS total_subtotal
     , SUM(o.total_discount)                                                        AS total_discount
     , SUM(o.total_cogs)                                                            AS total_cogs
     , SUM(o.product_revenue) - SUM(o.total_cogs)                                   AS total_product_margin
     , SUM(o.total_shipping_revenue)                                                AS total_shipping_revenue

     , SUM(IFF(order_type = 'Activating VIP', o.order_count, 0))                    AS activating_orders
     , SUM(IFF(order_type = 'Activating VIP', o.units, 0))                          AS activating_qty_sold
     , SUM(IFF(order_type = 'Activating VIP', o.product_revenue, 0))                AS activating_product_revenue
     , SUM(IFF(order_type = 'Activating VIP', o.total_subtotal, 0))                 AS activating_subtotal
     , SUM(IFF(order_type = 'Activating VIP', o.total_discount, 0))                 AS activating_discount
     , SUM(IFF(order_type = 'Activating VIP', o.total_cogs, 0))                     AS activating_cogs
     , activating_product_revenue - activating_cogs                                 AS activating_product_margin
     , SUM(IFF(order_type = 'Activating VIP', o.total_shipping_revenue, 0))         AS activating_shipping_revenue
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.order_count, 0))            AS repeat_orders
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.units, 0))                  AS repeat_qty_sold
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.product_revenue, 0))        AS repeat_product_revenue
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.total_subtotal, 0))         AS repeat_subtotal
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.total_discount, 0))         AS repeat_discount
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.total_cogs, 0))             AS repeat_cogs
     , repeat_product_revenue - repeat_cogs                                         AS repeat_product_margin
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.total_shipping_revenue, 0)) AS repeat_shipping_revenue
     , SUM(IFF(order_type IN ('Guest'), o.order_count, 0))                          AS guest_orders
     , SUM(IFF(order_type IN ('Guest'), o.units, 0))                                AS guest_qty_sold
     , SUM(IFF(order_type IN ('Guest'), o.product_revenue, 0))                      AS guest_product_revenue
     , SUM(IFF(order_type IN ('Guest'), o.total_subtotal, 0))                       AS guest_subtotal
     , SUM(IFF(order_type IN ('Guest'), o.total_discount, 0))                       AS guest_discount
     , SUM(IFF(order_type IN ('Guest'), o.total_cogs, 0))                           AS guest_cogs
     , guest_product_revenue - guest_cogs                                           AS guest_product_margin
     , SUM(IFF(order_type IN ('Guest'), o.total_shipping_revenue, 0))               AS guest_shipping_revenue
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.cash_percent_order, 0))     AS repeat_cash_percent_order
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.cash_collected, 0))         AS repeat_cash_collected
     , SUM(token_product_revenue)                                                   AS token_product_revenue
FROM _orders o
GROUP BY o.business_unit
       , o.region
       , o.date
UNION
SELECT o.business_unit
     , o.region
     , o.date
     , 'Today'                                                               AS data_set_filter
     , MAX(max_refresh_time)                                                 AS max_refresh_time
     , SUM(o.order_count)                                                    AS total_orders
     , SUM(o.units)                                                          AS total_qty_sold
     , SUM(o.product_revenue)                                                AS total_product_revenue
     , SUM(o.total_subtotal)                                                 AS total_subtotal
     , SUM(o.total_discount)                                                 AS total_discount
     , SUM(o.total_cogs)                                                     AS total_cogs
     , SUM(o.product_revenue) - SUM(o.total_cogs)                            AS total_product_margin
     , 0                                                                     AS total_shipping_revenue
     , SUM(IFF(order_type = 'Activating VIP', o.order_count, 0))             AS activating_orders
     , SUM(IFF(order_type = 'Activating VIP', o.units, 0))                   AS activating_qty_sold
     , SUM(IFF(order_type = 'Activating VIP', o.product_revenue, 0))         AS activating_product_revenue
     , SUM(IFF(order_type = 'Activating VIP', o.total_subtotal, 0))          AS activating_subtotal
     , SUM(IFF(order_type = 'Activating VIP', o.total_discount, 0))          AS activating_discount
     , SUM(IFF(order_type = 'Activating VIP', o.total_cogs, 0))              AS activating_cogs
     , activating_product_revenue - activating_cogs                          AS activating_product_margin
     , 0                                                                     AS activating_shipping_revenue
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.order_count, 0))     AS repeat_orders
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.units, 0))           AS repeat_qty_sold
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.product_revenue, 0)) AS repeat_product_revenue
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.total_subtotal, 0))  AS repeat_subtotal
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.total_discount, 0))  AS repeat_discount
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.total_cogs, 0))      AS repeat_cogs
     , repeat_product_revenue - repeat_cogs                                  AS repeat_product_margin
     , 0                                                                     AS repeat_shipping_revenue
     , SUM(IFF(order_type IN ('Guest'), o.order_count, 0))                   AS guest_orders
     , SUM(IFF(order_type IN ('Guest'), o.units, 0))                         AS guest_qty_sold
     , SUM(IFF(order_type IN ('Guest'), o.product_revenue, 0))               AS guest_product_revenue
     , SUM(IFF(order_type IN ('Guest'), o.total_subtotal, 0))                AS guest_subtotal
     , SUM(IFF(order_type IN ('Guest'), o.total_discount, 0))                AS guest_discount
     , SUM(IFF(order_type IN ('Guest'), o.total_cogs, 0))                    AS guest_cogs
     , guest_product_revenue - guest_cogs                                    AS guest_product_margin
     , 0                                                                     AS guest_shipping_revenue
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.cash_percent, 0))    AS repeat_cash_percent_order
     , SUM(IFF(order_type IN ('Repeat VIP', 'Guest'), o.cash_collected, 0))  AS repeat_cash_collected
     , SUM(token_product_revenue)                                            AS token_product_revenue
FROM _orders_today o
GROUP BY o.business_unit
       , o.region
       , o.date;


CREATE OR REPLACE TEMPORARY TABLE _vips AS
SELECT UPPER(ds.store_brand)  AS                             business_unit
     , UPPER(ds.store_region) AS                             region
     , (CASE
            WHEN (
                        ds.store_region = 'NA'
                    AND (
                                    DATE_TRUNC(DAY, m.datetime_activated) = CURRENT_DATE()
                                OR DATE_TRUNC(DAY, m.datetime_activated) =
                                   DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                OR DATE_TRUNC(DAY, m.datetime_activated) =
                                   DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
                            )
                )
                THEN DATE_TRUNC(HOUR, m.datetime_activated)
            WHEN (
                        ds.store_region = 'EU'
                    AND (
                                    DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_activated)) = CURRENT_DATE()
                                OR DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_activated)) =
                                   DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                OR DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_activated)) =
                                   DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
                            )
                )
                THEN DATE_TRUNC(HOUR, DATEADD(HOUR, 9, m.datetime_activated))
            ELSE DATE_TRUNC(HOUR, m.datetime_activated) END) date
     , COUNT(1)               AS                             vips
     , MAX(m.datetime_added)  AS                             max_refresh_time
FROM lake_jfb_view.ultra_merchant.membership m
         JOIN lake_jfb_view.ultra_merchant.customer c
              ON c.customer_id = m.customer_id
         JOIN reporting_prod.gfb.vw_store ds
              ON ds.store_id = m.store_id
WHERE (
        (m.datetime_activated >= $start_date_ly AND m.datetime_activated < $end_date_ly)
        OR
        (m.datetime_activated >= $start_date_lly AND m.datetime_activated < $end_date_lly)
        OR
        DATE_TRUNC(DAY, m.datetime_activated) =
        DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
        OR
        DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_activated)) =
        DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
    )
  AND c.email NOT LIKE '%@test%'
  AND c.email NOT LIKE '%@example%'
  AND c.email NOT LIKE '%@fkqa'
GROUP BY UPPER(ds.store_brand)
       , UPPER(ds.store_region)
       , (CASE
              WHEN (
                          ds.store_region = 'NA'
                      AND (
                                      DATE_TRUNC(DAY, m.datetime_activated) = CURRENT_DATE()
                                  OR DATE_TRUNC(DAY, m.datetime_activated) =
                                     DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                  OR DATE_TRUNC(DAY, m.datetime_activated) =
                                     DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
                              )
                  )
                  THEN DATE_TRUNC(HOUR, m.datetime_activated)
              WHEN (
                          ds.store_region = 'EU'
                      AND (
                                      DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_activated)) = CURRENT_DATE()
                                  OR DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_activated)) =
                                     DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                  OR DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_activated)) =
                                     DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
                              )
                  )
                  THEN DATE_TRUNC(HOUR, DATEADD(HOUR, 9, m.datetime_activated))
              ELSE DATE_TRUNC(HOUR, m.datetime_activated) END);


CREATE OR REPLACE TEMPORARY TABLE _leads AS
SELECT UPPER(ds.store_brand)  AS                         business_unit
     , UPPER(ds.store_region) AS                         region
     , (CASE
            WHEN (
                        ds.store_region = 'NA'
                    AND (
                                    DATE_TRUNC(DAY, m.datetime_added) = CURRENT_DATE()
                                OR DATE_TRUNC(DAY, m.datetime_added) =
                                   DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                OR DATE_TRUNC(DAY, m.datetime_added) =
                                   DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
                            )
                )
                THEN DATE_TRUNC(HOUR, m.datetime_added)
            WHEN (
                        ds.store_region = 'EU'
                    AND (
                                    DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_added)) = CURRENT_DATE()
                                OR DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_added)) =
                                   DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                OR DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_added)) =
                                   DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
                            )
                )
                THEN DATE_TRUNC(HOUR, DATEADD(HOUR, 9, m.datetime_added))
            ELSE DATE_TRUNC(HOUR, m.datetime_added) END) date
     , COUNT(1)               AS                         leads
     , MAX(m.datetime_added)  AS                         max_refresh_time
FROM lake_jfb_view.ultra_merchant.membership m
         JOIN lake_jfb_view.ultra_merchant.customer c
              ON c.customer_id = m.customer_id
         JOIN reporting_prod.gfb.vw_store ds
              ON ds.store_id = m.store_id
WHERE (
        (m.datetime_added >= $start_date_ly AND m.datetime_added < $end_date_ly)
        OR
        (m.datetime_activated >= $start_date_lly AND m.datetime_activated < $end_date_lly)
        OR
        DATE_TRUNC(DAY, m.datetime_added) =
        DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
        OR
        DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_added)) =
        DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
    )
  AND c.email NOT LIKE '%@test%'
  AND c.email NOT LIKE '%@example%'
  AND c.email NOT LIKE '%@fkqa'
GROUP BY UPPER(ds.store_brand)
       , UPPER(ds.store_region)
       , (CASE
              WHEN (
                          ds.store_region = 'NA'
                      AND (
                                      DATE_TRUNC(DAY, m.datetime_added) = CURRENT_DATE()
                                  OR DATE_TRUNC(DAY, m.datetime_added) =
                                     DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                  OR DATE_TRUNC(DAY, m.datetime_added) =
                                     DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
                              )
                  )
                  THEN DATE_TRUNC(HOUR, m.datetime_added)
              WHEN (
                          ds.store_region = 'EU'
                      AND (
                                      DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_added)) = CURRENT_DATE()
                                  OR DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_added)) =
                                     DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_ly)
                                  OR DATE_TRUNC(DAY, DATEADD(HOUR, 9, m.datetime_added)) =
                                     DATEADD(DAY, DATEDIFF(DAY, $thanksgiving_ty, CURRENT_DATE()), $thanksgiving_lly)
                              )
                  )
                  THEN DATE_TRUNC(HOUR, DATEADD(HOUR, 9, m.datetime_added))
              ELSE DATE_TRUNC(HOUR, m.datetime_added) END);


CREATE OR REPLACE TEMPORARY TABLE _daily_forecast AS
SELECT (CASE
            WHEN fd.business_unit = 'JF' THEN 'JUSTFAB'
            WHEN fd.business_unit = 'SD' THEN 'SHOEDAZZLE'
            WHEN fd.business_unit = 'FK' THEN 'FABKIDS'
    END)                                             AS business_unit
     , fd.region
     , fd.date
     , MAX(CASE
               WHEN fd.order_type = 'Activating'
                   THEN fd.ty_promo_description END) AS ty_activating_promo_description
     , MAX(CASE
               WHEN fd.order_type = 'Repeat'
                   THEN fd.ty_promo_description END) AS ty_repeat_promo_description
     , MAX(CASE
               WHEN fd.order_type = 'Activating'
                   THEN fd.ly_promo_description END) AS ly_activating_promo_description
     , MAX(CASE
               WHEN fd.order_type = 'Repeat'
                   THEN fd.ly_promo_description END) AS ly_repeat_promo_description

     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.orders
               ELSE 0 END)                           AS forecast_total_orders
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.orders
               ELSE 0 END)                           AS forecast_activating_orders
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.orders
               ELSE 0 END)                           AS forecast_repeat_orders
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.units
               ELSE 0 END)                           AS forecast_total_units
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.units
               ELSE 0 END)                           AS forecast_activating_units
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.units
               ELSE 0 END)                           AS forecast_repeat_units
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.aur
               ELSE 0 END)                           AS forecast_total_aur
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.aur
               ELSE 0 END)                           AS forecast_activating_aur
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.aur
               ELSE 0 END)                           AS forecast_repeat_aur
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.auc
               ELSE 0 END)                           AS forecast_total_auc
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.auc
               ELSE 0 END)                           AS forecast_activating_auc
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.auc
               ELSE 0 END)                           AS forecast_repeat_auc
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.gm_percent
               ELSE 0 END)                           AS forecast_total_gm_percent
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.gm_percent
               ELSE 0 END)                           AS forecast_activating_gm_percent
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.gm_percent
               ELSE 0 END)                           AS forecast_repeat_gm_percent
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.air
               ELSE 0 END)                           AS forecast_total_air
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.air
               ELSE 0 END)                           AS forecast_activating_air
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.air
               ELSE 0 END)                           AS forecast_repeat_air
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.aov
               ELSE 0 END)                           AS forecast_total_aov
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.aov
               ELSE 0 END)                           AS forecast_activating_aov
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.aov
               ELSE 0 END)                           AS forecast_repeat_aov
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.aov_include_shipping
               ELSE 0 END)                           AS forecast_total_aov_include_shipping
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.aov_include_shipping
               ELSE 0 END)                           AS forecast_activating_aov_include_shipping
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.aov_include_shipping
               ELSE 0 END)                           AS forecast_repeat_aov_include_shipping
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.gaap_revenue
               ELSE 0 END)                           AS forecast_total_gaap_revenue
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.gaap_revenue
               ELSE 0 END)                           AS forecast_activating_gaap_revenue
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.gaap_revenue
               ELSE 0 END)                           AS forecast_repeat_gaap_revenue
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.gaap_gm
               ELSE 0 END)                           AS forecast_total_gaap_gm
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.gaap_gm
               ELSE 0 END)                           AS forecast_activating_gaap_gm
     , SUM(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.gaap_gm
               ELSE 0 END)                           AS forecast_repeat_gaap_gm
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.gaap_gm_percent
               ELSE 0 END)                           AS forecast_total_gaap_gm_percent
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.gaap_gm_percent
               ELSE 0 END)                           AS forecast_activating_gaap_gm_percent
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.gaap_gm_percent
               ELSE 0 END)                           AS forecast_repeat_gaap_gm_percent
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'total' THEN fd.discount_rate
               ELSE 0 END)                           AS forecast_total_discount_rate
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'activating' THEN fd.discount_rate
               ELSE 0 END)                           AS forecast_activating_discount_rate
     , AVG(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.discount_rate
               ELSE 0 END)                           AS forecast_repeat_discount_rate
     , MAX(CASE
               WHEN LOWER(fd.order_type) = 'repeat' THEN fd.cash_collected
               ELSE 0 END)                           AS forecast_repeat_cash_collected
FROM lake_view.sharepoint.gfb_peak_forecast_daily fd
GROUP BY (CASE
              WHEN fd.business_unit = 'JF' THEN 'JUSTFAB'
              WHEN fd.business_unit = 'SD' THEN 'SHOEDAZZLE'
              WHEN fd.business_unit = 'FK' THEN 'FABKIDS'
    END)
       , fd.region
       , fd.date;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb044_holiday_data_set_hist AS
SELECT o.business_unit
     , o.region
     , o.date
     , o.data_set_filter
     , o.total_orders
     , o.total_qty_sold
     , o.total_product_revenue
     , o.total_subtotal
     , o.total_discount
     , o.total_cogs
     , o.total_product_margin
     , o.total_shipping_revenue
     , o.activating_orders
     , o.activating_qty_sold
     , o.activating_product_revenue
     , o.activating_subtotal
     , o.activating_discount
     , o.activating_cogs
     , o.activating_product_margin
     , o.activating_shipping_revenue
     , o.repeat_orders
     , o.repeat_qty_sold
     , o.repeat_product_revenue
     , o.repeat_subtotal
     , o.repeat_discount
     , o.repeat_cogs
     , o.repeat_product_margin
     , o.repeat_shipping_revenue
     , o.token_product_revenue
     , COALESCE(v.vips, 0)                                                       AS vips
     , COALESCE(l.leads, 0)                                                      AS leads
     , DATEDIFF(DAY, (CASE
                          WHEN YEAR(o.date) = 2022 THEN $thanksgiving_lly
                          WHEN YEAR(o.date) = 2023 THEN $thanksgiving_ly
                          WHEN YEAR(o.date) = 2024 THEN $thanksgiving_ty END)
    , DATE_TRUNC(DAY, o.date))                                                   AS days_from_thanksgiving
     , DATEDIFF(WEEK, (CASE
                           WHEN YEAR(o.date) = 2022 THEN $thanksgiving_lly
                           WHEN YEAR(o.date) = 2023 THEN $thanksgiving_ly
                           WHEN YEAR(o.date) = 2024 THEN $thanksgiving_ty END)
    , DATE_TRUNC(DAY, o.date))                                                   AS weeks_to_thanksgiving
     , 'Thanksgiving ' || CAST(days_from_thanksgiving AS VARCHAR(20)) || ' days' AS thanksgiving_days
     , (CASE
            WHEN weeks_to_thanksgiving < 0 THEN CAST(ABS(weeks_to_thanksgiving) AS VARCHAR(20)) ||
                                                ' Weeks Before Thanksgiving'
            WHEN weeks_to_thanksgiving > 0 THEN CAST(ABS(weeks_to_thanksgiving) AS VARCHAR(20)) ||
                                                ' Weeks After Thanksgiving'
            ELSE 'Thanksgiving Week' END)                                        AS thanksgiving_weeks
     , df.ty_activating_promo_description
     , df.ty_repeat_promo_description
     , df.ly_activating_promo_description
     , df.ly_repeat_promo_description
     , df.forecast_total_orders
     , df.forecast_activating_orders
     , df.forecast_repeat_orders
     , df.forecast_total_units
     , df.forecast_activating_units
     , df.forecast_repeat_units
     , df.forecast_activating_aur
     , df.forecast_repeat_aur
     , df.forecast_total_auc
     , df.forecast_activating_auc
     , df.forecast_repeat_auc
     , df.forecast_total_gm_percent
     , df.forecast_activating_gm_percent
     , df.forecast_repeat_gm_percent
     , df.forecast_total_air
     , df.forecast_activating_air
     , df.forecast_repeat_air
     , o.repeat_cash_collected
     , df.forecast_activating_aov
     , df.forecast_repeat_aov
     , df.forecast_total_aov_include_shipping
     , df.forecast_activating_aov_include_shipping
     , df.forecast_repeat_aov_include_shipping
     , df.forecast_total_gaap_revenue
     , df.forecast_activating_gaap_revenue
     , df.forecast_repeat_gaap_revenue
     , df.forecast_total_gaap_gm
     , df.forecast_activating_gaap_gm
     , df.forecast_repeat_gaap_gm
     , df.forecast_total_gaap_gm_percent
     , df.forecast_activating_gaap_gm_percent
     , df.forecast_repeat_gaap_gm_percent
     , df.forecast_repeat_cash_collected
     , df.forecast_activating_discount_rate
     , df.forecast_repeat_discount_rate
     , o.repeat_cash_percent_order
FROM _order_final o
         LEFT JOIN _vips v
                   ON o.business_unit = v.business_unit
                       AND o.region = v.region
                       AND o.date = v.date
         LEFT JOIN _leads l
                   ON o.business_unit = l.business_unit
                       AND o.region = l.region
                       AND o.date = l.date
         LEFT JOIN _daily_forecast df
                   ON df.business_unit = o.business_unit
                       AND df.region = o.region
                       AND df.date = DATE_TRUNC(DAY, o.date);
