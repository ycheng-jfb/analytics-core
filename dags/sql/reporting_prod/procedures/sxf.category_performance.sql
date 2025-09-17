// when adding new columns, check column order between_final cte and _weekly_sales_inv cte

CREATE OR REPLACE TEMPORARY TABLE _base_date AS
SELECT DISTINCT full_date       AS date,
                DAYOFYEAR(date) AS dayofyear,
                MONTH(date)     AS monthofyear,
                YEAR(date)      AS year,
                NULL            AS weekofyear,
                NULL            AS yearofweek
FROM edw_prod.data_model_sxf.dim_date
WHERE year >= 2018
  AND year <= YEAR(CURRENT_DATE);

CREATE OR REPLACE TEMPORARY TABLE _on_order AS
SELECT color_sku_po,
    region,
    wh_type,
    SUM(IFF(DATEDIFF('day',CURRENT_DATE ,PULSE_FC_ARRIVAL_DATE) <= 90, PO_QTY, 0)) AS three_months_arrival_date_qty,
    SUM(IFF(DATEDIFF('day',CURRENT_DATE ,PULSE_FC_ARRIVAL_DATE) <= 180, PO_QTY, 0))  AS six_months_arrival_date_qty,
    SUM(PO_QTY) PO_QTY
FROM reporting_prod.sxf.view_on_order
GROUP BY color_sku_po,
    region,
    wh_type;


----create date,product sku base
CREATE OR REPLACE TEMPORARY TABLE _base_sku_date_region AS
SELECT DISTINCT date,
                dayofyear,
                year,
                region,
                color_sku_po    AS color_sku,
                style_number_po AS style_number,
                category,
                is_color_sku_on_order
FROM _base_date dv
     LEFT JOIN
     (SELECT DISTINCT sms.color_sku_po,
                      CASE WHEN oo.color_sku_po IS NULL THEN 0 ELSE 1 END AS is_color_sku_on_order,
                      style_number_po,
                      category,
                      first_occurrence_date,
                      first_inventory_occurrence_date
      FROM reporting_prod.sxf.view_style_master_size sms
      LEFT JOIN (SELECT DISTINCT color_sku_po FROM _on_order WHERE three_months_arrival_date_qty != 0) oo ON oo.color_sku_po = sms.color_sku_po
     ) x
     ON 1 = 1
     LEFT JOIN (SELECT DISTINCT region FROM reporting_prod.sxf.view_base_inventory_dataset) r
               ON 1 = 1
WHERE (CASE WHEN is_color_sku_on_order = 0 THEN date >= IFNULL(first_occurrence_date, first_inventory_occurrence_date) --get the dates for skus(not on order) from sales/inv start date
            WHEN is_color_sku_on_order = 1 AND IFNULL(first_occurrence_date, first_inventory_occurrence_date) IS NOT NULL THEN date >= IFNULL(first_occurrence_date, first_inventory_occurrence_date)--get the dates for skus which are on order but are old skus(RE-ORDER). On order skus where the sales/inv exists
            WHEN is_color_sku_on_order = 1 AND IFNULL(first_occurrence_date, first_inventory_occurrence_date) IS NULL AND date >= DATEADD('day',-30,CURRENT_DATE()) THEN TRUE -- get the data only for past 60 days for skus which are first time on order(NEW/FUTURE POS)
       ELSE FALSE
                END
      ) -- to optimize dataset, begins pulling in daily calendar on/after the first occurence of a color sku.
  -- First Occurence = the earliest of A) qty_onhand + qty_replen + qty_ghost  >0  B) first inventory received in FC date  C) first recorded sales date
  AND date <= CURRENT_DATE()
  AND CASE WHEN region = 'EU-UK' AND date < '2021-03-18' THEN 0 ELSE 1 END = 1
ORDER BY 1, 2;

CREATE OR REPLACE TEMPORARY TABLE _store_dimensions AS
SELECT store_number,
       store_name,
       store_id,
       warehouse_id,
       MAX(store_open_date) AS store_open_date
FROM (SELECT DISTINCT TRIM(rl.retail_location_code)                    AS store_number
                    , TRIM(rl.label)                                   AS store_name
                    , TRIM(rl.store_id)                                AS store_id
                    , TRIM(rl.warehouse_id)                            AS warehouse_id
                    , TRIM(COALESCE(rl.open_date, rl.grand_open_date)) AS store_open_date
      FROM lake.ultra_warehouse.retail_location rl
           JOIN edw_prod.data_model_sxf.dim_store ds
                ON ds.store_id = rl.store_id
      WHERE ds.store_brand = 'Savage X'
        AND ds.store_type = 'Retail')
WHERE store_id NOT IN (144, 145, 156, 157)
GROUP BY 1, 2, 3, 4;


CREATE OR REPLACE TEMPORARY TABLE _sku_date_region_store AS
SELECT DISTINCT sdm.date,
                sdm.color_sku,
                ds.store_type,
                store_country,
                store_full_name,
                sd.store_open_date,
                CASE
                    WHEN store_region = 'EU' AND store_country = 'UK' AND date > '2021-03-18' THEN 'EU-UK'
                    ELSE store_region END     AS store_region_use,
                CASE
                    WHEN store_type = 'Retail' THEN UPPER(store_full_name)
                    ELSE store_region_use END AS store_warehouse_region,
                    is_color_sku_on_order
FROM _base_sku_date_region sdm
     JOIN edw_prod.data_model_sxf.dim_store ds
          ON ds.store_region = CASE
                                   WHEN sdm.region = 'EU-UK' AND ds.store_country = 'UK'
                                       THEN 'EU'
                                   ELSE sdm.region END
              AND ds.store_brand = 'Savage X'
              AND store_full_name NOT ILIKE '%(DM)%'
              AND store_country NOT IN ('CA', 'SE', 'NL', 'DK')
              AND store_full_name NOT ILIKE 'Savage Test Store 804'
     LEFT JOIN _store_dimensions sd
               ON sd.store_id = ds.store_id
WHERE CASE WHEN sd.store_id = ds.store_id AND date < sd.store_open_date THEN 0 ELSE 1 END = 1
  AND CASE
          WHEN store_full_name = 'RTLSXF-Valley Fair' AND (date < '2018-10-27' OR date > '2019-01-22') THEN 0
          WHEN store_full_name = 'RTLSXF-SOHO' AND (date < '2018-09-11' OR date > '2019-09-12') THEN 0
          WHEN store_full_name = 'RTLSXF-Mall of America' AND (date < '2018-08-30' OR date > '2019-01-23') THEN 0
          WHEN store_full_name = 'RTLSXF-Las Vegas' AND (date < '2018-10-27' OR date > '2019-01-22') THEN 0
          ELSE 1 END = 1
  AND CASE WHEN store_country = 'UK' AND region = 'EU' AND date > '2021-03-18' THEN 0 ELSE 1 END = 1;

CREATE OR REPLACE TEMPORARY TABLE _weekly_date_view AS
SELECT DISTINCT full_date                              AS date,
                NULL                                   AS dayofyear,
                NULL                                   AS monthofyear,
                NULL                                   AS year,
                WEEKOFYEAR(DATEADD(DAY, 1, full_date)) AS weekofyear, --- Alter to Start week 1 day before Monday (Sun-Sat)
                YEAROFWEEK(DATEADD(DAY, 1, full_date)) AS yearofweek, --- Alter to Start week 1 day before Monday (Sun-Sat)
                'Weekly'                               AS date_view
FROM edw_prod.data_model_sxf.dim_date
WHERE YEAR(full_date) >= 2018
  AND full_date <=
      CASE
          WHEN DAYNAME(CURRENT_DATE) = 'Sun' THEN DATEADD(DAY, 6, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Mon' THEN DATEADD(DAY, 5, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Tue' THEN DATEADD(DAY, 4, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Wed' THEN DATEADD(DAY, 3, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Thu' THEN DATEADD(DAY, 2, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Fri' THEN DATEADD(DAY, 1, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Sat' THEN CURRENT_DATE
          END;

CREATE OR REPLACE TEMPORARY TABLE _weeklyrange AS
SELECT weekofyear, yearofweek, MIN(date) AS start_date, MAX(date) AS end_date
FROM _weekly_date_view
WHERE weekofyear IS NOT NULL
GROUP BY weekofyear, yearofweek
ORDER BY yearofweek, weekofyear;

CREATE OR REPLACE TEMPORARY TABLE _weekly_region_store AS
SELECT color_sku,
       store_type,
       store_country,
       store_full_name,
       store_region_use,
       store_warehouse_region,
       store_open_date,
       weekofyear,
       yearofweek,
       start_date,
       end_date,
       is_color_sku_on_order
FROM _weeklyrange w
     LEFT JOIN _sku_date_region_store s
               ON w.weekofyear = WEEKOFYEAR(DATEADD(DAY, 1, s.date))
                   AND w.yearofweek = YEAROFWEEK(DATEADD(DAY, 1, s.date))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;

CREATE OR REPLACE TEMP TABLE _marked_down_history AS
SELECT DISTINCT color_sku_po,
    full_date,
    store_country,
    CASE WHEN st.store_country = 'UK' AND full_date >= '2021-03-18' THEN 'EU-UK' -- started shipping UK store orders from UK warehouse on 03/18/21
    ELSE st.store_region END AS region
FROM reporting_prod.sxf.view_markdown_history m
JOIN edw_prod.data_model_sxf.dim_store st on st.store_id = m.store_id;


CREATE OR REPLACE TEMPORARY TABLE _sales AS
SELECT product_sku AS                                                           color_sku,
       date(order_hq_date)                                                      date,
       order_store_type,
       store_region_abbr,
       store_country,
       order_store_full_name,
       sd.store_open_date,
       SUM(product_gross_revenue_excl_shipping_amount)                          ttl_revenue_excl_shipping,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Activating VIP' THEN product_gross_revenue_excl_shipping_amount
               ELSE 0
           END)                                                                 act_vip_revenue_excl_shipping,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Repeat VIP' THEN product_gross_revenue_excl_shipping_amount
               ELSE 0
           END)                                                                 rpt_vip_revenue_excl_shipping,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('First Guest')
                   THEN product_gross_revenue_excl_shipping_amount
               ELSE 0
           END)                                                                 frst_guest_revenue_excl_shipping,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('Repeat Guest')
                   THEN product_gross_revenue_excl_shipping_amount
               ELSE 0
           END)                                                                 rpt_guest_revenue_excl_shipping,
       SUM(olrd.item_quantity)                                                  total_qty,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Activating VIP' THEN olrd.item_quantity
               ELSE 0
           END)                                                                 act_vip_ttl_qty,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Repeat VIP' THEN olrd.item_quantity
               ELSE 0
           END)                                                                 rpt_vip_ttl_qty,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('First Guest') THEN olrd.item_quantity
               ELSE 0
           END)                                                                 frst_guest_ttl_qty,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('Repeat Guest') THEN olrd.item_quantity
               ELSE 0
           END)                                                                 rpt_guest_ttl_qty,
       SUM(product_margin_pre_return_excl_shipping)                             gross_margin,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Activating VIP' THEN product_margin_pre_return_excl_shipping
               ELSE 0
           END)                                                                 act_vip_gross_margin,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Repeat VIP' THEN product_margin_pre_return_excl_shipping
               ELSE 0
           END)                                                                 rpt_vip_gross_margin,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('First Guest')
                   THEN product_margin_pre_return_excl_shipping
               ELSE 0
           END)                                                                 frst_guest_gross_margin,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('Repeat Guest')
                   THEN product_margin_pre_return_excl_shipping
               ELSE 0
           END)                                                                 rpt_guest_gross_margin,
       SUM(reporting_landed_cost)                                               ttl_reporting_cost,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Activating VIP' THEN reporting_landed_cost
               ELSE 0
           END)                                                                 act_vip_reporting_cost,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Repeat VIP' THEN reporting_landed_cost
               ELSE 0
           END)                                                                 rpt_vip_reporting_cost,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('First Guest') THEN reporting_landed_cost
               ELSE 0
           END)                                                                 frst_guest_reporting_cost,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('Repeat Guest') THEN reporting_landed_cost
               ELSE 0
           END)                                                                 rpt_guest_reporting_cost,
       SUM(estimated_landed_cost)                                               ttl_est_cost,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Activating VIP' THEN estimated_landed_cost
               ELSE 0
           END)                                                                 act_vip_est_cost,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Repeat VIP' THEN estimated_landed_cost
               ELSE 0
           END)                                                                 rpt_vip_est_cost,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('First Guest') THEN estimated_landed_cost
               ELSE 0
           END)                                                                 frst_guest_est_cost,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('Repeat Guest') THEN estimated_landed_cost
               ELSE 0
           END)                                                                 rpt_guest_est_cost,

       0           AS                                                           ttl_qty_returned,
       0           AS                                                           act_vip_qty_returned,
       0           AS                                                           rpt_vip_qty_returned,
       0           AS                                                           frst_guest_qty_returned,
       0           AS                                                           rpt_guest_qty_returned,
       SUM(olrd.initial_retail_price_excl_vat * order_date_usd_conversion_rate) sum_initial_retail_price_excl_vat,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Activating VIP'
                   THEN olrd.initial_retail_price_excl_vat * order_date_usd_conversion_rate
               ELSE 0
           END)                                                                 act_vip_initial_retail_price,
       SUM(CASE
               WHEN is_ecomm_detailed ILIKE 'Repeat VIP'
                   THEN olrd.initial_retail_price_excl_vat * order_date_usd_conversion_rate
               ELSE 0
           END)                                                                 rpt_vip_initial_retail_price,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('First Guest')
                   THEN olrd.initial_retail_price_excl_vat * order_date_usd_conversion_rate
               ELSE 0
           END)                                                                 frst_guest_initial_retail_price,
       SUM(CASE
               WHEN is_ecomm_detailed IN ('Repeat Guest')
                   THEN olrd.initial_retail_price_excl_vat * order_date_usd_conversion_rate
               ELSE 0
           END)                                                                 rpt_guest_initial_retail_price,
       SUM(olrd.vip_unit_price * order_date_usd_conversion_rate)                sum_vip_unit_price,
       MAX(order_hq_datetime)                                                   max_order_data_through,
       SUM(olrd.finalsales_quantity)                                            ttl_finalsales_qty,
       SUM(olrd.vip_unit_price_excl_vat * order_date_usd_conversion_rate)       sum_vip_unit_price_excl_vat,
       SUM(olrd.retail_unit_price * order_date_usd_conversion_rate)             sum_msrp_unit_price
FROM reporting_prod.sxf.view_order_line_recognized_dataset olrd
     JOIN edw_prod.data_model_sxf.fact_order_line ol
     ON olrd.order_line_id = ol.order_line_id
     LEFT JOIN _store_dimensions sd
     ON ol.store_id = sd.store_id
GROUP BY 1, 2, 3, 4, 5, 6, 7
UNION
SELECT dp.product_sku                                                                   AS color_sku
     , date(CONVERT_TIMEZONE('America/Los_Angeles', frl.return_receipt_local_datetime)) AS date
     , ds.store_type                                                                    AS store_type
     , CASE
           WHEN store_country = 'UK' AND date(date) >= '2021-03-18'
               THEN 'EU-UK' -- started shipping UK store orders from UK warehouse on 03/18/21
           ELSE store_region END                                                        AS region
     , ds.store_country                                                                 AS country
     , ds.store_full_name
     , sd.store_open_date
     , 0                                                                                AS ttl_revenue_excl_shipping
     , 0                                                                                AS act_vip_revenue_excl_shipping
     , 0                                                                                AS rpt_vip_revenue_excl_shipping
     , 0                                                                                AS frst_guest_revenue_excl_shipping
     , 0                                                                                AS rpt_guest_revenue_excl_shipping
     , 0                                                                                AS total_qty
     , 0                                                                                AS act_vip_ttl_qty
     , 0                                                                                AS rpt_vip_ttl_qty
     , 0                                                                                AS frst_guest_ttl_qty
     , 0                                                                                AS rpt_guest_ttl_qty
     , 0                                                                                AS gross_margin
     , 0                                                                                AS act_vip_gross_margin
     , 0                                                                                AS rpt_vip_gross_margin
     , 0                                                                                AS frst_guest_gross_margin
     , 0                                                                                AS rpt_guest_gross_margin
     , 0                                                                                AS ttl_reporting_cost
     , 0                                                                                AS act_vip_reporting_cost
     , 0                                                                                AS rpt_vip_reporting_cost
     , 0                                                                                AS frst_guest_reporting_cost
     , 0                                                                                AS rpt_guest_reporting_cost
     , 0                                                                                AS ttl_est_cost
     , 0                                                                                AS act_vip_est_cost
     , 0                                                                                AS rpt_vip_est_cost
     , 0                                                                                AS frst_guest_est_cost
     , 0                                                                                AS rpt_guest_est_cost
     , SUM(COALESCE(frl.return_item_quantity, 0))                                       AS total_return_units
     , SUM(IFF(domc.membership_order_type_l3 = 'Activating VIP', COALESCE(frl.return_item_quantity, 0),
               0))                                                                      AS activating_vip_return_units
     , SUM(IFF(domc.membership_order_type_l3 = 'Repeat VIP', COALESCE(frl.return_item_quantity, 0),
               0))                                                                      AS repeat_vip_return_units
     , SUM(IFF(domc.membership_order_type_l3 IN ('First Guest'),
               COALESCE(frl.return_item_quantity, 0),
               0))                                                                      AS frst_guest_return_units
     , SUM(IFF(domc.membership_order_type_l3 IN ('Repeat Guest'),
               COALESCE(frl.return_item_quantity, 0),
               0))                                                                      AS rpt_guest_return_units
     , 0                                                                                AS sum_initial_retail_price_excl_vat
     , 0                                                                                AS act_vip_initial_retail_price
     , 0                                                                                AS rpt_vip_initial_retail_price
     , 0                                                                                AS frst_guest_initial_retail_price
     , 0                                                                                AS rpt_guest_initial_retail_price
     , 0                                                                                AS sum_vip_unit_price
     , MAX(date)                                                                        AS max_return_data_through
     , 0                                                                                AS ttl_finalsales_qty
     , 0                                                                                AS sum_vip_unit_price_excl_vat
     , 0                                                                                AS sum_msrp_unit_price
FROM edw_prod.data_model_sxf.fact_return_line frl
     JOIN edw_prod.data_model_sxf.dim_store ds
          ON frl.store_id = ds.store_id
     JOIN edw_prod.data_model_sxf.dim_product dp
          ON frl.product_id = dp.product_id
     LEFT JOIN reporting_prod.sxf.style_master sm
               ON sm.color_sku_po = dp.product_sku
     JOIN edw_prod.data_model_sxf.dim_return_status drs
          ON frl.return_status_key = drs.return_status_key
     JOIN edw_prod.data_model_sxf.dim_return_condition drc
          ON frl.return_condition_key = drc.return_condition_key
     JOIN edw_prod.data_model_sxf.fact_order_line fol
          ON frl.order_line_id = fol.order_line_id
              AND frl.store_id = fol.store_id
     JOIN edw_prod.data_model_sxf.dim_product_type dpt
          ON fol.product_type_key = dpt.product_type_key
              AND dpt.is_free = 'FALSE'
              AND dpt.product_type_name NOT IN (
                                                'Offer Gift',
                                                'Gift Certificate',
                                                'Membership Gift',
                                                'Membership Reward Points Item'
                  )
     JOIN edw_prod.data_model_sxf.dim_order_membership_classification domc
          ON fol.order_membership_classification_key = domc.order_membership_classification_key
     JOIN edw_prod.data_model_sxf.fact_order fo
          ON fol.order_id = fo.order_id
     JOIN edw_prod.data_model_sxf.dim_order_sales_channel dosc
          ON fo.order_sales_channel_key = dosc.order_sales_channel_key
              AND dosc.order_classification_l1 IN ('Product Order', 'Exchange', 'Reship')
     JOIN edw_prod.data_model_sxf.dim_customer dc
          ON dc.customer_id = fo.customer_id
     LEFT JOIN edw_prod.data_model_sxf.dim_product_price_history ph
               ON (ph.product_price_history_key = fol.product_price_history_key)
     LEFT JOIN _store_dimensions sd
     ON ds.store_id = sd.store_id
WHERE ds.store_brand_abbr = 'SX'
  AND frl.return_receipt_local_datetime >= '2020-01-01'
  AND drs.return_status = 'Resolved'
  AND NOT (ds.store_brand = 'Fabletics' AND dp.product_category ILIKE 'Savage')
  AND NOT (ds.store_brand = 'Fabletics' AND dp.product_category ILIKE 'Savage' AND sku = 'Unknown')
  AND ds.store_country NOT IN ('DK', 'CA', 'NL', 'SE')
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE OR REPLACE TEMPORARY TABLE _sales_returns AS
SELECT color_sku,
       date,
       order_store_type,
       store_region_abbr,
       store_country,
       order_store_full_name,
       store_open_date,
       SUM(ttl_revenue_excl_shipping)         ttl_revenue_excl_shipping,
       SUM(act_vip_revenue_excl_shipping)     act_vip_revenue_excl_shipping,
       SUM(rpt_vip_revenue_excl_shipping)     rpt_vip_revenue_excl_shipping,
       SUM(frst_guest_revenue_excl_shipping)  frst_guest_revenue_excl_shipping,
       SUM(rpt_guest_revenue_excl_shipping)   rpt_guest_revenue_excl_shipping,
       SUM(total_qty)                         total_qty,
       SUM(act_vip_ttl_qty)                   act_vip_ttl_qty,
       SUM(rpt_vip_ttl_qty)                   rpt_vip_ttl_qty,
       SUM(frst_guest_ttl_qty)                frst_guest_ttl_qty,
       SUM(rpt_guest_ttl_qty)                 rpt_guest_ttl_qty,
       SUM(gross_margin)                      gross_margin,
       SUM(act_vip_gross_margin)              act_vip_gross_margin,
       SUM(rpt_vip_gross_margin)              rpt_vip_gross_margin,
       SUM(frst_guest_gross_margin)           frst_guest_gross_margin,
       SUM(rpt_guest_gross_margin)            rpt_guest_gross_margin,
       SUM(ttl_reporting_cost)                ttl_reporting_cost,
       SUM(act_vip_reporting_cost)            act_vip_reporting_cost,
       SUM(rpt_vip_reporting_cost)            rpt_vip_reporting_cost,
       SUM(frst_guest_reporting_cost)         frst_guest_reporting_cost,
       SUM(rpt_guest_reporting_cost)          rpt_guest_reporting_cost,
       SUM(ttl_est_cost)                      ttl_est_cost,
       SUM(act_vip_est_cost)                  act_vip_est_cost,
       SUM(rpt_vip_est_cost)                  rpt_vip_est_cost,
       SUM(frst_guest_est_cost)               frst_guest_est_cost,
       SUM(rpt_guest_est_cost)                rpt_guest_est_cost,
       SUM(ttl_qty_returned)                  ttl_qty_returned,
       SUM(act_vip_qty_returned)              act_vip_qty_returned,
       SUM(rpt_vip_qty_returned)              rpt_vip_qty_returned,
       SUM(frst_guest_qty_returned)           frst_guest_qty_returned,
       SUM(rpt_guest_qty_returned)            rpt_guest_qty_returned,
       SUM(sum_initial_retail_price_excl_vat) sum_initial_retail_price_excl_vat,
       SUM(act_vip_initial_retail_price)      act_vip_initial_retail_price,
       SUM(rpt_vip_initial_retail_price)      rpt_vip_initial_retail_price,
       SUM(frst_guest_initial_retail_price)   frst_guest_initial_retail_price,
       SUM(rpt_guest_initial_retail_price)    rpt_guest_initial_retail_price,
       SUM(sum_vip_unit_price)                sum_vip_unit_price,
       MAX(max_order_data_through)            max_order_data_through,
       SUM(ttl_finalsales_qty)                ttl_finalsales_qty,
       SUM(sum_vip_unit_price_excl_vat)       sum_vip_unit_price_excl_vat,
       SUM(sum_msrp_unit_price)               sum_msrp_unit_price
FROM _sales
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE OR REPLACE TEMPORARY TABLE _ly_sales AS
SELECT s.*
     , DATEADD(YEAR, 1, date)            ly_sales_date
     , CASE
           WHEN ly_sales_date > '2021-03-18' AND store_country = 'UK' THEN 'EU-UK'
           WHEN ly_sales_date < '2021-03-18' AND store_region_abbr = 'EU-UK' THEN 'EU'
           WHEN ly_sales_date > '2021-03-18' AND store_region_abbr <> 'NA' AND store_country <> 'UK' THEN 'EU'
           ELSE store_region_abbr END AS ly_store_region_abbr
FROM _sales_returns s
WHERE (DAYOFMONTH(date) <> 29 OR MONTH(date) <> 2);

CREATE OR REPLACE TEMPORARY TABLE _sales_final AS
SELECT COALESCE(s.color_sku, ly.color_sku)                                color_sku,
       COALESCE(s.date, ly.ly_sales_date)                                 date,
       COALESCE(s.order_store_type, ly.order_store_type)                  order_store_type,
       COALESCE(s.store_region_abbr, ly.ly_store_region_abbr)             store_region_abbr,
       COALESCE(s.store_country, ly.store_country)                        store_country,
       COALESCE(s.order_store_full_name, ly.order_store_full_name)        order_store_full_name,
       COALESCE(s.store_open_date, ly.store_open_date)                    store_open_date,
       MAX(COALESCE(s.max_order_data_through, ly.max_order_data_through)) max_order_data_through,
       SUM(COALESCE(s.ttl_revenue_excl_shipping, 0))                      ttl_revenue_excl_shipping,
       SUM(COALESCE(s.act_vip_revenue_excl_shipping, 0))                  act_vip_revenue_excl_shipping,
       SUM(COALESCE(s.rpt_vip_revenue_excl_shipping, 0))                  rpt_vip_revenue_excl_shipping,
       SUM(COALESCE(s.frst_guest_revenue_excl_shipping, 0))               frst_guest_revenue_excl_shipping,
       SUM(COALESCE(s.rpt_guest_revenue_excl_shipping, 0))                rpt_guest_revenue_excl_shipping,
       SUM(COALESCE(s.total_qty, 0))                                      total_qty,
       SUM(COALESCE(s.act_vip_ttl_qty, 0))                                act_vip_ttl_qty,
       SUM(COALESCE(s.rpt_vip_ttl_qty, 0))                                rpt_vip_ttl_qty,
       SUM(COALESCE(s.frst_guest_ttl_qty, 0))                             frst_guest_ttl_qty,
       SUM(COALESCE(s.rpt_guest_ttl_qty, 0))                              rpt_guest_ttl_qty,
       SUM(COALESCE(s.gross_margin, 0))                                   gross_margin,
       SUM(COALESCE(s.act_vip_gross_margin, 0))                           act_vip_gross_margin,
       SUM(COALESCE(s.rpt_vip_gross_margin, 0))                           rpt_vip_gross_margin,
       SUM(COALESCE(s.frst_guest_gross_margin, 0))                        frst_guest_gross_margin,
       SUM(COALESCE(s.rpt_guest_gross_margin, 0))                         rpt_guest_gross_margin,
       SUM(COALESCE(s.ttl_reporting_cost, 0))                             ttl_reporting_cost,
       SUM(COALESCE(s.act_vip_reporting_cost, 0))                         act_vip_reporting_cost,
       SUM(COALESCE(s.rpt_vip_reporting_cost, 0))                         rpt_vip_reporting_cost,
       SUM(COALESCE(s.frst_guest_reporting_cost, 0))                      frst_guest_reporting_cost,
       SUM(COALESCE(s.rpt_guest_reporting_cost, 0))                       rpt_guest_reporting_cost,
       SUM(COALESCE(s.ttl_est_cost, 0))                                   ttl_est_cost,
       SUM(COALESCE(s.act_vip_est_cost, 0))                               act_vip_est_cost,
       SUM(COALESCE(s.rpt_vip_est_cost, 0))                               rpt_vip_est_cost,
       SUM(COALESCE(s.frst_guest_est_cost, 0))                            frst_guest_est_cost,
       SUM(COALESCE(s.rpt_guest_est_cost, 0))                             rpt_guest_est_cost,
       SUM(COALESCE(s.ttl_qty_returned, 0))                               ttl_qty_returned,
       SUM(COALESCE(s.act_vip_qty_returned, 0))                           act_vip_qty_returned,
       SUM(COALESCE(s.rpt_vip_qty_returned, 0))                           rpt_vip_qty_returned,
       SUM(COALESCE(s.frst_guest_qty_returned, 0))                        frst_guest_qty_returned,
       SUM(COALESCE(s.rpt_guest_qty_returned, 0))                         rpt_guest_qty_returned,
       SUM(COALESCE(s.sum_initial_retail_price_excl_vat, 0))              sum_initial_retail_price_excl_vat,
       SUM(COALESCE(s.act_vip_initial_retail_price, 0))                   act_vip_initial_retail_price,
       SUM(COALESCE(s.rpt_vip_initial_retail_price, 0))                   rpt_vip_initial_retail_price,
       SUM(COALESCE(s.frst_guest_initial_retail_price, 0))                frst_guest_initial_retail_price,
       SUM(COALESCE(s.rpt_guest_initial_retail_price, 0))                 rpt_guest_initial_retail_price,
       SUM(COALESCE(s.sum_vip_unit_price, 0))                             sum_vip_unit_price,
       SUM(COALESCE(s.ttl_finalsales_qty, 0))                             ttl_finalsales_qty,
       SUM(COALESCE(s.sum_vip_unit_price_excl_vat, 0))                    sum_vip_unit_price_excl_vat,
       SUM(COALESCE(s.sum_msrp_unit_price, 0))                            sum_msrp_unit_price,
       SUM(COALESCE(ly.ttl_revenue_excl_shipping, 0))                     ly_ttl_revenue_excl_shipping,
       SUM(COALESCE(ly.act_vip_revenue_excl_shipping, 0))                 ly_act_vip_revenue_excl_shipping,
       SUM(COALESCE(ly.rpt_vip_revenue_excl_shipping, 0))                 ly_rpt_vip_revenue_excl_shipping,
       SUM(COALESCE(ly.frst_guest_revenue_excl_shipping, 0))              ly_frst_guest_revenue_excl_shipping,
       SUM(COALESCE(ly.rpt_guest_revenue_excl_shipping, 0))               ly_rpt_guest_revenue_excl_shipping,
       SUM(COALESCE(ly.total_qty, 0))                                     ly_total_qty,
       SUM(COALESCE(ly.act_vip_ttl_qty, 0))                               ly_act_vip_ttl_qty,
       SUM(COALESCE(ly.rpt_vip_ttl_qty, 0))                               ly_rpt_vip_ttl_qty,
       SUM(COALESCE(ly.frst_guest_ttl_qty, 0))                            ly_frst_guest_ttl_qty,
       SUM(COALESCE(ly.rpt_guest_ttl_qty, 0))                             ly_rpt_guest_ttl_qty,
       SUM(COALESCE(ly.gross_margin, 0))                                  ly_gross_margin,
       SUM(COALESCE(ly.act_vip_gross_margin, 0))                          ly_act_vip_gross_margin,
       SUM(COALESCE(ly.rpt_vip_gross_margin, 0))                          ly_rpt_vip_gross_margin,
       SUM(COALESCE(ly.frst_guest_gross_margin, 0))                       ly_frst_guest_gross_margin,
       SUM(COALESCE(ly.rpt_guest_gross_margin, 0))                        ly_rpt_guest_gross_margin,
       SUM(COALESCE(ly.ttl_reporting_cost, 0))                            ly_ttl_reporting_cost,
       SUM(COALESCE(ly.act_vip_reporting_cost, 0))                        ly_act_vip_reporting_cost,
       SUM(COALESCE(ly.rpt_vip_reporting_cost, 0))                        ly_rpt_vip_reporting_cost,
       SUM(COALESCE(ly.frst_guest_reporting_cost, 0))                     ly_frst_guest_reporting_cost,
       SUM(COALESCE(ly.rpt_guest_reporting_cost, 0))                      ly_rpt_guest_reporting_cost,
       SUM(COALESCE(ly.ttl_est_cost, 0))                                  ly_ttl_est_cost,
       SUM(COALESCE(ly.act_vip_est_cost, 0))                              ly_act_vip_est_cost,
       SUM(COALESCE(ly.rpt_vip_est_cost, 0))                              ly_rpt_vip_est_cost,
       SUM(COALESCE(ly.frst_guest_est_cost, 0))                           ly_frst_guest_est_cost,
       SUM(COALESCE(ly.rpt_guest_est_cost, 0))                            ly_rpt_guest_est_cost,
       SUM(COALESCE(ly.ttl_qty_returned, 0))                              ly_ttl_qty_returned,
       SUM(COALESCE(ly.act_vip_qty_returned, 0))                          ly_act_vip_qty_returned,
       SUM(COALESCE(ly.rpt_vip_qty_returned, 0))                          ly_rpt_vip_qty_returned,
       SUM(COALESCE(ly.frst_guest_qty_returned, 0))                       ly_frst_guest_qty_returned,
       SUM(COALESCE(ly.rpt_guest_qty_returned, 0))                        ly_rpt_guest_qty_returned,
       SUM(COALESCE(ly.sum_initial_retail_price_excl_vat, 0))             ly_sum_initial_retail_price_excl_vat,
       SUM(COALESCE(ly.act_vip_initial_retail_price, 0))                  ly_act_vip_initial_retail_price,
       SUM(COALESCE(ly.rpt_vip_initial_retail_price, 0))                  ly_rpt_vip_initial_retail_price,
       SUM(COALESCE(ly.frst_guest_initial_retail_price, 0))               ly_frst_guest_initial_retail_price,
       SUM(COALESCE(ly.rpt_guest_initial_retail_price, 0))                ly_rpt_guest_initial_retail_price,
       SUM(COALESCE(ly.sum_vip_unit_price, 0))                            ly_sum_vip_unit_price,
       SUM(COALESCE(ly.ttl_finalsales_qty, 0))                            ly_ttl_finalsales_qty,
       SUM(COALESCE(ly.sum_vip_unit_price_excl_vat, 0))                   ly_sum_vip_unit_price_excl_vat,
       SUM(COALESCE(ly.sum_msrp_unit_price, 0))                           ly_sum_msrp_unit_price
FROM _sales_returns s
     FULL JOIN _ly_sales ly
               ON ly.color_sku = s.color_sku
               AND ly.ly_sales_date = s.date
               AND s.order_store_type = ly.order_store_type
               AND s.store_country = ly.store_country
               AND s.order_store_full_name = ly.order_store_full_name
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE OR REPLACE TEMPORARY TABLE _inventory AS
SELECT i.product_sku,
       date(date)                                                  date,
       region,
       CASE WHEN is_retail = 'Y' THEN warehouse ELSE region END AS store_warehouse_region,
       MAX(max_inventory_data_through)                             max_inventory_data_through,
       SUM(qty_available_to_sell)                                  qty_available_to_sell,
       SUM(QTY_INTRANSIT)                                          qty_intransit //bringing in in transit for retail version of playbook to replace On Order
FROM reporting_prod.sxf.view_base_inventory_dataset i
     LEFT JOIN (SELECT product_sku, MAX(datetime) AS max_inventory_data_through
                FROM reporting_prod.sxf.view_base_inventory_dataset i
                WHERE is_current = 'Y'
                GROUP BY 1) max_inv
               ON max_inv.product_sku = i.product_sku
GROUP BY i.product_sku,
         region,
         store_warehouse_region,
         date;

CREATE OR REPLACE TEMPORARY TABLE _eop_inv AS
SELECT DATEADD(DAY, -1, date) eop_date,
       product_sku,
       region,
       store_warehouse_region,
       max_inventory_data_through,
       qty_available_to_sell,
       qty_intransit // eop inventory for in transit
FROM _inventory;

CREATE OR REPLACE TEMPORARY TABLE _inventory_final AS
SELECT COALESCE(i.product_sku, eop.product_sku,
                ly_eop.product_sku,
                ly_bop.product_sku)                                 product_sku,
       COALESCE(i.date, eop.eop_date, ly_eop.ly_eop_date,
                ly_bop.ly_bop_date)                                 date,
       COALESCE(i.region, eop.region, ly_eop.region, ly_bop.region) region,
       COALESCE(i.store_warehouse_region,
                eop.store_warehouse_region,
                ly_eop.store_warehouse_region,
                ly_bop.store_warehouse_region)                      store_warehouse_region,
       MAX(COALESCE(i.max_inventory_data_through,
                    eop.max_inventory_data_through,
                    ly_eop.max_inventory_data_through,
                    ly_bop.max_inventory_data_through))             max_inventory_data_through,
       SUM(COALESCE(i.qty_available_to_sell, 0))                    qty_available_to_sell,
       SUM(COALESCE(eop.qty_available_to_sell, 0))                  eop_inv,
       SUM(COALESCE(eop.qty_intransit, 0))                          eop_in_transit,
       SUM(COALESCE(ly_bop.qty_available_to_sell, 0))               ly_bop_inv,
       SUM(COALESCE(ly_eop.qty_available_to_sell, 0))               ly_eop_inv
FROM _inventory i
     LEFT JOIN _eop_inv eop // adjusted join to match correct inv on eop date
               ON eop.eop_date = i.date AND
                  eop.product_sku = i.product_sku
                   AND eop.region = i.region AND
                  eop.store_warehouse_region =
                  i.store_warehouse_region
     FULL JOIN (SELECT DATEADD(YEAR, 1, eop_date) ly_eop_date,
                       product_sku,
                       region,
                       store_warehouse_region,
                       max_inventory_data_through,
                       qty_available_to_sell
                FROM _eop_inv
                WHERE DAYOFMONTH(eop_date) <> 29
                   OR MONTH(eop_date) <> 2) ly_eop
               ON ly_eop.ly_eop_date = i.date AND
                  ly_eop.product_sku = i.product_sku
                   AND ly_eop.region = i.region AND
                  ly_eop.store_warehouse_region =
                  i.store_warehouse_region
     FULL JOIN (SELECT DATEADD(YEAR, 1, date) ly_bop_date,
                       product_sku,
                       region,
                       store_warehouse_region,
                       max_inventory_data_through,
                       qty_available_to_sell
                FROM _inventory
                WHERE (DAYOFMONTH(date) <> 29 OR MONTH(date) <> 2)) ly_bop
               ON ly_bop.ly_bop_date = i.date AND
                  ly_bop.product_sku = i.product_sku
                   AND ly_bop.region = i.region AND
                  ly_bop.store_warehouse_region =
                  i.store_warehouse_region
GROUP BY COALESCE(i.product_sku, eop.product_sku,
                  ly_eop.product_sku, ly_bop.product_sku),
         COALESCE(i.date, eop.eop_date, ly_eop.ly_eop_date,
                  ly_bop.ly_bop_date),
         COALESCE(i.region, eop.region, ly_eop.region, ly_bop.region),
         COALESCE(i.store_warehouse_region,
                  eop.store_warehouse_region,
                  ly_eop.store_warehouse_region,
                  ly_bop.store_warehouse_region);


CREATE OR REPLACE TEMP TABLE _ty_ly_marked_down AS
SELECT bpd.color_sku,
    bpd.date,
    bpd.store_type,
    bpd.store_warehouse_region,
    bpd.store_region_use,
    bpd.store_country,
    bpd.store_full_name,
    bpd.store_open_date,
    bpd.is_color_sku_on_order,
    CASE WHEN mk.color_sku_po IS NOT null THEN 'MD' ELSE 'FP' END AS ty_is_marked_down,  --if sku is in markdown table then sku was marked down
    CASE WHEN ly_mk.color_sku_po IS NOT null THEN 'MD' ELSE 'FP' END AS ly_is_marked_down --if sku is in markdown table then sku was marked down for last year
FROM _sku_date_region_store bpd
LEFT JOIN _marked_down_history mk --join to dim store to bring in store country and region
               ON mk.color_sku_po = bpd.color_sku
               AND mk.full_date = bpd.date
               AND mk.store_country = bpd.store_country    --- join markdown on color sku, date, and country
LEFT JOIN _marked_down_history ly_mk --join to dim store to bring in store country and region
               ON ly_mk.color_sku_po = bpd.color_sku
               AND DATEADD(YEAR,1,ly_mk.full_date) = bpd.date
               AND ly_mk.store_country = bpd.store_country;    --- join markdown on color sku, date, and country  for last year



CREATE OR REPLACE TEMP TABLE _base_dup_rows_for_marked_down AS
SELECT color_sku,
    date,
    store_type,
    store_warehouse_region,
    store_region_use,
    store_country,
    store_full_name,
    store_open_date,
    is_color_sku_on_order,
    ty_is_marked_down,
    ly_is_marked_down,
    ty_is_marked_down AS dup_is_marked_down
FROM _ty_ly_marked_down
WHERE ty_is_marked_down = ly_is_marked_down
UNION ALL
SELECT color_sku,
    date,
    store_type,
    store_warehouse_region,
    store_region_use,
    store_country,
    store_full_name,
    store_open_date,
    is_color_sku_on_order,
    ty_is_marked_down,
    ly_is_marked_down,
    'MD' AS dup_is_marked_down
FROM _ty_ly_marked_down
WHERE ty_is_marked_down <> ly_is_marked_down
UNION ALL
SELECT color_sku,
    date,
    store_type,
    store_warehouse_region,
    store_region_use,
    store_country,
    store_full_name,
    store_open_date,
    is_color_sku_on_order,
    ty_is_marked_down,
    ly_is_marked_down,
    'FP' AS dup_is_marked_down
FROM _ty_ly_marked_down
WHERE ty_is_marked_down <> ly_is_marked_down;


CREATE OR REPLACE TEMP TABLE _sku_date_warehouse_current_date AS
SELECT date, product_sku, UPPER(TRIM(w.label)) warehouse,
    CASE
        WHEN iw.warehouse_id IN (107, 154, 421, 231,466) THEN 'NA'
        WHEN warehouse ilike '%KENTUCKY%' THEN 'NA'
        WHEN warehouse ilike '%PERRIS%' THEN 'NA'
        WHEN UPPER(LEFT(WAREHOUSE, 3)) IN ('RTL', 'SXF') THEN 'NA'
        WHEN warehouse IN ('JF - UK', 'JF - UK (JFUKRETURNS)') THEN 'EU-UK'
        WHEN iw.warehouse_id IN (108, 221, 366) THEN 'EU'
        WHEN warehouse in ('SXU') THEN 'NA'
        ELSE '?'
    END AS region_new,
    CASE WHEN is_retail = 'TRUE' THEN UPPER(TRIM(w.label)) ELSE region_new END AS store_warehouse_region
FROM edw_prod.data_model_sxf.fact_inventory_system_date_history iw
JOIN lake_view.ultra_warehouse.warehouse w ON iw.warehouse_id = w.warehouse_id
WHERE brand ILIKE 'SAVAGE X'
AND (
        warehouse IN (
            'JF - KENTUCKY (JFW001)',
            'JF - KENTUCKY (JFW002)',
            'JF - KENTUCKY (SDF3)',
            'JF - PERRIS (JFPERRIS)',
            'JF - NETHERLANDS (JFEU0001)',
            'JF - UK',
            'JF - UK (JFUKRETURNS)',
            'SXU',
            'JF - TIJUANA-2 (TIJ2)'
        )
        OR warehouse LIKE 'RTLSXF%' -- old pop up retail started with RTL
        OR warehouse LIKE 'SXF%' -- retails launching in 2022 started with SXF
    )
AND date = CURRENT_DATE
GROUP BY 1,2,3,4,5;


CREATE OR REPLACE TEMP TABLE _daily_active_inventory AS
SELECT  date,
    product_sku,
    store_warehouse_region
FROM _inventory
WHERE date <> CURRENT_DATE
GROUP BY 1,2,3
UNION ALL
SELECT date,
    product_sku,
    store_warehouse_region
FROM _sku_date_warehouse_current_date
GROUP BY 1,2,3;


CREATE OR REPLACE TEMP TABLE _active_skus_daily AS
SELECT COALESCE(i.date, s.date) date, COALESCE(i.product_sku,s.color_sku) color_sku,
        COALESCE(i.store_warehouse_region,CASE
    WHEN order_store_type = 'Retail' THEN UPPER(order_store_full_name)
    ELSE store_region_abbr END) store_warehouse_region
FROM _daily_active_inventory i
FULL JOIN _sales s ON s.date = i.date AND s.color_sku = i.product_sku AND
    CASE
        WHEN order_store_type = 'Retail' THEN UPPER(order_store_full_name)
        ELSE store_region_abbr END = i.store_warehouse_region
GROUP BY 1,2,3;

// make sure column order matches _weekly_sales_inv CTE
CREATE OR REPLACE TEMPORARY TABLE _final AS
SELECT bpd.color_sku,
       sm.style_number_po                                      style_number,
       sm.department,
       sm.sub_department,
       sm.category,
       bpd.date,
       'Daily'                                  AS             date_view,
       NULL                                     AS             weekofyear,
       NULL                                     AS             yearofweek,
       NULL                                     AS             start_date,
       NULL                                     AS             end_date,
       bpd.store_type                                          order_store_type,
       bpd.store_warehouse_region,
       bpd.store_region_use                                    region,
       bpd.store_country,
       bpd.store_full_name                      AS             order_store_full_name,
       bpd.store_open_date,
       sm.site_name                             AS             descr,
       LEFT(sm.core_fashion, 4)                 AS             type,
       sm.core_fashion                          AS             core_fashion,
       sm.size_range,
       sm.savage_showroom,
       sm.first_showroom,
       sm.subcategory,
       sm.site_color,
       sm.color_family,
       sm.fabric,
       sm.persona,
       sm.collection,
       sm.active_bundle_programs                AS             bundle,
       sm.image_url,
       sm.vip_price                             AS             planned_vip,
       sm.msrp                                  AS             planned_msrp,
       sm.new_core_fashion,
       sm.color_roll_up,
       sm.fabric_grouping,
       sm.current_vip_price_on_site_na          AS              onsite_vip_price,
       sm.current_retail_price_on_site_na       AS              onsite_retail_price,
       bpd.ty_is_marked_down,
       bpd.ly_is_marked_down,
       bpd.dup_is_marked_down                                  is_marked_down,
       sm.is_prepack,
       CASE WHEN act.color_sku IS NOT NULL THEN 1 ELSE 0 END AS color_sku_is_active,
       MAX(max_order_data_through)                             max_order_data_through,
       MAX(max_inventory_data_through)                         max_inventory_data_through,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.ttl_revenue_excl_shipping) ELSE 0 END)            ty_ttl_revenue_excl_shipping,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.act_vip_revenue_excl_shipping) ELSE 0 END)        ty_act_vip_revenue_excl_shipping,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_vip_revenue_excl_shipping) ELSE 0 END)        ty_rpt_vip_revenue_excl_shipping,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.frst_guest_revenue_excl_shipping) ELSE 0 END)     ty_frst_guest_revenue_excl_shipping,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_guest_revenue_excl_shipping) ELSE 0 END)      ty_rpt_guest_revenue_excl_shipping,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.total_qty) ELSE 0 END)                            ty_total_qty,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.act_vip_ttl_qty) ELSE 0 END)                      ty_act_vip_ttl_qty,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_vip_ttl_qty) ELSE 0 END)                      ty_rpt_vip_ttl_qty,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.frst_guest_ttl_qty) ELSE 0 END)                   ty_frst_guest_ttl_qty,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_guest_ttl_qty) ELSE 0 END)                    ty_rpt_guest_ttl_qty,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.gross_margin) ELSE 0 END)                         ty_gross_margin,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.act_vip_gross_margin) ELSE 0 END)                 ty_act_vip_gross_margin,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_vip_gross_margin) ELSE 0 END)                 ty_rpt_vip_gross_margin,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.frst_guest_gross_margin) ELSE 0 END)              ty_frst_guest_gross_margin,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_guest_gross_margin) ELSE 0 END)               ty_rpt_guest_gross_margin,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.ttl_reporting_cost) ELSE 0 END)                   ty_ttl_reporting_cost,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.act_vip_reporting_cost) ELSE 0 END)               ty_act_vip_reporting_cost,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_vip_reporting_cost) ELSE 0 END)               ty_rpt_vip_reporting_cost,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.frst_guest_reporting_cost) ELSE 0 END)            ty_frst_guest_reporting_cost,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_guest_reporting_cost) ELSE 0 END)             ty_rpt_guest_reporting_cost,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.ttl_est_cost) ELSE 0 END)                         ty_ttl_est_cost,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.act_vip_est_cost) ELSE 0 END)                     ty_act_vip_est_cost,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_vip_est_cost) ELSE 0 END)                     ty_rpt_vip_est_cost,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.frst_guest_est_cost) ELSE 0 END)                  ty_frst_guest_est_cost,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_guest_est_cost) ELSE 0 END)                   ty_rpt_guest_est_cost,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.ttl_qty_returned) ELSE 0 END)                     ty_ttl_qty_returned,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.act_vip_qty_returned) ELSE 0 END)                 ty_act_vip_qty_returned,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_vip_qty_returned) ELSE 0 END)                 ty_rpt_vip_qty_returned,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.frst_guest_qty_returned) ELSE 0 END)              ty_frst_guest_qty_returned,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_guest_qty_returned) ELSE 0 END)               ty_rpt_guest_qty_returned,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.sum_initial_retail_price_excl_vat) ELSE 0 END)    ty_sum_initial_retail_price_excl_vat,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.act_vip_initial_retail_price) ELSE 0 END)         ty_act_vip_initial_retail_price,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_vip_initial_retail_price) ELSE 0 END)         ty_rpt_vip_initial_retail_price,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.frst_guest_initial_retail_price) ELSE 0 END)      ty_frst_guest_initial_retail_price,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.rpt_guest_initial_retail_price) ELSE 0 END)       ty_rpt_guest_initial_retail_price,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.sum_vip_unit_price) ELSE 0 END)                   ty_sum_vip_unit_price,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.ttl_finalsales_qty) ELSE 0 END)                   ty_ttl_finalsales_qty,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.sum_vip_unit_price_excl_vat) ELSE 0 END)          ty_sum_vip_unit_price_excl_vat,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(s.sum_msrp_unit_price) ELSE 0 END)                  ty_sum_msrp_unit_price,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_ttl_revenue_excl_shipping)  ELSE 0 END)         ly_ttl_revenue_excl_shipping,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_act_vip_revenue_excl_shipping) ELSE 0 END)     ly_act_vip_revenue_excl_shipping,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_vip_revenue_excl_shipping) ELSE 0 END)     ly_rpt_vip_revenue_excl_shipping,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_frst_guest_revenue_excl_shipping) ELSE 0 END)  ly_frst_guest_revenue_excl_shipping,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_guest_revenue_excl_shipping) ELSE 0 END)   ly_rpt_guest_revenue_excl_shipping,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_total_qty) ELSE 0 END)                         ly_total_qty,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_act_vip_ttl_qty) ELSE 0 END)                   ly_act_vip_ttl_qty,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_vip_ttl_qty) ELSE 0 END)                   ly_rpt_vip_ttl_qty,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_frst_guest_ttl_qty) ELSE 0 END)                ly_frst_guest_ttl_qty,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_guest_ttl_qty) ELSE 0 END)                 ly_rpt_guest_ttl_qty,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_gross_margin) ELSE 0 END)                      ly_gross_margin,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_act_vip_gross_margin) ELSE 0 END)              ly_act_vip_gross_margin,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_vip_gross_margin) ELSE 0 END)              ly_rpt_vip_gross_margin,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_frst_guest_gross_margin) ELSE 0 END)           ly_frst_guest_gross_margin,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_guest_gross_margin) ELSE 0 END)            ly_rpt_guest_gross_margin,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_ttl_reporting_cost) ELSE 0 END)                ly_ttl_reporting_cost,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_act_vip_reporting_cost) ELSE 0 END)            ly_act_vip_reporting_cost,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_vip_reporting_cost) ELSE 0 END)            ly_rpt_vip_reporting_cost,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_frst_guest_reporting_cost) ELSE 0 END)         ly_frst_guest_reporting_cost,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_guest_reporting_cost) ELSE 0 END)          ly_rpt_guest_reporting_cost,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_ttl_est_cost) ELSE 0 END)                      ly_ttl_est_cost,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_act_vip_est_cost) ELSE 0 END)                  ly_act_vip_est_cost,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_vip_est_cost) ELSE 0 END)                  ly_rpt_vip_est_cost,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_frst_guest_est_cost) ELSE 0 END)               ly_frst_guest_est_cost,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_guest_est_cost) ELSE 0 END)                ly_rpt_guest_est_cost,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_ttl_qty_returned) ELSE 0 END)                  ly_ttl_qty_returned,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_act_vip_qty_returned) ELSE 0 END)              ly_act_vip_qty_returned,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_vip_qty_returned) ELSE 0 END)              ly_rpt_vip_qty_returned,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_frst_guest_qty_returned) ELSE 0 END)           ly_frst_guest_qty_returned,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_guest_qty_returned) ELSE 0 END)            ly_rpt_guest_qty_returned,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_sum_initial_retail_price_excl_vat) ELSE 0 END) ly_sum_initial_retail_price_excl_vat,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_act_vip_initial_retail_price) ELSE 0 END)      ly_act_vip_initial_retail_price,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_vip_initial_retail_price) ELSE 0 END)      ly_rpt_vip_initial_retail_price,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_frst_guest_initial_retail_price) ELSE 0 END)   ly_frst_guest_initial_retail_price,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_rpt_guest_initial_retail_price) ELSE 0 END)    ly_rpt_guest_initial_retail_price,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_sum_vip_unit_price) ELSE 0 END)                ly_sum_vip_unit_price,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_ttl_finalsales_qty) ELSE 0 END)                ly_ttl_finalsales_qty,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_sum_vip_unit_price_excl_vat) ELSE 0 END)       ly_sum_vip_unit_price_excl_vat,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(s.ly_sum_msrp_unit_price) ELSE 0 END)               ly_sum_msrp_unit_price,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(i.qty_available_to_sell) ELSE 0 END) AS             qty_available_to_sell,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(i.eop_inv) ELSE 0 END)               AS             eop_inv,
       ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN SUM(i.eop_in_transit) ELSE 0 END)        AS             eop_in_transit,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(i.ly_bop_inv) ELSE 0 END)            AS             ly_bop_inv,
       ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN SUM(i.ly_eop_inv) ELSE 0 END)            AS             ly_eop_inv
FROM _base_dup_rows_for_marked_down bpd
     LEFT JOIN _sales_final AS s
               ON (s.color_sku = bpd.color_sku) AND s.date = bpd.date AND
                  bpd.store_region_use = s.store_region_abbr
                   AND bpd.store_full_name = s.order_store_full_name
     LEFT JOIN _inventory_final AS i
               ON (i.product_sku = bpd.color_sku) AND i.date = bpd.date AND i.region = bpd.store_region_use
                   AND i.store_warehouse_region = bpd.store_warehouse_region
     LEFT JOIN _on_order oo ON oo.color_sku_po = bpd.color_sku AND oo.region = bpd.store_region_use AND oo.wh_type = bpd.store_type
     LEFT JOIN _active_skus_daily act ON act.color_sku = bpd.color_sku AND act.store_warehouse_region = bpd.store_warehouse_region AND act.date = bpd.date
     LEFT JOIN (SELECT DISTINCT color_sku_po AS product_sku,
                                site_name,
                                style_number_po,
                                category,
                                core_fashion,
                                size_range,
                                savage_showroom,
                                first_showroom,
                                subcategory,
                                site_color,
                                color_family,
                                fabric,
                                persona,
                                collection,
                                active_bundle_programs,
                                image_url,
                                vip_price,
                                msrp,
                                department,
                                sub_department,
                                new_core_fashion,
                                color_roll_up,
                                fabric_grouping,
                                current_vip_price_on_site_na,
                                current_retail_price_on_site_na,
                                is_prepack
                FROM reporting_prod.sxf.view_style_master_size) sm
               ON bpd.color_sku = sm.product_sku
WHERE --COALESCE(s.color_sku, i.product_sku) IS NOT NULL AND-- remove rows where there are no sales and no inventory
    CASE WHEN is_color_sku_on_order = 1 AND oo.color_sku_po IS NULL AND s.color_sku IS NULL AND i.product_sku IS NULL THEN 0
        WHEN is_color_sku_on_order = 0 AND COALESCE(s.color_sku, i.product_sku) IS NULL THEN 0 -- remove rows where there are no sales and no inventory AND no on order
        ELSE 1 END = 1
        AND bpd.date >= '2018-05-06'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
         30, 31, 32, 33, 34, 35, 36, 37, 38,39, 40, 41, 42, 43;

CREATE OR REPLACE TEMPORARY TABLE _weekly_sales AS
SELECT color_sku,
        order_store_type,
        store_region_abbr,
        store_country,
        order_store_full_name,
        store_open_date,
        CASE
            WHEN order_store_type = 'Retail' THEN UPPER(order_store_full_name)
            ELSE UPPER(store_region_abbr)
        END AS store_warehouse_region,
        NULL     AS                                             date,
       'Weekly' AS                                             date_view,
       wr.weekofyear,
       wr.yearofweek,
       wr.start_date,
       wr.end_date,
       s.ty_is_marked_down,
       MAX(max_order_data_through)                             max_order_data_through,
       ZEROIFNULL(SUM(s.ttl_revenue_excl_shipping))         ty_ttl_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.act_vip_revenue_excl_shipping))     ty_act_vip_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.rpt_vip_revenue_excl_shipping))     ty_rpt_vip_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.frst_guest_revenue_excl_shipping))  ty_frst_guest_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.rpt_guest_revenue_excl_shipping))   ty_rpt_guest_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.total_qty))                         ty_total_qty,
       ZEROIFNULL(SUM(s.act_vip_ttl_qty))                   ty_act_vip_ttl_qty,
       ZEROIFNULL(SUM(s.rpt_vip_ttl_qty))                   ty_rpt_vip_ttl_qty,
       ZEROIFNULL(SUM(s.frst_guest_ttl_qty))                ty_frst_guest_ttl_qty,
       ZEROIFNULL(SUM(s.rpt_guest_ttl_qty))                 ty_rpt_guest_ttl_qty,
       ZEROIFNULL(SUM(s.gross_margin))                      ty_gross_margin,
       ZEROIFNULL(SUM(s.act_vip_gross_margin))              ty_act_vip_gross_margin,
       ZEROIFNULL(SUM(s.rpt_vip_gross_margin))              ty_rpt_vip_gross_margin,
       ZEROIFNULL(SUM(s.frst_guest_gross_margin))           ty_frst_guest_gross_margin,
       ZEROIFNULL(SUM(s.rpt_guest_gross_margin))            ty_rpt_guest_gross_margin,
       ZEROIFNULL(SUM(s.ttl_reporting_cost))                ty_ttl_reporting_cost,
       ZEROIFNULL(SUM(s.act_vip_reporting_cost))            ty_act_vip_reporting_cost,
       ZEROIFNULL(SUM(s.rpt_vip_reporting_cost))            ty_rpt_vip_reporting_cost,
       ZEROIFNULL(SUM(s.frst_guest_reporting_cost))         ty_frst_guest_reporting_cost,
       ZEROIFNULL(SUM(s.rpt_guest_reporting_cost))          ty_rpt_guest_reporting_cost,
       ZEROIFNULL(SUM(s.ttl_est_cost))                      ty_ttl_est_cost,
       ZEROIFNULL(SUM(s.act_vip_est_cost))                  ty_act_vip_est_cost,
       ZEROIFNULL(SUM(s.rpt_vip_est_cost))                  ty_rpt_vip_est_cost,
       ZEROIFNULL(SUM(s.frst_guest_est_cost))               ty_frst_guest_est_cost,
       ZEROIFNULL(SUM(s.rpt_guest_est_cost))                ty_rpt_guest_est_cost,
       ZEROIFNULL(SUM(s.ttl_qty_returned))                  ty_ttl_qty_returned,
       ZEROIFNULL(SUM(s.act_vip_qty_returned))              ty_act_vip_qty_returned,
       ZEROIFNULL(SUM(s.rpt_vip_qty_returned))              ty_rpt_vip_qty_returned,
       ZEROIFNULL(SUM(s.frst_guest_qty_returned))           ty_frst_guest_qty_returned,
       ZEROIFNULL(SUM(s.rpt_guest_qty_returned))            ty_rpt_guest_qty_returned,
       ZEROIFNULL(SUM(s.sum_initial_retail_price_excl_vat)) ty_sum_initial_retail_price_excl_vat,
       ZEROIFNULL(SUM(s.act_vip_initial_retail_price))      ty_act_vip_initial_retail_price,
       ZEROIFNULL(SUM(s.rpt_vip_initial_retail_price))      ty_rpt_vip_initial_retail_price,
       ZEROIFNULL(SUM(s.frst_guest_initial_retail_price))   ty_frst_guest_initial_retail_price,
       ZEROIFNULL(SUM(s.rpt_guest_initial_retail_price))    ty_rpt_guest_initial_retail_price,
       ZEROIFNULL(SUM(s.sum_vip_unit_price))                ty_sum_vip_unit_price,
       ZEROIFNULL(SUM(s.ttl_finalsales_qty))                ty_ttl_finalsales_qty,
       ZEROIFNULL(SUM(s.sum_vip_unit_price_excl_vat))       ty_sum_vip_unit_price_excl_vat,
       ZEROIFNULL(SUM(s.sum_msrp_unit_price))               ty_sum_msrp_unit_price
FROM _weeklyrange wr
LEFT JOIN (SELECT s.*,
                CASE WHEN mk.color_sku_po IS NOT NULL THEN 'MD' ELSE 'FP' END AS ty_is_marked_down
                FROM _sales_returns s
                JOIN _sku_date_region_store bpd ON (s.color_sku = bpd.color_sku)
                    AND s.date = bpd.date
                    AND bpd.store_region_use = s.store_region_abbr
                   AND bpd.store_full_name = s.order_store_full_name
               LEFT JOIN _marked_down_history mk --join to dim store to bring in store country and region
               ON mk.color_sku_po = s.color_sku
               AND mk.full_date = s.date
               AND mk.store_country = s.store_country
          ) s
               ON wr.weekofyear = WEEKOFYEAR(DATEADD(DAY, 1, s.date)) AND
                  wr.yearofweek = YEAROFWEEK(DATEADD(DAY, 1, s.date))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14;


CREATE OR REPLACE TEMPORARY TABLE _ly_weekly_sales AS
SELECT s.*,
       wr.yearofweek ly_yearofweek,
       wr.weekofyear ly_weekofyear,
       wr.start_date ly_start_date,
       wr.end_date   ly_end_date
FROM _weekly_sales s
     JOIN _weeklyrange wr
          ON s.yearofweek + 1 = wr.yearofweek AND s.weekofyear = wr.weekofyear;


CREATE OR REPLACE TEMPORARY TABLE _weekly_sales_final AS
SELECT COALESCE(s.color_sku, ly.color_sku)                                        color_sku,
       NULL     AS                                                                date,
       'Weekly' AS                                                                date_view,
       COALESCE(s.weekofyear, ly.ly_weekofyear)                                   weekofyear,
       COALESCE(s.yearofweek, ly.ly_yearofweek)                                   yearofweek,
       COALESCE(s.start_date, ly.ly_start_date)                                   start_date,
       COALESCE(s.end_date, ly.ly_end_date)                                       end_date,
       COALESCE(s.order_store_type, ly.order_store_type)                          order_store_type,
       COALESCE(s.store_warehouse_region, ly.store_warehouse_region)              store_warehouse_region,
       COALESCE(s.store_region_abbr, ly.store_region_abbr)                        region,
       COALESCE(s.store_country, ly.store_country)                                store_country,
       COALESCE(s.order_store_full_name, ly.order_store_full_name)                order_store_full_name,
       COALESCE(s.store_open_date, ly.store_open_date)                            store_open_date,
       s.ty_is_marked_down,
       ly.ty_is_marked_down ly_is_marked_down,
       MAX(COALESCE(s.max_order_data_through, ly.max_order_data_through))         max_order_data_through,
      -- MAX(COALESCE(s.max_inventory_data_through, ly.max_inventory_data_through)) max_inventory_data_through,
       SUM(COALESCE(s.ty_ttl_revenue_excl_shipping, 0))                           ty_ttl_revenue_excl_shipping,
       SUM(COALESCE(s.ty_act_vip_revenue_excl_shipping, 0))                       ty_act_vip_revenue_excl_shipping,
       SUM(COALESCE(s.ty_rpt_vip_revenue_excl_shipping, 0))                       ty_rpt_vip_revenue_excl_shipping,
       SUM(COALESCE(s.ty_frst_guest_revenue_excl_shipping, 0))                    ty_frst_guest_revenue_excl_shipping,
       SUM(COALESCE(s.ty_rpt_guest_revenue_excl_shipping, 0))                     ty_rpt_guest_revenue_excl_shipping,
       SUM(COALESCE(s.ty_total_qty, 0))                                           ty_total_qty,
       SUM(COALESCE(s.ty_act_vip_ttl_qty, 0))                                     ty_act_vip_ttl_qty,
       SUM(COALESCE(s.ty_rpt_vip_ttl_qty, 0))                                     ty_rpt_vip_ttl_qty,
       SUM(COALESCE(s.ty_frst_guest_ttl_qty, 0))                                  ty_frst_guest_ttl_qty,
       SUM(COALESCE(s.ty_rpt_guest_ttl_qty, 0))                                   ty_rpt_guest_ttl_qty,
       SUM(COALESCE(s.ty_gross_margin, 0))                                        ty_gross_margin,
       SUM(COALESCE(s.ty_act_vip_gross_margin, 0))                                ty_act_vip_gross_margin,
       SUM(COALESCE(s.ty_rpt_vip_gross_margin, 0))                                ty_rpt_vip_gross_margin,
       SUM(COALESCE(s.ty_frst_guest_gross_margin, 0))                             ty_frst_guest_gross_margin,
       SUM(COALESCE(s.ty_rpt_guest_gross_margin, 0))                              ty_rpt_guest_gross_margin,
       SUM(COALESCE(s.ty_ttl_reporting_cost, 0))                                  ty_ttl_reporting_cost,
       SUM(COALESCE(s.ty_act_vip_reporting_cost, 0))                              ty_act_vip_reporting_cost,
       SUM(COALESCE(s.ty_rpt_vip_reporting_cost, 0))                              ty_rpt_vip_reporting_cost,
       SUM(COALESCE(s.ty_frst_guest_reporting_cost, 0))                           ty_frst_guest_reporting_cost,
       SUM(COALESCE(s.ty_rpt_guest_reporting_cost, 0))                            ty_rpt_guest_reporting_cost,
       SUM(COALESCE(s.ty_ttl_est_cost, 0))                                        ty_ttl_est_cost,
       SUM(COALESCE(s.ty_act_vip_est_cost, 0))                                    ty_act_vip_est_cost,
       SUM(COALESCE(s.ty_rpt_vip_est_cost, 0))                                    ty_rpt_vip_est_cost,
       SUM(COALESCE(s.ty_frst_guest_est_cost, 0))                                 ty_frst_guest_est_cost,
       SUM(COALESCE(s.ty_rpt_guest_est_cost, 0))                                  ty_rpt_guest_est_cost,
       SUM(COALESCE(s.ty_ttl_qty_returned, 0))                                    ty_ttl_qty_returned,
       SUM(COALESCE(s.ty_act_vip_qty_returned, 0))                                ty_act_vip_qty_returned,
       SUM(COALESCE(s.ty_rpt_vip_qty_returned, 0))                                ty_rpt_vip_qty_returned,
       SUM(COALESCE(s.ty_frst_guest_qty_returned, 0))                             ty_frst_guest_qty_returned,
       SUM(COALESCE(s.ty_rpt_guest_qty_returned, 0))                              ty_rpt_guest_qty_returned,
       SUM(COALESCE(s.ty_sum_initial_retail_price_excl_vat, 0))                   ty_sum_initial_retail_price_excl_vat,
       SUM(COALESCE(s.ty_act_vip_initial_retail_price, 0))                        ty_act_vip_initial_retail_price,
       SUM(COALESCE(s.ty_rpt_vip_initial_retail_price, 0))                        ty_rpt_vip_initial_retail_price,
       SUM(COALESCE(s.ty_frst_guest_initial_retail_price, 0))                     ty_frst_guest_initial_retail_price,
       SUM(COALESCE(s.ty_rpt_guest_initial_retail_price, 0))                      ty_rpt_guest_initial_retail_price,
       SUM(COALESCE(s.ty_sum_vip_unit_price, 0))                                  ty_sum_vip_unit_price,
       SUM(COALESCE(s.ty_ttl_finalsales_qty, 0))                                  ty_ttl_finalsales_qty,
       SUM(COALESCE(s.ty_sum_vip_unit_price_excl_vat, 0))                         ty_sum_vip_unit_price_excl_vat,
       SUM(COALESCE(s.ty_sum_msrp_unit_price, 0))                                 ty_sum_msrp_unit_price,
       SUM(COALESCE(ly.ty_ttl_revenue_excl_shipping, 0))                          ly_ttl_revenue_excl_shipping,
       SUM(COALESCE(ly.ty_act_vip_revenue_excl_shipping, 0))                      ly_act_vip_revenue_excl_shipping,
       SUM(COALESCE(ly.ty_rpt_vip_revenue_excl_shipping, 0))                      ly_rpt_vip_revenue_excl_shipping,
       SUM(COALESCE(ly.ty_frst_guest_revenue_excl_shipping, 0))                   ly_frst_guest_revenue_excl_shipping,
       SUM(COALESCE(ly.ty_rpt_guest_revenue_excl_shipping, 0))                    ly_rpt_guest_revenue_excl_shipping,
       SUM(COALESCE(ly.ty_total_qty, 0))                                          ly_total_qty,
       SUM(COALESCE(ly.ty_act_vip_ttl_qty, 0))                                    ly_act_vip_ttl_qty,
       SUM(COALESCE(ly.ty_rpt_vip_ttl_qty, 0))                                    ly_rpt_vip_ttl_qty,
       SUM(COALESCE(ly.ty_frst_guest_ttl_qty, 0))                                 ly_frst_guest_ttl_qty,
       SUM(COALESCE(ly.ty_rpt_guest_ttl_qty, 0))                                  ly_rpt_guest_ttl_qty,
       SUM(COALESCE(ly.ty_gross_margin, 0))                                       ly_gross_margin,
       SUM(COALESCE(ly.ty_act_vip_gross_margin, 0))                               ly_act_vip_gross_margin,
       SUM(COALESCE(ly.ty_rpt_vip_gross_margin, 0))                               ly_rpt_vip_gross_margin,
       SUM(COALESCE(ly.ty_frst_guest_gross_margin, 0))                            ly_frst_guest_gross_margin,
       SUM(COALESCE(ly.ty_rpt_guest_gross_margin, 0))                             ly_rpt_guest_gross_margin,
       SUM(COALESCE(ly.ty_ttl_reporting_cost, 0))                                 ly_ttl_reporting_cost,
       SUM(COALESCE(ly.ty_act_vip_reporting_cost, 0))                             ly_act_vip_reporting_cost,
       SUM(COALESCE(ly.ty_rpt_vip_reporting_cost, 0))                             ly_rpt_vip_reporting_cost,
       SUM(COALESCE(ly.ty_frst_guest_reporting_cost, 0))                          ly_frst_guest_reporting_cost,
       SUM(COALESCE(ly.ty_rpt_guest_reporting_cost, 0))                           ly_rpt_guest_reporting_cost,
       SUM(COALESCE(ly.ty_ttl_est_cost, 0))                                       ly_ttl_est_cost,
       SUM(COALESCE(ly.ty_act_vip_est_cost, 0))                                   ly_act_vip_est_cost,
       SUM(COALESCE(ly.ty_rpt_vip_est_cost, 0))                                   ly_rpt_vip_est_cost,
       SUM(COALESCE(ly.ty_frst_guest_est_cost, 0))                                ly_frst_guest_est_cost,
       SUM(COALESCE(ly.ty_rpt_guest_est_cost, 0))                                 ly_rpt_guest_est_cost,
       SUM(COALESCE(ly.ty_ttl_qty_returned, 0))                                   ly_ttl_qty_returned,
       SUM(COALESCE(ly.ty_act_vip_qty_returned, 0))                               ly_act_vip_qty_returned,
       SUM(COALESCE(ly.ty_rpt_vip_qty_returned, 0))                               ly_rpt_vip_qty_returned,
       SUM(COALESCE(ly.ty_frst_guest_qty_returned, 0))                            ly_frst_guest_qty_returned,
       SUM(COALESCE(ly.ty_rpt_guest_qty_returned, 0))                             ly_rpt_guest_qty_returned,
       SUM(COALESCE(ly.ty_sum_initial_retail_price_excl_vat, 0))                  ly_sum_initial_retail_price_excl_vat,
       SUM(COALESCE(ly.ty_act_vip_initial_retail_price, 0))                       ly_act_vip_initial_retail_price,
       SUM(COALESCE(ly.ty_rpt_vip_initial_retail_price, 0))                       ly_rpt_vip_initial_retail_price,
       SUM(COALESCE(ly.ty_frst_guest_initial_retail_price, 0))                    ly_frst_guest_initial_retail_price,
       SUM(COALESCE(ly.ty_rpt_guest_initial_retail_price, 0))                     ly_rpt_guest_initial_retail_price,
       SUM(COALESCE(ly.ty_sum_vip_unit_price, 0))                                 ly_sum_vip_unit_price,
       SUM(COALESCE(ly.ty_ttl_finalsales_qty, 0))                                 ly_ttl_finalsales_qty,
       SUM(COALESCE(ly.ty_sum_vip_unit_price_excl_vat, 0))                        ly_sum_vip_unit_price_excl_vat,
       SUM(COALESCE(ly.ty_sum_msrp_unit_price, 0))                                ly_sum_msrp_unit_price
FROM _weekly_sales s
     FULL JOIN _ly_weekly_sales ly
               ON ly.color_sku = s.color_sku
               AND ly.ly_weekofyear = s.weekofyear
               AND ly.ly_yearofweek = s.yearofweek
               AND s.order_store_type = ly.order_store_type
               AND s.store_region_abbr = ly.store_region_abbr
               AND s.store_country = ly.store_country
               AND s.order_store_full_name = ly.order_store_full_name
               AND s.ty_is_marked_down = ly.ty_is_marked_down
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15;

CREATE OR REPLACE TEMP TABLE _weekly_sales_dup_md_fp AS
SELECT *,
ty_is_marked_down AS dup_is_marked_down
FROM _weekly_sales_final
WHERE ty_is_marked_down = ly_is_marked_down
UNION ALL
SELECT *,
'MD' AS dup_is_marked_down
FROM _weekly_sales_final
WHERE ty_is_marked_down IS NULL OR ly_is_marked_down IS NULL
UNION ALL
SELECT *,
'FP' AS dup_is_marked_down
FROM _weekly_sales_final
WHERE ty_is_marked_down IS NULL OR ly_is_marked_down IS NULL;

CREATE OR REPLACE TEMP TABLE _weekly_sales_md_fp AS
SELECT color_sku,
    date,
    date_view,
    weekofyear,
    yearofweek,
    start_date,
    end_date,
    order_store_type,
    store_warehouse_region,
    region,
    store_country,
    order_store_full_name,
    store_open_date,
    dup_is_marked_down,
    ty_is_marked_down,
    ly_is_marked_down,
    max_order_data_through,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_ttl_revenue_excl_shipping ELSE 0 END)                           ty_ttl_revenue_excl_shipping,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_act_vip_revenue_excl_shipping ELSE 0 END)                       ty_act_vip_revenue_excl_shipping,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_vip_revenue_excl_shipping ELSE 0 END)                       ty_rpt_vip_revenue_excl_shipping,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_frst_guest_revenue_excl_shipping ELSE 0 END)                    ty_frst_guest_revenue_excl_shipping,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_guest_revenue_excl_shipping ELSE 0 END)                     ty_rpt_guest_revenue_excl_shipping,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_total_qty ELSE 0 END)                                           ty_total_qty,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_act_vip_ttl_qty ELSE 0 END)                                     ty_act_vip_ttl_qty,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_vip_ttl_qty ELSE 0 END)                                     ty_rpt_vip_ttl_qty,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_frst_guest_ttl_qty ELSE 0 END)                                  ty_frst_guest_ttl_qty,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_guest_ttl_qty ELSE 0 END)                                   ty_rpt_guest_ttl_qty,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_gross_margin ELSE 0 END)                                        ty_gross_margin,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_act_vip_gross_margin ELSE 0 END)                                ty_act_vip_gross_margin,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_vip_gross_margin ELSE 0 END)                                ty_rpt_vip_gross_margin,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_frst_guest_gross_margin ELSE 0 END)                             ty_frst_guest_gross_margin,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_guest_gross_margin ELSE 0 END)                              ty_rpt_guest_gross_margin,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_ttl_reporting_cost ELSE 0 END)                                  ty_ttl_reporting_cost,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_act_vip_reporting_cost ELSE 0 END)                              ty_act_vip_reporting_cost,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_vip_reporting_cost ELSE 0 END)                              ty_rpt_vip_reporting_cost,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_frst_guest_reporting_cost ELSE 0 END)                           ty_frst_guest_reporting_cost,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_guest_reporting_cost ELSE 0 END)                            ty_rpt_guest_reporting_cost,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_ttl_est_cost ELSE 0 END)                                        ty_ttl_est_cost,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_act_vip_est_cost ELSE 0 END)                                    ty_act_vip_est_cost,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_vip_est_cost ELSE 0 END)                                    ty_rpt_vip_est_cost,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_frst_guest_est_cost ELSE 0 END)                                 ty_frst_guest_est_cost,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_guest_est_cost ELSE 0 END)                                  ty_rpt_guest_est_cost,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_ttl_qty_returned ELSE 0 END)                                    ty_ttl_qty_returned,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_act_vip_qty_returned ELSE 0 END)                                ty_act_vip_qty_returned,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_vip_qty_returned ELSE 0 END)                                ty_rpt_vip_qty_returned,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_frst_guest_qty_returned ELSE 0 END)                             ty_frst_guest_qty_returned,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_guest_qty_returned ELSE 0 END)                              ty_rpt_guest_qty_returned,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_sum_initial_retail_price_excl_vat ELSE 0 END)                   ty_sum_initial_retail_price_excl_vat,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_act_vip_initial_retail_price ELSE 0 END)                        ty_act_vip_initial_retail_price,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_vip_initial_retail_price ELSE 0 END)                        ty_rpt_vip_initial_retail_price,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_frst_guest_initial_retail_price ELSE 0 END)                     ty_frst_guest_initial_retail_price,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_rpt_guest_initial_retail_price ELSE 0 END)                      ty_rpt_guest_initial_retail_price,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_sum_vip_unit_price ELSE 0 END)                                  ty_sum_vip_unit_price,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_ttl_finalsales_qty ELSE 0 END)                                  ty_ttl_finalsales_qty,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_sum_vip_unit_price_excl_vat ELSE 0 END)                         ty_sum_vip_unit_price_excl_vat,
    ZEROIFNULL(CASE WHEN ty_is_marked_down = dup_is_marked_down THEN ty_sum_msrp_unit_price ELSE 0 END)                                 ty_sum_msrp_unit_price,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_ttl_revenue_excl_shipping ELSE 0 END)                          ly_ttl_revenue_excl_shipping,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_act_vip_revenue_excl_shipping ELSE 0 END)                      ly_act_vip_revenue_excl_shipping,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_vip_revenue_excl_shipping ELSE 0 END)                      ly_rpt_vip_revenue_excl_shipping,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_frst_guest_revenue_excl_shipping ELSE 0 END)                   ly_frst_guest_revenue_excl_shipping,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_guest_revenue_excl_shipping ELSE 0 END)                    ly_rpt_guest_revenue_excl_shipping,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_total_qty ELSE 0 END)                                          ly_total_qty,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_act_vip_ttl_qty ELSE 0 END)                                    ly_act_vip_ttl_qty,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_vip_ttl_qty ELSE 0 END)                                    ly_rpt_vip_ttl_qty,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_frst_guest_ttl_qty ELSE 0 END)                                 ly_frst_guest_ttl_qty,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_guest_ttl_qty ELSE 0 END)                                  ly_rpt_guest_ttl_qty,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_gross_margin ELSE 0 END)                                       ly_gross_margin,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_act_vip_gross_margin ELSE 0 END)                               ly_act_vip_gross_margin,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_vip_gross_margin ELSE 0 END)                               ly_rpt_vip_gross_margin,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_frst_guest_gross_margin ELSE 0 END)                            ly_frst_guest_gross_margin,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_guest_gross_margin ELSE 0 END)                             ly_rpt_guest_gross_margin,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_ttl_reporting_cost ELSE 0 END)                                 ly_ttl_reporting_cost,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_act_vip_reporting_cost ELSE 0 END)                             ly_act_vip_reporting_cost,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_vip_reporting_cost ELSE 0 END)                             ly_rpt_vip_reporting_cost,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_frst_guest_reporting_cost ELSE 0 END)                          ly_frst_guest_reporting_cost,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_guest_reporting_cost ELSE 0 END)                           ly_rpt_guest_reporting_cost,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_ttl_est_cost ELSE 0 END)                                       ly_ttl_est_cost,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_act_vip_est_cost ELSE 0 END)                                   ly_act_vip_est_cost,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_vip_est_cost ELSE 0 END)                                   ly_rpt_vip_est_cost,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_frst_guest_est_cost ELSE 0 END)                                ly_frst_guest_est_cost,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_guest_est_cost ELSE 0 END)                                 ly_rpt_guest_est_cost,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_ttl_qty_returned ELSE 0 END)                                   ly_ttl_qty_returned,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_act_vip_qty_returned ELSE 0 END)                               ly_act_vip_qty_returned,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_vip_qty_returned ELSE 0 END)                               ly_rpt_vip_qty_returned,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_frst_guest_qty_returned ELSE 0 END)                            ly_frst_guest_qty_returned,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_guest_qty_returned ELSE 0 END)                             ly_rpt_guest_qty_returned,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_sum_initial_retail_price_excl_vat ELSE 0 END)                  ly_sum_initial_retail_price_excl_vat,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_act_vip_initial_retail_price ELSE 0 END)                       ly_act_vip_initial_retail_price,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_vip_initial_retail_price ELSE 0 END)                       ly_rpt_vip_initial_retail_price,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_frst_guest_initial_retail_price ELSE 0 END)                    ly_frst_guest_initial_retail_price,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_rpt_guest_initial_retail_price ELSE 0 END)                     ly_rpt_guest_initial_retail_price,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_sum_vip_unit_price ELSE 0 END)                                 ly_sum_vip_unit_price,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_ttl_finalsales_qty ELSE 0 END)                                 ly_ttl_finalsales_qty,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_sum_vip_unit_price_excl_vat ELSE 0 END)                        ly_sum_vip_unit_price_excl_vat,
    ZEROIFNULL(CASE WHEN ly_is_marked_down = dup_is_marked_down THEN ly_sum_msrp_unit_price ELSE 0 END)                               ly_sum_msrp_unit_price
FROM _weekly_sales_dup_md_fp;

CREATE OR REPLACE TEMPORARY TABLE _weekly_sales_md_fp_final AS
SELECT color_sku,
    date,
    date_view,
    weekofyear,
    yearofweek,
    start_date,
    end_date,
    order_store_type,
    store_warehouse_region,
    region,
    store_country,
    order_store_full_name,
    store_open_date,
    dup_is_marked_down,
    MAX(max_order_data_through)           max_order_data_through,
    SUM(COALESCE(ty_ttl_revenue_excl_shipping, 0))                          ty_ttl_revenue_excl_shipping,
    SUM(COALESCE(ty_act_vip_revenue_excl_shipping, 0))                      ty_act_vip_revenue_excl_shipping,
    SUM(COALESCE(ty_rpt_vip_revenue_excl_shipping, 0))                      ty_rpt_vip_revenue_excl_shipping,
    SUM(COALESCE(ty_frst_guest_revenue_excl_shipping, 0))                   ty_frst_guest_revenue_excl_shipping,
    SUM(COALESCE(ty_rpt_guest_revenue_excl_shipping, 0))                    ty_rpt_guest_revenue_excl_shipping,
    SUM(COALESCE(ty_total_qty, 0))                                          ty_total_qty,
    SUM(COALESCE(ty_act_vip_ttl_qty, 0))                                    ty_act_vip_ttl_qty,
    SUM(COALESCE(ty_rpt_vip_ttl_qty, 0))                                    ty_rpt_vip_ttl_qty,
    SUM(COALESCE(ty_frst_guest_ttl_qty, 0))                                 ty_frst_guest_ttl_qty,
    SUM(COALESCE(ty_rpt_guest_ttl_qty, 0))                                  ty_rpt_guest_ttl_qty,
    SUM(COALESCE(ty_gross_margin, 0))                                       ty_gross_margin,
    SUM(COALESCE(ty_act_vip_gross_margin, 0))                               ty_act_vip_gross_margin,
    SUM(COALESCE(ty_rpt_vip_gross_margin, 0))                               ty_rpt_vip_gross_margin,
    SUM(COALESCE(ty_frst_guest_gross_margin, 0))                            ty_frst_guest_gross_margin,
    SUM(COALESCE(ty_rpt_guest_gross_margin, 0))                             ty_rpt_guest_gross_margin,
    SUM(COALESCE(ty_ttl_reporting_cost, 0))                                 ty_ttl_reporting_cost,
    SUM(COALESCE(ty_act_vip_reporting_cost, 0))                             ty_act_vip_reporting_cost,
    SUM(COALESCE(ty_rpt_vip_reporting_cost, 0))                             ty_rpt_vip_reporting_cost,
    SUM(COALESCE(ty_frst_guest_reporting_cost, 0))                          ty_frst_guest_reporting_cost,
    SUM(COALESCE(ty_rpt_guest_reporting_cost, 0))                           ty_rpt_guest_reporting_cost,
    SUM(COALESCE(ty_ttl_est_cost, 0))                                       ty_ttl_est_cost,
    SUM(COALESCE(ty_act_vip_est_cost, 0))                                   ty_act_vip_est_cost,
    SUM(COALESCE(ty_rpt_vip_est_cost, 0))                                   ty_rpt_vip_est_cost,
    SUM(COALESCE(ty_frst_guest_est_cost, 0))                                ty_frst_guest_est_cost,
    SUM(COALESCE(ty_rpt_guest_est_cost, 0))                                 ty_rpt_guest_est_cost,
    SUM(COALESCE(ty_ttl_qty_returned, 0))                                   ty_ttl_qty_returned,
    SUM(COALESCE(ty_act_vip_qty_returned, 0))                               ty_act_vip_qty_returned,
    SUM(COALESCE(ty_rpt_vip_qty_returned, 0))                               ty_rpt_vip_qty_returned,
    SUM(COALESCE(ty_frst_guest_qty_returned, 0))                            ty_frst_guest_qty_returned,
    SUM(COALESCE(ty_rpt_guest_qty_returned, 0))                             ty_rpt_guest_qty_returned,
    SUM(COALESCE(ty_sum_initial_retail_price_excl_vat, 0))                  ty_sum_initial_retail_price_excl_vat,
    SUM(COALESCE(ty_act_vip_initial_retail_price, 0))                       ty_act_vip_initial_retail_price,
    SUM(COALESCE(ty_rpt_vip_initial_retail_price, 0))                       ty_rpt_vip_initial_retail_price,
    SUM(COALESCE(ty_frst_guest_initial_retail_price, 0))                    ty_frst_guest_initial_retail_price,
    SUM(COALESCE(ty_rpt_guest_initial_retail_price, 0))                     ty_rpt_guest_initial_retail_price,
    SUM(COALESCE(ty_sum_vip_unit_price, 0))                                 ty_sum_vip_unit_price,
    SUM(COALESCE(ty_ttl_finalsales_qty, 0))                                 ty_ttl_finalsales_qty,
    SUM(COALESCE(ty_sum_vip_unit_price_excl_vat, 0))                        ty_sum_vip_unit_price_excl_vat,
    SUM(COALESCE(ty_sum_msrp_unit_price, 0))                                ty_sum_msrp_unit_price,
    SUM(COALESCE(ly_ttl_revenue_excl_shipping, 0))                          ly_ttl_revenue_excl_shipping,
    SUM(COALESCE(ly_act_vip_revenue_excl_shipping, 0))                      ly_act_vip_revenue_excl_shipping,
    SUM(COALESCE(ly_rpt_vip_revenue_excl_shipping, 0))                      ly_rpt_vip_revenue_excl_shipping,
    SUM(COALESCE(ly_frst_guest_revenue_excl_shipping, 0))                   ly_frst_guest_revenue_excl_shipping,
    SUM(COALESCE(ly_rpt_guest_revenue_excl_shipping, 0))                    ly_rpt_guest_revenue_excl_shipping,
    SUM(COALESCE(ly_total_qty, 0))                                          ly_total_qty,
    SUM(COALESCE(ly_act_vip_ttl_qty, 0))                                    ly_act_vip_ttl_qty,
    SUM(COALESCE(ly_rpt_vip_ttl_qty, 0))                                    ly_rpt_vip_ttl_qty,
    SUM(COALESCE(ly_frst_guest_ttl_qty, 0))                                 ly_frst_guest_ttl_qty,
    SUM(COALESCE(ly_rpt_guest_ttl_qty, 0))                                  ly_rpt_guest_ttl_qty,
    SUM(COALESCE(ly_gross_margin, 0))                                       ly_gross_margin,
    SUM(COALESCE(ly_act_vip_gross_margin, 0))                               ly_act_vip_gross_margin,
    SUM(COALESCE(ly_rpt_vip_gross_margin, 0))                               ly_rpt_vip_gross_margin,
    SUM(COALESCE(ly_frst_guest_gross_margin, 0))                            ly_frst_guest_gross_margin,
    SUM(COALESCE(ly_rpt_guest_gross_margin, 0))                             ly_rpt_guest_gross_margin,
    SUM(COALESCE(ly_ttl_reporting_cost, 0))                                 ly_ttl_reporting_cost,
    SUM(COALESCE(ly_act_vip_reporting_cost, 0))                             ly_act_vip_reporting_cost,
    SUM(COALESCE(ly_rpt_vip_reporting_cost, 0))                             ly_rpt_vip_reporting_cost,
    SUM(COALESCE(ly_frst_guest_reporting_cost, 0))                          ly_frst_guest_reporting_cost,
    SUM(COALESCE(ly_rpt_guest_reporting_cost, 0))                           ly_rpt_guest_reporting_cost,
    SUM(COALESCE(ly_ttl_est_cost, 0))                                       ly_ttl_est_cost,
    SUM(COALESCE(ly_act_vip_est_cost, 0))                                   ly_act_vip_est_cost,
    SUM(COALESCE(ly_rpt_vip_est_cost, 0))                                   ly_rpt_vip_est_cost,
    SUM(COALESCE(ly_frst_guest_est_cost, 0))                                ly_frst_guest_est_cost,
    SUM(COALESCE(ly_rpt_guest_est_cost, 0))                                 ly_rpt_guest_est_cost,
    SUM(COALESCE(ly_ttl_qty_returned, 0))                                   ly_ttl_qty_returned,
    SUM(COALESCE(ly_act_vip_qty_returned, 0))                               ly_act_vip_qty_returned,
    SUM(COALESCE(ly_rpt_vip_qty_returned, 0))                               ly_rpt_vip_qty_returned,
    SUM(COALESCE(ly_frst_guest_qty_returned, 0))                            ly_frst_guest_qty_returned,
    SUM(COALESCE(ly_rpt_guest_qty_returned, 0))                             ly_rpt_guest_qty_returned,
    SUM(COALESCE(ly_sum_initial_retail_price_excl_vat, 0))                  ly_sum_initial_retail_price_excl_vat,
    SUM(COALESCE(ly_act_vip_initial_retail_price, 0))                       ly_act_vip_initial_retail_price,
    SUM(COALESCE(ly_rpt_vip_initial_retail_price, 0))                       ly_rpt_vip_initial_retail_price,
    SUM(COALESCE(ly_frst_guest_initial_retail_price, 0))                    ly_frst_guest_initial_retail_price,
    SUM(COALESCE(ly_rpt_guest_initial_retail_price, 0))                     ly_rpt_guest_initial_retail_price,
    SUM(COALESCE(ly_sum_vip_unit_price, 0))                                 ly_sum_vip_unit_price,
    SUM(COALESCE(ly_ttl_finalsales_qty, 0))                                 ly_ttl_finalsales_qty,
    SUM(COALESCE(ly_sum_vip_unit_price_excl_vat, 0))                        ly_sum_vip_unit_price_excl_vat,
    SUM(COALESCE(ly_sum_msrp_unit_price, 0))                                ly_sum_msrp_unit_price
FROM _weekly_sales_md_fp s
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14;


---GET TY BOP INV
CREATE OR REPLACE TEMPORARY TABLE _weekly_inventory AS
SELECT r.product_sku,
       wr.weekofyear,
       wr.yearofweek,
       wr.start_date,
       wr.end_date,
       r.region,
       CASE WHEN mk.color_sku_po IS NOT NULL THEN 'MD' ELSE 'FP' END ty_bop_is_marked_down,
       CASE WHEN is_retail = 'Y' THEN warehouse ELSE r.region END AS store_warehouse_region,
       MAX(max_inventory_data_through)                             max_inventory_data_through,
       SUM(COALESCE(qty_available_to_sell, 0))                     qty_available_to_sell,
       SUM(COALESCE(QTY_INTRANSIT, 0))                             qty_intransit
FROM reporting_prod.sxf.view_base_inventory_dataset r
     LEFT JOIN _weeklyrange wr
               ON wr.start_date = r.date
     LEFT JOIN (SELECT product_sku, MAX(datetime) AS max_inventory_data_through
                FROM reporting_prod.sxf.view_base_inventory_dataset i
                WHERE is_current = 'Y'
                GROUP BY 1) max_inv
               ON max_inv.product_sku = r.product_sku
    LEFT JOIN (SELECT DISTINCT color_sku_po, full_date,region
               FROM _marked_down_history) mk --join to dim store to bring in store country and region
                    ON mk.color_sku_po = r.product_sku
                    AND mk.full_date =r.date
                    AND mk.region = r.region
GROUP BY r.product_sku,
         wr.weekofyear,
         wr.yearofweek,
         wr.start_date,
         wr.end_date,
         r.region,
         ty_bop_is_marked_down,
         store_warehouse_region;

---GET LY BOP INV
CREATE OR REPLACE TEMPORARY TABLE _weekly_ly_bop_inv AS
SELECT wr.start_date ly_bop_start_date,
    wr.end_date   ly_bop_end_date,
    wr.weekofyear,
    wr.yearofweek,
    product_sku,
    region,
    ty_bop_is_marked_down ly_bop_is_marked_down,
    store_warehouse_region,
    max_inventory_data_through,
    qty_available_to_sell
FROM _weeklyrange wr
JOIN _weekly_inventory inv
    ON wr.yearofweek = inv.yearofweek + 1 AND wr.weekofyear = inv.weekofyear;

---COMBINE TY AND LY BOP INV
CREATE OR REPLACE TEMPORARY TABLE _weekly_ty_ly_bop_inv AS
SELECT COALESCE(i.product_sku, ly_bop.product_sku)                                                    product_sku,
       COALESCE(i.start_date, ly_bop.ly_bop_start_date)                                               start_date,
       COALESCE(i.end_date, ly_bop.ly_bop_end_date)                                                   end_date,
       COALESCE(i.weekofyear, ly_bop.weekofyear)                                                      weekofyear,
       COALESCE(i.yearofweek, ly_bop.yearofweek)                                                      yearofweek,
       COALESCE(i.region, ly_bop.region)                                                              region,
       i.ty_bop_is_marked_down                                                                        ty_bop_is_marked_down,
       ly_bop.ly_bop_is_marked_down                                                                   ly_bop_is_marked_down,
       COALESCE(i.store_warehouse_region, ly_bop.store_warehouse_region)                              store_warehouse_region,
       COALESCE(i.max_inventory_data_through, ly_bop.max_inventory_data_through)                      max_inventory_data_through,
       SUM(COALESCE(i.qty_available_to_sell, 0))                                                      qty_available_to_sell,
       SUM(COALESCE(ly_bop.qty_available_to_sell, 0))                                                 ly_bop_inv
FROM _weekly_inventory i
FULL JOIN _weekly_ly_bop_inv ly_bop
    ON ly_bop.ly_bop_start_date = i.start_date AND ly_bop.product_sku = i.product_sku
    AND ly_bop.region = i.region AND ly_bop.store_warehouse_region = i.store_warehouse_region
    AND ly_bop.ly_bop_is_marked_down = i.ty_bop_is_marked_down
GROUP BY COALESCE(i.product_sku, ly_bop.product_sku),
        COALESCE(i.start_date, ly_bop.ly_bop_start_date),
        COALESCE(i.end_date, ly_bop.ly_bop_end_date),
        COALESCE(i.weekofyear, ly_bop.weekofyear),
        COALESCE(i.yearofweek, ly_bop.yearofweek),
        COALESCE(i.region, ly_bop.region),
        i.ty_bop_is_marked_down,
        ly_bop.ly_bop_is_marked_down,
        COALESCE(i.store_warehouse_region, ly_bop.store_warehouse_region),
        COALESCE(i.max_inventory_data_through, ly_bop.max_inventory_data_through);

--CREATE DUPLICATE ROWS WHERE TY AND LY MARKED DOWN DO NOT MATCH
CREATE OR REPLACE TEMPORARY TABLE _weekly_bop_inv_dup_md_fp AS
SELECT *,
ty_bop_is_marked_down AS dup_bop_is_marked_down
FROM _weekly_ty_ly_bop_inv
WHERE ty_bop_is_marked_down = ly_bop_is_marked_down
UNION ALL
SELECT *,
'MD' AS dup_bop_is_marked_down
FROM _weekly_ty_ly_bop_inv
WHERE ty_bop_is_marked_down IS NULL OR ly_bop_is_marked_down IS NULL
UNION ALL
SELECT *,
'FP' AS dup_bop_is_marked_down
FROM _weekly_ty_ly_bop_inv
WHERE ty_bop_is_marked_down IS NULL OR ly_bop_is_marked_down IS NULL;


--CONVERT THE DUPLICATED METRICS TO ZERO BY ADDING CASE WHEN STATEMENT. THE ROWS WERE DUPLICATED IN PREVIOUS TEMP TABLE.
CREATE OR REPLACE TEMP TABLE _weekly_bop_inv_md_fp AS
SELECT product_sku,
    start_date,
    end_date,
    weekofyear,
    yearofweek,
    region,
    ty_bop_is_marked_down,
    ly_bop_is_marked_down,
    dup_bop_is_marked_down,
    store_warehouse_region,
    max_inventory_data_through,
    ZEROIFNULL(CASE WHEN ty_bop_is_marked_down = dup_bop_is_marked_down THEN qty_available_to_sell ELSE 0 END) qty_available_to_sell,
    ZEROIFNULL(CASE WHEN ly_bop_is_marked_down = dup_bop_is_marked_down THEN ly_bop_inv ELSE 0 END) ly_bop_inv
FROM _weekly_bop_inv_dup_md_fp;

--AGGREGATE THE TY AND LY BOP DATA
CREATE OR REPLACE TEMP TABLE _weekly_bop_inv_md_fp_final AS
SELECT product_sku,
    start_date,
    end_date,
    weekofyear,
    yearofweek,
    region,
    dup_bop_is_marked_down,
    store_warehouse_region,
    max_inventory_data_through,
    SUM(qty_available_to_sell) qty_available_to_sell,
    SUM(ly_bop_inv) ly_bop_inv
FROM _weekly_bop_inv_md_fp
GROUP BY 1,2,3,4,5,6,7,8,9;

--GET TY EOP DATA
CREATE OR REPLACE TEMPORARY TABLE _weekly_eop_inv AS
SELECT DATEADD(DAY, -7, inv.start_date) eop_start_date,
       DATEADD(DAY, -7, inv.end_date)   eop_end_date,
       product_sku,
       wr.weekofyear,
       wr.yearofweek,
       region,
       ty_bop_is_marked_down ty_eop_is_marked_down,
       store_warehouse_region,
       max_inventory_data_through,
       qty_available_to_sell,
       qty_intransit
FROM _weekly_inventory inv
JOIN _weeklyrange wr
    ON DATEADD(DAY, -7, inv.start_date) = wr.start_date AND
    DATEADD(DAY, -7, inv.end_date) = wr.end_date;

--COMBINE ty/ly BOP AND TY EOP
CREATE OR REPLACE TEMPORARY TABLE _weekly_ty_eop_inv AS
SELECT COALESCE(i.product_sku, eop.product_sku)                                                    product_sku,
       COALESCE(i.start_date, eop.eop_start_date)                                           start_date,
       COALESCE(i.end_date, eop.eop_end_date)                                               end_date,
       COALESCE(i.weekofyear, eop.weekofyear)                                                      weekofyear,
       COALESCE(i.yearofweek, eop.yearofweek)                                                      yearofweek,
       COALESCE(i.region, eop.region)                                                              region,
       i.dup_bop_is_marked_down                                                                        dup_bop_is_marked_down,
       eop.ty_eop_is_marked_down                                                                   ty_eop_is_marked_down,
       COALESCE(i.store_warehouse_region, eop.store_warehouse_region)                              store_warehouse_region,
       COALESCE(i.max_inventory_data_through, eop.max_inventory_data_through)                      max_inventory_data_through,
       SUM(COALESCE(i.qty_available_to_sell, 0))                                                      qty_available_to_sell,
       SUM(COALESCE(i.ly_bop_inv, 0))                                                                   ly_bop_inv,
       SUM(COALESCE(eop.qty_available_to_sell, 0))                                                      eop_inv,
       SUM(COALESCE(eop.qty_intransit, 0))                                                              eop_intransit
FROM _weekly_bop_inv_md_fp_final i
FULL JOIN _weekly_eop_inv eop
    ON eop.eop_start_date = i.start_date AND eop.product_sku = i.product_sku
    AND eop.region = i.region AND eop.store_warehouse_region = i.store_warehouse_region
    AND eop.ty_eop_is_marked_down = i.dup_bop_is_marked_down
GROUP BY COALESCE(i.product_sku, eop.product_sku),
       COALESCE(i.start_date, eop.eop_start_date),
       COALESCE(i.end_date, eop.eop_end_date),
       COALESCE(i.weekofyear, eop.weekofyear),
       COALESCE(i.yearofweek, eop.yearofweek),
       COALESCE(i.region, eop.region),
       i.dup_bop_is_marked_down,
       eop.ty_eop_is_marked_down,
       COALESCE(i.store_warehouse_region, eop.store_warehouse_region),
       COALESCE(i.max_inventory_data_through, eop.max_inventory_data_through) ;

--CREATE DUPLICATE ROWS WHERE TY AND LY MARKED DOWN DO NOT MATCH
CREATE OR REPLACE TEMPORARY TABLE _weekly_ty_eop_inv_dup_md_fp AS
SELECT *,
dup_bop_is_marked_down AS dup_ty_eop_is_marked_down
FROM _weekly_ty_eop_inv
WHERE dup_bop_is_marked_down = ty_eop_is_marked_down
UNION ALL
SELECT *,
'MD' AS dup_ty_eop_is_marked_down
FROM _weekly_ty_eop_inv
WHERE dup_bop_is_marked_down IS NULL OR ty_eop_is_marked_down IS NULL
UNION ALL
SELECT *,
'FP' AS dup_ty_eop_is_marked_down
FROM _weekly_ty_eop_inv
WHERE dup_bop_is_marked_down IS NULL OR ty_eop_is_marked_down IS NULL;

--CONVERT THE DUPLICATED METRICS TO ZERO BY ADDING CASE WHEN STATEMENT. THE ROWS WERE DUPLICATED IN PREVIOUS TEMP TABLE.
CREATE OR REPLACE TEMP TABLE _weekly_ty_eop_inv_md_fp AS
SELECT product_sku,
    start_date,
    end_date,
    weekofyear,
    yearofweek,
    region,
    dup_bop_is_marked_down,
    ty_eop_is_marked_down,
    dup_ty_eop_is_marked_down,
    store_warehouse_region,
    max_inventory_data_through,
    ZEROIFNULL(CASE WHEN dup_bop_is_marked_down = dup_ty_eop_is_marked_down THEN qty_available_to_sell ELSE 0 END) qty_available_to_sell,
    ZEROIFNULL(CASE WHEN dup_bop_is_marked_down = dup_ty_eop_is_marked_down THEN ly_bop_inv ELSE 0 END) ly_bop_inv,
    ZEROIFNULL(CASE WHEN ty_eop_is_marked_down = dup_ty_eop_is_marked_down THEN eop_inv ELSE 0 END) eop_inv,
    ZEROIFNULL(CASE WHEN ty_eop_is_marked_down = dup_ty_eop_is_marked_down THEN eop_intransit ELSE 0 END) eop_intransit
FROM _weekly_ty_eop_inv_dup_md_fp;

--AGGREGATE THE TY/LY BOP AND TY EOP DATA
CREATE OR REPLACE TEMP TABLE _weekly_ty_eop_inv_md_fp_final AS
SELECT product_sku,
    start_date,
    end_date,
    weekofyear,
    yearofweek,
    region,
    dup_ty_eop_is_marked_down,
    store_warehouse_region,
    max_inventory_data_through,
    SUM(qty_available_to_sell) qty_available_to_sell,
    SUM(ly_bop_inv) ly_bop_inv,
    SUM(eop_inv) eop_inv,
    SUM(eop_intransit) eop_intransit
FROM _weekly_ty_eop_inv_md_fp
GROUP BY 1,2,3,4,5,6,7,8,9;

--GET LY EOP DATE
CREATE OR REPLACE TEMPORARY TABLE _weekly_ly_eop_inv AS
SELECT wr.start_date ly_eop_start_date,
    wr.end_date   ly_eop_end_date,
    wr.weekofyear,
    wr.yearofweek,
    product_sku,
    region,
    ty_eop_is_marked_down ly_eop_is_marked_down,
    store_warehouse_region,
    max_inventory_data_through,
    qty_available_to_sell,
    qty_intransit
FROM _weeklyrange wr
JOIN _weekly_eop_inv inv
    ON wr.yearofweek = inv.yearofweek + 1 AND wr.weekofyear = inv.weekofyear;

--COMBINE TY/LY BOP, TY EOP AND LY EOP
CREATE OR REPLACE TEMPORARY TABLE _weekly_ty_ly_eop_inv AS
SELECT COALESCE(i.product_sku, eop.product_sku)                                                    product_sku,
       COALESCE(i.start_date, eop.ly_eop_start_date)                                           start_date,
       COALESCE(i.end_date, eop.ly_eop_end_date)                                               end_date,
       COALESCE(i.weekofyear, eop.weekofyear)                                                      weekofyear,
       COALESCE(i.yearofweek, eop.yearofweek)                                                      yearofweek,
       COALESCE(i.region, eop.region)                                                              region,
       i.dup_ty_eop_is_marked_down                                                                  dup_ty_eop_is_marked_down,
       eop.ly_eop_is_marked_down                                                                   ly_eop_is_marked_down,
       COALESCE(i.store_warehouse_region, eop.store_warehouse_region)                              store_warehouse_region,
       COALESCE(i.max_inventory_data_through, eop.max_inventory_data_through)                      max_inventory_data_through,
        SUM(COALESCE(i.qty_available_to_sell, 0))                                                      qty_available_to_sell,
        SUM(COALESCE(i.ly_bop_inv, 0))                                                      ly_bop_inv,
        SUM(COALESCE(i.eop_inv, 0))                                                      eop_inv,
       SUM(COALESCE(eop.qty_available_to_sell, 0))                                                      ly_eop_inv,
       SUM(COALESCE(i.eop_intransit, 0))                                                            eop_intransit
FROM _weekly_ty_eop_inv_md_fp_final i
FULL JOIN _weekly_ly_eop_inv eop
    ON eop.ly_eop_start_date = i.start_date AND eop.product_sku = i.product_sku
    AND eop.region = i.region AND eop.store_warehouse_region = i.store_warehouse_region
    AND eop.ly_eop_is_marked_down = i.dup_ty_eop_is_marked_down
GROUP BY COALESCE(i.product_sku, eop.product_sku),
       COALESCE(i.start_date, eop.ly_eop_start_date),
       COALESCE(i.end_date, eop.ly_eop_end_date),
       COALESCE(i.weekofyear, eop.weekofyear),
       COALESCE(i.yearofweek, eop.yearofweek),
       COALESCE(i.region, eop.region),
       i.dup_ty_eop_is_marked_down,
       eop.ly_eop_is_marked_down,
       COALESCE(i.store_warehouse_region, eop.store_warehouse_region),
       COALESCE(i.max_inventory_data_through, eop.max_inventory_data_through) ;

--CREATE DUPLICATE ROWS WHERE TY AND LY MARKED DOWN DO NOT MATCH
CREATE OR REPLACE TEMPORARY TABLE _weekly_ly_eop_inv_dup_md_fp AS
SELECT *,
dup_ty_eop_is_marked_down AS dup_ly_eop_is_marked_down
FROM _weekly_ty_ly_eop_inv
WHERE dup_ty_eop_is_marked_down = ly_eop_is_marked_down
UNION ALL
SELECT *,
'MD' AS dup_ly_eop_is_marked_down
FROM _weekly_ty_ly_eop_inv
WHERE dup_ty_eop_is_marked_down IS NULL OR ly_eop_is_marked_down IS NULL
UNION ALL
SELECT *,
'FP' AS dup_ly_eop_is_marked_down
FROM _weekly_ty_ly_eop_inv
WHERE dup_ty_eop_is_marked_down IS NULL OR ly_eop_is_marked_down IS NULL;

--CONVERT THE DUPLICATED METRICS TO ZERO BY ADDING CASE WHEN STATEMENT. THE ROWS WERE DUPLICATED IN PREVIOUS TEMP TABLE
CREATE OR REPLACE TEMP TABLE _weekly_ly_eop_inv_md_fp AS
SELECT product_sku,
    start_date,
    end_date,
    weekofyear,
    yearofweek,
    region,
    dup_ty_eop_is_marked_down,
    ly_eop_is_marked_down,
    dup_ly_eop_is_marked_down,
    store_warehouse_region,
    max_inventory_data_through,
    ZEROIFNULL(CASE WHEN dup_ty_eop_is_marked_down = dup_ly_eop_is_marked_down THEN qty_available_to_sell ELSE 0 END) qty_available_to_sell,
    ZEROIFNULL(CASE WHEN dup_ty_eop_is_marked_down = dup_ly_eop_is_marked_down THEN ly_bop_inv ELSE 0 END) ly_bop_inv,
    ZEROIFNULL(CASE WHEN dup_ty_eop_is_marked_down = dup_ly_eop_is_marked_down THEN eop_inv ELSE 0 END) eop_inv,
    ZEROIFNULL(CASE WHEN ly_eop_is_marked_down = dup_ly_eop_is_marked_down THEN ly_eop_inv ELSE 0 END) ly_eop_inv,
    ZEROIFNULL(CASE WHEN dup_ty_eop_is_marked_down = dup_ly_eop_is_marked_down THEN eop_intransit ELSE 0 END) eop_intransit
FROM _weekly_ly_eop_inv_dup_md_fp;

--AGGREGATE THE TY/LY BOP AND TY/LY EOP DATA
CREATE OR REPLACE TEMP TABLE _weekly_ly_eop_inv_md_fp_final AS
SELECT product_sku,
    start_date,
    end_date,
    weekofyear,
    yearofweek,
    region,
    dup_ly_eop_is_marked_down,
    store_warehouse_region,
    max_inventory_data_through,
    SUM(qty_available_to_sell) qty_available_to_sell,
    SUM(ly_bop_inv) ly_bop_inv,
    SUM(eop_inv) eop_inv,
    SUM(ly_eop_inv) ly_eop_inv,
    SUM(eop_intransit) eop_intransit
FROM _weekly_ly_eop_inv_md_fp
GROUP BY 1,2,3,4,5,6,7,8,9;

---ADD MD FP ROWS ACROSS ALL THE WEEKS
CREATE OR REPLACE TEMP TABLE _weekly_region_store_md_fp AS
SELECT color_sku,
    store_type,
    store_country,
    store_full_name,
    store_region_use,
    store_warehouse_region,
    store_open_date,
    is_color_sku_on_order,
    weekofyear,
    yearofweek,
    start_date,
    end_date,
    'MD' AS is_marked_down
FROM _weekly_region_store w
LEFT JOIN (SELECT color_sku_po
           FROM reporting_prod.sxf.style_master
          WHERE first_occurrence_date IS NULL AND first_inventory_occurrence_date IS NULL) sm ON sm.color_sku_po = w.color_sku
WHERE sm.color_sku_po IS NULL
UNION ALL
SELECT color_sku,
    store_type,
    store_country,
    store_full_name,
    store_region_use,
    store_warehouse_region,
    store_open_date,
    is_color_sku_on_order,
    weekofyear,
    yearofweek,
    start_date,
    end_date,
    'FP' AS is_marked_down
FROM _weekly_region_store;

CREATE OR REPLACE TEMP TABLE _weekly_active_inventory AS
SELECT
start_date,
product_sku,
store_warehouse_region
FROM _weekly_inventory
WHERE start_date <> CURRENT_DATE
GROUP BY 1,2,3
UNION ALL
SELECT
date,
product_sku,
store_warehouse_region
FROM _sku_date_warehouse_current_date
GROUP BY 1,2,3;


CREATE OR REPLACE TEMP TABLE _active_skus_weekly AS
SELECT COALESCE(i.start_date, s.start_date) start_date,
COALESCE(i.product_sku,s.color_sku) color_sku,
COALESCE(i.store_warehouse_region,CASE
    WHEN order_store_type = 'Retail' THEN UPPER(order_store_full_name)
    ELSE store_region_abbr END) store_warehouse_region
FROM _weekly_active_inventory i
FULL JOIN _weekly_sales s ON s.start_date = i.start_date AND s.color_sku = i.product_sku AND
CASE
    WHEN order_store_type = 'Retail' THEN UPPER(order_store_full_name)
    ELSE store_region_abbr END = i.store_warehouse_region
GROUP BY 1,2,3;


// make sure column order matches _final CTE
CREATE OR REPLACE TEMPORARY TABLE _weekly_sales_inv AS
SELECT wr.color_sku,
       sm.style_number_po                                      style_numbe,
       sm.department,
       sm.sub_department,
       sm.category,
       NULL                                       AS           date,
       'Weekly'                                   AS           date_view,
       wr.weekofyear,
       wr.yearofweek,
       wr.start_date,
       wr.end_date,
       wr.store_type                                           order_store_type,
       wr.store_warehouse_region,
       wr.store_region_use                                     region,
       wr.store_country,
       wr.store_full_name                                      order_store_full_name,
       wr.store_open_date,
       sm.site_name                               AS           descr,
       LEFT(sm.core_fashion, 4)                   AS           type,
       sm.core_fashion                            AS           core_fashion,
       sm.size_range,
       sm.savage_showroom,
       sm.first_showroom,
       sm.subcategory,
       sm.site_color,
       sm.color_family,
       sm.fabric,
       sm.persona,
       sm.collection,
       sm.active_bundle_programs                  AS           bundle,
       sm.image_url,
       sm.vip_price                               AS           planned_vip,
       sm.msrp                                    AS           planned_msrp,
       sm.new_core_fashion,
       sm.color_roll_up,
       sm.fabric_grouping,
       sm.current_vip_price_on_site_na            AS           onsite_vip_price,
       sm.current_retail_price_on_site_na         AS           onsite_retail_price,
       s.dup_is_marked_down,
       inv.dup_ly_eop_is_marked_down,
       wr.is_marked_down,
       sm.IS_PREPACK,
       CASE WHEN act.color_sku IS NOT NULL THEN 1 ELSE 0 END AS color_sku_is_active,
       MAX(s.max_order_data_through)                           max_order_data_through,
       MAX(inv.max_inventory_data_through)                       max_inventory_data_through,
       ZEROIFNULL(SUM(s.ty_ttl_revenue_excl_shipping))         ty_ttl_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.ty_act_vip_revenue_excl_shipping))     ty_act_vip_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.ty_rpt_vip_revenue_excl_shipping))     ty_rpt_vip_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.ty_frst_guest_revenue_excl_shipping))  ty_frst_guest_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.ty_rpt_guest_revenue_excl_shipping))   ty_rpt_guest_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.ty_total_qty))                         ty_total_qty,
       ZEROIFNULL(SUM(s.ty_act_vip_ttl_qty))                   ty_act_vip_ttl_qty,
       ZEROIFNULL(SUM(s.ty_rpt_vip_ttl_qty))                   ty_rpt_vip_ttl_qty,
       ZEROIFNULL(SUM(s.ty_frst_guest_ttl_qty))                ty_frst_guest_ttl_qty,
       ZEROIFNULL(SUM(s.ty_rpt_guest_ttl_qty))                 ty_rpt_guest_ttl_qty,
       ZEROIFNULL(SUM(s.ty_gross_margin))                      ty_gross_margin,
       ZEROIFNULL(SUM(s.ty_act_vip_gross_margin))              ty_act_vip_gross_margin,
       ZEROIFNULL(SUM(s.ty_rpt_vip_gross_margin))              ty_rpt_vip_gross_margin,
       ZEROIFNULL(SUM(s.ty_frst_guest_gross_margin))           ty_frst_guest_gross_margin,
       ZEROIFNULL(SUM(s.ty_rpt_guest_gross_margin))            ty_rpt_guest_gross_margin,
       ZEROIFNULL(SUM(s.ty_ttl_reporting_cost))                ty_ttl_reporting_cost,
       ZEROIFNULL(SUM(s.ty_act_vip_reporting_cost))            ty_act_vip_reporting_cost,
       ZEROIFNULL(SUM(s.ty_rpt_vip_reporting_cost))            ty_rpt_vip_reporting_cost,
       ZEROIFNULL(SUM(s.ty_frst_guest_reporting_cost))         ty_frst_guest_reporting_cost,
       ZEROIFNULL(SUM(s.ty_rpt_guest_reporting_cost))          ty_rpt_guest_reporting_cost,
       ZEROIFNULL(SUM(s.ty_ttl_est_cost))                      ty_ttl_est_cost,
       ZEROIFNULL(SUM(s.ty_act_vip_est_cost))                  ty_act_vip_est_cost,
       ZEROIFNULL(SUM(s.ty_rpt_vip_est_cost))                  ty_rpt_vip_est_cost,
       ZEROIFNULL(SUM(s.ty_frst_guest_est_cost))               ty_frst_guest_est_cost,
       ZEROIFNULL(SUM(s.ty_rpt_guest_est_cost))                ty_rpt_guest_est_cost,
       ZEROIFNULL(SUM(s.ty_ttl_qty_returned))                  ty_ttl_qty_returned,
       ZEROIFNULL(SUM(s.ty_act_vip_qty_returned))              ty_act_vip_qty_returned,
       ZEROIFNULL(SUM(s.ty_rpt_vip_qty_returned))              ty_rpt_vip_qty_returned,
       ZEROIFNULL(SUM(s.ty_frst_guest_qty_returned))           ty_frst_guest_qty_returned,
       ZEROIFNULL(SUM(s.ty_rpt_guest_qty_returned))            ty_rpt_guest_qty_returned,
       ZEROIFNULL(SUM(s.ty_sum_initial_retail_price_excl_vat)) ty_sum_initial_retail_price_excl_vat,
       ZEROIFNULL(SUM(s.ty_act_vip_initial_retail_price))      ty_act_vip_initial_retail_price,
       ZEROIFNULL(SUM(s.ty_rpt_vip_initial_retail_price))      ty_rpt_vip_initial_retail_price,
       ZEROIFNULL(SUM(s.ty_frst_guest_initial_retail_price))   ty_frst_guest_initial_retail_price,
       ZEROIFNULL(SUM(s.ty_rpt_guest_initial_retail_price))    ty_rpt_guest_initial_retail_price,
       ZEROIFNULL(SUM(s.ty_sum_vip_unit_price))                ty_sum_vip_unit_price,
       ZEROIFNULL(SUM(s.ty_ttl_finalsales_qty))                ty_ttl_finalsales_qty,
       ZEROIFNULL(SUM(s.ty_sum_vip_unit_price_excl_vat))       ty_sum_vip_unit_price_excl_vat,
       ZEROIFNULL(SUM(s.ty_sum_msrp_unit_price))               ty_sum_msrp_unit_price,
       ZEROIFNULL(SUM(s.ly_ttl_revenue_excl_shipping))         ly_ttl_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.ly_act_vip_revenue_excl_shipping))     ly_act_vip_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.ly_rpt_vip_revenue_excl_shipping))     ly_rpt_vip_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.ly_frst_guest_revenue_excl_shipping))  ly_frst_guest_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.ly_rpt_guest_revenue_excl_shipping))   ly_rpt_guest_revenue_excl_shipping,
       ZEROIFNULL(SUM(s.ly_total_qty))                         ly_total_qty,
       ZEROIFNULL(SUM(s.ly_act_vip_ttl_qty))                   ly_act_vip_ttl_qty,
       ZEROIFNULL(SUM(s.ly_rpt_vip_ttl_qty))                   ly_rpt_vip_ttl_qty,
       ZEROIFNULL(SUM(s.ly_frst_guest_ttl_qty))                ly_frst_guest_ttl_qty,
       ZEROIFNULL(SUM(s.ly_rpt_guest_ttl_qty))                 ly_rpt_guest_ttl_qty,
       ZEROIFNULL(SUM(s.ly_gross_margin))                      ly_gross_margin,
       ZEROIFNULL(SUM(s.ly_act_vip_gross_margin))              ly_act_vip_gross_margin,
       ZEROIFNULL(SUM(s.ly_rpt_vip_gross_margin))              ly_rpt_vip_gross_margin,
       ZEROIFNULL(SUM(s.ly_frst_guest_gross_margin))           ly_frst_guest_gross_margin,
       ZEROIFNULL(SUM(s.ly_rpt_guest_gross_margin))            ly_rpt_guest_gross_margin,
       ZEROIFNULL(SUM(s.ly_ttl_reporting_cost))                ly_ttl_reporting_cost,
       ZEROIFNULL(SUM(s.ly_act_vip_reporting_cost))            ly_act_vip_reporting_cost,
       ZEROIFNULL(SUM(s.ly_rpt_vip_reporting_cost))            ly_rpt_vip_reporting_cost,
       ZEROIFNULL(SUM(s.ly_frst_guest_reporting_cost))         ly_frst_guest_reporting_cost,
       ZEROIFNULL(SUM(s.ly_rpt_guest_reporting_cost))          ly_rpt_guest_reporting_cost,
       ZEROIFNULL(SUM(s.ly_ttl_est_cost))                      ly_ttl_est_cost,
       ZEROIFNULL(SUM(s.ly_act_vip_est_cost))                  ly_act_vip_est_cost,
       ZEROIFNULL(SUM(s.ly_rpt_vip_est_cost))                  ly_rpt_vip_est_cost,
       ZEROIFNULL(SUM(s.ly_frst_guest_est_cost))               ly_frst_guest_est_cost,
       ZEROIFNULL(SUM(s.ly_rpt_guest_est_cost))                ly_rpt_guest_est_cost,
       ZEROIFNULL(SUM(s.ly_ttl_qty_returned))                  ly_ttl_qty_returned,
       ZEROIFNULL(SUM(s.ly_act_vip_qty_returned))              ly_act_vip_qty_returned,
       ZEROIFNULL(SUM(s.ly_rpt_vip_qty_returned))              ly_rpt_vip_qty_returned,
       ZEROIFNULL(SUM(s.ly_frst_guest_qty_returned))           ly_frst_guest_qty_returned,
       ZEROIFNULL(SUM(s.ly_rpt_guest_qty_returned))            ly_rpt_guest_qty_returned,
       ZEROIFNULL(SUM(s.ly_sum_initial_retail_price_excl_vat)) ly_sum_initial_retail_price_excl_vat,
       ZEROIFNULL(SUM(s.ly_act_vip_initial_retail_price))      ly_act_vip_initial_retail_price,
       ZEROIFNULL(SUM(s.ly_rpt_vip_initial_retail_price))      ly_rpt_vip_initial_retail_price,
       ZEROIFNULL(SUM(s.ly_frst_guest_initial_retail_price))   ly_frst_guest_initial_retail_price,
       ZEROIFNULL(SUM(s.ly_rpt_guest_initial_retail_price))    ly_rpt_guest_initial_retail_price,
       ZEROIFNULL(SUM(s.ly_sum_vip_unit_price))                ly_sum_vip_unit_price,
       ZEROIFNULL(SUM(s.ly_ttl_finalsales_qty))                ly_ttl_finalsales_qty,
       ZEROIFNULL(SUM(s.ly_sum_vip_unit_price_excl_vat))       ly_sum_vip_unit_price_excl_vat,
       ZEROIFNULL(SUM(s.ly_sum_msrp_unit_price))               ly_sum_msrp_unit_price,
       ZEROIFNULL(SUM(inv.qty_available_to_sell)) AS           qty_available_to_sell,
       ZEROIFNULL(SUM(inv.eop_inv))               AS           eop_inv,
       ZEROIFNULL(SUM(inv.eop_intransit))         AS           eop_intransit,
       ZEROIFNULL(SUM(inv.ly_bop_inv))            AS           ly_bop_inv,
       ZEROIFNULL(SUM(inv.ly_eop_inv))            AS           ly_eop_inv
FROM _weekly_region_store_md_fp wr
     LEFT JOIN _weekly_sales_md_fp_final s
               ON s.color_sku = wr.color_sku AND s.weekofyear = wr.weekofyear AND s.yearofweek = wr.yearofweek
                   AND s.store_warehouse_region = wr.store_warehouse_region AND
                  s.region = wr.store_region_use AND s.order_store_type = wr.store_type
                   AND s.store_country = wr.store_country AND s.order_store_full_name = wr.store_full_name
                   AND wr.is_marked_down = s.dup_is_marked_down
     LEFT JOIN _weekly_ly_eop_inv_md_fp_final inv
               ON wr.color_sku = inv.product_sku AND wr.weekofyear = inv.weekofyear
                   AND wr.yearofweek = inv.yearofweek AND wr.store_region_use = inv.region AND
                  wr.store_warehouse_region = inv.store_warehouse_region
                  AND wr.is_marked_down = inv.dup_ly_eop_is_marked_down
     LEFT JOIN _on_order oo ON oo.color_sku_po = wr.color_sku AND oo.region = wr.store_region_use AND oo.wh_type = wr.store_type
     LEFT JOIN _active_skus_weekly act ON act.color_sku = wr.color_sku AND act.start_date = wr.start_date AND act.store_warehouse_region = wr.store_warehouse_region
     LEFT JOIN (SELECT DISTINCT color_sku_po AS product_sku,
                                site_name,
                                style_number_po,
                                category,
                                core_fashion,
                                size_range,
                                savage_showroom,
                                first_showroom,
                                subcategory,
                                site_color,
                                color_family,
                                fabric,
                                persona,
                                collection,
                                active_bundle_programs,
                                image_url,
                                vip_price,
                                msrp,
                                department,
                                sub_department,
                                new_core_fashion,
                                color_roll_up,
                                fabric_grouping,
                                current_vip_price_on_site_na,
                                current_retail_price_on_site_na,
                                is_prepack
                FROM reporting_prod.sxf.view_style_master_size) sm
               ON wr.color_sku = sm.product_sku
WHERE --COALESCE(s.color_sku, inv.product_sku) IS NOT NULL AND
CASE WHEN is_color_sku_on_order = 1 AND oo.color_sku_po IS NULL AND s.color_sku IS NULL AND inv.product_sku IS NULL THEN 0
        WHEN is_color_sku_on_order = 0 AND COALESCE(s.color_sku, inv.product_sku) IS NULL THEN 0 -- remove rows where there are no sales and no inventory AND no on order
        ELSE 1 END = 1
        AND wr.start_date >= '2018-05-06'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
         30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43;


// make sure column order is the same for both unioned CTEs
CREATE OR REPLACE TEMPORARY TABLE _daily_weekly_final AS
SELECT *
FROM _final
UNION
SELECT *
FROM _weekly_sales_inv s;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.sxf.category_performance AS
SELECT df.*,
    COALESCE(oo.three_months_arrival_date_qty,0) AS three_months_arrival_date_qty,
    COALESCE(oo.six_months_arrival_date_qty,0) AS six_months_arrival_date_qty,
    Product_status AS NA_Product_Status
FROM _daily_weekly_final df
LEFT JOIN _on_order oo ON oo.color_sku_po = df.color_sku
                       AND oo.region = df.region
                       AND oo.wh_type = df.order_store_type
LEFT JOIN reporting_prod.sxf.style_master ds ON ds.color_sku_po = df.color_sku;
