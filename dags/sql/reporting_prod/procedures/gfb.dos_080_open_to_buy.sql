SET start_date = CAST('2019-01-01' AS DATE);
SET end_date = CURRENT_DATE();


CREATE OR REPLACE TEMPORARY TABLE _received_qty AS
SELECT mds.business_unit
     , mds.country
     , mds.department
     , mds.vendor
     , mds.department_detail
     , mds.is_plussize
     , mds.shared
     , mds.region
     , mds.latest_launch_date
     , mds.previous_launch_date
     , mds.subcategory
     , (CASE
            WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, pds.date_received)) < 0
                THEN 'FUTURE SHOWROOM'
            WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, pds.date_received)) < 9 THEN 'CURRENT'
            WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, pds.date_received)) < 15 THEN 'TIER 1'
            WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, pds.date_received)) < 24 THEN 'TIER 2'
            WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, pds.date_received)) >= 24 THEN 'TIER 3'
    END)                                        AS tier
     , mds.product_sku
     , pds.date_received
     , mds.og_retail
     , mds.current_vip_retail
     , SUM(pds.qty_receipt)                     AS qty_received
     , SUM(pds.landed_cost * (pds.qty_receipt)) AS landed_cost_received
     , SUM(COALESCE(IFF(mds.og_retail = 0, NULL, mds.og_retail), mds.current_vip_retail) *
           (pds.qty_receipt))                   AS received_retail
FROM reporting_prod.gfb.gfb_po_data_set pds
         JOIN reporting_prod.gfb.merch_dim_product mds
              ON mds.product_sku = pds.product_sku
                  AND LOWER(mds.business_unit) = LOWER(pds.business_unit)
                  AND LOWER(mds.region) = LOWER(pds.region)
                  AND LOWER(mds.country) = LOWER(pds.country)
WHERE pds.date_received >= $start_date
  AND pds.date_received < $end_date
GROUP BY mds.business_unit
       , mds.country
       , mds.department
       , mds.department_detail
       , mds.vendor
       , mds.is_plussize
       , mds.shared
       , mds.region
       , mds.latest_launch_date
       , mds.previous_launch_date
       , mds.subcategory
       , (CASE
              WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, pds.date_received)) < 0
                  THEN 'FUTURE SHOWROOM'
              WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, pds.date_received)) < 9 THEN 'CURRENT'
              WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, pds.date_received)) < 15 THEN 'TIER 1'
              WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, pds.date_received)) < 24 THEN 'TIER 2'
              WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, pds.date_received)) >= 24 THEN 'TIER 3'
    END)
       , mds.product_sku
       , pds.date_received
       , mds.og_retail
       , mds.current_vip_retail;


CREATE OR REPLACE TEMPORARY TABLE _sales AS
SELECT mds.business_unit
     , mds.country
     , mds.department
     , mds.department_detail
     , mds.is_plussize
     , mds.shared
     , mds.date
     , mds.vendor
     , mds.region
     , mds.latest_launch_date
     , mds.previous_launch_date
     , mds.subcategory
     , (CASE
            WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, mds.date)) < 0 THEN 'FUTURE SHOWROOM'
            WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, mds.date)) < 9 THEN 'CURRENT'
            WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, mds.date)) < 15 THEN 'TIER 1'
            WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, mds.date)) < 24 THEN 'TIER 2'
            WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, mds.date)) >= 24 THEN 'TIER 3'
    END)                                               AS tier
     , mds.product_sku
     , mds.clearance_flag
     , mds.og_retail
     , mds.current_vip_retail
     , SUM(mds.total_qty_sold)                         AS total_qty_sold
     , SUM(mds.total_product_revenue)                  AS total_product_revenue
     , SUM(mds.total_product_revenue_with_tariff)      AS total_product_revenue_with_tariff
     , SUM(CASE
               WHEN mds.date = LAST_DAY(mds.date)
                   THEN mds.qty_available_to_sell
               ELSE 0 END)                             AS bop_available_to_sell
     , SUM(mds.total_cogs)                             AS total_cogs
     , SUM(mds.total_discount)                         AS total_discount
     , SUM(mds.activating_qty_sold)                    AS activating_qty_sold
     , SUM(mds.activating_product_revenue)             AS activating_product_revenue
     , SUM(mds.activating_product_revenue_with_tariff) AS activating_product_revenue_with_tariff
     , SUM(mds.activating_cogs)                        AS activating_cogs
     , SUM(mds.activating_discount)                    AS activating_discount
     , SUM(mds.repeat_qty_sold)                        AS repeat_qty_sold
     , SUM(mds.repeat_product_revenue)                 AS repeat_product_revenue
     , SUM(mds.repeat_product_revenue_with_tariff)     AS repeat_product_revenue_with_tariff
     , SUM(mds.repeat_cogs)                            AS repeat_cogs
     , SUM(mds.repeat_discount)                        AS repeat_discount
     , SUM(CASE
               WHEN mds.date = LAST_DAY(mds.date)
                   THEN mds.qty_onhand
               ELSE 0 END)                             AS bop_qty_onhand
     , SUM(CASE
               WHEN mds.date = LAST_DAY(mds.date)
                   THEN mds.qty_intransit
               ELSE 0 END)                             AS bop_qty_intransit
FROM reporting_prod.gfb.dos_107_merch_data_set_by_ship_date mds
GROUP BY mds.business_unit
       , mds.country
       , mds.department
       , mds.department_detail
       , mds.is_plussize
       , mds.shared
       , mds.vendor
       , mds.date
       , mds.region
       , mds.latest_launch_date
       , mds.previous_launch_date
       , mds.subcategory
       , (CASE
              WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, mds.date)) < 0 THEN 'FUTURE SHOWROOM'
              WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, mds.date)) < 9 THEN 'CURRENT'
              WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, mds.date)) < 15 THEN 'TIER 1'
              WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, mds.date)) < 24 THEN 'TIER 2'
              WHEN DATEDIFF(MONTH, mds.latest_launch_date, DATE_TRUNC(MONTH, mds.date)) >= 24 THEN 'TIER 3'
    END)
       , mds.product_sku
       , mds.clearance_flag
       , mds.og_retail
       , mds.current_vip_retail;


CREATE OR REPLACE TEMPORARY TABLE _sales_received AS
SELECT COALESCE(s.business_unit, rq.business_unit)                AS business_unit
     , COALESCE(s.country, rq.country)                            AS country
     , COALESCE(s.department, rq.department)                      AS department
     , COALESCE(s.department_detail, rq.department_detail)        AS department_detail
     , COALESCE(s.vendor, rq.vendor)                              AS vendor
     , COALESCE(s.is_plussize, rq.is_plussize)                    AS is_plussize
     , COALESCE(s.shared, rq.shared)                              AS shared
     , COALESCE(s.date, rq.date_received)                         AS date
     , COALESCE(s.region, rq.region)                              AS region
     , COALESCE(s.latest_launch_date, rq.latest_launch_date)      AS latest_launch_date
     , COALESCE(s.previous_launch_date, rq.previous_launch_date)  AS previous_launch_date
     , COALESCE(s.subcategory, rq.subcategory)                    AS subcategory
     , COALESCE(s.tier, rq.tier)                                  AS tier
     , COALESCE(s.product_sku, rq.product_sku)                    AS product_sku
     , COALESCE(s.clearance_flag, 'regular')                      AS clearance_flag
     , COALESCE(s.og_retail, rq.og_retail)                        AS og_retail
     , COALESCE(s.current_vip_retail, rq.current_vip_retail)      AS current_vip_retail
     , SUM(COALESCE(s.total_qty_sold, 0))                         AS total_qty_sold
     , SUM(COALESCE(s.total_product_revenue, 0))                  AS total_product_revenue
     , SUM(COALESCE(s.total_product_revenue_with_tariff, 0))      AS total_product_revenue_with_tariff
     , SUM(COALESCE(s.bop_available_to_sell, 0))                  AS bop_available_to_sell
     , SUM(COALESCE(s.total_cogs, 0))                             AS total_cogs
     , SUM(COALESCE(rq.qty_received, 0))                          AS qty_received
     , SUM(COALESCE(s.activating_qty_sold, 0))                    AS activating_qty_sold
     , SUM(COALESCE(s.activating_product_revenue, 0))             AS activating_product_revenue
     , SUM(COALESCE(s.activating_product_revenue_with_tariff, 0)) AS activating_product_revenue_with_tariff
     , SUM(COALESCE(s.activating_cogs, 0))                        AS activating_cogs
     , SUM(COALESCE(s.activating_discount, 0))                    AS activating_discount
     , SUM(COALESCE(s.repeat_qty_sold, 0))                        AS repeat_qty_sold
     , SUM(COALESCE(s.repeat_product_revenue, 0))                 AS repeat_product_revenue
     , SUM(COALESCE(s.repeat_product_revenue_with_tariff, 0))     AS repeat_product_revenue_wtih_tariff
     , SUM(COALESCE(s.repeat_cogs, 0))                            AS repeat_cogs
     , SUM(COALESCE(s.repeat_discount, 0))                        AS repeat_discount
     , SUM(COALESCE(rq.received_retail, 0))                       AS received_retail
     , SUM(COALESCE(rq.landed_cost_received, 0))                  AS landed_cost_received
     , SUM(COALESCE(s.bop_qty_onhand, 0))                         AS bop_qty_onhand
     , SUM(COALESCE(s.bop_qty_intransit, 0))                      AS bop_qty_intransit
FROM _sales s
         FULL JOIN _received_qty rq
                   ON rq.product_sku = s.product_sku
                       AND LOWER(rq.business_unit) = LOWER(s.business_unit)
                       AND LOWER(rq.region) = LOWER(s.region)
                       AND rq.date_received = s.date
                       AND LOWER(rq.country) = LOWER(s.country)
GROUP BY COALESCE(s.business_unit, rq.business_unit)
       , COALESCE(s.country, rq.country)
       , COALESCE(s.department, rq.department)
       , COALESCE(s.department_detail, rq.department_detail)
       , COALESCE(s.vendor, rq.vendor)
       , COALESCE(s.is_plussize, rq.is_plussize)
       , COALESCE(s.shared, rq.shared)
       , COALESCE(s.date, rq.date_received)
       , COALESCE(s.region, rq.region)
       , COALESCE(s.latest_launch_date, rq.latest_launch_date)
       , COALESCE(s.previous_launch_date, rq.previous_launch_date)
       , COALESCE(s.subcategory, rq.subcategory)
       , COALESCE(s.tier, rq.tier)
       , COALESCE(s.product_sku, rq.product_sku)
       , COALESCE(s.clearance_flag, 'regular')
       , COALESCE(s.og_retail, rq.og_retail)
       , COALESCE(s.current_vip_retail, rq.current_vip_retail);


CREATE OR REPLACE TEMPORARY TABLE _inventory_adj AS
SELECT DISTINCT COALESCE(sr.business_unit, sr1.business_unit)               AS business_unit
              , COALESCE(sr.country, sr1.country)                           AS country
              , COALESCE(sr.department, sr1.department)                     AS department
              , COALESCE(sr.department_detail, sr1.department_detail)       AS department_detail
              , COALESCE(sr.vendor, sr1.vendor)                             AS vendor
              , COALESCE(sr.is_plussize, sr1.is_plussize)                   AS is_plussize
              , COALESCE(sr.shared, sr1.shared)                             AS shared
              , COALESCE(sr.date::DATE, DATEADD(DAY, 1, sr1.date::DATE))    AS date
              , COALESCE(sr.region, sr1.region)                             AS region
              , COALESCE(sr.latest_launch_date, sr1.latest_launch_date)     AS latest_launch_date
              , COALESCE(sr.previous_launch_date, sr1.previous_launch_date) AS previous_launch_date
              , COALESCE(sr.subcategory, sr1.subcategory)                   AS subcategory
              , COALESCE(sr.tier, sr1.tier)                                 AS tier
              , COALESCE(sr.product_sku, sr1.product_sku)                   AS product_sku
              , COALESCE(sr.clearance_flag, sr1.clearance_flag)             AS clearance_flag
              , COALESCE(sr.og_retail, sr1.og_retail)                       AS og_retail
              , COALESCE(sr.current_vip_retail, sr1.current_vip_retail)     AS current_vip_retail
              , COALESCE(sr.total_qty_sold, 0)                              AS total_qty_sold
              , COALESCE(sr.total_product_revenue, 0)                       AS total_product_revenue
              , COALESCE(sr.total_product_revenue_with_tariff, 0)           AS total_product_revenue_with_tariff
              , COALESCE(sr.total_cogs, 0)                                  AS total_cogs
              , COALESCE(sr.qty_received, 0)                                AS qty_received
              , COALESCE(sr.activating_qty_sold, 0)                         AS activating_qty_sold
              , COALESCE(sr.activating_product_revenue, 0)                  AS activating_product_revenue
              , COALESCE(sr.activating_product_revenue_with_tariff, 0)      AS activating_product_revenue_with_tariff
              , COALESCE(sr.activating_cogs, 0)                             AS activating_cogs
              , COALESCE(sr.activating_discount, 0)                         AS activating_discount
              , COALESCE(sr.repeat_qty_sold, 0)                             AS repeat_qty_sold
              , COALESCE(sr.repeat_product_revenue, 0)                      AS repeat_product_revenue
              , COALESCE(sr.repeat_product_revenue_wtih_tariff, 0)          AS repeat_product_revenue_wtih_tariff
              , COALESCE(sr.repeat_cogs, 0)                                 AS repeat_cogs
              , COALESCE(sr.repeat_discount, 0)                             AS repeat_discount
              , COALESCE(sr.received_retail, 0)                             AS received_retail
              , COALESCE(sr.landed_cost_received, 0)                        AS landed_cost_received
              , COALESCE(sr1.bop_available_to_sell, 0)                      AS bop_available_to_sell
              , COALESCE(sr1.bop_qty_onhand, 0)                             AS bop_qty_onhand
              , COALESCE(sr1.bop_qty_intransit, 0)                          AS bop_qty_intransit
              , COALESCE(sr.bop_available_to_sell, 0)                       AS eop_available_to_sell
              , COALESCE(sr.bop_qty_intransit, 0)                           AS eop_qty_intransit
              , COALESCE(sr.bop_qty_onhand, 0)                              AS eop_qty_onhand
FROM _sales_received sr
         FULL JOIN _sales_received sr1
                   ON sr1.business_unit = sr.business_unit
                       AND sr1.country = sr.country
                       AND sr1.region = sr.region
                       AND sr1.product_sku = sr.product_sku
--     and sr1.clearance_flag = sr.clearance_flag
                       AND DATEADD(DAY, 1, sr1.date::DATE) = sr.date::DATE;


CREATE OR REPLACE TEMPORARY TABLE _final_adj AS
SELECT a.*
     , a.bop_available_to_sell / b.adj_count AS adj_bop_available_to_sell
     , a.bop_qty_onhand / b.adj_count        AS adj_bop_qty_onhand
     , a.eop_available_to_sell / b.adj_count AS adj_eop_available_to_sell
     , a.eop_qty_onhand / b.adj_count        AS adj_eop_qty_onhand
     , b.adj_count
FROM _inventory_adj a
         JOIN
     (
         SELECT a.business_unit
              , a.region
              , a.country
              , a.product_sku
              , a.date
              , COUNT(a.product_sku) AS adj_count
         FROM _inventory_adj a
         GROUP BY a.business_unit
                , a.region
                , a.country
                , a.product_sku
                , a.date
     ) b ON b.business_unit = a.business_unit
         AND b.region = a.region
         AND b.country = a.country
         AND b.product_sku = a.product_sku
         AND b.date = a.date;


CREATE OR REPLACE TEMPORARY TABLE _final AS
SELECT a.business_unit
     , a.country
     , a.department
     , a.department_detail
     , a.vendor
     , a.is_plussize
     , a.shared
     , DATE_TRUNC(MONTH, a.date)                     AS month_date
     , a.region
     , a.latest_launch_date
     , a.previous_launch_date
     , a.subcategory
     , a.tier
     , a.product_sku
     , a.clearance_flag
     , a.current_vip_retail
     , a.og_retail
     , SUM(a.total_qty_sold)                         AS total_qty_sold
     , SUM(a.total_product_revenue)                  AS total_product_revenue
     , SUM(a.total_product_revenue_with_tariff)      AS total_product_revenue_with_tariff
     , SUM(a.adj_bop_available_to_sell)              AS bop_available_to_sell
     , SUM(a.adj_eop_available_to_sell)              AS eop_available_to_sell
     , SUM(a.total_cogs)                             AS total_cogs
     , SUM(a.qty_received)                           AS qty_received
     , SUM(a.activating_qty_sold)                    AS activating_qty_sold
     , SUM(a.activating_product_revenue)             AS activating_product_revenue
     , SUM(a.activating_product_revenue_with_tariff) AS activating_product_revenue_with_tariff
     , SUM(a.activating_cogs)                        AS activating_cogs
     , SUM(a.activating_discount)                    AS activating_discount
     , SUM(a.repeat_qty_sold)                        AS repeat_qty_sold
     , SUM(a.repeat_product_revenue)                 AS repeat_product_revenue
     , SUM(a.repeat_product_revenue_wtih_tariff)     AS repeat_product_revenue_with_tariff
     , SUM(a.repeat_cogs)                            AS repeat_cogs
     , SUM(a.repeat_discount)                        AS repeat_discount
     , SUM(a.received_retail)                        AS received_retail
     , SUM(a.landed_cost_received)                   AS landed_cost_received
     , SUM(a.adj_bop_qty_onhand)                     AS bop_qty_onhand
     , SUM(a.adj_eop_qty_onhand)                     AS eop_qty_onhand
     , SUM(a.eop_qty_intransit)                      AS eop_qty_intransit
     , SUM(a.bop_qty_intransit)                      AS bop_qty_intransit
FROM _final_adj a
GROUP BY a.business_unit
       , a.country
       , a.department
       , a.department_detail
       , a.vendor
       , a.is_plussize
       , a.shared
       , DATE_TRUNC(MONTH, a.date)
       , a.region
       , a.latest_launch_date
       , a.previous_launch_date
       , a.subcategory
       , a.tier
       , a.product_sku
       , a.clearance_flag
       , a.current_vip_retail
       , a.og_retail;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.dos_080_open_to_buy AS
SELECT DISTINCT a.business_unit
              , mdp.sub_brand
              , a.country
              , a.department
              , a.department_detail
              , a.vendor
              , a.is_plussize
              , a.shared
              , a.month_date
              , a.region
              , a.latest_launch_date
              , a.previous_launch_date
              , a.subcategory
              , mdp.subclass
              , a.tier
              , a.product_sku
              , (CASE
                     WHEN mdp.shared = mdp.business_unit THEN 'Not Shared'
                     ELSE 'Shared' END)                                                                 AS is_shared
              , mdp.style_name
              , mdp.product_style_number                                                                AS style_number
              , mdp.master_color
              , mdp.reorder_status
              , a.clearance_flag
              , a.current_vip_retail
              , a.og_retail
              , mdp.qty_pending
              , a.total_qty_sold
              , a.total_product_revenue
              , a.total_product_revenue_with_tariff
              , a.bop_available_to_sell
              , a.eop_available_to_sell
              , a.total_cogs
              , a.qty_received
              , (a.bop_available_to_sell + a.qty_received - a.total_qty_sold - a.eop_available_to_sell) AS shrinkage
              , mdp.qty_pending * COALESCE(mdp.og_retail, mdp.current_vip_retail)                       AS pending_retail
              , mdp.qty_pending * mdp.landing_cost                                                      AS landed_cost_pending
              , a.activating_qty_sold
              , a.activating_product_revenue
              , a.activating_product_revenue_with_tariff
              , a.activating_cogs
              , a.activating_discount
              , a.repeat_qty_sold
              , a.repeat_product_revenue
              , a.repeat_product_revenue_with_tariff
              , a.repeat_cogs
              , a.repeat_discount
              , a.received_retail
              , a.landed_cost_received
              , mdp.msrp
              , mdp.landing_cost
              , mdp.na_landed_cost
              , mdp.eu_landed_cost
              , a.bop_qty_onhand
              , a.eop_qty_onhand
              , a.eop_qty_intransit
              , a.bop_qty_intransit
FROM _final a
         JOIN reporting_prod.gfb.merch_dim_product mdp
              ON LOWER(mdp.business_unit) = LOWER(a.business_unit)
                  AND LOWER(mdp.region) = LOWER(a.region)
                  AND LOWER(mdp.country) = LOWER(a.country)
                  AND mdp.product_sku = a.product_sku;
