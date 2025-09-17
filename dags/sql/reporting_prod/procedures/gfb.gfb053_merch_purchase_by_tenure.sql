SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));


CREATE OR REPLACE TEMPORARY TABLE _bop_vip_with_m1 AS
SELECT dv.business_unit
     , dv.region
     , dv.country
     , mon.month_date
     , dv.customer_id
     , (CASE
            WHEN DATEDIFF(MONTH, dv.first_activating_cohort, mon.month_date) + 1 > 24 THEN 'M25+'
            WHEN DATEDIFF(MONTH, dv.first_activating_cohort, mon.month_date) + 1 > 12 THEN 'M13 to M24'
            ELSE 'M' || CAST(DATEDIFF(MONTH, dv.first_activating_cohort, mon.month_date) + 1 AS VARCHAR(20))
    END) AS tenure
     , dv.membership_price
FROM reporting_prod.gfb.gfb_dim_vip dv
         JOIN
     (
         SELECT DISTINCT dd.month_date
         FROM edw_prod.data_model_jfb.dim_date dd
         WHERE dd.full_date >= $start_date
     ) mon ON (mon.month_date BETWEEN dv.first_activating_cohort AND COALESCE(
         IFF(dv.recent_activating_date > dv.recent_vip_cancellation_date, CURRENT_DATE(),
             dv.recent_vip_cancellation_date), CURRENT_DATE()))
WHERE dv.business_unit IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS')
  AND DATEDIFF(MONTH, dv.first_activating_cohort, mon.month_date) + 1 >= 1;


CREATE OR REPLACE TEMPORARY TABLE _merch_purchase_detail AS
SELECT DISTINCT olp.business_unit
              , olp.region
              , olp.country
              , DATE_TRUNC(MONTH, olp.order_date) AS order_month
              , olp.customer_id
              , mdp.department_detail
              , mdp.subcategory
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         JOIN reporting_prod.gfb.merch_dim_product mdp
              ON mdp.business_unit = olp.business_unit
                  AND mdp.region = olp.region
                  AND mdp.country = olp.country
                  AND mdp.product_sku = olp.product_sku
WHERE olp.order_classification = 'product order'
  AND olp.order_type = 'vip repeat'
  AND olp.order_date >= $start_date;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb053_merch_purchase_by_tenure AS
SELECT bop.business_unit
     , bop.region
     , bop.country
     , bop.month_date
     , bop.customer_id
     , bop.tenure
     , bop.membership_price

     , mpd.customer_id                                  AS purchsed_customer_id
     , COALESCE(mpd.department_detail, 'Non Purchased') AS department_detail
     , COALESCE(mpd.subcategory, 'Non Purchased')       AS subcategory
FROM _bop_vip_with_m1 bop
         LEFT JOIN _merch_purchase_detail mpd
                   ON mpd.customer_id = bop.customer_id
                       AND mpd.order_month = bop.month_date;
