CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb014_01_bop_vip AS
SELECT st.store_name                            AS store_name
     , st.store_brand                           AS store_brand_name
     , st.store_region                          AS store_region_abbr
     , (CASE
            WHEN a.finance_specialty_store = 'CA' THEN 'CA'
            ELSE st.store_country END)          AS store_country_abbr
     , a.date

     , SUM(a.bop_vips)                          AS bop_vip_count
     , SUM(a.bop_vips - a.cancels + a.new_vips) AS eop_vip_count
     , SUM(a.new_vips)                          AS vip_activation_count
FROM edw_prod.analytics_base.acquisition_media_spend_daily_agg a
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = a.vip_store_id
WHERE a.date_object = 'placed'
  AND a.currency_type = 'USD'
  AND a.date < CURRENT_DATE()
GROUP BY st.store_name
       , st.store_brand
       , st.store_region
       , (CASE
            WHEN a.finance_specialty_store = 'CA' THEN 'CA'
            ELSE st.store_country END)
       , a.date;
