CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb_loyalty_points_activity AS
SELECT UPPER(st.store_brand)                  AS business_unit
     , UPPER(st.store_region)                 AS region
     , (CASE
            WHEN dc.specialty_country_code = 'GB' THEN 'UK'
            WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
            ELSE UPPER(st.store_country) END) AS country
     , st.store_id
     , dc.customer_id
     , CAST(mrt.datetime_added AS DATE)       AS activity_date
     , mrtt.type
     , mrtt.object
     , mrt.object_id
     , mrt.points
     , mrtt.label                             AS activity_reason
     , sc.label                               AS status
     , fo.order_id
     , fr.refund_id
     , mp.promo_id
FROM (SELECT *,
             ROW_NUMBER() OVER (PARTITION BY membership_id,points ORDER BY membership_reward_transaction_id) AS row_num
      FROM lake_jfb_view.ultra_merchant.membership_reward_transaction) mrt
         JOIN lake_jfb_view.ultra_merchant.membership_reward_transaction_type mrtt
              ON mrtt.membership_reward_transaction_type_id = mrt.membership_reward_transaction_type_id
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.membership_id = mrt.membership_id
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = m.customer_id
                  AND dc.is_test_customer = 0
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = dc.store_id
         JOIN lake_jfb_view.ultra_merchant.statuscode sc
              ON sc.statuscode = mrt.statuscode
         LEFT JOIN edw_prod.data_model_jfb.fact_order fo
                   ON fo.order_id = mrt.object_id
                       AND mrtt.object LIKE '%order%'
         LEFT JOIN edw_prod.data_model_jfb.fact_refund fr
                   ON fr.refund_id = mrt.object_id
                       AND mrtt.object LIKE '%refund%'
         LEFT JOIN lake_jfb_view.ultra_merchant.membership_promo mp
                   ON mp.membership_promo_id = mrt.object_id
                       AND mrtt.object = 'membership_promo'
WHERE mrt.row_num = 1
  AND sc.label != 'Pending';
