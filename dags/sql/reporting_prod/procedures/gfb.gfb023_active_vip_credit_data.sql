CREATE OR REPLACE TEMPORARY TABLE _active_vips AS
SELECT st.store_brand
     , (CASE
            WHEN dc.specialty_country_code = 'GB' THEN 'UK'
            WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
            ELSE st.store_country END) AS store_country
     , st.store_region
     , clv.customer_id
     , clv.first_activating_cohort AS first_activation_cohort
     , m.price                     AS membership_price
FROM reporting_prod.gfb.gfb_dim_vip clv
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = clv.customer_id
                  AND dc.is_test_customer = 0
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = clv.store_id
         JOIN lake_jfb_view.ultra_merchant.membership m
              ON m.customer_id = clv.customer_id
WHERE clv.current_membership_status = 'VIP';


CREATE OR REPLACE TEMPORARY TABLE _outstanding_credit AS
SELECT dc.customer_id
     , sc.store_credit_id
     , dc.credit_reason
     , dc.credit_type
     , dc.credit_tender
     , sc.balance                                                           AS outstanding_amount
     , dc.credit_issued_local_amount * dc.credit_issued_usd_conversion_rate AS credit_issued_price
FROM edw_prod.data_model_jfb.dim_credit dc
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = dc.store_id
         JOIN lake_jfb_view.ultra_merchant.store_credit sc
              ON sc.store_credit_id = dc.credit_id
         JOIN lake_jfb_view.ultra_merchant.statuscode scod
              ON scod.statuscode = sc.statuscode
                  AND scod.label = 'Active'
WHERE dc.source_credit_id_type = 'store_credit_id';


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb023_active_vip_credit_data AS
SELECT av.store_brand
     , av.store_region
     , av.store_country
     , av.customer_id
     , oc.credit_reason
     , oc.credit_type
     , oc.credit_tender
     , av.first_activation_cohort
     , av.membership_price
     , oc.credit_issued_price
     , COUNT(oc.store_credit_id)               AS outstanding_credit_count
     , SUM(COALESCE(oc.outstanding_amount, 0)) AS outstanding_credit_amount
FROM _active_vips av
         LEFT JOIN _outstanding_credit oc
                   ON oc.customer_id = av.customer_id
GROUP BY av.store_brand
       , av.store_region
       , av.store_country
       , av.customer_id
       , oc.credit_reason
       , oc.credit_type
       , oc.credit_tender
       , av.first_activation_cohort
       , av.membership_price
       , oc.credit_issued_price;
