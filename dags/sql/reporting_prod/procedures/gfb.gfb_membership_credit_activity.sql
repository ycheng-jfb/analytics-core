CREATE OR REPLACE TEMPORARY TABLE _issued AS
SELECT UPPER(st.store_brand)                                                                 AS business_unit
     , UPPER(st.store_region)                                                                AS region
     , (CASE
            WHEN cc.specialty_country_code = 'GB' THEN 'UK'
            WHEN cc.specialty_country_code != 'Unknown' THEN UPPER(cc.specialty_country_code)
            ELSE UPPER(st.store_country) END)                                                AS country
     , dc.credit_id
     , dc.customer_id
     , dc.credit_type
     , dc.credit_reason
     , CAST(a.credit_activity_local_datetime AS DATE)                                        AS issued_date
     , SUM(a.credit_activity_gross_vat_local_amount * a.credit_activity_usd_conversion_rate) AS issued_amount
     , SUM(a.credit_activity_gross_vat_local_amount)                                         AS issued_amount_local
FROM edw_prod.data_model_jfb.fact_credit_event a
         JOIN edw_prod.data_model_jfb.dim_credit dc
              ON dc.credit_key = a.credit_key
                  AND dc.credit_tender = 'Cash'
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = dc.store_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cc
                   ON cc.customer_id = dc.customer_id
WHERE a.credit_activity_type = 'Issued'
GROUP BY UPPER(st.store_brand)
       , UPPER(st.store_region)
       , (CASE
              WHEN cc.specialty_country_code = 'GB' THEN 'UK'
              WHEN cc.specialty_country_code != 'Unknown' THEN UPPER(cc.specialty_country_code)
              ELSE UPPER(st.store_country) END)
       , dc.credit_id
       , dc.customer_id
       , dc.credit_type
       , dc.credit_reason
       , CAST(a.credit_activity_local_datetime AS DATE);


CREATE OR REPLACE TEMPORARY TABLE _redeemed AS
SELECT UPPER(st.store_brand)                                                                 AS business_unit
     , UPPER(st.store_region)                                                                AS region
     , (CASE
            WHEN cc.specialty_country_code = 'GB' THEN 'UK'
            WHEN cc.specialty_country_code != 'Unknown' THEN UPPER(cc.specialty_country_code)
            ELSE UPPER(st.store_country) END)                                                AS country
     , dc.credit_id
     , CAST(a.credit_activity_local_datetime AS DATE)                                        AS redeemed_date
     , CAST(fo.order_local_datetime AS DATE)                                                 AS redeemed_order_date
     , a.redemption_order_id
     , SUM(a.credit_activity_gross_vat_local_amount * a.credit_activity_usd_conversion_rate) AS redeemed_amount
     , SUM(a.credit_activity_gross_vat_local_amount)                                         AS redeemed_amount_local
FROM edw_prod.data_model_jfb.fact_credit_event a
         JOIN edw_prod.data_model_jfb.dim_credit dc
              ON dc.credit_key = a.credit_key
         JOIN _issued i
              ON i.credit_id = dc.credit_id
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = dc.store_id
         LEFT JOIN edw_prod.data_model_jfb.fact_order fo
                   ON fo.order_id = a.redemption_order_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cc
                   ON cc.customer_id = dc.customer_id
WHERE a.credit_activity_type = 'Redeemed'
GROUP BY UPPER(st.store_brand)
       , UPPER(st.store_region)
       , (CASE
              WHEN cc.specialty_country_code = 'GB' THEN 'UK'
              WHEN cc.specialty_country_code != 'Unknown' THEN UPPER(cc.specialty_country_code)
              ELSE UPPER(st.store_country) END)
       , dc.credit_id
       , CAST(a.credit_activity_local_datetime AS DATE)
       , CAST(fo.order_local_datetime AS DATE)
       , a.redemption_order_id;


CREATE OR REPLACE TEMPORARY TABLE _cancelled AS
SELECT UPPER(st.store_brand)                                                                 AS business_unit
     , UPPER(st.store_region)                                                                AS region
     , (CASE
            WHEN cc.specialty_country_code = 'GB' THEN 'UK'
            WHEN cc.specialty_country_code != 'Unknown' THEN UPPER(cc.specialty_country_code)
            ELSE UPPER(st.store_country) END)                                                AS country
     , dc.credit_id
     , CAST(a.credit_activity_local_datetime AS DATE)                                        AS cancelled_date
     , SUM(a.credit_activity_gross_vat_local_amount * a.credit_activity_usd_conversion_rate) AS cancelled_amount
     , SUM(a.credit_activity_gross_vat_local_amount)                                         AS cancelled_amount_local
FROM edw_prod.data_model_jfb.fact_credit_event a
         JOIN edw_prod.data_model_jfb.dim_credit dc
              ON dc.credit_key = a.credit_key
         JOIN _issued i
              ON i.credit_id = dc.credit_id
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = dc.store_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cc
                   ON cc.customer_id = dc.customer_id
WHERE a.credit_activity_type = 'Cancelled'
GROUP BY UPPER(st.store_brand)
       , UPPER(st.store_region)
       , (CASE
              WHEN cc.specialty_country_code = 'GB' THEN 'UK'
              WHEN cc.specialty_country_code != 'Unknown' THEN UPPER(cc.specialty_country_code)
              ELSE UPPER(st.store_country) END)
       , dc.credit_id
       , CAST(a.credit_activity_local_datetime AS DATE);


CREATE OR REPLACE TEMPORARY TABLE _other_activity AS
SELECT UPPER(st.store_brand)                                                                 AS business_unit
     , UPPER(st.store_region)                                                                AS region
     , (CASE
            WHEN cc.specialty_country_code = 'GB' THEN 'UK'
            WHEN cc.specialty_country_code != 'Unknown' THEN UPPER(cc.specialty_country_code)
            ELSE UPPER(st.store_country) END)                                                AS country
     , dc.credit_id
     , CAST(a.credit_activity_local_datetime AS DATE)                                        AS other_activity_date
     , SUM(a.credit_activity_gross_vat_local_amount * a.credit_activity_usd_conversion_rate) AS other_activity_amount
     , SUM(a.credit_activity_gross_vat_local_amount)                                         AS other_activity_amount_local
FROM edw_prod.data_model_jfb.fact_credit_event a
         JOIN edw_prod.data_model_jfb.dim_credit dc
              ON dc.credit_key = a.credit_key
         JOIN _issued i
              ON i.credit_id = dc.credit_id
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = dc.store_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer cc
                   ON cc.customer_id = dc.customer_id
WHERE a.credit_activity_type NOT IN ('Issued', 'Redeemed', 'Cancelled')
GROUP BY UPPER(st.store_brand)
       , UPPER(st.store_region)
       , (CASE
              WHEN cc.specialty_country_code = 'GB' THEN 'UK'
              WHEN cc.specialty_country_code != 'Unknown' THEN UPPER(cc.specialty_country_code)
              ELSE UPPER(st.store_country) END)
       , dc.credit_id
       , CAST(a.credit_activity_local_datetime AS DATE);


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb_membership_credit_activity AS
SELECT i.business_unit
     , i.region
     , i.country
     , i.credit_id
     , i.credit_type
     , i.credit_reason
     , i.customer_id
     , i.issued_date
     , i.issued_amount
     , i.issued_amount_local

     , r.redeemed_date
     , r.redeemed_order_date
     , r.redeemed_amount
     , r.redeemed_amount_local
     , r.redemption_order_id

     , c.cancelled_date
     , c.cancelled_amount
     , c.cancelled_amount_local

     , oa.other_activity_date
     , oa.other_activity_amount
     , oa.other_activity_amount_local
FROM _issued i
         LEFT JOIN _redeemed r
                   ON r.credit_id = i.credit_id
                       AND i.issued_date <= COALESCE(r.redeemed_date, CURRENT_DATE())
         LEFT JOIN _cancelled c
                   ON c.credit_id = i.credit_id
                       AND i.issued_date <= COALESCE(c.cancelled_date, CURRENT_DATE())
         LEFT JOIN _other_activity oa
                   ON oa.credit_id = i.credit_id
                       AND i.issued_date <= COALESCE(oa.other_activity_date, CURRENT_DATE());
