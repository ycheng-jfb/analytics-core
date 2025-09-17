CREATE OR REPLACE TEMPORARY TABLE _reviewed_products AS
SELECT UPPER(st.store_brand)                  AS business_unit
     , UPPER(st.store_region)                 AS region
     , (CASE
            WHEN dc.specialty_country_code = 'GB' THEN 'UK'
            WHEN dc.specialty_country_code != 'Unknown' THEN UPPER(dc.specialty_country_code)
            ELSE UPPER(st.store_country) END) AS country
     , r.review_id
     , r.product_id
     , CAST(r.datetime_added AS DATE)         AS review_submission_date
     , r.statuscode                           AS status_code
     , sc.label                               AS status
     , r.rating                               AS overall_rating
     , r.recommended
     , dp.sku
     , dp.size                                AS dp_size
     , dp.product_sku
     , r.body                                 AS customer_review
     , r.order_id
     , dc.customer_id
FROM lake_jfb_view.ultra_merchant.review r
         JOIN edw_prod.data_model_jfb.dim_product dp
              ON dp.product_id = r.product_id
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = dp.store_id
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = r.customer_id
                  AND dc.is_test_customer = 0
         JOIN lake_jfb_view.ultra_merchant.statuscode sc
              ON r.statuscode = sc.statuscode
WHERE sc.label != 'Declined';


CREATE OR REPLACE TEMPORARY TABLE _review_details AS
SELECT r.review_id
     , r.product_id
     , r.order_id
     , rtf.label          AS review_field
     , rtfg.label         AS review_type
     , rtfa.label         AS review_field_answer
     , rtfa.score         AS review_field_score
     , rtfa.boolean_value AS review_field_boolean_answer
     , rfa.value          AS special_review
FROM lake_jfb_view.ultra_merchant.review r
         JOIN lake_jfb_view.ultra_merchant.review_field_answer rfa
              ON rfa.review_id = r.review_id
         LEFT JOIN lake_jfb_view.ultra_merchant.review_template_field_answer rtfa
                   ON rtfa.review_template_field_answer_id = rfa.review_template_field_answer_id
         LEFT JOIN lake_jfb_view.ultra_merchant.review_template_field rtf
                   ON rtf.review_template_field_id = rfa.review_template_field_id
                       AND rtf.review_template_id = r.review_template_id
         LEFT JOIN lake_jfb_view.ultra_merchant.review_template_field_group rtfg
                   ON rtfg.review_template_field_group_id = rtf.review_template_field_group_id
         JOIN _reviewed_products rp
              ON rp.review_id = r.review_id
                  AND rp.product_id = r.product_id;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb_product_review_data_set AS
SELECT rp.business_unit
     , rp.region
     , rp.country
     , rp.review_id
     , rp.order_id
     , rp.product_id
     , rp.review_submission_date
     , rp.status_code
     , rp.status
     , rp.recommended
     , rp.sku
     , rp.product_sku
     , rp.customer_review
     , rp.dp_size
     , rp.customer_id

     , ord.review_field_answer                             AS overall_review_field_answer
     , COALESCE(rp.overall_rating, ord.review_field_score) AS overall_review_field_score

     , qvrd.review_field_answer                            AS quality_value_review_field_answer
     , qvrd.review_field_score                             AS quality_value_review_field_score

     , srd.review_field_answer                             AS style_review_field_answer
     , srd.review_field_score                              AS style_review_field_score

     , crd.review_field_answer                             AS comfort_review_field_answer
     , crd.review_field_score                              AS comfort_review_field_score

     , szrd.review_field_answer                            AS size_review_field_answer
     , szrd.review_field_score                             AS size_review_field_score

     , lkh.review_field_answer                             AS likelihood_answer

     , psw.special_review                                  AS please_share_why
FROM _reviewed_products rp
         LEFT JOIN _review_details ord
                   ON ord.review_id = rp.review_id
                       AND ord.product_id = rp.product_id
                       AND ord.review_type = 'Overall'
         LEFT JOIN _review_details qvrd
                   ON qvrd.review_id = rp.review_id
                       AND qvrd.product_id = rp.product_id
                       AND qvrd.review_type = 'Quality & Value'
         LEFT JOIN _review_details srd
                   ON srd.review_id = rp.review_id
                       AND srd.product_id = rp.product_id
                       AND srd.review_type = 'Style'
         LEFT JOIN _review_details crd
                   ON crd.review_id = rp.review_id
                       AND crd.product_id = rp.product_id
                       AND crd.review_type = 'Comfort'
         LEFT JOIN _review_details szrd
                   ON szrd.review_id = rp.review_id
                       AND szrd.product_id = rp.product_id
                       AND szrd.review_type = 'Size Fit'
         LEFT JOIN _review_details lkh
                   ON lkh.review_id = rp.review_id
                       AND lkh.product_id = rp.product_id
                       AND (
                                  LOWER(lkh.review_field) LIKE '%recommend%'
                              OR
                                  LOWER(lkh.review_field) LIKE '%weiterempfehlung%'
                              OR
                                  LOWER(lkh.review_field) LIKE '%recommande%'
                              OR
                                  LOWER(lkh.review_field) LIKE '%anbefaling%'
                              OR
                                  LOWER(lkh.review_field) LIKE '%recomendar%'
                              OR
                                  LOWER(lkh.review_field) LIKE '%aanbevelen%'
                              OR
                                  LOWER(lkh.review_field) LIKE '%rekomendera%'
                          )
         LEFT JOIN _review_details psw
                   ON psw.review_id = rp.review_id
                       AND psw.product_id = rp.product_id
                       AND LOWER(psw.review_field) = 'please share why';
