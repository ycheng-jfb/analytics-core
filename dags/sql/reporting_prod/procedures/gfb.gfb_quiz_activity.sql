SET start_date = DATE_TRUNC(MONTH, DATEADD(MONTH, -3, CURRENT_DATE()));

DELETE
FROM reporting_prod.gfb.gfb_quiz_activity qa
WHERE qa.customer_quiz_date >= $start_date;


INSERT INTO reporting_prod.gfb.gfb_quiz_activity
SELECT DISTINCT UPPER(st.store_brand)                  AS business_unit
              , UPPER(st.store_region)                 AS region
              , (CASE
                     WHEN dc.specialty_country_code = 'GB' THEN 'UK'
                     WHEN dc.specialty_country_code != 'Unknown' THEN UPPER(dc.specialty_country_code)
                     ELSE UPPER(st.store_country) END) AS country
              , cq.customer_quiz_id
              , q.label                                AS quiz_version
              , qsc.label                              AS quiz_status
              , s.session_id
              , s.customer_id
              , CAST(cq.datetime_added AS DATE)        AS customer_quiz_date
              , cqqa.quiz_question_id
              , qq.question_text
              , cqqa.quiz_question_answer_id
              , qqa.customer_detail_value
              , qqa.answer_text
              , cqd.object
              , cqd.object_id
FROM lake_jfb_view.ultra_merchant.customer_quiz cq
         LEFT JOIN lake_jfb_view.ultra_merchant.quiz q
                   ON q.quiz_id = cq.quiz_id
         LEFT JOIN lake_jfb_view.ultra_merchant.statuscode qsc
                   ON qsc.statuscode = q.statuscode
         JOIN lake_jfb_view.ultra_merchant.session s
              ON s.session_id = cq.session_id
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = s.store_id
         LEFT JOIN edw_prod.data_model_jfb.dim_customer dc
                   ON dc.customer_id = s.customer_id
         LEFT JOIN lake_jfb_view.ultra_merchant.customer_quiz_question_answer cqqa
                   ON cqqa.customer_quiz_id = cq.customer_quiz_id
         LEFT JOIN lake_jfb_view.ultra_merchant.quiz_question qq
                   ON qq.quiz_question_id = cqqa.quiz_question_id
         LEFT JOIN lake_jfb_view.ultra_merchant.quiz_question_answer qqa
                   ON qqa.quiz_question_id = qq.quiz_question_id
                       AND qqa.quiz_question_answer_id = cqqa.quiz_question_answer_id
         LEFT JOIN lake_jfb_view.ultra_merchant.customer_quiz_detail cqd
                   ON cqd.customer_quiz_id = cq.customer_quiz_id
                       AND cqd.quiz_question_id = qq.quiz_question_id
WHERE customer_quiz_date >= $start_date
  AND s.customer_id IS NOT NULL
  AND qq.question_text IS NOT NULL
  AND qqa.answer_text IS NOT NULL;
