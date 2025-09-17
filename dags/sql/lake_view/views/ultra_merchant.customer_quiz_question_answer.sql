CREATE OR REPLACE VIEW LAKE_VIEW.ULTRA_MERCHANT.CUSTOMER_QUIZ_QUESTION_ANSWER AS
SELECT
    customer_quiz_question_answer_id,
    customer_quiz_id,
    quiz_question_id,
    quiz_question_answer_id,
    answer_text,
    datetime_added,
    datetime_modified,
    meta_create_datetime,
    meta_update_datetime
FROM lake.ultra_merchant.customer_quiz_question_answer s
WHERE NOT exists(
        SELECT
            1
        FROM lake_archive.ultra_merchant_cdc.customer_quiz_question_answer__del cd
        WHERE cd.customer_quiz_question_answer_id = s.customer_quiz_question_answer_id
    );
