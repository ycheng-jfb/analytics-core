/*
Vectorize reviews that have not already been vectorized.
*/

UPDATE reporting_prod.data_science.tableau_customer_reviews_calculated AS c
SET c.embedding_snowflake = SNOWFLAKE.CORTEX.EMBED_TEXT_1024('nv-embed-qa-4', b.review_text)
FROM (
    SELECT
        r.review_id,
        TRIM(CONCAT(COALESCE(r.review_title, ''), ' ', COALESCE(r.review_text, ''))) AS review_text
    FROM reporting_prod.data_science.tableau_customer_reviews AS r
    JOIN reporting_prod.data_science.tableau_customer_reviews_calculated AS c
        ON r.review_id = c.review_id
    WHERE c.embedding_snowflake IS NULL
    AND TRIM(CONCAT(COALESCE(r.review_title, ''), ' ', COALESCE(r.review_text, ''))) IS NOT NULL
    ) AS b
WHERE c.review_id = b.review_id;
