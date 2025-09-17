SET low_watermark_ltz = (
    SELECT MAX(a.month_date)
    FROM reporting_prod.gfb.gfb_customer_dataset_base a
);


CREATE OR REPLACE TEMP TABLE _review_base AS
SELECT customer_id
     , month_date
     , store_id
FROM reporting_prod.gfb.gfb_customer_dataset_base cd
WHERE cd.month_date >= $low_watermark_ltz
  AND cd.month_date < DATE_TRUNC('MONTH', CURRENT_DATE());


DELETE
FROM reporting_prod.gfb.gfb_customer_dataset_product_review a
WHERE a.month_date >= $low_watermark_ltz;


INSERT INTO reporting_prod.gfb.gfb_customer_dataset_product_review
SELECT DISTINCT rb.customer_id,
                rb.month_date,
                0 AS review_count,
                0 AS review_avg_recommended_score,
                0 AS review_avg_style_score,
                0 AS review_avg_comfort_score,
                0 AS review_avg_quality_value_score,
                0 AS review_avg_size_fit_score,
                0 AS review_avg_nps_score,
                0 AS review_min_recommended_score,
                0 AS review_min_style_score,
                0 AS review_min_comfort_score,
                0 AS review_min_quality_value_score,
                0 AS review_min_size_fit_score,
                0 AS review_min_nps_score,
                0 AS is_left_review
FROM _review_base rb
         JOIN edw_prod.reference.store_timezone st
              ON st.store_id = rb.store_id;
