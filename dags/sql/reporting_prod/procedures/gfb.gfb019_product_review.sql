SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));
SET end_date = CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _sales_return_info AS
SELECT olp.business_unit
     , mdp.sub_brand
     , olp.region
     , olp.country
     , olp.sku
     , olp.product_sku
     , psm.clean_size AS size
     , psm.core_size_flag
     , mdp.department_detail
     , CASE
           WHEN mdp.department_detail IN ('GIRLS SHOES', 'BOYS SHOES') THEN mdp.subclass
           ELSE mdp.subcategory
    END                           AS subcategory
     , mdp.ww_wc
     , mdp.large_img_url          AS image_url
     , mdp.style_name
     , mdp.latest_launch_date
     , mdp.show_room
     , olp.order_date
     , olp.order_id
     , olp.order_type
     , SUM(olp.total_qty_sold)    AS total_qty_sold
     , SUM(olp.total_return_unit) AS total_return_unit
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         JOIN reporting_prod.gfb.merch_dim_product mdp
              ON olp.business_unit = mdp.business_unit
                  AND olp.region = mdp.region
                  AND olp.country = mdp.country
                  AND olp.product_sku = mdp.product_sku
         LEFT JOIN (SELECT DISTINCT business_unit, product_sku, region, size, clean_size, core_size_flag
                    FROM reporting_prod.gfb.view_product_size_mapping) psm
                   ON psm.product_sku = olp.product_sku
                       AND psm.region = olp.region
                       AND psm.business_unit = olp.business_unit
                       AND LOWER(psm.size) = LOWER(olp.dp_size)
WHERE olp.order_classification = 'product order'
  AND olp.order_date >= $start_date
  AND olp.order_date < $end_date
GROUP BY olp.business_unit
       , mdp.sub_brand
       , olp.region
       , olp.country
       , olp.sku
       , olp.product_sku
       , psm.clean_size
       , psm.core_size_flag
       , olp.order_date
       , olp.order_id
       , olp.order_type
       , mdp.department_detail
       , (CASE
              WHEN mdp.department_detail IN ('GIRLS SHOES', 'BOYS SHOES') THEN mdp.subclass
              ELSE mdp.subcategory
    END)
       , mdp.ww_wc
       , mdp.large_img_url
       , mdp.style_name
       , mdp.latest_launch_date
       , mdp.show_room;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb019_product_review AS
SELECT pr.business_unit
     , mdp.sub_brand
     , pr.region
     , pr.country
     , pr.review_submission_date
     , pr.review_id
     , pr.order_id
     , pr.product_sku
     , pr.sku
     , pr.recommended
     , pr.customer_review
     , pr.overall_review_field_score       AS overall_rating
     , pr.quality_value_review_field_score AS quality_rating
     , pr.style_review_field_score         AS style_rating
     , pr.comfort_review_field_score       AS comfort_rating
     , pr.size_review_field_answer         AS size_review
     , pr.size_review_field_score          AS size_rating
     , pr.likelihood_answer
     , psm.clean_size                      AS size
     , psm.core_size_flag
     , mdp.department_detail
     , mdp.show_room
     , CASE
           WHEN mdp.department_detail IN ('GIRLS SHOES', 'BOYS SHOES') THEN mdp.subclass
           ELSE mdp.subcategory
    END                                    AS subcategory
     , mdp.ww_wc
     , mdp.large_img_url                   AS image_url
     , mdp.style_name
     , mdp.latest_launch_date
     , sri.order_type
     , pr.please_share_why
FROM reporting_prod.gfb.gfb_product_review_data_set pr
         JOIN reporting_prod.gfb.merch_dim_product mdp
              ON mdp.business_unit = pr.business_unit
                  AND mdp.region = pr.region
                  AND mdp.country = pr.country
                  AND mdp.product_sku = pr.product_sku
         LEFT JOIN (SELECT DISTINCT business_unit, product_sku, region, size, clean_size, core_size_flag
                    FROM reporting_prod.gfb.view_product_size_mapping) psm
                   ON psm.product_sku = pr.product_sku
                       AND psm.region = pr.region
                       AND psm.business_unit = pr.business_unit
                       AND LOWER(psm.size) = LOWER(pr.dp_size)
         LEFT JOIN _sales_return_info sri
                   ON sri.business_unit = pr.business_unit
                       AND sri.region = pr.region
                       AND sri.country = pr.country
                       AND sri.sku = pr.sku
                       AND sri.order_id = pr.order_id
WHERE pr.review_submission_date >= $start_date
  AND pr.review_submission_date < $end_date;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb019_01_product_review_summary AS
SELECT COALESCE(r.business_unit, sri.business_unit)           AS business_unit
     , COALESCE(r.sub_brand, sri.sub_brand)                   AS sub_brand
     , COALESCE(r.region, sri.region)                         AS region
     , COALESCE(r.country, sri.country)                       AS country
     , COALESCE(r.review_submission_date, sri.order_date)     AS review_submission_date
     , COALESCE(r.product_sku, sri.product_sku)               AS product_sku
     , COALESCE(r.sku, sri.sku)                               AS sku
     , COALESCE(r.size, sri.size)                             AS size
     , COALESCE(r.core_size_flag, sri.core_size_flag)         AS core_size_flag
     , COALESCE(r.department_detail, sri.department_detail)   AS department_detail
     , COALESCE(r.subcategory, sri.subcategory)               AS subcategory
     , COALESCE(r.ww_wc, sri.ww_wc)                           AS ww_wc
     , COALESCE(r.image_url, sri.image_url)                   AS image_url
     , COALESCE(r.style_name, sri.style_name)                 AS style_name
     , COALESCE(r.latest_launch_date, sri.latest_launch_date) AS latest_lauch_date
     , COALESCE(r.show_room,sri.show_room)                    AS show_room
     , COALESCE(r.order_id, sri.order_id)                     AS order_id
     , avg_recommended
     , avg_overall_rating
     , avg_quality_rating
     , avg_style_rating
     , avg_comfort_rating
     , avg_size_rating
     , total_reviews
     , avg_likelihood_answer
     , sri.total_qty_sold
     , sri.total_return_unit
     , sri.order_type
FROM (
         SELECT pr.business_unit
              , pr.sub_brand
              , pr.region
              , pr.country
              , pr.review_submission_date
              , pr.product_sku
              , pr.sku
              , pr.size
              , pr.core_size_flag
              , pr.department_detail
              , pr.subcategory
              , pr.ww_wc
              , pr.image_url
              , pr.style_name
              , pr.latest_launch_date
              , pr.show_room
              , pr.order_id

              , AVG(pr.recommended)          AS avg_recommended
              , AVG(pr.overall_rating)       AS avg_overall_rating
              , AVG(pr.quality_rating)       AS avg_quality_rating
              , AVG(pr.style_rating)         AS avg_style_rating
              , AVG(pr.comfort_rating)       AS avg_comfort_rating
              , AVG(pr.size_rating)          AS avg_size_rating
              , COUNT(DISTINCT pr.review_id) AS total_reviews
              , AVG(pr.likelihood_answer)    AS avg_likelihood_answer
         FROM reporting_prod.gfb.gfb019_product_review pr
         GROUP BY pr.business_unit
                , pr.sub_brand
                , pr.region
                , pr.country
                , pr.review_submission_date
                , pr.product_sku
                , pr.sku
                , pr.size
                , pr.core_size_flag
                , pr.department_detail
                , pr.subcategory
                , pr.ww_wc
                , pr.image_url
                , pr.style_name
                , pr.latest_launch_date
                , pr.show_room
                , pr.order_id
     ) r
         FULL JOIN _sales_return_info sri
                   ON sri.business_unit = r.business_unit
                       AND sri.region = r.region
                       AND sri.country = r.country
                       AND sri.sku = r.sku
                       AND sri.order_id = r.order_id;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb019_02_product_return_reason AS
SELECT olp.business_unit
     , mdp.sub_brand
     , olp.region
     , olp.country
     , olp.product_sku
     , olp.sku
     , olp.return_date
     , olp.return_reason
     , psm.clean_size             AS size
     , psm.core_size_flag
     , mdp.department_detail
     , CASE
           WHEN mdp.department_detail IN ('GIRLS SHOES', 'BOYS SHOES') THEN mdp.subclass
           ELSE mdp.subcategory
    END                           AS subcategory
     , mdp.ww_wc
     , mdp.large_img_url          AS image_url
     , mdp.style_name
     , mdp.latest_launch_date
     , olp.order_type

     , SUM(olp.total_return_unit) AS total_return_unit
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
         JOIN reporting_prod.gfb.merch_dim_product mdp
              ON mdp.business_unit = olp.business_unit
                  AND mdp.region = olp.region
                  AND mdp.country = olp.country
                  AND mdp.product_sku = olp.product_sku
         LEFT JOIN (SELECT DISTINCT business_unit, product_sku, region, size, clean_size, core_size_flag
                    FROM reporting_prod.gfb.view_product_size_mapping) psm
                   ON psm.product_sku = olp.product_sku
                       AND psm.region = olp.region
                       AND psm.business_unit = olp.business_unit
                       AND LOWER(psm.size) = LOWER(olp.dp_size)
WHERE olp.order_classification = 'product order'
  AND olp.return_date >= $start_date
  AND olp.return_date < $end_date
GROUP BY olp.business_unit
       , mdp.sub_brand
       , olp.region
       , olp.country
       , olp.product_sku
       , olp.sku
       , olp.return_date
       , olp.return_reason
       , psm.clean_size
       , psm.core_size_flag
       , mdp.department_detail
       , (CASE
              WHEN mdp.department_detail IN ('GIRLS SHOES', 'BOYS SHOES') THEN mdp.subclass
              ELSE mdp.subcategory
    END)
       , mdp.ww_wc
       , mdp.large_img_url
       , mdp.style_name
       , mdp.latest_launch_date
       , olp.order_type;


CREATE OR REPLACE TEMPORARY TABLE _review_words AS
SELECT rw.business_unit
     , rw.sub_brand
     , rw.region
     , rw.country
     , rw.review_id
     , rw.review_submission_date
     , rw.customer_review
     , words.value::STRING AS word
FROM (
    SELECT pr.business_unit
         , pr.sub_brand
         , pr.region
         , pr.country
         , pr.review_id
         , pr.review_submission_date
         , pr.customer_review
         , LOWER(REPLACE(REPLACE(REPLACE(pr.customer_review, ',', ''), '.', ''), '!', '')) AS clean_review
         , SPLIT(TRIM(clean_review), ' ')                                                  AS review_words
    FROM reporting_prod.gfb.gfb019_product_review pr
) rw
   , LATERAL FLATTEN(INPUT => rw.review_words) words;


CREATE OR REPLACE TEMPORARY TABLE _review_details AS
SELECT pr.review_id
     , pr.product_sku
     , pr.sku
     , pr.department_detail
     , pr.subcategory
     , pr.ww_wc
     , pr.core_size_flag
     , pr.order_type
     , pr.size_review
     , pr.latest_launch_date

     , AVG(pr.overall_rating) AS overall_rating
     , AVG(pr.recommended)    AS recommended
FROM reporting_prod.gfb.gfb019_product_review pr
GROUP BY pr.review_id
       , pr.product_sku
       , pr.sku
       , pr.department_detail
       , pr.subcategory
       , pr.ww_wc
       , pr.core_size_flag
       , pr.order_type
       , pr.size_review
       , pr.latest_launch_date;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb019_03_product_review_words AS
SELECT rw.business_unit
     , rw.sub_brand
     , rw.region
     , rw.country
     , rw.review_id
     , rw.review_submission_date
     , rw.customer_review
     , rw.word
     , rd.product_sku
     , rd.sku
     , rd.department_detail
     , rd.subcategory
     , rd.ww_wc
     , rd.core_size_flag
     , rd.order_type
     , rd.size_review
     , rd.overall_rating
     , rd.recommended
     , rd.latest_launch_date
FROM _review_words rw
         JOIN _review_details rd
              ON rd.review_id = rw.review_id;


DELETE
FROM reporting_prod.gfb.gfb019_03_product_review_words
WHERE word = '';
