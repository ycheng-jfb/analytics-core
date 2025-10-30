CREATE or replace TRANSIENT TABLE lake_consolidated.ultra_merchant_history.MEMBERSHIP (
    data_source_id INT,
    meta_company_id INT,
    membership_id INT,
    customer_id INT,
    store_id INT,
    membership_type_id INT,
    membership_plan_id INT,
    membership_completion_method_id INT,
    membership_signup_id INT,
    membership_reward_plan_id INT,
    order_id INT,
    order_tracking_id INT,
    discount_id INT,
    shipping_option_id INT,
    offer_id INT,
    shipping_address_id INT,
    payment_method VARCHAR(25),
    payment_object_id INT,
    payment_option_id INT,
    current_membership_recommendation_id INT,
    current_period_id INT,
    next_period_id INT,
    membership_level_id INT,
    membership_team_id INT,
    price NUMBER(19, 4),
    max_prepaid_credits INT,
    date_added TIMESTAMP_NTZ(0),
    datetime_added TIMESTAMP_NTZ(3),
    datetime_modified TIMESTAMP_NTZ(3),
    date_next_scheduled TIMESTAMP_NTZ(0),
    datetime_activated TIMESTAMP_NTZ(3),
    datetime_cancelled TIMESTAMP_NTZ(3),
    date_expires TIMESTAMP_NTZ(0),
    statuscode INT,
    membership_reward_tier_id INT,
    date_reward_tier_recalculate TIMESTAMP_NTZ(0),
    date_reward_tier_updated TIMESTAMP_NTZ(0),
    billing_month INT,
    billing_day INT,
    membership_brand_id INT,
    meta_original_membership_id INT,
    meta_row_source VARCHAR(40),
    hvr_change_op INT,
    effective_start_datetime TIMESTAMP_LTZ(3),
    effective_end_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);


SET lake_jfb_watermark = (
    SELECT MIN(last_update)
    FROM (
        SELECT CAST(min(META_SOURCE_CHANGE_DATETIME) AS TIMESTAMP_LTZ(3)) AS last_update
        FROM lake_jfb.ultra_merchant_history.MEMBERSHIP
    ) AS A
);


CREATE OR REPLACE TEMP TABLE _lake_jfb_membership_history AS
SELECT DISTINCT

    membership_id,
    customer_id,
    store_id,
    membership_type_id,
    membership_plan_id,
    membership_completion_method_id,
    membership_signup_id,
    membership_reward_plan_id,
    order_id,
    order_tracking_id,
    discount_id,
    shipping_option_id,
    offer_id,
    shipping_address_id,
    payment_method,
    payment_object_id,
    payment_option_id,
    current_membership_recommendation_id,
    current_period_id,
    next_period_id,
    membership_level_id,
    membership_team_id,
    price,
    max_prepaid_credits,
    date_added,
    datetime_added,
    datetime_modified,
    date_next_scheduled,
    datetime_activated,
    datetime_cancelled,
    date_expires,
    statuscode,
    membership_reward_tier_id,
    date_reward_tier_recalculate,
    date_reward_tier_updated,
    billing_month,
    billing_day,
    membership_brand_id,
    meta_row_source,
    hvr_change_op,
    CAST(META_SOURCE_CHANGE_DATETIME AS TIMESTAMP_LTZ(3))  as effective_start_datetime,
    10 as data_source_id
FROM lake_jfb.ultra_merchant_history.MEMBERSHIP
WHERE META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
QUALIFY ROW_NUMBER() OVER(PARTITION BY membership_id, META_SOURCE_CHANGE_DATETIME
    ORDER BY HVR_CHANGE_SEQUENCE DESC) = 1;


CREATE OR REPLACE TEMP TABLE _lake_jfb_company_membership AS
SELECT membership_id, company_id as meta_company_id
FROM (
     SELECT DISTINCT
         L.membership_id,
         DS.company_id
     FROM lake_jfb.REFERENCE.DIM_STORE AS DS
     INNER JOIN lake_jfb.ultra_merchant.membership_plan AS mp
      ON DS.STORE_ID= mp.STORE_ID
     INNER JOIN lake_jfb.ultra_merchant_history.membership AS L
     ON L.MEMBERSHIP_PLAN_ID=mp.MEMBERSHIP_PLAN_ID
    WHERE l.META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
    ) AS l
QUALIFY ROW_NUMBER() OVER(PARTITION BY membership_id ORDER BY company_id ASC) = 1;


INSERT INTO lake_consolidated.ultra_merchant_history.MEMBERSHIP (
    data_source_id,
    meta_company_id,
    membership_id,
    customer_id,
    store_id,
    membership_type_id,
    membership_plan_id,
    membership_completion_method_id,
    membership_signup_id,
    membership_reward_plan_id,
    order_id,
    order_tracking_id,
    discount_id,
    shipping_option_id,
    offer_id,
    shipping_address_id,
    payment_method,
    payment_object_id,
    payment_option_id,
    current_membership_recommendation_id,
    current_period_id,
    next_period_id,
    membership_level_id,
    membership_team_id,
    price,
    max_prepaid_credits,
    date_added,
    datetime_added,
    datetime_modified,
    date_next_scheduled,
    datetime_activated,
    datetime_cancelled,
    date_expires,
    statuscode,
    membership_reward_tier_id,
    date_reward_tier_recalculate,
    date_reward_tier_updated,
    billing_month,
    billing_day,
    membership_brand_id,
    meta_original_membership_id,
    meta_row_source,
    hvr_change_op,
    effective_start_datetime,
    effective_end_datetime,
    meta_update_datetime
)
SELECT DISTINCT
    s.data_source_id,
    s.meta_company_id,

    CASE WHEN NULLIF(s.membership_id, '0') IS NOT NULL THEN CONCAT(s.membership_id, s.meta_company_id) ELSE NULL END as membership_id,
    CASE WHEN NULLIF(s.customer_id, '0') IS NOT NULL THEN CONCAT(s.customer_id, s.meta_company_id) ELSE NULL END as customer_id,
    s.store_id,
    s.membership_type_id,
    CASE WHEN NULLIF(s.membership_plan_id, '0') IS NOT NULL THEN CONCAT(s.membership_plan_id, s.meta_company_id) ELSE NULL END as membership_plan_id,
    s.membership_completion_method_id,
    CASE WHEN NULLIF(s.membership_signup_id, '0') IS NOT NULL THEN CONCAT(s.membership_signup_id, s.meta_company_id) ELSE NULL END as membership_signup_id,
    CASE WHEN NULLIF(s.membership_reward_plan_id, '0') IS NOT NULL THEN CONCAT(s.membership_reward_plan_id, s.meta_company_id) ELSE NULL END as membership_reward_plan_id,
    CASE WHEN NULLIF(s.order_id, '0') IS NOT NULL THEN CONCAT(s.order_id, s.meta_company_id) ELSE NULL END as order_id,
    s.order_tracking_id,
    CASE WHEN NULLIF(s.discount_id, '0') IS NOT NULL THEN CONCAT(s.discount_id, s.meta_company_id) ELSE NULL END as discount_id,
    s.shipping_option_id,
    s.offer_id,
    CASE WHEN NULLIF(s.shipping_address_id, '0') IS NOT NULL THEN CONCAT(s.shipping_address_id, s.meta_company_id) ELSE NULL END as shipping_address_id,
    s.payment_method,
    s.payment_object_id,
    s.payment_option_id,
    s.current_membership_recommendation_id,
    s.current_period_id,
    s.next_period_id,
    s.membership_level_id,
    s.membership_team_id,
    s.price,
    s.max_prepaid_credits,
    s.date_added,
    s.datetime_added,
    s.datetime_modified,
    s.date_next_scheduled,
    s.datetime_activated,
    s.datetime_cancelled,
    s.date_expires,
    s.statuscode,
    CASE WHEN NULLIF(s.membership_reward_tier_id, '0') IS NOT NULL THEN CONCAT(s.membership_reward_tier_id, s.meta_company_id) ELSE NULL END as membership_reward_tier_id,
    s.date_reward_tier_recalculate,
    s.date_reward_tier_updated,
    s.billing_month,
    s.billing_day,
    s.membership_brand_id,
    s.membership_id as meta_original_membership_id,
    s.meta_row_source,
    s.hvr_change_op,
    s.effective_start_datetime,
    NULL AS effective_end_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime
FROM (
    SELECT
        A.*,
        COALESCE(CJ.meta_company_id, 40) AS meta_company_id
    FROM _lake_jfb_membership_history AS A
    LEFT JOIN _lake_jfb_company_membership AS CJ
        ON cj.membership_id = a.membership_id
    WHERE COALESCE(CJ.meta_company_id, 40) IN (10)
    ) AS s
WHERE NOT EXISTS (
    SELECT
        1
    FROM lake_consolidated.ultra_merchant_history.MEMBERSHIP AS t
    WHERE
        s.data_source_id = t.data_source_id
        AND          s.membership_id = t.meta_original_membership_id
        AND s.effective_start_datetime = t.effective_start_datetime
    )
ORDER BY s.effective_start_datetime ASC;


CREATE OR REPLACE temp table _membership_updates as
SELECT s.*,
    row_number() over(partition by s.data_source_id,

        s.meta_original_membership_id
ORDER BY s.effective_start_datetime ASC) AS rnk
FROM (
SELECT DISTINCT
    s.data_source_id,

        s.meta_original_membership_id,
    s.effective_start_datetime,
    s.effective_end_datetime
FROM lake_consolidated.ultra_merchant_history.MEMBERSHIP as s
INNER JOIN (
    SELECT
        data_source_id,

        meta_original_membership_id,
        effective_start_datetime
    FROM lake_consolidated.ultra_merchant_history.MEMBERSHIP
    WHERE effective_end_datetime is NULL
    ) AS t
    ON s.data_source_id = t.data_source_id
    AND     s.meta_original_membership_id = t.meta_original_membership_id
WHERE (
    s.effective_start_datetime >= t.effective_start_datetime
    OR s.effective_end_datetime = '9999-12-31 00:00:00.000 -0800'
)) s;


CREATE OR REPLACE TEMP TABLE _membership_delta AS
SELECT
    s.data_source_id,

        s.meta_original_membership_id,
    s.effective_start_datetime,
    COALESCE(dateadd(MILLISECOND, -1, t.effective_start_datetime), '9999-12-31 00:00:00.000 -0800') AS new_effective_end_datetime
FROM _membership_updates AS s
    LEFT JOIN _membership_updates AS t
    ON s.data_source_id = t.data_source_id
    AND     s.meta_original_membership_id = t.meta_original_membership_id
    AND S.rnk = T.rnk - 1
WHERE (S.effective_end_datetime <> dateadd(MILLISECOND, -1, T.effective_start_datetime)
    OR S.effective_end_datetime IS NULL) ;


 UPDATE lake_consolidated.ultra_merchant_history.MEMBERSHIP AS t
SET t.effective_end_datetime = s.new_effective_end_datetime,
    META_UPDATE_DATETIME = current_timestamp()
FROM _membership_delta AS s
WHERE s.data_source_id = t.data_source_id
    AND     s.meta_original_membership_id = t.meta_original_membership_id
    AND s.effective_start_datetime = t.effective_start_datetime
    AND COALESCE(t.effective_end_datetime, '1900-01-01 00:00:00 -0800') <> s.new_effective_end_datetime;
