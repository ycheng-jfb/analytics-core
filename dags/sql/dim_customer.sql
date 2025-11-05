truncate table EDW_PROD.NEW_STG.DIM_CUSTOMER;

insert into EDW_PROD.NEW_STG.DIM_CUSTOMER

WITH latest_subscription AS (
    SELECT *
    FROM LAKE_MMOS."mmos_membership_marketing_shoedazzle"."user_subscription_actions_shard_all" a
--     WHERE a."event" = 'create_subscription'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a."user_id" ORDER BY a."created_at" DESC) = 1
)
,latest_subscription_jf AS (
    SELECT *
    FROM LAKE_MMOS."mmos_membership_marketing_us"."user_subscription_actions_shard_all" a
--     WHERE a."event" = 'create_subscription'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a."user_id" ORDER BY a."created_at" DESC) = 1
)
,latest_subscription_fk AS (
    SELECT *
    FROM LAKE_MMOS."mmos_membership_marketing_fabkids"."user_subscription_actions_shard_all" a
--     WHERE a."event" = 'create_subscription'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a."user_id" ORDER BY a."created_at" DESC) = 1
)
,latest_subscription_eu AS (
    SELECT *
    FROM LAKE_MMOS."mmos_membership_marketing_eu".user_subscription_actions_shard_all a
--     WHERE a."event" = 'create_subscription'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a."user_id" ORDER BY a."created_at" DESC) = 1
)
,user_shard_all_store_id as (
    select *
    from LAKE_MMOS."mmos_membership_marketing_eu".user_shard_all
    where "is_delete" = 0
)
, results as (
    SELECT
        us."id" AS customer_id,
        us."member_number" AS meta_original_customer_id,
        ud."first_name",
        ud."last_name",
        us."email",
        55 AS store_id,
        55 AS primary_registration_store_id,
        55 AS secondary_registration_store_id,
        NULL AS is_secondary_registration_after_vip,
        NULL AS default_address_id,
        NULL AS default_address1,
        NULL AS default_address2,
        NULL AS default_city,
        NULL AS default_state_province,
        NULL AS default_postal_code,
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US'),
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US') AS specialty_country_code,
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US') AS finance_specialty_store,
        ud."birthday_day" AS birth_day,
        ud."birthday_month" AS birth_month,
        ud."birthday_year" AS birth_year,
        NULL AS quiz_zip,
        NULL AS top_size,
        NULL AS bottom_size,
        NULL AS bra_size,
        NULL AS onepiece_size,
        NULL AS denim_size,
        NULL AS dress_size,
        NULL AS shoe_size,
        NULL AS gender,
        NULL AS first_gender,
        NULL AS is_friend_referral,
        CASE
                WHEN us."level" = 0 THEN 'Member'
                WHEN us."level" = 1 THEN 'Purchasing Member (Previous Active Subscription)'
                WHEN us."level" = 10 THEN 'VIP'
                WHEN us."level" = 11 THEN 'Elite VIP'
                ELSE 'Unknown'
        END AS membership_level,
        NULL AS how_did_you_hear,
        NULL AS how_did_you_hear_parent,
        TO_TIMESTAMP(us."created_at") AS registration_local_datetime,
        NULL AS secondary_registration_local_datetime,
        CASE WHEN bl."email" IS NOT NULL THEN TRUE ELSE FALSE END AS is_blacklisted,
        NULL AS blacklist_reason,
        NULL AS is_free_trial,
        NULL AS is_free_trial_promo,
        NULL AS is_free_trial_gift_order,
        NULL AS is_cross_promo,
        NULL AS is_metanav,
        NULL AS is_scrubs_customer,
        0 AS is_test_customer,
        NULL AS is_opt_out,
        NULL AS email_opt_in_datetime,
        NULL AS sms_opt_in_datetime,
        -- CASE
        --      WHEN lms.membership_event_type IN ('Registration','Failed Activation from Lead') THEN 'Lead'
        --      WHEN lms.membership_event_type = 'Deactivated Lead' THEN 'Deactivated Lead'
        --      WHEN lms.membership_event_type IN ('Activation','Failed Activation from Classic VIP') THEN 'VIP'
        --      WHEN lms.membership_event_type = 'Guest Purchasing Member' AND lms.membership_type_detail = 'Unidentified' THEN 'Guest - Unidentified'
        --      WHEN (lms.membership_event_type = 'Guest Purchasing Member' AND lms.membership_type_detail = 'Previous VIP') OR (lms.membership_event_type = 'Cancellation' AND lms.membership_event_type = 'Guest Purchasing Member') THEN 'Guest - Previous VIP'
        --      WHEN lms.membership_event_type IN ('Guest Purchasing Member','Failed Activation from Guest') THEN 'Guest'
        --      WHEN lms.membership_event_type IN ('Cancellation','Failed Activation from Cancel') THEN 'Cancelled'
        --      WHEN lms.membership_event_type = 'Free Trial Activation' THEN 'Free Trial VIP'
        --      WHEN lms.membership_event_type IN ('Free Trial Downgrade', 'Failed Activation from Free Trial Downgrade', 'Failed Activation from Free Trial Activation') THEN 'Free Trial Downgrade'
        --      WHEN lms.membership_event_type IN ('Email Signup') THEN 'Prospect'
        -- ELSE 'Unknown'
        -- END AS membership_state,
        null AS membership_state,

        NULL AS bralette_size,
        NULL AS undie_size,
        NULL AS lingerie_sleep_size,
        NULL AS emp_optin_datetime,
        NULL AS first_mobile_app_session_local_datetime,
        NULL AS first_mobile_app_session_id,
        NULL AS first_mobile_app_order_local_datetime,
        NULL AS first_mobile_app_order_id,
        NULL AS mobile_app_cohort_month_date,
        NULL AS mobile_app_cohort_os,
        NULL AS onetrust_consent_strictlynecessary,
        NULL AS onetrust_consent_performance,
        NULL AS onetrust_consent_functional,
        NULL AS onetrust_consent_targeting,
        NULL AS onetrust_consent_social,
        NULL AS onetrust_update_datetime,
        NULL AS registration_type,
        'Membership Token' current_membership_type,
        ls."pay_order_unit_price" AS membership_price,
        55 membership_plan_id,
        null customer_bucket_group ,--     IFF(us.datetime_added::DATE < '2023-07-27', (ud.meta_original_customer_id % 20)+1,ROUND(((ud.meta_original_customer_id % 80) / 4),0)+1) customer_bucket_group
        current_date,
        current_date
    FROM LAKE_MMOS."mmos_membership_marketing_shoedazzle"."user_shard_all" us
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_shoedazzle"."user_data_shard_all" ud ON us."id" = ud."id"
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_shoedazzle"."user_blacklist" bl ON us."email" = bl."email"
    left join latest_subscription ls on ls."user_id" = us."id"
    -- left join EDW_PROD.data_model_jfb.fact_membership_event lms on lms.CUSTOMER_ID = us."member_number"
    WHERE us."is_delete" = 0

    union all 

    SELECT distinct
        us."id" AS customer_id,
        us."member_number" AS meta_original_customer_id,
        ud."first_name",
        ud."last_name",
        us."email",
        26 AS store_id,
        26 AS primary_registration_store_id,
        26 AS secondary_registration_store_id,
        NULL AS is_secondary_registration_after_vip,
        NULL AS default_address_id,
        NULL AS default_address1,
        NULL AS default_address2,
        NULL AS default_city,
        NULL AS default_state_province,
        NULL AS default_postal_code,
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US'),
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US') AS specialty_country_code,
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US') AS finance_specialty_store,
        ud."birthday_day" AS birth_day,
        ud."birthday_month" AS birth_month,
        ud."birthday_year" AS birth_year,
        NULL AS quiz_zip,
        NULL AS top_size,
        NULL AS bottom_size,
        NULL AS bra_size,
        NULL AS onepiece_size,
        NULL AS denim_size,
        NULL AS dress_size,
        NULL AS shoe_size,
        NULL AS gender,
        NULL AS first_gender,
        NULL AS is_friend_referral,
        CASE
                WHEN us."level" = 0 THEN 'Member'
                WHEN us."level" = 1 THEN 'Purchasing Member (Previous Active Subscription)'
                WHEN us."level" = 10 THEN 'VIP'
                WHEN us."level" = 11 THEN 'Elite VIP'
                ELSE 'Unknown'
        END AS membership_level,
        NULL AS how_did_you_hear,
        NULL AS how_did_you_hear_parent,
        TO_TIMESTAMP(us."created_at") AS registration_local_datetime,
        NULL AS secondary_registration_local_datetime,
        CASE WHEN bl."email" IS NOT NULL THEN TRUE ELSE FALSE END AS is_blacklisted,
        NULL AS blacklist_reason,
        NULL AS is_free_trial,
        NULL AS is_free_trial_promo,
        NULL AS is_free_trial_gift_order,
        NULL AS is_cross_promo,
        NULL AS is_metanav,
        NULL AS is_scrubs_customer,
        0 AS is_test_customer,
        NULL AS is_opt_out,
        NULL AS email_opt_in_datetime,
        NULL AS sms_opt_in_datetime,
        -- CASE
        --      WHEN lms.membership_event_type IN ('Registration','Failed Activation from Lead') THEN 'Lead'
        --      WHEN lms.membership_event_type = 'Deactivated Lead' THEN 'Deactivated Lead'
        --      WHEN lms.membership_event_type IN ('Activation','Failed Activation from Classic VIP') THEN 'VIP'
        --      WHEN lms.membership_event_type = 'Guest Purchasing Member' AND lms.membership_type_detail = 'Unidentified' THEN 'Guest - Unidentified'
        --      WHEN (lms.membership_event_type = 'Guest Purchasing Member' AND lms.membership_type_detail = 'Previous VIP') OR (lms.membership_event_type = 'Cancellation' AND lms.membership_event_type = 'Guest Purchasing Member') THEN 'Guest - Previous VIP'
        --      WHEN lms.membership_event_type IN ('Guest Purchasing Member','Failed Activation from Guest') THEN 'Guest'
        --      WHEN lms.membership_event_type IN ('Cancellation','Failed Activation from Cancel') THEN 'Cancelled'
        --      WHEN lms.membership_event_type = 'Free Trial Activation' THEN 'Free Trial VIP'
        --      WHEN lms.membership_event_type IN ('Free Trial Downgrade', 'Failed Activation from Free Trial Downgrade', 'Failed Activation from Free Trial Activation') THEN 'Free Trial Downgrade'
        --      WHEN lms.membership_event_type IN ('Email Signup') THEN 'Prospect'
        -- ELSE 'Unknown'
        -- END AS membership_state,
        null AS membership_state,
        NULL AS bralette_size,
        NULL AS undie_size,
        NULL AS lingerie_sleep_size,
        NULL AS emp_optin_datetime,
        NULL AS first_mobile_app_session_local_datetime,
        NULL AS first_mobile_app_session_id,
        NULL AS first_mobile_app_order_local_datetime,
        NULL AS first_mobile_app_order_id,
        NULL AS mobile_app_cohort_month_date,
        NULL AS mobile_app_cohort_os,
        NULL AS onetrust_consent_strictlynecessary,
        NULL AS onetrust_consent_performance,
        NULL AS onetrust_consent_functional,
        NULL AS onetrust_consent_targeting,
        NULL AS onetrust_consent_social,
        NULL AS onetrust_update_datetime,
        NULL AS registration_type,
        'Membership Token' current_membership_type,
        ls."pay_order_unit_price" AS membership_price,
        26 membership_plan_id,
        null customer_bucket_group ,--     IFF(us.datetime_added::DATE < '2023-07-27', (ud.meta_original_customer_id % 20)+1,ROUND(((ud.meta_original_customer_id % 80) / 4),0)+1) customer_bucket_group
        current_date,
        current_date
    FROM LAKE_MMOS."mmos_membership_marketing_us"."user_shard_all" us
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_us"."user_data_shard_all" ud ON us."id" = ud."id"
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_us"."user_blacklist" bl ON us."email" = bl."email"
    left join latest_subscription_jf ls on ls."user_id" = us."id"
    -- left join EDW_PROD.data_model_jfb.fact_membership_event lms on lms.CUSTOMER_ID = us."member_number"
    WHERE us."is_delete" = 0


    UNION ALL

    SELECT distinct
        us."id" AS customer_id,
        us."member_number" AS meta_original_customer_id,
        ud."first_name",
        ud."last_name",
        us."email",
        46 AS store_id,
        46 AS primary_registration_store_id,
        46 AS secondary_registration_store_id,
        NULL AS is_secondary_registration_after_vip,
        NULL AS default_address_id,
        NULL AS default_address1,
        NULL AS default_address2,
        NULL AS default_city,
        NULL AS default_state_province,
        NULL AS default_postal_code,
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US'),
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US') AS specialty_country_code,
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US') AS finance_specialty_store,
        ud."birthday_day" AS birth_day,
        ud."birthday_month" AS birth_month,
        ud."birthday_year" AS birth_year,
        NULL AS quiz_zip,
        NULL AS top_size,
        NULL AS bottom_size,
        NULL AS bra_size,
        NULL AS onepiece_size,
        NULL AS denim_size,
        NULL AS dress_size,
        NULL AS shoe_size,
        NULL AS gender,
        NULL AS first_gender,
        NULL AS is_friend_referral,
        CASE
                WHEN us."level" = 0 THEN 'Member'
                WHEN us."level" = 1 THEN 'Purchasing Member (Previous Active Subscription)'
                WHEN us."level" = 10 THEN 'VIP'
                WHEN us."level" = 11 THEN 'Elite VIP'
                ELSE 'Unknown'
        END AS membership_level,
        NULL AS how_did_you_hear,
        NULL AS how_did_you_hear_parent,
        TO_TIMESTAMP(us."created_at") AS registration_local_datetime,
        NULL AS secondary_registration_local_datetime,
        CASE WHEN bl."email" IS NOT NULL THEN TRUE ELSE FALSE END AS is_blacklisted,
        NULL AS blacklist_reason,
        NULL AS is_free_trial,
        NULL AS is_free_trial_promo,
        NULL AS is_free_trial_gift_order,
        NULL AS is_cross_promo,
        NULL AS is_metanav,
        NULL AS is_scrubs_customer,
        0 AS is_test_customer,
        NULL AS is_opt_out,
        NULL AS email_opt_in_datetime,
        NULL AS sms_opt_in_datetime,
        -- CASE
        --      WHEN lms.membership_event_type IN ('Registration','Failed Activation from Lead') THEN 'Lead'
        --      WHEN lms.membership_event_type = 'Deactivated Lead' THEN 'Deactivated Lead'
        --      WHEN lms.membership_event_type IN ('Activation','Failed Activation from Classic VIP') THEN 'VIP'
        --      WHEN lms.membership_event_type = 'Guest Purchasing Member' AND lms.membership_type_detail = 'Unidentified' THEN 'Guest - Unidentified'
        --      WHEN (lms.membership_event_type = 'Guest Purchasing Member' AND lms.membership_type_detail = 'Previous VIP') OR (lms.membership_event_type = 'Cancellation' AND lms.membership_event_type = 'Guest Purchasing Member') THEN 'Guest - Previous VIP'
        --      WHEN lms.membership_event_type IN ('Guest Purchasing Member','Failed Activation from Guest') THEN 'Guest'
        --      WHEN lms.membership_event_type IN ('Cancellation','Failed Activation from Cancel') THEN 'Cancelled'
        --      WHEN lms.membership_event_type = 'Free Trial Activation' THEN 'Free Trial VIP'
        --      WHEN lms.membership_event_type IN ('Free Trial Downgrade', 'Failed Activation from Free Trial Downgrade', 'Failed Activation from Free Trial Activation') THEN 'Free Trial Downgrade'
        --      WHEN lms.membership_event_type IN ('Email Signup') THEN 'Prospect'
        -- ELSE 'Unknown'
        -- END AS membership_state,
        null AS membership_state,
        NULL AS bralette_size,
        NULL AS undie_size,
        NULL AS lingerie_sleep_size,
        NULL AS emp_optin_datetime,
        NULL AS first_mobile_app_session_local_datetime,
        NULL AS first_mobile_app_session_id,
        NULL AS first_mobile_app_order_local_datetime,
        NULL AS first_mobile_app_order_id,
        NULL AS mobile_app_cohort_month_date,
        NULL AS mobile_app_cohort_os,
        NULL AS onetrust_consent_strictlynecessary,
        NULL AS onetrust_consent_performance,
        NULL AS onetrust_consent_functional,
        NULL AS onetrust_consent_targeting,
        NULL AS onetrust_consent_social,
        NULL AS onetrust_update_datetime,
        NULL AS registration_type,
        'Membership Token' current_membership_type,
        ls."pay_order_unit_price" AS membership_price,
        46 membership_plan_id,
        null customer_bucket_group ,--     IFF(us.datetime_added::DATE < '2023-07-27', (ud.meta_original_customer_id % 20)+1,ROUND(((ud.meta_original_customer_id % 80) / 4),0)+1) customer_bucket_group
        current_date,
        current_date
    FROM LAKE_MMOS."mmos_membership_marketing_fabkids".user_shard_all us
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_fabkids"."user_data_shard_all" ud ON us."id" = ud."id"
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_fabkids"."user_blacklist" bl ON us."email" = bl."email"
    left join latest_subscription_fk ls on ls."user_id" = us."id"
    -- left join EDW_PROD.data_model_jfb.fact_membership_event lms on lms.CUSTOMER_ID = us."member_number"
    WHERE us."is_delete" = 0

    UNION ALL

    SELECT distinct
        us."id" AS customer_id,
        us."member_number" AS meta_original_customer_id,
        ud."first_name",
        ud."last_name",
        us."email",
        us.store_id AS store_id,
        us.store_id AS primary_registration_store_id,
        us.store_id AS secondary_registration_store_id,
        NULL AS is_secondary_registration_after_vip,
        NULL AS default_address_id,
        NULL AS default_address1,
        NULL AS default_address2,
        NULL AS default_city,
        NULL AS default_state_province,
        NULL AS default_postal_code,
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US'),
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US') AS specialty_country_code,
        iff(us."country_code" is not null and us."country_code"<>'',us."country_code",'US') AS finance_specialty_store,
        ud."birthday_day" AS birth_day,
        ud."birthday_month" AS birth_month,
        ud."birthday_year" AS birth_year,
        NULL AS quiz_zip,
        NULL AS top_size,
        NULL AS bottom_size,
        NULL AS bra_size,
        NULL AS onepiece_size,
        NULL AS denim_size,
        NULL AS dress_size,
        NULL AS shoe_size,
        NULL AS gender,
        NULL AS first_gender,
        NULL AS is_friend_referral,
        CASE
                WHEN us."level" = 0 THEN 'Member'
                WHEN us."level" = 1 THEN 'Purchasing Member (Previous Active Subscription)'
                WHEN us."level" = 10 THEN 'VIP'
                WHEN us."level" = 11 THEN 'Elite VIP'
                ELSE 'Unknown'
        END AS membership_level,
        NULL AS how_did_you_hear,
        NULL AS how_did_you_hear_parent,
        TO_TIMESTAMP(us."created_at") AS registration_local_datetime,
        NULL AS secondary_registration_local_datetime,
        CASE WHEN bl."email" IS NOT NULL THEN TRUE ELSE FALSE END AS is_blacklisted,
        NULL AS blacklist_reason,
        NULL AS is_free_trial,
        NULL AS is_free_trial_promo,
        NULL AS is_free_trial_gift_order,
        NULL AS is_cross_promo,
        NULL AS is_metanav,
        NULL AS is_scrubs_customer,
        0 AS is_test_customer,
        NULL AS is_opt_out,
        NULL AS email_opt_in_datetime,
        NULL AS sms_opt_in_datetime,
        -- CASE
        --      WHEN lms.membership_event_type IN ('Registration','Failed Activation from Lead') THEN 'Lead'
        --      WHEN lms.membership_event_type = 'Deactivated Lead' THEN 'Deactivated Lead'
        --      WHEN lms.membership_event_type IN ('Activation','Failed Activation from Classic VIP') THEN 'VIP'
        --      WHEN lms.membership_event_type = 'Guest Purchasing Member' AND lms.membership_type_detail = 'Unidentified' THEN 'Guest - Unidentified'
        --      WHEN (lms.membership_event_type = 'Guest Purchasing Member' AND lms.membership_type_detail = 'Previous VIP') OR (lms.membership_event_type = 'Cancellation' AND lms.membership_event_type = 'Guest Purchasing Member') THEN 'Guest - Previous VIP'
        --      WHEN lms.membership_event_type IN ('Guest Purchasing Member','Failed Activation from Guest') THEN 'Guest'
        --      WHEN lms.membership_event_type IN ('Cancellation','Failed Activation from Cancel') THEN 'Cancelled'
        --      WHEN lms.membership_event_type = 'Free Trial Activation' THEN 'Free Trial VIP'
        --      WHEN lms.membership_event_type IN ('Free Trial Downgrade', 'Failed Activation from Free Trial Downgrade', 'Failed Activation from Free Trial Activation') THEN 'Free Trial Downgrade'
        --      WHEN lms.membership_event_type IN ('Email Signup') THEN 'Prospect'
        -- ELSE 'Unknown'
        -- END AS membership_state,
        null AS membership_state,
        NULL AS bralette_size,
        NULL AS undie_size,
        NULL AS lingerie_sleep_size,
        NULL AS emp_optin_datetime,
        NULL AS first_mobile_app_session_local_datetime,
        NULL AS first_mobile_app_session_id,
        NULL AS first_mobile_app_order_local_datetime,
        NULL AS first_mobile_app_order_id,
        NULL AS mobile_app_cohort_month_date,
        NULL AS mobile_app_cohort_os,
        NULL AS onetrust_consent_strictlynecessary,
        NULL AS onetrust_consent_performance,
        NULL AS onetrust_consent_functional,
        NULL AS onetrust_consent_targeting,
        NULL AS onetrust_consent_social,
        NULL AS onetrust_update_datetime,
        NULL AS registration_type,
        'Membership Token' current_membership_type,
        ls."pay_order_unit_price" AS membership_price,
        us.store_id membership_plan_id,
        null customer_bucket_group ,--     IFF(us.datetime_added::DATE < '2023-07-27', (ud.meta_original_customer_id % 20)+1,ROUND(((ud.meta_original_customer_id % 80) / 4),0)+1) customer_bucket_group
        current_date,
        current_date
    FROM user_shard_all_store_id us
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_eu".user_data_shard_all ud ON us."id" = ud."id"
    LEFT JOIN LAKE_MMOS."mmos_membership_marketing_eu"."user_blacklist" bl ON us."email" = bl."email"
    left join latest_subscription_eu ls on ls."user_id" = us."id"
)
SELECT * from results
;


