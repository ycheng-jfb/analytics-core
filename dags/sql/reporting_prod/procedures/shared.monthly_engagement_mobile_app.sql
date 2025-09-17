SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _monthly_engagement_mobile_app__first_app_orders AS
SELECT DISTINCT s.customer_id,
                s.date,
                s.order_platform,
                s.rnk_order_by_store_type,
                s.session_month_date,
                s.membership_state,
                s.operating_system
FROM shared.sessions_order_stg s
WHERE s.order_platform = 'Mobile App'
  AND s.rnk_order_by_store_type IN (1, 2);

CREATE OR REPLACE TEMPORARY TABLE _first_app_session_to_app_order AS
SELECT s.brand,
       s.region,
       s.country,
       s.monthly_vip_tenure                                                AS vip_tenure,
       CASE
           WHEN s.monthly_vip_tenure BETWEEN 13 AND 24 THEN 'M13-24'
           WHEN s.monthly_vip_tenure >= 25 THEN 'M25+'
           ELSE CONCAT('M', vip_tenure) END                                AS vip_tenure_group,
       s.session_month_date,
       IFF(o1.customer_id IS NOT NULL OR o2.customer_id IS NOT NULL, 1, 0) AS is_app_order,
       o1.rnk_order_by_store_type                                          AS order_rank_1,
       o1.session_month_date                                               AS order_month_date_1,
       DATEDIFF('month', s.session_month_date, o1.session_month_date) + 1     months_to_order_1,
       o2.rnk_order_by_store_type                                          AS order_rank_2,
       o2.session_month_date                                               AS order_month_date_2,
       DATEDIFF('month', s.session_month_date, o2.session_month_date) + 1     months_to_order_2,
       s.customer_id,
       s.membership_state,
       s.operating_system
FROM shared.mobile_app_sessions_stg s
         LEFT JOIN _monthly_engagement_mobile_app__first_app_orders AS o1
                   ON s.customer_id = o1.customer_id
                       AND o1.session_month_date >= s.session_month_date
                       AND o1.rnk_order_by_store_type = 1
         LEFT JOIN _monthly_engagement_mobile_app__first_app_orders AS o2
                   ON s.customer_id = o2.customer_id
                       AND o2.session_month_date >= s.session_month_date
                       AND o2.rnk_order_by_store_type = 2
WHERE s.is_first_app_session = TRUE;

CREATE OR REPLACE TEMPORARY TABLE _mobile_app_sessions_agg AS
SELECT DISTINCT brand,
                region,
                country,
                monthly_vip_tenure              AS vip_tenure,
                vip_tenure_group,
                session_month_date,
                customer_id,
                customer_gender,
                membership_state,
                is_first_app_session,
                operating_system,
                SUM(IFF(rnk_daily = 1, 1, 0))   AS active_users_daily,
                SUM(IFF(rnk_monthly = 1, 1, 0)) AS active_users_monthly
FROM shared.mobile_app_sessions_stg AS s
GROUP BY brand,
         region,
         country,
         monthly_vip_tenure,
         vip_tenure_group,
         session_month_date,
         customer_id,
         customer_gender,
         membership_state,
         is_first_app_session,
         operating_system;

TRUNCATE TABLE shared.monthly_engagement_mobile_app;

INSERT INTO shared.monthly_engagement_mobile_app (
    brand,
    region,
    country,
    viptenure,
    viptenuregroup,
    membership_state,
    customergender,
    date,
    isapporder,
    ordermonthdate1,
    monthstoorders1,
    ordermonthdate2,
    monthstoorders2,
    operating_system,
    newappcustomers,
    activeusersdaily,
    activeusersmonthly,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT s.brand,
       s.region,
       s.country,
       s.vip_tenure                                                                 AS viptenure,
       s.vip_tenure_group                                                           AS viptenuregroup,
       s.membership_state,
       s.customer_gender                                                            AS customergender,
       s.session_month_date                                                         AS date,
       o.is_app_order                                                               AS isapporder,
       o.order_month_date_1                                                         AS ordermonthdate1,
       o.months_to_order_1                                                          AS monthstoorders1,
       o.order_month_date_2                                                         AS ordermonthdate2,
       o.months_to_order_2                                                          AS monthstoorders2,
       s.operating_system,
       COUNT(DISTINCT CASE WHEN is_first_app_session = TRUE THEN s.customer_id END) AS newappcustomers,
       SUM(s.active_users_daily)                                                    AS activeusersdaily,
       SUM(s.active_users_monthly)                                                  AS activeusersmonthly,
       $execution_start_time                                                        AS meta_create_datetime,
       $execution_start_time                                                        AS meta_update_datetime
FROM _mobile_app_sessions_agg AS s
         LEFT JOIN _first_app_session_to_app_order AS o
                   ON s.session_month_date = o.session_month_date
                       AND s.customer_id = o.customer_id
GROUP BY s.brand,
         s.region,
         s.country,
         s.vip_tenure,
         s.vip_tenure_group,
         s.membership_state,
         s.customer_gender,
         s.session_month_date,
         o.is_app_order,
         o.order_month_date_1,
         o.months_to_order_1,
         o.order_month_date_2,
         o.months_to_order_2,
         s.operating_system;
