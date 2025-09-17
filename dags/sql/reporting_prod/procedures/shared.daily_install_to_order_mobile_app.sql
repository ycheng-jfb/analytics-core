SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _first_app_orders AS
SELECT DISTINCT customer_id,
                date,
                order_platform,
                rnk_order_by_store_type,
                session_month_date,
                membership_state,
                operating_system
FROM shared.sessions_order_stg
WHERE order_platform = 'Mobile App'
  AND rnk_order_by_store_type IN (1, 2);

TRUNCATE TABLE shared.daily_install_to_order_mobile_app;

INSERT INTO shared.daily_install_to_order_mobile_app (
    brand,
    region,
    country,
    store,
    date,
    viptenure,
    viptenuregroup,
    customergender,
    membershipstate,
    daystoapporder7,
    monthstoapporder1,
    isapporder,
    operating_system,
    newappcustomers,
    meta_create_datetime,
    meta_update_datetime
)
SELECT s.brand,
       s.region,
       s.country,
       s.store,
       s.session_date                                            AS date,
       s.monthly_vip_tenure                                      AS viptenure,
       s.vip_tenure_group                                        AS viptenuregroup,
       s.customer_gender                                         AS customergender,
       s.membership_state                                        AS membershipstate,
       IFF(DATEDIFF('day', s.session_date, o.date) <= 7, 1, 0)   AS daystoapporder7,
       IFF(DATEDIFF('month', s.session_date, o.date) <= 1, 1, 0) AS monthstoapporder1,
       IFF(o.customer_id IS NOT NULL, 1, 0)                      AS isapporder,
       s.operating_system,
       COUNT(*)                                                  AS newappcustomers,
       $execution_start_time                                     AS meta_create_datetime,
       $execution_start_time                                     AS meta_update_datetime
FROM shared.mobile_app_sessions_stg s
         LEFT JOIN _first_app_orders AS o ON s.customer_id = o.customer_id
    AND rnk_order_by_store_type = 1
WHERE s.is_first_app_session = TRUE
GROUP BY s.brand,
         s.region,
         s.country,
         s.store,
         s.session_date,
         s.monthly_vip_tenure,
         s.vip_tenure_group,
         s.customer_gender,
         s.membership_state,
         IFF(DATEDIFF('day', s.session_date, o.date) <= 7, 1, 0),
         IFF(DATEDIFF('month', s.session_date, o.date) <= 1, 1, 0),
         IFF(o.customer_id IS NOT NULL, 1, 0),
         s.operating_system;
