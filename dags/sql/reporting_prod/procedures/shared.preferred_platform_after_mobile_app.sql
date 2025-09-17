SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _preferred_platform_after_mobile_app__first_app_order AS
SELECT DISTINCT session_id,
                customer_id,
                order_id,
                order_local_datetime,
                is_activating
FROM shared.sessions_order_stg
WHERE order_platform = 'Mobile App'
  AND rnk_order_by_store_type = 1;

CREATE OR REPLACE TEMPORARY TABLE _preferred_platform_after_mobile_app__orders_after_app AS
SELECT s.store_brand_name                                            AS brand,
       s.store_region_abbr                                           AS region,
       s.store_country_abbr                                          AS country,
       s.store_group                                                 AS store,
       CONCAT(s.store_brand_abbr, s.store_country_abbr)              AS store_abbr,
       IFF(o1.order_id = o2.order_id, 1, 0)                          AS is_first_app_order,
       CAST(o1.order_local_datetime AS DATE)                         AS first_app_order_date,
       DATE_TRUNC('month', CAST(o1.order_local_datetime AS DATE))    AS first_app_order_month_date,
       CAST(o2.order_local_datetime AS DATE)                            order_date,
       DATE_TRUNC('month', CAST(o2.order_local_datetime AS DATE))       order_month_date,
       s.customer_id,
       s.session_id,
       o2.order_platform,
       o2.order_id                                                   AS order_id,
       RANK() OVER (PARTITION BY s.customer_id ORDER BY o2.order_id) AS rnk_orders
FROM _preferred_platform_after_mobile_app__first_app_order AS o1
         JOIN shared.sessions_order_stg AS o2 ON o1.customer_id = o2.customer_id
    AND o2.order_id >= o1.order_id
         JOIN shared.sessions_by_platform AS s ON o2.session_id = s.session_id;

TRUNCATE TABLE shared.preferred_platform_after_mobile_app;

INSERT INTO shared.preferred_platform_after_mobile_app
    (brand,
     region,
     country,
     store,
     storeabbr,
     isfirstapporder,
     firstappordermonthdate,
     ordermonthdate,
     orderplatform,
     rnkorders,
     orders,
     meta_create_datetime,
     meta_update_datetime)
SELECT brand,
       region,
       country,
       store,
       store_abbr                 AS storeabbr,
       is_first_app_order         AS isfirstapporder,
       first_app_order_month_date AS firstappordermonthdate,
       order_month_date           AS ordermonthdate,
       order_platform             AS orderplatform,
       rnk_orders                 AS rnkorders,
       COUNT(*)                   AS orders,
       $execution_start_time      AS meta_create_datetime,
       $execution_start_time      AS meta_update_datetime
FROM _preferred_platform_after_mobile_app__orders_after_app
GROUP BY brand,
         region,
         country,
         store,
         store_abbr,
         is_first_app_order,
         first_app_order_month_date,
         order_month_date,
         order_platform,
         rnk_orders;
