SET data_update_days = 3;
SET start_date = DATEADD(DAY, -$data_update_days, TO_DATE(CURRENT_TIMESTAMP()));
SET brand_abbr = 'SX';


CREATE OR REPLACE TEMPORARY TABLE _identify AS
SELECT userid                                                                               AS user_id,
       TO_TIMESTAMP(timestamp)                                                              AS memb_start,
       IFF(LOWER(traits_membership_status) LIKE 'payg', 'paygo',
           LOWER(traits_membership_status))                                                    membership_status,
       IFF(traits_email LIKE '%@test.com' OR traits_email LIKE '%@example.com', 'test', '') AS test_account,
       COALESCE(LEAD(TO_TIMESTAMP(timestamp)) OVER (PARTITION BY userid
           ORDER BY TO_TIMESTAMP(timestamp)), '9999-12-31')                                 AS memb_end
FROM lake.segment_sxf.javascript_sxf_identify;


CREATE OR REPLACE TEMPORARY TABLE _ses_user AS
SELECT properties_session_id                     AS session_id,
       MAX(COALESCE(userid, properties_user_id)) AS user_id
FROM lake.segment_sxf.javascript_sxf_page
WHERE COALESCE(userid, properties_user_id) IS NOT NULL
  AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp))::DATE >= $start_date
GROUP BY session_id;


CREATE OR REPLACE TEMPORARY TABLE _raw_pdp_impressions AS
SELECT DISTINCT CAST(IFF(properties_product_id::STRING = '', '0',
                         properties_product_id::STRING) AS BIGINT)                      AS impression_mpid,
                CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) AS impression_date,
                so.properties_session_id                                                AS session_id,
                u.user_id                                                               AS customer_id,
                COALESCE(i.membership_status, 'prospect')                               AS membership_status,
                country as impression_country,
                properties_list as list_id
FROM lake.segment_sxf.javascript_sxf_product_viewed so
         LEFT JOIN _ses_user AS u ON so.properties_session_id = u.session_id
         LEFT JOIN _identify i ON u.user_id = i.user_id
    AND (TO_TIMESTAMP(timestamp) >= i.memb_start AND
         TO_TIMESTAMP(timestamp) < i.memb_end)
WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) >= $start_date
  AND properties_automated_test = FALSE
  AND IFNULL(i.test_account, '') <> 'test';


CREATE OR REPLACE TEMPORARY TABLE _pdp_impressions AS
SELECT ri.impression_mpid,
       impression_date::DATE                AS _impression_date,
       impression_country,
       list_id,
       COUNT(*)                             AS impressions,
       count_if(membership_status <> 'vip') AS lead_impressions,
       count_if(membership_status = 'vip')  AS vip_impressions
FROM _raw_pdp_impressions ri
GROUP BY ri.impression_mpid, _impression_date, impression_country, list_id;



CREATE OR REPLACE TEMPORARY TABLE _segment_orders AS
SELECT CAST(IFF(properties_products_product_id::STRING = '', '0',
                properties_products_product_id::STRING) AS BIGINT)                   AS order_mpid,
       CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp))::DATE AS order_date,
       country as order_country,
       properties_products_list_id as psource,
       COUNT(*)                                                                      AS orders,
       count_if(COALESCE(i.membership_status, 'prospect') <> 'vip')                  AS lead_orders,
       count_if(COALESCE(i.membership_status, 'prospect') = 'vip')                   AS vip_orders
FROM lake.segment_sxf.java_sxf_order_completed so
         LEFT JOIN _identify i
                   ON so.userid = i.user_id AND
                      (TO_TIMESTAMP(timestamp) >= i.memb_start AND TO_TIMESTAMP(timestamp) < i.memb_end)
WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) >= $start_date
  AND properties_automated_test = FALSE
  AND IFNULL(i.test_account, '') <> 'test'
GROUP BY order_mpid, order_date, order_country, psource;


CREATE OR REPLACE TEMPORARY TABLE _sxf_spv AS
SELECT COALESCE(i.impression_mpid, o.order_mpid)  AS mpid,
       COALESCE(i._impression_date, o.order_date) AS date,
       UPPER(coalesce(i.impression_country, o.order_country)) as country,
       'sxf_spv'                                  AS grid_name,
       COALESCE(SUM(i.impressions), 0)            AS impressions,
       COALESCE(SUM(i.lead_impressions), 0)       AS lead_impressions,
       COALESCE(SUM(i.vip_impressions), 0)        AS vip_impressions,
       COALESCE(SUM(o.orders), 0)                 AS orders,
       COALESCE(SUM(o.lead_orders), 0)            AS lead_orders,
       COALESCE(SUM(o.vip_orders), 0)             AS vip_orders
FROM _pdp_impressions i
         FULL OUTER JOIN _segment_orders o
                         ON i.impression_mpid = o.order_mpid AND i._impression_date = o.order_date and i.list_id=o.psource
where country='US'
GROUP BY mpid, date, country;

CREATE OR REPLACE TEMPORARY TABLE _sxf_spv_eu AS
SELECT COALESCE(i.impression_mpid, o.order_mpid)  AS mpid,
       COALESCE(i._impression_date, o.order_date) AS date,
       UPPER(coalesce(impression_country, order_country)) as country,
       'sxf_spv_eu'                                  AS grid_name,
       COALESCE(SUM(i.impressions), 0)            AS impressions,
       COALESCE(SUM(i.lead_impressions), 0)       AS lead_impressions,
       COALESCE(SUM(i.vip_impressions), 0)        AS vip_impressions,
       COALESCE(SUM(o.orders), 0)                 AS orders,
       COALESCE(SUM(o.lead_orders), 0)            AS lead_orders,
       COALESCE(SUM(o.vip_orders), 0)             AS vip_orders
FROM _pdp_impressions i
         FULL OUTER JOIN _segment_orders o
                         ON i.impression_mpid = o.order_mpid AND i._impression_date = o.order_date and i.list_id=o.psource
where country!='US'
GROUP BY mpid, date, country;


CREATE OR REPLACE TEMPORARY TABLE _sxf_spv_nosale AS
SELECT COALESCE(i.impression_mpid, o.order_mpid)  AS mpid,
       COALESCE(i._impression_date, o.order_date) AS date,
       UPPER(coalesce(impression_country, order_country)) as country,
       'sxf_spv_nosale'                                  AS grid_name,
       COALESCE(SUM(i.impressions), 0)            AS impressions,
       COALESCE(SUM(i.lead_impressions), 0)       AS lead_impressions,
       COALESCE(SUM(i.vip_impressions), 0)        AS vip_impressions,
       COALESCE(SUM(o.orders), 0)                 AS orders,
       COALESCE(SUM(o.lead_orders), 0)            AS lead_orders,
       COALESCE(SUM(o.vip_orders), 0)             AS vip_orders
FROM _pdp_impressions i
         FULL OUTER JOIN _segment_orders o
                         ON i.impression_mpid = o.order_mpid AND i._impression_date = o.order_date and i.list_id=o.psource
where country='US'
and coalesce(i.list_id, o.psource) not ilike 'sale' and coalesce(i.list_id, o.psource) not ilike 'sale-secondary'
GROUP BY mpid, date, country;

CREATE OR REPLACE TEMPORARY TABLE _sxf_spv_sale AS
SELECT COALESCE(i.impression_mpid, o.order_mpid)  AS mpid,
       COALESCE(i._impression_date, o.order_date) AS date,
       UPPER(coalesce(impression_country, order_country)) as country,
       'sxf_spv_sale'                                  AS grid_name,
       COALESCE(SUM(i.impressions), 0)            AS impressions,
       COALESCE(SUM(i.lead_impressions), 0)       AS lead_impressions,
       COALESCE(SUM(i.vip_impressions), 0)        AS vip_impressions,
       COALESCE(SUM(o.orders), 0)                 AS orders,
       COALESCE(SUM(o.lead_orders), 0)            AS lead_orders,
       COALESCE(SUM(o.vip_orders), 0)             AS vip_orders
FROM _pdp_impressions i
         FULL OUTER JOIN _segment_orders o
                         ON i.impression_mpid = o.order_mpid AND i._impression_date = o.order_date and i.list_id=o.psource
where country='US'
and (lower(coalesce(i.list_id, o.psource)) ilike 'sale' or lower(coalesce(i.list_id, o.psource)) ilike 'sale-secondary')
GROUP BY mpid, date, country;


CREATE OR REPLACE TEMPORARY TABLE _sxf_spv_nosale_eu AS
SELECT COALESCE(i.impression_mpid, o.order_mpid)  AS mpid,
       COALESCE(i._impression_date, o.order_date) AS date,
       UPPER(coalesce(impression_country, order_country)) as country,
       'sxf_spv_nosale_eu'                                  AS grid_name,
       COALESCE(SUM(i.impressions), 0)            AS impressions,
       COALESCE(SUM(i.lead_impressions), 0)       AS lead_impressions,
       COALESCE(SUM(i.vip_impressions), 0)        AS vip_impressions,
       COALESCE(SUM(o.orders), 0)                 AS orders,
       COALESCE(SUM(o.lead_orders), 0)            AS lead_orders,
       COALESCE(SUM(o.vip_orders), 0)             AS vip_orders
FROM _pdp_impressions i
         FULL OUTER JOIN _segment_orders o
                         ON i.impression_mpid = o.order_mpid AND i._impression_date = o.order_date and i.list_id=o.psource
where country!='US'
and coalesce(i.list_id, o.psource) not ilike 'sale' and coalesce(i.list_id, o.psource) not ilike 'sale-secondary'
GROUP BY mpid, date, country;

CREATE OR REPLACE TEMPORARY TABLE _sxf_spv_sale_eu AS
SELECT COALESCE(i.impression_mpid, o.order_mpid)  AS mpid,
       COALESCE(i._impression_date, o.order_date) AS date,
       UPPER(coalesce(impression_country, order_country)) as country,
       'sxf_spv_sale_eu'                                  AS grid_name,
       COALESCE(SUM(i.impressions), 0)            AS impressions,
       COALESCE(SUM(i.lead_impressions), 0)       AS lead_impressions,
       COALESCE(SUM(i.vip_impressions), 0)        AS vip_impressions,
       COALESCE(SUM(o.orders), 0)                 AS orders,
       COALESCE(SUM(o.lead_orders), 0)            AS lead_orders,
       COALESCE(SUM(o.vip_orders), 0)             AS vip_orders
FROM _pdp_impressions i
         FULL OUTER JOIN _segment_orders o
                         ON i.impression_mpid = o.order_mpid AND i._impression_date = o.order_date and i.list_id=o.psource
where country!='US'
and (coalesce(i.list_id, o.psource) ilike 'sale' or coalesce(i.list_id, o.psource) ilike 'sale-secondary')
GROUP BY mpid, date, country;

CREATE OR REPLACE TEMP TABLE _all_spi AS
select * from _sxf_spv
UNION ALL
select * from _sxf_spv_eu
UNION ALL
select * from _sxf_spv_nosale
UNION ALL
select * from _sxf_spv_sale
UNION ALL
select * from _sxf_spv_nosale_eu
UNION ALL
select * from _sxf_spv_sale_eu;


CREATE OR REPLACE TEMPORARY TABLE _merge_with_department AS (
    SELECT $brand_abbr        AS brand_abbr,
           country      AS country_abbr,
           'All'              AS department,
           m.mpid,
           m.date,
           m.grid_name,
           p.image_url,
           p.category,
           m.impressions      AS total_impressions,
           m.lead_impressions AS lead_impressions,
           m.vip_impressions  AS vip_impressions,
           m.orders           AS total_sales,
           m.lead_orders      AS lead_sales,
           m.vip_orders       AS vip_sales
    FROM _all_spi m
             JOIN edw_prod.data_model_sxf.dim_product p ON m.mpid = p.product_id);


MERGE INTO reporting_prod.DATA_SCIENCE.EXPORT_POSTREG_SEGMENT_SPI_RANKING t
    USING _merge_with_department m
    ON t.mpid = m.mpid AND t.impressions_date = m.date AND t.grid_name = m.grid_name
    WHEN NOT MATCHED THEN
        INSERT (brand_abbr, country_abbr, department,
                mpid, impressions_date, grid_name,
                total_impressions, lead_impressions, vip_impressions,
                total_sales, lead_sales, vip_sales, meta_create_datetime, meta_update_datetime, meta_company_id)
            VALUES (brand_abbr, country_abbr, department,
                    mpid, date, grid_name,
                    total_impressions, lead_impressions, vip_impressions,
                    total_sales, lead_sales, vip_sales, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 30)
    WHEN MATCHED AND
        (
                NOT EQUAL_NULL(t.total_impressions, m.total_impressions) OR
                NOT EQUAL_NULL(t.lead_impressions, m.lead_impressions) OR
                NOT EQUAL_NULL(t.vip_impressions, m.vip_impressions) OR
                NOT EQUAL_NULL(t.total_sales, m.total_sales) OR
                NOT EQUAL_NULL(t.lead_sales, m.lead_sales) OR
                NOT EQUAL_NULL(t.vip_sales, m.vip_sales)
            )
        THEN
        UPDATE
            SET t.total_impressions = m.total_impressions,
                t.lead_impressions = m.lead_impressions,
                t.vip_impressions = m.vip_impressions,
                t.total_sales = m.total_sales,
                t.lead_sales = m.lead_sales,
                t.vip_sales = m.vip_sales,
                t.meta_update_datetime = CURRENT_TIMESTAMP();
