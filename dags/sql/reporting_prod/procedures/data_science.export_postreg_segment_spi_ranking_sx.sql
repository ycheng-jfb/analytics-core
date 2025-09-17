SET data_update_days = 3;
SET start_date = DATEADD(DAY, -$data_update_days, TO_DATE(CURRENT_TIMESTAMP()));
SET store_group = 'Savage X';
SET brand_abbr = 'SX';
SET country_abbr = 'US';

CREATE OR REPLACE TEMPORARY TABLE _identify
AS (SELECT userid                                                                               AS user_id,
           TO_TIMESTAMP(timestamp)                                                              AS memb_start,
           IFF(LOWER(traits_membership_status) LIKE 'payg', 'paygo',
               LOWER(traits_membership_status))                                                    membership_status,
           IFF(traits_email LIKE '%@test.com' OR traits_email LIKE '%@example.com', 'test', '') AS test_account,
           COALESCE(LEAD(TO_TIMESTAMP(timestamp)) OVER (PARTITION BY userid
               ORDER BY TO_TIMESTAMP(timestamp)), '9999-12-31')                                 AS memb_end
    FROM lake.segment_sxf.javascript_sxf_identify
    WHERE segment_shard = 'javascript_sxf_us_production'
);

CREATE OR REPLACE TEMPORARY TABLE _ses_user AS (
    SELECT properties_session_id                     AS session_id,
           MAX(COALESCE(userid, properties_user_id)) AS user_id
    FROM lake.segment_sxf.javascript_sxf_page
    WHERE COALESCE(userid, properties_user_id) IS NOT NULL
      AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp))::DATE >= $start_date
      AND segment_shard = 'javascript_sxf_us_production'
    GROUP BY session_id
);

----Querying PDP impressions by session & product
CREATE OR REPLACE TEMPORARY TABLE _raw_pdp_impressions AS
SELECT DISTINCT CAST(IFF(properties_product_id::STRING = '', '0',
                         properties_product_id::STRING) AS BIGINT)                      AS impression_mpid,
                CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) AS impression_date,
                so.properties_session_id                                                AS session_id,
                COALESCE(so.userid, u.user_id)                                          AS customer_id
FROM lake.segment_sxf.javascript_sxf_product_viewed so
         LEFT JOIN _ses_user AS u ON so.properties_session_id = u.session_id
         LEFT JOIN _identify i ON u.user_id = i.user_id
WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) >= $start_date
  AND properties_automated_test = FALSE
  AND IFNULL(i.test_account, '') <> 'test'
  AND segment_shard = 'javascript_sxf_us_production';


--Rolling up the previous query by mpid & Date
CREATE OR REPLACE TEMPORARY TABLE _pdp_impressions AS
SELECT ri.impression_mpid,
       impression_date::DATE AS _impression_date,
       COUNT(*)              AS impressions
FROM _raw_pdp_impressions ri
GROUP BY ri.impression_mpid, _impression_date;

------ Querying Segment Orders. Product_id is stored in a json string so have to parse

CREATE OR REPLACE TEMPORARY TABLE _segment_orders AS
SELECT CAST(IFF(properties_products_product_id::STRING = '', '0',
                properties_products_product_id::STRING) AS BIGINT)                   AS order_mpid,
       country,
       CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp))::DATE AS order_date,
       COUNT(*)                                                                      AS orders
        ,
       SUM(properties_products_quantity)                                             AS quantity
FROM lake.segment_sxf.java_sxf_order_completed so
         LEFT JOIN _identify i ON so.userid = i.user_id AND TO_TIMESTAMP(timestamp) >= i.memb_start AND
                                  TO_TIMESTAMP(timestamp) < i.memb_end
WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(timestamp)) >= $start_date
  AND properties_automated_test = FALSE
  AND IFNULL(i.test_account, '') <> 'test'
  AND segment_shard = 'java_sxf_us_production'
GROUP BY 1, 2, 3;

--Combining Sales and Impression data
CREATE OR REPLACE TEMPORARY TABLE _all_spi AS
SELECT COALESCE(i.impression_mpid, o.order_mpid)  AS mpid,
       COALESCE(i._impression_date, o.order_date) AS date,
       'Categories_PDP'                           AS grid_name,
       COALESCE(SUM(i.impressions), 0)            AS impressions,
       COALESCE(SUM(o.orders), 0)                 AS orders
FROM _pdp_impressions i
         FULL OUTER JOIN _segment_orders o
                         ON i.impression_mpid = o.order_mpid AND i._impression_date = o.order_date
GROUP BY mpid, date;

--Structuring data in the way it is needed for the table that gets piped into total grid
CREATE OR REPLACE TEMPORARY TABLE _merge_with_department AS (
    SELECT $brand_abbr   AS brand_abbr,
           $country_abbr AS country_abbr,
           'All'         AS department,
           m.mpid,
           m.date,
           m.grid_name,
           p.image_url,                       -- Only used in Dashboard
           p.product_category,
           m.impressions AS total_impressions,
           0             AS lead_impressions, --Will need to add in the future
           0             AS vip_impressions,  --Will need to add in the future
           m.orders      AS total_sales,
           0             AS lead_sales,       --Will need to add in the future
           0             AS vip_sales         --Will need to add in the future
    FROM _all_spi m
             JOIN EDW_PROD.DATA_MODEL_SXF.dim_product p ON m.mpid = p.product_id);


--Merging data into main table once data has been QA'ed. Shouldn't need to modify anything here
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
