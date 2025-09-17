CREATE OR REPLACE TEMPORARY TABLE _psource AS
SELECT
    STORE_BRAND_NAME
     ,country
     ,region
     ,sales_channel
     ,membership_order_type
     ,ORDER_LOCAL_DATE AS date
     ,psource
     ,customer_gender
     ,product_department
     ,sum(units) AS units
     ,sum(outfit_component_units) AS outfit_component_units
     ,sum(product_gross_revenue_excl_shipping) AS revenue_excl_shipping
     ,SUM(product_cost) AS product_cost
     ,max(datetime_added) AS datetime_added
FROM REPORTING_PROD.SHARED.PSOURCE_REVENUE_BY_DAY
WHERE date >= dateadd(Day,-30,current_date) and order_status in ('Success','Pending')
GROUP BY STORE_BRAND_NAME,
        country,
        region,
        sales_channel,
        membership_order_type,
        ORDER_LOCAL_DATE,
        psource,
        customer_gender,
        product_department;

------------------------------------------------------------------------
-- the following is to create as few combinations as possible, but still ensure that when we calculate a % of Total, we are not missing any data

-- problem example:
-- in dash, was filtering for FLNA, Online Sales Channel, Activating VIP Order
-- was filtering for a specific psource and % of total looked like it was 40%
-- inspected further and realized that because this was a mens only psource, there was no data in the dataset for that psource for womens, so it was not being included in the calculation

CREATE OR REPLACE TEMPORARY TABLE _scaffold1 AS
SELECT DISTINCT
    p1.store_brand_name
    ,p1.region
    ,p1.country
    ,p1.sales_channel
    ,p1.date
    ,p1.psource
FROM _psource p1
    JOIN
    (SELECT DISTINCT
        store_brand_name
        ,region
        ,country
        ,sales_channel
        ,date
        ,psource
FROM _psource) p2 ON p1.store_brand_name = p2.store_brand_name
    AND p1.region = p2.region
    AND p1.country = p2.country
    AND p1.sales_channel = p2.sales_channel
    AND p1.date = p2.date
    AND p1.psource = p2.psource;

CREATE OR REPLACE temporary table _membership_order_type AS
SELECT DISTINCT membership_order_type
FROM _psource;

CREATE OR REPLACE temporary table _product_department AS
SELECT DISTINCT product_department
FROM _psource;

CREATE OR REPLACE temporary table _customer_gender AS
SELECT DISTINCT customer_gender
FROM _psource;

CREATE OR REPLACE temporary table _scaffold_output AS
SELECT s.*,membership_order_type,product_department,customer_gender
 FROM _scaffold1 s
    JOIN _membership_order_type ot
    JOIN _product_department p
    JOIN _customer_gender as g;

CREATE OR REPLACE TEMP TABLE _PSOURCE_SCAFFOLD_REVENUE_BY_DAY_stg AS
SELECT
    so.store_brand_name
    ,so.country
    ,so.region
    ,so.sales_channel
    ,so.membership_order_type
    ,so.date
    ,so.customer_gender
    ,so.product_department
//  ,(SELECT MAX(datetime_added) FROM _psource) AS refresh_datetime
    ,max(ps.datetime_added) AS refresh_datetime
    ,CASE
          WHEN so.store_brand_name = 'JustFab' THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE
                (so.PSOURCE, '\:plussize|pluss',''), '\:widewidth',''), 'look\:product.+','look\:product'),
                'look\:.+','look\:product')
          ELSE so.PSOURCE END AS PSOURCE
     ,SUM(IFNULL(UNITS,0)) AS units
     ,SUM(IFNULL(REVENUE_EXCL_SHIPPING,0)) AS revenue
     ,SUM(IFNULL(outfit_component_units,0)) AS outfit_component_units
     ,current_timestamp as datetime_added
     ,current_timestamp as datetime_modified
FROM _scaffold_output so
    LEFT JOIN _psource ps on ps.store_brand_name = so.store_brand_name
    AND ps.region = so.region
    AND ps.country = so.country
    AND ps.sales_channel = so.sales_channel
    AND ps.membership_order_type = so.membership_order_type
    AND ps.date = so.date
    AND ps.customer_gender = so.customer_gender
    AND ps.product_department = so.product_department
    AND ps.psource = so.psource
group by
    so.store_brand_name,
    so.country,
    so.region,
    so.sales_channel,
    so.membership_order_type,
    so.date,
    so.customer_gender,
    so.product_department,
    CASE
          WHEN so.store_brand_name = 'JustFab' THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE
                (so.PSOURCE, '\:plussize|pluss',''), '\:widewidth',''), 'look\:product.+','look\:product'),
                'look\:.+','look\:product')
          ELSE so.PSOURCE END;

DELETE FROM REPORTING_PROD.SHARED.PSOURCE_SCAFFOLD_REVENUE_BY_DAY WHERE date>= dateadd(Day,-30,current_date);

INSERT INTO REPORTING_PROD.SHARED.PSOURCE_SCAFFOLD_REVENUE_BY_DAY (
        STORE_BRAND_NAME
        ,COUNTRY
        ,REGION
        ,SALES_CHANNEL
        ,MEMBERSHIP_ORDER_TYPE
        ,DATE
        ,CUSTOMER_GENDER
        ,PRODUCT_DEPARTMENT
        ,REFRESH_DATETIME
        ,PSOURCE
        ,UNITS
        ,REVENUE
        ,OUTFIT_COMPONENT_UNITS
        ,DATETIME_ADDED
        ,DATETIME_MODIFIED)
SELECT STORE_BRAND_NAME
        ,COUNTRY
        ,REGION
        ,SALES_CHANNEL
        ,MEMBERSHIP_ORDER_TYPE
        ,DATE
        ,CUSTOMER_GENDER
        ,PRODUCT_DEPARTMENT
        ,REFRESH_DATETIME
        ,PSOURCE
        ,UNITS
        ,REVENUE
        ,OUTFIT_COMPONENT_UNITS
        ,DATETIME_ADDED
        ,DATETIME_MODIFIED FROM _PSOURCE_SCAFFOLD_REVENUE_BY_DAY_stg;
