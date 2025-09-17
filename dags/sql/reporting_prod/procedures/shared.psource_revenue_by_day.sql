CREATE OR REPLACE TEMP TABLE _PSOURCE_REVENUE_BY_DAY_stg AS
SELECT
    brand AS store_brand_name
     ,country
     ,region
     ,STORE_TYPE_SALES_CHANNEL AS sales_channel
     ,MEMBERSHIP_ORDER_TYPE_L3 AS membership_order_type
     ,session_start_membership_state
     ,PLATFORM
     ,os
     ,browser
     ,order_local_date
     ,ship_local_date
     ,ORDER_STATUS
     ,CASE WHEN order_status in ('Success','Pending') THEN TRUE ELSE FALSE END AS is_successful_pending_order
     ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
           WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
           WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
                AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
                coalesce(psource_pieced,psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
                OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
                AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
           ELSE 'None' END AS psource
     ,promo_code
     ,customer_gender
     ,product_department
     ,sum(units) AS units
     ,sum(outfit_component_units) AS outfit_component_units
     ,sum(product_gross_revenue_excl_shipping) AS product_gross_revenue_excl_shipping
     ,SUM(product_cost) AS product_cost
     ,current_timestamp AS datetime_added
     ,current_timestamp AS datetime_modified
--   ,refresh_datetime_hq AS refresh_datetime
FROM REPORTING_BASE_PROD.SHARED.order_line_psource_payment
WHERE
    order_local_date >= dateadd(Day,-30,current_date)
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
    AND ORDER_CLASSIFICATION_L1 = 'Product Order'
GROUP BY brand,
         country,
         region,
         STORE_TYPE_SALES_CHANNEL,
         MEMBERSHIP_ORDER_TYPE_L3 ,
         session_start_membership_state,
         PLATFORM,
         os,
         browser,
         order_local_date,
         ship_local_date,
         ORDER_STATUS,
         CASE WHEN order_status in ('Success','Pending') THEN TRUE ELSE FALSE END,
         CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
           WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
           WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
                AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
                coalesce(psource_pieced,psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
                OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
                AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
           ELSE 'None' END,
        promo_code,
        customer_gender,
        product_department;

DELETE FROM REPORTING_PROD.SHARED.PSOURCE_REVENUE_BY_DAY WHERE order_local_date >= dateadd(Day,-30,current_date);

INSERT INTO REPORTING_PROD.SHARED.PSOURCE_REVENUE_BY_DAY (
        STORE_BRAND_NAME
        ,COUNTRY
        ,REGION
        ,SALES_CHANNEL
        ,MEMBERSHIP_ORDER_TYPE
        ,SESSION_START_MEMBERSHIP_STATE
        ,PLATFORM
        ,OS
        ,BROWSER
        ,ORDER_LOCAL_DATE
        ,SHIP_LOCAL_DATE
        ,ORDER_STATUS
        ,IS_SUCCESSFUL_PENDING_ORDER
        ,PSOURCE
        ,PROMO_CODE
        ,CUSTOMER_GENDER
        ,PRODUCT_DEPARTMENT
        ,UNITS
        ,OUTFIT_COMPONENT_UNITS
        ,PRODUCT_GROSS_REVENUE_EXCL_SHIPPING
        ,PRODUCT_COST
        ,DATETIME_ADDED
        ,DATETIME_MODIFIED )
SELECT STORE_BRAND_NAME
        ,COUNTRY
        ,REGION
        ,SALES_CHANNEL
        ,MEMBERSHIP_ORDER_TYPE
        ,SESSION_START_MEMBERSHIP_STATE
        ,PLATFORM
        ,OS
        ,BROWSER
        ,ORDER_LOCAL_DATE
        ,SHIP_LOCAL_DATE
        ,ORDER_STATUS
        ,IS_SUCCESSFUL_PENDING_ORDER
        ,PSOURCE
        ,PROMO_CODE
        ,CUSTOMER_GENDER
        ,PRODUCT_DEPARTMENT
        ,UNITS
        ,OUTFIT_COMPONENT_UNITS
        ,PRODUCT_GROSS_REVENUE_EXCL_SHIPPING
        ,PRODUCT_COST
        ,DATETIME_ADDED
        ,DATETIME_MODIFIED  FROM _PSOURCE_REVENUE_BY_DAY_stg;
