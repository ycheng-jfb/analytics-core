SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _scaffold AS
SELECT DISTINCT CASE WHEN country = 'UK' THEN 'EU-UK' ELSE region END region
     ,CASE WHEN country = 'UK' THEN 'EU-UK' ELSE region END AS warehouse_id
     ,CASE WHEN country = 'UK' THEN 'EU-UK' ELSE region END AS store_id
FROM EDW_PROD.REFERENCE.STORE_WAREHOUSE sw
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds ON sw.store_id = ds.store_id
WHERE ds.store_brand = 'Savage X'
    AND ds.store_country NOT IN ('CA','DL','NL','SE')
    AND ds.store_full_name NOT ILIKE '%(DM)%'
    AND ds.store_type = 'Online'
UNION
SELECT 'NA Retail' AS region
    ,TO_VARCHAR(warehouse_id)
    ,TO_VARCHAR(sw.store_id)
FROM EDW_PROD.REFERENCE.STORE_WAREHOUSE sw
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds ON sw.store_id = ds.store_id
WHERE ds.store_brand = 'Savage X'
    AND ds.store_country NOT IN ('CA','DL','NL','SE')
    AND ds.store_full_name NOT ILIKE '%(DM)%'
    AND ds.store_type = 'Retail'
    AND store_full_name NOT IN ('RTLSXF-Las Vegas','RTLSXF-Mall of America','RTLSXF-SOHO','RTLSXF-Valley Fair')
UNION
SELECT store_full_name AS region
    ,TO_VARCHAR(warehouse_id)
    ,TO_VARCHAR(sw.store_id)
FROM EDW_PROD.REFERENCE.STORE_WAREHOUSE sw
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds ON sw.store_id = ds.store_id
WHERE ds.store_brand = 'Savage X'
    AND ds.store_country NOT IN ('CA','DL','NL','SE')
    AND ds.store_full_name NOT ILIKE '%(DM)%'
    AND ds.store_type = 'Retail'
    AND store_full_name NOT IN ('RTLSXF-Las Vegas','RTLSXF-Mall of America','RTLSXF-SOHO','RTLSXF-Valley Fair');

CREATE OR REPLACE TEMPORARY TABLE _hq_data AS
SELECT is_retail,
    region,
    warehouse_id,
    DATE(date) date,
    sku,
    product_sku,
    qty_available_to_sell
FROM REPORTING_PROD.SXF.VIEW_BASE_INVENTORY_DATASET;

CREATE OR REPLACE TEMPORARY TABLE _local_data AS
SELECT w.is_retail,
    w.region,
    inv.warehouse_id,
    local_date date,
    inv.sku,
    UPPER(TRIM(substring(inv.sku,1,charindex('-',inv.sku,charindex('-',inv.sku)+1)-1))) AS product_sku,
    available_to_sell_quantity
FROM EDW_PROD.DATA_MODEL_SXF.FACT_INVENTORY_HISTORY inv
JOIN (SELECT DISTINCT sw.warehouse_id,
        CASE WHEN s.store_country = 'UK' THEN 'EU-UK' ELSE sw.region END region,
        IFF(w.is_retail = 1, 'Y','N') is_retail
     FROM EDW_PROD.DATA_MODEL_SXF.DIM_WAREHOUSE w
JOIN EDW_PROD.REFERENCE.STORE_WAREHOUSE sw ON sw.warehouse_id = w.warehouse_id
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE s ON s.store_id = sw.store_id
WHERE s.store_brand = 'Savage X' AND s.is_core_store = TRUE AND store_country NOT IN ('CA','DL','NL','SE')) w ON inv.warehouse_id = w.warehouse_id
JOIN REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE sm on (sm.sku=inv.sku) AND DATE(local_date) >= '2019-01-01';


CREATE OR REPLACE TEMPORARY TABLE _inv_all_sku_dates AS
SELECT is_retail,
    region,
    warehouse_id,
    DATE(date) date,
    sku,
    product_sku
FROM _hq_data
UNION
SELECT is_retail,
    region,
    warehouse_id,
    date(date) date,
    sku,
    product_sku
FROM _local_data;

CREATE OR REPLACE TEMPORARY TABLE _hq_qty_data AS
SELECT a.* , qty_available_to_sell
FROM _inv_all_sku_dates a
LEFT JOIN _hq_data inv ON a.warehouse_id = inv.warehouse_id
    AND a.date = DATE(inv.date) AND a.sku = inv.sku;

CREATE OR REPLACE TEMPORARY TABLE _local_hq_qty_data AS
SELECT a.is_retail,
    a.region,
    a.warehouse_id,
    a.date,
    a.sku,
    a.product_sku,
    COALESCE(a.qty_available_to_sell,inv.available_to_sell_quantity)  qty_available_to_sell
FROM _hq_qty_data a
LEFT JOIN _local_data inv ON a.warehouse_id = inv.warehouse_id
    AND a.date = DATE(inv.date) AND a.sku = inv.sku AND a.qty_available_to_sell IS NULL;

CREATE OR REPLACE TEMPORARY TABLE _inventory_size_backbone AS
(
    SELECT i.region
        ,s.warehouse_id
        ,sm.sku
        ,sm.color_sku_po
        ,sm.size
    FROM _scaffold s
    JOIN _local_hq_qty_data i ON
             TO_VARCHAR(CASE WHEN i.is_retail = 'N' THEN i.region ELSE TO_VARCHAR(i.warehouse_id) END) =  TO_VARCHAR(s.warehouse_id)
    LEFT JOIN REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE sm ON sm.sku = i.sku
    WHERE i.qty_available_to_sell>0 --only include sizes that ever had any inventory
  GROUP BY i.region
        ,s.warehouse_id
        ,sm.sku
        ,sm.color_sku_po
        ,sm.size
);

--get each inventory date for each product_sku
CREATE OR REPLACE TEMPORARY TABLE _inventory_date_backbone AS
(
    SELECT i.region
        ,s.warehouse_id
        ,i.date
        ,sm.color_sku_po
        ,sm.category
     FROM _scaffold s
     JOIN _local_hq_qty_data i ON
        TO_VARCHAR(CASE WHEN i.is_retail = 'N' THEN i.region ELSE TO_VARCHAR(i.warehouse_id) END) =  TO_VARCHAR(s.warehouse_id)
    LEFT JOIN REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE sm ON sm.sku = i.sku
  GROUP BY i.region
        ,s.warehouse_id
        ,i.date
        ,sm.color_sku_po
        ,sm.category
);

--get overall backbone of date and inventory
CREATE OR REPLACE TEMPORARY TABLE _inventory_backbone AS
(
    SELECT
        db.region
        ,db.warehouse_id
        ,db.date
        ,db.color_sku_po
        ,db.category
        ,sb.sku
        ,sb.size
    FROM _inventory_date_backbone db
    LEFT JOIN _inventory_size_backbone sb on db.region = sb.region and sb.color_sku_po=db.color_sku_po AND db.warehouse_id = sb.warehouse_id
    );

--daily inventory status for each color size sku
CREATE OR REPLACE TEMPORARY TABLE _inventory_data AS
(
     SELECT TO_DATE(isb.date) date
        ,sf.region as store_group
        ,isb.region
        ,isb.sku
        ,isb.color_sku_po
        ,isb.size
        ,isb.category
        ,SUM(COALESCE(i.qty_available_to_sell,0)) AS eop_inventory
        , CASE
            WHEN SUM(COALESCE(i.qty_available_to_sell,0)) <= 10 and sf.region='NA' THEN 'Out of Stock'
            WHEN SUM(COALESCE(i.qty_available_to_sell,0)) <= 2 and (sf.region='EU' or sf.region='EU-UK') THEN 'Out of Stock' --new thresholds for eu biz
            WHEN sf.region NOT IN ('EU','EU-UK','NA') AND UPPER(isb.category) = 'BRA' AND SUM(COALESCE(i.qty_available_to_sell,0)) = 0 THEN 'Out of Stock'
            WHEN sf.region NOT IN ('EU','EU-UK','NA') AND UPPER(isb.category) NOT IN ('BRA') AND SUM(COALESCE(i.qty_available_to_sell,0)) <= 2 THEN 'Out of Stock'
            ELSE 'In Stock' END AS stock_status
        ,CASE
            WHEN SUM(COALESCE(i.qty_available_to_sell,0)) <= 10 and sf.region='NA' THEN 0
            WHEN SUM(COALESCE(i.qty_available_to_sell,0)) <= 2 and (sf.region='EU' or sf.region='EU-UK') THEN 0
            WHEN sf.region NOT IN ('EU','EU-UK','NA') AND UPPER(isb.category) = 'BRA' AND SUM(COALESCE(i.qty_available_to_sell,0)) = 0 THEN 0
            WHEN sf.region NOT IN ('EU','EU-UK','NA') AND UPPER(isb.category) NOT IN ('BRA') AND SUM(COALESCE(i.qty_available_to_sell,0)) <= 2 THEN 0
            ELSE 1 END AS stock_status_numerical
    FROM _scaffold sf
    JOIN _inventory_backbone isb ON isb.warehouse_id = sf.warehouse_id
    LEFT JOIN _local_hq_qty_data i ON i.region = isb.region AND i.date = isb.date AND i.sku = isb.sku
                AND TO_VARCHAR(CASE WHEN i.is_retail = 'N' THEN i.region ELSE TO_VARCHAR(i.warehouse_id) END) = TO_VARCHAR(isb.warehouse_id)
    LEFT JOIN REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE sm ON (sm.sku=isb.sku)
    GROUP BY TO_DATE(isb.date)
        ,sf.region
        ,isb.region
        ,isb.sku
        ,isb.color_sku_po
        ,isb.size
        ,isb.category
);

CREATE OR REPLACE TEMPORARY TABLE _all_sizes_ats AS
(
    SELECT
        date
        ,region
        ,store_group
        ,sku
        ,color_sku_po
        ,size
        ,min(stock_status_numerical) over (partition by date,region,store_group,color_sku_po) as all_sizes_ats
    FROM _inventory_data
  WHERE sku IS NOT NULL
);

CREATE OR REPLACE TEMPORARY TABLE _define_periods AS
(
    SELECT
        a.date
        ,a.region
        ,a.store_group
        ,a.sku
        ,a.color_sku_po
        ,a.size
        ,a.all_sizes_ats
        ,ROUND(COALESCE(a1.all_sizes_ats,0)) all_sizes_ats_daybefore
        ,ROUND(COALESCE(a2.all_sizes_ats,0)) all_sizes_ats_dayafter
        ,CASE
            WHEN a.all_sizes_ats=1 and all_sizes_ats_daybefore=0 THEN 'start_period'
            WHEN a.all_sizes_ats=0 and all_sizes_ats_daybefore=1 THEN 'end_period'
            ELSE 'inbetween'
        END where_in_period
    FROM _all_sizes_ats as a
    LEFT JOIN _all_sizes_ats a1 ON DATEADD('day',1,a1.date) = a.date AND a1.region = a.region AND a1.sku = a.sku AND a1.color_sku_po = a.color_sku_po AND a1.store_group = a.store_group
    LEFT JOIN _all_sizes_ats a2 ON DATEADD('day',-1,a2.date) = a.date AND a2.region = a.region AND a2.sku = a.sku AND a2.color_sku_po = a.color_sku_po AND a2.store_group = a.store_group
    ORDER BY a.date,a.region,a.color_sku_po,a.size
);
--get all prebreak start dates
CREATE OR REPLACE TEMPORARY TABLE _period_starts AS
(
    SELECT
        DISTINCT date
        ,region
        ,store_group
        ,color_sku_po
        ,RANK() over (PARTITION BY store_group,color_sku_po ORDER BY date,region,color_sku_po,store_group ASC) date_order
    FROM
        (SELECT
            DISTINCT date
            ,region
            ,store_group
            ,color_sku_po
        FROM _define_periods
        WHERE where_in_period='start_period')
);
--get all prebreak end dates
CREATE OR REPLACE TEMPORARY TABLE _period_ends AS
(
    SELECT
        DISTINCT date
        ,region
        ,store_group
        ,color_sku_po
        ,RANK() over (PARTITION BY  store_group,color_sku_po ORDER BY date,region,color_sku_po,store_group ASC) date_order
    FROM
        (SELECT
            DISTINCT date
            ,region
            ,store_group
            ,color_sku_po
        FROM _define_periods
        WHERE where_in_period='end_period')
);
--pair start and end times
CREATE OR REPLACE TEMPORARY TABLE _attempted_prebreak_period_test AS
(
    SELECT
        s.date prebreak_start_date
        ,s.region
        ,s.store_group
        ,s.color_sku_po
        ,e.date prebreak_end_date
    FROM _period_starts s
    LEFT JOIN _period_ends e ON (s.region = e.region AND s.color_sku_po = e.color_sku_po AND s.date_order=e.date_order AND s.store_group = e.store_group)
);
CREATE OR REPLACE TEMPORARY TABLE _seeding_orders AS
(
    SELECT
        DISTINCT o.order_id
    FROM EDW_PROD.DATA_MODEL_SXF.FACT_ORDER o
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_CUSTOMER AS c ON (c.customer_id = o.customer_id)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE st  ON (st.store_id = o.store_id)
    LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.ORDER_DISCOUNT od ON (od.order_id = o.order_id)
    LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.PROMO p ON (p.promo_id = od.promo_id)
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.DIM_PROMO_HISTORY dp ON (dp.promo_id = p.promo_id)
    WHERE
        st.store_brand = 'Savage X'
        AND convert_timezone('America/Los_Angeles', o.order_local_datetime) >= '2018-05-01'
        AND ((dp.promo_code ilike '%influencer%' OR dp.promo_code ilike '%seeding_%')
        OR  (c.email ilike '%influencer%@savagex.com%' OR c.email ilike '%ambassador%savage%'))
);

CREATE OR REPLACE TEMPORARY TABLE _first_sales_test AS
(
    SELECT s.region AS store_group
        ,o.product_sku color_sku_po
        ,TO_DATE(MIN(o.order_local_datetime)) first_sku_sales_date
    FROM _scaffold s
    JOIN REPORTING_PROD.SXF.ORDER_LINE_DATASET o ON
        TO_VARCHAR(CASE WHEN o.order_store_type <> 'Retail' THEN o.store_region_abbr ELSE TO_VARCHAR(o.order_store_id) END) = TO_VARCHAR(s.store_id)
    LEFT JOIN _seeding_orders so ON so.order_id=o.order_id
    WHERE
        so.order_id IS NULL --remove product seeing orders
        AND order_line_status <>'Cancelled'
        AND (order_status = 'Success' OR order_status = 'Pending')
    GROUP BY s.region
            ,o.product_sku
);

CREATE OR REPLACE TEMPORARY TABLE _prebreak_period as
(
    SELECT
        fp.region
        ,fp.store_group
        ,fp.color_sku_po
        ,fs.first_sku_sales_date
        ,CASE WHEN fp.prebreak_start_date <= fs.first_sku_sales_date THEN fs.first_sku_sales_date ELSE fp.prebreak_start_date END true_prebreak_start_date
        ,CASE WHEN fp.prebreak_end_date IS null THEN CURRENT_DATE() ELSE fp.prebreak_end_date END true_prebreak_end_date
        ,CASE WHEN fp.prebreak_end_date IS null THEN 'Has Not Broken' ELSE 'Has Broken' END has_style_broken
        ,DATEDIFF('day',true_prebreak_start_date,true_prebreak_end_date) days_before_break
    FROM _attempted_prebreak_period_test fp
    LEFT JOIN _first_sales_test fs ON (fs.color_sku_po=fp.color_sku_po AND fs.store_group = fp.store_group) --this should remove product order seeding and influencer orders
    WHERE
        fs.first_sku_sales_date IS NOT NULL -- don't want to pull data for items that haven't sold anything yet
);

CREATE OR REPLACE TEMPORARY TABLE _sizes_broke_style AS
(
    SELECT i.color_sku_po
        ,i.store_group
        ,i.date
        ,i.size sizes_that_broke_style
    FROM _inventory_data i
    JOIN _period_ends p ON i.color_sku_po = p.color_sku_po AND i.date = p.date AND i.region = p.region AND i.store_group = p.store_group
    WHERE stock_status_numerical = 0 AND i.sku IS NOT NULL
    GROUP BY i.color_sku_po
        ,i.store_group
        ,i.date
        ,i.size
);

CREATE OR REPLACE TEMPORARY TABLE _sizes_that_broke_style_with_start_date AS
(
    SELECT
        fp.region
        ,fp.store_group
        ,fp.color_sku_po
        ,fs.first_sku_sales_date
        ,CASE WHEN fp.prebreak_start_date<=fs.first_sku_sales_date THEN fs.first_sku_sales_date ELSE fp.prebreak_start_date END true_prebreak_start_date
        ,CASE WHEN fp.prebreak_end_date IS null THEN CURRENT_DATE() ELSE fp.prebreak_end_date END true_prebreak_end_date
        ,CASE WHEN fp.prebreak_end_date IS null THEN 'Has Not Broken' ELSE 'Has Broken' END has_style_broken
        ,sizes_that_broke_style
        ,DATEDIFF('day',true_prebreak_start_date,true_prebreak_end_date) days_before_break
    FROM _attempted_prebreak_period_test fp
    LEFT JOIN _first_sales_test fs ON (fs.color_sku_po=fp.color_sku_po AND fs.store_group = fp.store_group) --this should remove product order seeding and influencer orders
    LEFT JOIN _sizes_broke_style sbs ON sbs.date = fp.prebreak_end_date AND sbs.color_sku_po=fp.color_sku_po AND sbs.store_group = fp.store_group
    WHERE
        fs.first_sku_sales_date IS NOT NULL -- don't want to pull data for items that haven't sold anything yet
);

CREATE OR REPLACE TEMPORARY TABLE _sizes_that_broke_style_final AS
(
    SELECT TO_DATE(DATE_TRUNC('month',true_prebreak_start_date)) month
        ,CASE WHEN store_group = 'NA' THEN 'NA Ecomm'
                WHEN store_group = 'EU-UK' THEN 'EU (UK only)'
                ELSE store_group END AS store_group
        ,color_sku_po
        ,has_style_broken
        ,listagg(DISTINCT sizes_that_broke_style, ', ') within GROUP (ORDER BY sizes_that_broke_style) AS sizes_that_broke_style
        ,MIN(true_prebreak_start_date) min_style_allsizes_avail_date
        ,MAX(true_prebreak_end_date) max_style_break_date
    FROM _sizes_that_broke_style_with_start_date
    GROUP BY month
        ,CASE WHEN store_group = 'NA' THEN 'NA Ecomm'
                WHEN store_group = 'EU-UK' THEN 'EU (UK only)'
                ELSE store_group END
        ,color_sku_po
        ,has_style_broken
);

CREATE OR REPLACE TEMPORARY TABLE _sales AS
(
    SELECT
        order_hq_date AS day
       ,o.store_region_abbr AS region
       ,s.region AS store_group
       ,dp.product_name
       ,dp.sku
       ,dp.product_sku
       ,dp.color
       ,sms.size
       ,dp.current_showroom_date
       ,o.is_marked_down
       ,TO_DATE(MIN(ORDER_HQ_DATE) OVER (PARTITION BY s.region, dp.sku)) AS sku_first_purchase_date
       ,TO_DATE(MIN(ORDER_HQ_DATE) OVER (PARTITION BY s.region, dp.product_sku)) AS style_first_purchase_date
       ,COUNT(DISTINCT o.order_id) as order_count
       ,COUNT(o.item_quantity) AS unit_count
    FROM _scaffold s
    JOIN REPORTING_PROD.sxf.ORDER_LINE_DATASET o ON
        TO_VARCHAR(CASE WHEN o.order_store_type <> 'Retail' THEN o.store_region_abbr ELSE TO_VARCHAR(o.order_store_id) END) = TO_VARCHAR(s.store_id)
    JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ORDER_LINE l ON (o.order_line_id = l.order_line_id)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_CUSTOMER c ON (c.customer_id = o.customer_id)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_PRODUCT dp ON (dp.product_id = l.product_id)
    LEFT JOIN REPORTING_PROD.SXF.STYLE_MASTER sm ON (sm.color_sku_po = dp.product_sku)
    LEFT JOIN REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE sms ON (sms.sku=dp.sku)
    LEFT JOIN _seeding_orders so on (so.order_id = o.order_id)
    WHERE order_line_status <>'Cancelled'
        AND (order_status = 'Success' OR order_status = 'Pending' )
        AND ORDER_HQ_DATETIME >= '2018-05-01'
        AND (c.email NOT ILIKE '%@ambassador.com%') -- Remove influencer orders
        AND dp.current_showroom_date < CURRENT_DATE()--current_date; hard coded because core/fashion mapping is manual --replaced dp.showroom_date
        AND dp.department != 'Apparel'
        AND so.order_id IS NULL -- Remove product seeding orders
    GROUP BY order_hq_date
       ,o.store_region_abbr
       ,s.region
       ,dp.product_name
       ,dp.sku
       ,dp.product_sku
       ,dp.color
       ,sms.size
       ,dp.current_showroom_date
       ,is_marked_down
);

--gets sales of each sku within prebreak period
CREATE OR REPLACE TEMPORARY TABLE _base_data AS
(
    SELECT DISTINCT
        TO_DATE(DATE_TRUNC('month',pp.true_prebreak_start_date)) month
       ,TO_DATE(s.day) day
       ,pp.region
       ,s.store_group
       ,s.sku
       ,s.product_sku
       ,sm.style_number_po
       ,sm.size_scale_production
       ,s.size
       ,sm.core_fashion
       ,sm.new_core_fashion
       ,sm.size_range
       ,sm.style_name
       ,sm.site_name
       ,sm.po_color
       ,sm.site_color
       ,sm.color_family
       ,sm.color_roll_up
       ,sm.category
       ,sm.subcategory
       ,sm.department
       ,sm.sub_department
       ,sm.collection
       ,sm.gender
       ,sm.image_url
       ,sm.first_showroom
       ,sm.latest_showroom
       ,sm.savage_showroom
       ,pp.true_prebreak_start_date style_allsizes_avail_date
       ,pp.true_prebreak_end_date style_break_date
       ,pp.has_style_broken
       ,pp.days_before_break
       ,s.style_first_purchase_date
       ,s.sku_first_purchase_date
       ,s.order_count
       ,s.unit_count
       ,sm.is_prepack
       ,s.is_marked_down
    FROM _sales s
    LEFT JOIN REPORTING_PROD.sxf.STYLE_MASTER sm ON (sm.color_sku_po = s.product_sku)
    JOIN _prebreak_period pp ON (pp.color_sku_po=sm.color_sku_po AND pp.region= s.region  AND pp.store_group = s.store_group
                                AND pp.true_prebreak_start_date<=TO_DATE(s.day) AND pp.true_prebreak_end_date>=TO_DATE(s.day))
);

--there could be more than one prebreak period within a given month so let's aggregate to the month
CREATE OR REPLACE TEMPORARY TABLE _final_nonaggregated_data as
(
    SELECT
        month
         ,region
         ,store_group
         ,sku
         ,product_sku
         ,style_number_po
         ,size_scale_production
         ,size
         ,core_fashion
         ,size_range
         ,style_name
         ,site_name
         ,po_color
         ,site_color
         ,color_family
         ,category
         ,subcategory
         ,department
         ,sub_department
         ,collection
         ,gender
         ,image_url
         ,has_style_broken
         ,color_roll_up
         ,new_core_fashion
         ,is_prepack
         ,is_marked_down
         ,MIN(first_showroom) first_showroom
         ,MIN(latest_showroom) latest_showroom
         ,MIN(savage_showroom) savage_showroom
         ,MIN(style_allsizes_avail_date) min_style_allsizes_avail_date
         ,MAX(style_break_date) max_style_break_date
         ,MIN(style_first_purchase_date) style_first_purchase_date
         ,MIN(sku_first_purchase_date) sku_first_purchase_date
         ,SUM(order_count) AS sku_total_orders
         ,SUM(unit_count) AS sku_total_units
    FROM _base_data
    GROUP BY month
         ,region
         ,store_group
         ,sku
         ,product_sku
         ,style_number_po
         ,size_scale_production
         ,size
         ,core_fashion
         ,size_range
         ,style_name
         ,site_name
         ,po_color
         ,site_color
         ,color_family
         ,category
         ,subcategory
         ,department
         ,sub_department
         ,collection
         ,gender
         ,image_url
         ,has_style_broken
         ,color_roll_up
         ,new_core_fashion
         ,is_prepack
         ,is_marked_down
);

--for those color skus with more than one prebreak period within a given month,
-- lets use the earliest date of the first prebreak period as the start date and the latest date of the last prebreak period as the end date
CREATE OR REPLACE TEMPORARY TABLE _final_nonaggregated_data_2 AS
(
    SELECT DISTINCT
        month
        ,region
        ,CASE WHEN store_group = 'NA' THEN 'NA Ecomm'
            WHEN store_group = 'EU-UK' THEN 'EU (UK only)'
            ELSE store_group END AS store_group_final
       ,sku
       ,product_sku
       ,style_number_po
       ,size_scale_production
       ,size
       ,core_fashion
       ,size_range
       ,style_name
       ,site_name
       ,po_color
       ,site_color
       ,color_family
       ,category
       ,subcategory
       ,department
       ,sub_department
       ,collection
       ,gender
       ,image_url
       ,has_style_broken
       ,color_roll_up
       ,new_core_fashion
       ,is_prepack
       ,is_marked_down
       ,first_showroom
       ,latest_showroom
       ,savage_showroom
       ,MIN(min_style_allsizes_avail_date) OVER (PARTITION BY month,region,product_sku,store_group, has_style_broken) min_style_allsizes_avail_date
       ,MAX(max_style_break_date) OVER (PARTITION BY month,region,product_sku,store_group, has_style_broken) max_style_break_date
       ,style_first_purchase_date
       ,sku_first_purchase_date
       ,sku_total_orders
       ,sku_total_units
    FROM _final_nonaggregated_data
);

TRUNCATE TABLE REPORTING_PROD.SXF.PREBREAK_SIZE_SCALES;

INSERT INTO REPORTING_PROD.SXF.PREBREAK_SIZE_SCALES
(
    month
    ,store_group
    ,sku
    ,product_sku
    ,style_number_po
    ,size_scale_production
    ,size
    ,core_fashion
    ,size_range
    ,style_name
    ,site_name
    ,po_color
    ,site_color
    ,color_family
    ,category
    ,subcategory
    ,department
    ,sub_department
    ,collection
    ,gender
    ,image_url
    ,has_style_broken
    ,sizes_that_broke_style
    ,color_roll_up
    ,new_core_fashion
    ,is_prepack
    ,is_marked_down
    ,first_showroom
    ,latest_showroom
    ,savage_showroom
    ,min_style_allsizes_avail_date
    ,max_style_break_date
    ,style_first_purchase_date
    ,sku_first_purchase_date
    ,sku_total_orders
    ,sku_total_units
    ,style_unit_count
    ,meta_create_datetime
    ,meta_update_datetime
)
SELECT f.month
    ,store_group_final store_group
    ,sku
    ,product_sku
    ,style_number_po
    ,size_scale_production
    ,size
    ,core_fashion
    ,size_range
    ,style_name
    ,site_name
    ,po_color
    ,site_color
    ,color_family
    ,category
    ,subcategory
    ,department
    ,sub_department
    ,collection
    ,gender
    ,image_url
    ,f.has_style_broken
    ,sizes_that_broke_style
    ,f.color_roll_up
    ,f.new_core_fashion
    ,f.is_prepack
    ,f.is_marked_down
    ,first_showroom
    ,latest_showroom
    ,savage_showroom
    ,f.min_style_allsizes_avail_date
    ,f.max_style_break_date
    ,style_first_purchase_date
    ,sku_first_purchase_date
    ,sku_total_orders
    ,sku_total_units
    ,SUM(sku_total_units) OVER (PARTITION BY region,f.month,product_sku,f.store_group_final,f.has_style_broken) AS style_unit_count
    ,$execution_start_time AS meta_update_time
    ,$execution_start_time AS meta_create_time
FROM _final_nonaggregated_data_2 f
JOIN _sizes_that_broke_style_final s ON s.store_group = f.store_group_final AND s.color_sku_po = f.product_sku
  AND s.month = f.month AND s.has_style_broken = f.has_style_broken;
