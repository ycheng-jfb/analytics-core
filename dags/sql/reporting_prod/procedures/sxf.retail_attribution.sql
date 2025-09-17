SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _weekly_date_view AS
SELECT DISTINCT full_date                              AS date,
                NULL                                   AS dayofyear,
                NULL                                   AS monthofyear,
                NULL                                   AS year,
                WEEKOFYEAR(DATEADD(DAY, 1, full_date)) AS weekofyear, --- Alter to Start week 1 day before Monday (Sun-Sat)
                YEAROFWEEK(DATEADD(DAY, 1, full_date)) AS yearofweek, --- Alter to Start week 1 day before Monday (Sun-Sat)
                'Weekly'                               AS date_view
FROM edw_prod.data_model_sxf.dim_date
WHERE YEAR(full_date) >= 2018
  AND full_date <=
      CASE
          WHEN DAYNAME(CURRENT_DATE) = 'Sun' THEN DATEADD(DAY, 6, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Mon' THEN DATEADD(DAY, 5, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Tue' THEN DATEADD(DAY, 4, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Wed' THEN DATEADD(DAY, 3, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Thu' THEN DATEADD(DAY, 2, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Fri' THEN DATEADD(DAY, 1, CURRENT_DATE)
          WHEN DAYNAME(CURRENT_DATE) = 'Sat' THEN CURRENT_DATE
          END;

CREATE OR REPLACE TEMPORARY TABLE _weeklyrange AS
    (select weekofyear, yearofweek, min(date) as start_date, max(date) as end_date
     from _weekly_date_view
     where weekofyear is not null
     group by weekofyear, yearofweek
     order by yearofweek, weekofyear);

CREATE or REPLACE TEMPORARY TABLE _vip_activation_detail AS
    (SELECT DISTINCT a.customer_id,
                     a.activation_local_datetime      as activation_datetime_start,
                     a.next_activation_local_datetime as activation_datetime_end,
                     a.activation_key,
                     st.store_type,
                     st.store_sub_type                as retail_store_type,
                     st.store_full_name               as activation_store_name,
                     st.store_retail_state            as activation_store_state,
                     st.store_retail_city             as activation_store_city,
                     st.store_retail_zip_code         as activation_store_zip_code,
                     st.store_id                      as activation_store_id,
                     zcs.latitude                     as activation_store_latitude,
                     zcs.longitude                    as activation_store_longitude
     FROM EDW_PROD.DATA_MODEL_SXF.fact_activation a
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st on st.store_id = a.sub_store_id
              LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.zip_city_state zcs on zcs.zip = st.store_retail_zip_code
     WHERE st.store_brand_abbr = 'SX');

CREATE or REPLACE TEMPORARY TABLE _administrator AS
    (SELECT fo.order_id,
            otd.object_id as administrator_id
     FROM EDW_PROD.DATA_MODEL_SXF.fact_order fo
              JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT."ORDER" o ON fo.order_id = o.order_id
              JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.order_tracking_detail otd ON o.order_tracking_id = otd.order_tracking_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds on ds.store_id = fo.store_id
     WHERE otd.object = 'administrator'
       AND ds.store_brand_abbr = 'SX');

CREATE or REPLACE TEMPORARY TABLE _membership_status AS
    (SELECT DISTINCT fme.customer_id,
                     o.order_id,
                     fme.event_start_local_datetime                                                           as event_start_datetime,
                     fme.event_end_local_datetime                                                             as event_end_datetime,
                     -- CASE WHEN fme_p.membership_event_key IS NOT NULL THEN 1 ELSE 0 END as paygo_activation,
                     CASE
                         WHEN fme.membership_event_type ILIKE 'Activation' THEN 'VIP'
                         WHEN fme.membership_event_type ILIKE 'Cancellation' THEN 'Prior VIP'
                         WHEN fme.membership_event_type ILIKE 'Guest Purchasing Member' THEN 'PAYG'
                         WHEN fme.membership_event_type ILIKE ANY ('Failed Activation', 'Email Signup', 'Registration')
                             THEN 'Lead'
                         ELSE 'New'
                         END                                                                                  as previous_membership_status,
                     CASE
                         WHEN fme_p.membership_event_key IS NOT NULL AND previous_membership_status IN ('Lead', 'New')
                             THEN 1
                         ELSE 0 END                                                                           as paygo_activation,
                     ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY fme.event_start_local_datetime DESC) as rn
     FROM EDW_PROD.DATA_MODEL_SXF.fact_order o
              LEFT JOIN EDW_PROD.DATA_MODEL_SXF.fact_membership_event fme ON fme.customer_id = o.customer_id
         AND fme.event_start_local_datetime < o.order_local_datetime
         AND fme.event_end_local_datetime >= o.order_local_datetime
         AND fme.membership_event_type NOT ILIKE 'Failed Activation%'
              LEFT JOIN EDW_PROD.DATA_MODEL_SXF.fact_membership_event fme_p ON fme_p.customer_id = o.customer_id
         AND
                                                                               fme_p.event_start_local_datetime BETWEEN dateadd(second, -10, o.order_local_datetime) AND dateadd(second, 3600, o.order_local_datetime)
         AND fme_p.membership_state ilike 'guest'
         AND fme_p.membership_event_type NOT ILIKE 'Failed Activation%'
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds on ds.store_id = o.store_id
     WHERE ds.store_brand_abbr = 'SX');

CREATE or REPLACE TEMPORARY TABLE _store_dimensions as
    (SELECT store_number,
            store_name,
            store_id,
            warehouse_id,
            store_region,
            LONGITUDE,
            LATITUDE,
            SQ_FOOTAGE,
            store_zip,
            store_city,
            store_state,
            store_type,
            STORE_SUB_TYPE,
            max(store_open_date) as store_open_date
     FROM (SELECT DISTINCT trim(rl.retail_location_code)                    as store_number
                         , trim(rl.label)                                   as store_name
                         , trim(rl.store_id)                                as store_id
                         , trim(rl.warehouse_id)                            as warehouse_id
                         , trim(COALESCE(rl.open_date, rl.grand_open_date)) as store_open_date
                         , ds.STORE_REGION                                  as store_region
                         , zcs.LONGITUDE
                         , zcs.LATITUDE
                         , asa.SQ_FOOTAGE
                         , ds.STORE_RETAIL_ZIP_CODE                         as store_zip
                         , ds.STORE_RETAIL_CITY                             as store_city
                         , ds.STORE_RETAIL_STATE                            as store_state
                         , ds.STORE_TYPE
                         , ds.STORE_SUB_TYPE
           FROM lake.ultra_warehouse.retail_location rl
                    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds on ds.store_id = rl.store_id
                    left join LAKE_SXF_VIEW.ULTRA_MERCHANT.zip_city_state zcs ON zcs.zip = ds.STORE_RETAIL_ZIP_CODE
                    left join REPORTING_PROD.SXF.ADDITIONAL_STORE_ATTRIBUTES asa on asa.STORE_ID = ds.STORE_ID
           WHERE ds.Store_Brand = 'Savage X'
             AND ds.store_type = 'Retail')
     GROUP BY store_number,
              store_name,
              store_id,
              warehouse_id,
              store_region,
              LONGITUDE,
              LATITUDE,
              SQ_FOOTAGE,
              store_zip,
              store_city,
              store_state,
              STORE_TYPE,
              STORE_SUB_TYPE);

CREATE or REPLACE TEMPORARY TABLE _retail_orders AS
    (SELECT DISTINCT o.order_id,
                     o.store_id                                      AS order_store_id,
                     st.store_type                                   AS order_store_type,
                     o.customer_id,
                     a.administrator_id,
                     vad.activation_datetime_start,
                     vad.activation_store_id                         AS activation_store_id,
                     vad.store_type                                  AS customer_activation_channel,
                     om.order_channel                                AS shopped_channel,
                     ms.previous_membership_status,
                     o.order_local_datetime                          AS order_datetime_added,
                     oms.membership_order_type_l3                    AS membership_order_type,
                     osc.is_retail_ship_only_order,
                     SUM(ifnull(product_subtotal_local_amount, 0) *
                         ifnull(o.reporting_usd_conversion_rate, 1)) AS subtotal,
                     SUM(ifnull(product_discount_local_amount, 0) *
                         ifnull(o.reporting_usd_conversion_rate, 1)) AS discount,
                     SUM(ifnull(shipping_revenue_local_amount, 0) *
                         ifnull(o.reporting_usd_conversion_rate, 1)) AS shipping,
                     SUM(ifnull(tariff_revenue_local_amount, 0) *
                         ifnull(o.reporting_usd_conversion_rate, 1)) AS tariff
     FROM EDW_PROD.DATA_MODEL_SXF.fact_order o
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = o.store_id
              JOIN _store_dimensions rst ON rst.store_id = st.store_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status dos ON dos.order_status_key = o.order_status_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms
                   ON oms.order_membership_classification_key = o.order_membership_classification_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc
                   ON osc.order_sales_channel_key = o.order_sales_channel_key
              LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
         AND o.order_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
              LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) >=
             date_trunc('minute', om.order_local_datetime_start)
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) <
             date_trunc('minute', om.order_local_datetime_end)
              LEFT JOIN _administrator a ON a.order_id = o.order_id
              LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
     WHERE CASE WHEN date(o.order_local_datetime) < rst.store_open_date THEN 0 ELSE 1 END = 1
       AND o.order_local_datetime >= '2018-01-01' --retail inception
       AND st.store_brand_abbr = 'SX'
       AND st.store_type like 'Retail'
       AND dos.order_status = 'Success'
     GROUP BY o.order_id,
              o.store_id,
              st.store_type,
              o.customer_id,
              a.administrator_id,
              vad.activation_datetime_start,
              vad.activation_store_id,
              vad.store_type,
              om.order_channel,
              ms.previous_membership_status,
              o.order_local_datetime,
              oms.membership_order_type_l3,
              osc.is_retail_ship_only_order);

CREATE or REPLACE TEMPORARY TABLE _retail_returns as
    (SELECT rr.store_id                                  AS order_store_id,
            COALESCE(ro.order_store_type, st.store_type) AS order_store_type,
            dc.customer_id,
            o.order_id,
            a.administrator_id,
            vad.activation_store_id,
            vad.store_type                               AS customer_activation_channel,
            vad.activation_datetime_start,
            om.order_channel                             AS shopped_channel,
            oms.membership_order_type_l3                 AS membership_order_type,
            osc.is_retail_ship_only_order,
            r.datetime_added                             AS return_date,
            ms.previous_membership_status,
            CASE
                WHEN ro.order_id IS NOT NULL THEN 'Retail Purchase Retail Return'
                ELSE 'Online Purchase Retail Return' END AS purchase_location,
            SUM(rl.quantity)                             AS quantity
     FROM LAKE_SXF_VIEW.ULTRA_MERCHANT.retail_return_detail rrd
              JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.retail_return rr ON rrd.retail_return_id = rr.retail_return_id
              JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.return r ON r.return_id = rrd.return_id
              JOIN EDW_PROD.DATA_MODEL_SXF.fact_order o ON o.order_id = r.order_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms
                   ON oms.order_membership_classification_key = o.order_membership_classification_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc
                   ON osc.order_sales_channel_key = o.order_sales_channel_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status dos ON dos.order_status_key = o.order_status_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer dc ON dc.customer_id = o.customer_id
              JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.return_line rl ON rl.return_id = r.return_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = rr.store_id
              LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
              LEFT JOIN _retail_orders ro ON ro.order_id = o.order_id
              LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
         AND o.order_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
              LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) >=
             date_trunc('minute', om.order_local_datetime_start)
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) <
             date_trunc('minute', om.order_local_datetime_end)
              LEFT JOIN _administrator a ON a.order_id = o.order_id
              LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
     WHERE st.store_brand_abbr = 'SX'
       AND dos.order_status = 'Success'
       AND CASE WHEN date(o.order_local_datetime) < rst.store_open_date THEN 0 ELSE 1 END = 1
     GROUP BY rr.store_id,
              COALESCE(ro.order_store_type, st.store_type),
              dc.customer_id,
              o.order_id,
              a.administrator_id,
              vad.activation_store_id,
              vad.store_type,
              vad.activation_datetime_start,
              om.order_channel,
              oms.membership_order_type_l3,
              osc.is_retail_ship_only_order,
              r.datetime_added,
              ms.previous_membership_status,
              purchase_location

     UNION
     SELECT o.order_store_id,
            o.order_store_type,
            o.customer_id,
            o.order_id,
            o.administrator_id,
            vad.activation_store_id,
            vad.store_type                  AS customer_activation_channel,
            vad.activation_datetime_start,
            om.order_channel                AS shopped_channel,
            o.membership_order_type,
            o.is_retail_ship_only_order,
            to_date(r.datetime_added)       AS return_date,
            ms.previous_membership_status,
            'Retail Purchase Online Return' AS purchase_location,
            SUM(rl.quantity)                AS quantity
     FROM _retail_orders o
              JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.return r ON r.order_id = o.order_id
              JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.return_line rl ON rl.return_id = r.return_id
              LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.retail_return_detail rrd ON rrd.return_id = r.return_id
              LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
         AND o.order_datetime_added between vad.activation_datetime_start AND vad.activation_datetime_end
              LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
         AND date_trunc('minute', to_timestamp_ntz(o.order_datetime_added)) >=
             date_trunc('minute', om.order_local_datetime_start)
         AND date_trunc('minute', to_timestamp_ntz(o.order_datetime_added)) <
             date_trunc('minute', om.order_local_datetime_end)
              LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
     WHERE rrd.retail_return_detail_id IS NULL
     GROUP BY o.order_store_id,
              o.order_store_type,
              o.customer_id,
              o.order_id,
              o.administrator_id,
              vad.activation_store_id,
              vad.store_type,
              vad.activation_datetime_start,
              om.order_channel,
              o.membership_order_type,
              o.is_retail_ship_only_order,
              to_date(r.datetime_added),
              ms.previous_membership_status,
              purchase_location);

CREATE or REPLACE TEMPORARY TABLE _retail_exchanges AS
    (SELECT o.order_store_id,
            o.order_store_type,
            o.order_id,
            o.customer_id,
            o.administrator_id,
            TO_DATE(o.order_datetime_added)                      AS date,
            r.activation_datetime_start,
            r.activation_store_id,
            r.customer_activation_channel,
            o.shopped_channel,
            o.membership_order_type,
            o.is_retail_ship_only_order,
            o.previous_membership_status,
            COUNT(1)                                             as total_exchanges,
            SUM(CASE
                    WHEN r.purchase_location = 'Retail Purchase Online Return' THEN 1
                    ELSE 0 END)                                  AS exchanges_after_online_return,
            SUM(CASE
                    WHEN r.purchase_location IN ('Retail Purchase Retail Return', 'Online Purchase Retail Return')
                        THEN 1
                    ELSE 0 END)                                  AS exchanges_after_in_store_return,
            SUM(CASE
                    WHEN r.purchase_location = 'Online Purchase Retail Return' THEN 1
                    ELSE 0 END)                                  AS exchanges_after_in_store_return_from_online_purchase,
            SUM(o.subtotal - o.discount + o.shipping + o.tariff) AS total_exchanges_value,
            SUM(CASE
                    WHEN r.purchase_location = 'Retail Purchase Online Return'
                        THEN o.subtotal - o.discount + o.shipping + o.tariff
                    ELSE 0 END)                                  AS exchanges_value_after_online_return,
            SUM(CASE
                    WHEN r.purchase_location IN ('Retail Purchase Retail Return', 'Online Purchase Retail Return')
                        THEN o.subtotal - o.discount + o.shipping + o.tariff
                    ELSE 0 END)                                  AS exchanges_value_after_in_store_return,
            SUM(CASE
                    WHEN r.purchase_location = 'Online Purchase Retail Return'
                        THEN o.subtotal - o.discount + o.shipping + o.tariff
                    ELSE 0 END)                                  AS exchanges_value_after_in_store_return_from_online_purchase
     FROM _retail_returns r
              JOIN _retail_orders o ON o.customer_id = r.customer_id
         AND DATEDIFF(DAY, r.return_date, o.order_datetime_added) = 0
     GROUP BY o.order_store_id,
              o.order_store_type,
              o.order_id,
              o.customer_id,
              o.administrator_id,
              TO_DATE(o.order_datetime_added),
              r.activation_datetime_start,
              r.activation_store_id,
              r.customer_activation_channel,
              o.shopped_channel,
              o.membership_order_type,
              o.is_retail_ship_only_order,
              o.previous_membership_status);

CREATE or REPLACE TEMPORARY TABLE _financial_refunds AS
    (SELECT to_date(r.refund_completion_local_datetime) AS refund_date,
            to_date(vad.activation_datetime_start)      AS activation_cohort,
            activation_store_id,
            vad.store_type                              AS customer_activation_channel,
            a.administrator_id,
            om.order_channel                            AS shopped_channel,
            r.store_id                                  AS refund_store_id,
            st.store_full_name                          AS refund_store_name,
            st.store_type                               AS refund_store_type,
            st.store_sub_type                           AS refund_retail_store_type,
            oms.membership_order_type_l3                AS membership_order_type,
            osc.is_retail_ship_only_order,
            ms.previous_membership_status,
            SUM(CASE
                    WHEN osc.order_classification_l1 = 'Product Order' THEN ifnull(r.cash_refund_local_amount, 0) *
                                                                            ifnull(o.reporting_usd_conversion_rate, 1)
                    ELSE 0 END)                         AS product_order_cash_refund_amount,
            SUM(CASE
                    WHEN osc.order_sales_channel_l1 = 'Billing Order' THEN ifnull(r.cash_refund_local_amount, 0) *
                                                                           ifnull(o.reporting_usd_conversion_rate, 1)
                    ELSE 0 END)                         AS billing_cash_refund_amount,
            SUM(CASE
                    WHEN osc.order_classification_l1 = 'Product Order' THEN
                        ifnull(r.cash_store_credit_refund_local_amount, 0) *
                        ifnull(o.reporting_usd_conversion_rate, 1)
                    ELSE 0 END)                         AS product_order_cash_credit_refund_amount
     FROM EDW_PROD.DATA_MODEL_SXF.fact_refund r
              JOIN EDW_PROD.DATA_MODEL_SXF.fact_order o ON r.order_id = o.order_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc
                   ON osc.order_sales_channel_key = o.order_sales_channel_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms
                   ON oms.order_membership_classification_key = o.order_membership_classification_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = r.customer_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = r.store_id
              LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
         AND r.refund_completion_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
              LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
         AND date_trunc('minute', to_timestamp_ntz(r.refund_completion_local_datetime)) >=
             date_trunc('minute', om.order_local_datetime_start)
         AND date_trunc('minute', to_timestamp_ntz(r.refund_completion_local_datetime)) <
             date_trunc('minute', om.order_local_datetime_end)
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_refund_payment_method dfpm
                   ON dfpm.refund_payment_method_key = r.refund_payment_method_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_refund_status rs ON rs.refund_status_key = r.refund_status_key
              LEFT JOIN _administrator a ON a.order_id = o.order_id
              LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
              LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
     WHERE st.store_brand_abbr = 'SX'
       AND c.is_test_customer = 0
       AND r.is_chargeback = 0
       AND LOWER(rs.refund_status) = 'refunded'
       AND CASE WHEN date(o.order_local_datetime) < rst.store_open_date THEN 0 ELSE 1 END = 1
     GROUP BY to_date(r.refund_completion_local_datetime),
              to_date(vad.activation_datetime_start),
              activation_store_id,
              vad.store_type,
              a.administrator_id,
              om.order_channel,
              r.store_id,
              st.store_full_name,
              st.store_type,
              st.store_sub_type,
              oms.membership_order_type_l3,
              osc.is_retail_ship_only_order,
              ms.previous_membership_status);

CREATE or REPLACE TEMPORARY TABLE _financial_chargebacks AS
    (SELECT to_date(r.chargeback_datetime)         AS chargeback_date,
            to_date(vad.activation_datetime_start) AS activation_cohort,
            activation_store_id,
            vad.store_type                         AS customer_activation_channel,
            a.administrator_id,
            om.order_channel                       AS shopped_channel,
            r.store_id                             AS chargeback_store_id,
            st.store_type                          AS chargeback_store_type,
            oms.membership_order_type_l3           AS membership_order_type,
            osc.is_retail_ship_only_order,
            ms.previous_membership_status,
            SUM(ifnull(IFF(osc.order_classification_l1 = 'Product Order',
                           ifnull(r.chargeback_local_amount, 0) * ifnull(o.reporting_usd_conversion_rate, 1), 0),
                       0))                         AS product_order_cash_chargeback_amount,
            SUM(ifnull(IFF(osc.order_sales_channel_l1 = 'Billing Order',
                           ifnull(r.chargeback_local_amount, 0) * ifnull(o.reporting_usd_conversion_rate, 1), 0),
                       0))                         AS billing_cash_chargeback_amount
     FROM EDW_PROD.DATA_MODEL_SXF.fact_chargeback r
              JOIN EDW_PROD.DATA_MODEL_SXF.fact_order o ON r.order_id = o.order_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc
                   ON osc.order_sales_channel_key = o.order_sales_channel_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms
                   ON oms.order_membership_classification_key = o.order_membership_classification_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = r.customer_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = r.store_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_chargeback_payment dcp
                   ON dcp.chargeback_payment_key = r.chargeback_payment_key
              LEFT JOIN _vip_activation_detail vad ON vad.customer_id = r.customer_id
         AND r.chargeback_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
              LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
         AND date_trunc('minute', to_timestamp_ntz(r.chargeback_datetime)) >=
             date_trunc('minute', om.order_local_datetime_start)
         AND date_trunc('minute', to_timestamp_ntz(r.chargeback_datetime)) <
             date_trunc('minute', om.order_local_datetime_end)
              LEFT JOIN _administrator a ON a.order_id = o.order_id
              LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
              LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
     WHERE st.store_brand_abbr = 'SX'
       AND c.is_test_customer = 0
       AND CASE WHEN date(o.order_local_datetime) < rst.store_open_date THEN 0 ELSE 1 END = 1
     GROUP BY to_date(r.chargeback_datetime),
              to_date(vad.activation_datetime_start),
              activation_store_id,
              vad.store_type,
              a.administrator_id,
              om.order_channel,
              r.store_id,
              st.store_type,
              oms.membership_order_type_l3,
              osc.is_retail_ship_only_order,
              ms.previous_membership_status);

CREATE or REPLACE TEMPORARY TABLE _financial_exchanges AS
    (SELECT to_date(o.order_local_datetime)                 AS exchange_date,
            vad.activation_store_id,
            vad.store_type                                  AS customer_activation_channel,
            a.administrator_id,
            to_date(vad.activation_datetime_start)          AS activation_cohort,
            o.store_id                                      AS exchange_store_id,
            st.store_type                                   AS exchange_store_type,
            om.order_channel                                AS shopped_channel,
            oms.membership_order_type_l3                    AS membership_order_type,
            osc.is_retail_ship_only_order,
            ms.previous_membership_status,
            COUNT(DISTINCT o.order_id)                      AS exchange_orders,
            SUM(unit_count)                                 AS exchange_units,
            SUM(ifnull(coalesce(opc.oracle_cost_local_amount, opc.reporting_landed_cost_local_amount), 0) *
                ifnull(o.reporting_usd_conversion_rate, 1)) AS exchange_product_cost,
            SUM(ifnull(o.shipping_cost_local_amount, 0) *
                ifnull(o.reporting_usd_conversion_rate, 1)) AS exchange_shipping_cost,
            SUM(ifnull(o.estimated_shipping_supplies_cost_local_amount, 0) *
                ifnull(o.reporting_usd_conversion_rate, 1)) AS exchange_shipping_supplies_cost
     FROM EDW_PROD.DATA_MODEL_SXF.fact_order o
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc
                   ON osc.order_sales_channel_key = o.order_sales_channel_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms
                   ON oms.order_membership_classification_key = o.order_membership_classification_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = o.store_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = o.customer_id
              LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
         AND o.order_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
              LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) >=
             date_trunc('minute', om.order_local_datetime_start)
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) <
             date_trunc('minute', om.order_local_datetime_end)
              LEFT JOIN EDW_PROD.DATA_MODEL_SXF.fact_order_product_cost opc on opc.order_id = o.order_id
              LEFT JOIN _administrator a ON a.order_id = o.order_id
              LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
              LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
     WHERE osc.order_classification_l2 ILIKE 'Exchange'
       AND order_status IN ('Success', 'Pending')
       AND st.store_brand_abbr = 'SX'
       AND c.is_test_customer = 0
       AND CASE WHEN date(o.order_local_datetime) < rst.store_open_date THEN 0 ELSE 1 END = 1
     GROUP BY to_date(o.order_local_datetime),
              vad.activation_store_id,
              vad.store_type,
              a.administrator_id,
              to_date(vad.activation_datetime_start),
              o.store_id,
              st.store_type,
              om.order_channel,
              oms.membership_order_type_l3,
              osc.is_retail_ship_only_order,
              ms.previous_membership_status);

CREATE or REPLACE TEMPORARY TABLE _financial_reships AS
    (SELECT to_date(o.order_local_datetime)                 as reship_date,
            vad.activation_store_id,
            vad.store_type                                  AS customer_activation_channel,
            a.administrator_id,
            om.order_channel                                AS shopped_channel,
            to_date(vad.activation_datetime_start)          as activation_cohort,
            o.store_id                                      AS reship_store_id,
            st.store_type                                   AS reship_store_type,
            oms.membership_order_type_l3                    AS membership_order_type,
            osc.is_retail_ship_only_order,
            ms.previous_membership_status,
            COUNT(DISTINCT o.order_id)                      AS reship_orders,
            SUM(unit_count)                                 as reship_units,
            SUM(ifnull(COALESCE(opc.oracle_cost_local_amount, opc.reporting_landed_cost_local_amount), 0) *
                ifnull(o.reporting_usd_conversion_rate, 1)) AS reship_product_cost,
            SUM(ifnull(o.shipping_cost_local_amount, 0) *
                ifnull(o.reporting_usd_conversion_rate, 1)) AS reship_shipping_cost,
            SUM(ifnull(o.estimated_shipping_supplies_cost_local_amount, 0) *
                ifnull(o.reporting_usd_conversion_rate, 1)) AS reship_shipping_supplies_cost
     FROM EDW_PROD.DATA_MODEL_SXF.fact_order o
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc
                   ON osc.order_sales_channel_key = o.order_sales_channel_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms
                   ON oms.order_membership_classification_key = o.order_membership_classification_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = o.customer_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = o.store_id
              LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
         AND o.order_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
              LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) >=
             date_trunc('minute', om.order_local_datetime_start)
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) <
             date_trunc('minute', om.order_local_datetime_end)
              LEFT JOIN EDW_PROD.DATA_MODEL_SXF.fact_order_product_cost opc ON opc.order_id = o.order_id
              LEFT JOIN _administrator a ON a.order_id = o.order_id
              LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
              LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
     WHERE osc.order_classification_l2 ILIKE 'Reship'
       AND order_status IN ('Success', 'Pending')
       AND st.store_brand_abbr = 'SX'
       AND c.is_test_customer = 0
       AND CASE WHEN date(o.order_local_datetime) < rst.store_open_date THEN 0 ELSE 1 END = 1
     GROUP BY to_date(o.order_local_datetime),
              vad.activation_store_id,
              vad.store_type,
              a.administrator_id,
              om.order_channel,
              to_date(vad.activation_datetime_start),
              o.store_id,
              st.store_type,
              oms.membership_order_type_l3,
              osc.is_retail_ship_only_order,
              ms.previous_membership_status);

CREATE or REPLACE TEMPORARY TABLE _financial_return_product_cost AS
    (SELECT to_date(return_completion_local_datetime)       as return_date,
            vad.activation_store_id,
            vad.store_type                                  AS customer_activation_channel,
            to_date(vad.activation_datetime_start)          AS activation_cohort,
            om.order_channel                                AS shopped_channel,
            rl.store_id                                     AS return_store_id,
            st.store_type                                   AS return_store_type,
            rl.administrator_id,
            osc.is_retail_ship_only_order,
            oms.membership_order_type_l3                    AS membership_order_type,
            ms.previous_membership_status,
            SUM(ifnull(estimated_returned_product_local_cost, 0) *
                ifnull(o.reporting_usd_conversion_rate, 1)) AS return_product_cost_local_amount,
            SUM(ifnull(estimated_return_shipping_cost_local_amount, 0) *
                ifnull(o.reporting_usd_conversion_rate, 1)) AS return_shipping_cost_amount,
            SUM(ifnull(estimated_returned_product_cost_local_amount_resaleable, 0) *
                ifnull(o.reporting_usd_conversion_rate, 1)) AS return_product_cost_local_amount_resaleable
     FROM EDW_PROD.DATA_MODEL_SXF.fact_return_line rl
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_return_status rs ON rs.return_status_key = rl.return_status_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = rl.customer_id
              JOIN EDW_PROD.DATA_MODEL_SXF.fact_order_line o ON o.order_line_id = rl.order_line_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc
                   ON osc.order_sales_channel_key = o.order_sales_channel_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms
                   ON oms.order_membership_classification_key = o.order_membership_classification_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = rl.store_id
              LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
         AND o.order_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
              LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) >=
             date_trunc('minute', om.order_local_datetime_start)
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) <
             date_trunc('minute', om.order_local_datetime_end)
              LEFT JOIN _administrator a ON a.order_id = o.order_id
              LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
              LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
     WHERE c.is_test_customer = 0
       AND st.store_brand_abbr = 'SX'
       AND lower(return_status) = 'resolved'
       AND CASE WHEN date(o.order_local_datetime) < rst.store_open_date THEN 0 ELSE 1 END = 1
     GROUP BY to_date(return_completion_local_datetime),
              vad.activation_store_id,
              vad.store_type,
              to_date(vad.activation_datetime_start),
              om.order_channel,
              rl.store_id,
              st.store_type,
              rl.administrator_id,
              osc.is_retail_ship_only_order,
              oms.membership_order_type_l3,
              ms.previous_membership_status);

CREATE or REPLACE TEMPORARY TABLE _gift_cards_issued AS
    (SELECT DISTINCT order_id,
                     sum(ifnull(product_subtotal_local_amount, 0) *
                         ifnull(ol.reporting_usd_conversion_rate, 1)) AS gift_card_amount
     FROM EDW_PROD.DATA_MODEL_SXF.fact_order_line ol
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_product_type pt ON pt.product_type_key = ol.product_type_key
     WHERE product_type_name IN ('Gift Certificate', 'Gift Card')
     GROUP BY order_id);

// metrics needed for planning discount
// coalesce to 0 for null values
CREATE or REPLACE TEMPORARY TABLE _planning_discount_rate_metrics as
    (SELECT ORDER_ID,
            sum(INITIAL_RETAIL_PRICE_EXCL_VAT) AS INITIAL_RETAIL_PRICE_EXCL_VAT,
            sum(SUBTOTAL_AMOUNT_EXCL_TARIFF)   AS SUBTOTAL_AMOUNT_EXCL_TARIFF,
            sum(PRODUCT_DISCOUNT_AMOUNT)       AS PRODUCT_DISCOUNT_AMOUNT
     FROM REPORTING_PROD.SXF.VIEW_ORDER_LINE_RECOGNIZED_DATASET
     GROUP BY ORDER_ID);

CREATE or REPLACE TEMPORARY TABLE _order_metrics AS
    (SELECT
         --dimensions
         to_date(o.order_local_datetime)                                 AS order_date,
         rv.store_type                                                   AS customer_activation_channel,
         om.order_channel                                                AS shopped_channel,
         CASE
             WHEN rv.retail_store_type = 'Store' THEN 'Store'
             ELSE rv.retail_store_type END                               AS retail_activation_store_type,
         rv.activation_store_name,
         a.administrator_id,
         to_date(rv.activation_datetime_start)                           AS activation_cohort,     --should this be activation date?
         rv.activation_store_id,
         st.store_full_name                                              AS order_store_name,
         st.store_id                                                     AS order_store_id,
         st.store_type                                                   AS order_store_type,
         CASE
             WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type
             ELSE 'Online' END                                           AS retail_order_store_type,
         oms.membership_order_type_l3                                    AS membership_order_type,
         osc.is_retail_ship_only_order,
         ms.previous_membership_status,
         ms.paygo_activation,
         max(o.order_local_datetime)                                        latest_order,
         COUNT(DISTINCT CASE
                            WHEN osc.order_classification_l2 IN ('Product Order', 'Credit Billing') THEN o.order_id
                            ELSE null END)                               AS orders,
         COUNT(DISTINCT CASE
                            WHEN ms.previous_membership_status <> 'VIP' THEN o.order_id
                            ELSE null END)                               as conversion_eligible_orders,
         COUNT(DISTINCT gci.order_id)                                    as gift_card_orders,
         SUM(CASE
                 WHEN osc.order_classification_l2 IN ('Product Order', 'Credit Billing') THEN unit_count
                 ELSE 0 END)                                             AS units,
         SUM(CASE
                 WHEN osc.order_classification_l2 IN ('Product Order') THEN
                     ifnull(o.product_gross_revenue_local_amount, 0) *
                     ifnull(o.reporting_usd_conversion_rate, 1) END)     AS product_gross_revenue,
         SUM(CASE
                 WHEN (lower(osc.order_classification_l1) = 'product order' OR
                       lower(osc.order_sales_channel_l1) = 'billing order') THEN
                     ifnull(o.cash_gross_revenue_local_amount, 0) *
                     ifnull(o.reporting_usd_conversion_rate, 1) END)     AS cash_gross_revenue,
         SUM(CASE
                 WHEN osc.order_classification_l2 IN ('Product Order', 'Credit Billing', 'Token Billing') THEN
                     ifnull(o.shipping_revenue_local_amount, 0) * ifnull(o.reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS shipping_revenue,
         SUM(CASE
                 WHEN osc.order_classification_l2 IN ('Product Order', 'Credit Billing', 'Token Billing') THEN
                     ifnull(o.shipping_cost_local_amount, 0) * ifnull(o.reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS shipping_cost,
         SUM(CASE
                 WHEN osc.order_classification_l2 IN ('Product Order', 'Credit Billing', 'Token Billing') THEN
                     ifnull(o.estimated_shipping_supplies_cost_local_amount, 0) *
                     ifnull(o.reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS shipping_supplies_cost,
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.product_discount_local_amount, 0) *
                                                                         ifnull(o.reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS discount,
         SUM(IFF(osc.ORDER_CLASSIFICATION_L2 = 'Product Order', pdrm.INITIAL_RETAIL_PRICE_EXCL_VAT,
                 0))                                                     AS INITIAL_RETAIL_PRICE_EXCL_VAT,
         SUM(IFF(osc.ORDER_CLASSIFICATION_L2 = 'Product Order', pdrm.SUBTOTAL_AMOUNT_EXCL_TARIFF,
                 0))                                                     AS SUBTOTAL_AMOUNT_EXCL_TARIFF,
         SUM(IFF(osc.ORDER_CLASSIFICATION_L2 = 'Product Order', pdrm.PRODUCT_DISCOUNT_AMOUNT,
                 0))                                                     AS PRODUCT_DISCOUNT_AMOUNT,
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.misc_cogs_local_amount, 0) *
                                                                         ifnull(o.reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             as misc_cogs,
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Product Order' THEN
                     ifnull(o.cash_membership_credit_local_amount, 0) * ifnull(o.reporting_usd_conversion_rate, 1)
                 ELSE 0 END) + sum(CASE
                                       WHEN osc.order_classification_l2 = 'Product Order' THEN
                                           ifnull(o.cash_giftco_credit_local_amount, 0) *
                                           ifnull(reporting_usd_conversion_rate, 1)
                                       ELSE 0 END) + sum(CASE
                                                             WHEN osc.order_classification_l2 = 'Product Order' THEN
                                                                 ifnull(o.token_local_amount, 0) *
                                                                 ifnull(reporting_usd_conversion_rate, 1)
                                                             ELSE 0 END) AS credit_redemption_amount,
         COUNT(DISTINCT CASE
                            WHEN osc.order_classification_l2 = 'Product Order' AND
                                 ifnull(o.cash_membership_credit_local_amount, 0) > 0 OR
                                 ifnull(o.cash_giftco_credit_local_amount, 0) OR ifnull(o.token_local_amount, 0) > 0
                                THEN o.order_id END)                     as orders_with_credit_redemption,
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.cash_membership_credit_count, 0) *
                                                                         ifnull(reporting_usd_conversion_rate, 1)
                 ELSE 0 END) + sum(CASE
                                       WHEN osc.order_classification_l2 = 'Product Order' THEN
                                           ifnull(cash_giftco_credit_count, 0) *
                                           ifnull(reporting_usd_conversion_rate, 1)
                                       ELSE 0 END)                       AS credit_redemption_count,
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Gift Certificate' THEN ifnull(gift_card_amount, 0) *
                                                                            ifnull(reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             as gift_card_issuance_amount,
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Credit Billing' THEN
                     ifnull(o.cash_membership_credit_local_amount, 0) * ifnull(reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS credit_issuance_amount,
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Credit Billing' THEN ifnull(o.cash_membership_credit_count, 0) *
                                                                          ifnull(reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS credit_issuance_count, --credit billings can be used for LTV later on
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Product Order' THEN
                     ifnull(o.cash_giftcard_credit_local_amount, 0) * ifnull(reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS gift_card_redemption_amount,
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Product Order' THEN
                     ifnull(o.reporting_landed_cost_local_amount, 0) * ifnull(reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS product_cost,
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(opc.oracle_product_cost, 0) *
                                                                         ifnull(reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS oracle_product_cost,   --why are we bringing in oracle_product_cost
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.product_subtotal_local_amount, 0) *
                                                                         ifnull(o.reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS product_subtotal_amount,
         SUM(CASE
                 WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.non_cash_credit_local_amount, 0) *
                                                                         ifnull(o.reporting_usd_conversion_rate, 1)
                 ELSE 0 END)                                             AS non_cash_credit_amount
     FROM EDW_PROD.DATA_MODEL_SXF.fact_order o
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc
                   ON osc.order_sales_channel_key = o.order_sales_channel_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms
                   ON oms.order_membership_classification_key = o.order_membership_classification_key
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = o.store_id
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = o.customer_id
              LEFT JOIN _vip_activation_detail rv ON rv.customer_id = o.customer_id
         AND o.order_local_datetime BETWEEN rv.activation_datetime_start AND rv.activation_datetime_end
              LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om on om.customer_id = o.customer_id
         AND COALESCE(om.activation_key, -1) = COALESCE(rv.activation_key, -1)
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) >=
             date_trunc('minute', om.order_local_datetime_start)
         AND date_trunc('minute', to_timestamp_ntz(o.order_local_datetime)) <
             date_trunc('minute', om.order_local_datetime_end)
              LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
              LEFT JOIN _gift_cards_issued gci ON ms.order_id = gci.order_id
              LEFT JOIN (SELECT DISTINCT order_id, SUM(ifnull(oracle_cost_local_amount, 0)) as oracle_product_cost
                         FROM EDW_PROD.DATA_MODEL_SXF.fact_order_product_cost
                         GROUP BY 1) opc on opc.order_id = o.order_id --why is Ashley pulling in oracle costs
              LEFT JOIN _administrator a ON a.order_id = o.order_id
              LEFT JOIN _store_dimensions sd ON sd.store_id = st.store_id
              LEFT JOIN _planning_discount_rate_metrics pdrm on o.ORDER_ID = pdrm.ORDER_ID
     WHERE osc.order_classification_l2 IN ('Credit Billing', 'Product Order', 'Gift Certificate', 'Reship', 'Exchange')
       AND order_status IN ('Success', 'Pending')
       AND st.store_brand_abbr = 'SX'
       AND c.is_test_customer = 0
       AND CASE WHEN st.store_type = 'Retail' AND DATE(o.order_local_datetime) < sd.store_open_date THEN 0 ELSE 1 END =
           1
     GROUP BY to_date(o.order_local_datetime),
              rv.store_type,
              om.order_channel,
              retail_activation_store_type,
              rv.activation_store_name,
              a.administrator_id,
              to_date(rv.activation_datetime_start),
              rv.activation_store_id,
              st.store_full_name,
              st.store_id,
              st.store_type,
              retail_order_store_type,
              oms.membership_order_type_l3,
              osc.is_retail_ship_only_order,
              ms.previous_membership_status,
              ms.paygo_activation);

CREATE or REPLACE TEMPORARY TABLE _retail_traffic as
    (SELECT 'daily' as                    date_view
          , rt.date
          , w.weekofyear
          , w.yearofweek
          , rt.store_id
          , rt.store_full_name
          , sd.store_region
          , sd.LONGITUDE
          , sd.LATITUDE
          , sd.store_zip
          , sd.store_city
          , sd.store_state
          , sd.store_number
          , sd.store_open_date
          , sd.sq_footage
          , sd.store_type
          , CASE
                WHEN sd.store_type ILIKE 'Retail' THEN sd.store_sub_type
                WHEN sd.store_type ILIKE 'Online' THEN 'Online'
                WHEN sd.store_type ILIKE 'Mobile App' THEN 'Online'
            END     AS                    store_retail_type
          , wr.start_date
          , wr.end_date
          , sum(incoming_visitor_traffic) incoming_visitor_traffic
          , sum(exiting_visitor_traffic)  exiting_visitor_traffic
     FROM REPORTING_PROD.sxf.retail_traffic rt
              JOIN _store_dimensions sd ON rt.store_id = sd.store_id
              left join _weekly_date_view w on w.date = rt.date
              left join _weeklyrange wr on w.weekofyear = wr.weekofyear and w.yearofweek = wr.yearofweek
     WHERE iff(rt.store_id in (297, 298, 265)
                   and (rt.date between '2023-01-01' and '2023-11-10'), rt.date >= sd.store_open_date,
               during_business_hours_excluding_open_time = true AND rt.date >= sd.store_open_date)
     GROUP BY rt.store_id,
              rt.store_full_name,
              sd.store_region,
              sd.longitude,
              sd.latitude,
              sd.store_zip,
              sd.store_city,
              sd.store_state,
              sd.store_type,
              sd.STORE_SUB_TYPE,
              sd.store_number,
              sd.store_open_date,
              sd.sq_footage,
              wr.start_date,
              wr.end_date,
              rt.date,
              w.weekofyear,
              w.yearofweek);

// aggregated daily retail traffic for both TY and LY
CREATE OR REPLACE TEMPORARY TABLE _daily_retail_traffic as
    (select ty.date_view,
            '4-Wall'                                 as METRIC_TYPE,
            'Financial'                              as metric_method,
            ty.yearofweek,
            ty.weekofyear,
            ty.date,
            ty.start_date                            as ty_start_date,
            ty.end_date                              as ty_end_date,
            ly.start_date                            as ly_start_date,
            ly.end_date                              as ly_end_date,
            ty.store_id,
            ty.store_number,
            ty.store_type,
            ty.store_retail_type,
            ty.store_full_name,
            ty.store_region,
            ty.LONGITUDE,
            ty.LATITUDE,
            ty.store_zip,
            ty.store_city,
            ty.store_state,
            ty.store_open_date,
            ty.sq_footage,
            IFF(datediff(month, ty.store_open_date, ty.date) >=
                13, 'Comp',
                'New')                               as comp_new,
            ty.exiting_visitor_traffic               as exiting_traffic_ty,
            coalesce(ly.exiting_visitor_traffic, 0)  as exiting_traffic_ly,
            ty.incoming_visitor_traffic              as incoming_traffic_ty,
            coalesce(ly.incoming_visitor_traffic, 0) as incoming_traffic_ly
     from _retail_traffic ty
              left join _retail_traffic ly
                        on ty.store_id = ly.store_id and ty.date = dateadd('year', 1, ly.date));

// aggregated weekly retail traffic for TY
CREATE OR REPLACE TEMPORARY TABLE _weekly_retail_traffic_ty as
    (select 'weekly'                     as date_view,
            yearofweek,
            weekofyear,
            start_date,
            end_date,
            store_id,
            store_full_name,
            sum(exiting_visitor_traffic) as exiting_visitor_traffic
     from _retail_traffic
     group by date_view, yearofweek, weekofyear, start_date,
              end_date,
              store_id, store_full_name);

// aggregated weekly retail traffic for both TY and LY
CREATE OR REPLACE TEMPORARY TABLE _weekly_retail_traffic as
    (select ty.date_view,
            ty.store_id,
            ty.store_full_name,
            ty.yearofweek,
            ty.weekofyear,
            ty.start_date,
            ty.end_date,
            ty.exiting_visitor_traffic              as exiting_visitor_traffic_ty,
            coalesce(ly.exiting_visitor_traffic, 0) as exiting_visitor_traffic_ly
     from _weekly_retail_traffic_ty ty
              left join _weekly_retail_traffic_ty ly
                        on ty.yearofweek =
                           ly.yearofweek + 1 and
                           ty.weekofyear =
                           ly.weekofyear and
                           ty.store_id = ly.store_id);

CREATE or REPLACE TEMPORARY TABLE _financial_retail_returns AS
    (SELECT order_store_id,
            order_store_type,
            activation_store_id,
            customer_activation_channel,
            administrator_id,
            to_date(activation_datetime_start) AS activation_cohort,
            shopped_channel,
            membership_order_type,
            is_retail_ship_only_order,
            previous_membership_status,
            to_date(return_date)               AS return_date,
            purchase_location,
            COUNT(DISTINCT order_id)              return_orders,
            sum(quantity)                      AS quantity
     FROM _retail_returns rr
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = rr.customer_id
     GROUP BY order_store_id,
              order_store_type,
              activation_store_id,
              customer_activation_channel,
              administrator_id,
              to_date(activation_datetime_start),
              shopped_channel,
              membership_order_type,
              is_retail_ship_only_order,
              previous_membership_status,
              to_date(return_date),
              purchase_location);

CREATE or REPLACE TEMPORARY TABLE _financial_retail_exchanges AS
    (SELECT order_store_id,
            order_store_type,
            activation_store_id,
            customer_activation_channel,
            administrator_id,
            to_date(activation_datetime_start)                              AS activation_cohort,
            shopped_channel,
            membership_order_type,
            is_retail_ship_only_order,
            date,
            previous_membership_status,
            SUM(total_exchanges)                                            AS total_exchanges,
            SUM(exchanges_after_online_return)                              AS exchanges_after_online_return,
            SUM(exchanges_after_in_store_return)                            AS exchanges_after_in_store_return,
            SUM(exchanges_after_in_store_return_from_online_purchase)       AS exchanges_after_in_store_return_from_online_purchase,
            SUM(total_exchanges_value)                                      AS total_exchanges_value,
            SUM(exchanges_value_after_online_return)                        AS exchanges_value_after_online_return,
            SUM(exchanges_value_after_in_store_return)                      AS exchanges_value_after_in_store_return,
            SUM(exchanges_value_after_in_store_return_from_online_purchase) AS exchanges_value_after_in_store_return_from_online_purchase
     FROM _retail_exchanges re
              JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = re.customer_id
     GROUP BY order_store_id,
              order_store_type,
              activation_store_id,
              customer_activation_channel,
              administrator_id,
              to_date(activation_datetime_start),
              shopped_channel,
              membership_order_type,
              is_retail_ship_only_order,
              date,
              previous_membership_status);

CREATE or REPLACE TEMPORARY TABLE _final_temp AS
    (SELECT to_date(COALESCE(o.order_date, r.refund_date, c.chargeback_date, rpc.return_date, e.exchange_date,
                             rs.reship_date, rr.return_date, re.date))         AS order_date,
            COALESCE(o.administrator_id, r.administrator_id, c.administrator_id, rpc.administrator_id,
                     e.administrator_id, rs.administrator_id, rr.administrator_id,
                     re.administrator_id)                                      AS administrator_id,
            COALESCE(o.customer_activation_channel, r.customer_activation_channel, c.customer_activation_channel,
                     rpc.customer_activation_channel, e.customer_activation_channel, rs.customer_activation_channel,
                     rr.customer_activation_channel,
                     re.customer_activation_channel)                           AS customer_activation_channel,
            CASE
                WHEN datediff(month, ifnull(arsd.store_open_date, '2079-01-01'),
                              to_date(coalesce(o.order_date, r.refund_date, c.chargeback_date, rpc.return_date,
                                               e.exchange_date, rs.reship_date, rr.return_date, re.date))) >= 13
                    THEN 'Comp'
                ELSE 'New' END                                                 AS activation_comp_new,
            COALESCE(o.activation_store_id, r.activation_store_id, c.activation_store_id, rpc.activation_store_id,
                     e.activation_store_id, rs.activation_store_id, rr.activation_store_id,
                     re.activation_store_id)                                   AS activation_store_id,
            COALESCE(o.shopped_channel, r.shopped_channel, c.shopped_channel, rpc.shopped_channel, e.shopped_channel,
                     rs.shopped_channel, rr.shopped_channel,
                     re.shopped_channel)                                       AS shopped_channel,
            COALESCE(o.order_store_type, r.refund_store_type, c.chargeback_store_type, rpc.return_store_type,
                     e.exchange_store_type, rs.reship_store_type, rr.order_store_type,
                     re.order_store_type)                                      AS order_store_type,
            COALESCE(o.order_store_id, r.refund_store_id, c.chargeback_store_id, rpc.return_store_id,
                     e.exchange_store_id, rs.reship_store_id, rr.order_store_id,
                     re.order_store_id)                                        AS order_store_id,
            CASE
                WHEN datediff(month, ifnull(orsd.store_open_date, '2079-01-01'),
                              to_date(coalesce(o.order_date, r.refund_date, c.chargeback_date, rpc.return_date,
                                               e.exchange_date, rs.reship_date, rr.return_date, re.date))) >= 13
                    THEN 'Comp'
                ELSE 'New' END                                                 AS order_comp_new,
            COALESCE(o.membership_order_type, r.membership_order_type, c.membership_order_type,
                     rpc.membership_order_type, e.membership_order_type, rs.membership_order_type,
                     rr.membership_order_type,
                     re.membership_order_type)                                 AS membership_order_type,
            COALESCE(o.is_retail_ship_only_order, r.is_retail_ship_only_order, c.is_retail_ship_only_order,
                     rpc.is_retail_ship_only_order, e.is_retail_ship_only_order, rs.is_retail_ship_only_order,
                     rr.is_retail_ship_only_order,
                     re.is_retail_ship_only_order)                             AS is_retail_ship_only_order,
            o.latest_order,
            SUM(COALESCE(orders, 0))                                           AS orders,
            SUM(COALESCE(units, 0))                                            AS units,
            SUM(COALESCE(gift_card_orders, 0))                                 as gift_card_orders,
            SUM(COALESCE(orders_with_credit_redemption, 0))                    as orders_with_credit_redemption,
            SUM(COALESCE(o.product_gross_revenue, 0))                          AS product_gross_revenue,
            SUM(COALESCE(o.cash_gross_revenue, 0))                             AS cash_gross_revenue,
            SUM(COALESCE(o.product_gross_revenue, 0))
                - SUM(ifnull(r.product_order_cash_credit_refund_amount, 0))
                - SUM(ifnull(r.product_order_cash_refund_amount, 0))
                - SUM(ifnull(c.product_order_cash_chargeback_amount, 0))
                                                                               AS financial_product_net_revenue,
            SUM(COALESCE(o.cash_gross_revenue, 0))
                - SUM(ifnull(r.product_order_cash_refund_amount, 0))
                - SUM(ifnull(r.billing_cash_refund_amount, 0))
                - SUM(ifnull(c.product_order_cash_chargeback_amount, 0))
                - SUM(ifnull(c.billing_cash_chargeback_amount, 0))
                                                                               AS financial_cash_net_revenue,
            SUM(COALESCE(o.product_gross_revenue, 0)) --correct
                - SUM(ifnull(r.product_order_cash_credit_refund_amount, 0))
                - SUM(ifnull(r.product_order_cash_refund_amount, 0))
                - SUM(ifnull(c.product_order_cash_chargeback_amount, 0))
                - SUM(ifnull(product_cost, 0))
                - SUM(ifnull(shipping_cost, 0))
                - SUM(ifnull(shipping_supplies_cost, 0))
                - SUM(ifnull(rpc.return_shipping_cost_amount, 0))
                + SUM(ifnull(rpc.return_product_cost_local_amount_resaleable, 0))
                - SUM(ifnull(rs.reship_product_cost, 0))
                - SUM(ifnull(rs.reship_shipping_cost, 0))
                - SUM(ifnull(rs.reship_shipping_supplies_cost, 0))
                - SUM(ifnull(e.exchange_product_cost, 0))
                - SUM(ifnull(e.exchange_shipping_cost, 0))
                - SUM(ifnull(e.exchange_shipping_supplies_cost, 0))
                - SUM(ifnull(misc_cogs, 0))
                                                                               AS financial_product_gross_profit,
            SUM(COALESCE(o.cash_gross_revenue, 0))
                - SUM(ifnull(r.product_order_cash_refund_amount, 0))
                - SUM(ifnull(r.billing_cash_refund_amount, 0))
                - SUM(ifnull(c.product_order_cash_chargeback_amount, 0))
                - SUM(ifnull(c.billing_cash_chargeback_amount, 0))
                - SUM(ifnull(product_cost, 0))
                - SUM(ifnull(shipping_cost, 0))
                - SUM(ifnull(shipping_supplies_cost, 0))
                - SUM(ifnull(rpc.return_shipping_cost_amount, 0))
                + SUM(ifnull(rpc.return_product_cost_local_amount_resaleable, 0))
                - SUM(ifnull(rs.reship_product_cost, 0))
                - SUM(ifnull(rs.reship_shipping_cost, 0))
                - SUM(ifnull(rs.reship_shipping_supplies_cost, 0))
                - SUM(ifnull(e.exchange_product_cost, 0))
                - SUM(ifnull(e.exchange_shipping_cost, 0))
                - SUM(ifnull(e.exchange_shipping_supplies_cost, 0))
                - SUM(ifnull(misc_cogs, 0))
                                                                               AS financial_cash_gross_profit,
            SUM(coalesce(shipping_revenue, 0))                                 AS shipping_revenue,
            SUM(ifnull(misc_cogs, 0))                                          AS misc_cogs,
            SUM(shipping_cost)                                                 AS shipping_cost,
            SUM(shipping_supplies_cost)                                        AS shipping_supplies_cost,
            SUM(rpc.return_product_cost_local_amount_resaleable)               AS return_product_cost_local_amount_resaleable,
            SUM(discount)                                                      AS discount,
            SUM(initial_retail_price_excl_vat)                                 AS initial_retail_price_excl_vat,
            SUM(SUBTOTAL_AMOUNT_EXCL_TARIFF)                                   as SUBTOTAL_AMOUNT_EXCL_TARIFF,
            sum(PRODUCT_DISCOUNT_AMOUNT)                                       as PRODUCT_DISCOUNT_AMOUNT,
            SUM(product_cost)                                                  AS product_cost_amount,
            SUM(product_subtotal_amount)                                       as product_subtotal_amount,
            SUM(non_cash_credit_amount)                                        as non_cash_credit_amount,
            SUM(oracle_product_cost)                                           AS oracle_product_cost,
            SUM(credit_redemption_amount)                                      AS credit_redemption_amount,
            SUM(credit_redemption_count)                                       AS credit_redemption_count,
            SUM(gift_card_issuance_amount)                                     AS gift_card_issuance_amount,
            SUM(credit_issuance_amount)                                        AS credit_issuance_amount,
            SUM(credit_issuance_count)                                         AS credit_issuance_count,
            SUM(gift_card_redemption_amount)                                   AS gift_card_redemption_amount,
            SUM(conversion_eligible_orders)                                    AS conversion_eligible_orders,
            SUM(CASE WHEN paygo_activation = 1 THEN 1 ELSE 0 END)              AS paygo_activations,
            SUM(c.billing_cash_chargeback_amount)                              AS financial_billing_cash_chargeback_amount,
            SUM(c.product_order_cash_chargeback_amount)                        AS financial_product_order_cash_chargeback_amount,
            SUM(r.billing_cash_refund_amount)                                  AS financial_billing_cash_refund_amount,
            SUM(r.product_order_cash_refund_amount)                            AS financial_product_order_cash_refund_amount,
            SUM(r.product_order_cash_credit_refund_amount)                     AS financial_product_order_cash_credit_refund_amount,
            SUM(e.exchange_orders)                                             AS financial_exchange_orders,
            SUM(e.exchange_units)                                              AS financial_exchange_units,
            SUM(e.exchange_product_cost)                                       AS financial_exchange_product_cost,
            SUM(e.exchange_shipping_cost)                                      AS financial_exchange_shipping_cost,
            SUM(e.exchange_shipping_supplies_cost)                             AS financial_exchange_shipping_supplies_cost,
            SUM(rs.reship_orders)                                              AS financial_reship_orders,
            SUM(rs.reship_units)                                               AS financial_reship_units,
            SUM(rs.reship_product_cost)                                        AS financial_reship_product_cost,
            SUM(rs.reship_shipping_cost)                                       AS financial_reship_shipping_cost,
            SUM(rs.reship_shipping_supplies_cost)                              AS financial_reship_shipping_supplies_cost,
            SUM(CASE
                    WHEN rr.purchase_location = 'Online Purchase Retail Return' THEN quantity
                    ELSE 0 END)                                                AS financial_online_purchase_retail_returns,
            SUM(CASE
                    WHEN rr.purchase_location = 'Retail Purchase Retail Return' THEN quantity
                    ELSE 0 END)                                                AS financial_retail_purchase_retail_returns,
            SUM(CASE
                    WHEN rr.purchase_location = 'Retail Purchase Online Return' THEN quantity
                    ELSE 0 END)                                                AS financial_retail_purchase_online_returns,
            SUM(CASE
                    WHEN rr.purchase_location = 'Online Purchase Retail Return' THEN return_orders
                    ELSE 0 END)                                                AS financial_online_purchase_retail_return_orders,
            SUM(CASE
                    WHEN rr.purchase_location = 'Retail Purchase Retail Return' THEN return_orders
                    ELSE 0 END)                                                AS financial_retail_purchase_retail_return_orders,
            SUM(CASE
                    WHEN rr.purchase_location = 'Retail Purchase Online Return' THEN return_orders
                    ELSE 0 END)                                                AS financial_retail_purchase_online_return_orders,
            SUM(re.total_exchanges)                                            AS financial_total_exchanges,
            SUM(re.exchanges_after_online_return)                              AS financial_exchanges_after_online_return,
            SUM(re.exchanges_after_in_store_return)                            AS financial_exchanges_after_in_store_return,
            SUM(re.exchanges_after_in_store_return_from_online_purchase)       AS exchanges_after_in_store_return_from_online_purchase,
            SUM(re.total_exchanges_value)                                      AS financial_total_exchanges_value,
            SUM(re.exchanges_value_after_online_return)                        AS financial_exchanges_value_after_online_return,
            SUM(re.exchanges_value_after_in_store_return)                      AS financial_exchanges_value_after_in_store_return,
            SUM(re.exchanges_value_after_in_store_return_from_online_purchase) AS exchanges_value_after_in_store_return_from_online_purchase,
            SUM(CASE
                    WHEN o.customer_activation_channel != 'Retail' AND o.order_store_type = 'Retail' THEN o.orders
                    ELSE 0 END)                                                AS omni_orders,
            SUM(rpc.return_product_cost_local_amount)                          AS financial_return_product_cost_local_amount,
            SUM(rpc.return_shipping_cost_amount)                               AS financial_return_shipping_cost_amount
     FROM _order_metrics o
              FULL JOIN _financial_refunds r ON to_date(o.order_date) = to_date(r.refund_date)
         AND o.administrator_id = r.administrator_id
         AND o.activation_store_id = r.activation_store_id
         AND o.activation_cohort = r.activation_cohort
         AND o.shopped_channel = r.shopped_channel
         AND o.order_store_id = r.refund_store_id
         AND o.membership_order_type = r.membership_order_type
         AND o.is_retail_ship_only_order = r.is_retail_ship_only_order
         AND o.previous_membership_status = r.previous_membership_status
              FULL JOIN _financial_chargebacks c
                        ON to_date(c.chargeback_date) = to_date(COALESCE(o.order_date, r.refund_date))
                            AND c.administrator_id = COALESCE(o.administrator_id, r.administrator_id)
                            AND c.activation_store_id = COALESCE(o.activation_store_id, r.activation_store_id)
                            AND c.shopped_channel = COALESCE(o.shopped_channel, r.shopped_channel)
                            AND c.activation_cohort = COALESCE(o.activation_cohort, r.activation_cohort)
                            AND c.chargeback_store_id = COALESCE(o.order_store_id, r.refund_store_id)
                            AND c.membership_order_type = COALESCE(o.membership_order_type, r.membership_order_type)
                            AND c.is_retail_ship_only_order =
                                COALESCE(o.is_retail_ship_only_order, r.is_retail_ship_only_order)
                            AND c.previous_membership_status =
                                COALESCE(o.previous_membership_status, r.previous_membership_status)
              FULL JOIN _financial_return_product_cost rpc
                        ON to_date(rpc.return_date) = to_date(COALESCE(o.order_date, r.refund_date, c.chargeback_date))
                            AND
                           rpc.administrator_id = COALESCE(o.administrator_id, r.administrator_id, c.administrator_id)
                            AND rpc.activation_store_id =
                                COALESCE(o.activation_store_id, r.activation_store_id, c.activation_store_id)
                            AND rpc.activation_cohort =
                                COALESCE(o.activation_cohort, r.activation_cohort, c.activation_cohort)
                            AND rpc.shopped_channel = COALESCE(o.shopped_channel, r.shopped_channel, c.shopped_channel)
                            AND
                           rpc.return_store_id = COALESCE(o.order_store_id, r.refund_store_id, c.chargeback_store_id)
                            AND rpc.membership_order_type =
                                COALESCE(o.membership_order_type, r.membership_order_type, c.membership_order_type)
                            AND rpc.is_retail_ship_only_order =
                                COALESCE(o.is_retail_ship_only_order, r.is_retail_ship_only_order,
                                         c.is_retail_ship_only_order)
                            AND rpc.previous_membership_status =
                                COALESCE(o.previous_membership_status, r.previous_membership_status,
                                         c.previous_membership_status)
              FULL JOIN _financial_exchanges e on to_date(e.exchange_date) =
                                                  to_date(COALESCE(o.order_date, r.refund_date, c.chargeback_date,
                                                                   rpc.return_date))
         AND e.administrator_id =
             COALESCE(o.administrator_id, r.administrator_id, c.administrator_id, rpc.administrator_id)
         AND e.activation_store_id =
             COALESCE(o.activation_store_id, r.activation_store_id, c.activation_store_id, rpc.activation_store_id)
         AND e.activation_cohort =
             COALESCE(o.activation_cohort, r.activation_cohort, c.activation_cohort, rpc.activation_cohort)
         AND e.shopped_channel = COALESCE(o.shopped_channel, r.shopped_channel, c.shopped_channel, rpc.shopped_channel)
         AND e.exchange_store_id =
             COALESCE(o.order_store_id, r.refund_store_id, c.chargeback_store_id, rpc.return_store_id)
         AND e.membership_order_type =
             COALESCE(o.membership_order_type, r.membership_order_type, c.membership_order_type,
                      rpc.membership_order_type)
         AND e.is_retail_ship_only_order =
             COALESCE(o.is_retail_ship_only_order, r.is_retail_ship_only_order, c.is_retail_ship_only_order,
                      rpc.is_retail_ship_only_order)
         AND e.previous_membership_status =
             COALESCE(o.previous_membership_status, r.previous_membership_status, c.previous_membership_status,
                      rpc.previous_membership_status)
              FULL JOIN _financial_reships rs on to_date(rs.reship_date) =
                                                 to_date(COALESCE(o.order_date, r.refund_date, c.chargeback_date,
                                                                  rpc.return_date, e.exchange_date))
         AND rs.administrator_id =
             COALESCE(o.administrator_id, r.administrator_id, c.administrator_id, rpc.administrator_id,
                      e.administrator_id)
         AND rs.activation_store_id =
             COALESCE(o.activation_store_id, r.activation_store_id, c.activation_store_id, rpc.activation_store_id,
                      e.activation_store_id)
         AND rs.activation_cohort =
             COALESCE(o.activation_cohort, r.activation_cohort, c.activation_cohort, rpc.activation_cohort,
                      e.activation_cohort)
         AND rs.shopped_channel =
             COALESCE(o.shopped_channel, r.shopped_channel, c.shopped_channel, rpc.shopped_channel, e.shopped_channel)
         AND rs.reship_store_id =
             COALESCE(o.order_store_id, r.refund_store_id, c.chargeback_store_id, rpc.return_store_id,
                      e.exchange_store_id)
         AND rs.membership_order_type =
             COALESCE(o.membership_order_type, r.membership_order_type, c.membership_order_type,
                      rpc.membership_order_type, e.membership_order_type)
         AND rs.is_retail_ship_only_order =
             COALESCE(o.is_retail_ship_only_order, r.is_retail_ship_only_order, c.is_retail_ship_only_order,
                      rpc.is_retail_ship_only_order, e.is_retail_ship_only_order)
         AND rs.previous_membership_status =
             COALESCE(o.previous_membership_status, r.previous_membership_status, c.previous_membership_status,
                      rpc.previous_membership_status, e.previous_membership_status)
              FULL JOIN _financial_retail_returns rr ON to_date(rr.return_date) =
                                                        to_date(COALESCE(o.order_date, r.refund_date, c.chargeback_date,
                                                                         rpc.return_date, e.exchange_date,
                                                                         rs.reship_date))
         AND rr.administrator_id =
             COALESCE(o.administrator_id, r.administrator_id, c.administrator_id, rpc.administrator_id,
                      e.administrator_id, rs.administrator_id)
         AND rr.activation_store_id =
             COALESCE(o.activation_store_id, r.activation_store_id, c.activation_store_id, rpc.activation_store_id,
                      e.activation_store_id, rs.activation_store_id)
         AND rr.activation_cohort =
             COALESCE(o.activation_cohort, r.activation_cohort, c.activation_cohort, rpc.activation_cohort,
                      e.activation_cohort, rs.activation_cohort)
         AND rr.shopped_channel =
             COALESCE(o.shopped_channel, r.shopped_channel, c.shopped_channel, rpc.shopped_channel, e.shopped_channel,
                      rs.shopped_channel)
         AND rr.order_store_id =
             COALESCE(o.order_store_id, r.refund_store_id, c.chargeback_store_id, rpc.return_store_id,
                      e.exchange_store_id, rs.reship_store_id)
         AND rr.membership_order_type =
             COALESCE(o.membership_order_type, r.membership_order_type, c.membership_order_type,
                      rpc.membership_order_type, e.membership_order_type, rs.membership_order_type)
         AND rr.is_retail_ship_only_order =
             COALESCE(o.is_retail_ship_only_order, r.is_retail_ship_only_order, c.is_retail_ship_only_order,
                      rpc.is_retail_ship_only_order, e.is_retail_ship_only_order, rs.is_retail_ship_only_order)
         AND rr.previous_membership_status =
             COALESCE(o.previous_membership_status, r.previous_membership_status, c.previous_membership_status,
                      rpc.previous_membership_status, e.previous_membership_status, rs.previous_membership_status)
              FULL JOIN _financial_retail_exchanges re ON to_date(re.date) =
                                                          to_date(COALESCE(o.order_date, r.refund_date,
                                                                           c.chargeback_date, rpc.return_date,
                                                                           e.exchange_date, rs.reship_date,
                                                                           rr.return_date))
         AND re.administrator_id =
             COALESCE(o.administrator_id, r.administrator_id, c.administrator_id, rpc.administrator_id,
                      e.administrator_id, rs.administrator_id, rr.administrator_id)
         AND re.activation_store_id =
             COALESCE(o.activation_store_id, r.activation_store_id, c.activation_store_id, rpc.activation_store_id,
                      e.activation_store_id, rs.activation_store_id, rr.activation_store_id)
         AND re.activation_cohort =
             COALESCE(o.activation_cohort, r.activation_cohort, c.activation_cohort, rpc.activation_cohort,
                      e.activation_cohort, rs.activation_cohort, rr.activation_cohort)
         AND re.shopped_channel =
             COALESCE(o.shopped_channel, r.shopped_channel, c.shopped_channel, rpc.shopped_channel, e.shopped_channel,
                      rs.shopped_channel, rr.shopped_channel)
         AND re.order_store_id =
             COALESCE(o.order_store_id, r.refund_store_id, c.chargeback_store_id, rpc.return_store_id,
                      e.exchange_store_id, rs.reship_store_id, rr.order_store_id)
         AND re.membership_order_type =
             COALESCE(o.membership_order_type, r.membership_order_type, c.membership_order_type,
                      rpc.membership_order_type, e.membership_order_type, rs.membership_order_type,
                      rr.membership_order_type)
         AND re.is_retail_ship_only_order =
             COALESCE(o.is_retail_ship_only_order, r.is_retail_ship_only_order, c.is_retail_ship_only_order,
                      rpc.is_retail_ship_only_order, e.is_retail_ship_only_order, rs.is_retail_ship_only_order,
                      rr.is_retail_ship_only_order)
         AND re.previous_membership_status =
             COALESCE(o.previous_membership_status, r.previous_membership_status, c.previous_membership_status,
                      rpc.previous_membership_status, e.previous_membership_status, rs.previous_membership_status,
                      rr.previous_membership_status)
              LEFT JOIN _store_dimensions arsd ON arsd.store_id = COALESCE(o.activation_store_id, r.activation_store_id,
                                                                           c.activation_store_id,
                                                                           rpc.activation_store_id,
                                                                           e.activation_store_id,
                                                                           rs.activation_store_id,
                                                                           rr.activation_store_id,
                                                                           re.activation_store_id)
              LEFT JOIN _store_dimensions orsd ON orsd.store_id =
                                                  COALESCE(o.order_store_id, r.refund_store_id, c.chargeback_store_id,
                                                           rpc.return_store_id, e.exchange_store_id, rs.reship_store_id,
                                                           rr.order_store_id, re.order_store_id)
     GROUP BY to_date(COALESCE(o.order_date, r.refund_date, c.chargeback_date, rpc.return_date, e.exchange_date,
                               rs.reship_date, rr.return_date, re.date)),
              COALESCE(o.administrator_id, r.administrator_id, c.administrator_id, rpc.administrator_id,
                       e.administrator_id, rs.administrator_id, rr.administrator_id, re.administrator_id),
              COALESCE(o.customer_activation_channel, r.customer_activation_channel, c.customer_activation_channel,
                       rpc.customer_activation_channel, e.customer_activation_channel, rs.customer_activation_channel,
                       rr.customer_activation_channel, re.customer_activation_channel),
              CASE
                  WHEN datediff(month, ifnull(arsd.store_open_date, '2079-01-01'),
                                to_date(coalesce(o.order_date, r.refund_date, c.chargeback_date, rpc.return_date,
                                                 e.exchange_date, rs.reship_date, rr.return_date, re.date))) >= 13
                      THEN 'Comp'
                  ELSE 'New' END,
              COALESCE(o.activation_store_id, r.activation_store_id, c.activation_store_id, rpc.activation_store_id,
                       e.activation_store_id, rs.activation_store_id, rr.activation_store_id, re.activation_store_id),
              COALESCE(o.shopped_channel, r.shopped_channel, c.shopped_channel, rpc.shopped_channel, e.shopped_channel,
                       rs.shopped_channel, rr.shopped_channel, re.shopped_channel),
              COALESCE(o.order_store_type, r.refund_store_type, c.chargeback_store_type, rpc.return_store_type,
                       e.exchange_store_type, rs.reship_store_type, rr.order_store_type, re.order_store_type),
              COALESCE(o.order_store_id, r.refund_store_id, c.chargeback_store_id, rpc.return_store_id,
                       e.exchange_store_id, rs.reship_store_id, rr.order_store_id, re.order_store_id),
              CASE
                  WHEN datediff(month, ifnull(orsd.store_open_date, '2079-01-01'),
                                to_date(coalesce(o.order_date, r.refund_date, c.chargeback_date, rpc.return_date,
                                                 e.exchange_date, rs.reship_date, rr.return_date, re.date))) >= 13
                      THEN 'Comp'
                  ELSE 'New' END,
              COALESCE(o.membership_order_type, r.membership_order_type, c.membership_order_type,
                       rpc.membership_order_type, e.membership_order_type, rs.membership_order_type,
                       rr.membership_order_type, re.membership_order_type),
              COALESCE(o.is_retail_ship_only_order, r.is_retail_ship_only_order, c.is_retail_ship_only_order,
                       rpc.is_retail_ship_only_order, e.is_retail_ship_only_order, rs.is_retail_ship_only_order,
                       rr.is_retail_ship_only_order, re.is_retail_ship_only_order),
              o.latest_order);

// all financials for 4-Wall
CREATE OR REPLACE TEMPORARY TABLE _4_wall_financial as
    (SELECT '4-Wall'                                                                         AS metric_type,
            'Financial'                                                                      AS metric_method,
            order_date                                                                       AS date,
            order_store_id                                                                   AS store_id,
            st.store_full_name                                                               AS store_name,
            st.store_region                                                                  as region,
            zcs.longitude                                                                    AS longitude,
            zcs.latitude                                                                     AS latitude,
            st.store_retail_zip_code                                                         AS store_zip,
            st.store_retail_city                                                             AS store_city,
            st.store_retail_state                                                            AS store_state,
            orsd.store_open_date                                                             AS store_opening_date,
            orsd.sq_footage,
            orsd.store_number,
            CASE
                WHEN st.store_type ilike 'Retail' THEN st.store_sub_type
                ELSE 'Online' END                                                            AS store_retail_type,
            order_store_type                                                                 AS store_type,
            order_comp_new                                                                   AS comp_new,
            is_retail_ship_only_order,
            SUM(IFF(order_store_type = 'Retail', orders, 0))                                 AS total_orders,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest', orders,
                    0))                                                                      as first_guest_orders,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest', orders,
                    0))                                                                      as repeat_guest_orders,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP', orders,
                    0))                                                                      as act_vip_orders,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', orders,
                    0))                                                                      as repeat_vip_orders,
            SUM(IFF(order_store_type = 'Retail', units, 0))                                  AS total_units,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest', units,
                    0))                                                                      as first_guest_units,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest', units,
                    0))                                                                      as repeat_guest_units,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP', units,
                    0))                                                                      as act_vip_units,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', units,
                    0))                                                                      as repeat_vip_units,
            SUM(CASE WHEN order_store_type = 'Retail' THEN gift_card_orders ELSE 0 END)      as gift_card_orders,
            SUM(IFF(order_store_type = 'Retail', orders_with_credit_redemption, 0))          AS total_orders_with_credit_redemption,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest',
                    orders_with_credit_redemption,
                    0))                                                                      as first_guest_orders_with_credit_redemption,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest',
                    orders_with_credit_redemption,
                    0))                                                                      as repeat_guest_orders_with_credit_redemption,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP',
                    orders_with_credit_redemption,
                    0))                                                                      as act_vip_orders_with_credit_redemption,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', orders_with_credit_redemption,
                    0))                                                                      as repeat_vip_orders_with_credit_redemption,
            SUM(IFF(order_store_type = 'Retail', product_gross_revenue, 0))                  AS total_product_gross_revenue,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest', product_gross_revenue,
                    0))                                                                      as first_guest_product_gross_revenue,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest', product_gross_revenue,
                    0))                                                                      as repeat_guest_product_gross_revenue,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP', product_gross_revenue,
                    0))                                                                      as act_vip_product_gross_revenue,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', product_gross_revenue,
                    0))                                                                      as repeat_vip_product_gross_revenue,
            SUM(CASE WHEN order_store_type = 'Retail' THEN cash_gross_revenue ELSE 0 END)    AS cash_gross_revenue,
            SUM(IFF(order_store_type = 'Retail', financial_product_net_revenue, 0))          AS total_product_net_revenue,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest',
                    financial_product_net_revenue,
                    0))                                                                      as first_guest_product_net_revenue,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest',
                    financial_product_net_revenue,
                    0))                                                                      as repeat_guest_product_net_revenue,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP',
                    financial_product_net_revenue,
                    0))                                                                      as act_vip_product_net_revenue,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', financial_product_net_revenue,
                    0))                                                                      as repeat_vip_product_net_revenue,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_cash_net_revenue
                    ELSE 0 END)                                                              AS cash_net_revenue,
            SUM(IFF(order_store_type = 'Retail', financial_product_gross_profit, 0))         AS total_product_gross_profit,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest',
                    financial_product_gross_profit,
                    0))                                                                      as first_guest_product_gross_profit,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest',
                    financial_product_gross_profit,
                    0))                                                                      as repeat_guest_product_gross_profit,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP',
                    financial_product_gross_profit,
                    0))                                                                      as act_vip_product_gross_profit,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP',
                    financial_product_gross_profit,
                    0))                                                                      as repeat_vip_product_gross_profit,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_cash_gross_profit
                    ELSE 0 END)                                                              AS cash_gross_profit,
            SUM(CASE WHEN order_store_type = 'Retail' THEN shipping_revenue ELSE 0 END)      AS shipping_revenue,
            SUM(CASE WHEN order_store_type = 'Retail' THEN shipping_cost ELSE 0 END)         AS shipping_cost,
            SUM(IFF(order_store_type = 'Retail', discount, 0))                               AS total_discount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest', discount,
                    0))                                                                      as first_guest_discount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest', discount,
                    0))                                                                      as repeat_guest_discount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP', discount,
                    0))                                                                      as act_vip_discount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', discount,
                    0))                                                                      as repeat_vip_discount,
            SUM(IFF(order_store_type = 'Retail', initial_retail_price_excl_vat, 0))          AS total_initial_retail_price_excl_vat,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest',
                    initial_retail_price_excl_vat,
                    0))                                                                      as first_guest_initial_retail_price_excl_vat,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest',
                    initial_retail_price_excl_vat,
                    0))                                                                      as repeat_guest_initial_retail_price_excl_vat,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP',
                    initial_retail_price_excl_vat,
                    0))                                                                      as act_vip_initial_retail_price_excl_vat,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', initial_retail_price_excl_vat,
                    0))                                                                      as repeat_vip_initial_retail_price_excl_vat,
            --          SUBTOTAL_AMOUNT_EXCL_TARIFF
            SUM(IFF(order_store_type = 'Retail', subtotal_amount_excl_tariff, 0))            AS total_subtotal_amount_excl_tariff,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest',
                    subtotal_amount_excl_tariff,
                    0))                                                                      as first_guest_subtotal_amount_excl_tariff,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest',
                    subtotal_amount_excl_tariff,
                    0))                                                                      as repeat_guest_subtotal_amount_excl_tariff,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP',
                    subtotal_amount_excl_tariff,
                    0))                                                                      as act_vip_subtotal_amount_excl_tariff,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', subtotal_amount_excl_tariff,
                    0))                                                                      as repeat_vip_subtotal_amount_excl_tariff,
            --          PRODUCT_DISCOUNT_AMOUNT
            SUM(IFF(order_store_type = 'Retail', product_discount_amount, 0))                AS total_product_discount_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest',
                    product_discount_amount,
                    0))                                                                      as first_guest_product_discount_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest',
                    product_discount_amount,
                    0))                                                                      as repeat_guest_product_discount_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP',
                    product_discount_amount,
                    0))                                                                      as act_vip_product_discount_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', product_discount_amount,
                    0))                                                                      as repeat_vip_product_discount_amount,
            SUM(IFF(order_store_type = 'Retail', product_cost_amount, 0))                    AS total_product_cost_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest', product_cost_amount,
                    0))                                                                      as first_guest_product_cost_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest', product_cost_amount,
                    0))                                                                      as repeat_guest_product_cost_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP', product_cost_amount,
                    0))                                                                      as act_vip_product_cost_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', product_cost_amount,
                    0))                                                                      as repeat_vip_product_cost_amount,
            SUM(IFF(order_store_type = 'Retail', product_subtotal_amount, 0))                AS total_product_subtotal_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest', product_subtotal_amount,
                    0))                                                                      as first_guest_product_subtotal_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest', product_subtotal_amount,
                    0))                                                                      as repeat_guest_product_subtotal_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP', product_subtotal_amount,
                    0))                                                                      as act_vip_product_subtotal_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', product_subtotal_amount,
                    0))                                                                      as repeat_vip_product_subtotal_amount,
            SUM(IFF(order_store_type = 'Retail', non_cash_credit_amount, 0))                 AS total_non_cash_credit_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'First Guest', non_cash_credit_amount,
                    0))                                                                      as first_guest_non_cash_credit_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat Guest', non_cash_credit_amount,
                    0))                                                                      as repeat_guest_non_cash_credit_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Activating VIP', non_cash_credit_amount,
                    0))                                                                      as act_vip_non_cash_credit_amount,
            SUM(IFF(order_store_type = 'Retail' and membership_order_type = 'Repeat VIP', non_cash_credit_amount,
                    0))                                                                      as repeat_vip_non_cash_credit_amount,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN credit_redemption_amount
                    ELSE 0 END)                                                              AS credit_redemption_amount,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN credit_redemption_count
                    ELSE 0 END)                                                              AS credit_redemption_count,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN gift_card_issuance_amount
                    ELSE 0 END)                                                              AS gift_card_issuance_amount,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN credit_issuance_amount
                    ELSE 0 END)                                                              AS credit_issuance_amount,
            SUM(CASE WHEN order_store_type = 'Retail' THEN credit_issuance_count ELSE 0 END) AS credit_issuance_count,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN gift_card_redemption_amount
                    ELSE 0 END)                                                              AS gift_card_redemption_amount,
            SUM(IFF(order_store_type = 'Retail', conversion_eligible_orders, 0))             AS conversion_eligible_orders,
            SUM(CASE WHEN order_store_type = 'Retail' THEN paygo_activations ELSE 0 END)     AS paygo_activations,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_product_order_cash_refund_amount
                    ELSE 0 END)                                                              AS product_order_cash_refund_amount,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_product_order_cash_credit_refund_amount
                    ELSE 0 END)                                                              AS product_order_cash_credit_refund_amount,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_billing_cash_refund_amount
                    ELSE 0 END)                                                              AS billing_cash_refund_amount,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_billing_cash_chargeback_amount
                    ELSE 0 END)                                                              AS billing_cash_chargeback_amount,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_product_order_cash_chargeback_amount
                    ELSE 0 END)                                                              AS product_order_cash_chargeback_amount,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_reship_orders
                    ELSE 0 END)                                                              AS reship_orders,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_reship_units
                    ELSE 0 END)                                                              AS reship_units,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_reship_product_cost
                    ELSE 0 END)                                                              AS reship_product_cost,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_exchange_orders
                    ELSE 0 END)                                                              AS exchange_orders,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_exchange_units
                    ELSE 0 END)                                                              AS exchange_units,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_exchange_product_cost
                    ELSE 0 END)                                                              AS exchange_product_cost,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_return_product_cost_local_amount
                    ELSE 0 END)                                                              AS return_product_cost,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_return_shipping_cost_amount
                    ELSE 0 END)                                                              AS return_shipping_cost,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_online_purchase_retail_returns
                    ELSE 0 END)                                                              AS online_purchase_retail_returns,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_retail_purchase_retail_returns
                    ELSE 0 END)                                                              AS retail_purchase_retail_returns,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_retail_purchase_online_returns
                    ELSE 0 END)                                                              AS retail_purchase_online_returns,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_online_purchase_retail_return_orders
                    ELSE 0 END)                                                              AS online_purchase_retail_return_orders,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_retail_purchase_retail_return_orders
                    ELSE 0 END)                                                              AS retail_purchase_retail_return_orders,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_retail_purchase_online_return_orders
                    ELSE 0 END)                                                              AS retail_purchase_online_return_orders,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_total_exchanges
                    ELSE 0 END)                                                              AS total_exchanges,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_exchanges_after_online_return
                    ELSE 0 END)                                                              AS exchanges_after_online_return,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_exchanges_after_in_store_return
                    ELSE 0 END)                                                              AS exchanges_after_in_store_return,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN exchanges_after_in_store_return_from_online_purchase
                    ELSE 0 END)                                                              AS exchanges_after_in_store_return_from_online_purchase,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_total_exchanges_value
                    ELSE 0 END)                                                              AS total_exchanges_value,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_exchanges_value_after_online_return
                    ELSE 0 END)                                                              AS exchanges_value_after_online_return,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN financial_exchanges_value_after_in_store_return
                    ELSE 0 END)                                                              AS exchanges_value_after_in_store_return,
            SUM(CASE
                    WHEN order_store_type = 'Retail' THEN exchanges_value_after_in_store_return_from_online_purchase
                    ELSE 0 END)                                                              AS exchanges_value_after_in_store_return_from_online_purchase
     FROM _final_temp f
              LEFT JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = f.order_store_id
              LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.zip_city_state zcs ON zcs.zip = st.store_retail_zip_code
              LEFT JOIN _store_dimensions orsd ON orsd.store_id = f.order_store_id

     GROUP BY '4-Wall',
              'Financial',
              order_date,
              order_store_id,
              st.store_full_name,
              st.store_region,
              zcs.longitude,
              zcs.latitude,
              st.store_retail_zip_code,
              st.store_retail_city,
              st.store_retail_state,
              orsd.store_open_date,
              orsd.sq_footage,
              orsd.store_number,
              CASE WHEN st.store_type ilike 'Retail' THEN st.store_sub_type ELSE 'Online' END,
              order_store_type,
              order_comp_new,
              is_retail_ship_only_order);

// all financials for both 4-Wall and attribution unioned together
CREATE OR REPLACE TEMPORARY TABLE _4_wall_attribution_financial as
    (select coalesce(fwf.metric_type, drt.metric_type)                            as metric_type,
            coalesce(fwf.metric_method, drt.metric_method)                        as metric_method,
            coalesce(fwf.date, drt.date)                                          as date,
            coalesce(fwf.store_id, drt.store_id)                                  as store_id,
            coalesce(fwf.store_name, drt.store_full_name)                         as store_name,
            coalesce(fwf.store_number, drt.store_number)                          as store_number,
            coalesce(fwf.region, drt.store_region)                                as region,
            coalesce(fwf.longitude, drt.longitude)                                as longitude,
            coalesce(fwf.latitude, drt.latitude)                                  as latitude,
            coalesce(fwf.store_zip, drt.store_zip)                                as store_zip,
            coalesce(fwf.store_city, drt.store_city)                              as store_city,
            coalesce(fwf.store_state, drt.store_state)                            as store_state,
            coalesce(fwf.store_opening_date, drt.store_open_date)                 as store_opening_date,
            coalesce(fwf.sq_footage, drt.sq_footage)                              as sq_footage,
            coalesce(fwf.store_retail_type, drt.store_retail_type)                as store_retail_type,
            coalesce(fwf.store_type, drt.store_type)                              as store_type,
            coalesce(fwf.comp_new, drt.comp_new)                                  as comp_new,
            coalesce(is_retail_ship_only_order, false)                            as is_retail_ship_only_order,
            coalesce(fwf.total_orders, 0)                                         as total_orders,
            coalesce(fwf.first_guest_orders, 0)                                   as first_guest_orders,
            coalesce(fwf.repeat_guest_orders, 0)                                  as repeat_guest_orders,
            coalesce(fwf.act_vip_orders, 0)                                       as act_vip_orders,
            coalesce(fwf.repeat_vip_orders, 0)                                    as repeat_vip_orders,
            coalesce(fwf.total_units, 0)                                          as total_units,
            coalesce(fwf.first_guest_units, 0)                                    as first_guest_units,
            coalesce(fwf.repeat_guest_units, 0)                                   as repeat_guest_units,
            coalesce(fwf.act_vip_units, 0)                                        as act_vip_units,
            coalesce(fwf.repeat_vip_units, 0)                                     as repeat_vip_units,
            coalesce(fwf.gift_card_orders, 0)                                     as gift_card_orders,
            coalesce(fwf.total_orders_with_credit_redemption, 0)                  as total_orders_with_credit_redemption,
            coalesce(fwf.first_guest_orders_with_credit_redemption, 0)            as first_guest_orders_with_credit_redemption,
            coalesce(fwf.repeat_guest_orders_with_credit_redemption, 0)           as repeat_guest_orders_with_credit_redemption,
            coalesce(fwf.act_vip_orders_with_credit_redemption, 0)                as act_vip_orders_with_credit_redemption,
            coalesce(fwf.repeat_vip_orders_with_credit_redemption, 0)             as repeat_vip_orders_with_credit_redemption,
            coalesce(fwf.total_product_gross_revenue, 0)                          as total_product_gross_revenue,
            coalesce(fwf.first_guest_product_gross_revenue, 0)                    as first_guest_product_gross_revenue,
            coalesce(fwf.repeat_guest_product_gross_revenue, 0)                   as repeat_guest_product_gross_revenue,
            coalesce(fwf.act_vip_product_gross_revenue, 0)                        as act_vip_product_gross_revenue,
            coalesce(fwf.repeat_vip_product_gross_revenue, 0)                     as repeat_vip_product_gross_revenue,
            coalesce(fwf.cash_gross_revenue, 0)                                   as cash_gross_revenue,
            coalesce(fwf.total_product_net_revenue, 0)                            as total_product_net_revenue,
            coalesce(fwf.first_guest_product_net_revenue, 0)                      as first_guest_product_net_revenue,
            coalesce(fwf.repeat_guest_product_net_revenue, 0)                     as repeat_guest_product_net_revenue,
            coalesce(fwf.act_vip_product_net_revenue, 0)                          as act_vip_product_net_revenue,
            coalesce(fwf.repeat_vip_product_net_revenue, 0)                       as repeat_vip_product_net_revenue,
            coalesce(fwf.cash_net_revenue, 0)                                     as cash_net_revenue,
            coalesce(fwf.total_product_gross_profit, 0)                           as total_product_gross_profit,
            coalesce(fwf.first_guest_product_gross_profit, 0)                     as first_guest_product_gross_profit,
            coalesce(fwf.repeat_guest_product_gross_profit, 0)                    as repeat_guest_product_gross_profit,
            coalesce(fwf.act_vip_product_gross_profit, 0)                         as act_vip_product_gross_profit,
            coalesce(fwf.repeat_vip_product_gross_profit, 0)                      as repeat_vip_product_gross_profit,
            coalesce(fwf.cash_gross_profit, 0)                                    as cash_gross_profit,
            coalesce(fwf.shipping_revenue, 0)                                     as shipping_revenue,
            coalesce(fwf.shipping_cost, 0)                                        as shipping_cost,
            coalesce(fwf.total_discount, 0)                                       as total_discount,
            coalesce(fwf.first_guest_discount, 0)                                 as first_guest_discount,
            coalesce(fwf.repeat_guest_discount, 0)                                as repeat_guest_discount,
            coalesce(fwf.act_vip_discount, 0)                                     as act_vip_discount,
            coalesce(fwf.repeat_vip_discount, 0)                                  as repeat_vip_discount,
            coalesce(fwf.total_initial_retail_price_excl_vat, 0)                  as total_initial_retail_price_excl_vat,
            coalesce(fwf.first_guest_initial_retail_price_excl_vat, 0)            as first_guest_initial_retail_price_excl_vat,
            coalesce(fwf.repeat_guest_initial_retail_price_excl_vat, 0)           as repeat_guest_initial_retail_price_excl_vat,
            coalesce(fwf.act_vip_initial_retail_price_excl_vat, 0)                as act_vip_initial_retail_price_excl_vat,
            coalesce(fwf.repeat_vip_initial_retail_price_excl_vat, 0)             as repeat_vip_initial_retail_price_excl_vat,
            coalesce(fwf.total_subtotal_amount_excl_tariff, 0)                    as total_subtotal_amount_excl_tariff,
            coalesce(fwf.first_guest_subtotal_amount_excl_tariff, 0)              as first_guest_subtotal_amount_excl_tariff,
            coalesce(fwf.repeat_guest_subtotal_amount_excl_tariff, 0)             as repeat_guest_subtotal_amount_excl_tariff,
            coalesce(fwf.act_vip_subtotal_amount_excl_tariff, 0)                  as act_vip_subtotal_amount_excl_tariff,
            coalesce(fwf.repeat_vip_subtotal_amount_excl_tariff, 0)               as repeat_vip_subtotal_amount_excl_tariff,
            coalesce(fwf.total_product_discount_amount, 0)                        as total_product_discount_amount,
            coalesce(fwf.first_guest_product_discount_amount, 0)                  as first_guest_product_discount_amount,
            coalesce(fwf.repeat_guest_product_discount_amount, 0)                 as repeat_guest_product_discount_amount,
            coalesce(fwf.act_vip_product_discount_amount, 0)                      as act_vip_product_discount_amount,
            coalesce(fwf.repeat_vip_product_discount_amount, 0)                   as repeat_vip_product_discount_amount,
            coalesce(fwf.total_product_cost_amount, 0)                            as total_product_cost_amount,
            coalesce(fwf.first_guest_product_cost_amount, 0)                      as first_guest_product_cost_amount,
            coalesce(fwf.repeat_guest_product_cost_amount, 0)                     as repeat_guest_product_cost_amount,
            coalesce(fwf.act_vip_product_cost_amount, 0)                          as act_vip_product_cost_amount,
            coalesce(fwf.repeat_vip_product_cost_amount, 0)                       as repeat_vip_product_cost_amount,
            coalesce(fwf.total_product_subtotal_amount, 0)                        as total_product_subtotal_amount,
            coalesce(fwf.first_guest_product_subtotal_amount, 0)                  as first_guest_product_subtotal_amount,
            coalesce(fwf.repeat_guest_product_subtotal_amount, 0)                 as repeat_guest_product_subtotal_amount,
            coalesce(fwf.act_vip_product_subtotal_amount, 0)                      as act_vip_product_subtotal_amount,
            coalesce(fwf.repeat_vip_product_subtotal_amount, 0)                   as repeat_vip_product_subtotal_amount,
            coalesce(fwf.total_non_cash_credit_amount, 0)                         as total_non_cash_credit_amount,
            coalesce(fwf.first_guest_non_cash_credit_amount, 0)                   as first_guest_non_cash_credit_amount,
            coalesce(fwf.repeat_guest_non_cash_credit_amount, 0)                  as repeat_guest_non_cash_credit_amount,
            coalesce(fwf.act_vip_non_cash_credit_amount, 0)                       as act_vip_non_cash_credit_amount,
            coalesce(fwf.repeat_vip_non_cash_credit_amount, 0)                    as repeat_vip_non_cash_credit_amount,
            coalesce(fwf.credit_redemption_amount, 0)                             as credit_redemption_amount,
            coalesce(fwf.credit_redemption_count, 0)                              as credit_redemption_count,
            coalesce(fwf.gift_card_issuance_amount, 0)                            as gift_card_issuance_amount,
            coalesce(fwf.credit_issuance_amount, 0)                               as credit_issuance_amount,
            coalesce(fwf.credit_issuance_count, 0)                                as credit_issuance_count,
            coalesce(fwf.gift_card_redemption_amount, 0)                          as gift_card_redemption_amount,
            coalesce(fwf.conversion_eligible_orders, 0)                           as conversion_eligible_orders,
            coalesce(fwf.paygo_activations, 0)                                    as paygo_activations,
            coalesce(fwf.product_order_cash_refund_amount, 0)                     as product_order_cash_refund_amount,
            coalesce(fwf.product_order_cash_credit_refund_amount, 0)              as product_order_cash_credit_refund_amount,
            coalesce(fwf.billing_cash_refund_amount, 0)                           as billing_cash_refund_amount,
            coalesce(fwf.billing_cash_chargeback_amount, 0)                       as billing_cash_chargeback_amount,
            coalesce(fwf.product_order_cash_chargeback_amount, 0)                 as product_order_cash_chargeback_amount,
            coalesce(fwf.reship_orders, 0)                                        as reship_orders,
            coalesce(fwf.reship_units, 0)                                         as reship_units,
            coalesce(fwf.reship_product_cost, 0)                                  as reship_product_cost,
            coalesce(fwf.exchange_orders, 0)                                      as exchange_orders,
            coalesce(fwf.exchange_units, 0)                                       as exchange_units,
            coalesce(fwf.exchange_product_cost, 0)                                as exchange_product_cost,
            coalesce(fwf.return_product_cost, 0)                                  as return_product_cost,
            coalesce(fwf.return_shipping_cost, 0)                                 as return_shipping_cost,
            coalesce(fwf.online_purchase_retail_returns, 0)                       as online_purchase_retail_returns,
            coalesce(fwf.retail_purchase_retail_returns, 0)                       as retail_purchase_retail_returns,
            coalesce(fwf.retail_purchase_online_returns, 0)                       as retail_purchase_online_returns,
            coalesce(fwf.online_purchase_retail_return_orders, 0)                 as online_purchase_retail_return_orders,
            coalesce(fwf.retail_purchase_retail_return_orders, 0)                 as retail_purchase_retail_return_orders,
            coalesce(fwf.retail_purchase_online_return_orders, 0)                 as retail_purchase_online_return_orders,
            coalesce(fwf.total_exchanges, 0)                                      as total_exchanges,
            coalesce(fwf.exchanges_after_online_return, 0)                        as exchanges_after_online_return,
            coalesce(fwf.exchanges_after_in_store_return, 0)                      as exchanges_after_in_store_return,
            coalesce(fwf.exchanges_after_in_store_return_from_online_purchase, 0) as exchanges_after_in_store_return_from_online_purchase,
            coalesce(fwf.total_exchanges_value, 0)                                as total_exchanges_value,
            coalesce(fwf.exchanges_value_after_online_return, 0)                  as exchanges_value_after_online_return,
            coalesce(fwf.exchanges_value_after_in_store_return, 0)                as exchanges_value_after_in_store_return,
            coalesce(fwf.exchanges_value_after_in_store_return_from_online_purchase,
                     0)                                                           as exchanges_value_after_in_store_return_from_online_purchase,
            coalesce(drt.incoming_traffic_ty, 0)                                  as incoming_traffic,
            coalesce(drt.exiting_traffic_ty, 0)                                   as exiting_traffic
     from _4_wall_financial fwf
              full outer join _daily_retail_traffic drt
                              on fwf.date = drt.date and fwf.store_id = drt.store_id AND
                                 fwf.metric_type = drt.METRIC_TYPE
              left join _store_dimensions sd on sd.store_id = fwf.store_id
     UNION
     SELECT 'Attribution'                                                                AS metric_type,
            'Financial'                                                                  AS metric_method,
            order_date                                                                   AS date,
            f.activation_store_id                                                        AS store_id,
            st.store_full_name                                                           AS store_name,
            arsd.store_number,
            st.store_region                                                              as region,
            rv.activation_store_longitude                                                AS longitude,
            rv.activation_store_latitude                                                 AS latitude,
            rv.activation_store_zip_code                                                 AS store_zip,
            rv.activation_store_city                                                     AS store_city,
            rv.activation_store_state                                                    AS store_state,
            arsd.store_open_date                                                         AS store_opening_date,
            arsd.sq_footage,
            CASE
                WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type
                ELSE 'Online'
                END                                                                      AS store_retail_type,
            customer_activation_channel                                                  AS store_type,
            activation_comp_new                                                          AS comp_new,
            is_retail_ship_only_order,
            SUM(IFF(order_store_type != 'Retail', orders, 0))                            AS total_orders,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest', orders,
                    0))                                                                  as first_guest_orders,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest', orders,
                    0))                                                                  as repeat_guest_orders,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP', orders,
                    0))                                                                  as act_vip_orders,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP', orders,
                    0))                                                                  as repeat_vip_orders,
            SUM(IFF(order_store_type != 'Retail', units, 0))                             AS total_units,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest', units,
                    0))                                                                  as first_guest_units,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest', units,
                    0))                                                                  as repeat_guest_units,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP', units,
                    0))                                                                  as act_vip_units,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP', units,
                    0))                                                                  as repeat_vip_units,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN gift_card_orders
                    ELSE 0 END)                                                          as gift_card_orders,
            SUM(IFF(order_store_type != 'Retail', orders_with_credit_redemption, 0))     AS total_orders_with_credit_redemption,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest',
                    orders_with_credit_redemption,
                    0))                                                                  as first_guest_orders_with_credit_redemption,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest',
                    orders_with_credit_redemption,
                    0))                                                                  as repeat_guest_orders_with_credit_redemption,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP',
                    orders_with_credit_redemption,
                    0))                                                                  as act_vip_orders_with_credit_redemption,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP',
                    orders_with_credit_redemption,
                    0))                                                                  as repeat_vip_orders_with_credit_redemption,
            SUM(IFF(order_store_type != 'Retail', product_gross_revenue, 0))             AS total_product_gross_revenue,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest', product_gross_revenue,
                    0))                                                                  as first_guest_product_gross_revenue,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest', product_gross_revenue,
                    0))                                                                  as repeat_guest_product_gross_revenue,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP', product_gross_revenue,
                    0))                                                                  as act_vip_product_gross_revenue,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP', product_gross_revenue,
                    0))                                                                  as repeat_vip_product_gross_revenue,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN cash_gross_revenue
                    ELSE 0 END)                                                          AS cash_gross_revenue,
            SUM(IFF(order_store_type != 'Retail', financial_product_net_revenue, 0))     AS total_product_net_revenue,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest',
                    financial_product_net_revenue,
                    0))                                                                  as first_guest_product_net_revenue,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest',
                    financial_product_net_revenue,
                    0))                                                                  as repeat_guest_product_net_revenue,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP',
                    financial_product_net_revenue,
                    0))                                                                  as act_vip_product_net_revenue,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP',
                    financial_product_net_revenue,
                    0))                                                                  as repeat_vip_product_net_revenue,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_cash_net_revenue
                    ELSE 0 END)                                                          AS cash_net_revenue,
            SUM(IFF(order_store_type != 'Retail', financial_product_gross_profit, 0))    AS total_product_gross_profit,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest',
                    financial_product_gross_profit,
                    0))                                                                  as first_guest_product_gross_profit,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest',
                    financial_product_gross_profit,
                    0))                                                                  as repeat_guest_product_gross_profit,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP',
                    financial_product_gross_profit,
                    0))                                                                  as act_vip_product_gross_profit,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP',
                    financial_product_gross_profit,
                    0))                                                                  as repeat_vip_product_gross_profit,
            SUM(CASE
                    WHEN order_store_type != 'Retail' THEN financial_cash_gross_profit
                    ELSE 0 END)                                                          AS cash_gross_profit,
            SUM(CASE WHEN order_store_type != 'Retail' THEN shipping_revenue ELSE 0 END) AS shipping_revenue,
            SUM(CASE WHEN order_store_type != 'Retail' THEN shipping_cost ELSE 0 END)    AS shipping_cost,
            SUM(IFF(order_store_type != 'Retail', discount, 0))                          AS total_discount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest', discount,
                    0))                                                                  as first_guest_discount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest', discount,
                    0))                                                                  as repeat_guest_discount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP', discount,
                    0))                                                                  as act_vip_discount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP', discount,
                    0))                                                                  as repeat_vip_discount,
            SUM(IFF(order_store_type != 'Retail', initial_retail_price_excl_vat, 0))     AS total_initial_retail_price_excl_vat,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest',
                    initial_retail_price_excl_vat,
                    0))                                                                  as first_guest_initial_retail_price_excl_vat,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest',
                    initial_retail_price_excl_vat,
                    0))                                                                  as repeat_guest_initial_retail_price_excl_vat,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP',
                    initial_retail_price_excl_vat,
                    0))                                                                  as act_vip_initial_retail_price_excl_vat,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP',
                    initial_retail_price_excl_vat,
                    0))                                                                  as repeat_vip_initial_retail_price_excl_vat,
            --          SUBTOTAL_AMOUNT_EXCL_TARIFF
            SUM(IFF(order_store_type != 'Retail', subtotal_amount_excl_tariff, 0))       AS total_subtotal_amount_excl_tariff,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest',
                    subtotal_amount_excl_tariff,
                    0))                                                                  as first_guest_subtotal_amount_excl_tariff,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest',
                    subtotal_amount_excl_tariff,
                    0))                                                                  as repeat_guest_subtotal_amount_excl_tariff,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP',
                    subtotal_amount_excl_tariff,
                    0))                                                                  as act_vip_subtotal_amount_excl_tariff,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP', subtotal_amount_excl_tariff,
                    0))                                                                  as repeat_vip_subtotal_amount_excl_tariff,
            --          PRODUCT_DISCOUNT_AMOUNT
            SUM(IFF(order_store_type != 'Retail', product_discount_amount, 0))           AS total_product_discount_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest',
                    product_discount_amount,
                    0))                                                                  as first_guest_product_discount_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest',
                    product_discount_amount,
                    0))                                                                  as repeat_guest_product_discount_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP',
                    product_discount_amount,
                    0))                                                                  as act_vip_product_discount_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP', product_discount_amount,
                    0))                                                                  as repeat_vip_product_discount_amount,
            SUM(IFF(order_store_type != 'Retail', product_cost_amount, 0))               AS total_product_cost_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest', product_cost_amount,
                    0))                                                                  as first_guest_product_cost_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest', product_cost_amount,
                    0))                                                                  as repeat_guest_product_cost_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP', product_cost_amount,
                    0))                                                                  as act_vip_product_cost_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP', product_cost_amount,
                    0))                                                                  as repeat_vip_product_cost_amount,
            SUM(IFF(order_store_type != 'Retail', product_subtotal_amount, 0))           AS total_product_subtotal_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest', product_subtotal_amount,
                    0))                                                                  as first_guest_product_subtotal_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest', product_subtotal_amount,
                    0))                                                                  as repeat_guest_product_subtotal_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP', product_subtotal_amount,
                    0))                                                                  as act_vip_product_subtotal_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP', product_subtotal_amount,
                    0))                                                                  as repeat_vip_product_subtotal_amount,
            SUM(IFF(order_store_type != 'Retail', non_cash_credit_amount, 0))            AS total_non_cash_credit_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'First Guest', non_cash_credit_amount,
                    0))                                                                  as first_guest_non_cash_credit_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat Guest', non_cash_credit_amount,
                    0))                                                                  as repeat_guest_non_cash_credit_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Activating VIP', non_cash_credit_amount,
                    0))                                                                  as act_vip_non_cash_credit_amount,
            SUM(IFF(order_store_type != 'Retail' and membership_order_type = 'Repeat VIP', non_cash_credit_amount,
                    0))                                                                  as repeat_vip_non_cash_credit_amount,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN credit_redemption_amount
                    ELSE 0 END)                                                          AS credit_redemption_amount,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN credit_redemption_count
                    ELSE 0 END)                                                          AS credit_redemption_count,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN gift_card_issuance_amount
                    ELSE 0 END)                                                          AS gift_card_issuance_amount,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN credit_issuance_amount
                    ELSE 0 END)                                                          AS credit_issuance_amount,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN credit_issuance_count
                    ELSE 0 END)                                                          AS credit_issuance_count,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN gift_card_redemption_amount
                    ELSE 0 END)                                                          AS gift_card_redemption_amount,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN conversion_eligible_orders
                    ELSE 0 END)                                                          AS conversion_eligible_orders,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN paygo_activations
                    ELSE 0 END)                                                          AS paygo_activations,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_product_order_cash_refund_amount
                    ELSE 0 END)                                                          AS product_order_cash_refund_amount,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_product_order_cash_credit_refund_amount
                    ELSE 0 END)                                                          AS product_order_cash_credit_refund_amount,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_billing_cash_refund_amount
                    ELSE 0 END)                                                          AS billing_cash_refund_amount,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_billing_cash_chargeback_amount
                    ELSE 0 END)                                                          AS billing_cash_chargeback_amount,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_product_order_cash_chargeback_amount
                    ELSE 0 END)                                                          AS product_order_cash_chargeback_amount,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_reship_orders
                    ELSE 0 END)                                                          AS reship_orders,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_reship_units
                    ELSE 0 END)                                                          AS reship_units,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_reship_product_cost
                    ELSE 0 END)                                                          AS reship_product_cost,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_exchange_orders
                    ELSE 0 END)                                                          AS exchange_orders,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_exchange_units
                    ELSE 0 END)                                                          AS exchange_units,
            SUM(CASE
                    WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                        THEN financial_exchange_product_cost
                    ELSE 0 END)                                                          AS exchange_product_cost,
            SUM(ifnull(CASE
                           WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                               THEN financial_return_product_cost_local_amount
                           ELSE 0 END,
                       0))                                                               AS return_product_cost,
            SUM(ifnull(CASE
                           WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                               THEN financial_return_shipping_cost_amount
                           ELSE 0 END,
                       0))                                                               AS return_shipping_cost,
            '0'                                                                          AS online_purchase_retail_returns,
            '0'                                                                          AS retail_purchase_retail_returns,
            '0'                                                                          AS retail_purchase_online_returns,
            '0'                                                                          AS online_purchase_retail_return_orders,
            '0'                                                                          AS retail_purchase_retail_return_orders,
            '0'                                                                          AS retail_purchase_online_return_orders,
            '0'                                                                          AS total_exchanges,
            '0'                                                                          AS exchanges_after_online_return,
            '0'                                                                          AS exchanges_after_in_store_return,
            '0'                                                                          AS exchanges_after_in_store_return_from_online_purchase,
            '0'                                                                          AS total_exchanges_value,
            '0'                                                                          AS exchanges_value_after_online_return,
            '0'                                                                          AS exchanges_value_after_in_store_return,
            '0'                                                                          AS exchanges_value_after_in_store_return_from_online_purchase,
            '0'                                                                          AS incoming_traffic_ty,
            '0'                                                                          AS exiting_traffic_ty
     FROM _final_temp f
              LEFT JOIN (SELECT DISTINCT activation_store_id,
                                         activation_store_state,
                                         activation_store_longitude,
                                         activation_store_latitude,
                                         activation_store_zip_code,
                                         activation_store_city
                         FROM _vip_activation_detail) rv ON f.activation_store_id = rv.activation_store_id
              LEFT JOIN _store_dimensions arsd ON arsd.store_id = f.activation_store_id
              LEFT JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = f.activation_store_id
     GROUP BY 'Attribution',
              'Financial',
              order_date,
              f.activation_store_id,
              st.store_full_name,
              st.store_region,
              rv.activation_store_longitude,
              rv.activation_store_latitude,
              rv.activation_store_zip_code,
              rv.activation_store_city,
              rv.activation_store_state,
              arsd.store_open_date,
              arsd.SQ_FOOTAGE,
              arsd.store_number,
              CASE
                  WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type
                  ELSE 'Online'
                  END,
              customer_activation_channel,
              activation_comp_new,
              is_retail_ship_only_order);

// all financials for 4-Wall with attribution
CREATE OR REPLACE TEMPORARY TABLE _4_wall_with_attribution as
    (SELECT '4-Wall with Attribution'                                       AS metric_type,
            metric_method,
            date,
            store_id,
            longitude,
            latitude,
            store_name,
            region,
            store_zip,
            store_city,
            store_state,
            store_opening_date,
            sq_footage,
            store_number,
            CASE
                WHEN store_retail_type ilike 'Partnership' THEN 'Activations'
                ELSE store_retail_type END                                  AS store_retail_type,
            store_type,
            comp_new,
            coalesce(is_retail_ship_only_order, false)                      as is_retail_ship_only_order,
            SUM(TOTAL_ORDERS)                                               AS total_orders,
            SUM(FIRST_GUEST_ORDERS)                                         as first_guest_orders,
            SUM(REPEAT_GUEST_ORDERS)                                        as repeat_guest_orders,
            SUM(ACT_VIP_ORDERS)                                             as act_vip_orders,
            SUM(REPEAT_VIP_ORDERS)                                          as repeat_vip_orders,
            SUM(TOTAL_UNITS)                                                AS total_units,
            SUM(FIRST_GUEST_UNITS)                                          as first_guest_units,
            SUM(REPEAT_GUEST_UNITS)                                         as repeat_guest_units,
            SUM(ACT_VIP_UNITS)                                              as act_vip_units,
            SUM(REPEAT_VIP_UNITS)                                           as repeat_vip_units,
            SUM(gift_card_orders)                                           as gift_card_orders,
            SUM(TOTAL_orders_with_credit_redemption)                        AS total_orders_with_credit_redemption,
            SUM(FIRST_GUEST_orders_with_credit_redemption)                  as first_guest_orders_with_credit_redemption,
            SUM(REPEAT_GUEST_orders_with_credit_redemption)                 as repeat_guest_orders_with_credit_redemption,
            SUM(ACT_VIP_orders_with_credit_redemption)                      as act_vip_orders_with_credit_redemption,
            SUM(REPEAT_VIP_orders_with_credit_redemption)                   as repeat_vip_orders_with_credit_redemption,
            SUM(TOTAL_PRODUCT_GROSS_REVENUE)                                AS total_product_gross_revenue,
            SUM(FIRST_GUEST_PRODUCT_GROSS_REVENUE)                          as FIRST_GUEST_PRODUCT_GROSS_REVENUE,
            SUM(REPEAT_GUEST_PRODUCT_GROSS_REVENUE)                         as REPEAT_GUEST_PRODUCT_GROSS_REVENUE,
            SUM(ACT_VIP_PRODUCT_GROSS_REVENUE)                              as ACT_VIP_PRODUCT_GROSS_REVENUE,
            SUM(REPEAT_VIP_PRODUCT_GROSS_REVENUE)                           as REPEAT_VIP_PRODUCT_GROSS_REVENUE,
            SUM(cash_gross_revenue)                                         AS cash_gross_revenue,
            SUM(TOTAL_PRODUCT_NET_REVENUE)                                  AS total_product_net_revenue,
            SUM(FIRST_GUEST_PRODUCT_NET_REVENUE)                            as FIRST_GUEST_PRODUCT_NET_REVENUE,
            SUM(REPEAT_GUEST_PRODUCT_NET_REVENUE)                           as REPEAT_GUEST_PRODUCT_NET_REVENUE,
            SUM(ACT_VIP_PRODUCT_NET_REVENUE)                                as ACT_VIP_PRODUCT_NET_REVENUE,
            SUM(REPEAT_VIP_PRODUCT_NET_REVENUE)                             as REPEAT_VIP_PRODUCT_NET_REVENUE,
            SUM(cash_net_revenue)                                           AS cash_net_revenue,
            SUM(TOTAL_PRODUCT_GROSS_PROFIT)                                 AS total_PRODUCT_GROSS_PROFIT,
            SUM(FIRST_GUEST_PRODUCT_GROSS_PROFIT)                           as FIRST_GUEST_PRODUCT_GROSS_PROFIT,
            SUM(REPEAT_GUEST_PRODUCT_GROSS_PROFIT)                          as REPEAT_GUEST_PRODUCT_GROSS_PROFIT,
            SUM(ACT_VIP_PRODUCT_GROSS_PROFIT)                               as ACT_VIP_PRODUCT_GROSS_PROFIT,
            SUM(REPEAT_VIP_PRODUCT_GROSS_PROFIT)                            as REPEAT_VIP_PRODUCT_GROSS_PROFIT,
            SUM(cash_gross_profit)                                          AS cash_gross_profit,
            SUM(shipping_revenue)                                           AS shipping_revenue,
            SUM(shipping_cost)                                              AS shipping_cost,
            SUM(TOTAL_DISCOUNT)                                             AS total_DISCOUNT,
            SUM(FIRST_GUEST_DISCOUNT)                                       as FIRST_GUEST_DISCOUNT,
            SUM(REPEAT_GUEST_DISCOUNT)                                      as REPEAT_GUEST_DISCOUNT,
            SUM(ACT_VIP_DISCOUNT)                                           as ACT_VIP_DISCOUNT,
            SUM(REPEAT_VIP_DISCOUNT)                                        as REPEAT_VIP_DISCOUNT,
            SUM(TOTAL_INITIAL_RETAIL_PRICE_EXCL_VAT)                        AS total_INITIAL_RETAIL_PRICE_EXCL_VAT,
            SUM(FIRST_GUEST_INITIAL_RETAIL_PRICE_EXCL_VAT)                  as FIRST_GUEST_INITIAL_RETAIL_PRICE_EXCL_VAT,
            SUM(REPEAT_GUEST_INITIAL_RETAIL_PRICE_EXCL_VAT)                 as REPEAT_GUEST_INITIAL_RETAIL_PRICE_EXCL_VAT,
            SUM(ACT_VIP_INITIAL_RETAIL_PRICE_EXCL_VAT)                      as ACT_VIP_INITIAL_RETAIL_PRICE_EXCL_VAT,
            SUM(REPEAT_VIP_INITIAL_RETAIL_PRICE_EXCL_VAT)                   as REPEAT_VIP_INITIAL_RETAIL_PRICE_EXCL_VAT,
            SUM(TOTAL_SUBTOTAL_AMOUNT_EXCL_TARIFF)                          AS total_SUBTOTAL_AMOUNT_EXCL_TARIFF,
            SUM(FIRST_GUEST_SUBTOTAL_AMOUNT_EXCL_TARIFF)                    as FIRST_GUEST_SUBTOTAL_AMOUNT_EXCL_TARIFF,
            SUM(REPEAT_GUEST_SUBTOTAL_AMOUNT_EXCL_TARIFF)                   as REPEAT_GUEST_SUBTOTAL_AMOUNT_EXCL_TARIFF,
            SUM(ACT_VIP_SUBTOTAL_AMOUNT_EXCL_TARIFF)                        as ACT_VIP_SUBTOTAL_AMOUNT_EXCL_TARIFF,
            SUM(REPEAT_VIP_SUBTOTAL_AMOUNT_EXCL_TARIFF)                     as REPEAT_VIP_SUBTOTAL_AMOUNT_EXCL_TARIFF,
            SUM(TOTAL_PRODUCT_DISCOUNT_AMOUNT)                              AS total_PRODUCT_DISCOUNT_AMOUNT,
            SUM(FIRST_GUEST_PRODUCT_DISCOUNT_AMOUNT)                        as FIRST_GUEST_PRODUCT_DISCOUNT_AMOUNT,
            SUM(REPEAT_GUEST_PRODUCT_DISCOUNT_AMOUNT)                       as REPEAT_GUEST_PRODUCT_DISCOUNT_AMOUNT,
            SUM(ACT_VIP_PRODUCT_DISCOUNT_AMOUNT)                            as ACT_VIP_PRODUCT_DISCOUNT_AMOUNT,
            SUM(REPEAT_VIP_PRODUCT_DISCOUNT_AMOUNT)                         as REPEAT_VIP_PRODUCT_DISCOUNT_AMOUNT,
            SUM(TOTAL_PRODUCT_COST_AMOUNT)                                  AS total_product_COST_AMOUNT,
            SUM(FIRST_GUEST_PRODUCT_COST_AMOUNT)                            as FIRST_GUEST_PRODUCT_COST_AMOUNT,
            SUM(REPEAT_GUEST_PRODUCT_COST_AMOUNT)                           as REPEAT_GUEST_PRODUCT_COST_AMOUNT,
            SUM(ACT_VIP_PRODUCT_COST_AMOUNT)                                as ACT_VIP_PRODUCT_COST_AMOUNT,
            SUM(REPEAT_VIP_PRODUCT_COST_AMOUNT)                             as REPEAT_VIP_PRODUCT_COST_AMOUNT,
            SUM(TOTAL_PRODUCT_SUBTOTAL_AMOUNT)                              AS total_product_SUBTOTAL_AMOUNT,
            SUM(FIRST_GUEST_PRODUCT_SUBTOTAL_AMOUNT)                        as FIRST_GUEST_PRODUCT_SUBTOTAL_AMOUNT,
            SUM(REPEAT_GUEST_PRODUCT_SUBTOTAL_AMOUNT)                       as REPEAT_GUEST_PRODUCT_SUBTOTAL_AMOUNT,
            SUM(ACT_VIP_PRODUCT_SUBTOTAL_AMOUNT)                            as ACT_VIP_PRODUCT_SUBTOTAL_AMOUNT,
            SUM(REPEAT_VIP_PRODUCT_SUBTOTAL_AMOUNT)                         as REPEAT_VIP_PRODUCT_SUBTOTAL_AMOUNT,
            SUM(TOTAL_NON_CASH_CREDIT_AMOUNT)                               AS total_NON_CASH_CREDIT_AMOUNT,
            SUM(FIRST_GUEST_NON_CASH_CREDIT_AMOUNT)                         as FIRST_GUEST_NON_CASH_CREDIT_AMOUNT,
            SUM(REPEAT_GUEST_NON_CASH_CREDIT_AMOUNT)                        as REPEAT_GUEST_NON_CASH_CREDIT_AMOUNT,
            SUM(ACT_VIP_NON_CASH_CREDIT_AMOUNT)                             as ACT_VIP_NON_CASH_CREDIT_AMOUNT,
            SUM(REPEAT_VIP_NON_CASH_CREDIT_AMOUNT)                          as REPEAT_VIP_NON_CASH_CREDIT_AMOUNT,
            SUM(credit_redemption_amount)                                   AS credit_redemption_amount,
            SUM(credit_redemption_count)                                    AS credit_redemption_count,
            SUM(gift_card_issuance_amount)                                  AS gift_card_issuance_amount,
            SUM(credit_issuance_amount)                                     AS credit_issuance_amount,
            SUM(credit_issuance_count)                                      AS credit_issuance_count,
            SUM(gift_card_redemption_amount)                                AS gift_card_redemption_amount,
            SUM(conversion_eligible_orders)                                 AS conversion_eligible_orders,
            SUM(paygo_activations)                                          AS paygo_activations,
            SUM(product_order_cash_refund_amount)                           AS product_order_cash_refund_amount,
            SUM(product_order_cash_credit_refund_amount)                    AS product_order_cash_credit_refund_amount,
            SUM(billing_cash_refund_amount)                                 AS billing_cash_refund_amount,
            SUM(billing_cash_chargeback_amount)                             AS billing_cash_chargeback_amount,
            SUM(product_order_cash_chargeback_amount)                       AS product_order_cash_chargeback_amount,
            SUM(reship_orders)                                              AS reship_orders,
            SUM(reship_units)                                               AS reship_units,
            SUM(reship_product_cost)                                        AS reship_product_cost,
            SUM(exchange_orders)                                            AS exchange_orders,
            SUM(exchange_units)                                             AS exchange_units,
            SUM(exchange_product_cost)                                      AS exchange_product_cost,
            SUM(return_product_cost)                                        AS return_product_cost,
            SUM(return_shipping_cost)                                       AS return_shipping_cost,
            SUM(online_purchase_retail_returns)                             AS online_purchase_retail_returns,
            SUM(retail_purchase_retail_returns)                             AS retail_purchase_retail_returns,
            SUM(retail_purchase_online_returns)                             AS retail_purchase_online_returns,
            SUM(online_purchase_retail_return_orders)                       AS online_purchase_retail_return_orders,
            SUM(retail_purchase_retail_return_orders)                       AS retail_purchase_retail_return_orders,
            SUM(retail_purchase_online_return_orders)                       AS retail_purchase_online_return_orders,
            SUM(total_exchanges)                                            AS total_exchanges,
            SUM(exchanges_after_online_return)                              AS exchanges_after_online_return,
            SUM(exchanges_after_in_store_return)                            AS exchanges_after_in_store_return,
            SUM(exchanges_after_in_store_return_from_online_purchase)       AS exchanges_after_in_store_return_from_online_purchase,
            SUM(total_exchanges_value)                                      AS total_exchanges_value,
            SUM(exchanges_value_after_online_return)                        AS exchanges_value_after_online_return,
            SUM(exchanges_value_after_in_store_return)                      AS exchanges_value_after_in_store_return,
            SUM(exchanges_value_after_in_store_return_from_online_purchase) AS exchanges_value_after_in_store_return_from_online_purchase,
            SUM(incoming_traffic)                                           AS incoming_visitor_traffic,
            SUM(exiting_traffic)                                            AS exiting_visitor_traffic
     --             $execution_start_time                                           AS meta_update_time,
     --             $execution_start_time                                           AS meta_create_time
     FROM _4_wall_attribution_financial f
              FULL OUTER JOIN _weeklyrange wr ON wr.weekofyear = weekofyear(dateadd(day, 1, f.date)) AND
                                                 wr.yearofweek = yearofweek(dateadd(day, 1, f.date))
     WHERE metric_type IN ('4-Wall', 'Attribution')
       AND date >= '2018-01-01'
       AND date < current_date() + 1
     GROUP BY IS_RETAIL_SHIP_ONLY_ORDER,
              '4-Wall with Attribution',
              metric_method,
              date,
              store_id,
              longitude,
              latitude,
              store_name,
              region,
              store_zip,
              store_city,
              store_state,
              store_opening_date,
              sq_footage,
              store_number,
              CASE WHEN store_retail_type ilike 'Partnership' THEN 'Activations' ELSE store_retail_type END,
              store_type,
              comp_new);


// sq footage to below temp tables into final table
// all financials for 4-Wall, Attribution, and 4-Wall with Attribution
CREATE OR REPLACE TEMPORARY TABLE _final_metrics as
    (select metric_type,
            metric_method,
            date,
            store_id,
            store_name,
            region,
            longitude,
            latitude,
            store_zip,
            store_city,
            store_state,
            store_opening_date,
            sq_footage,
            store_number,
            store_retail_type,
            store_type,
            comp_new,
            is_retail_ship_only_order,
            total_orders,
            first_guest_orders,
            repeat_guest_orders,
            act_vip_orders,
            repeat_vip_orders,
            total_units,
            first_guest_units,
            repeat_guest_units,
            act_vip_units,
            repeat_vip_units,
            gift_card_orders,
            total_orders_with_credit_redemption,
            first_guest_orders_with_credit_redemption,
            repeat_guest_orders_with_credit_redemption,
            act_vip_orders_with_credit_redemption,
            repeat_vip_orders_with_credit_redemption,
            total_product_gross_revenue,
            first_guest_product_gross_revenue,
            repeat_guest_product_gross_revenue,
            act_vip_product_gross_revenue,
            repeat_vip_product_gross_revenue,
            cash_gross_revenue,
            total_product_net_revenue,
            first_guest_product_net_revenue,
            repeat_guest_product_net_revenue,
            act_vip_product_net_revenue,
            repeat_vip_product_net_revenue,
            cash_net_revenue,
            total_product_gross_profit,
            first_guest_product_gross_profit,
            repeat_guest_product_gross_profit,
            act_vip_product_gross_profit,
            repeat_vip_product_gross_profit,
            cash_gross_profit,
            shipping_revenue,
            shipping_cost,
            total_discount,
            first_guest_discount,
            repeat_guest_discount,
            act_vip_discount,
            repeat_vip_discount,
            total_initial_retail_price_excl_vat,
            first_guest_initial_retail_price_excl_vat,
            repeat_guest_initial_retail_price_excl_vat,
            act_vip_initial_retail_price_excl_vat,
            repeat_vip_initial_retail_price_excl_vat,
            total_subtotal_amount_excl_tariff,
            first_guest_subtotal_amount_excl_tariff,
            repeat_guest_subtotal_amount_excl_tariff,
            act_vip_subtotal_amount_excl_tariff,
            repeat_vip_subtotal_amount_excl_tariff,
            total_product_discount_amount,
            first_guest_product_discount_amount,
            repeat_guest_product_discount_amount,
            act_vip_product_discount_amount,
            repeat_vip_product_discount_amount,
            total_product_cost_amount,
            first_guest_product_cost_amount,
            repeat_guest_product_cost_amount,
            act_vip_product_cost_amount,
            repeat_vip_product_cost_amount,
            total_product_subtotal_amount,
            first_guest_product_subtotal_amount,
            repeat_guest_product_subtotal_amount,
            act_vip_product_subtotal_amount,
            repeat_vip_product_subtotal_amount,
            total_non_cash_credit_amount,
            first_guest_non_cash_credit_amount,
            repeat_guest_non_cash_credit_amount,
            act_vip_non_cash_credit_amount,
            repeat_vip_non_cash_credit_amount,
            credit_redemption_amount,
            credit_redemption_count,
            gift_card_issuance_amount,
            credit_issuance_amount,
            credit_issuance_count,
            gift_card_redemption_amount,
            conversion_eligible_orders,
            paygo_activations,
            product_order_cash_refund_amount,
            product_order_cash_credit_refund_amount,
            billing_cash_refund_amount,
            billing_cash_chargeback_amount,
            product_order_cash_chargeback_amount,
            reship_orders,
            reship_units,
            reship_product_cost,
            exchange_orders,
            exchange_units,
            exchange_product_cost,
            return_product_cost,
            return_shipping_cost,
            online_purchase_retail_returns,
            retail_purchase_retail_returns,
            retail_purchase_online_returns,
            online_purchase_retail_return_orders,
            retail_purchase_retail_return_orders,
            retail_purchase_online_return_orders,
            total_exchanges,
            exchanges_after_online_return,
            exchanges_after_in_store_return,
            exchanges_after_in_store_return_from_online_purchase,
            total_exchanges_value,
            exchanges_value_after_online_return,
            exchanges_value_after_in_store_return,
            exchanges_value_after_in_store_return_from_online_purchase,
            incoming_traffic,
            exiting_traffic
     from _4_wall_attribution_financial
     union
     select metric_type,
            metric_method,
            date,
            store_id,
            store_name,
            region,
            longitude,
            latitude,
            store_zip,
            store_city,
            store_state,
            store_opening_date,
            sq_footage,
            store_number,
            store_retail_type,
            store_type,
            comp_new,
            is_retail_ship_only_order,
            total_orders,
            first_guest_orders,
            repeat_guest_orders,
            act_vip_orders,
            repeat_vip_orders,
            total_units,
            first_guest_units,
            repeat_guest_units,
            act_vip_units,
            repeat_vip_units,
            gift_card_orders,
            total_orders_with_credit_redemption,
            first_guest_orders_with_credit_redemption,
            repeat_guest_orders_with_credit_redemption,
            act_vip_orders_with_credit_redemption,
            repeat_vip_orders_with_credit_redemption,
            total_product_gross_revenue,
            first_guest_product_gross_revenue,
            repeat_guest_product_gross_revenue,
            act_vip_product_gross_revenue,
            repeat_vip_product_gross_revenue,
            cash_gross_revenue,
            total_product_net_revenue,
            first_guest_product_net_revenue,
            repeat_guest_product_net_revenue,
            act_vip_product_net_revenue,
            repeat_vip_product_net_revenue,
            cash_net_revenue,
            total_product_gross_profit,
            first_guest_product_gross_profit,
            repeat_guest_product_gross_profit,
            act_vip_product_gross_profit,
            repeat_vip_product_gross_profit,
            cash_gross_profit,
            shipping_revenue,
            shipping_cost,
            total_discount,
            first_guest_discount,
            repeat_guest_discount,
            act_vip_discount,
            repeat_vip_discount,
            total_initial_retail_price_excl_vat,
            first_guest_initial_retail_price_excl_vat,
            repeat_guest_initial_retail_price_excl_vat,
            act_vip_initial_retail_price_excl_vat,
            repeat_vip_initial_retail_price_excl_vat,
            total_subtotal_amount_excl_tariff,
            first_guest_subtotal_amount_excl_tariff,
            repeat_guest_subtotal_amount_excl_tariff,
            act_vip_subtotal_amount_excl_tariff,
            repeat_vip_subtotal_amount_excl_tariff,
            total_product_discount_amount,
            first_guest_product_discount_amount,
            repeat_guest_product_discount_amount,
            act_vip_product_discount_amount,
            repeat_vip_product_discount_amount,
            total_product_cost_amount,
            first_guest_product_cost_amount,
            repeat_guest_product_cost_amount,
            act_vip_product_cost_amount,
            repeat_vip_product_cost_amount,
            total_product_subtotal_amount,
            first_guest_product_subtotal_amount,
            repeat_guest_product_subtotal_amount,
            act_vip_product_subtotal_amount,
            repeat_vip_product_subtotal_amount,
            total_non_cash_credit_amount,
            first_guest_non_cash_credit_amount,
            repeat_guest_non_cash_credit_amount,
            act_vip_non_cash_credit_amount,
            repeat_vip_non_cash_credit_amount,
            credit_redemption_amount,
            credit_redemption_count,
            gift_card_issuance_amount,
            credit_issuance_amount,
            credit_issuance_count,
            gift_card_redemption_amount,
            conversion_eligible_orders,
            paygo_activations,
            product_order_cash_refund_amount,
            product_order_cash_credit_refund_amount,
            billing_cash_refund_amount,
            billing_cash_chargeback_amount,
            product_order_cash_chargeback_amount,
            reship_orders,
            reship_units,
            reship_product_cost,
            exchange_orders,
            exchange_units,
            exchange_product_cost,
            return_product_cost,
            return_shipping_cost,
            online_purchase_retail_returns,
            retail_purchase_retail_returns,
            retail_purchase_online_returns,
            online_purchase_retail_return_orders,
            retail_purchase_retail_return_orders,
            retail_purchase_online_return_orders,
            total_exchanges,
            exchanges_after_online_return,
            exchanges_after_in_store_return,
            exchanges_after_in_store_return_from_online_purchase,
            total_exchanges_value,
            exchanges_value_after_online_return,
            exchanges_value_after_in_store_return,
            exchanges_value_after_in_store_return_from_online_purchase,
            incoming_visitor_traffic,
            exiting_visitor_traffic
     from _4_wall_with_attribution);

// TY and LY metrics aggregated down to day
CREATE OR REPLACE TEMPORARY TABLE _daily_final_metrics as
    (select --*
            'daily'                                                              as date_view,
            coalesce(ty.metric_type, ly.metric_type)                             as metric_type,
            coalesce(ty.metric_method, ly.metric_method)                         as metric_method,
            coalesce(ty.date, dateadd('year', 1, ly.date))                       as date,
            w.yearofweek,
            w.weekofyear,
            wr.start_date,
            wr.end_date,
            coalesce(ty.store_id, ly.store_id)                                   as store_id,
            coalesce(ty.store_name, ly.store_name)                               as store_name,
            coalesce(ty.region, ly.region)                                       as region,
            coalesce(ty.longitude, ly.longitude)                                 as longitude,
            coalesce(ty.latitude, ly.latitude)                                   as latitude,
            coalesce(ty.store_zip, ly.store_zip)                                 as store_zip,
            coalesce(ty.store_city, ly.store_city)                               as store_city,
            coalesce(ty.store_state, ly.store_state)                             as store_state,
            coalesce(ty.store_opening_date, ly.store_opening_date)               as store_opening_date,
            coalesce(ty.sq_footage, ly.sq_footage)                               as sq_footage,
            coalesce(ty.store_number, ly.store_number)                           as store_number,
            coalesce(ty.store_retail_type, ly.store_retail_type)                 as store_retail_type,
            coalesce(ty.store_type, ly.store_type)                               as store_type,
            coalesce(ty.comp_new, ly.comp_new)                                   as comp_new,
            coalesce(ty.is_retail_ship_only_order, ly.is_retail_ship_only_order) as is_retail_ship_only_order,
            coalesce(ty.total_orders, 0)                                         as orders,
            coalesce(ly.total_orders, 0)                                         as orders_ly,
            coalesce(ty.first_guest_orders, 0)                                   as first_guest_orders,
            coalesce(ly.first_guest_orders, 0)                                   as first_guest_orders_ly,
            coalesce(ty.repeat_guest_orders, 0)                                  as repeat_guest_orders,
            coalesce(ly.repeat_guest_orders, 0)                                  as repeat_guest_orders_ly,
            coalesce(ty.act_vip_orders, 0)                                       as act_vip_orders,
            coalesce(ly.act_vip_orders, 0)                                       as act_vip_orders_ly,
            coalesce(ty.repeat_vip_orders, 0)                                    as repeat_vip_orders,
            coalesce(ly.repeat_vip_orders, 0)                                    as repeat_vip_orders_ly,
            coalesce(ty.total_units, 0)                                          as units,
            coalesce(ly.total_units, 0)                                          as units_ly,
            coalesce(ty.first_guest_units, 0)                                    as first_guest_units,
            coalesce(ly.first_guest_units, 0)                                    as first_guest_units_ly,
            coalesce(ty.repeat_guest_units, 0)                                   as repeat_guest_units,
            coalesce(ly.repeat_guest_units, 0)                                   as repeat_guest_units_ly,
            coalesce(ty.act_vip_units, 0)                                        as act_vip_units,
            coalesce(ly.act_vip_units, 0)                                        as act_vip_units_ly,
            coalesce(ty.repeat_vip_units, 0)                                     as repeat_vip_units,
            coalesce(ly.repeat_vip_units, 0)                                     as repeat_vip_units_ly,
            coalesce(ty.gift_card_orders, 0)                                     as gift_card_orders,
            coalesce(ly.gift_card_orders, 0)                                     as gift_card_orders_ly,
            coalesce(ty.total_orders_with_credit_redemption, 0)                  as orders_with_credit_redemption,
            coalesce(ly.total_orders_with_credit_redemption, 0)                  as orders_with_credit_redemption_ly,
            coalesce(ty.first_guest_orders_with_credit_redemption, 0)            as first_guest_orders_with_credit_redemption,
            coalesce(ly.first_guest_orders_with_credit_redemption, 0)            as first_guest_orders_with_credit_redemption_ly,
            coalesce(ty.repeat_guest_orders_with_credit_redemption, 0)           as repeat_guest_orders_with_credit_redemption,
            coalesce(ly.repeat_guest_orders_with_credit_redemption, 0)           as repeat_guest_orders_with_credit_redemption_ly,
            coalesce(ty.act_vip_orders_with_credit_redemption, 0)                as act_vip_orders_with_credit_redemption,
            coalesce(ly.act_vip_orders_with_credit_redemption, 0)                as act_vip_orders_with_credit_redemption_ly,
            coalesce(ty.repeat_vip_orders_with_credit_redemption, 0)             as repeat_vip_orders_with_credit_redemption,
            coalesce(ly.repeat_vip_orders_with_credit_redemption, 0)             as repeat_vip_orders_with_credit_redemption_ly,
            coalesce(ty.total_product_gross_revenue, 0)                          as product_gross_revenue,
            coalesce(ly.total_product_gross_revenue, 0)                          as product_gross_revenue_ly,
            coalesce(ty.first_guest_product_gross_revenue, 0)                    as first_guest_product_gross_revenue,
            coalesce(ly.first_guest_product_gross_revenue, 0)                    as first_guest_product_gross_revenue_ly,
            coalesce(ty.repeat_guest_product_gross_revenue, 0)                   as repeat_guest_product_gross_revenue,
            coalesce(ly.repeat_guest_product_gross_revenue, 0)                   as repeat_guest_product_gross_revenue_ly,
            coalesce(ty.act_vip_product_gross_revenue, 0)                        as act_vip_product_gross_revenue,
            coalesce(ly.act_vip_product_gross_revenue, 0)                        as act_vip_product_gross_revenue_ly,
            coalesce(ty.repeat_vip_product_gross_revenue, 0)                     as repeat_vip_product_gross_revenue,
            coalesce(ly.repeat_vip_product_gross_revenue, 0)                     as repeat_vip_product_gross_revenue_ly,
            coalesce(ty.cash_gross_revenue, 0)                                   as cash_gross_revenue,
            coalesce(ly.cash_gross_revenue, 0)                                   as cash_gross_revenue_ly,
            coalesce(ty.total_product_net_revenue, 0)                            as product_net_revenue,
            coalesce(ly.total_product_net_revenue, 0)                            as product_net_revenue_ly,
            coalesce(ty.first_guest_product_net_revenue, 0)                      as first_guest_product_net_revenue,
            coalesce(ly.first_guest_product_net_revenue, 0)                      as first_guest_product_net_revenue_ly,
            coalesce(ty.repeat_guest_product_net_revenue, 0)                     as repeat_guest_product_net_revenue,
            coalesce(ly.repeat_guest_product_net_revenue, 0)                     as repeat_guest_product_net_revenue_ly,
            coalesce(ty.act_vip_product_net_revenue, 0)                          as act_vip_product_net_revenue,
            coalesce(ly.act_vip_product_net_revenue, 0)                          as act_vip_product_net_revenue_ly,
            coalesce(ty.repeat_vip_product_net_revenue, 0)                       as repeat_vip_product_net_revenue,
            coalesce(ly.repeat_vip_product_net_revenue, 0)                       as repeat_vip_product_net_revenue_ly,
            coalesce(ty.cash_net_revenue, 0)                                     as cash_net_revenue,
            coalesce(ly.cash_net_revenue, 0)                                     as cash_net_revenue_ly,
            coalesce(ty.total_product_gross_profit, 0)                           as product_gross_profit,
            coalesce(ly.total_product_gross_profit, 0)                           as product_gross_profit_ly,
            coalesce(ty.first_guest_product_gross_profit, 0)                     as first_guest_product_gross_profit,
            coalesce(ly.first_guest_product_gross_profit, 0)                     as first_guest_product_gross_profit_ly,
            coalesce(ty.repeat_guest_product_gross_profit, 0)                    as repeat_guest_product_gross_profit,
            coalesce(ly.repeat_guest_product_gross_profit, 0)                    as repeat_guest_product_gross_profit_ly,
            coalesce(ty.act_vip_product_gross_profit, 0)                         as act_vip_product_gross_profit,
            coalesce(ly.act_vip_product_gross_profit, 0)                         as act_vip_product_gross_profit_ly,
            coalesce(ty.repeat_vip_product_gross_profit, 0)                      as repeat_vip_product_gross_profit,
            coalesce(ly.repeat_vip_product_gross_profit, 0)                      as repeat_vip_product_gross_profit_ly,
            coalesce(ty.cash_gross_profit, 0)                                    as cash_gross_profit,
            coalesce(ly.cash_gross_profit, 0)                                    as cash_gross_profit_ly,
            coalesce(ty.shipping_revenue, 0)                                     as shipping_revenue,
            coalesce(ly.shipping_revenue, 0)                                     as shipping_revenue_ly,
            coalesce(ty.shipping_cost, 0)                                        as shipping_cost,
            coalesce(ly.shipping_cost, 0)                                        as shipping_cost_ly,
            coalesce(ty.total_discount, 0)                                       as discount,
            coalesce(ly.total_discount, 0)                                       as discount_ly,
            coalesce(ty.first_guest_discount, 0)                                 as first_guest_discount,
            coalesce(ly.first_guest_discount, 0)                                 as first_guest_discount_ly,
            coalesce(ty.repeat_guest_discount, 0)                                as repeat_guest_discount,
            coalesce(ly.repeat_guest_discount, 0)                                as repeat_guest_discount_ly,
            coalesce(ty.act_vip_discount, 0)                                     as act_vip_discount,
            coalesce(ly.act_vip_discount, 0)                                     as act_vip_discount_ly,
            coalesce(ty.repeat_vip_discount, 0)                                  as repeat_vip_discount,
            coalesce(ly.repeat_vip_discount, 0)                                  as repeat_vip_discount_ly,
            coalesce(ty.total_initial_retail_price_excl_vat, 0)                  as initial_retail_price_excl_vat,
            coalesce(ly.total_initial_retail_price_excl_vat, 0)                  as initial_retail_price_excl_vat_ly,
            coalesce(ty.first_guest_initial_retail_price_excl_vat, 0)            as first_guest_initial_retail_price_excl_vat,
            coalesce(ly.first_guest_initial_retail_price_excl_vat, 0)            as first_guest_initial_retail_price_excl_vat_ly,
            coalesce(ty.repeat_guest_initial_retail_price_excl_vat, 0)           as repeat_guest_initial_retail_price_excl_vat,
            coalesce(ly.repeat_guest_initial_retail_price_excl_vat, 0)           as repeat_guest_initial_retail_price_excl_vat_ly,
            coalesce(ty.act_vip_initial_retail_price_excl_vat, 0)                as act_vip_initial_retail_price_excl_vat,
            coalesce(ly.act_vip_initial_retail_price_excl_vat, 0)                as act_vip_initial_retail_price_excl_vat_ly,
            coalesce(ty.repeat_vip_initial_retail_price_excl_vat, 0)             as repeat_vip_initial_retail_price_excl_vat,
            coalesce(ly.repeat_vip_initial_retail_price_excl_vat, 0)             as repeat_vip_initial_retail_price_excl_vat_ly,
            coalesce(ty.total_subtotal_amount_excl_tariff, 0)                    as subtotal_amount_excl_tariff,
            coalesce(ly.total_subtotal_amount_excl_tariff, 0)                    as subtotal_amount_excl_tariff_ly,
            coalesce(ty.first_guest_subtotal_amount_excl_tariff, 0)              as first_guest_subtotal_amount_excl_tariff,
            coalesce(ly.first_guest_subtotal_amount_excl_tariff, 0)              as first_guest_subtotal_amount_excl_tariff_ly,
            coalesce(ty.repeat_guest_subtotal_amount_excl_tariff, 0)             as repeat_guest_subtotal_amount_excl_tariff,
            coalesce(ly.repeat_guest_subtotal_amount_excl_tariff, 0)             as repeat_guest_subtotal_amount_excl_tariff_ly,
            coalesce(ty.act_vip_subtotal_amount_excl_tariff, 0)                  as act_vip_subtotal_amount_excl_tariff,
            coalesce(ly.act_vip_subtotal_amount_excl_tariff, 0)                  as act_vip_subtotal_amount_excl_tariff_ly,
            coalesce(ty.repeat_vip_subtotal_amount_excl_tariff, 0)               as repeat_vip_subtotal_amount_excl_tariff,
            coalesce(ly.repeat_vip_subtotal_amount_excl_tariff, 0)               as repeat_vip_subtotal_amount_excl_tariff_ly,
            coalesce(ty.total_product_discount_amount, 0)                        as product_discount_amount,
            coalesce(ly.total_product_discount_amount, 0)                        as product_discount_amount_ly,
            coalesce(ty.first_guest_product_discount_amount, 0)                  as first_guest_product_discount_amount,
            coalesce(ly.first_guest_product_discount_amount, 0)                  as first_guest_product_discount_amount_ly,
            coalesce(ty.repeat_guest_product_discount_amount, 0)                 as repeat_guest_product_discount_amount,
            coalesce(ly.repeat_guest_product_discount_amount, 0)                 as repeat_guest_product_discount_amount_ly,
            coalesce(ty.act_vip_product_discount_amount, 0)                      as act_vip_product_discount_amount,
            coalesce(ly.act_vip_product_discount_amount, 0)                      as act_vip_product_discount_amount_ly,
            coalesce(ty.repeat_vip_product_discount_amount, 0)                   as repeat_vip_product_discount_amount,
            coalesce(ly.repeat_vip_product_discount_amount, 0)                   as repeat_vip_product_discount_amount_ly,
            coalesce(ty.total_product_cost_amount, 0)                            as product_cost_amount,
            coalesce(ly.total_product_cost_amount, 0)                            as product_cost_amount_ly,
            coalesce(ty.first_guest_product_cost_amount, 0)                      as first_guest_product_cost_amount,
            coalesce(ly.first_guest_product_cost_amount, 0)                      as first_guest_product_cost_amount_ly,
            coalesce(ty.repeat_guest_product_cost_amount, 0)                     as repeat_guest_product_cost_amount,
            coalesce(ly.repeat_guest_product_cost_amount, 0)                     as repeat_guest_product_cost_amount_ly,
            coalesce(ty.act_vip_product_cost_amount, 0)                          as act_vip_product_cost_amount,
            coalesce(ly.act_vip_product_cost_amount, 0)                          as act_vip_product_cost_amount_ly,
            coalesce(ty.repeat_vip_product_cost_amount, 0)                       as repeat_vip_product_cost_amount,
            coalesce(ly.repeat_vip_product_cost_amount, 0)                       as repeat_vip_product_cost_amount_ly,
            coalesce(ty.total_product_subtotal_amount, 0)                        as product_subtotal_amount,
            coalesce(ly.total_product_subtotal_amount, 0)                        as product_subtotal_amount_ly,
            coalesce(ty.first_guest_product_subtotal_amount, 0)                  as first_guest_product_subtotal_amount,
            coalesce(ly.first_guest_product_subtotal_amount, 0)                  as first_guest_product_subtotal_amount_ly,
            coalesce(ty.repeat_guest_product_subtotal_amount, 0)                 as repeat_guest_product_subtotal_amount,
            coalesce(ly.repeat_guest_product_subtotal_amount, 0)                 as repeat_guest_product_subtotal_amount_ly,
            coalesce(ty.act_vip_product_subtotal_amount, 0)                      as act_vip_product_subtotal_amount,
            coalesce(ly.act_vip_product_subtotal_amount, 0)                      as act_vip_product_subtotal_amount_ly,
            coalesce(ty.repeat_vip_product_subtotal_amount, 0)                   as repeat_vip_product_subtotal_amount,
            coalesce(ly.repeat_vip_product_subtotal_amount, 0)                   as repeat_vip_product_subtotal_amount_ly,
            coalesce(ty.total_non_cash_credit_amount, 0)                         as non_cash_credit_amount,
            coalesce(ly.total_non_cash_credit_amount, 0)                         as non_cash_credit_amount_ly,
            coalesce(ty.first_guest_non_cash_credit_amount, 0)                   as first_guest_non_cash_credit_amount,
            coalesce(ly.first_guest_non_cash_credit_amount, 0)                   as first_guest_non_cash_credit_amount_ly,
            coalesce(ty.repeat_guest_non_cash_credit_amount, 0)                  as repeat_guest_non_cash_credit_amount,
            coalesce(ly.repeat_guest_non_cash_credit_amount, 0)                  as repeat_guest_non_cash_credit_amount_ly,
            coalesce(ty.act_vip_non_cash_credit_amount, 0)                       as act_vip_non_cash_credit_amount,
            coalesce(ly.act_vip_non_cash_credit_amount, 0)                       as act_vip_non_cash_credit_amount_ly,
            coalesce(ty.repeat_vip_non_cash_credit_amount, 0)                    as repeat_vip_non_cash_credit_amount,
            coalesce(ly.repeat_vip_non_cash_credit_amount, 0)                    as repeat_vip_non_cash_credit_amount_ly,
            coalesce(ty.credit_redemption_amount, 0)                             as credit_redemption_amount,
            coalesce(ly.credit_redemption_amount, 0)                             as credit_redemption_amount_ly,
            coalesce(ty.credit_redemption_count, 0)                              as credit_redemption_count,
            coalesce(ly.credit_redemption_count, 0)                              as credit_redemption_count_ly,
            coalesce(ty.gift_card_issuance_amount, 0)                            as gift_card_issuance_amount,
            coalesce(ly.gift_card_issuance_amount, 0)                            as gift_card_issuance_amount_ly,
            coalesce(ty.credit_issuance_amount, 0)                               as credit_issuance_amount,
            coalesce(ly.credit_issuance_amount, 0)                               as credit_issuance_amount_ly,
            coalesce(ty.credit_issuance_count, 0)                                as credit_issuance_count,
            coalesce(ly.credit_issuance_count, 0)                                as credit_issuance_count_ly,
            coalesce(ty.gift_card_redemption_amount, 0)                          as gift_card_redemption_amount,
            coalesce(ly.gift_card_redemption_amount, 0)                          as gift_card_redemption_amount_ly,
            coalesce(ty.conversion_eligible_orders, 0)                           as conversion_eligible_orders,
            coalesce(ly.conversion_eligible_orders, 0)                           as conversion_eligible_orders_ly,
            coalesce(ty.paygo_activations, 0)                                    as paygo_activations,
            coalesce(ly.paygo_activations, 0)                                    as paygo_activations_ly,
            coalesce(ty.product_order_cash_refund_amount, 0)                     as product_order_cash_refund_amount,
            coalesce(ly.product_order_cash_refund_amount, 0)                     as product_order_cash_refund_amount_ly,
            coalesce(ty.product_order_cash_credit_refund_amount, 0)              as product_order_cash_credit_refund_amount,
            coalesce(ly.product_order_cash_credit_refund_amount, 0)              as product_order_cash_credit_refund_amount_ly,
            coalesce(ty.billing_cash_refund_amount, 0)                           as billing_cash_refund_amount,
            coalesce(ly.billing_cash_refund_amount, 0)                           as billing_cash_refund_amount_ly,
            coalesce(ty.billing_cash_chargeback_amount, 0)                       as billing_cash_chargeback_amount,
            coalesce(ly.billing_cash_chargeback_amount, 0)                       as billing_cash_chargeback_amount_ly,
            coalesce(ty.product_order_cash_chargeback_amount, 0)                 as product_order_cash_chargeback_amount,
            coalesce(ly.product_order_cash_chargeback_amount, 0)                 as product_order_cash_chargeback_amount_ly,
            coalesce(ty.reship_orders, 0)                                        as reship_orders,
            coalesce(ly.reship_orders, 0)                                        as reship_orders_ly,
            coalesce(ty.reship_units, 0)                                         as reship_units,
            coalesce(ly.reship_units, 0)                                         as reship_units_ly,
            coalesce(ty.reship_product_cost, 0)                                  as reship_product_cost,
            coalesce(ly.reship_product_cost, 0)                                  as reship_product_cost_ly,
            coalesce(ty.exchange_orders, 0)                                      as exchange_orders,
            coalesce(ly.exchange_orders, 0)                                      as exchange_orders_ly,
            coalesce(ty.exchange_units, 0)                                       as exchange_units,
            coalesce(ly.exchange_units, 0)                                       as exchange_units_ly,
            coalesce(ty.exchange_product_cost, 0)                                as exchange_product_cost,
            coalesce(ly.exchange_product_cost, 0)                                as exchange_product_cost_ly,
            coalesce(ty.return_product_cost, 0)                                  as return_product_cost,
            coalesce(ly.return_product_cost, 0)                                  as return_product_cost_ly,
            coalesce(ty.return_shipping_cost, 0)                                 as return_shipping_cost,
            coalesce(ly.return_shipping_cost, 0)                                 as return_shipping_cost_ly,
            coalesce(ty.online_purchase_retail_returns, 0)                       as online_purchase_retail_returns,
            coalesce(ly.online_purchase_retail_returns, 0)                       as online_purchase_retail_returns_ly,
            coalesce(ty.retail_purchase_retail_returns, 0)                       as retail_purchase_retail_returns,
            coalesce(ly.retail_purchase_retail_returns, 0)                       as retail_purchase_retail_returns_ly,
            coalesce(ty.retail_purchase_online_returns, 0)                       as retail_purchase_online_returns,
            coalesce(ly.retail_purchase_online_returns, 0)                       as retail_purchase_online_returns_ly,
            coalesce(ty.online_purchase_retail_return_orders, 0)                 as online_purchase_retail_return_orders,
            coalesce(ly.online_purchase_retail_return_orders, 0)                 as online_purchase_retail_return_orders_ly,
            coalesce(ty.retail_purchase_retail_return_orders, 0)                 as retail_purchase_retail_return_orders,
            coalesce(ly.retail_purchase_retail_return_orders, 0)                 as retail_purchase_retail_return_orders_ly,
            coalesce(ty.retail_purchase_online_return_orders, 0)                 as retail_purchase_online_return_orders,
            coalesce(ly.retail_purchase_online_return_orders, 0)                 as retail_purchase_online_return_orders_ly,
            coalesce(ty.total_exchanges, 0)                                      as total_exchanges,
            coalesce(ly.total_exchanges, 0)                                      as total_exchanges_ly,
            coalesce(ty.exchanges_after_online_return, 0)                        as exchanges_after_online_return,
            coalesce(ly.exchanges_after_online_return, 0)                        as exchanges_after_online_return_ly,
            coalesce(ty.exchanges_after_in_store_return, 0)                      as exchanges_after_in_store_return,
            coalesce(ly.exchanges_after_in_store_return, 0)                      as exchanges_after_in_store_return_ly,
            coalesce(ty.exchanges_after_in_store_return_from_online_purchase, 0) as exchanges_after_in_store_return_from_online_purchase,
            coalesce(ly.exchanges_after_in_store_return_from_online_purchase, 0) as exchanges_after_in_store_return_from_online_purchase_ly,
            coalesce(ty.total_exchanges_value, 0)                                as total_exchanges_value,
            coalesce(ly.total_exchanges_value, 0)                                as total_exchanges_value_ly,
            coalesce(ty.exchanges_value_after_online_return, 0)                  as exchanges_value_after_online_return,
            coalesce(ly.exchanges_value_after_online_return, 0)                  as exchanges_value_after_online_return_ly,
            coalesce(ty.exchanges_value_after_in_store_return, 0)                as exchanges_value_after_in_store_return,
            coalesce(ly.exchanges_value_after_in_store_return, 0)                as exchanges_value_after_in_store_return_ly,
            coalesce(ty.exchanges_value_after_in_store_return_from_online_purchase,
                     0)                                                          as exchanges_value_after_in_store_return_from_online_purchase,
            coalesce(ly.exchanges_value_after_in_store_return_from_online_purchase,
                     0)                                                          as exchanges_value_after_in_store_return_from_online_purchase_ly,
            coalesce(ty.incoming_traffic, 0)                                     as incoming_visitor_traffic,
            coalesce(ly.incoming_traffic, 0)                                     as incoming_visitor_traffic_ly,
            coalesce(ty.exiting_traffic, 0)                                      as exiting_visitor_traffic,
            coalesce(ly.exiting_traffic, 0)                                      as exiting_visitor_traffic_ly
     from _final_metrics ty
              full outer join _final_metrics ly on ty.date = dateadd('year', 1, ly.date)
         and ty.store_id = ly.store_id and ty.metric_type = ly.metric_type
              left join _weekly_date_view w on w.date = ty.date
              left join _weeklyrange wr on w.weekofyear = wr.weekofyear and w.yearofweek = wr.yearofweek
     where ty.date <= dateadd('day', -1, current_date));

// TY metrics aggregated down to week
CREATE OR REPLACE TEMPORARY TABLE _weekly_metrics as
    (select 'weekly'                                                                    as date_view,
            metric_type,
            metric_method,
            null                                                                        as date,
            yearofweek,
            weekofyear,
            start_date,
            end_date,
            store_id,
            store_name,
            region,
            longitude,
            latitude,
            store_zip,
            store_city,
            store_state,
            store_opening_date,
            sq_footage,
            store_number,
            store_retail_type,
            store_type,
            iff(datediff('month', store_opening_date, start_date) >= 13, 'comp', 'new') as comp_new,
            is_retail_ship_only_order,
            sum(orders)                                                                 as orders,
            sum(first_guest_orders)                                                     as first_guest_orders,
            sum(repeat_guest_orders)                                                    as repeat_guest_orders,
            sum(act_vip_orders)                                                         as act_vip_orders,
            sum(repeat_vip_orders)                                                      as repeat_vip_orders,
            sum(units)                                                                  as units,
            sum(first_guest_units)                                                      as first_guest_units,
            sum(repeat_guest_units)                                                     as repeat_guest_units,
            sum(act_vip_units)                                                          as act_vip_units,
            sum(repeat_vip_units)                                                       as repeat_vip_units,
            sum(gift_card_orders)                                                       as gift_card_orders,
            sum(orders_with_credit_redemption)                                          as orders_with_credit_redemption,
            sum(first_guest_orders_with_credit_redemption)                              as first_guest_orders_with_credit_redemption,
            sum(repeat_guest_orders_with_credit_redemption)                             as repeat_guest_orders_with_credit_redemption,
            sum(act_vip_orders_with_credit_redemption)                                  as act_vip_orders_with_credit_redemption,
            sum(repeat_vip_orders_with_credit_redemption)                               as repeat_vip_orders_with_credit_redemption,
            sum(product_gross_revenue)                                                  as product_gross_revenue,
            sum(first_guest_product_gross_revenue)                                      as first_guest_product_gross_revenue,
            sum(repeat_guest_product_gross_revenue)                                     as repeat_guest_product_gross_revenue,
            sum(act_vip_product_gross_revenue)                                          as act_vip_product_gross_revenue,
            sum(repeat_vip_product_gross_revenue)                                       as repeat_vip_product_gross_revenue,
            sum(cash_gross_revenue)                                                     as cash_gross_revenue,
            sum(product_net_revenue)                                                    as product_net_revenue,
            sum(first_guest_product_net_revenue)                                        as first_guest_product_net_revenue,
            sum(repeat_guest_product_net_revenue)                                       as repeat_guest_product_net_revenue,
            sum(act_vip_product_net_revenue)                                            as act_vip_product_net_revenue,
            sum(repeat_vip_product_net_revenue)                                         as repeat_vip_product_net_revenue,
            sum(cash_net_revenue)                                                       as cash_net_revenue,
            sum(product_gross_profit)                                                   as product_gross_profit,
            sum(first_guest_product_gross_profit)                                       as first_guest_product_gross_profit,
            sum(repeat_guest_product_gross_profit)                                      as repeat_guest_product_gross_profit,
            sum(act_vip_product_gross_profit)                                           as act_vip_product_gross_profit,
            sum(repeat_vip_product_gross_profit)                                        as repeat_vip_product_gross_profit,
            sum(cash_gross_profit)                                                      as cash_gross_profit,
            sum(shipping_revenue)                                                       as shipping_revenue,
            sum(shipping_cost)                                                          as shipping_cost,
            sum(discount)                                                               as discount,
            sum(first_guest_discount)                                                   as first_guest_discount,
            sum(repeat_guest_discount)                                                  as repeat_guest_discount,
            sum(act_vip_discount)                                                       as act_vip_discount,
            sum(repeat_vip_discount)                                                    as repeat_vip_discount,
            sum(initial_retail_price_excl_vat)                                          as initial_retail_price_excl_vat,
            sum(first_guest_initial_retail_price_excl_vat)                              as first_guest_initial_retail_price_excl_vat,
            sum(repeat_guest_initial_retail_price_excl_vat)                             as repeat_guest_initial_retail_price_excl_vat,
            sum(act_vip_initial_retail_price_excl_vat)                                  as act_vip_initial_retail_price_excl_vat,
            sum(repeat_vip_initial_retail_price_excl_vat)                               as repeat_vip_initial_retail_price_excl_vat,
            sum(subtotal_amount_excl_tariff)                                            as subtotal_amount_excl_tariff,
            sum(first_guest_subtotal_amount_excl_tariff)                                as first_guest_subtotal_amount_excl_tariff,
            sum(repeat_guest_subtotal_amount_excl_tariff)                               as repeat_guest_subtotal_amount_excl_tariff,
            sum(act_vip_subtotal_amount_excl_tariff)                                    as act_vip_subtotal_amount_excl_tariff,
            sum(repeat_vip_subtotal_amount_excl_tariff)                                 as repeat_vip_subtotal_amount_excl_tariff,
            sum(product_discount_amount)                                                as product_discount_amount,
            sum(first_guest_product_discount_amount)                                    as first_guest_product_discount_amount,
            sum(repeat_guest_product_discount_amount)                                   as repeat_guest_product_discount_amount,
            sum(act_vip_product_discount_amount)                                        as act_vip_product_discount_amount,
            sum(repeat_vip_product_discount_amount)                                     as repeat_vip_product_discount_amount,
            sum(product_cost_amount)                                                    as product_cost_amount,
            sum(first_guest_product_cost_amount)                                        as first_guest_product_cost_amount,
            sum(repeat_guest_product_cost_amount)                                       as repeat_guest_product_cost_amount,
            sum(act_vip_product_cost_amount)                                            as act_vip_product_cost_amount,
            sum(repeat_vip_product_cost_amount)                                         as repeat_vip_product_cost_amount,
            sum(product_subtotal_amount)                                                as product_subtotal_amount,
            sum(first_guest_product_subtotal_amount)                                    as first_guest_product_subtotal_amount,
            sum(repeat_guest_product_subtotal_amount)                                   as repeat_guest_product_subtotal_amount,
            sum(act_vip_product_subtotal_amount)                                        as act_vip_product_subtotal_amount,
            sum(repeat_vip_product_subtotal_amount)                                     as repeat_vip_product_subtotal_amount,
            sum(non_cash_credit_amount)                                                 as non_cash_credit_amount,
            sum(first_guest_non_cash_credit_amount)                                     as first_guest_non_cash_credit_amount,
            sum(repeat_guest_non_cash_credit_amount)                                    as repeat_guest_non_cash_credit_amount,
            sum(act_vip_non_cash_credit_amount)                                         as act_vip_non_cash_credit_amount,
            sum(repeat_vip_non_cash_credit_amount)                                      as repeat_vip_non_cash_credit_amount,
            sum(credit_redemption_amount)                                               as credit_redemption_amount,
            sum(credit_redemption_count)                                                as credit_redemption_count,
            sum(gift_card_issuance_amount)                                              as gift_card_issuance_amount,
            sum(credit_issuance_amount)                                                 as credit_issuance_amount,
            sum(credit_issuance_count)                                                  as credit_issuance_count,
            sum(gift_card_redemption_amount)                                            as gift_card_redemption_amount,
            sum(conversion_eligible_orders)                                             as conversion_eligible_orders,
            sum(paygo_activations)                                                      as paygo_activations,
            sum(product_order_cash_refund_amount)                                       as product_order_cash_refund_amount,
            sum(product_order_cash_credit_refund_amount)                                as product_order_cash_credit_refund_amount,
            sum(billing_cash_refund_amount)                                             as billing_cash_refund_amount,
            sum(billing_cash_chargeback_amount)                                         as billing_cash_chargeback_amount,
            sum(product_order_cash_chargeback_amount)                                   as product_order_cash_chargeback_amount,
            sum(reship_orders)                                                          as reship_orders,
            sum(reship_units)                                                           as reship_units,
            sum(reship_product_cost)                                                    as reship_product_cost,
            sum(exchange_orders)                                                        as exchange_orders,
            sum(exchange_units)                                                         as exchange_units,
            sum(exchange_product_cost)                                                  as exchange_product_cost,
            sum(return_product_cost)                                                    as return_product_cost,
            sum(return_shipping_cost)                                                   as return_shipping_cost,
            sum(online_purchase_retail_returns)                                         as online_purchase_retail_returns,
            sum(retail_purchase_retail_returns)                                         as retail_purchase_retail_returns,
            sum(retail_purchase_online_returns)                                         as retail_purchase_online_returns,
            sum(online_purchase_retail_return_orders)                                   as online_purchase_retail_return_orders,
            sum(retail_purchase_retail_return_orders)                                   as retail_purchase_retail_return_orders,
            sum(retail_purchase_online_return_orders)                                   as retail_purchase_online_return_orders,
            sum(total_exchanges)                                                        as total_exchanges,
            sum(exchanges_after_online_return)                                          as exchanges_after_online_return,
            sum(exchanges_after_in_store_return)                                        as exchanges_after_in_store_return,
            sum(exchanges_after_in_store_return_from_online_purchase)                   as exchanges_after_in_store_return_from_online_purchase,
            sum(total_exchanges_value)                                                  as total_exchanges_value,
            sum(exchanges_value_after_online_return)                                    as exchanges_value_after_online_return,
            sum(exchanges_value_after_in_store_return)                                  as exchanges_value_after_in_store_return,
            sum(exchanges_value_after_in_store_return_from_online_purchase)             as exchanges_value_after_in_store_return_from_online_purchase,
            sum(incoming_visitor_traffic)                                               as incoming_visitor_traffic,
            sum(exiting_visitor_traffic)                                                as exiting_visitor_traffic
     from _daily_final_metrics
     group by metric_type, metric_method, null, 'weekly', yearofweek, weekofyear, start_date, end_date, store_id,
              store_name, region,
              longitude, latitude, store_zip, store_city, store_state, store_opening_date, sq_footage, store_number,
              store_retail_type, store_type,
              iff(datediff('month', store_opening_date, start_date) >= 13, 'comp', 'new'), is_retail_ship_only_order);

// TY and LY metrics aggregated down to week
CREATE OR REPLACE TEMPORARY TABLE _weekly_final_metrics as
    (select coalesce(ty.date_view, ly.date_view)                                 as date_view,
            coalesce(ty.metric_type, ly.metric_type)                             as metric_type,
            coalesce(ty.metric_method, ly.metric_method)                         as metric_method,
            coalesce(ty.date, ly.date)                                           as date,
            coalesce(ty.yearofweek, ly.yearofweek + 1)                           as yearofweek,
            coalesce(ty.weekofyear, ly.weekofyear)                               as weekofyear,
            coalesce(ty.start_date, ly.start_date)                               as start_date,
            coalesce(ty.end_date, ly.end_date)                                   as end_date,
            coalesce(ty.store_id, ly.store_id)                                   as store_id,
            coalesce(ty.store_name, ly.store_name)                               as store_name,
            coalesce(ty.region, ly.region)                                       as region,
            coalesce(ty.longitude, ly.longitude)                                 as longitude,
            coalesce(ty.latitude, ly.latitude)                                   as latitude,
            coalesce(ty.store_zip, ly.store_zip)                                 as store_zip,
            coalesce(ty.store_city, ly.store_city)                               as store_city,
            coalesce(ty.store_state, ly.store_state)                             as store_state,
            coalesce(ty.store_opening_date, ly.store_opening_date)               as store_opening_date,
            coalesce(ty.sq_footage, ly.sq_footage)                               as sq_footage,
            coalesce(ty.store_number, ly.store_number)                           as store_number,
            coalesce(ty.store_retail_type, ly.store_retail_type)                 as store_retail_type,
            coalesce(ty.store_type, ly.store_type)                               as store_type,
            coalesce(ty.comp_new, ly.comp_new)                                   as comp_new,
            coalesce(ty.is_retail_ship_only_order, ly.is_retail_ship_only_order) as is_retail_ship_only_order,
            coalesce(ty.orders, 0)                                               as orders,
            coalesce(ly.orders, 0)                                               as orders_ly,
            coalesce(ty.first_guest_orders, 0)                                   as first_guest_orders,
            coalesce(ly.first_guest_orders, 0)                                   as first_guest_orders_ly,
            coalesce(ty.repeat_guest_orders, 0)                                  as repeat_guest_orders,
            coalesce(ly.repeat_guest_orders, 0)                                  as repeat_guest_orders_ly,
            coalesce(ty.act_vip_orders, 0)                                       as act_vip_orders,
            coalesce(ly.act_vip_orders, 0)                                       as act_vip_orders_ly,
            coalesce(ty.repeat_vip_orders, 0)                                    as repeat_vip_orders,
            coalesce(ly.repeat_vip_orders, 0)                                    as repeat_vip_orders_ly,
            coalesce(ty.units, 0)                                                as units,
            coalesce(ly.units, 0)                                                as units_ly,
            coalesce(ty.first_guest_units, 0)                                    as first_guest_units,
            coalesce(ly.first_guest_units, 0)                                    as first_guest_units_ly,
            coalesce(ty.repeat_guest_units, 0)                                   as repeat_guest_units,
            coalesce(ly.repeat_guest_units, 0)                                   as repeat_guest_units_ly,
            coalesce(ty.act_vip_units, 0)                                        as act_vip_units,
            coalesce(ly.act_vip_units, 0)                                        as act_vip_units_ly,
            coalesce(ty.repeat_vip_units, 0)                                     as repeat_vip_units,
            coalesce(ly.repeat_vip_units, 0)                                     as repeat_vip_units_ly,
            coalesce(ty.gift_card_orders, 0)                                     as gift_card_orders,
            coalesce(ly.gift_card_orders, 0)                                     as gift_card_orders_ly,
            coalesce(ty.orders_with_credit_redemption, 0)                        as orders_with_credit_redemption,
            coalesce(ly.orders_with_credit_redemption, 0)                        as orders_with_credit_redemption_ly,
            coalesce(ty.first_guest_orders_with_credit_redemption, 0)            as first_guest_orders_with_credit_redemption,
            coalesce(ly.first_guest_orders_with_credit_redemption, 0)            as first_guest_orders_with_credit_redemption_ly,
            coalesce(ty.repeat_guest_orders_with_credit_redemption, 0)           as repeat_guest_orders_with_credit_redemption,
            coalesce(ly.repeat_guest_orders_with_credit_redemption, 0)           as repeat_guest_orders_with_credit_redemption_ly,
            coalesce(ty.act_vip_orders_with_credit_redemption, 0)                as act_vip_orders_with_credit_redemption,
            coalesce(ly.act_vip_orders_with_credit_redemption, 0)                as act_vip_orders_with_credit_redemption_ly,
            coalesce(ty.repeat_vip_orders_with_credit_redemption, 0)             as repeat_vip_orders_with_credit_redemption,
            coalesce(ly.repeat_vip_orders_with_credit_redemption, 0)             as repeat_vip_orders_with_credit_redemption_ly,
            coalesce(ty.product_gross_revenue, 0)                                as product_gross_revenue,
            coalesce(ly.product_gross_revenue, 0)                                as product_gross_revenue_ly,
            coalesce(ty.first_guest_product_gross_revenue, 0)                    as first_guest_product_gross_revenue,
            coalesce(ly.first_guest_product_gross_revenue, 0)                    as first_guest_product_gross_revenue_ly,
            coalesce(ty.repeat_guest_product_gross_revenue, 0)                   as repeat_guest_product_gross_revenue,
            coalesce(ly.repeat_guest_product_gross_revenue, 0)                   as repeat_guest_product_gross_revenue_ly,
            coalesce(ty.act_vip_product_gross_revenue, 0)                        as act_vip_product_gross_revenue,
            coalesce(ly.act_vip_product_gross_revenue, 0)                        as act_vip_product_gross_revenue_ly,
            coalesce(ty.repeat_vip_product_gross_revenue, 0)                     as repeat_vip_product_gross_revenue,
            coalesce(ly.repeat_vip_product_gross_revenue, 0)                     as repeat_vip_product_gross_revenue_ly,
            coalesce(ty.cash_gross_revenue, 0)                                   as cash_gross_revenue,
            coalesce(ly.cash_gross_revenue, 0)                                   as cash_gross_revenue_ly,
            coalesce(ty.product_net_revenue, 0)                                  as product_net_revenue,
            coalesce(ly.product_net_revenue, 0)                                  as product_net_revenue_ly,
            coalesce(ty.first_guest_product_net_revenue, 0)                      as first_guest_product_net_revenue,
            coalesce(ly.first_guest_product_net_revenue, 0)                      as first_guest_product_net_revenue_ly,
            coalesce(ty.repeat_guest_product_net_revenue, 0)                     as repeat_guest_product_net_revenue,
            coalesce(ly.repeat_guest_product_net_revenue, 0)                     as repeat_guest_product_net_revenue_ly,
            coalesce(ty.act_vip_product_net_revenue, 0)                          as act_vip_product_net_revenue,
            coalesce(ly.act_vip_product_net_revenue, 0)                          as act_vip_product_net_revenue_ly,
            coalesce(ty.repeat_vip_product_net_revenue, 0)                       as repeat_vip_product_net_revenue,
            coalesce(ly.repeat_vip_product_net_revenue, 0)                       as repeat_vip_product_net_revenue_ly,
            coalesce(ty.cash_net_revenue, 0)                                     as cash_net_revenue,
            coalesce(ly.cash_net_revenue, 0)                                     as cash_net_revenue_ly,
            coalesce(ty.product_gross_profit, 0)                                 as product_gross_profit,
            coalesce(ly.product_gross_profit, 0)                                 as product_gross_profit_ly,
            coalesce(ty.first_guest_product_gross_profit, 0)                     as first_guest_product_gross_profit,
            coalesce(ly.first_guest_product_gross_profit, 0)                     as first_guest_product_gross_profit_ly,
            coalesce(ty.repeat_guest_product_gross_profit, 0)                    as repeat_guest_product_gross_profit,
            coalesce(ly.repeat_guest_product_gross_profit, 0)                    as repeat_guest_product_gross_profit_ly,
            coalesce(ty.act_vip_product_gross_profit, 0)                         as act_vip_product_gross_profit,
            coalesce(ly.act_vip_product_gross_profit, 0)                         as act_vip_product_gross_profit_ly,
            coalesce(ty.repeat_vip_product_gross_profit, 0)                      as repeat_vip_product_gross_profit,
            coalesce(ly.repeat_vip_product_gross_profit, 0)                      as repeat_vip_product_gross_profit_ly,
            coalesce(ty.cash_gross_profit, 0)                                    as cash_gross_profit,
            coalesce(ly.cash_gross_profit, 0)                                    as cash_gross_profit_ly,
            coalesce(ty.shipping_revenue, 0)                                     as shipping_revenue,
            coalesce(ly.shipping_revenue, 0)                                     as shipping_revenue_ly,
            coalesce(ty.shipping_cost, 0)                                        as shipping_cost,
            coalesce(ly.shipping_cost, 0)                                        as shipping_cost_ly,
            coalesce(ty.discount, 0)                                             as discount,
            coalesce(ly.discount, 0)                                             as discount_ly,
            coalesce(ty.first_guest_discount, 0)                                 as first_guest_discount,
            coalesce(ly.first_guest_discount, 0)                                 as first_guest_discount_ly,
            coalesce(ty.repeat_guest_discount, 0)                                as repeat_guest_discount,
            coalesce(ly.repeat_guest_discount, 0)                                as repeat_guest_discount_ly,
            coalesce(ty.act_vip_discount, 0)                                     as act_vip_discount,
            coalesce(ly.act_vip_discount, 0)                                     as act_vip_discount_ly,
            coalesce(ty.repeat_vip_discount, 0)                                  as repeat_vip_discount,
            coalesce(ly.repeat_vip_discount, 0)                                  as repeat_vip_discount_ly,
            coalesce(ty.initial_retail_price_excl_vat, 0)                        as initial_retail_price_excl_vat,
            coalesce(ly.initial_retail_price_excl_vat, 0)                        as initial_retail_price_excl_vat_ly,
            coalesce(ty.first_guest_initial_retail_price_excl_vat, 0)            as first_guest_initial_retail_price_excl_vat,
            coalesce(ly.first_guest_initial_retail_price_excl_vat, 0)            as first_guest_initial_retail_price_excl_vat_ly,
            coalesce(ty.repeat_guest_initial_retail_price_excl_vat, 0)           as repeat_guest_initial_retail_price_excl_vat,
            coalesce(ly.repeat_guest_initial_retail_price_excl_vat, 0)           as repeat_guest_initial_retail_price_excl_vat_ly,
            coalesce(ty.act_vip_initial_retail_price_excl_vat, 0)                as act_vip_initial_retail_price_excl_vat,
            coalesce(ly.act_vip_initial_retail_price_excl_vat, 0)                as act_vip_initial_retail_price_excl_vat_ly,
            coalesce(ty.repeat_vip_initial_retail_price_excl_vat, 0)             as repeat_vip_initial_retail_price_excl_vat,
            coalesce(ly.repeat_vip_initial_retail_price_excl_vat, 0)             as repeat_vip_initial_retail_price_excl_vat_ly,
            coalesce(ty.subtotal_amount_excl_tariff, 0)                          as subtotal_amount_excl_tariff,
            coalesce(ly.subtotal_amount_excl_tariff, 0)                          as subtotal_amount_excl_tariff_ly,
            coalesce(ty.first_guest_subtotal_amount_excl_tariff, 0)              as first_guest_subtotal_amount_excl_tariff,
            coalesce(ly.first_guest_subtotal_amount_excl_tariff, 0)              as first_guest_subtotal_amount_excl_tariff_ly,
            coalesce(ty.repeat_guest_subtotal_amount_excl_tariff, 0)             as repeat_guest_subtotal_amount_excl_tariff,
            coalesce(ly.repeat_guest_subtotal_amount_excl_tariff, 0)             as repeat_guest_subtotal_amount_excl_tariff_ly,
            coalesce(ty.act_vip_subtotal_amount_excl_tariff, 0)                  as act_vip_subtotal_amount_excl_tariff,
            coalesce(ly.act_vip_subtotal_amount_excl_tariff, 0)                  as act_vip_subtotal_amount_excl_tariff_ly,
            coalesce(ty.repeat_vip_subtotal_amount_excl_tariff, 0)               as repeat_vip_subtotal_amount_excl_tariff,
            coalesce(ly.repeat_vip_subtotal_amount_excl_tariff, 0)               as repeat_vip_subtotal_amount_excl_tariff_ly,
            coalesce(ty.product_discount_amount, 0)                              as product_discount_amount,
            coalesce(ly.product_discount_amount, 0)                              as product_discount_amount_ly,
            coalesce(ty.first_guest_product_discount_amount, 0)                  as first_guest_product_discount_amount,
            coalesce(ly.first_guest_product_discount_amount, 0)                  as first_guest_product_discount_amount_ly,
            coalesce(ty.repeat_guest_product_discount_amount, 0)                 as repeat_guest_product_discount_amount,
            coalesce(ly.repeat_guest_product_discount_amount, 0)                 as repeat_guest_product_discount_amount_ly,
            coalesce(ty.act_vip_product_discount_amount, 0)                      as act_vip_product_discount_amount,
            coalesce(ly.act_vip_product_discount_amount, 0)                      as act_vip_product_discount_amount_ly,
            coalesce(ty.repeat_vip_product_discount_amount, 0)                   as repeat_vip_product_discount_amount,
            coalesce(ly.repeat_vip_product_discount_amount, 0)                   as repeat_vip_product_discount_amount_ly,
            coalesce(ty.product_cost_amount, 0)                                  as product_cost_amount,
            coalesce(ly.product_cost_amount, 0)                                  as product_cost_amount_ly,
            coalesce(ty.first_guest_product_cost_amount, 0)                      as first_guest_product_cost_amount,
            coalesce(ly.first_guest_product_cost_amount, 0)                      as first_guest_product_cost_amount_ly,
            coalesce(ty.repeat_guest_product_cost_amount, 0)                     as repeat_guest_product_cost_amount,
            coalesce(ly.repeat_guest_product_cost_amount, 0)                     as repeat_guest_product_cost_amount_ly,
            coalesce(ty.act_vip_product_cost_amount, 0)                          as act_vip_product_cost_amount,
            coalesce(ly.act_vip_product_cost_amount, 0)                          as act_vip_product_cost_amount_ly,
            coalesce(ty.repeat_vip_product_cost_amount, 0)                       as repeat_vip_product_cost_amount,
            coalesce(ly.repeat_vip_product_cost_amount, 0)                       as repeat_vip_product_cost_amount_ly,
            coalesce(ty.product_subtotal_amount, 0)                              as product_subtotal_amount,
            coalesce(ly.product_subtotal_amount, 0)                              as product_subtotal_amount_ly,
            coalesce(ty.first_guest_product_subtotal_amount, 0)                  as first_guest_product_subtotal_amount,
            coalesce(ly.first_guest_product_subtotal_amount, 0)                  as first_guest_product_subtotal_amount_ly,
            coalesce(ty.repeat_guest_product_subtotal_amount, 0)                 as repeat_guest_product_subtotal_amount,
            coalesce(ly.repeat_guest_product_subtotal_amount, 0)                 as repeat_guest_product_subtotal_amount_ly,
            coalesce(ty.act_vip_product_subtotal_amount, 0)                      as act_vip_product_subtotal_amount,
            coalesce(ly.act_vip_product_subtotal_amount, 0)                      as act_vip_product_subtotal_amount_ly,
            coalesce(ty.repeat_vip_product_subtotal_amount, 0)                   as repeat_vip_product_subtotal_amount,
            coalesce(ly.repeat_vip_product_subtotal_amount, 0)                   as repeat_vip_product_subtotal_amount_ly,
            coalesce(ty.non_cash_credit_amount, 0)                               as non_cash_credit_amount,
            coalesce(ly.non_cash_credit_amount, 0)                               as non_cash_credit_amount_ly,
            coalesce(ty.first_guest_non_cash_credit_amount, 0)                   as first_guest_non_cash_credit_amount,
            coalesce(ly.first_guest_non_cash_credit_amount, 0)                   as first_guest_non_cash_credit_amount_ly,
            coalesce(ty.repeat_guest_non_cash_credit_amount, 0)                  as repeat_guest_non_cash_credit_amount,
            coalesce(ly.repeat_guest_non_cash_credit_amount, 0)                  as repeat_guest_non_cash_credit_amount_ly,
            coalesce(ty.act_vip_non_cash_credit_amount, 0)                       as act_vip_non_cash_credit_amount,
            coalesce(ly.act_vip_non_cash_credit_amount, 0)                       as act_vip_non_cash_credit_amount_ly,
            coalesce(ty.repeat_vip_non_cash_credit_amount, 0)                    as repeat_vip_non_cash_credit_amount,
            coalesce(ly.repeat_vip_non_cash_credit_amount, 0)                    as repeat_vip_non_cash_credit_amount_ly,
            coalesce(ty.credit_redemption_amount, 0)                             as credit_redemption_amount,
            coalesce(ly.credit_redemption_amount, 0)                             as credit_redemption_amount_ly,
            coalesce(ty.credit_redemption_count, 0)                              as credit_redemption_count,
            coalesce(ly.credit_redemption_count, 0)                              as credit_redemption_count_ly,
            coalesce(ty.gift_card_issuance_amount, 0)                            as gift_card_issuance_amount,
            coalesce(ly.gift_card_issuance_amount, 0)                            as gift_card_issuance_amount_ly,
            coalesce(ty.credit_issuance_amount, 0)                               as credit_issuance_amount,
            coalesce(ly.credit_issuance_amount, 0)                               as credit_issuance_amount_ly,
            coalesce(ty.credit_issuance_count, 0)                                as credit_issuance_count,
            coalesce(ly.credit_issuance_count, 0)                                as credit_issuance_count_ly,
            coalesce(ty.gift_card_redemption_amount, 0)                          as gift_card_redemption_amount,
            coalesce(ly.gift_card_redemption_amount, 0)                          as gift_card_redemption_amount_ly,
            coalesce(ty.conversion_eligible_orders, 0)                           as conversion_eligible_orders,
            coalesce(ly.conversion_eligible_orders, 0)                           as conversion_eligible_orders_ly,
            coalesce(ty.paygo_activations, 0)                                    as paygo_activations,
            coalesce(ly.paygo_activations, 0)                                    as paygo_activations_ly,
            coalesce(ty.product_order_cash_refund_amount, 0)                     as product_order_cash_refund_amount,
            coalesce(ly.product_order_cash_refund_amount, 0)                     as product_order_cash_refund_amount_ly,
            coalesce(ty.product_order_cash_credit_refund_amount, 0)              as product_order_cash_credit_refund_amount,
            coalesce(ly.product_order_cash_credit_refund_amount, 0)              as product_order_cash_credit_refund_amount_ly,
            coalesce(ty.billing_cash_refund_amount, 0)                           as billing_cash_refund_amount,
            coalesce(ly.billing_cash_refund_amount, 0)                           as billing_cash_refund_amount_ly,
            coalesce(ty.billing_cash_chargeback_amount, 0)                       as billing_cash_chargeback_amount,
            coalesce(ly.billing_cash_chargeback_amount, 0)                       as billing_cash_chargeback_amount_ly,
            coalesce(ty.product_order_cash_chargeback_amount, 0)                 as product_order_cash_chargeback_amount,
            coalesce(ly.product_order_cash_chargeback_amount, 0)                 as product_order_cash_chargeback_amount_ly,
            coalesce(ty.reship_orders, 0)                                        as reship_orders,
            coalesce(ly.reship_orders, 0)                                        as reship_orders_ly,
            coalesce(ty.reship_units, 0)                                         as reship_units,
            coalesce(ly.reship_units, 0)                                         as reship_units_ly,
            coalesce(ty.reship_product_cost, 0)                                  as reship_product_cost,
            coalesce(ly.reship_product_cost, 0)                                  as reship_product_cost_ly,
            coalesce(ty.exchange_orders, 0)                                      as exchange_orders,
            coalesce(ly.exchange_orders, 0)                                      as exchange_orders_ly,
            coalesce(ty.exchange_units, 0)                                       as exchange_units,
            coalesce(ly.exchange_units, 0)                                       as exchange_units_ly,
            coalesce(ty.exchange_product_cost, 0)                                as exchange_product_cost,
            coalesce(ly.exchange_product_cost, 0)                                as exchange_product_cost_ly,
            coalesce(ty.return_product_cost, 0)                                  as return_product_cost,
            coalesce(ly.return_product_cost, 0)                                  as return_product_cost_ly,
            coalesce(ty.return_shipping_cost, 0)                                 as return_shipping_cost,
            coalesce(ly.return_shipping_cost, 0)                                 as return_shipping_cost_ly,
            coalesce(ty.online_purchase_retail_returns, 0)                       as online_purchase_retail_returns,
            coalesce(ly.online_purchase_retail_returns, 0)                       as online_purchase_retail_returns_ly,
            coalesce(ty.retail_purchase_retail_returns, 0)                       as retail_purchase_retail_returns,
            coalesce(ly.retail_purchase_retail_returns, 0)                       as retail_purchase_retail_returns_ly,
            coalesce(ty.retail_purchase_online_returns, 0)                       as retail_purchase_online_returns,
            coalesce(ly.retail_purchase_online_returns, 0)                       as retail_purchase_online_returns_ly,
            coalesce(ty.online_purchase_retail_return_orders, 0)                 as online_purchase_retail_return_orders,
            coalesce(ly.online_purchase_retail_return_orders, 0)                 as online_purchase_retail_return_orders_ly,
            coalesce(ty.retail_purchase_retail_return_orders, 0)                 as retail_purchase_retail_return_orders,
            coalesce(ly.retail_purchase_retail_return_orders, 0)                 as retail_purchase_retail_return_orders_ly,
            coalesce(ty.retail_purchase_online_return_orders, 0)                 as retail_purchase_online_return_orders,
            coalesce(ly.retail_purchase_online_return_orders, 0)                 as retail_purchase_online_return_orders_ly,
            coalesce(ty.total_exchanges, 0)                                      as total_exchanges,
            coalesce(ly.total_exchanges, 0)                                      as total_exchanges_ly,
            coalesce(ty.exchanges_after_online_return, 0)                        as exchanges_after_online_return,
            coalesce(ly.exchanges_after_online_return, 0)                        as exchanges_after_online_return_ly,
            coalesce(ty.exchanges_after_in_store_return, 0)                      as exchanges_after_in_store_return,
            coalesce(ly.exchanges_after_in_store_return, 0)                      as exchanges_after_in_store_return_ly,
            coalesce(ty.exchanges_after_in_store_return_from_online_purchase, 0) as exchanges_after_in_store_return_from_online_purchase,
            coalesce(ly.exchanges_after_in_store_return_from_online_purchase, 0) as exchanges_after_in_store_return_from_online_purchase_ly,
            coalesce(ty.total_exchanges_value, 0)                                as total_exchanges_value,
            coalesce(ly.total_exchanges_value, 0)                                as total_exchanges_value_ly,
            coalesce(ty.exchanges_value_after_online_return, 0)                  as exchanges_value_after_online_return,
            coalesce(ly.exchanges_value_after_online_return, 0)                  as exchanges_value_after_online_return_ly,
            coalesce(ty.exchanges_value_after_in_store_return, 0)                as exchanges_value_after_in_store_return,
            coalesce(ly.exchanges_value_after_in_store_return, 0)                as exchanges_value_after_in_store_return_ly,
            coalesce(ty.exchanges_value_after_in_store_return_from_online_purchase,
                     0)                                                          as exchanges_value_after_in_store_return_from_online_purchase,
            coalesce(ly.exchanges_value_after_in_store_return_from_online_purchase,
                     0)                                                          as exchanges_value_after_in_store_return_from_online_purchase_ly,
            coalesce(ty.incoming_visitor_traffic, 0)                             as incoming_visitor_traffic,
            coalesce(ly.incoming_visitor_traffic, 0)                             as incoming_visitor_traffic_ly,
            coalesce(ty.exiting_visitor_traffic, 0)                              as exiting_visitor_traffic,
            coalesce(ly.exiting_visitor_traffic, 0)                              as exiting_visitor_traffic_ly
     from _weekly_metrics ty
              full outer join _weekly_metrics ly on ty.store_id = ly.store_id
         and ty.metric_type = ly.metric_type
         and ty.weekofyear = ly.weekofyear
         and ty.yearofweek = ly.yearofweek + 1
     where ty.end_date <= dateadd('day', -1, current_date));

TRUNCATE TABLE reporting_prod.sxf.retail_attribution;

// all daily and weekly metrics unioned together and inserted into table
INSERT INTO reporting_prod.sxf.retail_attribution
(date_view,
 metric_type,
 metric_method,
 date,
 yearofweek,
 weekofyear,
 start_date,
 end_date,
 store_id,
 store_name,
 region,
 longitude,
 latitude,
 store_zip,
 store_city,
 store_state,
 store_opening_date,
 sq_footage,
 store_number,
 store_retail_type,
 store_type,
 comp_new,
 is_retail_ship_only_order,
 orders,
 orders_ly,
 first_guest_orders,
 first_guest_orders_ly,
 repeat_guest_orders,
 repeat_guest_orders_ly,
 act_vip_orders,
 act_vip_orders_ly,
 repeat_vip_orders,
 repeat_vip_orders_ly,
 units,
 units_ly,
 first_guest_units,
 first_guest_units_ly,
 repeat_guest_units,
 repeat_guest_units_ly,
 act_vip_units,
 act_vip_units_ly,
 repeat_vip_units,
 repeat_vip_units_ly,
 gift_card_orders,
 gift_card_orders_ly,
 orders_with_credit_redemption,
 orders_with_credit_redemption_ly,
 first_guest_orders_with_credit_redemption,
 first_guest_orders_with_credit_redemption_ly,
 repeat_guest_orders_with_credit_redemption,
 repeat_guest_orders_with_credit_redemption_ly,
 act_vip_orders_with_credit_redemption,
 act_vip_orders_with_credit_redemption_ly,
 repeat_vip_orders_with_credit_redemption,
 repeat_vip_orders_with_credit_redemption_ly,
 product_gross_revenue,
 product_gross_revenue_ly,
 first_guest_product_gross_revenue,
 first_guest_product_gross_revenue_ly,
 repeat_guest_product_gross_revenue,
 repeat_guest_product_gross_revenue_ly,
 act_vip_product_gross_revenue,
 act_vip_product_gross_revenue_ly,
 repeat_vip_product_gross_revenue,
 repeat_vip_product_gross_revenue_ly,
 cash_gross_revenue,
 cash_gross_revenue_ly,
 product_net_revenue,
 product_net_revenue_ly,
 first_guest_product_net_revenue,
 first_guest_product_net_revenue_ly,
 repeat_guest_product_net_revenue,
 repeat_guest_product_net_revenue_ly,
 act_vip_product_net_revenue,
 act_vip_product_net_revenue_ly,
 repeat_vip_product_net_revenue,
 repeat_vip_product_net_revenue_ly,
 cash_net_revenue,
 cash_net_revenue_ly,
 product_gross_profit,
 product_gross_profit_ly,
 first_guest_product_gross_profit,
 first_guest_product_gross_profit_ly,
 repeat_guest_product_gross_profit,
 repeat_guest_product_gross_profit_ly,
 act_vip_product_gross_profit,
 act_vip_product_gross_profit_ly,
 repeat_vip_product_gross_profit,
 repeat_vip_product_gross_profit_ly,
 cash_gross_profit,
 cash_gross_profit_ly,
 shipping_revenue,
 shipping_revenue_ly,
 shipping_cost,
 shipping_cost_ly,
 discount,
 discount_ly,
 first_guest_discount,
 first_guest_discount_ly,
 repeat_guest_discount,
 repeat_guest_discount_ly,
 act_vip_discount,
 act_vip_discount_ly,
 repeat_vip_discount,
 repeat_vip_discount_ly,
 initial_retail_price_excl_vat,
 initial_retail_price_excl_vat_ly,
 first_guest_initial_retail_price_excl_vat,
 first_guest_initial_retail_price_excl_vat_ly,
 repeat_guest_initial_retail_price_excl_vat,
 repeat_guest_initial_retail_price_excl_vat_ly,
 act_vip_initial_retail_price_excl_vat,
 act_vip_initial_retail_price_excl_vat_ly,
 repeat_vip_initial_retail_price_excl_vat,
 repeat_vip_initial_retail_price_excl_vat_ly,
 subtotal_amount_excl_tariff,
 subtotal_amount_excl_tariff_ly,
 first_guest_subtotal_amount_excl_tariff,
 first_guest_subtotal_amount_excl_tariff_ly,
 repeat_guest_subtotal_amount_excl_tariff,
 repeat_guest_subtotal_amount_excl_tariff_ly,
 act_vip_subtotal_amount_excl_tariff,
 act_vip_subtotal_amount_excl_tariff_ly,
 repeat_vip_subtotal_amount_excl_tariff,
 repeat_vip_subtotal_amount_excl_tariff_ly,
 product_discount_amount,
 product_discount_amount_ly,
 first_guest_product_discount_amount,
 first_guest_product_discount_amount_ly,
 repeat_guest_product_discount_amount,
 repeat_guest_product_discount_amount_ly,
 act_vip_product_discount_amount,
 act_vip_product_discount_amount_ly,
 repeat_vip_product_discount_amount,
 repeat_vip_product_discount_amount_ly,
 product_cost_amount,
 product_cost_amount_ly,
 first_guest_product_cost_amount,
 first_guest_product_cost_amount_ly,
 repeat_guest_product_cost_amount,
 repeat_guest_product_cost_amount_ly,
 act_vip_product_cost_amount,
 act_vip_product_cost_amount_ly,
 repeat_vip_product_cost_amount,
 repeat_vip_product_cost_amount_ly,
 product_subtotal_amount,
 product_subtotal_amount_ly,
 first_guest_product_subtotal_amount,
 first_guest_product_subtotal_amount_ly,
 repeat_guest_product_subtotal_amount,
 repeat_guest_product_subtotal_amount_ly,
 act_vip_product_subtotal_amount,
 act_vip_product_subtotal_amount_ly,
 repeat_vip_product_subtotal_amount,
 repeat_vip_product_subtotal_amount_ly,
 non_cash_credit_amount,
 non_cash_credit_amount_ly,
 first_guest_non_cash_credit_amount,
 first_guest_non_cash_credit_amount_ly,
 repeat_guest_non_cash_credit_amount,
 repeat_guest_non_cash_credit_amount_ly,
 act_vip_non_cash_credit_amount,
 act_vip_non_cash_credit_amount_ly,
 repeat_vip_non_cash_credit_amount,
 repeat_vip_non_cash_credit_amount_ly,
 credit_redemption_amount,
 credit_redemption_amount_ly,
 credit_redemption_count,
 credit_redemption_count_ly,
 gift_card_issuance_amount,
 gift_card_issuance_amount_ly,
 credit_issuance_amount,
 credit_issuance_amount_ly,
 credit_issuance_count,
 credit_issuance_count_ly,
 gift_card_redemption_amount,
 gift_card_redemption_amount_ly,
 conversion_eligible_orders,
 conversion_eligible_orders_ly,
 paygo_activations,
 paygo_activations_ly,
 product_order_cash_refund_amount,
 product_order_cash_refund_amount_ly,
 product_order_cash_credit_refund_amount,
 product_order_cash_credit_refund_amount_ly,
 billing_cash_refund_amount,
 billing_cash_refund_amount_ly,
 billing_cash_chargeback_amount,
 billing_cash_chargeback_amount_ly,
 product_order_cash_chargeback_amount,
 product_order_cash_chargeback_amount_ly,
 reship_orders,
 reship_orders_ly,
 reship_units,
 reship_units_ly,
 reship_product_cost,
 reship_product_cost_ly,
 exchange_orders,
 exchange_orders_ly,
 exchange_units,
 exchange_units_ly,
 exchange_product_cost,
 exchange_product_cost_ly,
 return_product_cost,
 return_product_cost_ly,
 return_shipping_cost,
 return_shipping_cost_ly,
 online_purchase_retail_returns,
 online_purchase_retail_returns_ly,
 retail_purchase_retail_returns,
 retail_purchase_retail_returns_ly,
 retail_purchase_online_returns,
 retail_purchase_online_returns_ly,
 online_purchase_retail_return_orders,
 online_purchase_retail_return_orders_ly,
 retail_purchase_retail_return_orders,
 retail_purchase_retail_return_orders_ly,
 retail_purchase_online_return_orders,
 retail_purchase_online_return_orders_ly,
 total_exchanges,
 total_exchanges_ly,
 exchanges_after_online_return,
 exchanges_after_online_return_ly,
 exchanges_after_in_store_return,
 exchanges_after_in_store_return_ly,
 exchanges_after_in_store_return_from_online_purchase,
 exchanges_after_in_store_return_from_online_purchase_ly,
 total_exchanges_value,
 total_exchanges_value_ly,
 exchanges_value_after_online_return,
 exchanges_value_after_online_return_ly,
 exchanges_value_after_in_store_return,
 exchanges_value_after_in_store_return_ly,
 exchanges_value_after_in_store_return_from_online_purchase,
 exchanges_value_after_in_store_return_from_online_purchase_ly,
 incoming_visitor_traffic,
 incoming_visitor_traffic_ly,
 exiting_visitor_traffic,
 exiting_visitor_traffic_ly,
 meta_update_datetime,
 meta_create_datetime)
select date_view,
       metric_type,
       metric_method,
       date,
       yearofweek,
       weekofyear,
       start_date,
       end_date,
       store_id,
       store_name,
       region,
       longitude,
       latitude,
       store_zip,
       store_city,
       store_state,
       store_opening_date,
       sq_footage,
       store_number,
       store_retail_type,
       store_type,
       comp_new,
       is_retail_ship_only_order,
       orders,
       orders_ly,
       first_guest_orders,
       first_guest_orders_ly,
       repeat_guest_orders,
       repeat_guest_orders_ly,
       act_vip_orders,
       act_vip_orders_ly,
       repeat_vip_orders,
       repeat_vip_orders_ly,
       units,
       units_ly,
       first_guest_units,
       first_guest_units_ly,
       repeat_guest_units,
       repeat_guest_units_ly,
       act_vip_units,
       act_vip_units_ly,
       repeat_vip_units,
       repeat_vip_units_ly,
       gift_card_orders,
       gift_card_orders_ly,
       orders_with_credit_redemption,
       orders_with_credit_redemption_ly,
       first_guest_orders_with_credit_redemption,
       first_guest_orders_with_credit_redemption_ly,
       repeat_guest_orders_with_credit_redemption,
       repeat_guest_orders_with_credit_redemption_ly,
       act_vip_orders_with_credit_redemption,
       act_vip_orders_with_credit_redemption_ly,
       repeat_vip_orders_with_credit_redemption,
       repeat_vip_orders_with_credit_redemption_ly,
       product_gross_revenue,
       product_gross_revenue_ly,
       first_guest_product_gross_revenue,
       first_guest_product_gross_revenue_ly,
       repeat_guest_product_gross_revenue,
       repeat_guest_product_gross_revenue_ly,
       act_vip_product_gross_revenue,
       act_vip_product_gross_revenue_ly,
       repeat_vip_product_gross_revenue,
       repeat_vip_product_gross_revenue_ly,
       cash_gross_revenue,
       cash_gross_revenue_ly,
       product_net_revenue,
       product_net_revenue_ly,
       first_guest_product_net_revenue,
       first_guest_product_net_revenue_ly,
       repeat_guest_product_net_revenue,
       repeat_guest_product_net_revenue_ly,
       act_vip_product_net_revenue,
       act_vip_product_net_revenue_ly,
       repeat_vip_product_net_revenue,
       repeat_vip_product_net_revenue_ly,
       cash_net_revenue,
       cash_net_revenue_ly,
       product_gross_profit,
       product_gross_profit_ly,
       first_guest_product_gross_profit,
       first_guest_product_gross_profit_ly,
       repeat_guest_product_gross_profit,
       repeat_guest_product_gross_profit_ly,
       act_vip_product_gross_profit,
       act_vip_product_gross_profit_ly,
       repeat_vip_product_gross_profit,
       repeat_vip_product_gross_profit_ly,
       cash_gross_profit,
       cash_gross_profit_ly,
       shipping_revenue,
       shipping_revenue_ly,
       shipping_cost,
       shipping_cost_ly,
       discount,
       discount_ly,
       first_guest_discount,
       first_guest_discount_ly,
       repeat_guest_discount,
       repeat_guest_discount_ly,
       act_vip_discount,
       act_vip_discount_ly,
       repeat_vip_discount,
       repeat_vip_discount_ly,
       initial_retail_price_excl_vat,
       initial_retail_price_excl_vat_ly,
       first_guest_initial_retail_price_excl_vat,
       first_guest_initial_retail_price_excl_vat_ly,
       repeat_guest_initial_retail_price_excl_vat,
       repeat_guest_initial_retail_price_excl_vat_ly,
       act_vip_initial_retail_price_excl_vat,
       act_vip_initial_retail_price_excl_vat_ly,
       repeat_vip_initial_retail_price_excl_vat,
       repeat_vip_initial_retail_price_excl_vat_ly,
       subtotal_amount_excl_tariff,
       subtotal_amount_excl_tariff_ly,
       first_guest_subtotal_amount_excl_tariff,
       first_guest_subtotal_amount_excl_tariff_ly,
       repeat_guest_subtotal_amount_excl_tariff,
       repeat_guest_subtotal_amount_excl_tariff_ly,
       act_vip_subtotal_amount_excl_tariff,
       act_vip_subtotal_amount_excl_tariff_ly,
       repeat_vip_subtotal_amount_excl_tariff,
       repeat_vip_subtotal_amount_excl_tariff_ly,
       product_discount_amount,
       product_discount_amount_ly,
       first_guest_product_discount_amount,
       first_guest_product_discount_amount_ly,
       repeat_guest_product_discount_amount,
       repeat_guest_product_discount_amount_ly,
       act_vip_product_discount_amount,
       act_vip_product_discount_amount_ly,
       repeat_vip_product_discount_amount,
       repeat_vip_product_discount_amount_ly,
       product_cost_amount,
       product_cost_amount_ly,
       first_guest_product_cost_amount,
       first_guest_product_cost_amount_ly,
       repeat_guest_product_cost_amount,
       repeat_guest_product_cost_amount_ly,
       act_vip_product_cost_amount,
       act_vip_product_cost_amount_ly,
       repeat_vip_product_cost_amount,
       repeat_vip_product_cost_amount_ly,
       product_subtotal_amount,
       product_subtotal_amount_ly,
       first_guest_product_subtotal_amount,
       first_guest_product_subtotal_amount_ly,
       repeat_guest_product_subtotal_amount,
       repeat_guest_product_subtotal_amount_ly,
       act_vip_product_subtotal_amount,
       act_vip_product_subtotal_amount_ly,
       repeat_vip_product_subtotal_amount,
       repeat_vip_product_subtotal_amount_ly,
       non_cash_credit_amount,
       non_cash_credit_amount_ly,
       first_guest_non_cash_credit_amount,
       first_guest_non_cash_credit_amount_ly,
       repeat_guest_non_cash_credit_amount,
       repeat_guest_non_cash_credit_amount_ly,
       act_vip_non_cash_credit_amount,
       act_vip_non_cash_credit_amount_ly,
       repeat_vip_non_cash_credit_amount,
       repeat_vip_non_cash_credit_amount_ly,
       credit_redemption_amount,
       credit_redemption_amount_ly,
       credit_redemption_count,
       credit_redemption_count_ly,
       gift_card_issuance_amount,
       gift_card_issuance_amount_ly,
       credit_issuance_amount,
       credit_issuance_amount_ly,
       credit_issuance_count,
       credit_issuance_count_ly,
       gift_card_redemption_amount,
       gift_card_redemption_amount_ly,
       conversion_eligible_orders,
       conversion_eligible_orders_ly,
       paygo_activations,
       paygo_activations_ly,
       product_order_cash_refund_amount,
       product_order_cash_refund_amount_ly,
       product_order_cash_credit_refund_amount,
       product_order_cash_credit_refund_amount_ly,
       billing_cash_refund_amount,
       billing_cash_refund_amount_ly,
       billing_cash_chargeback_amount,
       billing_cash_chargeback_amount_ly,
       product_order_cash_chargeback_amount,
       product_order_cash_chargeback_amount_ly,
       reship_orders,
       reship_orders_ly,
       reship_units,
       reship_units_ly,
       reship_product_cost,
       reship_product_cost_ly,
       exchange_orders,
       exchange_orders_ly,
       exchange_units,
       exchange_units_ly,
       exchange_product_cost,
       exchange_product_cost_ly,
       return_product_cost,
       return_product_cost_ly,
       return_shipping_cost,
       return_shipping_cost_ly,
       online_purchase_retail_returns,
       online_purchase_retail_returns_ly,
       retail_purchase_retail_returns,
       retail_purchase_retail_returns_ly,
       retail_purchase_online_returns,
       retail_purchase_online_returns_ly,
       online_purchase_retail_return_orders,
       online_purchase_retail_return_orders_ly,
       retail_purchase_retail_return_orders,
       retail_purchase_retail_return_orders_ly,
       retail_purchase_online_return_orders,
       retail_purchase_online_return_orders_ly,
       total_exchanges,
       total_exchanges_ly,
       exchanges_after_online_return,
       exchanges_after_online_return_ly,
       exchanges_after_in_store_return,
       exchanges_after_in_store_return_ly,
       exchanges_after_in_store_return_from_online_purchase,
       exchanges_after_in_store_return_from_online_purchase_ly,
       total_exchanges_value,
       total_exchanges_value_ly,
       exchanges_value_after_online_return,
       exchanges_value_after_online_return_ly,
       exchanges_value_after_in_store_return,
       exchanges_value_after_in_store_return_ly,
       exchanges_value_after_in_store_return_from_online_purchase,
       exchanges_value_after_in_store_return_from_online_purchase_ly,
       incoming_visitor_traffic,
       incoming_visitor_traffic_ly,
       exiting_visitor_traffic,
       exiting_visitor_traffic_ly,
       $execution_start_time :: TIMESTAMP_LTZ(3) AS meta_update_datetime,
       $execution_start_time :: TIMESTAMP_LTZ(3) AS meta_create_datetime
from _daily_final_metrics
union
select date_view,
       metric_type,
       metric_method,
       date,
       yearofweek,
       weekofyear,
       start_date,
       end_date,
       store_id,
       store_name,
       region,
       longitude,
       latitude,
       store_zip,
       store_city,
       store_state,
       store_opening_date,
       sq_footage,
       store_number,
       store_retail_type,
       store_type,
       comp_new,
       is_retail_ship_only_order,
       orders,
       orders_ly,
       first_guest_orders,
       first_guest_orders_ly,
       repeat_guest_orders,
       repeat_guest_orders_ly,
       act_vip_orders,
       act_vip_orders_ly,
       repeat_vip_orders,
       repeat_vip_orders_ly,
       units,
       units_ly,
       first_guest_units,
       first_guest_units_ly,
       repeat_guest_units,
       repeat_guest_units_ly,
       act_vip_units,
       act_vip_units_ly,
       repeat_vip_units,
       repeat_vip_units_ly,
       gift_card_orders,
       gift_card_orders_ly,
       orders_with_credit_redemption,
       orders_with_credit_redemption_ly,
       first_guest_orders_with_credit_redemption,
       first_guest_orders_with_credit_redemption_ly,
       repeat_guest_orders_with_credit_redemption,
       repeat_guest_orders_with_credit_redemption_ly,
       act_vip_orders_with_credit_redemption,
       act_vip_orders_with_credit_redemption_ly,
       repeat_vip_orders_with_credit_redemption,
       repeat_vip_orders_with_credit_redemption_ly,
       product_gross_revenue,
       product_gross_revenue_ly,
       first_guest_product_gross_revenue,
       first_guest_product_gross_revenue_ly,
       repeat_guest_product_gross_revenue,
       repeat_guest_product_gross_revenue_ly,
       act_vip_product_gross_revenue,
       act_vip_product_gross_revenue_ly,
       repeat_vip_product_gross_revenue,
       repeat_vip_product_gross_revenue_ly,
       cash_gross_revenue,
       cash_gross_revenue_ly,
       product_net_revenue,
       product_net_revenue_ly,
       first_guest_product_net_revenue,
       first_guest_product_net_revenue_ly,
       repeat_guest_product_net_revenue,
       repeat_guest_product_net_revenue_ly,
       act_vip_product_net_revenue,
       act_vip_product_net_revenue_ly,
       repeat_vip_product_net_revenue,
       repeat_vip_product_net_revenue_ly,
       cash_net_revenue,
       cash_net_revenue_ly,
       product_gross_profit,
       product_gross_profit_ly,
       first_guest_product_gross_profit,
       first_guest_product_gross_profit_ly,
       repeat_guest_product_gross_profit,
       repeat_guest_product_gross_profit_ly,
       act_vip_product_gross_profit,
       act_vip_product_gross_profit_ly,
       repeat_vip_product_gross_profit,
       repeat_vip_product_gross_profit_ly,
       cash_gross_profit,
       cash_gross_profit_ly,
       shipping_revenue,
       shipping_revenue_ly,
       shipping_cost,
       shipping_cost_ly,
       discount,
       discount_ly,
       first_guest_discount,
       first_guest_discount_ly,
       repeat_guest_discount,
       repeat_guest_discount_ly,
       act_vip_discount,
       act_vip_discount_ly,
       repeat_vip_discount,
       repeat_vip_discount_ly,
       initial_retail_price_excl_vat,
       initial_retail_price_excl_vat_ly,
       first_guest_initial_retail_price_excl_vat,
       first_guest_initial_retail_price_excl_vat_ly,
       repeat_guest_initial_retail_price_excl_vat,
       repeat_guest_initial_retail_price_excl_vat_ly,
       act_vip_initial_retail_price_excl_vat,
       act_vip_initial_retail_price_excl_vat_ly,
       repeat_vip_initial_retail_price_excl_vat,
       repeat_vip_initial_retail_price_excl_vat_ly,
       subtotal_amount_excl_tariff,
       subtotal_amount_excl_tariff_ly,
       first_guest_subtotal_amount_excl_tariff,
       first_guest_subtotal_amount_excl_tariff_ly,
       repeat_guest_subtotal_amount_excl_tariff,
       repeat_guest_subtotal_amount_excl_tariff_ly,
       act_vip_subtotal_amount_excl_tariff,
       act_vip_subtotal_amount_excl_tariff_ly,
       repeat_vip_subtotal_amount_excl_tariff,
       repeat_vip_subtotal_amount_excl_tariff_ly,
       product_discount_amount,
       product_discount_amount_ly,
       first_guest_product_discount_amount,
       first_guest_product_discount_amount_ly,
       repeat_guest_product_discount_amount,
       repeat_guest_product_discount_amount_ly,
       act_vip_product_discount_amount,
       act_vip_product_discount_amount_ly,
       repeat_vip_product_discount_amount,
       repeat_vip_product_discount_amount_ly,
       product_cost_amount,
       product_cost_amount_ly,
       first_guest_product_cost_amount,
       first_guest_product_cost_amount_ly,
       repeat_guest_product_cost_amount,
       repeat_guest_product_cost_amount_ly,
       act_vip_product_cost_amount,
       act_vip_product_cost_amount_ly,
       repeat_vip_product_cost_amount,
       repeat_vip_product_cost_amount_ly,
       product_subtotal_amount,
       product_subtotal_amount_ly,
       first_guest_product_subtotal_amount,
       first_guest_product_subtotal_amount_ly,
       repeat_guest_product_subtotal_amount,
       repeat_guest_product_subtotal_amount_ly,
       act_vip_product_subtotal_amount,
       act_vip_product_subtotal_amount_ly,
       repeat_vip_product_subtotal_amount,
       repeat_vip_product_subtotal_amount_ly,
       non_cash_credit_amount,
       non_cash_credit_amount_ly,
       first_guest_non_cash_credit_amount,
       first_guest_non_cash_credit_amount_ly,
       repeat_guest_non_cash_credit_amount,
       repeat_guest_non_cash_credit_amount_ly,
       act_vip_non_cash_credit_amount,
       act_vip_non_cash_credit_amount_ly,
       repeat_vip_non_cash_credit_amount,
       repeat_vip_non_cash_credit_amount_ly,
       credit_redemption_amount,
       credit_redemption_amount_ly,
       credit_redemption_count,
       credit_redemption_count_ly,
       gift_card_issuance_amount,
       gift_card_issuance_amount_ly,
       credit_issuance_amount,
       credit_issuance_amount_ly,
       credit_issuance_count,
       credit_issuance_count_ly,
       gift_card_redemption_amount,
       gift_card_redemption_amount_ly,
       conversion_eligible_orders,
       conversion_eligible_orders_ly,
       paygo_activations,
       paygo_activations_ly,
       product_order_cash_refund_amount,
       product_order_cash_refund_amount_ly,
       product_order_cash_credit_refund_amount,
       product_order_cash_credit_refund_amount_ly,
       billing_cash_refund_amount,
       billing_cash_refund_amount_ly,
       billing_cash_chargeback_amount,
       billing_cash_chargeback_amount_ly,
       product_order_cash_chargeback_amount,
       product_order_cash_chargeback_amount_ly,
       reship_orders,
       reship_orders_ly,
       reship_units,
       reship_units_ly,
       reship_product_cost,
       reship_product_cost_ly,
       exchange_orders,
       exchange_orders_ly,
       exchange_units,
       exchange_units_ly,
       exchange_product_cost,
       exchange_product_cost_ly,
       return_product_cost,
       return_product_cost_ly,
       return_shipping_cost,
       return_shipping_cost_ly,
       online_purchase_retail_returns,
       online_purchase_retail_returns_ly,
       retail_purchase_retail_returns,
       retail_purchase_retail_returns_ly,
       retail_purchase_online_returns,
       retail_purchase_online_returns_ly,
       online_purchase_retail_return_orders,
       online_purchase_retail_return_orders_ly,
       retail_purchase_retail_return_orders,
       retail_purchase_retail_return_orders_ly,
       retail_purchase_online_return_orders,
       retail_purchase_online_return_orders_ly,
       total_exchanges,
       total_exchanges_ly,
       exchanges_after_online_return,
       exchanges_after_online_return_ly,
       exchanges_after_in_store_return,
       exchanges_after_in_store_return_ly,
       exchanges_after_in_store_return_from_online_purchase,
       exchanges_after_in_store_return_from_online_purchase_ly,
       total_exchanges_value,
       total_exchanges_value_ly,
       exchanges_value_after_online_return,
       exchanges_value_after_online_return_ly,
       exchanges_value_after_in_store_return,
       exchanges_value_after_in_store_return_ly,
       exchanges_value_after_in_store_return_from_online_purchase,
       exchanges_value_after_in_store_return_from_online_purchase_ly,
       incoming_visitor_traffic,
       incoming_visitor_traffic_ly,
       exiting_visitor_traffic,
       exiting_visitor_traffic_ly,
       $execution_start_time :: TIMESTAMP_LTZ(3) AS meta_update_datetime,
       $execution_start_time :: TIMESTAMP_LTZ(3) AS meta_create_datetime
from _weekly_final_metrics;
