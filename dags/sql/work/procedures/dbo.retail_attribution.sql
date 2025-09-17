SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

CREATE or REPLACE TEMPORARY TABLE _vip_activation_detail AS
    (
     SELECT
        DISTINCT a.customer_id,
        a.activation_local_datetime as activation_datetime_start,
        a.next_activation_local_datetime as activation_datetime_end,
        a.activation_key,
        st.store_type,
        st.store_sub_type as retail_store_type,
        st.store_full_name as activation_store_name,
        st.store_retail_state as activation_store_state,
        st.store_retail_city as activation_store_city,
        st.store_retail_zip_code as activation_store_zip_code,
        st.store_id as activation_store_id,
        zcs.latitude as activation_store_latitude,
        zcs.longitude as activation_store_longitude
    FROM EDW_PROD.DATA_MODEL_SXF.fact_activation a
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st on st.store_id = a.sub_store_id
    LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.zip_city_state zcs on zcs.zip = st.store_retail_zip_code
    WHERE st.store_brand_abbr = 'SX'
    );

CREATE or REPLACE TEMPORARY TABLE _administrator AS
    (
     SELECT
        fo.order_id,
        otd.object_id as administrator_id
    FROM EDW_PROD.DATA_MODEL_SXF.fact_order fo
    JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT."ORDER" o ON fo.order_id = o.order_id
    JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.order_tracking_detail otd ON o.order_tracking_id = otd.order_tracking_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds on ds.store_id=fo.store_id
    WHERE
        otd.object='administrator'
        AND ds.store_brand_abbr='SX'
    );

CREATE or REPLACE TEMPORARY TABLE _membership_status AS
    (
    SELECT
        DISTINCT fme.customer_id,
        o.order_id,
        fme.event_start_local_datetime as event_start_datetime,
        fme.event_end_local_datetime as event_end_datetime,
       -- CASE WHEN fme_p.membership_event_key IS NOT NULL THEN 1 ELSE 0 END as paygo_activation,
        CASE
            WHEN fme.membership_event_type ILIKE 'Activation' THEN 'VIP'
            WHEN fme.membership_event_type ILIKE 'Cancellation' THEN 'Prior VIP'
            WHEN fme.membership_event_type ILIKE 'Guest Purchasing Member' THEN 'PAYG'
            WHEN fme.membership_event_type ILIKE ANY ('Failed Activation','Email Signup','Registration') THEN 'Lead'
            ELSE 'New'
        END as previous_membership_status,
        CASE WHEN fme_p.membership_event_key IS NOT NULL AND previous_membership_status IN ('Lead','New') THEN 1 ELSE 0 END as paygo_activation,
        ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY fme.event_start_local_datetime DESC) as rn
    FROM EDW_PROD.DATA_MODEL_SXF.fact_order o
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.fact_membership_event fme ON fme.customer_id = o.customer_id
        AND fme.event_start_local_datetime < o.order_local_datetime
        AND fme.event_end_local_datetime >= o.order_local_datetime
        AND fme.membership_event_type NOT ILIKE 'Failed Activation%'
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.fact_membership_event fme_p ON fme_p.customer_id = o.customer_id
        AND fme_p.event_start_local_datetime BETWEEN dateadd(second,-10,o.order_local_datetime) AND dateadd(second,3600,o.order_local_datetime)
        AND fme_p.membership_state ilike 'guest'
        AND fme_p.membership_event_type NOT ILIKE 'Failed Activation%'
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds on ds.store_id=o.store_id
    WHERE ds.store_brand_abbr='SX'
    );

CREATE or REPLACE TEMPORARY TABLE _store_dimensions as
    (
    SELECT store_number,
           store_name,
           store_id,
           warehouse_id,
           max(store_open_date) as store_open_date
    FROM (
        SELECT
            DISTINCT trim(rl.retail_location_code) as store_number
            , trim(rl.label) as store_name
            , trim(rl.store_id) as store_id
            , trim(rl.warehouse_id) as warehouse_id
            , trim(COALESCE(rl.open_date,rl.grand_open_date)) as store_open_date
        FROM lake.ultra_warehouse.retail_location rl
        JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds on ds.store_id=rl.store_id
        WHERE
            ds.Store_Brand='Savage X'
            AND ds.store_type='Retail'
        )
    GROUP BY store_number,
           store_name,
           store_id,
           warehouse_id
    );

CREATE or REPLACE TEMPORARY TABLE  _retail_orders AS
    (
    SELECT DISTINCT o.order_id,
        o.store_id AS order_store_id,
        st.store_type AS order_store_type,
        o.customer_id,
        a.administrator_id,
        vad.activation_datetime_start,
        vad.activation_store_id AS activation_store_id,
        vad.store_type AS customer_activation_channel,
        om.order_channel AS shopped_channel,
        ms.previous_membership_status,
        o.order_local_datetime AS order_datetime_added,
        oms.membership_order_type_l3 AS membership_order_type,
        osc.is_retail_ship_only_order,
        SUM(ifnull(product_subtotal_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS subtotal,
        SUM(ifnull(product_discount_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS discount,
        SUM(ifnull(shipping_revenue_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS shipping,
        SUM(ifnull(tariff_revenue_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS tariff
    FROM EDW_PROD.DATA_MODEL_SXF.fact_order o
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = o.store_id
    JOIN _store_dimensions rst ON rst.store_id = st.store_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status dos ON dos.order_status_key = o.order_status_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms ON oms.order_membership_classification_key = o.order_membership_classification_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
    LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
        AND o.order_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
    LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime))>= date_trunc('minute',om.order_local_datetime_start)
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime))< date_trunc('minute',om.order_local_datetime_end)
    LEFT JOIN _administrator a ON a.order_id=o.order_id
    LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
    WHERE
        CASE WHEN date(o.order_local_datetime) < rst.store_open_date THEN 0 ELSE 1 END = 1
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
            osc.is_retail_ship_only_order
    );


CREATE or REPLACE TEMPORARY TABLE _retail_returns as
    (
    SELECT
        rr.store_id AS order_store_id,
        COALESCE(ro.order_store_type, st.store_type) AS order_store_type,
        dc.customer_id,
        o.order_id,
        a.administrator_id,
        vad.activation_store_id,
        vad.store_type AS customer_activation_channel,
        vad.activation_datetime_start,
        om.order_channel AS shopped_channel,
        oms.membership_order_type_l3 AS membership_order_type,
        osc.is_retail_ship_only_order,
        r.datetime_added AS return_date,
        ms.previous_membership_status,
        CASE WHEN ro.order_id IS NOT NULL THEN 'Retail Purchase Retail Return' ELSE 'Online Purchase Retail Return' END AS purchase_location,
        SUM(rl.quantity) AS quantity
    FROM LAKE_SXF_VIEW.ULTRA_MERCHANT.retail_return_detail rrd
    JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.retail_return rr ON rrd.retail_return_id = rr.retail_return_id
    JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.return r ON r.return_id = rrd.return_id
    JOIN EDW_PROD.DATA_MODEL_SXF.fact_order o ON o.order_id = r.order_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms ON oms.order_membership_classification_key = o.order_membership_classification_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status dos ON dos.order_status_key = o.order_status_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer dc ON dc.customer_id = o.customer_id
    JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.return_line rl ON rl.return_id = r.return_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = rr.store_id
    LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
    LEFT JOIN _retail_orders ro ON ro.order_id = o.order_id
    LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
        AND o.order_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
    LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime)) >= date_trunc('minute',om.order_local_datetime_start)
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime)) < date_trunc('minute',om.order_local_datetime_end)
    LEFT JOIN _administrator a ON a.order_id=o.order_id
    LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
    WHERE
        st.store_brand_abbr = 'SX'
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
    SELECT
        o.order_store_id,
        o.order_store_type,
        o.customer_id,
        o.order_id,
        o.administrator_id,
        vad.activation_store_id,
        vad.store_type AS customer_activation_channel,
        vad.activation_datetime_start,
        om.order_channel AS shopped_channel,
        o.membership_order_type,
        o.is_retail_ship_only_order,
        to_date(r.datetime_added) AS return_date,
        ms.previous_membership_status,
        'Retail Purchase Online Return' AS purchase_location,
        SUM(rl.quantity) AS quantity
    FROM _retail_orders o
    JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.return r ON r.order_id = o.order_id
    JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.return_line rl ON rl.return_id = r.return_id
    LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.retail_return_detail rrd ON rrd.return_id = r.return_id
    LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
        AND o.order_datetime_added between vad.activation_datetime_start AND vad.activation_datetime_end
    LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
      AND date_trunc('minute',to_timestamp_ntz(o.order_datetime_added)) >= date_trunc('minute',om.order_local_datetime_start)
      AND date_trunc('minute',to_timestamp_ntz(o.order_datetime_added)) < date_trunc('minute',om.order_local_datetime_end)
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
            purchase_location
    );

CREATE or REPLACE TEMPORARY TABLE _retail_exchanges AS
    (
    SELECT
        o.order_store_id,
        o.order_store_type,
        o.order_id,
        o.customer_id,
        o.administrator_id,
        TO_DATE(o.order_datetime_added) AS date,
        r.activation_datetime_start,
        r.activation_store_id,
        r.customer_activation_channel,
        o.shopped_channel,
        o.membership_order_type,
        o.is_retail_ship_only_order,
        o.previous_membership_status,
        COUNT(1) as total_exchanges,
        SUM(CASE WHEN r.purchase_location = 'Retail Purchase Online Return' THEN 1 ELSE 0 END) AS exchanges_after_online_return,
        SUM(CASE WHEN r.purchase_location IN ('Retail Purchase Retail Return', 'Online Purchase Retail Return') THEN 1 ELSE 0 END) AS exchanges_after_in_store_return,
        SUM(CASE WHEN r.purchase_location = 'Online Purchase Retail Return' THEN 1 ELSE 0 END) AS exchanges_after_in_store_return_from_online_purchase,
        SUM(o.subtotal - o.discount + o.shipping + o.tariff) AS total_exchanges_value,
        SUM(CASE WHEN r.purchase_location = 'Retail Purchase Online Return' THEN o.subtotal - o.discount + o.shipping + o.tariff ELSE 0 END) AS exchanges_value_after_online_return,
        SUM(CASE WHEN r.purchase_location IN ('Retail Purchase Retail Return', 'Online Purchase Retail Return') THEN o.subtotal - o.discount + o.shipping + o.tariff ELSE 0 END) AS exchanges_value_after_in_store_return,
        SUM(CASE WHEN r.purchase_location ='Online Purchase Retail Return' THEN o.subtotal - o.discount + o.shipping + o.tariff ELSE 0 END) AS exchanges_value_after_in_store_return_from_online_purchase
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
            o.previous_membership_status
    );

CREATE or REPLACE TEMPORARY TABLE _financial_refunds AS
    (
    SELECT
        to_date(r.refund_completion_local_datetime) AS refund_date,
        to_date(vad.activation_datetime_start) AS activation_cohort,
        activation_store_id,
        vad.store_type AS customer_activation_channel,
        a.administrator_id,
        om.order_channel AS shopped_channel,
        r.store_id AS refund_store_id,
        st.store_full_name AS refund_store_name,
        st.store_type AS refund_store_type,
        st.store_sub_type AS refund_retail_store_type,
        oms.membership_order_type_l3 AS membership_order_type,
        osc.is_retail_ship_only_order,
        ms.previous_membership_status,
        SUM(CASE WHEN osc.order_classification_l1 = 'Product Order' THEN ifnull(r.cash_refund_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) AS product_order_cash_refund_amount,
        SUM(CASE WHEN osc.order_sales_channel_l1 = 'Billing Order' THEN ifnull(r.cash_refund_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) AS billing_cash_refund_amount,
        SUM(CASE WHEN osc.order_classification_l1 = 'Product Order' THEN ifnull(r.cash_store_credit_refund_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) AS product_order_cash_credit_refund_amount
    FROM EDW_PROD.DATA_MODEL_SXF.fact_refund r
    JOIN EDW_PROD.DATA_MODEL_SXF.fact_order o ON r.order_id = o.order_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms ON oms.order_membership_classification_key = o.order_membership_classification_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = r.customer_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = r.store_id
    LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
        AND r.refund_completion_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
    LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
        AND date_trunc('minute',to_timestamp_ntz(r.refund_completion_local_datetime)) >= date_trunc('minute',om.order_local_datetime_start)
        AND date_trunc('minute',to_timestamp_ntz(r.refund_completion_local_datetime)) < date_trunc('minute',om.order_local_datetime_end)
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_refund_payment_method dfpm ON dfpm.refund_payment_method_key = r.refund_payment_method_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_refund_status rs ON rs.refund_status_key = r.refund_status_key
    LEFT JOIN _administrator a ON a.order_id=o.order_id
    LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
    LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
    WHERE
        st.store_brand_abbr = 'SX'
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
            ms.previous_membership_status
    );

CREATE or REPLACE TEMPORARY TABLE _financial_chargebacks AS
    (
    SELECT
        to_date(r.chargeback_datetime) AS chargeback_date,
        to_date(vad.activation_datetime_start) AS activation_cohort,
        activation_store_id,
        vad.store_type AS customer_activation_channel,
        a.administrator_id,
        om.order_channel AS shopped_channel,
        r.store_id AS chargeback_store_id,
        st.store_type AS chargeback_store_type,
        oms.membership_order_type_l3 AS membership_order_type,
        osc.is_retail_ship_only_order,
        ms.previous_membership_status,
        SUM(ifnull(IFF(osc.order_classification_l1 = 'Product Order',ifnull(r.chargeback_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1),0),0)) AS product_order_cash_chargeback_amount,
        SUM(ifnull(IFF(osc.order_sales_channel_l1 = 'Billing Order',ifnull(r.chargeback_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1),0),0)) AS billing_cash_chargeback_amount
    FROM EDW_PROD.DATA_MODEL_SXF.fact_chargeback r
    JOIN EDW_PROD.DATA_MODEL_SXF.fact_order o ON r.order_id = o.order_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms ON oms.order_membership_classification_key = o.order_membership_classification_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = r.customer_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = r.store_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_chargeback_payment dcp ON dcp.chargeback_payment_key = r.chargeback_payment_key
    LEFT JOIN _vip_activation_detail vad ON vad.customer_id = r.customer_id
        AND r.chargeback_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
    LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
        AND date_trunc('minute',to_timestamp_ntz(r.chargeback_datetime)) >= date_trunc('minute',om.order_local_datetime_start)
        AND date_trunc('minute',to_timestamp_ntz(r.chargeback_datetime)) < date_trunc('minute',om.order_local_datetime_end)
    LEFT JOIN _administrator a ON a.order_id=o.order_id
    LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
    LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
    WHERE
        st.store_brand_abbr = 'SX'
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
            ms.previous_membership_status
    );

CREATE or REPLACE TEMPORARY TABLE _financial_exchanges AS
    (
    SELECT
        to_date(o.order_local_datetime) AS exchange_date,
        vad.activation_store_id,
        vad.store_type AS customer_activation_channel,
        a.administrator_id,
        to_date(vad.activation_datetime_start) AS activation_cohort,
        o.store_id AS exchange_store_id,
        st.store_type AS exchange_store_type,
        om.order_channel AS shopped_channel,
        oms.membership_order_type_l3 AS membership_order_type,
        osc.is_retail_ship_only_order,
        ms.previous_membership_status,
        COUNT(DISTINCT o.order_id) AS exchange_orders,
        SUM(unit_count) AS exchange_units,
        SUM(ifnull(coalesce(opc.oracle_cost_local_amount,opc.reporting_landed_cost_local_amount),0)* ifnull(o.reporting_usd_conversion_rate,1)) AS exchange_product_cost,
        SUM(ifnull(o.shipping_cost_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS exchange_shipping_cost,
        SUM(ifnull(o.estimated_shipping_supplies_cost_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS exchange_shipping_supplies_cost
    FROM EDW_PROD.DATA_MODEL_SXF.fact_order o
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms ON oms.order_membership_classification_key = o.order_membership_classification_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = o.store_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = o.customer_id
    LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
        AND o.order_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
    LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime)) >= date_trunc('minute',om.order_local_datetime_start)
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime)) < date_trunc('minute',om.order_local_datetime_end)
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.fact_order_product_cost opc on opc.order_id = o.order_id
    LEFT JOIN _administrator a ON a.order_id=o.order_id
    LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
    LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
    WHERE
        osc.order_classification_l2 ILIKE 'Exchange'
        AND order_status IN ('Success','Pending')
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
            ms.previous_membership_status
    );

CREATE or REPLACE TEMPORARY TABLE _financial_reships AS
    (
    SELECT
        to_date(o.order_local_datetime) as reship_date,
        vad.activation_store_id,
        vad.store_type AS customer_activation_channel,
        a.administrator_id,
        om.order_channel AS shopped_channel,
        to_date(vad.activation_datetime_start) as activation_cohort,
        o.store_id AS reship_store_id,
        st.store_type AS reship_store_type,
        oms.membership_order_type_l3 AS membership_order_type,
        osc.is_retail_ship_only_order,
        ms.previous_membership_status  ,
        COUNT(DISTINCT o.order_id) AS reship_orders,
        SUM(unit_count) as reship_units,
        SUM(ifnull(COALESCE(opc.oracle_cost_local_amount,opc.reporting_landed_cost_local_amount),0)* ifnull(o.reporting_usd_conversion_rate,1)) AS reship_product_cost,
        SUM(ifnull(o.shipping_cost_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS reship_shipping_cost,
        SUM(ifnull(o.estimated_shipping_supplies_cost_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS reship_shipping_supplies_cost
    FROM EDW_PROD.DATA_MODEL_SXF.fact_order o
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms ON oms.order_membership_classification_key = o.order_membership_classification_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = o.customer_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = o.store_id
    LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
        AND o.order_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
    LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime)) >= date_trunc('minute',om.order_local_datetime_start)
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime)) < date_trunc('minute',om.order_local_datetime_end)
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.fact_order_product_cost opc ON opc.order_id = o.order_id
    LEFT JOIN _administrator a ON a.order_id=o.order_id
    LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
    LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
    WHERE
        osc.order_classification_l2 ILIKE 'Reship'
        AND order_status IN ('Success','Pending')
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
            ms.previous_membership_status
    );

CREATE or REPLACE TEMPORARY TABLE _financial_return_product_cost AS
    (
    SELECT to_date(return_completion_local_datetime) as return_date,
        vad.activation_store_id,
        vad.store_type AS customer_activation_channel,
        to_date(vad.activation_datetime_start) AS activation_cohort,
        om.order_channel AS shopped_channel,
        rl.store_id AS return_store_id,
        st.store_type AS return_store_type,
        rl.administrator_id,
        osc.is_retail_ship_only_order,
        oms.membership_order_type_l3 AS membership_order_type,
        ms.previous_membership_status,
        SUM(ifnull(estimated_returned_product_local_cost,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS return_product_cost_local_amount,
        SUM(ifnull(estimated_return_shipping_cost_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS return_shipping_cost_amount,
        SUM(ifnull(estimated_returned_product_cost_local_amount_resaleable,0)* ifnull(o.reporting_usd_conversion_rate,1)) AS return_product_cost_local_amount_resaleable
    FROM EDW_PROD.DATA_MODEL_SXF.fact_return_line rl
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_return_status rs ON rs.return_status_key = rl.return_status_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = rl.customer_id
    JOIN EDW_PROD.DATA_MODEL_SXF.fact_order_line o ON o.order_line_id = rl.order_line_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms ON oms.order_membership_classification_key = o.order_membership_classification_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = rl.store_id
    LEFT JOIN _vip_activation_detail vad ON vad.customer_id = o.customer_id
        AND o.order_local_datetime BETWEEN vad.activation_datetime_start AND vad.activation_datetime_end
    LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om ON om.customer_id = o.customer_id
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime)) >= date_trunc('minute',om.order_local_datetime_start)
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime)) < date_trunc('minute',om.order_local_datetime_end)
    LEFT JOIN _administrator a ON a.order_id=o.order_id
    LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
    LEFT JOIN _store_dimensions rst ON rst.store_id = st.store_id
    WHERE
        c.is_test_customer = 0
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
            ms.previous_membership_status
    );

CREATE or REPLACE TEMPORARY TABLE _gift_cards_issued AS
    (
    SELECT DISTINCT order_id,
        sum(ifnull(product_subtotal_local_amount,0)* ifnull(ol.reporting_usd_conversion_rate,1)) AS gift_card_amount
    FROM EDW_PROD.DATA_MODEL_SXF.fact_order_line ol
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_product_type pt ON pt.product_type_key = ol.product_type_key
    WHERE product_type_name IN ('Gift Certificate','Gift Card')
    GROUP BY order_id
    );

CREATE or REPLACE TEMPORARY TABLE _order_metrics AS
    (
    SELECT
    --dimensions
        to_date(o.order_local_datetime) AS order_date,
        rv.store_type AS customer_activation_channel,
        om.order_channel AS shopped_channel,
        CASE WHEN rv.retail_store_type = 'Store' THEN 'Store' ELSE rv.retail_store_type END AS retail_activation_store_type,
        rv.activation_store_name,
        a.administrator_id,
        to_date(rv.activation_datetime_start) AS activation_cohort, --should this be activation date?
        rv.activation_store_id,
        st.store_full_name AS order_store_name,
        st.store_id AS order_store_id,
        st.store_type AS order_store_type,
        CASE WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type ELSE 'Online' END AS retail_order_store_type,
        oms.membership_order_type_l3 AS membership_order_type,
        osc.is_retail_ship_only_order,
        ms.previous_membership_status,
        ms.paygo_activation,
        max(o.order_local_datetime) latest_order,
        COUNT(DISTINCT CASE WHEN osc.order_classification_l2 IN ('Product Order','Credit Billing') THEN o.order_id ELSE null END) AS orders,
        COUNT(DISTINCT CASE WHEN ms.previous_membership_status <> 'VIP' THEN o.order_id ELSE null END) as conversion_eligible_orders,
        COUNT(DISTINCT gci.order_id) as gift_card_orders,
        SUM(CASE WHEN osc.order_classification_l2 IN ('Product Order','Credit Billing') THEN unit_count ELSE 0 END) AS units,
        SUM(CASE WHEN osc.order_classification_l2 IN ('Product Order') THEN ifnull(o.product_gross_revenue_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) END) AS product_gross_revenue,
        SUM(CASE WHEN (lower(osc.order_classification_l1) = 'product order' OR lower(osc.order_sales_channel_l1) = 'billing order') THEN ifnull(o.cash_gross_revenue_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) END) AS cash_gross_revenue,
        SUM(CASE WHEN osc.order_classification_l2 IN ('Product Order','Credit Billing','Token Billing') THEN ifnull(o.shipping_revenue_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) AS shipping_revenue,
        SUM(CASE WHEN osc.order_classification_l2 IN ('Product Order','Credit Billing','Token Billing') THEN ifnull(o.shipping_cost_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) AS shipping_cost,
        SUM(CASE WHEN osc.order_classification_l2 IN ('Product Order', 'Credit Billing','Token Billing') THEN ifnull(o.estimated_shipping_supplies_cost_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) AS shipping_supplies_cost,
        SUM(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.product_discount_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) AS discount,
        SUM(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.misc_cogs_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) as misc_cogs,
        SUM(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.cash_membership_credit_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) + sum(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.cash_giftco_credit_local_amount,0)* ifnull(reporting_usd_conversion_rate,1) ELSE 0 END) +sum(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.token_local_amount,0)* ifnull(reporting_usd_conversion_rate,1) ELSE 0 END) AS credit_redemption_amount,
        COUNT(DISTINCT CASE WHEN osc.order_classification_l2 = 'Product Order' AND ifnull(o.cash_membership_credit_local_amount,0) > 0 OR ifnull(o.cash_giftco_credit_local_amount,0) OR ifnull(o.token_local_amount,0) > 0 THEN o.order_id END) as orders_with_credit_redemption,
        SUM(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.cash_membership_credit_count,0)* ifnull(reporting_usd_conversion_rate,1) ELSE 0 END) + sum(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(cash_giftco_credit_count,0)* ifnull(reporting_usd_conversion_rate,1) ELSE 0 END) AS credit_redemption_count,
        SUM(CASE WHEN osc.order_classification_l2 = 'Gift Certificate' THEN ifnull(gift_card_amount,0)* ifnull(reporting_usd_conversion_rate,1) ELSE 0 END) as gift_card_issuance_amount,
        SUM(CASE WHEN osc.order_classification_l2 = 'Credit Billing' THEN ifnull(o.cash_membership_credit_local_amount,0)* ifnull(reporting_usd_conversion_rate,1) ELSE 0 END) AS credit_issuance_amount,
        SUM(CASE WHEN osc.order_classification_l2 = 'Credit Billing' THEN ifnull(o.cash_membership_credit_count,0)* ifnull(reporting_usd_conversion_rate,1) ELSE 0 END) AS credit_issuance_count, --credit billings can be used for LTV later on
        SUM(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.cash_giftcard_credit_local_amount,0)* ifnull(reporting_usd_conversion_rate,1) ELSE 0 END) AS gift_card_redemption_amount,
        SUM(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.reporting_landed_cost_local_amount,0)* ifnull(reporting_usd_conversion_rate,1) ELSE 0 END) AS product_cost,
        SUM(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(opc.oracle_product_cost,0)* ifnull(reporting_usd_conversion_rate,1) ELSE 0 END) AS oracle_product_cost, --why are we bringing in oracle_product_cost
        SUM(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.product_subtotal_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) AS product_subtotal_amount,
        SUM(CASE WHEN osc.order_classification_l2 = 'Product Order' THEN ifnull(o.non_cash_credit_local_amount,0)* ifnull(o.reporting_usd_conversion_rate,1) ELSE 0 END) AS non_cash_credit_amount
FROM EDW_PROD.DATA_MODEL_SXF.fact_order o
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_status os ON os.order_status_key = o.order_status_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_order_membership_classification oms ON oms.order_membership_classification_key = o.order_membership_classification_key
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = o.store_id
    JOIN EDW_PROD.DATA_MODEL_SXF.dim_customer c ON c.customer_id = o.customer_id
    LEFT JOIN _vip_activation_detail rv ON rv.customer_id = o.customer_id
        AND o.order_local_datetime BETWEEN rv.activation_datetime_start AND rv.activation_datetime_end
    LEFT JOIN REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION om on om.customer_id = o.customer_id
        AND COALESCE(om.activation_key,-1)=COALESCE(rv.activation_key,-1)
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime)) >= date_trunc('minute',om.order_local_datetime_start)
        AND date_trunc('minute',to_timestamp_ntz(o.order_local_datetime)) < date_trunc('minute',om.order_local_datetime_end)
    LEFT JOIN _membership_status ms ON o.order_id = ms.order_id AND ms.rn = 1
    LEFT JOIN _gift_cards_issued gci ON ms.order_id = gci.order_id
    LEFT JOIN (SELECT DISTINCT order_id, SUM(ifnull(oracle_cost_local_amount,0)) as oracle_product_cost
               FROM EDW_PROD.DATA_MODEL_SXF.fact_order_product_cost GROUP BY 1) opc on opc.order_id = o.order_id --why is Ashley pulling in oracle costs
    LEFT JOIN _administrator a ON a.order_id = o.order_id
    LEFT JOIN _store_dimensions sd ON sd.store_id = st.store_id
    WHERE
        osc.order_classification_l2 IN ('Credit Billing','Product Order','Gift Certificate','Reship','Exchange')
        AND order_status IN ('Success','Pending')
        AND st.store_brand_abbr = 'SX'
        AND c.is_test_customer = 0
        AND CASE WHEN st.store_type = 'Retail' AND DATE(o.order_local_datetime) < sd.store_open_date THEN 0 ELSE 1 END = 1
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
            ms.paygo_activation
    );


CREATE or REPLACE TEMPORARY TABLE _retail_traffic as
    (
    SELECT
        rt.store_id
        ,rt.store_full_name
        ,rt.date
        ,sum(incoming_visitor_traffic) incoming_visitor_traffic
        ,sum(exiting_visitor_traffic) exiting_visitor_traffic
    FROM REPORTING_PROD.sxf.retail_traffic rt
    JOIN _store_dimensions sd ON rt.store_id = sd.store_id
    WHERE during_business_hours_excluding_open_time = true
        AND rt.date >= sd.store_open_date
    GROUP BY rt.store_id,
        rt.store_full_name,
        rt.date
    );

CREATE or REPLACE TEMPORARY TABLE _financial_retail_returns AS
    (
    SELECT order_store_id,
        order_store_type,
        activation_store_id,
        customer_activation_channel,
        administrator_id,
        to_date(activation_datetime_start) AS activation_cohort,
        shopped_channel,
        membership_order_type,
        is_retail_ship_only_order,
        previous_membership_status,
        to_date(return_date) AS return_date,
        purchase_location,
        COUNT(DISTINCT order_id) return_orders,
        sum(quantity) AS quantity
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
            purchase_location
    );

CREATE or REPLACE TEMPORARY TABLE _financial_retail_exchanges AS
    (
    SELECT order_store_id,
        order_store_type,
        activation_store_id,
        customer_activation_channel,
        administrator_id,
        to_date(activation_datetime_start) AS activation_cohort,
        shopped_channel,
        membership_order_type,
        is_retail_ship_only_order,
        date,
        previous_membership_status,
        SUM(total_exchanges) AS total_exchanges,
        SUM(exchanges_after_online_return) AS exchanges_after_online_return,
        SUM(exchanges_after_in_store_return) AS exchanges_after_in_store_return,
        SUM(exchanges_after_in_store_return_from_online_purchase) AS exchanges_after_in_store_return_from_online_purchase,
        SUM(total_exchanges_value) AS total_exchanges_value,
        SUM(exchanges_value_after_online_return) AS exchanges_value_after_online_return,
        SUM(exchanges_value_after_in_store_return) AS exchanges_value_after_in_store_return,
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
            previous_membership_status
    );

CREATE or REPLACE TEMPORARY TABLE _final_temp AS
(
SELECT
    to_date(COALESCE(o.order_date,r.refund_date,c.chargeback_date,rpc.return_date,e.exchange_date,rs.reship_date,rr.return_date,re.date)) AS order_date,
    COALESCE(o.administrator_id,r.administrator_id,c.administrator_id,rpc.administrator_id,e.administrator_id,rs.administrator_id,rr.administrator_id,re.administrator_id) AS administrator_id,
    COALESCE(o.customer_activation_channel,r.customer_activation_channel,c.customer_activation_channel,rpc.customer_activation_channel,e.customer_activation_channel,rs.customer_activation_channel,rr.customer_activation_channel,re.customer_activation_channel) AS customer_activation_channel,
    CASE WHEN datediff(month,ifnull(arsd.store_open_date,'2079-01-01'), to_date(coalesce(o.order_date,r.refund_date,c.chargeback_date,rpc.return_date,e.exchange_date,rs.reship_date,rr.return_date,re.date))) >= 13 THEN 'Comp' ELSE 'New' END AS activation_comp_new,
    COALESCE(o.activation_store_id,r.activation_store_id,c.activation_store_id,rpc.activation_store_id,e.activation_store_id,rs.activation_store_id,rr.activation_store_id,re.activation_store_id) AS activation_store_id,
    COALESCE(o.shopped_channel,r.shopped_channel,c.shopped_channel,rpc.shopped_channel,e.shopped_channel,rs.shopped_channel,rr.shopped_channel,re.shopped_channel) AS shopped_channel,
    COALESCE(o.order_store_type,r.refund_store_type,c.chargeback_store_type,rpc.return_store_type,e.exchange_store_type,rs.reship_store_type,rr.order_store_type,re.order_store_type) AS order_store_type,
    COALESCE(o.order_store_id,r.refund_store_id,c.chargeback_store_id,rpc.return_store_id,e.exchange_store_id,rs.reship_store_id,rr.order_store_id,re.order_store_id) AS order_store_id,
    CASE WHEN datediff(month,ifnull(orsd.store_open_date,'2079-01-01'), to_date(coalesce(o.order_date,r.refund_date,c.chargeback_date,rpc.return_date,e.exchange_date,rs.reship_date,rr.return_date,re.date))) >= 13 THEN 'Comp' ELSE 'New' END AS order_comp_new,
    COALESCE(o.membership_order_type,r.membership_order_type,c.membership_order_type,rpc.membership_order_type,e.membership_order_type,rs.membership_order_type,rr.membership_order_type,re.membership_order_type) AS membership_order_type,
    COALESCE(o.is_retail_ship_only_order,r.is_retail_ship_only_order,c.is_retail_ship_only_order,rpc.is_retail_ship_only_order,e.is_retail_ship_only_order,rs.is_retail_ship_only_order,rr.is_retail_ship_only_order,re.is_retail_ship_only_order) AS is_retail_ship_only_order,
    o.latest_order,
    SUM(COALESCE(orders,0)) AS orders,
    SUM(COALESCE(units,0)) AS units,
    SUM(COALESCE(gift_card_orders,0)) as gift_card_orders,
    SUM(COALESCE(orders_with_credit_redemption,0)) as orders_with_credit_redemption,
    SUM(COALESCE(o.product_gross_revenue,0)) AS product_gross_revenue,
    SUM(COALESCE(o.cash_gross_revenue,0)) AS cash_gross_revenue,
    SUM(COALESCE(o.product_gross_revenue,0))
        - SUM(ifnull(r.product_order_cash_credit_refund_amount,0))
        - SUM(ifnull(r.product_order_cash_refund_amount,0))
        - SUM(ifnull(c.product_order_cash_chargeback_amount,0))
        AS financial_product_net_revenue,
    SUM(COALESCE(o.cash_gross_revenue,0))
        - SUM(ifnull(r.product_order_cash_refund_amount,0))
        - SUM(ifnull(r.billing_cash_refund_amount,0))
        - SUM(ifnull(c.product_order_cash_chargeback_amount,0))
        - SUM(ifnull(c.billing_cash_chargeback_amount,0))
        AS financial_cash_net_revenue,
    SUM(COALESCE(o.product_gross_revenue,0)) --correct
        - SUM(ifnull(r.product_order_cash_credit_refund_amount,0))
        - SUM(ifnull(r.product_order_cash_refund_amount,0))
        - SUM(ifnull(c.product_order_cash_chargeback_amount,0))
        - SUM(ifnull(product_cost,0))
        - SUM(ifnull(shipping_cost,0))
        - SUM(ifnull(shipping_supplies_cost,0))
        - SUM(ifnull(rpc.return_shipping_cost_amount,0))
        + SUM(ifnull(rpc.return_product_cost_local_amount_resaleable,0))
        - SUM(ifnull(rs.reship_product_cost,0))
        - SUM(ifnull(rs.reship_shipping_cost,0))
        - SUM(ifnull(rs.reship_shipping_supplies_cost,0))
        - SUM(ifnull(e.exchange_product_cost,0))
        - SUM(ifnull(e.exchange_shipping_cost,0))
        - SUM(ifnull(e.exchange_shipping_supplies_cost,0))
        - SUM(ifnull(misc_cogs,0))
        AS financial_product_gross_profit,
    SUM(COALESCE(o.cash_gross_revenue,0))
        - SUM(ifnull(r.product_order_cash_refund_amount,0))
        - SUM(ifnull(r.billing_cash_refund_amount,0))
        - SUM(ifnull(c.product_order_cash_chargeback_amount,0))
        - SUM(ifnull(c.billing_cash_chargeback_amount,0))
        - SUM(ifnull(product_cost,0))
        - SUM(ifnull(shipping_cost,0))
        - SUM(ifnull(shipping_supplies_cost,0))
        - SUM(ifnull(rpc.return_shipping_cost_amount,0))
        + SUM(ifnull(rpc.return_product_cost_local_amount_resaleable,0))
        - SUM(ifnull(rs.reship_product_cost,0))
        - SUM(ifnull(rs.reship_shipping_cost,0))
        - SUM(ifnull(rs.reship_shipping_supplies_cost,0))
        - SUM(ifnull(e.exchange_product_cost,0))
        - SUM(ifnull(e.exchange_shipping_cost,0))
        - SUM(ifnull(e.exchange_shipping_supplies_cost,0))
        - SUM(ifnull(misc_cogs,0))
        AS financial_cash_gross_profit,
    SUM(coalesce(shipping_revenue,0)) AS shipping_revenue,
    SUM(ifnull(misc_cogs,0)) AS misc_cogs,
    SUM(shipping_cost) AS shipping_cost,
    SUM(shipping_supplies_cost) AS shipping_supplies_cost,
    SUM(rpc.return_product_cost_local_amount_resaleable) AS return_product_cost_local_amount_resaleable,
    SUM(discount) AS discount,
    SUM(product_cost) AS product_cost_amount,
    SUM(product_subtotal_amount) as product_subtotal_amount,
    SUM(non_cash_credit_amount) as non_cash_credit_amount,
    SUM(oracle_product_cost) AS oracle_product_cost,
    SUM(credit_redemption_amount) AS credit_redemption_amount,
    SUM(credit_redemption_count) AS credit_redemption_count,
    SUM(gift_card_issuance_amount) AS gift_card_issuance_amount,
    SUM(credit_issuance_amount) AS credit_issuance_amount,
    SUM(credit_issuance_count) AS credit_issuance_count,
    SUM(gift_card_redemption_amount) AS gift_card_redemption_amount,
    SUM(conversion_eligible_orders) AS conversion_eligible_orders,
    SUM(CASE WHEN paygo_activation = 1 THEN 1 ELSE 0 END) AS paygo_activations,
    SUM(c.billing_cash_chargeback_amount) AS financial_billing_cash_chargeback_amount,
    SUM(c.product_order_cash_chargeback_amount) AS financial_product_order_cash_chargeback_amount,
    SUM(r.billing_cash_refund_amount) AS financial_billing_cash_refund_amount,
    SUM(r.product_order_cash_refund_amount) AS financial_product_order_cash_refund_amount,
    SUM(r.product_order_cash_credit_refund_amount) AS financial_product_order_cash_credit_refund_amount,
    SUM(e.exchange_orders) AS financial_exchange_orders,
    SUM(e.exchange_units) AS financial_exchange_units,
    SUM(e.exchange_product_cost) AS financial_exchange_product_cost,
    SUM(e.exchange_shipping_cost) AS financial_exchange_shipping_cost,
    SUM(e.exchange_shipping_supplies_cost) AS financial_exchange_shipping_supplies_cost,
    SUM(rs.reship_orders) AS financial_reship_orders,
    SUM(rs.reship_units) AS financial_reship_units,
    SUM(rs.reship_product_cost) AS financial_reship_product_cost,
    SUM(rs.reship_shipping_cost) AS financial_reship_shipping_cost,
    SUM(rs.reship_shipping_supplies_cost) AS financial_reship_shipping_supplies_cost,
    SUM(CASE WHEN rr.purchase_location = 'Online Purchase Retail Return' THEN quantity ELSE 0 END) AS financial_online_purchase_retail_returns,
    SUM(CASE WHEN rr.purchase_location = 'Retail Purchase Retail Return' THEN quantity ELSE 0 END) AS financial_retail_purchase_retail_returns,
    SUM(CASE WHEN rr.purchase_location = 'Retail Purchase Online Return' THEN quantity ELSE 0 END) AS financial_retail_purchase_online_returns,
    SUM(CASE WHEN rr.purchase_location = 'Online Purchase Retail Return' THEN return_orders ELSE 0 END) AS financial_online_purchase_retail_return_orders,
    SUM(CASE WHEN rr.purchase_location = 'Retail Purchase Retail Return' THEN return_orders ELSE 0 END) AS financial_retail_purchase_retail_return_orders,
    SUM(CASE WHEN rr.purchase_location = 'Retail Purchase Online Return' THEN return_orders ELSE 0 END) AS financial_retail_purchase_online_return_orders,
    SUM(re.total_exchanges) AS financial_total_exchanges,
    SUM(re.exchanges_after_online_return) AS financial_exchanges_after_online_return,
    SUM(re.exchanges_after_in_store_return) AS financial_exchanges_after_in_store_return,
    SUM(re.exchanges_after_in_store_return_from_online_purchase) AS exchanges_after_in_store_return_from_online_purchase,
    SUM(re.total_exchanges_value) AS financial_total_exchanges_value,
    SUM(re.exchanges_value_after_online_return) AS financial_exchanges_value_after_online_return,
    SUM(re.exchanges_value_after_in_store_return) AS financial_exchanges_value_after_in_store_return,
    SUM(re.exchanges_value_after_in_store_return_from_online_purchase) AS exchanges_value_after_in_store_return_from_online_purchase,
    SUM(CASE WHEN o.customer_activation_channel != 'Retail' AND o.order_store_type = 'Retail' THEN o.orders ELSE 0 END) AS omni_orders,
    SUM(rpc.return_product_cost_local_amount) AS financial_return_product_cost_local_amount,
    SUM(rpc.return_shipping_cost_amount) AS financial_return_shipping_cost_amount
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
FULL JOIN _financial_chargebacks c ON to_date(c.chargeback_date) = to_date(COALESCE(o.order_date,r.refund_date))
                                       AND c.administrator_id = COALESCE(o.administrator_id,r.administrator_id)
                                       AND c.activation_store_id = COALESCE(o.activation_store_id,r.activation_store_id)
                                       AND c.shopped_channel = COALESCE(o.shopped_channel,r.shopped_channel)
                                       AND c.activation_cohort = COALESCE(o.activation_cohort,r.activation_cohort)
                                       AND c.chargeback_store_id = COALESCE(o.order_store_id,r.refund_store_id)
                                       AND c.membership_order_type = COALESCE(o.membership_order_type,r.membership_order_type)
                                       AND c.is_retail_ship_only_order = COALESCE(o.is_retail_ship_only_order,r.is_retail_ship_only_order)
                                       AND c.previous_membership_status = COALESCE(o.previous_membership_status,r.previous_membership_status)
FULL JOIN _financial_return_product_cost rpc ON to_date(rpc.return_date) = to_date(COALESCE(o.order_date,r.refund_date,c.chargeback_date))
                                       AND rpc.administrator_id = COALESCE(o.administrator_id,r.administrator_id,c.administrator_id)
                                       AND rpc.activation_store_id = COALESCE(o.activation_store_id,r.activation_store_id,c.activation_store_id)
                                       AND rpc.activation_cohort = COALESCE(o.activation_cohort,r.activation_cohort,c.activation_cohort)
                                       AND rpc.shopped_channel = COALESCE(o.shopped_channel,r.shopped_channel,c.shopped_channel)
                                       AND rpc.return_store_id = COALESCE(o.order_store_id,r.refund_store_id,c.chargeback_store_id)
                                       AND rpc.membership_order_type = COALESCE(o.membership_order_type,r.membership_order_type,c.membership_order_type)
                                       AND rpc.is_retail_ship_only_order = COALESCE(o.is_retail_ship_only_order,r.is_retail_ship_only_order,c.is_retail_ship_only_order)
                                       AND rpc.previous_membership_status = COALESCE(o.previous_membership_status,r.previous_membership_status,c.previous_membership_status)
FULL JOIN _financial_exchanges e on to_date(e.exchange_date) = to_date(COALESCE(o.order_date,r.refund_date,c.chargeback_date,rpc.return_date))
                                       AND e.administrator_id = COALESCE(o.administrator_id,r.administrator_id,c.administrator_id,rpc.administrator_id)
                                       AND e.activation_store_id = COALESCE(o.activation_store_id,r.activation_store_id,c.activation_store_id,rpc.activation_store_id)
                                       AND e.activation_cohort = COALESCE(o.activation_cohort,r.activation_cohort,c.activation_cohort,rpc.activation_cohort)
                                       AND e.shopped_channel = COALESCE(o.shopped_channel,r.shopped_channel,c.shopped_channel,rpc.shopped_channel)
                                       AND e.exchange_store_id = COALESCE(o.order_store_id,r.refund_store_id,c.chargeback_store_id,rpc.return_store_id)
                                       AND e.membership_order_type = COALESCE(o.membership_order_type,r.membership_order_type,c.membership_order_type,rpc.membership_order_type)
                                       AND e.is_retail_ship_only_order = COALESCE(o.is_retail_ship_only_order,r.is_retail_ship_only_order,c.is_retail_ship_only_order,rpc.is_retail_ship_only_order)
                                       AND e.previous_membership_status = COALESCE(o.previous_membership_status,r.previous_membership_status,c.previous_membership_status,rpc.previous_membership_status)
FULL JOIN _financial_reships rs on to_date(rs.reship_date) = to_date(COALESCE(o.order_date,r.refund_date,c.chargeback_date,rpc.return_date,e.exchange_date))
                                       AND rs.administrator_id = COALESCE(o.administrator_id,r.administrator_id,c.administrator_id,rpc.administrator_id,e.administrator_id)
                                       AND rs.activation_store_id = COALESCE(o.activation_store_id,r.activation_store_id,c.activation_store_id,rpc.activation_store_id,e.activation_store_id)
                                       AND rs.activation_cohort = COALESCE(o.activation_cohort,r.activation_cohort,c.activation_cohort,rpc.activation_cohort,e.activation_cohort)
                                       AND rs.shopped_channel = COALESCE(o.shopped_channel,r.shopped_channel,c.shopped_channel,rpc.shopped_channel,e.shopped_channel)
                                       AND rs.reship_store_id = COALESCE(o.order_store_id,r.refund_store_id,c.chargeback_store_id,rpc.return_store_id,e.exchange_store_id)
                                       AND rs.membership_order_type = COALESCE(o.membership_order_type,r.membership_order_type,c.membership_order_type,rpc.membership_order_type,e.membership_order_type)
                                       AND rs.is_retail_ship_only_order = COALESCE(o.is_retail_ship_only_order,r.is_retail_ship_only_order,c.is_retail_ship_only_order,rpc.is_retail_ship_only_order,e.is_retail_ship_only_order)
                                       AND rs.previous_membership_status = COALESCE(o.previous_membership_status,r.previous_membership_status,c.previous_membership_status,rpc.previous_membership_status,e.previous_membership_status)
FULL JOIN _financial_retail_returns rr ON to_date(rr.return_date) = to_date(COALESCE(o.order_date,r.refund_date,c.chargeback_date,rpc.return_date,e.exchange_date,rs.reship_date))
                                       AND rr.administrator_id = COALESCE(o.administrator_id,r.administrator_id,c.administrator_id,rpc.administrator_id,e.administrator_id,rs.administrator_id)
                                       AND rr.activation_store_id = COALESCE(o.activation_store_id,r.activation_store_id,c.activation_store_id,rpc.activation_store_id,e.activation_store_id,rs.activation_store_id)
                                       AND rr.activation_cohort = COALESCE(o.activation_cohort,r.activation_cohort,c.activation_cohort,rpc.activation_cohort,e.activation_cohort,rs.activation_cohort)
                                       AND rr.shopped_channel = COALESCE(o.shopped_channel,r.shopped_channel,c.shopped_channel,rpc.shopped_channel,e.shopped_channel,rs.shopped_channel)
                                       AND rr.order_store_id = COALESCE(o.order_store_id,r.refund_store_id,c.chargeback_store_id,rpc.return_store_id,e.exchange_store_id,rs.reship_store_id)
                                       AND rr.membership_order_type = COALESCE(o.membership_order_type,r.membership_order_type,c.membership_order_type,rpc.membership_order_type,e.membership_order_type,rs.membership_order_type)
                                       AND rr.is_retail_ship_only_order = COALESCE(o.is_retail_ship_only_order,r.is_retail_ship_only_order,c.is_retail_ship_only_order,rpc.is_retail_ship_only_order,e.is_retail_ship_only_order,rs.is_retail_ship_only_order)
                                       AND rr.previous_membership_status = COALESCE(o.previous_membership_status,r.previous_membership_status,c.previous_membership_status,rpc.previous_membership_status,e.previous_membership_status,rs.previous_membership_status)
FULL JOIN _financial_retail_exchanges re ON to_date(re.date) = to_date(COALESCE(o.order_date,r.refund_date,c.chargeback_date,rpc.return_date,e.exchange_date,rs.reship_date,rr.return_date))
                                       AND re.administrator_id = COALESCE(o.administrator_id,r.administrator_id,c.administrator_id,rpc.administrator_id,e.administrator_id,rs.administrator_id,rr.administrator_id)
                                       AND re.activation_store_id = COALESCE(o.activation_store_id,r.activation_store_id,c.activation_store_id,rpc.activation_store_id,e.activation_store_id,rs.activation_store_id,rr.activation_store_id)
                                       AND re.activation_cohort = COALESCE(o.activation_cohort,r.activation_cohort,c.activation_cohort,rpc.activation_cohort,e.activation_cohort,rs.activation_cohort,rr.activation_cohort)
                                       AND re.shopped_channel = COALESCE(o.shopped_channel,r.shopped_channel,c.shopped_channel,rpc.shopped_channel,e.shopped_channel,rs.shopped_channel,rr.shopped_channel)
                                       AND re.order_store_id = COALESCE(o.order_store_id,r.refund_store_id,c.chargeback_store_id,rpc.return_store_id,e.exchange_store_id,rs.reship_store_id,rr.order_store_id)
                                       AND re.membership_order_type = COALESCE(o.membership_order_type,r.membership_order_type,c.membership_order_type,rpc.membership_order_type,e.membership_order_type,rs.membership_order_type,rr.membership_order_type)
                                       AND re.is_retail_ship_only_order = COALESCE(o.is_retail_ship_only_order,r.is_retail_ship_only_order,c.is_retail_ship_only_order,rpc.is_retail_ship_only_order,e.is_retail_ship_only_order,rs.is_retail_ship_only_order,rr.is_retail_ship_only_order)
                                       AND re.previous_membership_status = COALESCE(o.previous_membership_status,r.previous_membership_status,c.previous_membership_status,rpc.previous_membership_status,e.previous_membership_status,rs.previous_membership_status,rr.previous_membership_status)
LEFT JOIN _store_dimensions arsd ON arsd.store_id = COALESCE(o.activation_store_id,r.activation_store_id,c.activation_store_id,rpc.activation_store_id,e.activation_store_id,rs.activation_store_id,rr.activation_store_id,re.activation_store_id)
LEFT JOIN _store_dimensions orsd ON orsd.store_id = COALESCE(o.order_store_id,r.refund_store_id,c.chargeback_store_id,rpc.return_store_id,e.exchange_store_id,rs.reship_store_id,rr.order_store_id,re.order_store_id)
GROUP BY  to_date(COALESCE(o.order_date,r.refund_date,c.chargeback_date,rpc.return_date,e.exchange_date,rs.reship_date,rr.return_date,re.date)),
        COALESCE(o.administrator_id,r.administrator_id,c.administrator_id,rpc.administrator_id,e.administrator_id,rs.administrator_id,rr.administrator_id,re.administrator_id),
        COALESCE(o.customer_activation_channel,r.customer_activation_channel,c.customer_activation_channel,rpc.customer_activation_channel,e.customer_activation_channel,rs.customer_activation_channel,rr.customer_activation_channel,re.customer_activation_channel),
        CASE WHEN datediff(month,ifnull(arsd.store_open_date,'2079-01-01'), to_date(coalesce(o.order_date,r.refund_date,c.chargeback_date,rpc.return_date,e.exchange_date,rs.reship_date,rr.return_date,re.date))) >= 13 THEN 'Comp' ELSE 'New' END,
        COALESCE(o.activation_store_id,r.activation_store_id,c.activation_store_id,rpc.activation_store_id,e.activation_store_id,rs.activation_store_id,rr.activation_store_id,re.activation_store_id),
        COALESCE(o.shopped_channel,r.shopped_channel,c.shopped_channel,rpc.shopped_channel,e.shopped_channel,rs.shopped_channel,rr.shopped_channel,re.shopped_channel),
        COALESCE(o.order_store_type,r.refund_store_type,c.chargeback_store_type,rpc.return_store_type,e.exchange_store_type,rs.reship_store_type,rr.order_store_type,re.order_store_type),
        COALESCE(o.order_store_id,r.refund_store_id,c.chargeback_store_id,rpc.return_store_id,e.exchange_store_id,rs.reship_store_id,rr.order_store_id,re.order_store_id),
        CASE WHEN datediff(month,ifnull(orsd.store_open_date,'2079-01-01'), to_date(coalesce(o.order_date,r.refund_date,c.chargeback_date,rpc.return_date,e.exchange_date,rs.reship_date,rr.return_date,re.date))) >= 13 THEN 'Comp' ELSE 'New' END,
        COALESCE(o.membership_order_type,r.membership_order_type,c.membership_order_type,rpc.membership_order_type,e.membership_order_type,rs.membership_order_type,rr.membership_order_type,re.membership_order_type),
        COALESCE(o.is_retail_ship_only_order,r.is_retail_ship_only_order,c.is_retail_ship_only_order,rpc.is_retail_ship_only_order,e.is_retail_ship_only_order,rs.is_retail_ship_only_order,rr.is_retail_ship_only_order,re.is_retail_ship_only_order),
        o.latest_order
);

CREATE or REPLACE TEMPORARY TABLE _final_final_temp AS
(
SELECT '4-Wall' AS metric_type,
       'Financial' AS metric_method,
       shopped_channel,
       order_date AS date,
       f.administrator_id,
       a.firstname,
       a.lastname,
       order_store_id AS store_id,
       st.store_full_name AS store_name,
       st.store_region as region,
       zcs.longitude AS longitude,
       zcs.latitude AS latitude,
       st.store_retail_zip_code AS store_zip,
       st.store_retail_city AS store_city,
       st.store_retail_state AS store_state,
       orsd.store_open_date AS store_opening_date,
       orsd.store_number,
       CASE WHEN st.store_type ilike 'Retail' THEN st.store_sub_type ELSE 'Online' END AS store_retail_type,
       order_store_type AS store_type,
       order_comp_new AS comp_new,
       membership_order_type,
       is_retail_ship_only_order,
       customer_activation_channel,
       latest_order,
       SUM(CASE WHEN order_store_type = 'Retail' THEN orders ELSE 0 END) AS orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN units ELSE 0 END) AS units,
       SUM(CASE WHEN order_store_type = 'Retail' THEN gift_card_orders ELSE 0 END) as gift_card_orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN orders_with_credit_redemption ELSE 0 END) as orders_with_credit_redemption,
       SUM(CASE WHEN order_store_type = 'Retail' THEN product_gross_revenue ELSE 0 END) AS product_gross_revenue,
       SUM(CASE WHEN order_store_type = 'Retail' THEN cash_gross_revenue ELSE 0 END) AS cash_gross_revenue,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_product_net_revenue ELSE 0 END) AS product_net_revenue,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_cash_net_revenue ELSE 0 END) AS cash_net_revenue,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_product_gross_profit ELSE 0 END) AS product_gross_profit,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_cash_gross_profit ELSE 0 END) AS cash_gross_profit,
       SUM(CASE WHEN order_store_type = 'Retail' THEN shipping_revenue ELSE 0 END) AS shipping_revenue,
       SUM(CASE WHEN order_store_type = 'Retail' THEN shipping_cost ELSE 0 END) AS shipping_cost,
       SUM(CASE WHEN order_store_type = 'Retail' THEN discount ELSE 0 END) AS discount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN product_cost_amount ELSE 0 END) AS product_cost_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN product_subtotal_amount ELSE 0 END) AS product_subtotal_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN non_cash_credit_amount ELSE 0 END) AS non_cash_credit_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN credit_redemption_amount ELSE 0 END) AS credit_redemption_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN credit_redemption_count ELSE 0 END) AS credit_redemption_count,
       SUM(CASE WHEN order_store_type = 'Retail' THEN gift_card_issuance_amount ELSE 0 END) AS gift_card_issuance_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN credit_issuance_amount ELSE 0 END) AS credit_issuance_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN credit_issuance_count ELSE 0 END) AS credit_issuance_count,
       SUM(CASE WHEN order_store_type = 'Retail' THEN gift_card_redemption_amount ELSE 0 END) AS gift_card_redemption_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN conversion_eligible_orders ELSE 0 END) AS conversion_eligible_orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN paygo_activations ELSE 0 END) AS paygo_activations,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_product_order_cash_refund_amount ELSE 0 END) AS product_order_cash_refund_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_product_order_cash_credit_refund_amount ELSE 0 END) AS product_order_cash_credit_refund_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_billing_cash_refund_amount ELSE 0 END) AS billing_cash_refund_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_billing_cash_chargeback_amount ELSE 0 END) AS billing_cash_chargeback_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_product_order_cash_chargeback_amount ELSE 0 END) AS product_order_cash_chargeback_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_reship_orders ELSE 0 END) AS reship_orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_reship_units ELSE 0 END) AS reship_units,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_reship_product_cost ELSE 0 END) AS reship_product_cost,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_exchange_orders ELSE 0 END) AS exchange_orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_exchange_units ELSE 0 END) AS exchange_units,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_exchange_product_cost ELSE 0 END) AS exchange_product_cost,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_return_product_cost_local_amount ELSE 0 END) AS return_product_cost,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_return_shipping_cost_amount ELSE 0 END) AS return_shipping_cost,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_online_purchase_retail_returns ELSE 0 END) AS online_purchase_retail_returns,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_retail_purchase_retail_returns ELSE 0 END) AS retail_purchase_retail_returns,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_retail_purchase_online_returns ELSE 0 END) AS retail_purchase_online_returns,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_online_purchase_retail_return_orders ELSE 0 END) AS online_purchase_retail_return_orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_retail_purchase_retail_return_orders ELSE 0 END) AS retail_purchase_retail_return_orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_retail_purchase_online_return_orders ELSE 0 END) AS retail_purchase_online_return_orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_total_exchanges ELSE 0 END) AS total_exchanges,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_exchanges_after_online_return ELSE 0 END) AS exchanges_after_online_return,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_exchanges_after_in_store_return ELSE 0 END) AS exchanges_after_in_store_return,
       SUM(CASE WHEN order_store_type = 'Retail' THEN exchanges_after_in_store_return_from_online_purchase ELSE 0 END) AS exchanges_after_in_store_return_from_online_purchase,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_total_exchanges_value ELSE 0 END) AS total_exchanges_value,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_exchanges_value_after_online_return ELSE 0 END) AS exchanges_value_after_online_return,
       SUM(CASE WHEN order_store_type = 'Retail' THEN financial_exchanges_value_after_in_store_return ELSE 0 END) AS exchanges_value_after_in_store_return,
       SUM(CASE WHEN order_store_type = 'Retail' THEN exchanges_value_after_in_store_return_from_online_purchase ELSE 0 END) AS exchanges_value_after_in_store_return_from_online_purchase,
       '0' AS incoming_visitor_traffic,
       '0' AS exiting_visitor_traffic
FROM _final_temp f
LEFT JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = f.order_store_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.zip_city_state zcs ON zcs.zip = st.store_retail_zip_code
LEFT JOIN _store_dimensions orsd ON orsd.store_id = f.order_store_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.administrator a ON a.administrator_id=f.administrator_id
GROUP BY '4-Wall',
        'Financial',
       shopped_channel,
       order_date,
       f.administrator_id,
       a.firstname,
       a.lastname,
       order_store_id,
       st.store_full_name,
       st.store_region,
       zcs.longitude,
       zcs.latitude,
       st.store_retail_zip_code,
       st.store_retail_city,
       st.store_retail_state,
       orsd.store_open_date,
       orsd.store_number,
       CASE WHEN st.store_type ilike 'Retail' THEN st.store_sub_type ELSE 'Online' END,
       order_store_type,
       order_comp_new,
       membership_order_type,
       is_retail_ship_only_order,
       customer_activation_channel,
       latest_order
UNION
SELECT 'Attribution' AS metric_type,
    'Financial' AS metric_method,
    shopped_channel,
    order_date AS date,
    f.administrator_id,
    a.firstname,
    a.lastname,
    f.activation_store_id AS store_id,
    st.store_full_name AS store_name,
    st.store_region as region,
    rv.activation_store_longitude AS longitude,
    rv.activation_store_latitude AS latitude,
    rv.activation_store_zip_code AS store_zip,
    rv.activation_store_city AS store_city,
    rv.activation_store_state AS store_state,
    arsd.store_open_date AS store_opening_date,
    arsd.store_number,
    CASE WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type
        ELSE 'Online'
       END AS store_retail_type,
    customer_activation_channel AS store_type,
    activation_comp_new AS comp_new,
    membership_order_type,
    is_retail_ship_only_order,
    customer_activation_channel,
    latest_order,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN orders ELSE 0 END) AS orders,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN units ELSE 0 END) AS units,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN gift_card_orders ELSE 0 END) as gift_card_orders,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN orders_with_credit_redemption ELSE 0 END) as orders_with_credit_redemption,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN product_gross_revenue ELSE 0 END ) AS product_gross_revenue,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN cash_gross_revenue ELSE 0 END) AS cash_gross_revenue,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_product_net_revenue ELSE 0 END) AS product_net_revenue,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_cash_net_revenue ELSE 0 END) AS cash_net_revenue,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_product_gross_profit ELSE 0 END) AS product_gross_profit,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_cash_gross_profit ELSE 0 END) AS cash_gross_profit,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN shipping_revenue ELSE 0 END) AS shipping_revenue,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN shipping_cost ELSE 0 END) AS shipping_cost,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN discount ELSE 0 END) AS discount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN product_cost_amount ELSE 0 END) AS product_cost_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN product_subtotal_amount ELSE 0 END) AS product_subtotal_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN non_cash_credit_amount ELSE 0 END) AS non_cash_credit_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN credit_redemption_amount ELSE 0 END) AS credit_redemption_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN credit_redemption_count ELSE 0 END) AS credit_redemption_count,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN gift_card_issuance_amount ELSE 0 END) AS gift_card_issuance_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN credit_issuance_amount ELSE 0 END) AS credit_issuance_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN credit_issuance_count ELSE 0 END) AS credit_issuance_count,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN gift_card_redemption_amount ELSE 0 END) AS gift_card_redemption_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN conversion_eligible_orders ELSE 0 END) AS conversion_eligible_orders,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN paygo_activations ELSE 0 END) AS paygo_activations,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_product_order_cash_refund_amount ELSE 0 END) AS product_order_cash_refund_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_product_order_cash_credit_refund_amount ELSE 0 END) AS product_order_cash_credit_refund_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_billing_cash_refund_amount ELSE 0 END) AS billing_cash_refund_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_billing_cash_chargeback_amount ELSE 0 END) AS billing_cash_chargeback_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_product_order_cash_chargeback_amount ELSE 0 END) AS product_order_cash_chargeback_amount,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_reship_orders ELSE 0 END) AS reship_orders,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_reship_units ELSE 0 END) AS reship_units,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_reship_product_cost ELSE 0 END) AS reship_product_cost,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_exchange_orders ELSE 0 END) AS exchange_orders,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_exchange_units ELSE 0 END) AS exchange_units,
    SUM(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_exchange_product_cost ELSE 0 END) AS exchange_product_cost,
    SUM(ifnull(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_return_product_cost_local_amount ELSE 0 END,0)) AS return_product_cost,
    SUM(ifnull(CASE WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN financial_return_shipping_cost_amount ELSE 0 END,0)) AS return_shipping_cost,
    '0' AS online_purchase_retail_returns,
    '0' AS retail_purchase_retail_returns,
    '0' AS retail_purchase_online_returns,
    '0' AS online_purchase_retail_return_orders,
    '0' AS retail_purchase_retail_return_orders,
    '0' AS retail_purchase_online_return_orders,
    '0' AS total_exchanges,
    '0' AS exchanges_after_online_return,
    '0' AS exchanges_after_in_store_return,
    '0' AS exchanges_after_in_store_return_from_online_purchase,
    '0' AS total_exchanges_value,
    '0' AS exchanges_value_after_online_return,
    '0' AS exchanges_value_after_in_store_return,
    '0' AS exchanges_value_after_in_store_return_from_online_purchase,
    '0' AS incoming_visitor_traffic,
    '0' AS exiting_visitor_traffic
FROM _final_temp f
LEFT JOIN (SELECT DISTINCT activation_store_id,
               activation_store_state,
               activation_store_longitude,
               activation_store_latitude,
               activation_store_zip_code,
               activation_store_city
           FROM _vip_activation_detail)rv ON f.activation_store_id = rv.activation_store_id
LEFT JOIN _store_dimensions arsd ON arsd.store_id = f.activation_store_id
LEFT JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = f.activation_store_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.administrator a ON a.administrator_id=f.administrator_id
GROUP BY 'Attribution',
     'Financial',
    shopped_channel,
    order_date,
    f.administrator_id,
    a.firstname,
    a.lastname,
    f.activation_store_id,
    st.store_full_name,
    st.store_region,
    rv.activation_store_longitude,
    rv.activation_store_latitude,
    rv.activation_store_zip_code,
    rv.activation_store_city,
    rv.activation_store_state,
    arsd.store_open_date,
    arsd.store_number,
    CASE WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type
        ELSE 'Online'
       END,
    customer_activation_channel,
    activation_comp_new,
    membership_order_type,
    is_retail_ship_only_order,
    customer_activation_channel,
    latest_order

UNION

SELECT '4-Wall' AS metric_type,
    'Financial' AS metric_method,
    null AS shopped_channel,
    date,
    null AS administrator_id,
    null as firstname,
    null as lastname,
    rt.store_id,
    st.store_full_name AS store_name,
    st.store_region as region,
    zcs.longitude AS longitude,
    zcs.latitude AS latitude,
    st.store_retail_zip_code AS store_zip,
    st.store_retail_city AS store_city,
    st.store_retail_state AS store_state,
    rsd.store_open_date AS store_opening_date,
    rsd.store_number,
    CASE WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type
        WHEN st.store_type ILIKE 'Online' THEN 'Online'
        WHEN st.store_type ILIKE 'Mobile App' THEN 'Online'
       END AS store_retail_type,
    st.store_type AS store_type,
    CASE WHEN datediff(month,rsd.store_open_date,date) >= 13 THEN 'Comp' ELSE 'New' END AS comp_new,
    null AS membership_order_type,
    null AS is_retail_ship_only_order,
    null AS customer_activation_channel,
    null as latest_order,
    '0' AS orders,
    '0' AS units,
    '0' AS gift_card_orders,
    '0' AS orders_with_credit_redemption,
    '0' AS product_gross_revenue,
    '0' AS cash_gross_revenue,
    '0' AS product_net_revenue,
    '0' AS cash_net_revenue,
    '0' AS product_gross_profit,
    '0' AS cash_gross_profit,
    '0' AS shipping_revenue,
    '0' AS shipping_cost,
    '0' AS discount,
    '0' AS product_cost_amount,
    '0' AS product_subtotal_amount,
    '0' AS non_cash_credit_amount,
    '0' AS credit_redemption_amount,
    '0' AS credit_redemption_count,
    '0' AS gift_card_issuance_amount,
    '0' AS credit_issuance_amount,
    '0' AS credit_issuance_count,
    '0' AS gift_card_redemption_amount,
    '0' AS conversion_eligible_orders,
    '0' AS paygo_activations,
    '0' AS product_order_cash_refund_amount,
    '0' AS product_order_cash_credit_refund_amount,
    '0' AS billing_cash_refund_amount,
    '0' AS billing_cash_chargeback_amount,
    '0' AS product_order_cash_chargeback_amount,
    '0' AS reship_orders,
    '0' AS reship_units,
    '0' AS reship_product_cost,
    '0' AS exchange_orders,
    '0' AS exchange_units,
    '0' AS exchange_product_cost,
    '0' AS return_product_cost,
    '0' AS return_shipping_cost,
    '0' AS online_purchase_retail_returns,
    '0' AS retail_purchase_retail_returns,
    '0' AS retail_purchase_online_returns,
    '0' AS online_purchase_retail_return_orders,
    '0' AS retail_purchase_retail_return_orders,
    '0' AS retail_purchase_online_return_orders,
    '0' AS total_exchanges,
    '0' AS exchanges_after_online_return,
    '0' AS exchanges_after_in_store_return,
    '0' AS exchanges_after_in_store_return_from_online_purchase,
    '0' AS total_exchanges_value,
    '0' AS exchanges_value_after_online_return,
    '0' AS exchanges_value_after_in_store_return,
    '0' AS exchanges_value_after_in_store_return_from_online_purchase,
    SUM(incoming_visitor_traffic) AS incoming_visitor_traffic,
    SUM(exiting_visitor_traffic) AS exiting_visitor_traffic
FROM _retail_traffic rt
JOIN EDW_PROD.DATA_MODEL_SXF.dim_store st ON st.store_id = rt.store_id
LEFT JOIN _store_dimensions rsd ON rsd.store_id = rt.store_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.zip_city_state zcs ON zcs.zip = st.store_retail_zip_code
WHERE st.STORE_BRAND_ABBR='SX'
GROUP BY
    '4-Wall',
    'Financial',
    null ,
    date,
    null ,
    null,
    null,
    rt.store_id,
    st.store_full_name ,
    st.store_region ,
    zcs.longitude ,
    zcs.latitude ,
    st.store_retail_zip_code ,
    st.store_retail_city ,
    st.store_retail_state ,
    rsd.store_open_date,
    rsd.store_number,
    CASE WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type
        WHEN st.store_type ILIKE 'Online' THEN 'Online'
        WHEN st.store_type ILIKE 'Mobile App' THEN 'Online'
    END,
    st.store_type ,
    CASE WHEN datediff(month,rsd.store_open_date,date) > 13 THEN 'Comp' ELSE 'New' END,
    null ,
    null ,
    null,
    null
);

TRUNCATE TABLE work.dbo.retail_attribution;

INSERT INTO work.dbo.retail_attribution
(
    metric_type
    ,metric_method
    ,shopped_channel
    ,date
    ,administrator_id
    ,firstname
    ,lastname
    ,store_id
    ,longitude
    ,latitude
    ,store_name
    ,region
    ,store_zip
    ,store_city
    ,store_state
    ,store_opening_date
    ,store_number
    ,store_retail_type
    ,store_type
    ,comp_new
    ,membership_order_type
    ,is_retail_ship_only_order
    ,customer_activation_channel
    ,latest_order
    ,orders
    ,units
    ,gift_card_orders
    ,orders_with_credit_redemption
    ,product_gross_revenue
    ,cash_gross_revenue
    ,product_net_revenue
    ,cash_net_revenue
    ,product_gross_profit
    ,cash_gross_profit
    ,shipping_revenue
    ,shipping_cost
    ,discount
    ,product_cost_amount
    ,product_subtotal_amount
    ,non_cash_credit_amount
    ,credit_redemption_amount
    ,credit_redemption_count
    ,gift_card_issuance_amount
    ,credit_issuance_amount
    ,credit_issuance_count
    ,gift_card_redemption_amount
    ,conversion_eligible_orders
    ,paygo_activations
    ,product_order_cash_refund_amount
    ,product_order_cash_credit_refund_amount
    ,billing_cash_refund_amount
    ,billing_cash_chargeback_amount
    ,product_order_cash_chargeback_amount
    ,reship_orders
    ,reship_units
    ,reship_product_cost
    ,exchange_orders
    ,exchange_units
    ,exchange_product_cost
    ,return_product_cost
    ,return_shipping_cost
    ,online_purchase_retail_returns
    ,retail_purchase_retail_returns
    ,retail_purchase_online_returns
    ,online_purchase_retail_return_orders
    ,retail_purchase_retail_return_orders
    ,retail_purchase_online_return_orders
    ,total_exchanges
    ,exchanges_after_online_return
    ,exchanges_after_in_store_return
    ,exchanges_after_in_store_return_from_online_purchase
    ,total_exchanges_value
    ,exchanges_value_after_online_return
    ,exchanges_value_after_in_store_return
    ,exchanges_value_after_in_store_return_from_online_purchase
    ,incoming_visitor_traffic
    ,exiting_visitor_traffic
    ,meta_create_datetime
    ,meta_update_datetime
  )
SELECT metric_type,
    metric_method,
    shopped_channel,
    date,
    administrator_id,
    firstname,
    lastname,
    store_id,
    longitude,
    latitude,
    store_name,
    region,
    store_zip,
    store_city,
    store_state,
    store_opening_date,
    store_number,
    CASE WHEN store_retail_type ilike 'Partnership' THEN 'Activations' ELSE store_retail_type END AS store_retail_type,
    store_type,
    comp_new,
    membership_order_type,
    CASE WHEN is_retail_ship_only_order = 1 THEN 'Yes'
        WHEN is_retail_ship_only_order = 0 THEN 'No'
        ELSE 'Keep Checked for Traffic' END AS is_retail_ship_only_order,
    customer_activation_channel,
    latest_order,
    SUM(orders) AS orders,
    SUM(units) AS units,
    SUM(gift_card_orders) as gift_card_orders,
    SUM(orders_with_credit_redemption) as orders_with_credit_redemption,
    SUM(product_gross_revenue) AS product_gross_revenue,
    SUM(cash_gross_revenue) AS cash_gross_revenue,
    SUM(product_net_revenue) AS product_net_revenue,
    SUM(cash_net_revenue) AS cash_net_revenue,
    SUM(product_gross_profit) AS product_gross_profit,
    SUM(cash_gross_profit) AS cash_gross_profit,
    SUM(shipping_revenue) AS shipping_revenue,
    SUM(shipping_cost) AS shipping_cost,
    SUM(discount) AS discount,
    SUM(product_cost_amount) AS product_cost_amount,
    SUM(product_subtotal_amount) as product_subtotal_amount,
    SUM(non_cash_credit_amount) as non_cash_credit_amount,
    SUM(credit_redemption_amount) AS credit_redemption_amount,
    SUM(credit_redemption_count) AS credit_redemption_count,
    SUM(gift_card_issuance_amount) AS gift_card_issuance_amount,
    SUM(credit_issuance_amount) AS credit_issuance_amount,
    SUM(credit_issuance_count) AS credit_issuance_count,
    SUM(gift_card_redemption_amount) AS gift_card_redemption_amount,
    SUM(conversion_eligible_orders) AS conversion_eligible_orders,
    SUM(paygo_activations) AS paygo_activations,
    SUM(product_order_cash_refund_amount) AS product_order_cash_refund_amount,
    SUM(product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount,
    SUM(billing_cash_refund_amount) AS billing_cash_refund_amount,
    SUM(billing_cash_chargeback_amount) AS billing_cash_chargeback_amount,
    SUM(product_order_cash_chargeback_amount) AS product_order_cash_chargeback_amount,
    SUM(reship_orders) AS reship_orders,
    SUM(reship_units) AS reship_units,
    SUM(reship_product_cost) AS reship_product_cost,
    SUM(exchange_orders) AS exchange_orders,
    SUM(exchange_units) AS exchange_units,
    SUM(exchange_product_cost) AS exchange_product_cost,
    SUM(return_product_cost) AS return_product_cost,
    SUM(return_shipping_cost) AS return_shipping_cost,
    SUM(online_purchase_retail_returns) AS online_purchase_retail_returns,
    SUM(retail_purchase_retail_returns) AS retail_purchase_retail_returns,
    SUM(retail_purchase_online_returns) AS retail_purchase_online_returns,
    SUM(online_purchase_retail_return_orders) AS online_purchase_retail_return_orders,
    SUM(retail_purchase_retail_return_orders) AS retail_purchase_retail_return_orders,
    SUM(retail_purchase_online_return_orders) AS retail_purchase_online_return_orders,
    SUM(total_exchanges) AS total_exchanges,
    SUM(exchanges_after_online_return) AS exchanges_after_online_return,
    SUM(exchanges_after_in_store_return) AS exchanges_after_in_store_return,
    SUM(exchanges_after_in_store_return_from_online_purchase) AS exchanges_after_in_store_return_from_online_purchase,
    SUM(total_exchanges_value) AS total_exchanges_value,
    SUM(exchanges_value_after_online_return) AS exchanges_value_after_online_return,
    SUM(exchanges_value_after_in_store_return) AS exchanges_value_after_in_store_return,
    SUM(exchanges_value_after_in_store_return_from_online_purchase) AS exchanges_value_after_in_store_return_from_online_purchase,
    SUM(incoming_visitor_traffic) AS incoming_visitor_traffic,
    SUM(exiting_visitor_traffic) AS exiting_visitor_traffic,
    $execution_start_time AS meta_update_time,
    $execution_start_time AS meta_create_time
FROM _final_final_temp f
WHERE metric_type = '4-Wall'
    AND date >= '2018-01-01'
    AND date < current_date()+1
    AND store_type = 'Retail'
GROUP BY metric_type,
    metric_method,
    shopped_channel,
    date,
    administrator_id,
    firstname,
    lastname,
    store_id,
    longitude,
    latitude,
    store_name,
    region,
    store_zip,
    store_city,
    store_state,
    store_opening_date,
    store_number,
    CASE WHEN store_retail_type ilike 'Partnership' THEN 'Activations' ELSE store_retail_type END,
    store_type,
    comp_new,
    membership_order_type,
    CASE WHEN is_retail_ship_only_order = 1 THEN 'Yes'
        WHEN is_retail_ship_only_order = 0 THEN 'No'
        ELSE 'Keep Checked for Traffic' END,
    customer_activation_channel,
    latest_order
UNION
SELECT '4-Wall with Attribution' AS metric_type,
   metric_method,
   shopped_channel,
   date,
   administrator_id,
   firstname,
   lastname,
   store_id,
   longitude,
   latitude,
   store_name,
   region,
   store_zip,
   store_city,
   store_state,
   store_opening_date,
   store_number,
   CASE WHEN store_retail_type ilike 'Partnership' THEN 'Activations' ELSE store_retail_type END AS store_retail_type,
   store_type,
   comp_new,
   membership_order_type,
   CASE WHEN is_retail_ship_only_order = 1 THEN 'Yes'
        WHEN is_retail_ship_only_order = 0 THEN 'No'
        ELSE 'Keep Checked for Traffic' END AS is_retail_ship_only_order,
   customer_activation_channel,
   latest_order,
   SUM(orders) AS orders,
   SUM(units) AS units,
   SUM(gift_card_orders) as gift_card_orders,
   SUM(orders_with_credit_redemption) as orders_with_credit_redemption,
   SUM(product_gross_revenue) AS product_gross_revenue,
   SUM(cash_gross_revenue) AS cash_gross_revenue,
   SUM(product_net_revenue) AS product_net_revenue,
   SUM(cash_net_revenue) AS cash_net_revenue,
   SUM(product_gross_profit) AS product_gross_profit,
   SUM(cash_gross_profit) AS cash_gross_profit,
   SUM(shipping_revenue) AS shipping_revenue,
   SUM(shipping_cost) AS shipping_cost,
   SUM(discount) AS discount,
   SUM(product_cost_amount) AS product_cost_amount,
   SUM(product_subtotal_amount) as product_subtotal_amount,
   SUM(non_cash_credit_amount) as non_cash_credit_amount,
   SUM(credit_redemption_amount) AS credit_redemption_amount,
   SUM(credit_redemption_count) AS credit_redemption_count,
   SUM(gift_card_issuance_amount) AS gift_card_issuance_amount,
   SUM(credit_issuance_amount) AS credit_issuance_amount,
   SUM(credit_issuance_count) AS credit_issuance_count,
   SUM(gift_card_redemption_amount) AS gift_card_redemption_amount,
   SUM(conversion_eligible_orders) AS conversion_eligible_orders,
   SUM(paygo_activations) AS payg_activations,
   SUM(product_order_cash_refund_amount) AS product_order_cash_refund_amount,
   SUM(product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount,
   SUM(billing_cash_refund_amount) AS billing_cash_refund_amount,
   SUM(billing_cash_chargeback_amount) AS billing_cash_chargeback_amount,
   SUM(product_order_cash_chargeback_amount) AS product_order_cash_chargeback_amount,
   SUM(reship_orders) AS reship_orders,
   SUM(reship_units) AS reship_units,
   SUM(reship_product_cost) AS reship_product_cost,
   SUM(exchange_orders) AS exchange_orders,
   SUM(exchange_units) AS exchange_units,
   SUM(exchange_product_cost) AS exchange_product_cost,
   SUM(return_product_cost) AS return_product_cost,
   SUM(return_shipping_cost) AS return_shipping_cost,
   SUM(online_purchase_retail_returns) AS online_purchase_retail_returns,
   SUM(retail_purchase_retail_returns) AS retail_purchase_retail_returns,
   SUM(retail_purchase_online_returns) AS retail_purchase_online_returns,
   SUM(online_purchase_retail_return_orders) AS online_purchase_retail_return_orders,
   SUM(retail_purchase_retail_return_orders) AS retail_purchase_retail_return_orders,
   SUM(retail_purchase_online_return_orders) AS retail_purchase_online_return_orders,
   SUM(total_exchanges) AS total_exchanges,
   SUM(exchanges_after_online_return) AS exchanges_after_online_return,
   SUM(exchanges_after_in_store_return) AS exchanges_after_in_store_return,
   SUM(exchanges_after_in_store_return_from_online_purchase) AS exchanges_after_in_store_return_from_online_purchase,
   SUM(total_exchanges_value) AS total_exchanges_value,
   SUM(exchanges_value_after_online_return) AS exchanges_value_after_online_return,
   SUM(exchanges_value_after_in_store_return) AS exchanges_value_after_in_store_return,
   SUM(exchanges_value_after_in_store_return_from_online_purchase) AS exchanges_value_after_in_store_return_from_online_purchase,
   SUM(incoming_visitor_traffic) AS incoming_visitor_traffic,
   SUM(exiting_visitor_traffic) AS exiting_visitor_traffic,
    $execution_start_time AS meta_update_time,
    $execution_start_time AS meta_create_time
FROM _final_final_temp f
WHERE metric_type IN ('4-Wall','Attribution')
    AND date >= '2018-01-01'
    AND date < current_date()+1
GROUP BY '4-Wall with Attribution',
   metric_method,
   shopped_channel,
   date,
   administrator_id,
   firstname,
   lastname,
   store_id,
   longitude,
   latitude,
   store_name,
   region,
   store_zip,
   store_city,
   store_state,
   store_opening_date,
   store_number,
   CASE WHEN store_retail_type ilike 'Partnership' THEN 'Activations' ELSE store_retail_type END,
   store_type,
   comp_new,
   membership_order_type,
   CASE WHEN is_retail_ship_only_order = 1 THEN 'Yes'
        WHEN is_retail_ship_only_order = 0 THEN 'No'
        ELSE 'Keep Checked for Traffic' END,
   customer_activation_channel,
   latest_order;
