SET last_refreshed_datetime = (
    SELECT MIN(max_date) AS refresh_time
    FROM (
        SELECT MAX(meta_update_datetime) AS max_date
        FROM edw_prod.data_model_fl.fact_activation
        UNION ALL
        SELECT MAX(meta_update_datetime) AS max_date
        FROM edw_prod.data_model_fl.fact_order_line
        UNION ALL
        SELECT MAX(meta_update_datetime) AS max_date
        FROM edw_prod.data_model_fl.fact_order
    )
);

CREATE OR REPLACE TEMPORARY TABLE _omni_customer_orders AS
SELECT o.customer_id,
    rv.activation_key,
    o.order_local_datetime,
    CASE WHEN st.store_type ILIKE 'Retail' THEN 'retail' ELSE 'online' END AS order_store_type,
    COUNT(DISTINCT o.order_id)                                             AS product_orders
FROM edw_prod.data_model_fl.fact_order o
JOIN edw_prod.data_model_fl.dim_store st ON st.store_id = o.store_id
    AND st.store_brand_abbr = 'FL'
JOIN edw_prod.data_model_fl.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
    AND osc.order_classification_l2 = 'Product Order'
JOIN edw_prod.data_model_fl.dim_order_status os ON os.order_status_key = o.order_status_key
    AND order_status IN ('Success', 'Pending')
JOIN edw_prod.data_model_fl.dim_customer c ON c.customer_id = o.customer_id
LEFT JOIN edw_prod.data_model_fl.fact_activation rv ON rv.customer_id = o.customer_id
    AND o.order_local_datetime BETWEEN rv.activation_local_datetime AND rv.next_activation_local_datetime
GROUP BY o.customer_id,
    rv.activation_key,
    o.order_local_datetime,
    CASE WHEN st.store_type ILIKE 'Retail' THEN 'retail' ELSE 'online' END;

CREATE OR REPLACE TEMPORARY TABLE _omni_classification AS
SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_id, activation_key ORDER BY order_local_datetime) AS r_no2
FROM (
    SELECT customer_id,
        activation_key,
        order_store_type,
        order_local_datetime,
        product_orders,
        ROW_NUMBER() OVER (PARTITION BY customer_id,activation_key,order_store_type ORDER BY order_local_datetime) AS r_no
    FROM _omni_customer_orders -- change to your table name
)
WHERE r_no = 1;

CREATE OR REPLACE TEMPORARY TABLE _omni_set_up AS
SELECT *,
    CASE WHEN r_no2 = 1 THEN order_store_type WHEN r_no2 = 2 THEN 'omni' ELSE 'null' END AS channel
FROM _omni_classification;

CREATE OR REPLACE TEMPORARY TABLE _omni_ranks AS
SELECT customer_id,
    activation_key,
    order_store_type,
    order_local_datetime,
    channel,
    ROW_NUMBER() OVER (PARTITION BY customer_id, activation_key ORDER BY order_local_datetime) AS rn,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_local_datetime)                 AS rn2
FROM _omni_set_up
ORDER BY activation_key,
    order_local_datetime
;

CREATE OR REPLACE TEMPORARY TABLE _omni_combine_datetimes AS
SELECT *,
    LEAD(order_local_datetime) OVER (PARTITION BY
       customer_id,
       activation_key
       ORDER BY rn
    )  AS order_local_datetime_end,
    LEAD(order_local_datetime) OVER (PARTITION BY
       customer_id
       ORDER BY rn2
    ) AS order_local_datetime_end2
FROM _omni_ranks
;

CREATE OR REPLACE TEMPORARY TABLE _omni AS
SELECT customer_id,
    activation_key,
    channel,
    order_local_datetime                                                                AS order_local_datetime_start,
    IFNULL(COALESCE(order_local_datetime_end, order_local_datetime_end2), '2079-01-01') AS order_local_datetime_end
FROM _omni_combine_datetimes
ORDER BY activation_key,
    order_local_datetime_start
;

CREATE OR REPLACE TEMPORARY TABLE _retail_orders AS --retail orders and includes ship only orders (same logic from old mstr report)
SELECT o.order_id,
    o.store_id                                    AS order_store_id,
    st.store_type                                 AS order_store_type,
    o.customer_id,
    o.administrator_id,
    vad.activation_key,
   -- om.channel                                    AS shopped_channel,
    order_local_datetime                          AS order_datetime_added,
    oms.order_membership_classification_key,
    oms.membership_order_type_l3                  AS membership_order_type,
    SUM(IFNULL(product_subtotal_local_amount, 0)) AS subtotal,
    SUM(IFNULL(product_discount_local_amount, 0)) AS discount,
    SUM(IFNULL(shipping_revenue_local_amount, 0)) AS shipping,
    SUM(IFNULL(tariff_revenue_local_amount, 0))   AS tariff,
    SUM(IFNULL(item_quantity, 0))   AS units
FROM edw_prod.data_model_fl.fact_order_line o
JOIN edw_prod.data_model_fl.dim_store st ON st.store_id = o.store_id
    AND st.store_brand_abbr = 'FL'
    AND st.store_type = 'Retail'
JOIN edw_prod.data_model_fl.dim_order_status dos ON dos.order_status_key = o.order_status_key
    AND dos.order_status = 'Success'
JOIN edw_prod.data_model_fl.dim_order_membership_classification oms ON oms.order_membership_classification_key = o.order_membership_classification_key
JOIN edw_prod.data_model_fl.dim_order_status os ON os.order_status_key = o.order_status_key
JOIN edw_prod.data_model_fl.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
LEFT JOIN edw_prod.data_model_fl.fact_activation vad ON vad.customer_id = o.customer_id
    AND o.order_local_datetime BETWEEN vad.activation_local_datetime AND vad.next_activation_local_datetime
LEFT JOIN _omni om ON om.customer_id = o.customer_id
    AND TO_DATE(DATE_TRUNC('day', o.order_local_datetime)) >= om.order_local_datetime_start
    AND TO_DATE(DATE_TRUNC('day', o.order_local_datetime)) < om.order_local_datetime_end
WHERE o.order_local_datetime >= '2015-09-11' --retail inception
GROUP BY o.order_id,
    o.store_id,
    st.store_type,
    o.customer_id,
    o.administrator_id,
    vad.activation_key,
  --  om.channel,
    order_local_datetime,
    oms.order_membership_classification_key,
    oms.membership_order_type_l3
;

CREATE OR REPLACE TEMPORARY TABLE _retail_returns_setup AS
SELECT rr.store_id AS order_store_id,
    dc.customer_id,
    o.administrator_id,
    vad.activation_key,
  --  om.channel AS shopped_channel,
    r.datetime_added::DATE AS return_date,
    o.order_membership_classification_key,
    CASE WHEN ro.order_id IS NOT NULL THEN 'Retail Purchase Retail Return'
        ELSE 'Online Purchase Retail Return' END AS purchase_location, --considering retail ship only also as Retail as per requirement
    SUM(rl.quantity) AS quantity,
    SUM(o.product_subtotal_local_amount - o.product_discount_local_amount +o.shipping_revenue_local_amount) AS return_value
FROM lake_fl_view.ultra_merchant.retail_return_detail rrd --looking at all of the retail returns
JOIN lake_fl_view.ultra_merchant.retail_return rr ON rrd.retail_return_id = rr.retail_return_id
JOIN edw_prod.data_model_fl.dim_store st ON st.store_id = rr.store_id
    AND st.store_brand_abbr = 'FL'
JOIN lake_fl_view.ultra_merchant.return r ON r.return_id = rrd.return_id
JOIN lake_fl_view.ultra_merchant.return_line rl ON rl.return_id = r.return_id
JOIN edw_prod.data_model_fl.fact_order_line o ON o.order_line_id = rl.order_line_id
LEFT JOIN (select distinct order_id from _retail_orders) ro ON ro.order_id = o.order_id
JOIN edw_prod.data_model_fl.dim_order_membership_classification oms ON oms.order_membership_classification_key = o.order_membership_classification_key
JOIN edw_prod.data_model_fl.dim_order_sales_channel osc ON osc.order_sales_channel_key = o.order_sales_channel_key
JOIN edw_prod.data_model_fl.dim_order_status dos ON dos.order_status_key = o.order_status_key
    AND dos.order_status = 'Success'
JOIN edw_prod.data_model_fl.dim_customer dc ON dc.customer_id = o.customer_id
LEFT JOIN edw_prod.data_model_fl.fact_activation vad ON vad.customer_id = o.customer_id
    AND o.order_local_datetime BETWEEN vad.activation_local_datetime AND vad.next_activation_local_datetime
LEFT JOIN _omni om ON om.customer_id = o.customer_id
    AND TO_DATE(DATE_TRUNC('day', o.order_local_datetime)) >= om.order_local_datetime_start
    AND TO_DATE(DATE_TRUNC('day', o.order_local_datetime)) < om.order_local_datetime_end
GROUP BY rr.store_id,
    dc.customer_id,
    o.administrator_id,
    vad.activation_key,
    --  om.channel,
    r.datetime_added::DATE,
    o.order_membership_classification_key,
    CASE WHEN ro.order_id IS NOT NULL THEN 'Retail Purchase Retail Return' ELSE 'Online Purchase Retail Return' END

UNION

SELECT o.order_store_id,
    o.customer_id,
    o.administrator_id,
    vad.activation_key,
--    om.channel                                           AS shopped_channel,
    TO_DATE(r.datetime_added)                            AS return_date,
    o.order_membership_classification_key,
    'Retail Purchase Online Return'                      AS purchase_location, --considering retail ship only also as Retail as per requirement
    SUM(rl.quantity)                                     AS quantity,
    SUM(o.subtotal - o.discount + o.shipping + o.tariff) AS return_value
FROM _retail_orders o
JOIN lake_fl_view.ultra_merchant.return r ON r.order_id = o.order_id
JOIN lake_fl_view.ultra_merchant.return_line rl ON rl.return_id = r.return_id
LEFT JOIN lake_fl_view.ultra_merchant.retail_return_detail rrd ON rrd.return_id = r.return_id
LEFT JOIN edw_prod.data_model_fl.fact_activation vad ON vad.customer_id = o.customer_id
    AND o.order_datetime_added BETWEEN vad.activation_local_datetime AND vad.next_activation_local_datetime
LEFT JOIN _omni om ON om.customer_id = o.customer_id
    AND TO_DATE(DATE_TRUNC('day', o.order_datetime_added)) >= om.order_local_datetime_start
    AND TO_DATE(DATE_TRUNC('day', o.order_datetime_added)) < om.order_local_datetime_end
WHERE rrd.retail_return_detail_id IS NULL
GROUP BY o.order_store_id,
    o.customer_id,
    o.administrator_id,
    vad.activation_key,
--    om.channel,
    TO_DATE(r.datetime_added),
    o.order_membership_classification_key;


CREATE OR REPLACE TEMPORARY TABLE _retail_returns AS
SELECT order_store_id,
    customer_id,
    administrator_id,
    activation_key,
--   shopped_channel,
    return_date,
    order_membership_classification_key,
    SUM(CASE WHEN purchase_location = 'Online Purchase Retail Return' THEN IFNULL(quantity, 0) ELSE 0 END) AS financial_online_purchase_retail_returns,
    SUM(CASE WHEN purchase_location = 'Retail Purchase Retail Return' THEN IFNULL(quantity, 0) ELSE 0 END) AS financial_retail_purchase_retail_returns,
    SUM(CASE WHEN purchase_location = 'Retail Purchase Online Return' THEN IFNULL(quantity, 0) ELSE 0 END) AS financial_retail_purchase_online_returns,
    SUM(CASE WHEN purchase_location = 'Online Purchase Retail Return' THEN IFNULL(return_value, 0) ELSE 0 END) AS financial_online_purchase_retail_return_value,
    SUM(CASE WHEN purchase_location = 'Retail Purchase Retail Return' THEN IFNULL(return_value, 0) ELSE 0 END) AS financial_retail_purchase_retail_return_value,
    SUM(CASE WHEN purchase_location = 'Retail Purchase Online Return' THEN IFNULL(return_value, 0) ELSE 0 END) AS financial_retail_purchase_online_return_value
FROM _retail_returns_setup
GROUP BY order_store_id,
    customer_id,
    administrator_id,
    activation_key,
--    shopped_channel,
    return_date,
    order_membership_classification_key
;


create or replace temp table _retail_orders_agg as
select order_store_id,
    order_store_type,
    customer_id,
    administrator_id,
    activation_key,
--    shopped_channel,
    order_datetime_added::date as order_datetime_added,
    order_membership_classification_key,
    membership_order_type,
    sum(subtotal) as subtotal,
    sum(discount) as discount,
    sum(shipping) as shipping,
    sum(tariff) as tariff,
    sum(units) as units
from _retail_orders
GROUP BY order_store_id,
    order_store_type,
    customer_id,
    administrator_id,
    activation_key,
--    shopped_channel,
    order_datetime_added::date,
    order_membership_classification_key,
    membership_order_type;

CREATE OR REPLACE TEMPORARY TABLE _retail_exchanges AS
SELECT o.order_store_id,
       o.customer_id,
       o.administrator_id,
       TO_DATE(o.order_datetime_added)                                                        AS date,
       r.activation_key,
--       o.shopped_channel,
       o.order_membership_classification_key,
       -- SUM(units)                                                                               AS total_exchanges,
       SUM(CASE WHEN r.purchase_location = 'Retail Purchase Online Return' THEN units ELSE 0 END) AS exchanges_after_online_return,
       to_number(CASE WHEN  listagg(r.purchase_location,',') within group (order by purchase_location desc) ILIKE '%Retail Purchase Retail Return,Online Purchase Retail Return%'
       THEN SUM(units)/2
          WHEN listagg(r.purchase_location,',') = 'Retail Purchase Online Return'
          THEN 0
            ELSE SUM(units)
            END)                                                              AS exchanges_after_in_store_return,
       SUM(CASE WHEN r.purchase_location = 'Online Purchase Retail Return' THEN units ELSE 0 END) AS exchanges_after_in_store_return_from_online_purchase,
       -- SUM(o.subtotal - o.discount + o.shipping)                                   AS total_exchanges_value,
       SUM(CASE WHEN r.purchase_location = 'Retail Purchase Online Return'
           THEN o.subtotal - o.discount + o.shipping ELSE 0 END)                   AS exchanges_value_after_online_return,
           CASE WHEN  listagg(r.purchase_location,',') within group (order by purchase_location desc) ILIKE '%Retail Purchase Retail Return,Online Purchase Retail Return%'
       THEN SUM(o.subtotal - o.discount + o.shipping)/2
            WHEN listagg(r.purchase_location,',') = 'Retail Purchase Online Return'
          THEN 0
            ELSE SUM(o.subtotal - o.discount + o.shipping)
            END                                                              AS exchanges_value_after_in_store_return,
       SUM(CASE WHEN r.purchase_location = 'Online Purchase Retail Return'
           THEN o.subtotal - o.discount + o.shipping + o.tariff ELSE 0 END)                   AS exchanges_value_after_in_store_return_from_online_purchase
FROM _retail_returns_setup r
JOIN _retail_orders_agg o ON o.customer_id = r.customer_id
    AND DATEDIFF(DAY, r.return_date, o.order_datetime_added) = 0
GROUP BY o.order_store_id,
       o.customer_id,
       o.administrator_id,
       TO_DATE(o.order_datetime_added),
       r.activation_key,
--       o.shopped_channel,
       o.order_membership_classification_key
;

CREATE OR REPLACE TEMPORARY TABLE _msrp_price AS
SELECT fol.customer_id,
    fol.store_id AS order_store_id,
--    om.channel AS shopped_channel,
    vad.activation_key,
    fol.administrator_id,
    fol.order_membership_classification_key,
    fol.order_local_datetime::DATE AS order_date,
    SUM(ol.retail_unit_price) AS msrp_price
FROM edw_prod.data_model_fl.fact_order_line fol
JOIN edw_prod.data_model_fl.dim_store st ON st.store_id = fol.store_id
    AND st.store_brand_abbr = 'FL'
JOIN lake_fl_view.ultra_merchant.order_line ol ON fol.order_line_id = ol.order_line_id
JOIN edw_prod.data_model_fl.dim_order_status dos ON dos.order_status_key = fol.order_status_key
LEFT JOIN edw_prod.data_model_fl.fact_activation vad ON vad.customer_id = fol.customer_id
    AND fol.order_local_datetime BETWEEN vad.activation_local_datetime AND vad.next_activation_local_datetime
LEFT JOIN _omni om ON om.customer_id = fol.customer_id
    AND fol.order_local_datetime::DATE >= om.order_local_datetime_start::DATE
    AND fol.order_local_datetime::DATE < om.order_local_datetime_end::DATE
WHERE dos.order_status IN ('Success', 'Pending')
GROUP BY fol.customer_id,
    fol.store_id,
--    om.channel,
    vad.activation_key,
    fol.administrator_id,
    fol.order_membership_classification_key,
    fol.order_local_datetime::DATE
;

CREATE OR REPLACE TEMPORARY TABLE _gift_card_redemption_amount AS
SELECT fo.store_id AS order_store_id,
       fo.customer_id,
--       om.channel AS shopped_channel,
       fo.activation_key,
       fo.administrator_id,
       fo.order_membership_classification_key,
       fo.order_local_datetime::DATE AS order_date,
       SUM(ifnull(cash_giftcard_credit_local_amount, 0) * ifnull(order_date_usd_conversion_rate, 1)) AS gift_card_redemption_amount
FROM edw_prod.data_model_fl.fact_order fo
JOIN edw_prod.data_model_fl.dim_store ds ON ds.store_id = fo.store_id
    AND ds.store_brand_abbr = 'FL'
JOIN edw_prod.data_model_fl.dim_order_status dos ON dos.order_status_key = fo.order_status_key
    AND dos.order_status IN ('Success', 'Pending')
JOIN edw_prod.data_model_fl.dim_order_sales_channel dosc ON dosc.order_sales_channel_key = fo.order_sales_channel_key
    AND dosc.order_classification_l2 = 'Product Order'
LEFT JOIN _omni om ON om.customer_id = fo.customer_id
    AND fo.order_local_datetime::DATE >= om.order_local_datetime_start::DATE
    AND fo.order_local_datetime::DATE < om.order_local_datetime_end::DATE
GROUP BY fo.store_id,
       fo.customer_id,
--       om.channel,
       fo.activation_key,
       fo.administrator_id,
       fo.order_membership_classification_key,
       fo.order_local_datetime::DATE
;

CREATE OR REPLACE TEMPORARY TABLE _gift_card_orders AS
SELECT fol.store_id AS order_store_id,
    fol.customer_id,
--    om.channel AS shopped_channel,
    vad.activation_key,
    fol.administrator_id,
    fol.order_membership_classification_key,
    fol.order_local_datetime::DATE AS order_date,
    COUNT(DISTINCT fol.order_id) AS gift_card_orders
FROM edw_prod.data_model_fl.fact_order_line fol
JOIN edw_prod.data_model_fl.dim_store st ON st.store_id = fol.store_id
    AND st.store_brand_abbr = 'FL'
JOIN edw_prod.data_model_fl.dim_order_status dos ON dos.order_status_key = fol.order_status_key
    AND dos.order_status IN ('Success', 'Pending')
JOIN edw_prod.data_model_fl.dim_product_type pt ON pt.product_type_key = fol.product_type_key
LEFT JOIN edw_prod.data_model_fl.fact_activation vad ON vad.customer_id = fol.customer_id
    AND fol.order_local_datetime BETWEEN vad.activation_local_datetime AND vad.next_activation_local_datetime
LEFT JOIN _omni om ON om.customer_id = fol.customer_id
    AND fol.order_local_datetime::DATE >= om.order_local_datetime_start::DATE
    AND fol.order_local_datetime::DATE < om.order_local_datetime_end::DATE
WHERE product_type_name IN ('Gift Certificate','Gift Card')
GROUP BY fol.store_id,
    fol.customer_id,
--    om.channel,
    vad.activation_key,
    fol.administrator_id,
    fol.order_membership_classification_key,
    fol.order_local_datetime::DATE
;


CREATE OR REPLACE TEMPORARY TABLE _store_dimensions AS
SELECT store_number,
       store_name,
       store_id,
       MAX(store_open_date) AS store_open_date
FROM (
         SELECT DISTINCT TRIM(st.store_retail_location_code)                   AS store_number,
                         TRIM(rl.label)                                  AS store_name,
                         TRIM(rl.store_id)                               AS store_id,
                         TRIM(iff(rl.open_date >= COALESCE(
                             IFF(try_to_date(rlc_open_date.configuration_value) IS NOT NULL,rlc_open_date.configuration_value,
                                 IFF(try_to_date(rlc_open_date.configuration_value, 'mm-dd-yyyy') is null,
                                     to_date(rlc_open_date.configuration_value, 'yyyy/mm/dd'),
                                     to_date(rlc_open_date.configuration_value, 'mm-dd-yyyy'))), rl.open_date),
                                  rl.open_date, COALESCE(IFF(try_to_date(rlc_open_date.configuration_value) IS NOT NULL,rlc_open_date.configuration_value,
                                                IFF(try_to_date(rlc_open_date.configuration_value, 'mm-dd-yyyy') is null,
                                                    to_date(rlc_open_date.configuration_value, 'yyyy/mm/dd'),
                                                    to_date(rlc_open_date.configuration_value, 'mm-dd-yyyy'))), rl.open_date))) AS store_open_date
         FROM lake_view.ultra_warehouse.retail_location rl
                  LEFT JOIN EDW_PROD.data_model_fl.dim_store st on st.store_id = rl.store_id
                  LEFT JOIN lake_view.ultra_warehouse.retail_location_configuration rlc_region
                            ON rlc_region.retail_location_id = rl.retail_location_id AND
                               rlc_region.retail_configuration_id = 35
                  LEFT JOIN lake_view.ultra_warehouse.retail_configuration rlc_region_default
                            ON rlc_region_default.retail_configuration_id = 35
                  LEFT JOIN lake_view.ultra_warehouse.retail_location_configuration rlc_district
                            ON rlc_district.retail_location_id = rl.retail_location_id AND
                               rlc_district.retail_configuration_id = 18
                  LEFT JOIN lake_view.ultra_warehouse.retail_configuration rlc_district_default
                            ON rlc_district_default.retail_configuration_id = 18
                  LEFT JOIN lake_view.ultra_warehouse.retail_location_configuration rlc_open_date
                            ON rlc_open_date.retail_location_id = rl.retail_location_id AND
                               rlc_open_date.retail_configuration_id = 27)
GROUP BY store_number,
         store_name,
         store_id;

CREATE OR REPLACE TEMPORARY TABLE _finance_sales_ops AS
SELECT date,
        fso.store_id,
        edw_prod.stg.udf_unconcat_brand(customer_id) as customer_id,
        activation_key,
        date_object,
        currency_object,
        currency_type,
        order_membership_classification_key,
        associate_id,
        SUM(IFNULL(product_order_count, 0))                                                                   AS product_order_count,
        SUM(IFNULL(product_order_unit_count, 0))                                                              AS product_order_unit_count,
        SUM(IFNULL(product_order_zero_revenue_unit_count, 0))                                                 AS product_order_zero_revenue_unit_count,
        SUM(IFNULL(product_gross_revenue, 0))                                                                 AS product_gross_revenue,
        SUM(IFNULL(product_net_revenue, 0))                                                                   AS product_net_revenue,
        SUM(IFNULL(product_gross_profit, 0))                                                                  AS product_gross_profit,
        SUM(IFNULL(cash_gross_revenue, 0))                                                                    AS cash_gross_revenue,
        SUM(IFNULL(cash_net_revenue, 0))                                                                      AS cash_net_revenue,
        SUM(IFNULL(cash_gross_profit, 0))                                                                     AS cash_gross_profit,
        SUM(IFNULL(product_order_product_subtotal_amount, 0))                                                 AS product_order_product_subtotal_amount,
        SUM(IFNULL(product_order_shipping_revenue_before_discount_amount, 0))                                 AS product_order_shipping_revenue_before_discount_amount,
        SUM(IFNULL(product_order_shipping_cost_amount, 0))                                                    AS product_order_shipping_cost_amount,
        SUM(IFNULL(product_order_shipping_supplies_cost_amount, 0))                                           AS product_order_shipping_supplies_cost_amount,
        SUM(IFNULL(product_order_product_discount_amount, 0))                                                 AS product_order_product_discount_amount,
        SUM(IFNULL(product_order_misc_cogs_amount, 0))                                                        AS product_order_misc_cogs_amount,
        SUM(IFNULL(billed_cash_credit_redeemed_amount, 0))                                                    AS billed_cash_credit_redeemed_amount,
        SUM(IFNULL(billed_cash_credit_redeemed_equivalent_count, 0))                                          AS billed_cash_credit_redeemed_equivalent_count,
        SUM(IFNULL(product_order_cash_credit_redeemed_amount, 0))                                             AS product_order_cash_credit_redeemed_amount,
        SUM(IFNULL(product_order_noncash_credit_redeemed_amount, 0))                                          AS product_order_noncash_credit_redeemed_amount,
        SUM(IFNULL(gift_card_transaction_amount, 0))                                                          AS gift_card_transaction_amount,
        SUM(IFNULL(product_order_landed_product_cost_amount, 0))                                              AS product_order_landed_product_cost_amount,
        SUM(IFNULL(retail_ship_only_product_order_count, 0))                                                  AS retail_ship_only_product_order_count,
        SUM(IFNULL(retail_ship_only_product_order_unit_count, 0))                                             AS retail_ship_only_product_order_unit_count,
        SUM(IFNULL(retail_ship_only_product_gross_revenue, 0))                                                AS retail_ship_only_product_gross_revenue,
        SUM(IFNULL(retail_ship_only_product_net_revenue, 0))                                                  AS retail_ship_only_product_net_revenue,
        SUM(IFNULL(retail_ship_only_product_gross_profit, 0))                                                 AS retail_ship_only_product_gross_profit,
        SUM(IFNULL(retail_ship_only_product_order_cash_gross_revenue, 0))                                     AS retail_ship_only_product_order_cash_gross_revenue,
        SUM(IFNULL(retail_ship_only_product_order_cash_net_revenue, 0))                                       AS retail_ship_only_product_order_cash_net_revenue,
        SUM(IFNULL(retail_ship_only_product_order_cash_gross_profit, 0))                                      AS retail_ship_only_product_order_cash_gross_profit,
        SUM(IFNULL(product_order_cash_credit_refund_amount, 0))                                               AS product_order_cash_credit_refund_amount,
        SUM(IFNULL(product_order_cash_refund_amount, 0))                                                      AS product_order_cash_refund_amount,
        SUM(IFNULL(product_order_cash_chargeback_amount, 0))                                                  AS product_order_cash_chargeback_amount,
        SUM(IFNULL(product_order_cost_product_returned_resaleable_amount, 0))                                 AS product_order_cost_product_returned_resaleable_amount,
        SUM(IFNULL(product_order_cost_product_returned_damaged_amount, 0))                                    AS product_order_cost_product_returned_damaged_amount
FROM edw_prod.analytics_base.finance_sales_ops fso
JOIN edw_prod.data_model_fl.dim_store ds ON ds.store_id = fso.store_id
    AND ds.store_brand_abbr = 'FL'
WHERE fso.date_object = 'placed'
  AND currency_object = 'usd'
GROUP BY date,
       fso.store_id,
       customer_id,
       activation_key,
       date_object,
       currency_object,
       currency_type,
       order_membership_classification_key,
       associate_id;


create or replace temp table _online_orders as
select fso.store_id,
    store_type,
    fso.customer_id,
    associate_id,
    fso.activation_key,
    date as order_datetime_added,
    order_membership_classification_key,
--    om.channel  AS shopped_channel,
    SUM(IFNULL(product_gross_revenue, 0))  as exchange_value,
    SUM(product_order_unit_count) as units
FROM _finance_sales_ops fso
JOIN edw_prod.data_model_fl.dim_store ds on ds.store_id = fso.store_id
LEFT JOIN _omni om ON om.customer_id = fso.customer_id
    AND TO_DATE(DATE_TRUNC('day', fso.date)) >= om.order_local_datetime_start::date
    AND TO_DATE(DATE_TRUNC('day', fso.date)) < om.order_local_datetime_end::date
WHERE store_type <> 'Retail'
GROUP BY fso.store_id,
    store_type,
    fso.customer_id,
    associate_id,
    fso.activation_key,
    date,
    order_membership_classification_key;
--    om.channel;


CREATE OR REPLACE TEMPORARY TABLE _online_exchanges AS
 SELECT o.store_id,
       o.customer_id,
       o.associate_id,
       TO_DATE(o.order_datetime_added)                                                        AS date,
       r.activation_key,
--       o.shopped_channel,
       o.order_membership_classification_key,
       -- SUM(units)                                                                               AS total_exchanges,
       SUM(CASE WHEN r.purchase_location = 'Retail Purchase Online Return' THEN units ELSE 0 END) AS online_exchanges_after_online_return,
       to_number(CASE WHEN  listagg(r.purchase_location,',') within group (order by purchase_location desc) ILIKE '%Retail Purchase Retail Return,Online Purchase Retail Return%'
       THEN SUM(units)/2
            WHEN listagg(r.purchase_location,',') = 'Retail Purchase Online Return'
          THEN 0
            ELSE SUM(units)
            END)                                                              AS online_exchanges_after_in_store_return,
       SUM(CASE WHEN r.purchase_location = 'Online Purchase Retail Return' THEN units ELSE 0 END) AS online_exchanges_after_in_store_return_from_online_purchase,
       -- SUM(o.subtotal - o.discount + o.shipping)                                   AS total_exchanges_value,
       SUM(CASE WHEN r.purchase_location = 'Retail Purchase Online Return'
           THEN o.exchange_value ELSE 0 END)                   AS online_exchanges_value_after_online_return,
       CASE WHEN  listagg(r.purchase_location,',') within group (order by purchase_location desc) ILIKE '%Retail Purchase Retail Return,Online Purchase Retail Return%'
       THEN SUM(o.exchange_value)/2
            WHEN listagg(r.purchase_location,',') = 'Retail Purchase Online Return'
          THEN 0
            ELSE SUM(o.exchange_value)
            END                                                              AS online_exchanges_value_after_in_store_return,
       SUM(CASE WHEN r.purchase_location = 'Online Purchase Retail Return'
           THEN o.exchange_value ELSE 0 END)                   AS online_exchanges_value_after_in_store_return_from_online_purchase
FROM _retail_returns_setup r
JOIN _online_orders o ON o.customer_id = r.customer_id
    AND DATEDIFF(DAY, r.return_date, o.order_datetime_added) = 0
GROUP BY o.store_id,
       o.customer_id,
       o.associate_id,
       TO_DATE(o.order_datetime_added),
       r.activation_key,
--       o.shopped_channel,
       o.order_membership_classification_key
;

CREATE OR REPLACE TEMPORARY TABLE _orders1 AS
SELECT TO_DATE(COALESCE(fso.date, rr.return_date, re.date,oe.date, mp.order_date,gco.order_date,gcra.order_date))                                   AS order_date,
    CASE WHEN c.gender ilike 'M' AND c.registration_local_datetime < '2020-01-01' THEN 'F'
        WHEN c.gender ilike 'M' THEN 'M' ELSE 'F' END as customer_gender,
    --        c.gender                                                                                              AS customer_gender,
    ast.store_type                                                                                        AS customer_activation_channel,
    ast.store_sub_type                                                                                    AS retail_activation_store_type,
    ast.store_id                                                                                          AS activation_store_id,
    ast.store_region                                                                                      AS activation_region,
    ast.store_full_name                                                                                   AS activation_store_name,
    ast.store_retail_state                                                                                AS activation_store_state,
    ast.store_retail_city                                                                                 AS activation_store_city,
    ast.store_retail_zip_code                                                                             AS activation_store_zip_code,
    ast.store_retail_location_code                                                                        AS activation_store_number,
    azcs.latitude                                                                                         AS activation_store_latitude,
    azcs.longitude                                                                                        AS activation_store_longitude,
    arsd.region                                                                                           AS activation_store_region,
    arsd.district                                                                                         AS activation_store_district,
    case when activation_store_id = 102 AND current_date() < '2024-01-01' then '2015-10-26' else asd.store_open_date end   AS activation_store_open_date,
    st.store_full_name                                                                                    AS order_store_name,
    COALESCE(fso.store_id, rr.order_store_id, re.order_store_id,oe.store_id, mp.order_store_id,gco.order_store_id,gcra.order_store_id)                       AS order_store_id,
    st.store_region                                                                                       AS order_region,
    st.store_type                                                                                         AS order_store_type,
    CASE WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type ELSE 'Online' END                       AS retail_order_store_type,
    st.store_retail_zip_code                                                                              AS order_store_zip,
    st.store_retail_city                                                                                  AS order_store_city,
    st.store_retail_state                                                                                 AS order_store_state,
    st.store_retail_location_code                                                                         AS order_store_number,
    zcs.longitude                                                                                         AS order_longitude,
    zcs.latitude                                                                                          AS order_latitude,
    orsd.region                                                                                           AS order_store_region,
    orsd.district                                                                                         AS order_store_district,
    case when COALESCE(fso.store_id, rr.order_store_id, re.order_store_id,oe.store_id, mp.order_store_id,gco.order_store_id,gcra.order_store_id) = 102
              AND current_date() < '2024-01-01' then '2015-10-26' else osd.store_open_date end                                                                                  AS order_store_open_date,
    COALESCE(fso.associate_id, rr.administrator_id, re.administrator_id,oe.associate_id, mp.administrator_id,gco.administrator_id,gcra.administrator_id)             AS administrator_id,
    ad.firstname,
    ad.lastname,
    oms.membership_order_type_l3                                                                          AS membership_order_type,
    om.channel                                                                                            AS shopped_channel,
    SUM(IFNULL(product_order_count, 0))                                                                   AS orders,
    SUM(CASE WHEN oms.membership_order_type_l2 <> 'Repeat VIP' THEN IFNULL(product_order_count, 0) ELSE 0 END)  AS conversion_eligible_orders,
    SUM(IFNULL(product_order_unit_count, 0))                                                              AS units,
    SUM(IFNULL(product_order_zero_revenue_unit_count, 0))                                                 AS zero_revenue_units,
    SUM(IFNULL(product_gross_revenue, 0))                                                                 AS product_gross_revenue,
    SUM(IFNULL(product_net_revenue, 0))                                                                   AS product_net_revenue,
    SUM(IFNULL(product_gross_profit, 0))                                                                  AS product_gross_margin,
    SUM(IFNULL(cash_gross_revenue, 0))                                                                    AS cash_gross_revenue,
    SUM(IFNULL(cash_net_revenue, 0))                                                                      AS cash_net_revenue,
    SUM(IFNULL(cash_gross_profit, 0))                                                                     AS cash_gross_margin,
    SUM(IFNULL(product_order_product_subtotal_amount, 0))                                                 AS subtotal,
    SUM(IFNULL(product_order_shipping_revenue_before_discount_amount, 0))                                 AS shipping_revenue,
    SUM(IFNULL(product_order_shipping_cost_amount, 0))                                                    AS shipping_cost,
    SUM(IFNULL(product_order_shipping_supplies_cost_amount, 0))                                           AS shipping_supplies_cost,
    SUM(IFNULL(product_order_product_discount_amount, 0))                                                 AS discount,
    SUM(IFNULL(product_order_misc_cogs_amount, 0))                                                        AS misc_cogs,
    SUM(IFNULL(billed_cash_credit_redeemed_amount, 0))                                                    AS credit_redemption_amount,
    SUM(IFNULL(billed_cash_credit_redeemed_equivalent_count, 0))                                          AS credit_redemption_count,
    SUM(IFNULL(product_order_cash_credit_redeemed_amount, 0)
        + IFNULL(product_order_noncash_credit_redeemed_amount, 0))                                        AS total_credits_redeemed,
    SUM(IFNULL(gift_card_transaction_amount, 0))                                                          AS gift_card_issuance_amount,
    SUM(IFNULL(product_order_landed_product_cost_amount, 0))                                              AS product_cost,
    SUM(IFNULL(retail_ship_only_product_order_count, 0))                                                  AS ship_only_orders,
    SUM(IFNULL(retail_ship_only_product_order_unit_count, 0))                                             AS ship_only_units,
    SUM(IFNULL(retail_ship_only_product_gross_revenue, 0))                                                AS ship_only_product_gross_revenue,
    SUM(IFNULL(retail_ship_only_product_net_revenue, 0))                                                  AS ship_only_product_net_revenue,
    SUM(IFNULL(retail_ship_only_product_gross_profit, 0))                                                 AS ship_only_product_gross_margin,
    SUM(IFNULL(retail_ship_only_product_order_cash_gross_revenue, 0))                                     AS ship_only_cash_gross_revenue,
    SUM(IFNULL(retail_ship_only_product_order_cash_net_revenue, 0))                                       AS ship_only_cash_net_revenue,
    SUM(IFNULL(retail_ship_only_product_order_cash_gross_profit, 0))                                      AS ship_only_cash_gross_profit,
    SUM(IFF(oms.membership_order_type_l3 = 'First Guest', IFNULL(product_order_count, 0), 0))             AS payg_activations,
    SUM(rr.financial_online_purchase_retail_returns)                                                         AS financial_online_purchase_retail_returns,
    SUM(rr.financial_retail_purchase_retail_returns)                                                         AS financial_retail_purchase_retail_returns,
    SUM(rr.financial_retail_purchase_online_returns)                                                         AS financial_retail_purchase_online_returns,
    SUM(rr.financial_online_purchase_retail_return_value)                                                    AS financial_online_purchase_retail_return_value,
    SUM(rr.financial_retail_purchase_retail_return_value)                                                    AS financial_retail_purchase_retail_return_value,
    SUM(rr.financial_retail_purchase_online_return_value)                                                    AS financial_retail_purchase_online_return_value,
    SUM(IFNULL(re.exchanges_after_online_return + re.exchanges_after_in_store_return, 0))                 AS financial_total_exchanges,
    SUM(IFNULL(re.exchanges_after_online_return, 0))                                                      AS financial_exchanges_after_online_return,
    SUM(IFNULL(re.exchanges_after_in_store_return, 0))                                                    AS financial_exchanges_after_in_store_return,
    SUM(IFNULL(re.exchanges_after_in_store_return_from_online_purchase, 0))                               AS financial_exchanges_after_in_store_return_from_online_purchase,
    SUM(IFNULL(re.exchanges_value_after_online_return + exchanges_value_after_in_store_return, 0))        AS financial_total_exchanges_value,
    SUM(IFNULL(re.exchanges_value_after_online_return, 0))                                                AS financial_exchanges_value_after_online_return,
    SUM(IFNULL(re.exchanges_value_after_in_store_return, 0))                                              AS financial_exchanges_value_after_in_store_return,
    SUM(IFNULL(re.exchanges_value_after_in_store_return_from_online_purchase, 0))                         AS financial_exchanges_value_after_in_store_return_from_online_purchase,
    SUM(IFNULL(oe.online_exchanges_after_online_return + oe.online_exchanges_after_in_store_return, 0))   AS financial_online_total_exchanges,
    SUM(IFNULL(oe.online_exchanges_after_online_return, 0))                                               AS financial_online_exchanges_after_online_return,
    SUM(IFNULL(oe.online_exchanges_after_in_store_return, 0))                                             AS financial_online_exchanges_after_in_store_return,
    SUM(IFNULL(oe.online_exchanges_after_in_store_return_from_online_purchase, 0))                        AS financial_online_exchanges_after_in_store_return_from_online_purchase,
    SUM(IFNULL(oe.online_exchanges_value_after_online_return + oe.online_exchanges_value_after_in_store_return, 0)) AS financial_online_total_exchanges_value,
    SUM(IFNULL(oe.online_exchanges_value_after_online_return, 0))                                         AS financial_online_exchanges_value_after_online_return,
    SUM(IFNULL(oe.online_exchanges_value_after_in_store_return, 0))                                       AS financial_online_exchanges_value_after_in_store_return,
    SUM(IFNULL(oe.online_exchanges_value_after_in_store_return_from_online_purchase, 0))                  AS financial_online_exchanges_value_after_in_store_return_from_online_purchase,
    SUM(CASE
        WHEN ast.store_type != 'Retail' AND st.store_type = 'Retail' THEN IFNULL(product_order_count, 0)
        ELSE 0 END)                                                                                       AS omni_orders,
    SUM(IFNULL(product_order_cash_credit_refund_amount, 0))                                               AS product_order_cash_credit_refund_amount,
    SUM(IFNULL(product_order_cash_refund_amount, 0))                                                      AS product_order_cash_refund_amount,
    SUM(IFNULL(product_order_cash_credit_refund_amount, 0) + IFNULL(product_order_cash_refund_amount, 0)) AS product_order_refunds,
    SUM(IFNULL(product_order_cash_chargeback_amount, 0))                                                  AS product_order_cash_chargeback_amount,
    SUM(IFNULL(product_order_cost_product_returned_resaleable_amount, 0) +
        IFNULL(product_order_cost_product_returned_damaged_amount, 0))                                    AS return_product_cost_amount,
    SUM(IFNULL(mp.msrp_price, 0))                                                                         AS msrp_price,
    SUM(IFNULL(gco.gift_card_orders, 0))                                                                  AS gift_card_orders,
    SUM(IFNULL(gcra.gift_card_redemption_amount, 0))                                                      AS gift_card_redemption_amount
FROM _finance_sales_ops fso
FULL JOIN _retail_returns rr ON rr.customer_id = fso.customer_id
    AND rr.activation_key = fso.activation_key
    AND rr.order_store_id = fso.store_id
    AND rr.order_membership_classification_key = fso.order_membership_classification_key
    AND TO_DATE(rr.return_date) = fso.date
    AND rr.administrator_id = fso.associate_id
    -- AND COALESCE(rr.shopped_channel, 'null') = COALESCE(om.channel, 'null')
FULL JOIN _retail_exchanges re ON re.customer_id = coalesce(fso.customer_id,rr.customer_id)
    AND re.activation_key = coalesce(fso.activation_key,rr.activation_key)
    AND re.order_store_id = coalesce(fso.store_id,rr.order_store_id)
    AND re.order_membership_classification_key = coalesce(fso.order_membership_classification_key,rr.order_membership_classification_key)
    AND TO_DATE(re.date) = coalesce(fso.date,to_date(rr.return_date))
    AND re.administrator_id = coalesce(fso.associate_id,rr.administrator_id)
    -- AND COALESCE(re.shopped_channel, 'null') = COALESCE(om.channel,rr.shopped_channel, 'null')
FULL JOIN _online_exchanges oe ON oe.customer_id = coalesce(fso.customer_id,rr.customer_id,re.customer_id)
    AND oe.activation_key = coalesce(fso.activation_key,rr.activation_key,re.activation_key)
    AND oe.store_id = coalesce(fso.store_id,rr.order_store_id,re.order_store_id)
    AND oe.order_membership_classification_key = coalesce(fso.order_membership_classification_key,rr.order_membership_classification_key,re.order_membership_classification_key)
    AND TO_DATE(oe.date) = coalesce(fso.date,rr.return_date::date,re.date::date)
    AND oe.associate_id = coalesce(fso.associate_id,rr.administrator_id,re.administrator_id)
    -- AND COALESCE(oe.shopped_channel, 'null') = COALESCE(om.channel,rr.shopped_channel,re.shopped_channel, 'null')
FULL JOIN _msrp_price mp ON mp.customer_id = coalesce(fso.customer_id,rr.customer_id,re.customer_id,oe.customer_id)
    AND mp.activation_key = coalesce(fso.activation_key,rr.activation_key,re.activation_key,oe.activation_key)
    AND mp.order_store_id = coalesce(fso.store_id,rr.order_store_id,re.order_store_id,oe.store_id)
    AND mp.order_membership_classification_key = coalesce(fso.order_membership_classification_key,rr.order_membership_classification_key,re.order_membership_classification_key,oe.order_membership_classification_key)
    AND mp.order_date::date = coalesce(fso.date,rr.return_date::date,re.date::date,oe.date::date)
    AND mp.administrator_id = coalesce(fso.associate_id,rr.administrator_id,re.administrator_id,oe.associate_id)
    -- AND COALESCE(mp.shopped_channel, 'null') =  COALESCE(om.channel,rr.shopped_channel,re.shopped_channel,oe.shopped_channel, 'null')
FULL JOIN _gift_card_orders gco ON gco.customer_id = coalesce(fso.customer_id,rr.customer_id,re.customer_id,oe.customer_id,mp.customer_id)
    AND gco.activation_key = coalesce(fso.activation_key,rr.activation_key,re.activation_key,oe.activation_key,mp.activation_key)
    AND gco.order_store_id = coalesce(fso.store_id,rr.order_store_id,re.order_store_id,oe.store_id,mp.order_store_id)
    AND gco.order_membership_classification_key = coalesce(fso.order_membership_classification_key,rr.order_membership_classification_key,re.order_membership_classification_key,oe.order_membership_classification_key,mp.order_membership_classification_key)
    AND gco.order_date::date = coalesce(fso.date,rr.return_date::date,re.date::date,oe.date::date,mp.order_date::date)
    AND gco.administrator_id = coalesce(fso.associate_id,rr.administrator_id,re.administrator_id,oe.associate_id,mp.administrator_id)
    -- AND COALESCE(gco.shopped_channel, 'null') = COALESCE(om.channel,rr.shopped_channel,re.shopped_channel,oe.shopped_channel,mp.shopped_channel, 'null')
FULL JOIN _gift_card_redemption_amount gcra ON gcra.customer_id = coalesce(fso.customer_id,rr.customer_id,re.customer_id,oe.customer_id,mp.customer_id,gco.customer_id)
    AND gcra.activation_key = coalesce(fso.activation_key,rr.activation_key,re.activation_key,oe.activation_key,mp.activation_key,gco.activation_key)
    AND gcra.order_store_id = coalesce(fso.store_id,rr.order_store_id,re.order_store_id,oe.store_id,mp.order_store_id,gco.order_store_id)
    AND gcra.order_membership_classification_key = coalesce(fso.order_membership_classification_key,rr.order_membership_classification_key,re.order_membership_classification_key,oe.order_membership_classification_key,mp.order_membership_classification_key,gco.order_membership_classification_key)
    AND gcra.order_date::date = coalesce(fso.date,rr.return_date::date,re.date::date,oe.date::date,mp.order_date::date,gco.order_date::date)
    AND gcra.administrator_id = coalesce(fso.associate_id,rr.administrator_id,re.administrator_id,oe.associate_id,mp.administrator_id,gco.administrator_id)
    -- AND COALESCE(gcra.shopped_channel, 'null') = COALESCE(om.channel,rr.shopped_channel,re.shopped_channel,oe.shopped_channel,mp.shopped_channel,gco.shopped_channel, 'null')
JOIN edw_prod.data_model_fl.dim_store st ON
    st.store_id = COALESCE(fso.store_id, rr.order_store_id, re.order_store_id,oe.store_id, mp.order_store_id, gco.order_store_id, gcra.order_store_id)
LEFT JOIN edw_prod.data_model_fl.fact_activation a ON
    a.activation_key = COALESCE(fso.activation_key, rr.activation_key, re.activation_key,oe.activation_key, mp.activation_key, gco.activation_key, gcra.activation_key)
LEFT JOIN edw_prod.data_model_fl.dim_customer c ON
    c.customer_id = COALESCE(fso.customer_id, rr.customer_id, re.customer_id,oe.customer_id, mp.customer_id, gco.customer_id, gcra.customer_id)
LEFT JOIN _omni om ON
    om.customer_id = COALESCE(fso.customer_id, rr.customer_id, re.customer_id,oe.customer_id, mp.customer_id, gco.customer_id, gcra.customer_id)
    AND om.activation_key = COALESCE(fso.activation_key, rr.activation_key, re.activation_key,oe.activation_key, mp.activation_key, gco.activation_key, gcra.activation_key)
    AND coalesce(fso.date,rr.return_date::date,re.date::date,oe.date::date,mp.order_date,gco.order_date,gcra.order_date::date) >= om.order_local_datetime_start::DATE
    AND coalesce(fso.date,rr.return_date::date,re.date::date,oe.date::date,mp.order_date,gco.order_date,gcra.order_date::date) < om.order_local_datetime_end::DATE
LEFT JOIN edw_prod.data_model_fl.dim_store ast ON ast.store_id = a.sub_store_id
LEFT JOIN lake_fl_view.ultra_merchant.zip_city_state azcs ON azcs.zip = ast.store_retail_zip_code
LEFT JOIN lake_fl_view.ultra_merchant.zip_city_state zcs ON zcs.zip = st.store_retail_zip_code
JOIN edw_prod.data_model_fl.dim_order_membership_classification oms
    ON oms.order_membership_classification_key = COALESCE(fso.order_membership_classification_key, rr.order_membership_classification_key,
        re.order_membership_classification_key, oe.order_membership_classification_key, mp.order_membership_classification_key,
        gco.order_membership_classification_key, gcra.order_membership_classification_key)
LEFT JOIN lake_view.sharepoint.fl_retail_store_detail orsd ON orsd.store_id = st.store_id
LEFT JOIN lake_view.sharepoint.fl_retail_store_detail arsd ON arsd.store_id = ast.store_id
LEFT JOIN _store_dimensions osd ON osd.store_id = st.store_id
LEFT JOIN _store_dimensions asd ON asd.store_id = ast.store_id
LEFT JOIN lake_fl_view.ultra_merchant.administrator ad ON ad.administrator_id = COALESCE(fso.associate_id, rr.administrator_id, re.administrator_id, oe.associate_id,mp.administrator_id, gco.administrator_id, gcra.administrator_id)
WHERE st.store_brand_abbr = 'FL'
-- and st.store_Type = 'Retail'
GROUP BY TO_DATE(COALESCE(fso.date, rr.return_date, re.date,oe.date, mp.order_date,gco.order_date,gcra.order_date)),
    fso.customer_id,
    CASE WHEN c.gender ilike 'M' AND c.registration_local_datetime < '2020-01-01' THEN 'F' WHEN c.gender ilike 'M' THEN 'M' ELSE 'F' END,
    customer_activation_channel,
    retail_activation_store_type,
    activation_store_id,
    activation_region,
    activation_store_name,
    activation_store_state,
    activation_store_city,
    activation_store_zip_code,
    activation_store_number,
    activation_store_latitude,
    activation_store_longitude,
    activation_store_region,
    activation_store_district,
    activation_store_open_date,
    order_store_name,
    COALESCE(fso.store_id, rr.order_store_id, re.order_store_id,oe.store_id, mp.order_store_id,gco.order_store_id,gcra.order_store_id),
    order_region,
    order_store_type,
    retail_order_store_type,
    order_store_zip,
    order_store_city,
    order_store_state,
    order_store_number,
    order_longitude,
    order_latitude,
    order_store_region,
    order_store_district,
    order_store_open_date,
    COALESCE(fso.associate_id, rr.administrator_id, re.administrator_id,oe.associate_id, mp.administrator_id,gco.administrator_id,gcra.administrator_id),
    ad.firstname,
    ad.lastname,
    membership_order_type,
    om.channel
;



CREATE OR REPLACE TEMPORARY TABLE _final_temp1 AS
SELECT '4-Wall'                                                                                                 AS metric_type,
       'Financial'                                                                                              AS metric_method,
       customer_gender,
       shopped_channel,
       order_date                                                                                               AS date,
       administrator_id,
       firstname,
       lastname,
       order_store_id                                                                                           AS store_id,
       order_region                                                                                             AS region,
       order_store_name                                                                                         AS store_name,
       order_longitude                                                                                          AS longitude,
       order_latitude                                                                                           AS latitude,
       order_store_zip                                                                                          AS store_zip,
       order_store_city                                                                                         AS store_city,
       order_store_state                                                                                        AS store_state,
       order_store_region                                                                                       AS store_region,
       order_store_district                                                                                     AS store_district,
       order_store_open_date                                                                                    AS store_opening_date,
       order_store_number                                                                                       AS store_number,
       retail_order_store_type                                                                                  AS store_retail_type,
       order_store_type                                                                                         AS store_type,
       CASE
           WHEN DATEDIFF(MONTH, IFNULL(order_store_open_date, '2079-01-01'), order_date) >= 13 THEN 'Comp'
           ELSE 'New' END                                                                                       AS comp_new,
       membership_order_type,
       customer_activation_channel,
       SUM(CASE WHEN order_store_type = 'Retail' THEN orders ELSE 0 END)                                        AS orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN units ELSE 0 END)                                         AS units,
       SUM(CASE WHEN order_store_type = 'Retail' THEN ship_only_orders ELSE 0 END)                              AS ship_only_orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN ship_only_units ELSE 0 END)                               AS ship_only_units,
       SUM(CASE WHEN order_store_type = 'Retail' THEN zero_revenue_units ELSE 0 END)                            AS zero_revenue_units,
       SUM(CASE WHEN order_store_type = 'Retail' THEN cash_gross_revenue ELSE 0 END)                            AS cash_gross_revenue,
       SUM(CASE WHEN order_store_type = 'Retail' THEN product_gross_revenue ELSE 0 END)                         AS product_gross_revenue,
       SUM(CASE WHEN order_store_type = 'Retail' THEN cash_net_revenue ELSE 0 END)                              AS cash_net_revenue,
       SUM(CASE WHEN order_store_type = 'Retail' THEN product_net_revenue ELSE 0 END)                           AS product_net_revenue,
       SUM(CASE WHEN order_store_type = 'Retail' THEN cash_gross_margin ELSE 0 END)                             AS cash_gross_margin,
       SUM(CASE WHEN order_store_type = 'Retail' THEN product_gross_margin ELSE 0 END)                          AS product_gross_margin,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN ship_only_cash_gross_revenue
               ELSE 0 END)                                                                                      AS ship_only_cash_gross_revenue,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN ship_only_product_gross_revenue
               ELSE 0 END)                                                                                      AS ship_only_product_gross_revenue,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN ship_only_cash_net_revenue
               ELSE 0 END)                                                                                      AS ship_only_cash_net_revenue,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN ship_only_product_net_revenue
               ELSE 0 END)                                                                                      AS ship_only_product_net_revenue,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN ship_only_cash_gross_profit
               ELSE 0 END)                                                                                      AS ship_only_cash_gross_margin,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN ship_only_product_gross_margin
               ELSE 0 END)                                                                                      AS ship_only_product_gross_margin,
       SUM(CASE WHEN order_store_type = 'Retail' THEN subtotal ELSE 0 END)                                      AS subtotal,
       SUM(CASE WHEN order_store_type = 'Retail' THEN shipping_revenue ELSE 0 END)                              AS shipping_revenue,
       SUM(CASE WHEN order_store_type = 'Retail' THEN product_cost ELSE 0 END)                                  AS product_cost,
       SUM(CASE WHEN order_store_type = 'Retail' THEN shipping_supplies_cost ELSE 0 END)                        AS shipping_supplies_cost,
       SUM(CASE WHEN order_store_type = 'Retail' THEN shipping_cost ELSE 0 END)                                 AS shipping_cost,
       SUM(CASE WHEN order_store_type = 'Retail' THEN discount ELSE 0 END)                                      AS discount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN misc_cogs ELSE 0 END)                                     AS misc_cogs,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN credit_redemption_amount
               ELSE 0 END)                                                                                      AS credit_redemption_amount,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN credit_redemption_count
               ELSE 0 END)                                                                                      AS credit_redemption_count,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN total_credits_redeemed
               ELSE 0 END)                                                                                      AS total_credits_redeemed,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN gift_card_issuance_amount
               ELSE 0 END)                                                                                      AS gift_card_issuance_amount,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN conversion_eligible_orders
               ELSE 0 END)                                                                                      AS conversion_eligible_orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN payg_activations ELSE 0 END)                              AS payg_activations,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_online_purchase_retail_returns
               ELSE 0 END)                                                                                      AS online_purchase_retail_returns,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_retail_purchase_retail_returns
               ELSE 0 END)                                                                                      AS retail_purchase_retail_returns,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_retail_purchase_online_returns
               ELSE 0 END)                                                                                      AS retail_purchase_online_returns,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_online_purchase_retail_return_value
               ELSE 0 END)                                                                                      AS online_purchase_retail_return_value,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_retail_purchase_retail_return_value
               ELSE 0 END)                                                                                      AS retail_purchase_retail_return_value,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_retail_purchase_online_return_value
               ELSE 0 END)                                                                                      AS retail_purchase_online_return_value,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_total_exchanges
               ELSE 0 END)                                                                                      AS total_exchanges,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_exchanges_after_online_return
               ELSE 0 END)                                                                                         exchanges_after_online_return,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_exchanges_after_in_store_return
               ELSE 0 END)                                                                                      AS exchanges_after_in_store_return,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_exchanges_after_in_store_return_from_online_purchase
               ELSE 0 END)                                                                                      AS exchanges_after_in_store_return_from_online_purchase,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_total_exchanges_value
               ELSE 0 END)                                                                                      AS total_exchanges_value,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_exchanges_value_after_online_return
               ELSE 0 END)                                                                                      AS exchanges_value_after_online_return,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_exchanges_value_after_in_store_return
               ELSE 0 END)                                                                                      AS exchanges_value_after_in_store_return,
       SUM(CASE
               WHEN order_store_type = 'Retail'
                   THEN financial_exchanges_value_after_in_store_return_from_online_purchase
               ELSE 0 END)                                                                                      AS exchanges_value_after_in_store_return_from_online_purchase,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_online_total_exchanges
               ELSE 0 END)                                                                                      AS online_total_exchanges,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_online_exchanges_after_online_return
               ELSE 0 END)                                                                                         online_exchanges_after_online_return,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_online_exchanges_after_in_store_return
               ELSE 0 END)                                                                                      AS online_exchanges_after_in_store_return,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_online_exchanges_after_in_store_return_from_online_purchase
               ELSE 0 END)                                                                                      AS online_exchanges_after_in_store_return_from_online_purchase,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_online_total_exchanges_value
               ELSE 0 END)                                                                                      AS online_total_exchanges_value,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_online_exchanges_value_after_online_return
               ELSE 0 END)                                                                                      AS online_exchanges_value_after_online_return,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN financial_online_exchanges_value_after_in_store_return
               ELSE 0 END)                                                                                      AS online_exchanges_value_after_in_store_return,
       SUM(CASE
               WHEN order_store_type = 'Retail'
                   THEN financial_online_exchanges_value_after_in_store_return_from_online_purchase
               ELSE 0 END)                                                                                      AS online_exchanges_value_after_in_store_return_from_online_purchase,
       SUM(CASE WHEN order_store_type = 'Retail' THEN omni_orders ELSE 0 END)                                   AS omni_orders,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN product_order_cash_credit_refund_amount
               ELSE 0 END)                                                                                      AS product_order_cash_credit_refund_amount,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN product_order_cash_refund_amount
               ELSE 0 END)                                                                                      AS product_order_cash_refund_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN product_order_refunds ELSE 0 END)                         AS product_order_refunds,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN product_order_cash_chargeback_amount
               ELSE 0 END)                                                                                      AS product_order_cash_chargeback_amount,
       SUM(CASE
               WHEN order_store_type = 'Retail' THEN return_product_cost_amount
               ELSE 0 END)                                                                                      AS return_product_cost_amount,
       SUM(CASE WHEN order_store_type = 'Retail' THEN msrp_price ELSE 0 END)                                    AS msrp_price,
       SUM(CASE WHEN order_store_type = 'Retail' THEN gift_card_orders ELSE 0 END)                              AS gift_card_orders,
       SUM(CASE WHEN order_store_type = 'Retail' THEN gift_card_redemption_amount ELSE 0 END)                   AS gift_card_redemption_amount,
       '0'                                                                                                      AS incoming_visitor_traffic,
       '0'                                                                                                      AS incoming_fittingroom_traffic,
       '0'                                                                                                      AS adjusted_incoming_visitor_traffic,
        0                                                                                                       AS email_capture,
        0                                                                                                       AS eligible_email_capture
FROM _orders1
WHERE order_store_name NOT IN ('Fabletics (DM)', 'Fabletics - Sample Request')
GROUP BY metric_type,
    metric_method,
    customer_gender,
    shopped_channel,
    date,
    administrator_id,
    firstname,
    lastname,
    store_id,
    region,
    store_name,
    longitude,
    latitude,
    store_zip,
    store_city,
    store_state,
    store_region,
    store_district,
    store_opening_date,
    store_number,
    store_retail_type,
    store_type,
    comp_new,
    membership_order_type,
    customer_activation_channel

UNION

SELECT 'Attribution'                                                                                                  AS metric_type,
       'Financial'                                                                                                    AS metric_method,
       customer_gender,
       shopped_channel,
       order_date                                                                                                     AS date,
       administrator_id,
       firstname,
       lastname,
       activation_store_id                                                                                            AS store_id,
       activation_region                                                                                              AS region,
       activation_store_name                                                                                          AS store_name,
       activation_store_longitude                                                                                     AS longitude,
       activation_store_latitude                                                                                      AS latitude,
       activation_store_zip_code                                                                                      AS store_zip,
       activation_store_city                                                                                          AS store_city,
       activation_store_state                                                                                         AS store_state,
       activation_store_region                                                                                        AS store_region,
       activation_store_district                                                                                      AS store_district,
       activation_store_open_date                                                                                     AS store_opening_date,
       activation_store_number                                                                                        AS store_number,
       retail_activation_store_type                                                                                   AS store_retail_type,
       customer_activation_channel                                                                                    AS store_type,
       CASE
           WHEN DATEDIFF(MONTH, IFNULL(activation_store_open_date, '2079-01-01'), order_date) >= 13 THEN 'Comp'
           ELSE 'New' END                                                                                             AS comp_new,
       membership_order_type,
       customer_activation_channel,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN orders
               ELSE 0 END)                                                                                            AS orders,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN units
               ELSE 0 END)                                                                                            AS units,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN ship_only_orders
               ELSE 0 END)                                                                                            AS ship_only_orders,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN ship_only_units
               ELSE 0 END)                                                                                            AS ship_only_units,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN zero_revenue_units
               ELSE 0 END)                                                                                            AS zero_revenue_units,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN cash_gross_revenue
               ELSE 0 END)                                                                                            AS cash_gross_revenue,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN product_gross_revenue
               ELSE 0 END)                                                                                            AS product_gross_revenue,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN cash_net_revenue
               ELSE 0 END)                                                                                            AS cash_net_revenue,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN product_net_revenue
               ELSE 0 END)                                                                                            AS product_net_revenue,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN cash_gross_margin
               ELSE 0 END)                                                                                            AS cash_gross_margin,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN product_gross_margin
               ELSE 0 END)                                                                                            AS product_gross_margin,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN ship_only_cash_gross_revenue
               ELSE 0 END)                                                                                            AS ship_only_cash_gross_revenue,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN ship_only_product_gross_revenue
               ELSE 0 END)                                                                                            AS ship_only_product_gross_revenue,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN ship_only_cash_net_revenue
               ELSE 0 END)                                                                                            AS ship_only_cash_net_revenue,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN ship_only_product_net_revenue
               ELSE 0 END)                                                                                            AS ship_only_product_net_revenue,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN ship_only_cash_gross_profit
               ELSE 0 END)                                                                                            AS ship_only_cash_gross_margin,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN ship_only_product_gross_margin
               ELSE 0 END)                                                                                            AS ship_only_product_gross_margin,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN subtotal
               ELSE 0 END)                                                                                            AS subtotal,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN shipping_revenue
               ELSE 0 END)                                                                                            AS shipping_revenue,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN product_cost
               ELSE 0 END)                                                                                            AS product_cost,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN shipping_supplies_cost
               ELSE 0 END)                                                                                            AS shipping_supplies_cost,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN shipping_cost
               ELSE 0 END)                                                                                            AS shipping_cost,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN discount
               ELSE 0 END)                                                                                            AS discount,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN misc_cogs
               ELSE 0 END)                                                                                            AS misc_cogs,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN credit_redemption_amount
               ELSE 0 END)                                                                                            AS credit_redemption_amount,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN credit_redemption_count
               ELSE 0 END)                                                                                            AS credit_redemption_count,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN total_credits_redeemed
               ELSE 0 END)                                                                                            AS total_credits_redeemed,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN gift_card_issuance_amount
               ELSE 0 END)                                                                                            AS gift_card_issuance_amount,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN conversion_eligible_orders
               ELSE 0 END)                                                                                            AS conversion_eligible_orders,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN payg_activations
               ELSE 0 END)                                                                                            AS payg_activations,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_online_purchase_retail_returns
               ELSE 0 END)                                                                                            AS online_purchase_retail_returns,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_retail_purchase_retail_returns
               ELSE 0 END)                                                                                            AS retail_purchase_retail_returns,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_retail_purchase_online_returns
               ELSE 0 END)                                                                                            AS retail_purchase_online_returns,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_online_purchase_retail_return_value
               ELSE 0 END)                                                                                            AS online_purchase_retail_return_value,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_retail_purchase_retail_return_value
               ELSE 0 END)                                                                                            AS retail_purchase_retail_return_value,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_retail_purchase_online_return_value
               ELSE 0 END)                                                                                            AS retail_purchase_online_return_value,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_total_exchanges
               ELSE 0 END)                                                                                            AS total_exchanges,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_exchanges_after_online_return
               ELSE 0 END)                                                                                               exchanges_after_online_return,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_exchanges_after_in_store_return
               ELSE 0 END)                                                                                            AS exchanges_after_in_store_return,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_exchanges_after_in_store_return_from_online_purchase
               ELSE 0 END)                                                                                            AS exchanges_after_in_store_return_from_online_purchase,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_total_exchanges_value
               ELSE 0 END)                                                                                            AS total_exchanges_value,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_exchanges_value_after_online_return
               ELSE 0 END)                                                                                            AS exchanges_value_after_online_return,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_exchanges_value_after_in_store_return
               ELSE 0 END)                                                                                            AS exchanges_value_after_in_store_return,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_exchanges_value_after_in_store_return_from_online_purchase
               ELSE 0 END)                                                                                            AS exchanges_value_after_in_store_return_from_online_purchase,

       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_online_total_exchanges
               ELSE 0 END)                                                                                            AS online_total_exchanges,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_online_exchanges_after_online_return
               ELSE 0 END)                                                                                               online_exchanges_after_online_return,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_online_exchanges_after_in_store_return
               ELSE 0 END)                                                                                            AS online_exchanges_after_in_store_return,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_online_exchanges_after_in_store_return_from_online_purchase
               ELSE 0 END)                                                                                            AS online_exchanges_after_in_store_return_from_online_purchase,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_online_total_exchanges_value
               ELSE 0 END)                                                                                            AS online_total_exchanges_value,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_online_exchanges_value_after_online_return
               ELSE 0 END)                                                                                            AS online_exchanges_value_after_online_return,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_online_exchanges_value_after_in_store_return
               ELSE 0 END)                                                                                            AS online_exchanges_value_after_in_store_return,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN financial_online_exchanges_value_after_in_store_return_from_online_purchase
               ELSE 0 END)                                                                                            AS online_exchanges_value_after_in_store_return_from_online_purchase,

       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN omni_orders
               ELSE 0 END)                                                                                            AS omni_orders,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN product_order_cash_credit_refund_amount
               ELSE 0 END)                                                                                            AS product_order_cash_credit_refund_amount,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN product_order_cash_refund_amount
               ELSE 0 END)                                                                                            AS product_order_cash_refund_amount,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN product_order_refunds
               ELSE 0 END)                                                                                            AS product_order_refunds,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN product_order_cash_chargeback_amount
               ELSE 0 END)                                                                                            AS product_order_cash_chargeback_amount,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail')
                   THEN return_product_cost_amount
               ELSE 0 END)                                                                                            AS return_product_cost_amount,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN msrp_price
               ELSE 0 END)                                                                                            AS msrp_price,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN gift_card_orders
               ELSE 0 END)                                                                                            AS gift_card_orders,
       SUM(CASE
               WHEN (order_store_type != 'Retail' AND customer_activation_channel = 'Retail') THEN gift_card_redemption_amount
               ELSE 0 END)                                                                                            AS gift_card_redemption_amount,
       '0'                                                                                                            AS incoming_visitor_traffic,
       '0'                                                                                                            AS incoming_fittingroom_traffic,
       '0'                                                                                                            AS adjusted_incoming_visitor_traffic,
        0                                                                                                             AS email_capture,
        0                                                                                                             AS eligible_email_capture
FROM _orders1
WHERE order_store_name NOT IN ('Fabletics (DM)', 'Fabletics - Sample Request')
GROUP BY metric_type,
    metric_method,
    customer_gender,
    shopped_channel,
    date,
    administrator_id,
    firstname,
    lastname,
    store_id,
    region,
    store_name,
    longitude,
    latitude,
    store_zip,
    store_city,
    store_state,
    store_region,
    store_district,
    store_opening_date,
    store_number,
    store_retail_type,
    store_type,
    comp_new,
    membership_order_type,
    customer_activation_channel

UNION

SELECT '4-Wall'                                                                                                    AS metric_type,
       'Financial'                                                                                                 AS metric_method,
       NULL                                                                                                        AS customer_gender,
       NULL                                                                                                        AS shopped_channel,
       date,
       NULL                                                                                                        AS administrator_id,
       NULL                                                                                                        AS firstname,
       NULL                                                                                                        AS lastname,
       st.store_id,
       st.store_region                                                                                             AS region,
       st.store_full_name                                                                                          AS store_name,
       zcs.longitude                                                                                               AS longitude,
       zcs.latitude                                                                                                AS latitude,
       st.store_retail_zip_code                                                                                    AS store_zip,
       st.store_retail_city                                                                                        AS store_city,
       st.store_retail_state                                                                                       AS store_state,
       rsd2.region                                                                                                 AS store_region,
       rsd2.district                                                                                               AS store_district,
       rsd.store_open_date                                                                                         AS store_opening_date,
       st.store_retail_location_code                                                                               AS store_number,
       CASE
           WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type
           ELSE 'Online' END                                                                                       AS store_retail_type,
       st.store_type                                                                                               AS store_type,
       CASE
           WHEN DATEDIFF(MONTH, IFNULL(rsd.store_open_date, '2079-01-01'), date) >= 13 THEN 'Comp'
           ELSE 'New' END                                                                                          AS comp_new,
       NULL                                                                                                        AS membership_order_type,
       NULL                                                                                                        AS customer_activation_channel,
       '0'                                                                                                         AS orders,
       '0'                                                                                                         AS units,
       '0'                                                                                                         AS ship_only_orders,
       '0'                                                                                                         AS ship_only_units,
       '0'                                                                                                         AS zero_revenue_units,
       '0'                                                                                                         AS cash_gross_revenue,
       '0'                                                                                                         AS product_gross_revenue,
       '0'                                                                                                         AS cash_net_revenue,
       '0'                                                                                                         AS product_net_revenue,
       '0'                                                                                                         AS cash_gross_margin,
       '0'                                                                                                         AS product_gross_margin,
       '0'                                                                                                         AS ship_only_cash_gross_revenue,
       '0'                                                                                                         AS ship_only_product_gross_revenue,
       '0'                                                                                                         AS ship_only_cash_net_revenue,
       '0'                                                                                                         AS ship_only_product_net_revenue,
       '0'                                                                                                         AS ship_only_cash_gross_margin,
       '0'                                                                                                         AS ship_only_product_gross_margin,
       '0'                                                                                                         AS subtotal,
       '0'                                                                                                         AS shipping_revenue,
       '0'                                                                                                         AS product_cost,
       '0'                                                                                                         AS shipping_supplies_cost,
       '0'                                                                                                         AS shipping_cost,
       '0'                                                                                                         AS discount,
       '0'                                                                                                         AS misc_cogs,
       '0'                                                                                                         AS credit_redemption_amount,
       '0'                                                                                                         AS credit_redemption_count,
       '0'                                                                                                         AS total_credits_redeemed,
       '0'                                                                                                         AS gift_card_issuance_amount,
       '0'                                                                                                         AS conversion_eligible_orders,
       '0'                                                                                                         AS payg_activations,
       '0'                                                                                                         AS online_purchase_retail_returns,
       '0'                                                                                                         AS retail_purchase_retail_returns,
       '0'                                                                                                         AS retail_purchase_online_returns,
       '0'                                                                                                         AS online_purchase_retail_return_value,
       '0'                                                                                                         AS retail_purchase_retail_return_value,
       '0'                                                                                                         AS retail_purchase_online_return_value,
       '0'                                                                                                         AS total_exchanges,
       '0'                                                                                                         AS exchanges_after_online_return,
       '0'                                                                                                         AS exchanges_after_in_store_return,
       '0'                                                                                                         AS exchanges_after_in_store_return_from_online_purchase,
       '0'                                                                                                         AS total_exchanges_value,
       '0'                                                                                                         AS exchanges_value_after_online_return,
       '0'                                                                                                         AS exchanges_value_after_in_store_return,
       '0'                                                                                                         AS exchanges_value_after_in_store_return_from_online_purchase,
       '0'                                                                                                         AS online_total_exchanges,
       '0'                                                                                                         AS online_exchanges_after_online_return,
       '0'                                                                                                         AS online_exchanges_after_in_store_return,
       '0'                                                                                                         AS online_exchanges_after_in_store_return_from_online_purchase,
       '0'                                                                                                         AS online_total_exchanges_value,
       '0'                                                                                                         AS online_exchanges_value_after_online_return,
       '0'                                                                                                         AS online_exchanges_value_after_in_store_return,
       '0'                                                                                                         AS online_exchanges_value_after_in_store_return_from_online_purchase,
       '0'                                                                                                         AS omni_orders,
       '0'                                                                                                         AS product_order_cash_credit_refund_amount,
       '0'                                                                                                         AS product_order_cash_refund_amount,
       '0'                                                                                                         AS product_order_refunds,
       '0'                                                                                                         AS product_order_cash_chargeback_amount,
       '0'                                                                                                         AS return_product_cost_amount,
       '0'                                                                                                         AS msrp_price,
       '0'                                                                                                         AS gift_card_orders,
       '0'                                                                                                         AS gift_card_redemption_amount,
       SUM(incoming_visitor_traffic)                                                                               AS incoming_visitor_traffic,
       SUM(incoming_fittingroom_traffic)                                                                           AS incoming_fittingroom_traffic,
       SUM(adjusted_incoming_visitor_traffic)                                                                      AS adjusted_incoming_visitor_traffic,
       0                                                                                                           AS email_capture,
       0                                                                                                           AS eligible_email_capture
FROM reporting_prod.retail.retail_traffic rt
         JOIN edw_prod.data_model_fl.dim_store st ON st.store_id = rt.store_id
         LEFT JOIN _store_dimensions rsd ON rsd.store_id = rt.store_id
         LEFT JOIN lake_view.sharepoint.fl_retail_store_detail rsd2 ON rsd2.store_id = rt.store_id
         LEFT JOIN lake_fl_view.ultra_merchant.zip_city_state zcs ON zcs.zip = st.store_retail_zip_code
WHERE st.store_full_name NOT IN ('Fabletics (DM)', 'Fabletics - Sample Request')
GROUP BY metric_type,
    metric_method,
    customer_gender,
    shopped_channel,
    date,
    administrator_id,
    firstname,lastname,
    st.store_id,
    region,
    st.store_full_name,
    longitude,
    latitude,
    store_zip,
    store_city,
    store_state,
    store_region,
    store_district,
    store_opening_date,
    st.store_retail_location_code,
    store_retail_type,
    st.store_type,
    comp_new,
    membership_order_type,
    customer_activation_channel

 UNION

SELECT '4-Wall'                                                                                                    AS metric_type,
       'Financial'                                                                                                  AS metric_method,
       gender                                                                                                        AS customer_gender,
       NULL                                                                                                        AS shopped_channel,
       fr.registration_local_datetime::date                                                                        AS date,
       NULL                                                                                                        AS administrator_id,
       NULL                                                                                                        AS firstname,
       NULL                                                                                                        AS lastname,
       st.store_id,
       st.store_region                                                                                             AS region,
       st.store_full_name                                                                                          AS store_name,
       zcs.longitude                                                                                               AS longitude,
       zcs.latitude                                                                                                AS latitude,
       st.store_retail_zip_code                                                                                    AS store_zip,
       st.store_retail_city                                                                                        AS store_city,
       st.store_retail_state                                                                                       AS store_state,
       rsd2.region                                                                                                 AS store_region,
       rsd2.district                                                                                               AS store_district,
       rsd.store_open_date                                                                                         AS store_opening_date,
       st.store_retail_location_code                                                                               AS store_number,
       CASE
           WHEN st.store_type ILIKE 'Retail' THEN st.store_sub_type
           ELSE 'Online' END                                                                                       AS store_retail_type,
       st.store_type                                                                                               AS store_type,
       CASE
           WHEN DATEDIFF(MONTH, IFNULL(rsd.store_open_date, '2079-01-01'), date) >= 13 THEN 'Comp'
           ELSE 'New' END                                                                                          AS comp_new,
       NULL                                                                                                        AS membership_order_type,
       NULL                                                                                                        AS customer_activation_channel,
       '0'                                                                                                         AS orders,
       '0'                                                                                                         AS units,
       '0'                                                                                                         AS ship_only_orders,
       '0'                                                                                                         AS ship_only_units,
       '0'                                                                                                         AS zero_revenue_units,
       '0'                                                                                                         AS cash_gross_revenue,
       '0'                                                                                                         AS product_gross_revenue,
       '0'                                                                                                         AS cash_net_revenue,
       '0'                                                                                                         AS product_net_revenue,
       '0'                                                                                                         AS cash_gross_margin,
       '0'                                                                                                         AS product_gross_margin,
       '0'                                                                                                         AS ship_only_cash_gross_revenue,
       '0'                                                                                                         AS ship_only_product_gross_revenue,
       '0'                                                                                                         AS ship_only_cash_net_revenue,
       '0'                                                                                                         AS ship_only_product_net_revenue,
       '0'                                                                                                         AS ship_only_cash_gross_margin,
       '0'                                                                                                         AS ship_only_product_gross_margin,
       '0'                                                                                                         AS subtotal,
       '0'                                                                                                         AS shipping_revenue,
       '0'                                                                                                         AS product_cost,
       '0'                                                                                                         AS shipping_supplies_cost,
       '0'                                                                                                         AS shipping_cost,
       '0'                                                                                                         AS discount,
       '0'                                                                                                         AS misc_cogs,
       '0'                                                                                                         AS credit_redemption_amount,
       '0'                                                                                                         AS credit_redemption_count,
       '0'                                                                                                         AS total_credits_redeemed,
       '0'                                                                                                         AS gift_card_issuance_amount,
       '0'                                                                                                         AS conversion_eligible_orders,
       '0'                                                                                                         AS payg_activations,
       '0'                                                                                                         AS online_purchase_retail_returns,
       '0'                                                                                                         AS retail_purchase_retail_returns,
       '0'                                                                                                         AS retail_purchase_online_returns,
       '0'                                                                                                         AS online_purchase_retail_return_value,
       '0'                                                                                                         AS retail_purchase_retail_return_value,
       '0'                                                                                                         AS retail_purchase_online_return_value,
       '0'                                                                                                         AS total_exchanges,
       '0'                                                                                                         AS exchanges_after_online_return,
       '0'                                                                                                         AS exchanges_after_in_store_return,
       '0'                                                                                                         AS exchanges_after_in_store_return_from_online_purchase,
       '0'                                                                                                         AS total_exchanges_value,
       '0'                                                                                                         AS exchanges_value_after_online_return,
       '0'                                                                                                         AS exchanges_value_after_in_store_return,
       '0'                                                                                                         AS exchanges_value_after_in_store_return_from_online_purchase,
       '0'                                                                                                         AS online_total_exchanges,
       '0'                                                                                                         AS online_exchanges_after_online_return,
       '0'                                                                                                         AS online_exchanges_after_in_store_return,
       '0'                                                                                                         AS online_exchanges_after_in_store_return_from_online_purchase,
       '0'                                                                                                         AS online_total_exchanges_value,
       '0'                                                                                                         AS online_exchanges_value_after_online_return,
       '0'                                                                                                         AS online_exchanges_value_after_in_store_return,
       '0'                                                                                                         AS online_exchanges_value_after_in_store_return_from_online_purchase,
       '0'                                                                                                         AS omni_orders,
       '0'                                                                                                         AS product_order_cash_credit_refund_amount,
       '0'                                                                                                         AS product_order_cash_refund_amount,
       '0'                                                                                                         AS product_order_refunds,
       '0'                                                                                                         AS product_order_cash_chargeback_amount,
       '0'                                                                                                         AS return_product_cost_amount,
       '0'                                                                                                         AS msrp_price,
       '0'                                                                                                         AS gift_card_orders,
       '0'                                                                                                         AS gift_card_redemption_amount,
       '0'                                                                                                         AS incoming_visitor_traffic,
       '0'                                                                                                         AS incoming_fittingroom_traffic,
       '0'                                                                                                         AS adjusted_incoming_visitor_traffic,
       count(distinct iff(dc.email ilike '%retail.fabletics.com%',null,fr.customer_id))                            AS email_capture,
       count(distinct iff(dc.email ilike '%retail.fabletics.com%',fr.customer_id,null))                            AS eligible_email_capture
FROM edw_prod.data_model_fl.fact_registration fr
JOIN edw_prod.data_model_fl.fact_order fo on fo.customer_id = fr.customer_id
                                                and fr.registration_local_datetime::date = fo.order_local_datetime ::date
                                                and order_status_key = 1 -- successful order
         JOIN edw_prod.data_model_fl.dim_store st ON st.store_id = fr.retail_location
         JOIN edw_prod.data_model_fl.dim_customer dc ON dc.customer_id = fr.customer_id
         LEFT JOIN _store_dimensions rsd ON rsd.store_id = fr.retail_location
         LEFT JOIN lake_view.sharepoint.fl_retail_store_detail rsd2 ON rsd2.store_id = fr.retail_location
         LEFT JOIN lake_fl_view.ultra_merchant.zip_city_state zcs ON zcs.zip = st.store_retail_zip_code
WHERE st.store_full_name NOT IN ('Fabletics (DM)', 'Fabletics - Sample Request')
AND is_retail_registration = 'TRUE'
AND is_fake_retail_registration = 'FALSE'
AND is_secondary_registration = 'FALSE'
AND is_reactivated_lead = 'FALSE'
AND fr.registration_local_datetime::date >= '2015-09-11'
GROUP BY metric_type,
    metric_method,
    customer_gender,
    shopped_channel,
    date,
    /*administrator_id,
    firstname,
    lastname,*/
    st.store_id,
    region,
    st.store_full_name,
    longitude,
    latitude,
    store_zip,
    store_city,
    store_state,
    store_region,
    store_district,
    store_opening_date,
    st.store_retail_location_code,
    store_retail_type,
    st.store_type,
    comp_new,
    membership_order_type,
    customer_activation_channel
;


CREATE OR REPLACE TEMP TABLE _fabletics_retail_dataset AS
SELECT metric_type,
       metric_method,
       customer_gender,
       shopped_channel,
       date,
       administrator_id,
       firstname,
       lastname,
       store_id,
       store_name,
       region,
       longitude,
       latitude,
       store_zip,
       store_city,
       store_state,
       store_region,
       store_district,
       store_opening_date,
       store_number,
       store_retail_type,
       store_type,
       comp_new,
       membership_order_type,
       customer_activation_channel,
       $last_refreshed_datetime                     AS last_refreshed_datetime,
       SUM(email_capture)                           AS email_capture,
       SUM(eligible_email_capture)                  AS eligible_email_capture,
       SUM(orders)                                  AS orders,
       SUM(units)                                   AS units,
       SUM(ship_only_orders)                        AS ship_only_orders,
       SUM(ship_only_units)                         AS ship_only_units,
       SUM(zero_revenue_units)                      AS zero_revenue_units,
       SUM(cash_gross_revenue)                      AS cash_gross_revenue,
       SUM(product_gross_revenue)                   AS product_gross_revenue,
       SUM(cash_net_revenue)                        AS cash_net_revenue,
       SUM(product_net_revenue)                     AS product_net_revenue,
       SUM(cash_gross_margin)                       AS cash_gross_margin,
       SUM(product_gross_margin)                    AS product_gross_margin,
       SUM(ship_only_cash_gross_revenue)            AS ship_only_cash_gross_revenue,
       SUM(ship_only_product_gross_revenue)         AS ship_only_product_gross_revenue,
       SUM(ship_only_cash_net_revenue)              AS ship_only_cash_net_revenue,
       SUM(ship_only_product_net_revenue)           AS ship_only_product_net_revenue,
       SUM(ship_only_cash_gross_margin)             AS ship_only_cash_gross_margin,
       SUM(ship_only_product_gross_margin)          AS ship_only_product_gross_margin,
       SUM(subtotal)                                AS subtotal,
       SUM(shipping_revenue)                        AS shipping_revenue,
       SUM(product_cost)                            AS product_cost,
       SUM(shipping_supplies_cost)                  AS shipping_supplies_cost,
       SUM(shipping_cost)                           AS shipping_cost,
       SUM(discount)                                AS discount,
       SUM(misc_cogs)                               AS misc_cogs,
       SUM(credit_redemption_amount)                AS credit_redemption_amount,
       SUM(credit_redemption_count)                 AS credit_redemption_count,
       SUM(total_credits_redeemed)                  AS total_credits_redeemed,
       SUM(gift_card_issuance_amount)               AS gift_card_issuance_amount,
       SUM(conversion_eligible_orders)              AS conversion_eligible_orders,
       SUM(payg_activations)                        AS payg_activations,
       SUM(online_purchase_retail_returns)          AS online_purchase_retail_returns,
       SUM(online_purchase_retail_return_value)     AS online_purchase_retail_return_value,
       SUM(retail_purchase_retail_returns)          AS retail_purchase_retail_returns,
       SUM(retail_purchase_retail_return_value)     AS retail_purchase_retail_return_value,
       SUM(retail_purchase_online_returns)          AS retail_purchase_online_returns,
       SUM(retail_purchase_online_return_value)     AS retail_purchase_online_return_value,
       SUM(total_exchanges)                         AS total_exchanges,
       SUM(exchanges_after_online_return)           AS exchanges_after_online_return,
       SUM(exchanges_after_in_store_return)         AS exchanges_after_in_store_return,
       SUM(total_exchanges_value)                   AS total_exchanges_value,
       SUM(exchanges_value_after_online_return)     AS exchanges_value_after_online_return,
       SUM(exchanges_value_after_in_store_return)   AS exchanges_value_after_in_store_return,
       SUM(online_total_exchanges)                  AS online_total_exchanges,
       SUM(online_exchanges_after_online_return)    AS online_exchanges_after_online_return,
       SUM(online_exchanges_after_in_store_return)  AS online_exchanges_after_in_store_return,
       SUM(online_total_exchanges_value)            AS online_total_exchanges_value,
       SUM(online_exchanges_value_after_online_return) AS online_exchanges_value_after_online_return,
       SUM(online_exchanges_value_after_in_store_return) AS online_exchanges_value_after_in_store_return,
       SUM(omni_orders)                             AS omni_orders,
       SUM(product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount,
       SUM(product_order_cash_refund_amount)        AS product_order_cash_refund_amount,
       SUM(product_order_refunds)                   AS product_order_refunds,
       SUM(product_order_cash_chargeback_amount)    AS product_order_cash_chargeback_amount,
       SUM(return_product_cost_amount)              AS return_product_cost_amount,
       SUM(msrp_price)                              AS msrp_price,
       SUM(incoming_visitor_traffic)                AS incoming_visitor_traffic,
       SUM(incoming_fittingroom_traffic)            AS incoming_fittingroom_traffic,
       SUM(adjusted_incoming_visitor_traffic)       AS adjusted_incoming_visitor_traffic,
       SUM(exchanges_value_after_in_store_return_from_online_purchase) AS exchanges_value_after_in_store_return_from_online_purchase,
       SUM(exchanges_after_in_store_return_from_online_purchase) AS exchanges_after_in_store_return_from_online_purchase,
       SUM(online_exchanges_value_after_in_store_return_from_online_purchase) AS online_exchanges_value_after_in_store_return_from_online_purchase,
       SUM(online_exchanges_after_in_store_return_from_online_purchase) AS online_exchanges_after_in_store_return_from_online_purchase,
       SUM(gift_card_orders)                        AS gift_card_orders,
       SUM(gift_card_redemption_amount) as gift_card_redemption_amount
FROM _final_temp1
WHERE metric_type = '4-Wall'
  AND store_type = 'Retail'
GROUP BY metric_type,
        metric_method,
        customer_gender,
        shopped_channel,
        date,
        administrator_id,
        firstname,
        lastname,
        store_id,
        store_name,
        region,
        longitude,
        latitude,
        store_zip,
        store_city,
        store_state,
        store_region,
        store_district,
        store_opening_date,
        store_number,
        store_retail_type,
        store_type,
        comp_new,
        membership_order_type,
        customer_activation_channel,
        last_refreshed_datetime

UNION

SELECT 'Attribution'                    AS metric_type,
       metric_method,
       customer_gender,
       shopped_channel,
       date,
       administrator_id,
       firstname,
       lastname,
       store_id,
       store_name,
       region,
       longitude,
       latitude,
       store_zip,
       store_city,
       store_state,
       store_region,
       store_district,
       store_opening_date,
       store_number,
       store_retail_type,
       store_type,
       comp_new,
       membership_order_type,
       customer_activation_channel,
       $last_refreshed_datetime                     AS last_refreshed_datetime,
       SUM(email_capture)                           AS email_capture,
       SUM(eligible_email_capture)                  AS eligible_email_capture,
       SUM(orders)                                  AS orders,
       SUM(units)                                   AS units,
       SUM(ship_only_orders)                        AS ship_only_orders,
       SUM(ship_only_units)                         AS ship_only_units,
       SUM(zero_revenue_units)                      AS zero_revenue_units,
       SUM(cash_gross_revenue)                      AS cash_gross_revenue,
       SUM(product_gross_revenue)                   AS product_gross_revenue,
       SUM(cash_net_revenue)                        AS cash_net_revenue,
       SUM(product_net_revenue)                     AS product_net_revenue,
       SUM(cash_gross_margin)                       AS cash_gross_margin,
       SUM(product_gross_margin)                    AS product_gross_margin,
       SUM(ship_only_cash_gross_revenue)            AS ship_only_cash_gross_revenue,
       SUM(ship_only_product_gross_revenue)         AS ship_only_product_gross_revenue,
       SUM(ship_only_cash_net_revenue)              AS ship_only_cash_net_revenue,
       SUM(ship_only_product_net_revenue)           AS ship_only_product_net_revenue,
       SUM(ship_only_cash_gross_margin)             AS ship_only_cash_gross_margin,
       SUM(ship_only_product_gross_margin)          AS ship_only_product_gross_margin,
       SUM(subtotal)                                AS subtotal,
       SUM(shipping_revenue)                        AS shipping_revenue,
       SUM(product_cost)                            AS product_cost,
       SUM(shipping_supplies_cost)                  AS shipping_supplies_cost,
       SUM(shipping_cost)                           AS shipping_cost,
       SUM(discount)                                AS discount,
       SUM(misc_cogs)                               AS misc_cogs,
       SUM(credit_redemption_amount)                AS credit_redemption_amount,
       SUM(credit_redemption_count)                 AS credit_redemption_count,
       SUM(total_credits_redeemed)                  AS total_credits_redeemed,
       SUM(gift_card_issuance_amount)               AS gift_card_issuance_amount,
       SUM(conversion_eligible_orders)              AS conversion_eligible_orders,
       SUM(payg_activations)                        AS payg_activations,
       SUM(online_purchase_retail_returns)          AS online_purchase_retail_returns,
       SUM(online_purchase_retail_return_value)     AS online_purchase_retail_return_value,
       SUM(retail_purchase_retail_returns)          AS retail_purchase_retail_returns,
       SUM(retail_purchase_retail_return_value)     AS retail_purchase_retail_return_value,
       SUM(retail_purchase_online_returns)          AS retail_purchase_online_returns,
       SUM(retail_purchase_online_return_value)     AS retail_purchase_online_return_value,
       SUM(total_exchanges)                         AS total_exchanges,
       SUM(exchanges_after_online_return)           AS exchanges_after_online_return,
       SUM(exchanges_after_in_store_return)         AS exchanges_after_in_store_return,
       SUM(total_exchanges_value)                   AS total_exchanges_value,
       SUM(exchanges_value_after_online_return)     AS exchanges_value_after_online_return,
       SUM(exchanges_value_after_in_store_return)   AS exchanges_value_after_in_store_return,
       SUM(online_total_exchanges)                  AS online_total_exchanges,
       SUM(online_exchanges_after_online_return)    AS online_exchanges_after_online_return,
       SUM(online_exchanges_after_in_store_return)  AS online_exchanges_after_in_store_return,
       SUM(online_total_exchanges_value)            AS online_total_exchanges_value,
       SUM(online_exchanges_value_after_online_return) AS online_exchanges_value_after_online_return,
       SUM(online_exchanges_value_after_in_store_return) AS online_exchanges_value_after_in_store_return,
       SUM(omni_orders)                             AS omni_orders,
       SUM(product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount,
       SUM(product_order_cash_refund_amount)        AS product_order_cash_refund_amount,
       SUM(product_order_refunds)                   AS product_order_refunds,
       SUM(product_order_cash_chargeback_amount)    AS product_order_cash_chargeback_amount,
       SUM(return_product_cost_amount)              AS return_product_cost_amount,
       SUM(msrp_price)                              AS msrp_price,
       SUM(incoming_visitor_traffic)                AS incoming_visitor_traffic,
       SUM(incoming_fittingroom_traffic)            AS incoming_fittingroom_traffic,
       SUM(adjusted_incoming_visitor_traffic)       AS adjusted_incoming_visitor_traffic,
       SUM(exchanges_value_after_in_store_return_from_online_purchase) AS exchanges_value_after_in_store_return_from_online_purchase,
       SUM(exchanges_after_in_store_return_from_online_purchase) AS exchanges_after_in_store_return_from_online_purchase,
       SUM(online_exchanges_value_after_in_store_return_from_online_purchase) AS online_exchanges_value_after_in_store_return_from_online_purchase,
       SUM(online_exchanges_after_in_store_return_from_online_purchase) AS online_exchanges_after_in_store_return_from_online_purchase,
       SUM(gift_card_orders)                        AS gift_card_orders,
       SUM(gift_card_redemption_amount)             AS gift_card_redemption_amount
FROM _final_temp1
WHERE metric_type IN ('Attribution')
GROUP BY metric_type,
    metric_method,
    customer_gender,
    shopped_channel,
    date,administrator_id,
    firstname,
    lastname,
    store_id,
    store_name,
    region,
    longitude,
    latitude,
    store_zip,
    store_city,
    store_state,
    store_region,
    store_district,
    store_opening_date,
    store_number,
    store_retail_type,
    store_type,
    comp_new,
    membership_order_type,
    customer_activation_channel,
    last_refreshed_datetime;

CREATE OR REPLACE TEMP TABLE _last_year_metrics AS
SELECT COALESCE(frl.metric_type,frl1.metric_type,frl2.metric_type) AS metric_type,
    COALESCE(frl.metric_method,frl1.metric_method,frl2.metric_method) AS metric_method,
    COALESCE(frl.customer_gender,frl1.customer_gender,frl2.customer_gender) AS customer_gender,
    COALESCE(frl.shopped_channel,frl1.shopped_channel,frl2.shopped_channel) AS shopped_channel,
    COALESCE(frl.date, to_date(DATEADD('year', 1, frl1.date)), to_date(DATEADD('year', 2, frl2.date))) AS date,
    COALESCE(frl.administrator_id,frl1.administrator_id,frl2.administrator_id) AS administrator_id,
    COALESCE(frl.firstname,frl1.firstname,frl2.firstname) AS firstname,
    COALESCE(frl.lastname,frl1.lastname,frl2.lastname) AS lastname,
    COALESCE(frl.store_id,frl1.store_id,frl2.store_id) AS store_id,
    COALESCE(frl.store_name,frl1.store_name,frl2.store_name) AS store_name,
    COALESCE(frl.region,frl1.region,frl2.region) AS region,
    COALESCE(frl.longitude,frl1.longitude,frl2.longitude) AS longitude,
    COALESCE(frl.latitude,frl1.latitude,frl2.latitude) AS latitude,
    COALESCE(frl.store_zip,frl1.store_zip,frl2.store_zip) AS store_zip,
    COALESCE(frl.store_city,frl1.store_city,frl2.store_city) AS store_city,
    COALESCE(frl.store_state,frl1.store_state,frl2.store_state) AS store_state,
    COALESCE(frl.store_region,frl1.store_region,frl2.store_region) AS store_region,
    COALESCE(frl.store_district,frl1.store_district,frl2.store_district) AS store_district,
    COALESCE(frl.store_opening_date,frl1.store_opening_date,frl2.store_opening_date) AS store_opening_date,
    COALESCE(frl.store_number,frl1.store_number,frl2.store_number) AS store_number,
    COALESCE(frl.store_retail_type,frl1.store_retail_type,frl2.store_retail_type) AS store_retail_type,
    COALESCE(frl.store_type,frl1.store_type,frl2.store_type) AS store_type,
    COALESCE(frl.comp_new,frl1.comp_new,frl2.comp_new) AS comp_new,
    COALESCE(frl.membership_order_type,frl1.membership_order_type,frl2.membership_order_type) AS membership_order_type,
    COALESCE(frl.customer_activation_channel,frl1.customer_activation_channel,frl2.customer_activation_channel) AS customer_activation_channel,
	COALESCE(frl.last_refreshed_datetime,frl1.last_refreshed_datetime,frl2.last_refreshed_datetime) AS last_refreshed_datetime,
	COALESCE(frl.email_capture,0) AS email_capture,
    COALESCE(frl1.email_capture,0) AS ly_email_capture,
    COALESCE(frl2.email_capture,0) AS lly_email_capture,
    COALESCE(frl.eligible_email_capture,0) AS eligible_email_capture,
    COALESCE(frl1.eligible_email_capture,0) AS ly_eligible_email_capture,
    COALESCE(frl2.eligible_email_capture,0) AS lly_eligible_email_capture,
    COALESCE(frl.orders,0) AS orders,
    COALESCE(frl1.orders,0) AS ly_orders,
    COALESCE(frl2.orders,0) AS lly_orders,
    COALESCE(frl.units,0) AS units,
    COALESCE(frl1.units,0) AS ly_units,
    COALESCE(frl2.units,0) AS lly_units,
    COALESCE(frl.ship_only_orders,0) AS ship_only_orders,
    COALESCE(frl1.ship_only_orders,0) AS ly_ship_only_orders,
    COALESCE(frl2.ship_only_orders,0) AS lly_ship_only_orders,
    COALESCE(frl.ship_only_units,0) AS ship_only_units,
    COALESCE(frl1.ship_only_units,0) AS ly_ship_only_units,
    COALESCE(frl2.ship_only_units,0) AS lly_ship_only_units,
    COALESCE(frl.zero_revenue_units,0) AS zero_revenue_units,
    COALESCE(frl.cash_gross_revenue,0) AS cash_gross_revenue,
	COALESCE(frl1.cash_gross_revenue,0) AS ly_cash_gross_revenue,
    COALESCE(frl2.cash_gross_revenue,0) AS lly_cash_gross_revenue,
    COALESCE(frl.product_gross_revenue,0) AS product_gross_revenue,
    COALESCE(frl1.product_gross_revenue,0) AS ly_product_gross_revenue,
    COALESCE(frl2.product_gross_revenue,0) AS lly_product_gross_revenue,
    COALESCE(frl.cash_net_revenue,0) AS cash_net_revenue,
	COALESCE(frl1.cash_net_revenue,0) AS ly_cash_net_revenue,
    COALESCE(frl2.cash_net_revenue,0) AS lly_cash_net_revenue,
    COALESCE(frl.product_net_revenue,0) AS product_net_revenue,
    COALESCE(frl1.product_net_revenue,0) AS ly_product_net_revenue,
    COALESCE(frl2.product_net_revenue,0) AS lly_product_net_revenue,
    COALESCE(frl.cash_gross_margin,0) AS cash_gross_margin,
	COALESCE(frl1.cash_gross_margin,0) AS ly_cash_gross_margin,
    COALESCE(frl2.cash_gross_margin,0) AS lly_cash_gross_margin,
    COALESCE(frl.product_gross_margin,0) AS product_gross_margin,
    COALESCE(frl1.product_gross_margin,0) AS ly_product_gross_margin,
    COALESCE(frl2.product_gross_margin,0) AS lly_product_gross_margin,
    COALESCE(frl.ship_only_cash_gross_revenue,0) AS ship_only_cash_gross_revenue,
    COALESCE(frl.ship_only_product_gross_revenue,0) AS ship_only_product_gross_revenue,
    COALESCE(frl.ship_only_cash_net_revenue,0) AS ship_only_cash_net_revenue,
    COALESCE(frl.ship_only_product_net_revenue,0) AS ship_only_product_net_revenue,
    COALESCE(frl.ship_only_cash_gross_margin,0) AS ship_only_cash_gross_margin,
    COALESCE(frl.ship_only_product_gross_margin,0) AS ship_only_product_gross_margin,
    COALESCE(frl.subtotal,0) AS subtotal,
    COALESCE(frl.shipping_revenue,0) AS shipping_revenue,
    COALESCE(frl.product_cost,0) AS product_cost,
    COALESCE(frl.shipping_supplies_cost,0) AS shipping_supplies_cost,
    COALESCE(frl.shipping_cost,0) AS shipping_cost,
    COALESCE(frl.discount,0) AS discount,
    COALESCE(frl.misc_cogs,0) AS misc_cogs,
    COALESCE(frl.credit_redemption_amount,0) AS credit_redemption_amount,
    COALESCE(frl1.credit_redemption_amount,0) AS ly_credit_redemption_amount,
    COALESCE(frl2.credit_redemption_amount,0) AS lly_credit_redemption_amount,
    COALESCE(frl.credit_redemption_count,0) AS credit_redemption_count,
    COALESCE(frl.total_credits_redeemed,0) AS total_credits_redeemed,
    COALESCE(frl.gift_card_issuance_amount,0) AS gift_card_issuance_amount,
    COALESCE(frl.conversion_eligible_orders,0) AS conversion_eligible_orders,
    COALESCE(frl1.conversion_eligible_orders,0) AS ly_conversion_eligible_orders,
    COALESCE(frl2.conversion_eligible_orders,0) AS lly_conversion_eligible_orders,
    COALESCE(frl.payg_activations,0) AS payg_activations,
    COALESCE(frl.online_purchase_retail_returns,0) AS online_purchase_retail_returns,
    COALESCE(frl.online_purchase_retail_return_value,0) AS online_purchase_retail_return_value,
    COALESCE(frl.retail_purchase_retail_returns,0) AS retail_purchase_retail_returns,
    COALESCE(frl.retail_purchase_retail_return_value,0) AS retail_purchase_retail_return_value,
    COALESCE(frl.retail_purchase_online_returns,0) AS retail_purchase_online_returns,
    COALESCE(frl.retail_purchase_online_return_value,0) AS retail_purchase_online_return_value,
    COALESCE(frl.total_exchanges,0) AS total_exchanges,
    COALESCE(frl.exchanges_after_online_return,0) AS exchanges_after_online_return,
    COALESCE(frl.exchanges_after_in_store_return,0) AS exchanges_after_in_store_return,
    COALESCE(frl.total_exchanges_value,0) AS total_exchanges_value,
    COALESCE(frl.exchanges_value_after_online_return,0) AS exchanges_value_after_online_return,
    COALESCE(frl.exchanges_value_after_in_store_return,0) AS exchanges_value_after_in_store_return,
    COALESCE(frl.online_total_exchanges,0) AS online_total_exchanges,
    COALESCE(frl.online_exchanges_after_online_return,0) AS online_exchanges_after_online_return,
    COALESCE(frl.online_exchanges_after_in_store_return,0) AS online_exchanges_after_in_store_return,
    COALESCE(frl.online_total_exchanges_value,0) AS online_total_exchanges_value,
    COALESCE(frl.online_exchanges_value_after_online_return,0) AS online_exchanges_value_after_online_return,
    COALESCE(frl.online_exchanges_value_after_in_store_return,0) AS online_exchanges_value_after_in_store_return,
    COALESCE(frl.omni_orders,0) AS omni_orders,
	COALESCE(frl1.omni_orders,0) AS ly_omni_orders,
    COALESCE(frl2.omni_orders,0) AS lly_omni_orders,
    COALESCE(frl.product_order_cash_credit_refund_amount,0) AS product_order_cash_credit_refund_amount,
    COALESCE(frl.product_order_cash_refund_amount,0) AS product_order_cash_refund_amount,
    COALESCE(frl.product_order_refunds,0) AS product_order_refunds,
    COALESCE(frl.product_order_cash_chargeback_amount,0) AS product_order_cash_chargeback_amount,
    COALESCE(frl.return_product_cost_amount,0) AS return_product_cost_amount,
    COALESCE(frl.msrp_price,0) AS msrp_price,
    COALESCE(frl.incoming_visitor_traffic,0) AS incoming_visitor_traffic,
    COALESCE(frl1.incoming_visitor_traffic,0) AS ly_incoming_visitor_traffic,
    COALESCE(frl2.incoming_visitor_traffic,0) AS lly_incoming_visitor_traffic,
    COALESCE(frl.incoming_fittingroom_traffic,0) AS incoming_fittingroom_traffic,
    COALESCE(frl.adjusted_incoming_visitor_traffic,0) AS adjusted_incoming_visitor_traffic,
    COALESCE(frl.exchanges_value_after_in_store_return_from_online_purchase,0) AS exchanges_value_after_in_store_return_from_online_purchase,
    COALESCE(frl.exchanges_after_in_store_return_from_online_purchase,0) AS exchanges_after_in_store_return_from_online_purchase,
    COALESCE(frl.online_exchanges_value_after_in_store_return_from_online_purchase,0) AS online_exchanges_value_after_in_store_return_from_online_purchase,
    COALESCE(frl.online_exchanges_after_in_store_return_from_online_purchase,0) AS online_exchanges_after_in_store_return_from_online_purchase,
    COALESCE(frl.gift_card_orders,0) AS gift_card_orders,
    COALESCE(frl1.gift_card_orders,0) AS ly_gift_card_orders,
    COALESCE(frl2.gift_card_orders,0) AS lly_gift_card_orders,
    COALESCE(frl.gift_card_redemption_amount,0) AS gift_card_redemption_amount,
    COALESCE(frl1.gift_card_redemption_amount,0) AS ly_gift_card_redemption_amount,
    COALESCE(frl2.gift_card_redemption_amount,0) AS lly_gift_card_redemption_amount
FROM _fabletics_retail_dataset frl
FULL JOIN _fabletics_retail_dataset frl1 ON frl.METRIC_TYPE=frl1.METRIC_TYPE
    AND frl.comp_new = frl1.comp_new
    AND COALESCE(frl.customer_gender,'NA' )    = COALESCE(frl1.customer_gender,'NA')
    AND year(frl.date)                  = year(frl1.date)+1
    AND month(frl.date)                  =month(frl1.date)
    AND day(frl.date)                  =day(frl1.date)
    AND COALESCE(frl.ADMINISTRATOR_ID,99999)     =COALESCE(frl1.ADMINISTRATOR_ID,99999)
    AND COALESCE(frl.SHOPPED_CHANNEL,'NA')       =COALESCE(frl1.SHOPPED_CHANNEL,'NA')
    AND COALESCE(frl.STORE_ID,99999)             =COALESCE(frl1.STORE_ID,99999)
    AND COALESCE(frl.MEMBERSHIP_ORDER_TYPE,'NA') =COALESCE(frl1.MEMBERSHIP_ORDER_TYPE,'NA')
    AND COALESCE(frl.CUSTOMER_ACTIVATION_CHANNEL,'none')=COALESCE(frl1.CUSTOMER_ACTIVATION_CHANNEL,'none')
FULL JOIN _fabletics_retail_dataset  frl2 ON COALESCE(year(frl.date),year(frl1.date)+1) = (year(frl2.date)+ 2)
    AND COALESCE(month(frl.date),month(frl1.date)) = (month(frl2.date))
    AND COALESCE(day(frl.date),day(frl1.date)) = day(frl2.date)
    AND COALESCE(frl.metric_type,frl1.metric_type) = frl2.metric_type
    AND COALESCE(frl.comp_new,frl1.comp_new) = frl2.comp_new
    AND COALESCE(frl.customer_gender,frl1.customer_gender,'NA')       =COALESCE(frl2.customer_gender,'NA')
    AND COALESCE(frl.ADMINISTRATOR_ID,frl1.administrator_id,99999)     =COALESCE(frl2.ADMINISTRATOR_ID,99999)
    AND COALESCE(frl.SHOPPED_CHANNEL,frl1.shopped_channel,'NA')       =COALESCE(frl2.SHOPPED_CHANNEL,'NA')
    AND COALESCE(frl.STORE_ID,frl1.store_id,99999)             =COALESCE(frl2.STORE_ID,99999)
    AND COALESCE(frl.MEMBERSHIP_ORDER_TYPE,frl1.MEMBERSHIP_ORDER_TYPE,'NA') =COALESCE(frl2.MEMBERSHIP_ORDER_TYPE,'NA')
    AND COALESCE(frl.CUSTOMER_ACTIVATION_CHANNEL,frl1.CUSTOMER_ACTIVATION_CHANNEL,'none')=COALESCE(frl2.CUSTOMER_ACTIVATION_CHANNEL,'none')
WHERE COALESCE(frl.date, DATEADD('year',1, frl1.date), DATEADD('year', 2, frl2.date)) < CURRENT_DATE;

CREATE OR REPLACE TEMPORARY TABLE _feb_data_agg AS
SELECT METRIC_TYPE,
       METRIC_METHOD,
       CUSTOMER_GENDER,
       SHOPPED_CHANNEL,
       DATE,
       ADMINISTRATOR_ID,
       FIRSTNAME,
       LASTNAME,
       STORE_ID,
       STORE_NAME,
       REGION,
       LONGITUDE,
       LATITUDE,
       STORE_ZIP,
       STORE_CITY,
       STORE_STATE,
       STORE_REGION,
       STORE_DISTRICT,
       STORE_OPENING_DATE,
       STORE_NUMBER,
       STORE_RETAIL_TYPE,
       STORE_TYPE,
       COMP_NEW,
       MEMBERSHIP_ORDER_TYPE,
       CUSTOMER_ACTIVATION_CHANNEL,
       LAST_REFRESHED_DATETIME,
       SUM(email_capture) AS email_capture,
       SUM(ly_email_capture) AS ly_email_capture,
       SUM(lly_email_capture) AS lly_email_capture,
       SUM(eligible_email_capture) AS eligible_email_capture,
       SUM(ly_eligible_email_capture) AS ly_eligible_email_capture,
       SUM(lly_eligible_email_capture) AS lly_eligible_email_capture,
       SUM(ORDERS) AS ORDERS,
       SUM(LY_ORDERS) AS LY_ORDERS,
       SUM(LLY_ORDERS) AS LLY_ORDERS,
       SUM(UNITS) AS UNITS,
       SUM(LY_UNITS) AS LY_UNITS,
       SUM(LLY_UNITS) AS LLY_UNITS,
       SUM(SHIP_ONLY_ORDERS) AS SHIP_ONLY_ORDERS,
       SUM(LY_SHIP_ONLY_ORDERS) AS LY_SHIP_ONLY_ORDERS,
       SUM(LLY_SHIP_ONLY_ORDERS) AS LLY_SHIP_ONLY_ORDERS,
       SUM(SHIP_ONLY_UNITS) AS SHIP_ONLY_UNITS,
       SUM(LY_SHIP_ONLY_UNITS) AS LY_SHIP_ONLY_UNITS,
       SUM(LLY_SHIP_ONLY_UNITS) AS LLY_SHIP_ONLY_UNITS,
       SUM(ZERO_REVENUE_UNITS) AS ZERO_REVENUE_UNITS,
       SUM(CASH_GROSS_REVENUE) AS CASH_GROSS_REVENUE,
       SUM(LY_CASH_GROSS_REVENUE) AS LY_CASH_GROSS_REVENUE,
       SUM(LLY_CASH_GROSS_REVENUE) AS LLY_CASH_GROSS_REVENUE,
       SUM(PRODUCT_GROSS_REVENUE) AS PRODUCT_GROSS_REVENUE,
       SUM(LY_PRODUCT_GROSS_REVENUE) AS LY_PRODUCT_GROSS_REVENUE,
       SUM(LLY_PRODUCT_GROSS_REVENUE) AS LLY_PRODUCT_GROSS_REVENUE,
       SUM(CASH_NET_REVENUE) AS CASH_NET_REVENUE,
       SUM(LY_CASH_NET_REVENUE) AS LY_CASH_NET_REVENUE,
       SUM(LLY_CASH_NET_REVENUE) AS LLY_CASH_NET_REVENUE,
       SUM(PRODUCT_NET_REVENUE) AS PRODUCT_NET_REVENUE,
       SUM(LY_PRODUCT_NET_REVENUE) AS LY_PRODUCT_NET_REVENUE,
       SUM(LLY_PRODUCT_NET_REVENUE) AS LLY_PRODUCT_NET_REVENUE,
       SUM(CASH_GROSS_MARGIN) AS CASH_GROSS_MARGIN,
       SUM(LY_CASH_GROSS_MARGIN) AS LY_CASH_GROSS_MARGIN,
       SUM(LLY_CASH_GROSS_MARGIN) AS LLY_CASH_GROSS_MARGIN,
       SUM(PRODUCT_GROSS_MARGIN) AS PRODUCT_GROSS_MARGIN,
       SUM(LY_PRODUCT_GROSS_MARGIN) AS LY_PRODUCT_GROSS_MARGIN,
       SUM(LLY_PRODUCT_GROSS_MARGIN) AS LLY_PRODUCT_GROSS_MARGIN,
       SUM(SHIP_ONLY_CASH_GROSS_REVENUE) AS SHIP_ONLY_CASH_GROSS_REVENUE,
       SUM(SHIP_ONLY_PRODUCT_GROSS_REVENUE) AS SHIP_ONLY_PRODUCT_GROSS_REVENUE,
       SUM(SHIP_ONLY_CASH_NET_REVENUE) AS SHIP_ONLY_CASH_NET_REVENUE,
       SUM(SHIP_ONLY_PRODUCT_NET_REVENUE) AS SHIP_ONLY_PRODUCT_NET_REVENUE,
       SUM(SHIP_ONLY_CASH_GROSS_MARGIN) AS SHIP_ONLY_CASH_GROSS_MARGIN,
       SUM(SHIP_ONLY_PRODUCT_GROSS_MARGIN) AS SHIP_ONLY_PRODUCT_GROSS_MARGIN,
       SUM(SUBTOTAL) AS SUBTOTAL,
       SUM(SHIPPING_REVENUE) AS SHIPPING_REVENUE,
       SUM(PRODUCT_COST) AS PRODUCT_COST,
       SUM(SHIPPING_SUPPLIES_COST) AS SHIPPING_SUPPLIES_COST,
       SUM(SHIPPING_COST) AS SHIPPING_COST,
       SUM(DISCOUNT) AS DISCOUNT,
       SUM(MISC_COGS) AS MISC_COGS,
       SUM(CREDIT_REDEMPTION_AMOUNT) AS CREDIT_REDEMPTION_AMOUNT,
       SUM(LY_CREDIT_REDEMPTION_AMOUNT) AS LY_CREDIT_REDEMPTION_AMOUNT,
       SUM(LLY_CREDIT_REDEMPTION_AMOUNT) AS LLY_CREDIT_REDEMPTION_AMOUNT,
       SUM(CREDIT_REDEMPTION_COUNT) AS CREDIT_REDEMPTION_COUNT,
       SUM(TOTAL_CREDITS_REDEEMED) AS TOTAL_CREDITS_REDEEMED,
       SUM(GIFT_CARD_ISSUANCE_AMOUNT) AS GIFT_CARD_ISSUANCE_AMOUNT,
       SUM(CONVERSION_ELIGIBLE_ORDERS) AS CONVERSION_ELIGIBLE_ORDERS,
       SUM(LY_CONVERSION_ELIGIBLE_ORDERS) AS LY_CONVERSION_ELIGIBLE_ORDERS,
       SUM(LLY_CONVERSION_ELIGIBLE_ORDERS) AS LLY_CONVERSION_ELIGIBLE_ORDERS,
       SUM(PAYG_ACTIVATIONS) AS PAYG_ACTIVATIONS,
       SUM(ONLINE_PURCHASE_RETAIL_RETURNS) AS ONLINE_PURCHASE_RETAIL_RETURNS,
       SUM(ONLINE_PURCHASE_RETAIL_RETURN_VALUE) AS ONLINE_PURCHASE_RETAIL_RETURN_VALUE,
       SUM(RETAIL_PURCHASE_RETAIL_RETURNS) AS RETAIL_PURCHASE_RETAIL_RETURNS,
       SUM(RETAIL_PURCHASE_RETAIL_RETURN_VALUE) AS RETAIL_PURCHASE_RETAIL_RETURN_VALUE,
       SUM(RETAIL_PURCHASE_ONLINE_RETURNS) AS RETAIL_PURCHASE_ONLINE_RETURNS,
       SUM(RETAIL_PURCHASE_ONLINE_RETURN_VALUE) AS RETAIL_PURCHASE_ONLINE_RETURN_VALUE,
       SUM(TOTAL_EXCHANGES) AS TOTAL_EXCHANGES,
       SUM(EXCHANGES_AFTER_ONLINE_RETURN) AS EXCHANGES_AFTER_ONLINE_RETURN,
       SUM(EXCHANGES_AFTER_IN_STORE_RETURN) AS EXCHANGES_AFTER_IN_STORE_RETURN,
       SUM(TOTAL_EXCHANGES_VALUE) AS TOTAL_EXCHANGES_VALUE,
       SUM(EXCHANGES_VALUE_AFTER_ONLINE_RETURN) AS EXCHANGES_VALUE_AFTER_ONLINE_RETURN,
       SUM(EXCHANGES_VALUE_AFTER_IN_STORE_RETURN) AS EXCHANGES_VALUE_AFTER_IN_STORE_RETURN,
       SUM(ONLINE_TOTAL_EXCHANGES) AS ONLINE_TOTAL_EXCHANGES,
       SUM(ONLINE_EXCHANGES_AFTER_ONLINE_RETURN) AS ONLINE_EXCHANGES_AFTER_ONLINE_RETURN,
       SUM(ONLINE_EXCHANGES_AFTER_IN_STORE_RETURN) AS ONLINE_EXCHANGES_AFTER_IN_STORE_RETURN,
       SUM(ONLINE_TOTAL_EXCHANGES_VALUE) AS ONLINE_TOTAL_EXCHANGES_VALUE,
       SUM(ONLINE_EXCHANGES_VALUE_AFTER_ONLINE_RETURN) AS ONLINE_EXCHANGES_VALUE_AFTER_ONLINE_RETURN,
       SUM(ONLINE_EXCHANGES_VALUE_AFTER_IN_STORE_RETURN) AS ONLINE_EXCHANGES_VALUE_AFTER_IN_STORE_RETURN,
       SUM(OMNI_ORDERS) AS OMNI_ORDERS,
       SUM(LY_OMNI_ORDERS) AS LY_OMNI_ORDERS,
       SUM(LLY_OMNI_ORDERS) AS LLY_OMNI_ORDERS,
       SUM(PRODUCT_ORDER_CASH_CREDIT_REFUND_AMOUNT) AS PRODUCT_ORDER_CASH_CREDIT_REFUND_AMOUNT,
       SUM(PRODUCT_ORDER_CASH_REFUND_AMOUNT) AS PRODUCT_ORDER_CASH_REFUND_AMOUNT,
       SUM(PRODUCT_ORDER_REFUNDS) AS PRODUCT_ORDER_REFUNDS,
       SUM(PRODUCT_ORDER_CASH_CHARGEBACK_AMOUNT) AS PRODUCT_ORDER_CASH_CHARGEBACK_AMOUNT,
       SUM(RETURN_PRODUCT_COST_AMOUNT) AS RETURN_PRODUCT_COST_AMOUNT,
       SUM(MSRP_PRICE) AS MSRP_PRICE,
       SUM(INCOMING_VISITOR_TRAFFIC) AS INCOMING_VISITOR_TRAFFIC,
       SUM(LY_INCOMING_VISITOR_TRAFFIC) AS LY_INCOMING_VISITOR_TRAFFIC,
       SUM(LLY_INCOMING_VISITOR_TRAFFIC) AS LLY_INCOMING_VISITOR_TRAFFIC,
       SUM(INCOMING_FITTINGROOM_TRAFFIC) AS INCOMING_FITTINGROOM_TRAFFIC,
       SUM(ADJUSTED_INCOMING_VISITOR_TRAFFIC) AS ADJUSTED_INCOMING_VISITOR_TRAFFIC,
       SUM(EXCHANGES_VALUE_AFTER_IN_STORE_RETURN_FROM_ONLINE_PURCHASE) AS EXCHANGES_VALUE_AFTER_IN_STORE_RETURN_FROM_ONLINE_PURCHASE,
       SUM(EXCHANGES_AFTER_IN_STORE_RETURN_FROM_ONLINE_PURCHASE) AS EXCHANGES_AFTER_IN_STORE_RETURN_FROM_ONLINE_PURCHASE,
       SUM(ONLINE_EXCHANGES_VALUE_AFTER_IN_STORE_RETURN_FROM_ONLINE_PURCHASE) AS ONLINE_EXCHANGES_VALUE_AFTER_IN_STORE_RETURN_FROM_ONLINE_PURCHASE,
       SUM(ONLINE_EXCHANGES_AFTER_IN_STORE_RETURN_FROM_ONLINE_PURCHASE) AS ONLINE_EXCHANGES_AFTER_IN_STORE_RETURN_FROM_ONLINE_PURCHASE,
       SUM(GIFT_CARD_ORDERS) AS GIFT_CARD_ORDERS,
       SUM(LY_GIFT_CARD_ORDERS) AS LY_GIFT_CARD_ORDERS,
       SUM(LLY_GIFT_CARD_ORDERS) AS LLY_GIFT_CARD_ORDERS,
       SUM(GIFT_CARD_REDEMPTION_AMOUNT) AS GIFT_CARD_REDEMPTION_AMOUNT,
       SUM(LY_GIFT_CARD_REDEMPTION_AMOUNT) AS LY_GIFT_CARD_REDEMPTION_AMOUNT,
       SUM(LLY_GIFT_CARD_REDEMPTION_AMOUNT) AS LLY_GIFT_CARD_REDEMPTION_AMOUNT
FROM _last_year_metrics
WHERE month(date) = 2
    AND day(date) = 28
GROUP BY METRIC_TYPE,
METRIC_METHOD,
CUSTOMER_GENDER,
SHOPPED_CHANNEL,
DATE,
ADMINISTRATOR_ID,
FIRSTNAME,
LASTNAME,
STORE_ID,
STORE_NAME,
REGION,
LONGITUDE,
LATITUDE,
STORE_ZIP,
STORE_CITY,
STORE_STATE,
STORE_REGION,
STORE_DISTRICT,
STORE_OPENING_DATE,
STORE_NUMBER,
STORE_RETAIL_TYPE,
STORE_TYPE,COMP_NEW,
MEMBERSHIP_ORDER_TYPE,
CUSTOMER_ACTIVATION_CHANNEL,
LAST_REFRESHED_DATETIME
;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.retail.fabletics_retail_dataset AS
SELECT *
FROM _feb_data_agg
UNION ALL
SELECT *
FROM _last_year_metrics
WHERE NOT (month(date) = 2 AND day(date) = 28);
