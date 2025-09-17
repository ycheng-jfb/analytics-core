
SET max_snapshot_datetime = (SELECT MAX(snapshot_datetime) FROM fabletics.gc_historical_customer_request_snapshot);

SET thisweek_date = (SELECT IFF(DAYNAME($max_snapshot_datetime) = 'Sun', $max_snapshot_datetime::DATE, DATEADD(DAY, -1, DATE_TRUNC(WEEK, $max_snapshot_datetime))::DATE));
SET lastweek_date = DATEADD(WEEK, -1, $thisweek_date);

SET thisweek_snapshot = (
    SELECT MIN(snapshot_datetime)
    FROM fabletics.gc_historical_customer_request_snapshot
    WHERE snapshot_datetime::DATE = $thisweek_date
    );

SET lastweek_snapshot = (
    SELECT MIN(snapshot_datetime)
    FROM fabletics.gc_historical_customer_request_snapshot
    WHERE snapshot_datetime::DATE = $lastweek_date
    );

SET table_name = 'reporting_prod.sxf.gc_historical_customer_request_snapshot';

SET snapshot_detail = 'Snapshot ' || $lastweek_snapshot::VARCHAR || ' vs ' || $thisweek_snapshot::VARCHAR;

SET snapshot_datetime = CURRENT_TIMESTAMP();

CREATE OR REPLACE TEMPORARY TABLE _old AS
SELECT DATE_TRUNC('MONTH', payment_date) AS month_date,
    customer_segment,
    brand,
    vip_cohort,
    payment_date,
    billing_type,
    store_region,
    brand || ' ' || store_region AS brand_region,
    customer_segment || ' ' || store_region AS customer_segment_region,
    SUM(COALESCE(customer_count, 0))::NUMBER(38, 2) AS "Customer Count",
    SUM(COALESCE(cash_gross_revenue, 0))::NUMBER(38, 2) AS "Cash Gross Revenue",
    SUM(COALESCE(cash_net_rev, 0))::NUMBER(38, 2) AS "Cash Net Rev",
    SUM(COALESCE(cash_margin_pre_return, 0))::NUMBER(38, 2) AS "Cash Margin Pre Return",
    SUM(COALESCE(cash_gross_profit, 0))::NUMBER(38, 2) AS "Cash Gross Profit",
    SUM(COALESCE(product_order_cash_refund_amount, 0))::NUMBER(38, 2) AS "Product Order Cash Refund Amount",
    SUM(COALESCE(product_order_cash_chargeback_amount, 0))::NUMBER(38, 2) AS "Product Order Cash Chargeback Amount",
    SUM(COALESCE(billing_cash_refund_amount, 0))::NUMBER(38, 2) AS "Billing Cash Refund Amount",
    SUM(COALESCE(billing_cash_chargeback_amount, 0))::NUMBER(38, 2) AS "Billing Cash Chargeback Amount",
    SUM(COALESCE(product_order_landed_product_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Landed Product Cost Amount",
    SUM(COALESCE(product_order_exchange_product_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Exchange Product Cost Amount",
    SUM(COALESCE(product_order_reship_product_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Reship Product Cost Amount",
    SUM(COALESCE(product_order_shipping_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Shipping Cost Amount",
    SUM(COALESCE(product_order_reship_shipping_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Reship Shipping Cost Amount",
    SUM(COALESCE(product_order_exchange_shipping_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Exchange Shipping Cost Amount",
    SUM(COALESCE(product_order_return_shipping_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Return Shipping Cost Amount",
    SUM(COALESCE(product_order_shipping_supplies_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Shipping Supplies Cost Amount",
    SUM(COALESCE(product_order_exchange_shipping_supplies_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Exchange Shipping Supplies Cost Amount",
    SUM(COALESCE(product_order_reship_shipping_supplies_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Reship Shipping Supplies Cost Amount",
    SUM(COALESCE(product_order_misc_cogs_amount, 0))::NUMBER(38, 2) AS "Product Order Misc Cogs Amount",
    SUM(COALESCE(product_order_cost_product_returned_resaleable_amount, 0))::NUMBER(38, 2) AS "Product Order Cost Product Returned Resaleable Amount",
    SUM(COALESCE(product_order_variable_gms_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Variable GMS Cost Amount",
    SUM(COALESCE(billing_variable_gms_cost_amount, 0))::NUMBER(38, 2) AS "Billing Variable GMS Cost Amount",
    SUM(COALESCE(product_order_payment_processing_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Payment Processing Cost Amount",
    SUM(COALESCE(billing_payment_processing_cost_amount, 0))::NUMBER(38, 2) AS "Billing Payment Processing Cost Amount",
    SUM(COALESCE(product_order_variable_warehouse_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Variable Warehouse Cost Amount",
    SUM(COALESCE(selling_expenses, 0))::NUMBER(38, 2) AS "Selling Expenses",
    SUM(COALESCE(gfc_fixed, 0))::NUMBER(38, 2) AS "GFC Fixed",
    SUM(COALESCE(gfc_back_office, 0))::NUMBER(38, 2) AS "GFC Back Office",
    SUM(COALESCE(gms_fixed, 0))::NUMBER(38, 2) AS "GMS Fixed",
    SUM(COALESCE(gms_back_office, 0))::NUMBER(38, 2) AS "GMS Back Office",
    "Cash Gross Profit" - (
        "Product Order Variable GMS Cost Amount" + "Billing Variable GMS Cost Amount" + "Product Order Payment Processing Cost Amount"
        + "Billing Payment Processing Cost Amount" + "Product Order Variable Warehouse Cost Amount" + "Selling Expenses"
        + "GFC Fixed" + "GFC Back Office" + "GMS Fixed" + "GMS Back Office" ) AS "Cash Cont Profit",
    "Product Order Landed Product Cost Amount" + "Product Order Exchange Product Cost Amount"
        + "Product Order Reship Product Cost Amount" + "Product Order Shipping Cost Amount"
        + "Product Order Reship Shipping Cost Amount" + "Product Order Exchange Shipping Cost Amount"
        + "Product Order Return Shipping Cost Amount" + "Product Order Shipping Supplies Cost Amount"
        + "Product Order Exchange Shipping Supplies Cost Amount" + "Product Order Reship Shipping Supplies Cost Amount"
        + "Product Order Misc Cogs Amount" - "Product Order Cost Product Returned Resaleable Amount" AS "Total Cogs",
    "Product Order Variable GMS Cost Amount" + "Billing Variable GMS Cost Amount" + "Product Order Payment Processing Cost Amount"
        + "Billing Payment Processing Cost Amount" + "Product Order Variable Warehouse Cost Amount"
        + "Selling Expenses" + "GFC Fixed" + "GFC Back Office" + "GMS Fixed" + "GMS Back Office" AS "Total Expenses",
    "Product Order Cash Refund Amount" + "Product Order Cash Chargeback Amount"
        + "Billing Cash Refund Amount" + "Billing Cash Chargeback Amount" AS "Refunds And Chargebacks"
FROM fabletics.gc_historical_customer_request_snapshot
WHERE snapshot_datetime = $lastweek_snapshot
    AND payment_date < $lastweek_date
GROUP BY DATE_TRUNC('MONTH', payment_date),
    customer_segment,
    brand,
    vip_cohort,
    payment_date,
    billing_type,
    store_region,
    brand || ' ' || store_region,
    customer_segment || ' ' || store_region;

CREATE OR REPLACE TEMPORARY TABLE _new AS
SELECT DATE_TRUNC('MONTH', payment_date) AS month_date,
        customer_segment,
        brand,
        vip_cohort,
        payment_date,
        billing_type,
        store_region,
        brand || ' ' || store_region AS brand_region,
        customer_segment || ' ' || store_region AS customer_segment_region,
        SUM(COALESCE(customer_count, 0))::NUMBER(38, 2) AS "Customer Count",
        SUM(COALESCE(cash_gross_revenue, 0))::NUMBER(38, 2) AS "Cash Gross Revenue",
        SUM(COALESCE(cash_net_rev, 0))::NUMBER(38, 2) AS "Cash Net Rev",
        SUM(COALESCE(cash_margin_pre_return, 0))::NUMBER(38, 2) AS "Cash Margin Pre Return",
        SUM(COALESCE(cash_gross_profit, 0))::NUMBER(38, 2) AS "Cash Gross Profit",
        SUM(COALESCE(product_order_cash_refund_amount, 0))::NUMBER(38, 2) AS "Product Order Cash Refund Amount",
        SUM(COALESCE(product_order_cash_chargeback_amount, 0))::NUMBER(38, 2) AS "Product Order Cash Chargeback Amount",
        SUM(COALESCE(billing_cash_refund_amount, 0))::NUMBER(38, 2) AS "Billing Cash Refund Amount",
        SUM(COALESCE(billing_cash_chargeback_amount, 0))::NUMBER(38, 2) AS "Billing Cash Chargeback Amount",
        SUM(COALESCE(product_order_landed_product_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Landed Product Cost Amount",
        SUM(COALESCE(product_order_exchange_product_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Exchange Product Cost Amount",
        SUM(COALESCE(product_order_reship_product_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Reship Product Cost Amount",
        SUM(COALESCE(product_order_shipping_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Shipping Cost Amount",
        SUM(COALESCE(product_order_reship_shipping_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Reship Shipping Cost Amount",
        SUM(COALESCE(product_order_exchange_shipping_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Exchange Shipping Cost Amount",
        SUM(COALESCE(product_order_return_shipping_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Return Shipping Cost Amount",
        SUM(COALESCE(product_order_shipping_supplies_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Shipping Supplies Cost Amount",
        SUM(COALESCE(product_order_exchange_shipping_supplies_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Exchange Shipping Supplies Cost Amount",
        SUM(COALESCE(product_order_reship_shipping_supplies_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Reship Shipping Supplies Cost Amount",
        SUM(COALESCE(product_order_misc_cogs_amount, 0))::NUMBER(38, 2) AS "Product Order Misc Cogs Amount",
        SUM(COALESCE(product_order_cost_product_returned_resaleable_amount, 0))::NUMBER(38, 2) AS "Product Order Cost Product Returned Resaleable Amount",
        SUM(COALESCE(product_order_variable_gms_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Variable GMS Cost Amount",
        SUM(COALESCE(billing_variable_gms_cost_amount, 0))::NUMBER(38, 2) AS "Billing Variable GMS Cost Amount",
        SUM(COALESCE(product_order_payment_processing_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Payment Processing Cost Amount",
        SUM(COALESCE(billing_payment_processing_cost_amount, 0))::NUMBER(38, 2) AS "Billing Payment Processing Cost Amount",
        SUM(COALESCE(product_order_variable_warehouse_cost_amount, 0))::NUMBER(38, 2) AS "Product Order Variable Warehouse Cost Amount",
        SUM(COALESCE(selling_expenses, 0))::NUMBER(38, 2) AS "Selling Expenses",
        SUM(COALESCE(gfc_fixed, 0))::NUMBER(38, 2) AS "GFC Fixed",
        SUM(COALESCE(gfc_back_office, 0))::NUMBER(38, 2) AS "GFC Back Office",
        SUM(COALESCE(gms_fixed, 0))::NUMBER(38, 2) AS "GMS Fixed",
        SUM(COALESCE(gms_back_office, 0))::NUMBER(38, 2) AS "GMS Back Office",
    "Cash Gross Profit" - (
        "Product Order Variable GMS Cost Amount" + "Billing Variable GMS Cost Amount" + "Product Order Payment Processing Cost Amount"
        + "Billing Payment Processing Cost Amount" + "Product Order Variable Warehouse Cost Amount" + "Selling Expenses"
        + "GFC Fixed" + "GFC Back Office" + "GMS Fixed" + "GMS Back Office" ) AS "Cash Cont Profit",
    "Product Order Landed Product Cost Amount" + "Product Order Exchange Product Cost Amount"
        + "Product Order Reship Product Cost Amount" + "Product Order Shipping Cost Amount"
        + "Product Order Reship Shipping Cost Amount" + "Product Order Exchange Shipping Cost Amount"
        + "Product Order Return Shipping Cost Amount" + "Product Order Shipping Supplies Cost Amount"
        + "Product Order Exchange Shipping Supplies Cost Amount" + "Product Order Reship Shipping Supplies Cost Amount"
        + "Product Order Misc Cogs Amount" - "Product Order Cost Product Returned Resaleable Amount" AS "Total Cogs",
    "Product Order Variable GMS Cost Amount" + "Billing Variable GMS Cost Amount" + "Product Order Payment Processing Cost Amount"
        + "Billing Payment Processing Cost Amount" + "Product Order Variable Warehouse Cost Amount"
        + "Selling Expenses" + "GFC Fixed" + "GFC Back Office" + "GMS Fixed" + "GMS Back Office" AS "Total Expenses",
    "Product Order Cash Refund Amount" + "Product Order Cash Chargeback Amount"
        + "Billing Cash Refund Amount" + "Billing Cash Chargeback Amount" AS "Refunds And Chargebacks"
FROM fabletics.gc_historical_customer_request_snapshot
WHERE snapshot_datetime = $thisweek_snapshot
    AND payment_date < $lastweek_date
GROUP BY DATE_TRUNC('MONTH', payment_date),
    customer_segment,
    brand,
    vip_cohort,
    payment_date,
    billing_type,
    store_region,
    brand || ' ' || store_region,
    customer_segment || ' ' || store_region;

CREATE OR REPLACE TEMPORARY TABLE _source1 AS
SELECT payment_date AS Date,
    store_region,
    brand,
    customer_segment,
    vip_cohort,
    brand_region,
    customer_segment_region,
    metric,
    value
FROM _old
UNPIVOT (value FOR metric IN (
        "Customer Count",
        "Cash Gross Revenue",
        "Cash Net Rev",
        "Cash Margin Pre Return",
        "Cash Gross Profit",
        "Product Order Cash Refund Amount",
        "Product Order Cash Chargeback Amount",
        "Billing Cash Refund Amount",
        "Billing Cash Chargeback Amount",
        "Product Order Landed Product Cost Amount",
        "Product Order Exchange Product Cost Amount",
        "Product Order Reship Product Cost Amount",
        "Product Order Shipping Cost Amount",
        "Product Order Reship Shipping Cost Amount",
        "Product Order Exchange Shipping Cost Amount",
        "Product Order Return Shipping Cost Amount",
        "Product Order Shipping Supplies Cost Amount",
        "Product Order Exchange Shipping Supplies Cost Amount",
        "Product Order Reship Shipping Supplies Cost Amount",
        "Product Order Misc Cogs Amount",
        "Product Order Cost Product Returned Resaleable Amount",
        "Product Order Variable GMS Cost Amount",
        "Billing Variable GMS Cost Amount",
        "Product Order Payment Processing Cost Amount",
        "Billing Payment Processing Cost Amount",
        "Product Order Variable Warehouse Cost Amount",
        "Selling Expenses",
        "GFC Fixed",
        "GFC Back Office",
        "GMS Fixed",
        "GMS Back Office",
        "Cash Cont Profit",
        "Total Cogs",
        "Total Expenses",
        "Refunds And Chargebacks"
    )
);

CREATE OR REPLACE TEMPORARY TABLE _source2 AS
SELECT payment_date AS Date,
    store_region,
    brand,
    customer_segment,
    vip_cohort,
    brand_region,
    customer_segment_region,
    metric,
    value
FROM _new
UNPIVOT (value FOR metric IN (
        "Customer Count",
        "Cash Gross Revenue",
        "Cash Net Rev",
        "Cash Margin Pre Return",
        "Cash Gross Profit",
        "Product Order Cash Refund Amount",
        "Product Order Cash Chargeback Amount",
        "Billing Cash Refund Amount",
        "Billing Cash Chargeback Amount",
        "Product Order Landed Product Cost Amount",
        "Product Order Exchange Product Cost Amount",
        "Product Order Reship Product Cost Amount",
        "Product Order Shipping Cost Amount",
        "Product Order Reship Shipping Cost Amount",
        "Product Order Exchange Shipping Cost Amount",
        "Product Order Return Shipping Cost Amount",
        "Product Order Shipping Supplies Cost Amount",
        "Product Order Exchange Shipping Supplies Cost Amount",
        "Product Order Reship Shipping Supplies Cost Amount",
        "Product Order Misc Cogs Amount",
        "Product Order Cost Product Returned Resaleable Amount",
        "Product Order Variable GMS Cost Amount",
        "Billing Variable GMS Cost Amount",
        "Product Order Payment Processing Cost Amount",
        "Billing Payment Processing Cost Amount",
        "Product Order Variable Warehouse Cost Amount",
        "Selling Expenses",
        "GFC Fixed",
        "GFC Back Office",
        "GMS Fixed",
        "GMS Back Office",
        "Cash Cont Profit",
        "Total Cogs",
        "Total Expenses",
        "Refunds And Chargebacks"
    )
);

DELETE FROM fabletics.gc_historical_customer_request_validation_data
WHERE snapshot_detail = $snapshot_detail;

INSERT INTO fabletics.gc_historical_customer_request_validation_data (
    date,
    store_region,
    brand,
    customer_segment,
    vip_cohort,
    brand_region,
    customer_segment_region,
    snapshot_datetime,
    snapshot_detail,
    table_name,
    source1_snapshot,
    source2_snapshot,
    validation_approver,
    validation_name,
    validation_status,
    metric_name,
    source1_value,
    source2_value,
    difference
)
SELECT
    COALESCE(old.date, new.date) AS date,
    COALESCE(old.store_region, new.store_region) AS store_region,
    COALESCE(old.brand, new.brand) AS brand,
    COALESCE(old.customer_segment, new.customer_segment) AS customer_segment,
    COALESCE(old.vip_cohort, new.vip_cohort) AS vip_cohort,
    COALESCE(old.brand_region, new.brand_region) AS brand_region,
    COALESCE(old.customer_segment_region, new.customer_segment_region) AS customer_segment_region,
    $snapshot_datetime AS snapshot_datetime,
    $snapshot_detail AS snapshot_detail,
    $table_name AS table_name,
    $lastweek_snapshot::VARCHAR AS source1_snapshot,
    $thisweek_snapshot::VARCHAR AS source2_snapshot,
    '' AS validation_approver,
    '' AS validation_name,
    '' AS validation_status,
    COALESCE(old.metric, new.metric) AS metric_name,
    COALESCE(old.value, 0) AS source1_value,
    COALESCE(new.value, 0) AS source2_value,
    COALESCE(old.value, 0) - COALESCE(new.value, 0) AS difference
FROM _source1 old
FULL JOIN _source2 new ON old.date = new.date
    AND old.store_region = new.store_region
    AND old.brand = new.brand
    AND old.customer_segment = new.customer_segment
    AND old.vip_cohort = new.vip_cohort
    AND old.brand_region = new.brand_region
    AND old.customer_segment_region = new.customer_segment_region
    AND old.metric = new.metric;
