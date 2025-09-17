SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

-- Bundle Data
CREATE OR REPLACE TEMPORARY TABLE _bundle_name AS
SELECT fol.order_line_id,
       dp.product_name  AS bundle_name,
       dp.product_alias AS bundle_alias
FROM data_model.fact_order_line fol
JOIN lake_consolidated.ultra_merchant.order_line ol ON fol.bundle_order_line_id = ol.order_line_id
JOIN data_model.dim_product dp ON dp.product_id = ol.product_id;

-- Product Brand
CREATE OR REPLACE TEMPORARY TABLE _product_brand AS
SELECT DISTINCT i.item_number,
                CASE WHEN UPPER(label) = 'FABLETICS' AND UPPER(department) = 'SHAPEWEAR' THEN 'YITTY' ELSE UPPER(label) END AS product_brand
FROM lake_view.ultra_warehouse.item i
         JOIN lake_view.ultra_warehouse.company c ON i.company_id = c.company_id ;

CREATE OR REPLACE TEMPORARY TABLE _sales AS
SELECT CAST(fol.order_completion_local_datetime AS DATE) AS shipped_date,
    CAST(fol.order_local_datetime AS DATE) AS placed_date,
    ds.store_id,
    UPPER(ds.store_name) AS store_name,
    UPPER(ds.store_full_name) AS store_full_name,
    UPPER(ds.store_brand) AS store_brand,
    ds.store_type AS store_type,
    ds.store_region AS region,
    ds.store_country AS country,
    dc.finance_specialty_store,
    IFF(dip.sku = 'Unknown',dp.sku,dip.sku) AS sku,
    pb.product_brand,
    IFF(dip.product_sku = 'Unknown',dp.product_sku,dip.product_sku) AS product_sku,
    IFF(dip.base_sku = 'Unknown',dp.base_sku,dip.base_sku) AS base_sku,
    CASE WHEN ds.store_type = 'Retail' THEN 1 ELSE 0 END AS is_retail,
    (CASE WHEN dpt.product_type_name = 'Bundle Component' THEN 1 ELSE 0 END) AS is_sold_as_part_of_bundle,
    bn.bundle_name,
    bn.bundle_alias,
    domc.membership_order_type_l2,
    dos.order_status,
    dops.order_processing_status,
    SUM(COALESCE(fol.item_quantity, 0)) AS unit_count,
    SUM(COALESCE(fol.product_subtotal_local_amount, 0) * COALESCE(fol.reporting_usd_conversion_rate, 1)) AS product_order_subtotal_shipped,
    SUM(COALESCE(fol.product_discount_local_amount, 0) * COALESCE(fol.reporting_usd_conversion_rate, 1)) AS product_order_discount_shipped,
    SUM(COALESCE(fol.product_gross_revenue_excl_shipping_local_amount, 0) * COALESCE(fol.reporting_usd_conversion_rate, 1)) AS product_order_gross_revenue_excl_shipping_shipped,
    SUM(COALESCE(fol.product_margin_pre_return_excl_shipping_local_amount, 0) * COALESCE(fol.reporting_usd_conversion_rate, 1)) AS product_order_margin_pre_return_excl_shipping_shipped,
    SUM(COALESCE(fol.estimated_landed_cost_local_amount, 0) * COALESCE(fol.reporting_usd_conversion_rate, 1)) AS product_cost_shipped,
    SUM(COALESCE(fol.reporting_landed_cost_local_amount, 0) * COALESCE(fol.reporting_usd_conversion_rate, 1)) AS product_cost_shipped_c,
    CASE
        WHEN domc.membership_order_type_l2 ilike '%vip%' and UPPER(ds.store_brand) = 'SAVAGE X' THEN SUM((COALESCE(fol.item_quantity, 0) * COALESCE(dpph.vip_unit_price, 0) * COALESCE(fol.reporting_usd_conversion_rate, 1)) / (1 + COALESCE(fo.effective_vat_rate,0)))
        ELSE SUM((COALESCE(fol.item_quantity, 0) * COALESCE(dpph.retail_unit_price, 0) * COALESCE(fol.reporting_usd_conversion_rate, 1)) / (1 + COALESCE(fo.effective_vat_rate,0)))
    END AS non_reduced_vip_price_shipped,
    SUM(COALESCE(fol.product_subtotal_local_amount, 0) * COALESCE(fol.order_date_usd_conversion_rate, 1)) AS product_order_subtotal_placed,
    SUM(COALESCE(fol.product_discount_local_amount, 0) * COALESCE(fol.order_date_usd_conversion_rate, 1)) AS product_order_discount_placed,
    SUM(COALESCE(fol.product_gross_revenue_excl_shipping_local_amount, 0) * COALESCE(fol.order_date_usd_conversion_rate, 1)) AS product_order_gross_revenue_excl_shipping_placed,
    SUM(COALESCE(fol.product_margin_pre_return_excl_shipping_local_amount, 0) * COALESCE(fol.order_date_usd_conversion_rate, 1)) AS product_order_margin_pre_return_excl_shipping_placed,
    SUM(COALESCE(fol.estimated_landed_cost_local_amount, 0) * COALESCE(fol.order_date_usd_conversion_rate, 1)) AS product_cost_placed,
    SUM(COALESCE(fol.reporting_landed_cost_local_amount, 0) * COALESCE(fol.order_date_usd_conversion_rate, 1)) AS product_cost_placed_c,
    CASE
        WHEN domc.membership_order_type_l2 ilike '%vip%' and UPPER(ds.store_brand) = 'SAVAGE X' THEN SUM((COALESCE(fol.item_quantity, 0) * COALESCE(dpph.vip_unit_price, 0) * COALESCE(fol.order_date_usd_conversion_rate, 1)) / (1 + COALESCE(fo.effective_vat_rate,0)))
        ELSE SUM((COALESCE(fol.item_quantity, 0) * COALESCE(dpph.retail_unit_price, 0) * COALESCE(fol.order_date_usd_conversion_rate, 1)) / (1 + COALESCE(fo.effective_vat_rate,0)))
    END AS non_reduced_vip_price_placed
FROM data_model.fact_order_line fol
JOIN data_model.dim_store ds ON fol.store_id = ds.store_id
JOIN data_model.dim_product dp ON fol.product_id = dp.product_id
JOIN data_model.dim_item_price dip ON fol.item_price_key = dip.item_price_key
JOIN _product_brand pb ON IFF(dip.sku = 'Unknown',dp.sku,dip.sku) = pb.item_number
LEFT JOIN _bundle_name bn ON fol.order_line_id = bn.order_line_id
JOIN data_model.fact_order fo ON fol.order_id = fo.order_id
JOIN data_model.dim_order_processing_status dops ON fo.order_processing_status_key = dops.order_processing_status_key
JOIN data_model.dim_product_type dpt ON fol.product_type_key = dpt.product_type_key
    AND dpt.is_free = 'FALSE'
    AND dpt.product_type_name NOT IN (
        'Offer Gift',
        'Gift Certificate',
        'Membership Gift',
        'Membership Reward Points Item'
    )
JOIN data_model.dim_order_line_status dols ON fol.order_line_status_key = dols.order_line_status_key
JOIN data_model.dim_order_status dos ON fol.order_status_key = dos.order_status_key
    AND dos.order_status IN ('Success', 'Pending')
JOIN data_model.dim_order_membership_classification domc ON fol.order_membership_classification_key = domc.order_membership_classification_key
JOIN data_model.dim_order_sales_channel dosc ON fol.order_sales_channel_key = dosc.order_sales_channel_key
    AND dosc.order_classification_l1 = 'Product Order'
    AND dosc.is_ps_order = 'FALSE'
JOIN data_model.dim_product_price_history dpph ON dpph.product_price_history_key = fol.product_price_history_key
JOIN data_model.dim_customer dc ON dc.customer_id = fo.customer_id
WHERE (
        fol.order_completion_local_datetime >= '2010-01-01'
        OR fol.order_local_datetime >= '2010-01-01'
    )
    AND dols.order_line_status != 'Cancelled'
    AND NOT (ds.store_brand = 'Fabletics' AND dp.product_category ILIKE 'Savage')
    AND NOT (ds.store_brand = 'Fabletics' AND dp.product_category ILIKE 'Savage' AND IFF(dip.sku = 'Unknown',dp.sku,dip.sku) = 'Unknown')
GROUP BY CAST(fol.order_completion_local_datetime AS DATE),
    CAST(fol.order_local_datetime AS DATE),
    ds.store_id,
    UPPER(ds.store_name),
    UPPER(ds.store_full_name),
    UPPER(ds.store_brand),
    ds.store_type,
    ds.store_region,
    ds.store_country,
    dc.finance_specialty_store,
    IFF(dip.sku = 'Unknown',dp.sku,dip.sku),
    pb.product_brand,
    IFF(dip.product_sku = 'Unknown',dp.product_sku,dip.product_sku),
    IFF(dip.base_sku = 'Unknown',dp.base_sku,dip.base_sku),
    CASE WHEN ds.store_type = 'Retail' THEN 1 ELSE 0 END,
    (CASE WHEN dpt.product_type_name = 'Bundle Component' THEN 1 ELSE 0 END),
    bn.bundle_name,
    bn.bundle_alias,
    domc.membership_order_type_l2,
    dos.order_status,
    dops.order_processing_status;

TRUNCATE TABLE analytics_base.sales_returns_agg;

INSERT INTO analytics_base.sales_returns_agg (
    date,
    date_type,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    region,
    country,
    finance_specialty_store,
    sku,
    product_brand,
    product_sku,
    base_sku,
    is_retail,
    is_sold_as_part_of_bundle,
    bundle_name,
    bundle_alias,
    membership_order_type_l2,
    unit_count,
    product_order_subtotal,
    product_order_discount,
    product_order_gross_revenue_excl_shipping,
    product_order_margin_pre_return_excl_shipping,
    product_cost,
    product_cost_c,
    non_reduced_vip_price,
    return_unit_count,
    returned_product_gross_rev_excluding_shipping,
    meta_create_datetime,
    meta_update_datetime
)
--Placed Date Sales
SELECT placed_date AS date,
    'Placed' AS date_type,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    region,
    country,
    finance_specialty_store,
    sku,
    product_brand,
    product_sku,
    base_sku,
    is_retail,
    is_sold_as_part_of_bundle,
    bundle_name,
    bundle_alias,
    membership_order_type_l2,
    SUM(unit_count) AS unit_count,
    SUM(product_order_subtotal_placed) AS product_order_subtotal,
    SUM(product_order_discount_placed) AS product_order_discount,
    SUM(product_order_gross_revenue_excl_shipping_placed) AS product_order_gross_revenue_excl_shipping,
    SUM(product_order_margin_pre_return_excl_shipping_placed) AS product_order_margin_pre_return_excl_shipping,
    SUM(product_cost_placed) AS product_cost,
    SUM(product_cost_placed_c) AS product_cost_c,
    SUM(non_reduced_vip_price_placed) AS non_reduced_vip_price,
    0 AS return_unit_count,
    0 AS returned_product_gross_rev_excluding_shipping,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _sales
WHERE order_status IN ('Success', 'Pending')
    AND placed_date >= '2010-01-01'
GROUP BY placed_date,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    region,
    country,
    finance_specialty_store,
    sku,
    product_brand,
    product_sku,
    base_sku,
    is_retail,
    is_sold_as_part_of_bundle,
    bundle_name,
    bundle_alias,
    membership_order_type_l2

UNION ALL

--Shipped Date Sales
SELECT shipped_date AS date,
    'Shipped' AS date_type,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    region,
    country,
    finance_specialty_store,
    sku,
    product_brand,
    product_sku,
    base_sku,
    is_retail,
    is_sold_as_part_of_bundle,
    bundle_name,
    bundle_alias,
    membership_order_type_l2,
    SUM(unit_count) AS unit_count,
    SUM(product_order_subtotal_shipped) AS product_order_subtotal,
    SUM(product_order_discount_shipped) AS product_order_discount,
    SUM(product_order_gross_revenue_excl_shipping_shipped) AS product_order_gross_revenue_excl_shipping,
    SUM(product_order_margin_pre_return_excl_shipping_shipped) AS product_order_margin_pre_return_excl_shipping,
    SUM(product_cost_shipped) AS product_cost,
    SUM(product_cost_shipped_c) AS product_cost_c,
    SUM(non_reduced_vip_price_shipped) AS non_reduced_vip_price,
    0 AS return_unit_count,
    0 AS returned_product_gross_rev_excluding_shipping,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _sales
WHERE order_status IN ('Success', 'Pending')
    AND shipped_date >= '2010-01-01'
GROUP BY shipped_date,
    store_id,
    store_name,
    store_full_name,
    store_brand,
    store_type,
    region,
    country,
    finance_specialty_store,
    sku,
    product_brand,
    product_sku,
    base_sku,
    is_retail,
    is_sold_as_part_of_bundle,
    bundle_name,
    bundle_alias,
    membership_order_type_l2

UNION ALL

-- Returns Data
SELECT CAST(frl.return_receipt_local_datetime AS DATE) AS date,
    'Returns' AS date_type,
    ds.store_id,
    UPPER(ds.store_name) AS store_name,
    UPPER(ds.store_full_name) AS store_full_name,
    UPPER(ds.store_brand) AS store_brand,
    ds.store_type AS store_type,
    ds.store_region AS region,
    ds.store_country AS country,
    dc.finance_specialty_store,
    IFF(dip.sku = 'Unknown',dp.sku,dip.sku) AS sku,
    pb.product_brand,
    IFF(dip.product_sku = 'Unknown',dp.product_sku,dip.product_sku) AS product_sku,
    IFF(dip.base_sku = 'Unknown',dp.base_sku,dip.base_sku) AS base_sku,
    CASE WHEN ds.store_type = 'Retail' THEN 1 ELSE 0 END AS is_retail,
    CASE WHEN dpt.product_type_name = 'Bundle Component' THEN 1 ELSE 0 END AS is_sold_as_part_of_bundle,
    bn.bundle_name,
    bn.bundle_alias,
    domc.membership_order_type_l2,
    0 AS unit_count,
    0 AS product_order_subtotal,
    0 AS product_order_discount,
    0 AS product_order_gross_revenue_excl_shipping,
    0 AS product_order_margin_pre_return_excl_shipping,
    0 AS product_cost,
    0 AS product_cost_c,
    0 AS non_reduced_vip_price,
    SUM(COALESCE(frl.return_item_quantity, 0)) AS return_unit_count,
    SUM((COALESCE(frl.return_subtotal_local_amount, 0) - COALESCE(frl.return_discount_local_amount, 0)) * COALESCE(frl.return_receipt_date_usd_conversion_rate, 1)) AS returned_product_gross_rev_excluding_shipping,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM data_model.fact_return_line frl
JOIN data_model.dim_store ds ON frl.store_id = ds.store_id
JOIN data_model.dim_product dp ON frl.product_id = dp.product_id
JOIN data_model.dim_item_price dip ON frl.item_price_key = dip.item_price_key
JOIN _product_brand pb ON IFF(dip.sku = 'Unknown',dp.sku,dip.sku) = pb.item_number
JOIN data_model.dim_return_status drs ON frl.return_status_key = drs.return_status_key
JOIN data_model.dim_return_condition drc ON frl.return_condition_key = drc.return_condition_key
JOIN data_model.fact_order_line fol ON frl.order_line_id = fol.order_line_id
    AND frl.store_id = fol.store_id
JOIN data_model.dim_product_type dpt ON fol.product_type_key = dpt.product_type_key
    AND dpt.is_free = 'FALSE'
    AND dpt.product_type_name NOT IN (
        'Offer Gift',
        'Gift Certificate',
        'Membership Gift',
        'Membership Reward Points Item'
    )
JOIN data_model.dim_order_membership_classification domc ON fol.order_membership_classification_key = domc.order_membership_classification_key
JOIN data_model.fact_order fo ON fol.order_id = fo.order_id
JOIN data_model.dim_order_sales_channel dosc ON fo.order_sales_channel_key = dosc.order_sales_channel_key
    AND dosc.order_classification_l1 IN ('Product Order', 'Exchange', 'Reship')
JOIN data_model.dim_customer dc ON dc.customer_id = fo.customer_id
LEFT JOIN _bundle_name bn ON fol.order_line_id = bn.order_line_id
WHERE frl.return_receipt_local_datetime >= '2010-01-01'
    AND drs.return_status = 'Resolved'
--    AND drc.return_condition != 'Damaged'
    AND NOT (ds.store_brand = 'Fabletics' AND dp.product_category ILIKE 'Savage')
    AND NOT (ds.store_brand = 'Fabletics' AND dp.product_category ILIKE 'Savage' AND IFF(dip.sku = 'Unknown',dp.sku,dip.sku) = 'Unknown')
GROUP BY CAST(frl.return_receipt_local_datetime AS DATE),
    ds.store_id,
    UPPER(ds.store_name),
    UPPER(ds.store_full_name),
    UPPER(ds.store_brand),
    ds.store_type,
    ds.store_region,
    ds.store_country,
    dc.finance_specialty_store,
    IFF(dip.sku = 'Unknown',dp.sku,dip.sku),
    pb.product_brand,
    IFF(dip.product_sku = 'Unknown',dp.product_sku,dip.product_sku),
    IFF(dip.base_sku = 'Unknown',dp.base_sku,dip.base_sku),
    CASE WHEN ds.store_type = 'Retail' THEN 1 ELSE 0 END,
    CASE WHEN dpt.product_type_name = 'Bundle Component' THEN 1 ELSE 0 END,
    bn.bundle_name,
    bn.bundle_alias,
    domc.membership_order_type_l2;
