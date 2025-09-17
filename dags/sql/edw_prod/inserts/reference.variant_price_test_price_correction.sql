
/*
When we launched VPT in 2022, normal_unit_price for orders paid with token were showing the
control price when they experienced a price change.
That field was intended to show the variant price since that is what the customer would have paid, if they did not use token.
Dane deployed this fix on 2/17 10:30am pst but was a go-forward fix.
This reference table is to map these impacted orders to the correct price
*/

SET current_timestamp = current_timestamp;

CREATE OR REPLACE TEMP TABLE _order_line__price_change AS
with _sorting_hat as (
SELECT DISTINCT c.customer_id, c.meta_original_customer_id, sh.variant_version
FROM lake_view.experiments.sorting_hat AS sh
JOIN lake_consolidated.ultra_merchant.customer AS c
    ON sh.customer_id = c.meta_original_customer_id
WHERE test_metadata_id = 49
AND variant_version > 0)

SELECT
    o.order_id,
    o.meta_original_order_id,
    ol.order_line_id,
    ol.meta_original_order_line_id,
    ol.product_id,
    ol.product_type_id,
    ol.group_key,
    ol.purchase_unit_price,
    ol.normal_unit_price,
    vpo.unit_price AS variant_price,
    cpo.unit_price AS control_price,
    po.unit_price
FROM lake_consolidated.ultra_merchant."ORDER" AS o
JOIN _sorting_hat AS sh ON sh.customer_id = o.customer_id
JOIN lake_consolidated.ultra_merchant.order_line AS ol ON ol.order_id = o.order_id
LEFT JOIN lake_consolidated.ultra_merchant.pricing_option AS po ON po.pricing_option_id = ol.pricing_option_id
JOIN lake_consolidated.ultra_merchant.variant_pricing AS vp
    ON vp.product_id = ol.product_id
    AND vp.variant_number = sh.variant_version
    AND vp.test_metadata_id = 49
LEFT JOIN lake_consolidated.ultra_merchant.pricing_option AS vpo ON vpo.pricing_id = vp.pricing_id
LEFT JOIN lake_consolidated.ultra_merchant.pricing_option AS cpo ON cpo.pricing_id = vp.control_pricing_id
WHERE
    o.datetime_added >= '2022-02-01 17:00:00' -- get time on when echo fix was deployed around 2/1 5pm pst.  Test groups were seeing control prices before fix
    AND o.datetime_added < '2022-02-17 10:30:00' -- date on when normal_price_unit fix was deployed.  2/17 10:30am pst
    AND cpo.unit_price = ol.normal_unit_price
    AND po.unit_price <> ol.normal_unit_price;

CREATE OR REPLACE TEMP TABLE _order_line__bundle_component_price_change AS
SELECT
    ol.order_id,
    olpc.meta_original_order_id,
    ol.order_line_id,
    ol.meta_original_order_line_id,
    ROUND(IFNULL(ol.purchase_unit_price/NULLIF(olpc.purchase_unit_price,0),0) * olpc.variant_price,2) AS variant_price,
    ol.normal_unit_price AS control_price,
    ol.product_type_id,
    olpc.variant_price as variant_outfit_price,
    olpc.control_price as control_outfit_price
FROM _order_line__price_change  AS olpc
JOIN lake_consolidated.ultra_merchant.order_line AS ol
    ON ol.order_id = olpc.order_id
    AND ol.product_type_id = 15
    AND ol.group_key = olpc.group_key
WHERE olpc.product_type_id = 14; -- bundle

INSERT INTO reference.variant_price_test_price_correction
(
    order_id,
    meta_original_order_id,
    order_line_id,
    meta_original_order_line_id,
    product_type_id,
    variant_price,
    control_price,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    order_id,
    meta_original_order_id,
    order_line_id,
    meta_original_order_line_id,
    product_type_id,
    variant_price,
    control_price,
    $current_timestamp AS meta_create_datetime,
    $current_timestamp AS meta_update_datetime
FROM _order_line__price_change;

INSERT INTO reference.variant_price_test_price_correction
(
    order_id,
    meta_original_order_id,
    order_line_id,
    meta_original_order_line_id,
    product_type_id,
    variant_price,
    control_price,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    order_id,
    meta_original_order_id,
    order_line_id,
    meta_original_order_line_id,
    product_type_id,
    variant_price,
    control_price,
    $current_timestamp AS meta_create_datetime,
    $current_timestamp AS meta_update_datetime
FROM _order_line__bundle_component_price_change
WHERE order_line_id NOT IN (SELECT order_line_id FROM _order_line__price_change);
