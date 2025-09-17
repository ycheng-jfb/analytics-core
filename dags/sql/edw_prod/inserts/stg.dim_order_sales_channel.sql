-- DO NOT TRUNCATE - Doing so will most likely cause new keys to be assigned requiring a refresh
--                      on all dependent downstream processes
--TRUNCATE TABLE stg.dim_order_sales_channel;

-- Add the new column (Do not forget to also add it to the table create script)
-- ALTER TABLE stg.dim_order_sales_channel ADD COLUMN is_discreet_packaging BOOLEAN;

-- This table should hold the complete set of data (including the 'unknown' row)
CREATE OR REPLACE TEMP TABLE _dim_order_sales_channel__data AS
SELECT
    -1::INT AS order_sales_channel_key,
    'Unknown'::VARCHAR(50) AS order_sales_channel_l1,
    'Unknown'::VARCHAR(50) AS order_sales_channel_l2,
    'Unknown'::VARCHAR(50) AS order_classification_l1,
    'Unknown'::VARCHAR(50) AS order_classification_l2,
    FALSE::BOOLEAN AS is_border_free_order,
    FALSE::BOOLEAN AS is_ps_order,
    FALSE::BOOLEAN AS is_retail_ship_only_order,
    FALSE::BOOLEAN AS is_test_order,
    FALSE::BOOLEAN AS is_preorder,
    FALSE::BOOLEAN AS is_product_seeding_order,
    FALSE::BOOLEAN AS is_bops_order,
    FALSE::BOOLEAN AS is_custom_order,
    FALSE::BOOLEAN AS is_discreet_packaging,
    FALSE::BOOLEAN AS is_bill_me_now_online,
    FALSE::BOOLEAN AS is_bill_me_now_gms,
    FALSE::BOOLEAN AS is_membership_gift,
    FALSE::BOOLEAN AS is_dropship,
    FALSE::BOOLEAN AS is_warehouse_outlet_order,
    FALSE::BOOLEAN AS is_third_party,
    '1900-01-01'::TIMESTAMP_LTZ AS effective_start_datetime,
    '9999-12-31'::TIMESTAMP_LTZ AS effective_end_datetime,
    TRUE::BOOLEAN AS is_current,
    CURRENT_TIMESTAMP::TIMESTAMP_LTZ AS meta_create_datetime,
    CURRENT_TIMESTAMP::TIMESTAMP_LTZ AS meta_update_datetime
UNION ALL
SELECT
    ROW_NUMBER() OVER (ORDER BY
        order_sales_channel_l1,
        order_sales_channel_l2,
        order_classification_l1,
        order_classification_l2,
        is_border_free_order,
        is_ps_order,
        is_retail_ship_only_order,
        is_test_order,
        is_preorder,
        is_product_seeding_order,
        is_bops_order,
        is_custom_order,
        is_discreet_packaging,
        is_bill_me_now_online,
        is_bill_me_now_gms,
        is_membership_gift,
        is_dropship,
        is_warehouse_outlet_order,
        is_third_party
        ) AS order_sales_channel_key,
    order_sales_channel_l1,
    order_sales_channel_l2,
    order_classification_l1,
    order_classification_l2,
    is_border_free_order,
    is_ps_order,
    is_retail_ship_only_order,
    is_test_order,
    is_preorder,
    is_product_seeding_order,
    is_bops_order,
    is_custom_order,
    is_discreet_packaging,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_membership_gift,
    is_dropship,
    is_warehouse_outlet_order,
    is_third_party,
    '1900-01-01' AS effective_start_datetime,
    '9999-12-31' AS effective_end_datetime,
    TRUE AS is_current,
    CURRENT_TIMESTAMP AS meta_create_datetime,
    CURRENT_TIMESTAMP AS meta_update_datetime
FROM (
    SELECT
        'Retail Order' AS order_sales_channel_l1,
        'Retail Order' AS order_sales_channel_l2,
        'Product Order' AS order_classification_l1,
        'Product Order' AS order_classification_l2
    UNION ALL
    SELECT 'Retail Order', 'Retail Order', 'Reship', 'Reship'
    UNION ALL
    SELECT 'Retail Order', 'Retail Order', 'Exchange', 'Exchange'
    UNION ALL
    SELECT 'Online Order', 'Mobile App Order', 'Product Order', 'Product Order'
    UNION ALL
    SELECT 'Online Order', 'Mobile App Order', 'Reship', 'Reship'
    UNION ALL
    SELECT 'Online Order', 'Mobile App Order', 'Exchange', 'Exchange'
    UNION ALL
    SELECT 'Online Order', 'Web Order', 'Product Order', 'Product Order'
    UNION ALL
    SELECT 'Online Order', 'Web Order', 'Reship', 'Reship'
    UNION ALL
    SELECT 'Online Order', 'Web Order', 'Exchange', 'Exchange'
    UNION ALL
    SELECT 'Billing Order', 'Billing Order', 'Billing Order', 'Credit Billing'
    UNION ALL
    SELECT 'Billing Order', 'Billing Order', 'Billing Order', 'Membership Fee'
    UNION ALL
    SELECT 'Billing Order', 'Billing Order', 'Billing Order', 'Legacy Credit'
    UNION ALL
    SELECT 'Billing Order', 'Billing Order', 'Billing Order', 'Token Billing'
    UNION ALL
    SELECT 'Billing Order', 'Billing Order', 'Billing Order', 'Gift Certificate'
    ) AS order_channel_classification
    CROSS JOIN (SELECT FALSE AS is_border_free_order UNION SELECT TRUE) AS is_border_free_order
    CROSS JOIN (SELECT FALSE AS is_ps_order UNION SELECT TRUE) AS is_ps_order
    CROSS JOIN (SELECT FALSE AS is_retail_ship_only_order UNION SELECT TRUE) AS is_retail_ship_only_order
    CROSS JOIN (SELECT FALSE AS is_test_order UNION SELECT TRUE) AS is_test_order
    CROSS JOIN (SELECT FALSE AS is_preorder UNION SELECT TRUE) AS is_preorder
    CROSS JOIN (SELECT FALSE AS is_product_seeding_order UNION SELECT TRUE) AS is_product_seeding_order
    CROSS JOIN (SELECT FALSE AS is_bops_order UNION SELECT TRUE) AS is_bops_order
    CROSS JOIN (SELECT FALSE AS is_custom_order UNION SELECT TRUE) AS is_custom_order
    CROSS JOIN (SELECT FALSE AS is_discreet_packaging UNION SELECT TRUE) AS is_discreet_packaging
    CROSS JOIN (SELECT FALSE AS is_bill_me_now_online UNION SELECT TRUE) AS is_bill_me_now_online
    CROSS JOIN (SELECT FALSE AS is_bill_me_now_gms UNION SELECT TRUE) AS is_bill_me_now_gms
    CROSS JOIN (SELECT FALSE AS is_membership_gift UNION SELECT TRUE) AS is_membership_gift
    CROSS JOIN (SELECT FALSE AS is_dropship UNION SELECT TRUE) AS is_dropship
    CROSS JOIN (SELECT FALSE AS is_warehouse_outlet_order UNION SELECT TRUE) AS is_warehouse_outlet_order
    CROSS JOIN (SELECT FALSE AS is_third_party UNION SELECT TRUE) AS is_third_party;
-- SELECT COUNT(1) FROM _dim_order_sales_channel__data;

-- Insert the 'unknown' record if it does not already exist
INSERT INTO stg.dim_order_sales_channel (
    order_sales_channel_key,
    order_sales_channel_l1,
    order_sales_channel_l2,
    order_classification_l1,
    order_classification_l2,
    is_border_free_order,
    is_ps_order,
    is_retail_ship_only_order,
    is_test_order,
    is_preorder,
    is_product_seeding_order,
    is_bops_order,
    is_custom_order,
    is_discreet_packaging,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_membership_gift,
    is_dropship,
    is_warehouse_outlet_order,
    is_third_party,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    order_sales_channel_key,
    order_sales_channel_l1,
    order_sales_channel_l2,
    order_classification_l1,
    order_classification_l2,
    is_border_free_order,
    is_ps_order,
    is_retail_ship_only_order,
    is_test_order,
    is_preorder,
    is_product_seeding_order,
    is_bops_order,
    is_custom_order,
    is_discreet_packaging,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_membership_gift,
    is_dropship,
    is_warehouse_outlet_order,
    is_third_party,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
FROM _dim_order_sales_channel__data AS d
WHERE d.order_sales_channel_key = -1
    AND NOT EXISTS (
        SELECT TRUE AS is_exists
        FROM stg.dim_order_sales_channel AS src
        WHERE src.order_sales_channel_key = -1
        );

-- Update existing table populating new added fields having NULL values to default values defined by the 'unknown' row
UPDATE stg.dim_order_sales_channel AS src
SET
    src.order_sales_channel_l1 = COALESCE(src.order_sales_channel_l1, def.order_sales_channel_l1),
    src.order_sales_channel_l2 = COALESCE(src.order_sales_channel_l2, def.order_sales_channel_l2),
    src.order_classification_l1 = COALESCE(src.order_classification_l1, def.order_classification_l1),
    src.order_classification_l2 = COALESCE(src.order_classification_l2, def.order_classification_l2),
    src.is_border_free_order = COALESCE(src.is_border_free_order, def.is_border_free_order),
    src.is_ps_order = COALESCE(src.is_ps_order, def.is_ps_order),
    src.is_retail_ship_only_order = COALESCE(src.is_retail_ship_only_order, def.is_retail_ship_only_order),
    src.is_test_order = COALESCE(src.is_test_order, def.is_test_order),
    src.is_preorder = COALESCE(src.is_preorder, def.is_preorder),
    src.is_product_seeding_order = COALESCE(src.is_product_seeding_order, def.is_product_seeding_order),
    src.is_bops_order = COALESCE(src.is_bops_order, def.is_bops_order),
    src.is_custom_order = COALESCE(src.is_custom_order, def.is_custom_order),
    src.is_discreet_packaging = COALESCE(src.is_discreet_packaging, def.is_discreet_packaging),
    src.is_bill_me_now_online = COALESCE(src.is_bill_me_now_online, def.is_bill_me_now_online),
    src.is_bill_me_now_gms = COALESCE(src.is_bill_me_now_gms, def.is_bill_me_now_gms),
    src.is_membership_gift = COALESCE(src.is_membership_gift, def.is_membership_gift),
    src.is_dropship = COALESCE(src.is_dropship, def.is_dropship),
    src.is_warehouse_outlet_order = COALESCE(src.is_warehouse_outlet_order, def.is_warehouse_outlet_order),
    src.is_third_party = COALESCE(src.is_third_party, def.is_third_party),
    src.meta_update_datetime = CURRENT_TIMESTAMP
FROM (
    SELECT
        order_sales_channel_l1,
        order_sales_channel_l2,
        order_classification_l1,
        order_classification_l2,
        is_border_free_order,
        is_ps_order,
        is_retail_ship_only_order,
        is_test_order,
        is_preorder,
        is_product_seeding_order,
        is_bops_order,
        is_custom_order,
        is_discreet_packaging,
        is_bill_me_now_online,
        is_bill_me_now_gms,
        is_membership_gift,
        is_dropship,
        is_warehouse_outlet_order,
        is_third_party
    FROM _dim_order_sales_channel__data
    WHERE order_sales_channel_key = -1
    ) AS def
WHERE src.is_current
    AND (src.order_sales_channel_l1 IS NULL
        OR src.order_sales_channel_l2 IS NULL
        OR src.order_classification_l1 IS NULL
        OR src.order_classification_l2 IS NULL
        OR src.is_border_free_order IS NULL
        OR src.is_ps_order IS NULL
        OR src.is_retail_ship_only_order IS NULL
        OR src.is_test_order IS NULL
        OR src.is_preorder IS NULL
        OR src.is_product_seeding_order IS NULL
        OR src.is_bops_order IS NULL
        OR src.is_custom_order IS NULL
        OR src.is_discreet_packaging IS NULL
        OR src.is_bill_me_now_online IS NULL
        OR src.is_bill_me_now_gms IS NULL
        OR src.is_membership_gift IS NULL
        OR src.is_dropship IS NULL
        OR src.is_warehouse_outlet_order IS NULL
        OR src.is_third_party IS NULL);
-- SELECT * FROM stg.dim_order_sales_channel;

-- Insert new data records that do not already exist
SET start_order_sales_channel_key = (
    SELECT COALESCE(MAX(order_sales_channel_key), 0)
    FROM stg.dim_order_sales_channel
    WHERE order_sales_channel_key > 0
    );
INSERT INTO stg.dim_order_sales_channel (
    order_sales_channel_key,
    order_sales_channel_l1,
    order_sales_channel_l2,
    order_classification_l1,
    order_classification_l2,
    is_border_free_order,
    is_ps_order,
    is_retail_ship_only_order,
    is_test_order,
    is_preorder,
    is_product_seeding_order,
    is_bops_order,
    is_custom_order,
    is_discreet_packaging,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_membership_gift,
    is_dropship,
    is_warehouse_outlet_order,
    is_third_party,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    ROW_NUMBER() OVER (ORDER BY d.order_sales_channel_key) + $start_order_sales_channel_key AS order_sales_channel_key,
    d.order_sales_channel_l1,
    d.order_sales_channel_l2,
    d.order_classification_l1,
    d.order_classification_l2,
    d.is_border_free_order,
    d.is_ps_order,
    d.is_retail_ship_only_order,
    d.is_test_order,
    d.is_preorder,
    d.is_product_seeding_order,
    d.is_bops_order,
    d.is_custom_order,
    d.is_discreet_packaging,
    d.is_bill_me_now_online,
    d.is_bill_me_now_gms,
    d.is_membership_gift,
    d.is_dropship,
    d.is_warehouse_outlet_order,
    d.is_third_party,
    d.effective_start_datetime,
    d.effective_end_datetime,
    d.is_current,
    d.meta_create_datetime,
    d.meta_update_datetime
FROM _dim_order_sales_channel__data AS d
WHERE d.order_sales_channel_key > 0
    AND NOT EXISTS (
        SELECT TRUE AS is_exists
        FROM stg.dim_order_sales_channel AS src
        WHERE src.order_sales_channel_l1 = d.order_sales_channel_l1
            AND src.order_sales_channel_l2 = d.order_sales_channel_l2
            AND src.order_classification_l1 = d.order_classification_l1
            AND src.order_classification_l2 = d.order_classification_l2
            AND src.is_border_free_order = d.is_border_free_order
            AND src.is_ps_order = d.is_ps_order
            AND src.is_retail_ship_only_order = d.is_retail_ship_only_order
            AND src.is_test_order = d.is_test_order
            AND src.is_preorder = d.is_preorder
            AND src.is_product_seeding_order = d.is_product_seeding_order
            AND src.is_bops_order = d.is_bops_order
            AND src.is_custom_order = d.is_custom_order
            AND src.is_discreet_packaging = d.is_discreet_packaging
            AND src.is_bill_me_now_online = d.is_bill_me_now_online
            AND src.is_bill_me_now_gms = d.is_bill_me_now_gms
            AND src.is_membership_gift = d.is_membership_gift
            AND src.is_dropship = d.is_dropship
            AND src.is_warehouse_outlet_order = d.is_warehouse_outlet_order
            AND src.is_third_party = d.is_third_party
        )
ORDER BY d.order_sales_channel_key;

-- SELECT * FROM stg.dim_order_sales_channel ORDER BY order_sales_channel_key;
-- SELECT COUNT(1) FROM stg.dim_order_sales_channel;
