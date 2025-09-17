CREATE OR REPLACE VIEW data_model_fl.dim_order_sales_channel AS
SELECT
    order_sales_channel_key,
	order_sales_channel_l1,
	order_sales_channel_l2,
	order_classification_l1,
	order_classification_l2,
	is_border_free_order,
	is_custom_order,
	is_ps_order,
	is_retail_ship_only_order,
	is_test_order,
	is_preorder,
	is_product_seeding_order,
	is_bops_order,
    is_discreet_packaging,
    is_bill_me_now_online,
    is_bill_me_now_gms,
    is_membership_gift,
    is_warehouse_outlet_order,
    is_third_party,
	--effective_start_datetime,
	--effective_end_datetime,
	--is_current,
	meta_create_datetime,
	meta_update_datetime
FROM stg.dim_order_sales_channel
WHERE is_current;
