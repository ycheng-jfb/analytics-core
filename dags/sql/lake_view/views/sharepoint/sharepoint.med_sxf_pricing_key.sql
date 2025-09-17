CREATE OR REPLACE VIEW LAKE_VIEW.SHAREPOINT.MED_SXF_PRICING_KEY AS
SELECT VIP_PRICE
	,EURO_VIP
	,GBP_VIP
	,MSRP
	,EURO_MSRP
	,GBP_MSRP
	,US_DISCOUNT
	,EURO_DISCOUNT
	,GBP_DISCOUNT
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime
    ,_fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.med_sharepoint_acquisition_v1.style_master_merch_inputs_vip_and_msrp_pricing_key;
