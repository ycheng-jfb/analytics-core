CREATE OR REPLACE VIEW data_model_sxf.dim_lpn
(
     lpn_id
    ,lpn_code
    ,warehouse_id
    ,item_id_uw
    ,item_number
    ,receipt_id
    ,po_number
    ,receipt_received_datetime
	,meta_create_datetime
    ,meta_update_datetime
) AS
SELECT
     lpn_id
    ,lpn_code
    ,warehouse_id
    ,item_id_uw
    ,item_number
    ,receipt_id
    ,po_number
    ,receipt_received_datetime
	,meta_create_datetime
    ,meta_update_datetime
FROM stg.dim_lpn;
