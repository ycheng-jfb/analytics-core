CREATE OR REPLACE VIEW LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CASH_DEPOSIT_RECEIPTS
AS
select
    b.cash_receipt_url as cash_receipt_url,
    replace(a.deposit_image, 'deposit-slips/', '') as receipt_file_name,
    edw_prod.stg.udf_unconcat_brand(a.cash_deposit_id) AS cash_deposit_id,
    a.store_id,
    a.administrator_id,
    a.deposited_administrator_id,
    a.amount,
    a.date_deposit,
    a.datetime_deposited,
    a.statuscode
from lake_consolidated.ultra_merchant.cash_deposit as a
    left join (
        select distinct
            metadata$filename as deposit_image,
            get_presigned_url(@lake_stg.public.omnishop_sxf_images, replace(metadata$filename,'deposit-slips/', ''), 604800) as cash_receipt_url
        from @lake_stg.public.omnishop_sxf_images

        union all
        select distinct
            metadata$filename as deposit_image,
            get_presigned_url(@lake_stg.public.omnishop_fl_images, replace(metadata$filename,'deposit-slips/', ''), 604800) as cash_receipt_url
        from @lake_stg.public.omnishop_fl_images
    ) as b
    on a.deposit_image = b.deposit_image;
