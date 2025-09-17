SELECT DISTINCT fol.order_line_id
FROM edw_prod.DATA_MODEL_FL.fact_order_line fol
    LEFT JOIN REPORTING_BASE_PROD.FABLETICS.FACT_ORDER_LINE_EXTENDED_NEW fo2
        ON fol.order_line_id = fo2.order_line_id
    JOIN edw_prod.DATA_MODEL_FL.DIM_CUSTOMER dc ON dc.CUSTOMER_ID = fol.CUSTOMER_ID
    JOIN edw_prod.data_model_fl.DIM_ORDER_PRODUCT_SOURCE opc
        ON opc.order_product_source_key = fol.order_product_source_key
    LEFT JOIN reporting_prod.fabletics.psourcecategorization pscc
        ON lower(pscc.psource_level) = lower(opc.order_product_source_name)
    JOIN edw_prod.DATA_MODEL_FL.fact_order fo ON fol.order_id = fo.order_id
    JOIN edw_prod.DATA_MODEL_FL.FACT_ACTIVATION fa ON fa.ACTIVATION_KEY = fo.ACTIVATION_KEY
    JOIN edw_prod.DATA_MODEL_FL.DIM_ITEM_PRICE dp ON dp.ITEM_PRICE_KEY = fol.ITEM_PRICE_KEY
    LEFT JOIN lake_view.ultra_warehouse.item i ON i.item_number = dp.sku
    JOIN edw_prod.DATA_MODEL_FL.dim_store ds ON fol.store_id = ds.store_id
    JOIN edw_prod.DATA_MODEL_FL.dim_product_type dpt ON fol.product_type_key = dpt.product_type_key
    JOIN edw_prod.DATA_MODEL_FL.dim_order_membership_classification mc
        ON mc.order_membership_classification_key = fol.order_membership_classification_key
    JOIN edw_prod.DATA_MODEL_FL.dim_order_sales_channel doc
        ON fol.order_sales_channel_key = doc.order_sales_channel_key
    JOIN edw_prod.DATA_MODEL_FL.dim_order_status dos ON fol.order_status_key = dos.order_status_key
    JOIN edw_prod.DATA_MODEL_FL.dim_order_processing_status ops
        ON ops.order_processing_status_key = fo.order_processing_status_key
    JOIN edw_prod.DATA_MODEL_FL.dim_order_line_status ols  ON fol.order_line_status_key = ols.order_line_status_key
WHERE ds.store_brand IN ('Fabletics', 'Yitty')
    AND dpt.product_type_name NOT IN
       ('Offer Gift', 'Autoship Gift', 'Free Sample', 'Gift Certificate', 'Flyer', 'Candidate Product',
        'Membership Gift', 'Directed Insert', 'Free Third Party Subscription')
    AND doc.order_classification_l1 IN ('Product Order')
    AND doc.is_ps_order = 'FALSE'
    AND dos.order_status IN ('Success', 'Pending')
    AND ols.order_line_status <> 'Cancelled'
    AND fo2.order_line_id IS NULL
    AND fol.order_local_datetime::date >='2013-08-30';
