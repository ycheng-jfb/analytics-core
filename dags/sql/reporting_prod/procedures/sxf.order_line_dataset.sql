SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;
SET refresh_date = IFF(DATE_PART('weekday', CURRENT_DATE) = 0 AND HOUR(CURRENT_TIME) = 15, '1900-01-01'::TIMESTAMP_LTZ, $low_watermark_datetime);

use edw_prod;
CREATE OR REPLACE TEMP TABLE _base_orders AS (
select fo.order_id
from EDW_PROD.DATA_MODEL_SXF.FACT_ORDER fo
where fo.meta_update_datetime >= $refresh_date

UNION

select fol.order_id
from EDW_PROD.DATA_MODEL_SXF.FACT_ORDER_LINE fol
where fol.meta_update_datetime >= $refresh_date

UNION

select fol.order_id
from EDW_PROD.DATA_MODEL_SXF.FACT_ORDER_LINE fol
join LAKE_SXF_VIEW.ULTRA_MERCHANT.ORDER_LINE_DISCOUNT d on (fol.order_line_id=d.order_line_id)
where d.hvr_change_time >= $refresh_date

UNION

select fop.order_id
from EDW_PROD.DATA_MODEL_SXF.FACT_ORDER_PRODUCT_COST fop
where fop.meta_update_datetime >=$refresh_date

UNION

select fce.redemption_order_id
from EDW_PROD.DATA_MODEL_SXF.FACT_CREDIT_EVENT fce
where fce.meta_update_datetime >= $refresh_date and fce.redemption_order_id is not null

UNION

select frl.order_id
from EDW_PROD.DATA_MODEL_SXF.FACT_RETURN_LINE frl
where frl.meta_update_datetime >= $refresh_date

UNION

select fo.order_id
FROM LAKE_SXF_VIEW.ULTRA_MERCHANT."ORDER" fo
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds ON ds.store_id = fo.store_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.ORDER_CREDIT AS oc ON oc.order_id = fo.order_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.STORE_CREDIT AS sc ON sc.store_credit_id = oc.store_credit_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.MEMBERSHIP_STORE_CREDIT msc ON sc.store_credit_id = msc.store_credit_id
WHERE COALESCE(oc.store_credit_id, oc.membership_token_id, oc.gift_certificate_id) IS NOT NULL
AND store_brand = 'Savage X'
AND (oc.hvr_change_time >= $refresh_date
      OR sc.hvr_change_time >= $refresh_date
     OR msc.hvr_change_time >= $refresh_date)
);


--PROMOS--

CREATE OR REPLACE TEMP TABLE _promos as (
select
listagg(label,' | ') within group (order by label) as promo, --aggregating all promos to order line id
min(refunds_allowed) as refunds_allowed, -- In case 1+ promos used on a sale, checking to see at least 1 of the promos was final sale
fol.order_line_id
from EDW_PROD.DATA_MODEL_SXF.FACT_ORDER fo
JOIN _base_orders bo ON (bo.ORDER_ID=fo.ORDER_ID)
JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ORDER_LINE fol ON (fol.ORDER_ID=fo.ORDER_ID)
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE st ON (st.STORE_ID = fo.STORE_ID)
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_LINE_STATUS ols on (ols.ORDER_LINE_STATUS_KEY=fol.ORDER_LINE_STATUS_KEY)
join LAKE_SXF_VIEW.ULTRA_MERCHANT.ORDER_LINE_DISCOUNT d on (fol.order_id = d.order_id AND (fol.order_line_id=d.order_line_id OR fol.bundle_order_line_id=d.order_line_id))
join LAKE_SXF_VIEW.ULTRA_MERCHANT.PROMO pr ON (pr.promo_id = d.promo_id)
where
    st.STORE_BRAND='Savage X'
    and ols.ORDER_LINE_STATUS<>'Cancelled'
group by fol.order_line_id);

CREATE OR REPLACE TEMP TABLE _promos_order_level as (
select
listagg(DISTINCT label,' | ') within group (order by label) as promo, --aggregating all promos to order id
fol.order_id
from EDW_PROD.DATA_MODEL_SXF.FACT_ORDER fo
JOIN _base_orders bo ON (bo.ORDER_ID=fo.ORDER_ID)
JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ORDER_LINE fol ON (fol.ORDER_ID=fo.ORDER_ID)
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE st ON (st.STORE_ID = fo.STORE_ID)
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_LINE_STATUS ols on (ols.ORDER_LINE_STATUS_KEY=fol.ORDER_LINE_STATUS_KEY)
join LAKE_SXF_VIEW.ULTRA_MERCHANT.ORDER_LINE_DISCOUNT d on (fol.order_id = d.order_id AND (fol.order_line_id=d.order_line_id OR fol.bundle_order_line_id=d.order_line_id))
join LAKE_SXF_VIEW.ULTRA_MERCHANT.PROMO pr ON (pr.promo_id = d.promo_id)
where  st.STORE_BRAND='Savage X'
  and ols.ORDER_LINE_STATUS<>'Cancelled'
group by fol.order_id);

CREATE OR REPLACE TEMP TABLE _all_credits AS
SELECT
    fo.order_id,
    fo.customer_id,
    fo.order_local_datetime,
    COALESCE(oc.store_credit_id, oc.membership_token_id, oc.gift_certificate_id) AS credit_id
FROM EDW_PROD.DATA_MODEL_SXF.FACT_ORDER fo
JOIN _base_orders b ON fo.order_id = b.order_id
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds ON ds.store_id = fo.store_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.ORDER_CREDIT AS oc ON oc.order_id = fo.order_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.STORE_CREDIT AS sc ON sc.store_credit_id = oc.store_credit_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.MEMBERSHIP_STORE_CREDIT msc  ON sc.store_credit_id = msc.store_credit_id
-- LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.ORDER_CREDIT_DELETE_LOG ocdl ON ocdl.order_credit_id = oc.order_credit_id---------------------CHECK ME OUT
WHERE credit_id IS NOT NULL --AND ocdl.order_credit_id IS NULL
AND store_brand = 'Savage X';


CREATE OR REPLACE TEMPORARY TABLE _credits_per_order AS
SELECT
    order_id
    ,COUNT (DISTINCT cc.credit_id) num_credits_redeemed
FROM _all_credits cc
LEFT JOIN EDW_PROD.DATA_MODEL_SXF.DIM_CREDIT DC ON dc.credit_id = cc.credit_id and dc.customer_id = cc.customer_id
WHERE dc.credit_type NOT ilike 'Variable Credit'
GROUP BY order_id;

/*
Replace the above code with this code after the fix in https://jira.techstyle.net/browse/DA-24642

CREATE OR REPLACE TEMPORARY TABLE _credit_ids AS (
SELECT DISTINCT credit_id
FROM EDW_PROD.DATA_MODEL_SXF.dim_credit dc
JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds ON dc.store_id = ds.store_id
WHERE store_brand = 'Savage X'
    AND credit_type NOT ILIKE 'Variable Credit'
  );

CREATE OR REPLACE TEMPORARY TABLE _credits_per_order_new AS
(
SELECT redemption_order_id order_id,
COUNT(DISTINCT fc.credit_id) num_credits_redeemed,
ROUND(SUM(credit_activity_equivalent_count),0) credit_activity_equivalent_count,
ROUND(SUM(credit_activity_local_amount),2) credit_activity_local_amount
FROM EDW_PROD.DATA_MODEL_SXF.fact_credit_event AS fc
JOIN _credit_ids cd ON fc.credit_id = cd.credit_id
JOIN EDW_PROD.DATA_MODEL_SXF.fact_order fo ON fo.order_id = fc.redemption_order_id
JOIN EDW_PROD.DATA_MODEL_SXF.dim_store ds ON fo.store_id = ds.store_id
WHERE credit_activity_type = 'Redeemed'
  AND store_brand = 'Savage X'
  GROUP BY redemption_order_id
  );
*/

--BUNDLES--
-- Bundles for SXF is different than "bundles" for other brands. SXF Bundles means _ for _. For example 3 for $25 Undies
CREATE OR REPLACE TEMP TABLE _bundles as (
select
    distinct
    old.order_line_id,
    pro.label as bundle_promo_name,
    1 as bundle
FROM LAKE_SXF_VIEW.ULTRA_MERCHANT.ORDER_LINE_DISCOUNT old
JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ORDER_LINE fol on (fol.order_line_id=old.order_line_id)
JOIN _base_orders bo ON (bo.ORDER_ID=fol.ORDER_ID)
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.PROMO pro on (pro.promo_id = old.promo_id)
WHERE
lower(pro.label) like '%%for%%' -- Bundles Only
and lower(pro.label) not like '%%ship%%'
and lower(pro.label) not like '%%add%%'
and indirect_discount = 0
);


CREATE OR REPLACE TEMP TABLE _order_line_dataset_stg AS
SELECT distinct
    p.product_sku
    ,p.sku
    ,p.base_sku
    ,fo.order_local_datetime
    ,convert_timezone('America/Los_Angeles' , fol.order_local_datetime) ORDER_HQ_DATETIME
    ,date_trunc('day',to_timestamp(convert_timezone('America/Los_Angeles' , fol.order_local_datetime))) ORDER_HQ_DATE
    ,fol.order_id
    ,fo.master_order_id
    ,fo.store_id  as Order_store_id
    ,st.STORE_FULL_NAME as Order_Store_Full_Name
    ,st.STORE_TYPE as Order_Store_Type
    ,dst.store_id as customer_activating_store_id
    ,dst.STORE_FULL_NAME as Customer_Activation_Store_Full_Name
    ,dst.STORE_TYPE as Customer_Activation_Store_Type
    ,fol.order_line_id
    ,dc.customer_id
    ,dc.how_did_you_hear
    ,mt.label as membership_type_time_of_order
    ,pr.promo
    ,pro.promo AS order_promo_name
    ,CASE WHEN pr.promo ILIKE '%%GWP%%' OR pr.promo ILIKE '%%Gift%%with%%Purchase%%' OR pr.promo ILIKE '%%Free Gift%%' THEN TRUE
        ELSE FALSE
        END AS is_gwp
    ,case when st.store_country='UK' and fo.order_local_datetime>='2021-03-18' then 'EU-UK' -- started shipping UK store orders from UK warehouse on 03/18/21
        else st.store_region end as store_region_abbr
    ,st.store_country
    ,omc.membership_order_type_L2 as is_ecomm -- old WH: dod.is_ecomm
    ,omc.membership_order_type_L1 as is_activating -- old WH: dod.is_activating
    ,omc.membership_order_type_L3 as is_ecomm_detailed
    ,pt.is_free
    ,ph.vip_unit_price
    ,ph.retail_unit_price
    ,fol.item_quantity
    ,zeroifnull(iff(pr.refunds_allowed=0,1,0)) as finalsales_quantity -- Inverse or Return Allowed to see final sale sold unit
    ,zeroifnull(frl.RETURN_ITEM_QUANTITY) as RETURN_ITEM_QUANTITY
    ,frl.RETURN_COMPLETION_LOCAL_DATETIME
    ,convert_timezone('America/Los_Angeles' , frl.RETURN_COMPLETION_LOCAL_DATETIME ) as RETURN_COMPLETION_DATETIME -- SxF specific fso is using receipt b/c there were successful completions without a completion date, however that's not true for sxf so you should use return_completion_local_datetime, 95% they are the same - Per Dani R.
    ,convert_timezone('America/Los_Angeles' , frl.return_receipt_local_datetime ) as Return_receipt_datetime
    ,COALESCE( fol.Subtotal_Excl_Tariff_Local_Amount,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) Subtotal_Amount_Excl_Tariff
    ,COALESCE( fol.Tariff_Revenue_Local_Amount,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) Tariff_Revenue
    ,COALESCE( fol.Product_Subtotal_Local_Amount,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) Product_Subtotal_Amount
    ,COALESCE( fol.tax_local_amount,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) Tax_Amount
    ,COALESCE( fol.product_discount_local_amount,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) Product_Discount_Amount
    ,COALESCE(foL.SHIPPING_REVENUE_LOCAL_AMOUNT,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) Shipping_Revenue_Amount
    ,COALESCE( fol.product_gross_revenue_local_amount,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) Product_Gross_Revenue_Amount -- if product order subtotal_excl_tariff_local_amount+ tariff_revenue_local_amount- product_discount_local_amount+ shipping_revenue_before_discount_local_amount - shipping_discount_local_amount- non_cash_credit_local_amount
    ,COALESCE( fol.product_gross_revenue_excl_shipping_local_amount,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) Product_Gross_Revenue_Excl_Shipping_Amount -- If Product Order: subtotal_excl_tariff_local_amount+tariff_revenue_local_amount-product_discount_local_amount-non_cash_credit_local_amount
    ,COALESCE( fol.non_cash_credit_local_amount,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) NonCashCredit_Amount
    ,COALESCE( fol.payment_transaction_local_amount,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) Payment_Transaction_Amount
    ,COALESCE(fol.cash_gross_revenue_local_amount,0) * COALESCE(fol.payment_transaction_date_usd_conversion_rate,1) Cash_Gross_Revenue
    --,Payment_Transaction_Amount-Tax_Amount as Cash_Gross_Revenue
    ,COALESCE(fol.estimated_landed_cost_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1) Estimated_Landed_Cost --Estimated_Total_Cost
    ,COALESCE(fol.reporting_landed_cost_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1) reporting_landed_cost
    ,COALESCE(fol.oracle_cost_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1)  as Oracle_Cost
    ,COALESCE(fol.lpn_po_cost_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1)  as lpn_po_cost
    ,COALESCE(fol.po_cost_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1)  as po_cost
    ,COALESCE(fol.estimated_shipping_supplies_cost_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1)  as estimated_shipping_supplies_cost
    ,COALESCE(fol.estimated_shipping_cost_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1)  as estimated_shipping_cost
    ,COALESCE(fol.estimated_variable_warehouse_cost_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1)  as estimated_variable_warehouse_cost
    ,COALESCE(fol.estimated_variable_gms_cost_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1)  as estimated_variable_gms_cost
    ,(Subtotal_Amount_Excl_Tariff + Shipping_Revenue_Amount - Product_Discount_Amount) Revenue_incl_ShippingRev
    ,(Subtotal_Amount_Excl_Tariff - Product_Discount_Amount) Revenue_excl_ShippingRev
    ,(Product_Gross_Revenue_Amount- reporting_landed_cost) Gross_Margin
    ,(Product_Gross_Revenue_Excl_Shipping_Amount - reporting_landed_cost) Gross_Product_Margin
    ,COALESCE(fol.product_margin_pre_return_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1) as product_margin_pre_return
    ,COALESCE(fol.product_margin_pre_return_excl_shipping_local_amount,0)*COALESCE(fol.order_date_usd_conversion_rate,1) as product_margin_pre_return_excl_shipping
    ,IFF(b.PRODUCT_NAME <>'Not Applicable' and b.PRODUCT_NAME <>'Unknown'
      and (b.PRODUCT_NAME not like '%% Box %%' OR lower(b.PRODUCT_NAME) like '%% set%%') -- Rih Boxes for Savage, but getting 'Boxer sets'
      AND b.PRODUCT_NAME not like '%% Box'
      and b.product_type <>'BYO', -- BYO Sets
        fol.ITEM_QUANTITY,0) as is_pre_made_set
    ,IFF(b.product_type='BYO',fol.ITEM_QUANTITY,0) as is_byo_set
    ,IFF((b.PRODUCT_NAME like '%% Box %%' OR b.PRODUCT_NAME like '%% Box')
      and lower(b.product_name) not like '%% set'
      and lower(b.product_name) not like '%% set (',
        fol.ITEM_QUANTITY,0) as is_vip_box
    ,ifnull(bl.bundle,0) as is_bundle
    ,fol.item_quantity-is_pre_made_set-is_byo_set-is_bundle-is_vip_box as is_individual_item
    ,coalesce(bl.bundle_promo_name,b.PRODUCT_NAME) as set_box_bundle_name
    ,b.short_description as set_box_description
    ,ifnull(fol.token_count,0) as token_count -- order_line
    ,ifnull(fol.token_local_amount,0)*COALESCE(fol.payment_transaction_date_usd_conversion_rate,1)  as token_amount-- order_line
    ,ifnull(cpo.Num_Credits_Redeemed,0) as Num_Credits_Redeemed_On_Order -- order level duplicated at order_line
    ,fol.GROUP_KEY as bundle_group_key
    ,listagg(p.product_sku,' | ') within group (order by p.product_sku) over (partition by bundle_group_key,fol.order_id) as atb_grouped_items
    ,ols.order_line_status
    ,os.order_status
    ,dops.order_processing_status_code
    ,dops.order_processing_status
    ,pay.payment_method
    ,pay.creditcard_type
    ,osc.is_preorder as is_preorder_order_level
    ,iff(is_activating='NonActivating' and is_ecomm='Guest', p.retail_unit_price, p.vip_unit_price) as initial_retail_price
    ,fol.effective_vat_rate
    ,ph.vip_unit_price / (1 + fol.effective_vat_rate) as vip_unit_price_excl_vat
    ,ph.retail_unit_price / (1 + fol.effective_vat_rate) as retail_unit_price_excl_vat
    ,IFF(is_activating='NonActivating' and is_ecomm='Guest', retail_unit_price_excl_vat, vip_unit_price_excl_vat) as initial_retail_price_excl_vat
    ,convert_timezone('America/Los_Angeles' , fol.shipped_local_datetime) as SHIPPED_HQ_DATETIME
    ,date_trunc('day',convert_timezone('America/Los_Angeles' , fol.shipped_local_datetime)) as SHIPPED_HQ_DATE
    ,fol.shipped_local_datetime
    ,CASE
        WHEN mk.color_sku_po IS NOT NULL THEN 'MD'
        ELSE 'FP'
    END is_marked_down
FROM
    EDW_PROD.DATA_MODEL_SXF.FACT_ORDER fo
    JOIN _base_orders bo ON (bo.ORDER_ID=fo.ORDER_ID)
    JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ORDER_LINE fol ON (fol.ORDER_ID=fo.ORDER_ID)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_PRODUCT p ON (p.PRODUCT_ID = fol.PRODUCT_ID)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_PRODUCT_TYPE pt on (pt.PRODUCT_TYPE_KEY=fol.PRODUCT_TYPE_KEY)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_CUSTOMER dc ON (dc.CUSTOMER_ID=fo.CUSTOMER_ID)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE st ON (st.STORE_ID = fo.STORE_ID)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE stc ON (stc.STORE_ID = dc.STORE_ID)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_SALES_CHANNEL osc ON (fo.order_sales_channel_key=osc.order_sales_channel_key)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_MEMBERSHIP_CLASSIFICATION omc on (omc.order_membership_classification_key=fo.order_membership_classification_key)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_STATUS os ON (os.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_PROCESSING_STATUS dops ON (dops.ORDER_PROCESSING_STATUS_KEY=fo.ORDER_PROCESSING_STATUS_KEY)
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_LINE_STATUS ols on (ols.ORDER_LINE_STATUS_KEY=fol.ORDER_LINE_STATUS_KEY)
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.DIM_PRODUCT_PRICE_HISTORY ph ON (ph.PRODUCT_PRICE_HISTORY_KEY = fol.PRODUCT_PRICE_HISTORY_KEY)
    left join EDW_PROD.DATA_MODEL_SXF.DIM_PAYMENT pay on fo.payment_key=pay.payment_key
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.FACT_RETURN_LINE frl on (frl.ORDER_LINE_ID=fol.ORDER_LINE_ID)
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.DIM_RETURN_STATUS drs on (drs.RETURN_STATUS_KEY = frl.RETURN_STATUS_KEY) AND drs.return_status = 'Resolved'
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.DIM_PRODUCT as b on (b.product_id = fol.bundle_product_id) --sets/boxes
    LEFT JOIN _bundles bl on (fol.order_line_id=bl.order_line_id) --SxF bundles __ for $__
    LEFT JOIN _promos pr on (fol.order_line_id=pr.order_line_id)
    LEFT JOIN _promos_order_level pro on (fol.order_id=pro.order_id)
    LEFT JOIN _credits_per_order cpo ON (fol.order_id=cpo.order_id)
    LEFT JOIN lake_consolidated_view.ultra_merchant_history.membership mh on (mh.customer_id= concat(dc.customer_id,30))
        AND convert_timezone('America/Los_Angeles' ,fo.ORDER_LOCAL_DATETIME) >= date_trunc('minute',mh.EFFECTIVE_START_DATETIME)
        AND convert_timezone('America/Los_Angeles' ,fo.ORDER_LOCAL_DATETIME) < mh.EFFECTIVE_END_DATETIME
    LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TYPE mt ON mt.MEMBERSHIP_TYPE_ID = mh.MEMBERSHIP_TYPE_ID
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ACTIVATION act on act.activation_key=fo.activation_key and act.customer_id=fo.customer_id
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE dst ON (dst.store_id = act.SUB_STORE_ID)
    LEFT JOIN REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE sm ON p.sku = sm.sku
    LEFT JOIN (select distinct color_sku_po, full_date, store_country
                    from REPORTING_PROD.SXF.view_markdown_history m
                    join EDW_PROD.data_model_sxf.dim_store st on st.store_id = m.store_id)  mk
        ON p.product_sku = mk.color_sku_po
        AND date_trunc('day',to_timestamp(convert_timezone('America/Los_Angeles' , fol.order_local_datetime))) = mk.full_date
        AND st.store_country = mk.store_country
     WHERE
       1=1
        and st.STORE_BRAND='Savage X'
        AND st.STORE_NAME not like '%%(DM)%%'
        AND osc.ORDER_CLASSIFICATION_L1 = 'Product Order'
        AND pt.IS_FREE='FALSE'
    qualify row_number() over (partition by fol.order_line_id order by mh.meta_update_datetime desc) = 1;

DELETE FROM reporting_prod.sxf.order_line_dataset ol
    USING _base_orders bo
    WHERE bo.order_id = ol.order_id;

DELETE FROM reporting_prod.sxf.order_line_dataset ol
    USING edw_prod.stg.fact_order_line fol
WHERE ol.order_line_id = fol.meta_original_order_line_id
  AND (fol.is_deleted = 1 OR fol.is_test_customer = 1);

INSERT INTO reporting_prod.sxf.order_line_dataset
    (PRODUCT_SKU,SKU,BASE_SKU,ORDER_LOCAL_DATETIME,ORDER_HQ_DATETIME,ORDER_HQ_DATE,ORDER_ID,MASTER_ORDER_ID,ORDER_STORE_ID,
    ORDER_STORE_FULL_NAME,ORDER_STORE_TYPE,CUSTOMER_ACTIVATING_STORE_ID,CUSTOMER_ACTIVATION_STORE_FULL_NAME,CUSTOMER_ACTIVATION_STORE_TYPE,
    ORDER_LINE_ID,CUSTOMER_ID,HOW_DID_YOU_HEAR,MEMBERSHIP_TYPE_TIME_OF_ORDER,PROMO,STORE_REGION_ABBR,STORE_COUNTRY,IS_ECOMM,IS_ACTIVATING,
    IS_ECOMM_DETAILED,IS_FREE,VIP_UNIT_PRICE,RETAIL_UNIT_PRICE,ITEM_QUANTITY,FINALSALES_QUANTITY,RETURN_ITEM_QUANTITY,
    RETURN_COMPLETION_LOCAL_DATETIME,RETURN_COMPLETION_DATETIME,RETURN_RECEIPT_DATETIME,SUBTOTAL_AMOUNT_EXCL_TARIFF,TARIFF_REVENUE,
    PRODUCT_SUBTOTAL_AMOUNT,TAX_AMOUNT,PRODUCT_DISCOUNT_AMOUNT,SHIPPING_REVENUE_AMOUNT,PRODUCT_GROSS_REVENUE_AMOUNT,PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_AMOUNT,
    NONCASHCREDIT_AMOUNT,PAYMENT_TRANSACTION_AMOUNT,CASH_GROSS_REVENUE,ESTIMATED_LANDED_COST,REPORTING_LANDED_COST,ORACLE_COST,LPN_PO_COST,PO_COST,ESTIMATED_SHIPPING_SUPPLIES_COST,
    ESTIMATED_SHIPPING_COST,ESTIMATED_VARIABLE_WAREHOUSE_COST,ESTIMATED_VARIABLE_GMS_COST,REVENUE_INCL_SHIPPINGREV,REVENUE_EXCL_SHIPPINGREV,GROSS_MARGIN,
    GROSS_PRODUCT_MARGIN,PRODUCT_MARGIN_PRE_RETURN,PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING,IS_PRE_MADE_SET,IS_BYO_SET,IS_VIP_BOX,IS_BUNDLE,
    IS_INDIVIDUAL_ITEM,SET_BOX_BUNDLE_NAME,SET_BOX_DESCRIPTION,TOKEN_COUNT,TOKEN_AMOUNT,NUM_CREDITS_REDEEMED_ON_ORDER,BUNDLE_GROUP_KEY,ATB_GROUPED_ITEMS,
    ORDER_LINE_STATUS,ORDER_STATUS,ORDER_PROCESSING_STATUS_CODE,ORDER_PROCESSING_STATUS,PAYMENT_METHOD,CREDITCARD_TYPE,IS_PREORDER_ORDER_LEVEL,INITIAL_RETAIL_PRICE,
    EFFECTIVE_VAT_RATE,VIP_UNIT_PRICE_EXCL_VAT,RETAIL_UNIT_PRICE_EXCL_VAT,INITIAL_RETAIL_PRICE_EXCL_VAT,SHIPPED_HQ_DATETIME,SHIPPED_HQ_DATE,SHIPPED_LOCAL_DATETIME,ORDER_PROMO_NAME,IS_GWP,IS_MARKED_DOWN)

SELECT PRODUCT_SKU,SKU,BASE_SKU,ORDER_LOCAL_DATETIME,ORDER_HQ_DATETIME,ORDER_HQ_DATE,ORDER_ID,MASTER_ORDER_ID,ORDER_STORE_ID,
    ORDER_STORE_FULL_NAME,ORDER_STORE_TYPE,CUSTOMER_ACTIVATING_STORE_ID,CUSTOMER_ACTIVATION_STORE_FULL_NAME,CUSTOMER_ACTIVATION_STORE_TYPE,
    ORDER_LINE_ID,CUSTOMER_ID,HOW_DID_YOU_HEAR,MEMBERSHIP_TYPE_TIME_OF_ORDER,PROMO,STORE_REGION_ABBR,STORE_COUNTRY,IS_ECOMM,IS_ACTIVATING,
    IS_ECOMM_DETAILED,IS_FREE,VIP_UNIT_PRICE,RETAIL_UNIT_PRICE,ITEM_QUANTITY,FINALSALES_QUANTITY,RETURN_ITEM_QUANTITY,
    RETURN_COMPLETION_LOCAL_DATETIME,RETURN_COMPLETION_DATETIME,RETURN_RECEIPT_DATETIME,SUBTOTAL_AMOUNT_EXCL_TARIFF,TARIFF_REVENUE,
    PRODUCT_SUBTOTAL_AMOUNT,TAX_AMOUNT,PRODUCT_DISCOUNT_AMOUNT,SHIPPING_REVENUE_AMOUNT,PRODUCT_GROSS_REVENUE_AMOUNT,PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_AMOUNT,
    NONCASHCREDIT_AMOUNT,PAYMENT_TRANSACTION_AMOUNT,CASH_GROSS_REVENUE,ESTIMATED_LANDED_COST,REPORTING_LANDED_COST,ORACLE_COST,LPN_PO_COST,PO_COST,ESTIMATED_SHIPPING_SUPPLIES_COST,
    ESTIMATED_SHIPPING_COST,ESTIMATED_VARIABLE_WAREHOUSE_COST,ESTIMATED_VARIABLE_GMS_COST,REVENUE_INCL_SHIPPINGREV,REVENUE_EXCL_SHIPPINGREV,GROSS_MARGIN,
    GROSS_PRODUCT_MARGIN,PRODUCT_MARGIN_PRE_RETURN,PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING,IS_PRE_MADE_SET,IS_BYO_SET,IS_VIP_BOX,IS_BUNDLE,
    IS_INDIVIDUAL_ITEM,SET_BOX_BUNDLE_NAME,SET_BOX_DESCRIPTION,TOKEN_COUNT,TOKEN_AMOUNT,NUM_CREDITS_REDEEMED_ON_ORDER,BUNDLE_GROUP_KEY,ATB_GROUPED_ITEMS,
    ORDER_LINE_STATUS,ORDER_STATUS,ORDER_PROCESSING_STATUS_CODE,ORDER_PROCESSING_STATUS,PAYMENT_METHOD,CREDITCARD_TYPE,IS_PREORDER_ORDER_LEVEL,INITIAL_RETAIL_PRICE,
    EFFECTIVE_VAT_RATE,VIP_UNIT_PRICE_EXCL_VAT,RETAIL_UNIT_PRICE_EXCL_VAT,INITIAL_RETAIL_PRICE_EXCL_VAT,SHIPPED_HQ_DATETIME,SHIPPED_HQ_DATE,SHIPPED_LOCAL_DATETIME,ORDER_PROMO_NAME,IS_GWP,IS_MARKED_DOWN
    FROM _order_line_dataset_stg;
