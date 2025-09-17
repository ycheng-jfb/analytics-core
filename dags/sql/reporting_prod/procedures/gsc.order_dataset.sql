begin transaction name GSC_order_shipment;

truncate table REPORTING_PROD.GSC.ORDER_DATASET;

insert into REPORTING_PROD.GSC.ORDER_DATASET
(
ORDER_ID
,ORDER_TYPE
,IS_VIP
,ORDER_STORE
,BUSINESS_UNIT
,SYSTEM_DATE_PLACED
,SYSTEM_DATE_SHIPPED
,ORDERED_UNITS
,CUSTOMER_SELECTED_TYPE
,CUSTOMER_SELECTED_SERVICE
,CUSTOMER_SELECTED_COST
,CUSTOMER_SELECTED_DESCRIPTION
,IS_EXCHANGE_ORDER
,ORIGINAL_ORDER_ID
,ORDER_DATETIME_MODIFIED
,ORDER_STATUS
,PAYMENT_METHOD
,IS_ACTIVATING_C
,IS_VIP_C
)

select
      O.ORDER_ID,
      CASE WHEN OC.order_id is not null THEN 'Activating' ELSE 'Non-Activating' END as order_type,
      CASE WHEN ML.label like '%VIP%' then 'Y' else 'N' end as is_VIP,
      S.LABEL AS ORDER_STORE,
      Case
        when fod.store_id = 241
          then 'YITTY'
          else S.ALIAS
        end as BUSINESS_UNIT,
      O.DATE_PLACED AS SYSTEM_DATE_PLACED,
      O.DATETIME_TRANSACTION AS SYSTEM_DATE_SHIPPED,
      SUM(OL.QUANTITY) AS ORDERED_UNITS,
      COALESCE(SO.TYPE,OS.TYPE) AS CUSTOMER_SELECTED_TYPE,
      SO.LABEL AS CUSTOMER_SELECTED_SERVICE,
      SO.COST AS CUSTOMER_SELECTED_COST,
      SO.DESCRIPTION AS CUSTOMER_SELECTED_DESCRIPTION,
      IFF(E.EXCHANGE_ID IS NULL, 'N', 'Y') AS IS_EXCHANGE_ORDER,
      E.ORIGINAL_ORDER_ID,
      O.DATETIME_MODIFIED AS ORDER_DATETIME_MODIFIED,
      SC.LABEL AS ORDER_STATUS,
      O.PAYMENT_METHOD
      ,vip.IS_Activating_c
      ,vip.IS_VIP_c
  from
      LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT."ORDER" O
      LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.ORDER_LINE OL
        ON OL.ORDER_ID = O.ORDER_ID
      LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STORE S
      ON O.STORE_ID = S.STORE_ID
      LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.ORDER_SHIPPING OS
      ON O.ORDER_ID = OS.ORDER_ID
      --and NVL(to_char(OS.CARRIER_SERVICE_ID), 'MikeWasHere') != 'MikeWasHere'
      LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.SHIPPING_OPTION SO
      ON OS.SHIPPING_OPTION_ID = SO.SHIPPING_OPTION_ID
      LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.EXCHANGE E
      ON O.ORDER_ID = E.EXCHANGE_ORDER_ID
      LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.order_classification oc (nolock)
      ON O.ORDER_ID = OC.ORDER_ID and OC.ORDER_TYPE_ID in (23,33)
      left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_LEVEL ML (nolock)
      on O.MEMBERSHIP_LEVEL_ID = ML.MEMBERSHIP_LEVEL_ID
      LEFT JOIN LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STATUSCODE SC
        ON O.PROCESSING_STATUSCODE = SC.STATUSCODE
     LEFT JOIN (SELECT fo.order_id
            ,case
                  when domc.membership_order_type_l1='Activating VIP' then 'Y'
                  when domc.membership_order_type_l1='NonActivating' then 'N'
              end IS_Activating_c--new logic
              ,case
                  when domc.IS_VIP=0 then 'N'
                  when domc.IS_VIP=1 then 'Y'
              end IS_VIP_c --new logic
      from EDW_PROD.DATA_MODEL.FACT_ORDER fo
        JOIN EDW_PROD.DATA_MODEL.DIM_ORDER_MEMBERSHIP_CLASSIFICATION domc
                on domc.ORDER_MEMBERSHIP_CLASSIFICATION_KEY = fo.ORDER_MEMBERSHIP_CLASSIFICATION_KEY
        JOIN EDW_PROD.DATA_MODEL.DIM_ORDER_SALES_CHANNEL oc
                on oc.ORDER_SALES_CHANNEL_KEY = fo.ORDER_SALES_CHANNEL_KEY
          WHERE
              oc.ORDER_CLASSIFICATION_L1  IN ('Product Order', 'Exchange', 'Reship')
               ) vip
       on o.order_id = vip.order_id
      left join edw_prod.data_model.fact_order fod
        on o.order_id = fod.order_id
  WHERE
      S.LABEL NOT LIKE 'RTLSXF%'
      AND O.DATETIME_ADDED >= '2019-01-01'
  GROUP BY
      O.ORDER_ID,
      CASE WHEN OC.order_id is not null THEN 'Activating' ELSE 'Non-Activating' END,
      CASE WHEN ML.label like '%VIP%' then 'Y' else 'N' end,
      S.LABEL,
      Case
        when fod.store_id = 241
          then 'YITTY'
          else S.ALIAS
      end,
      O.DATE_PLACED,
      O.DATETIME_TRANSACTION,
      COALESCE(SO.TYPE,OS.TYPE),
      SO.LABEL,
      SO.COST,
      SO.DESCRIPTION,
      IFF(E.EXCHANGE_ID IS NULL, 'N', 'Y'),
      E.ORIGINAL_ORDER_ID,
      O.DATETIME_MODIFIED,
      SC.LABEL,
      O.PAYMENT_METHOD
      ,vip.IS_Activating_c
      ,vip.IS_VIP_c;

COMMIT;
