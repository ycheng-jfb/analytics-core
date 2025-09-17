CREATE OR REPLACE TEMPORARY TABLE _fact_credit_event AS
SELECT DISTINCT dc.credit_key,
       dc.credit_id,
       dc.credit_id as source_credit_id,
       mtt.administrator_id,
       CONVERT_TIMEZONE(sttt.store_time_zone, mtt.datetime_transaction)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       CAST('Membership Token Transaction Log' AS TEXT(155))                           AS credit_activity_source,
       CAST('Activity Captured Membership Token Transaction Log' AS TEXT(155))         AS credit_activity_source_reason,
       CAST(CASE
                WHEN mttt.label = 'Create' THEN 'Issued'
                WHEN mttt.label = 'Expire' THEN 'Expired'
                WHEN mttt.label = 'Redeem' THEN 'Redeemed'
                WHEN mttt.label = 'Cancel' THEN 'Cancelled'
                WHEN mttt.label = 'Transfer - Outbound' THEN 'Transferred'
                WHEN mttt.label = 'Convert To Credit' THEN 'Converted To Credit'
             END
           AS TEXT(155))                                                                  credit_activity_type,
       CAST(COALESCE(mttr.label, 'Unknown') AS TEXT(155))                              AS credit_activity_type_reason,
       CASE
           WHEN mttt.label = 'Create' THEN amount
           WHEN mttt.label = 'Expire' THEN mt.purchase_price
           WHEN mttt.label = 'Redeem' THEN amount
           WHEN mttt.label = 'Cancel' THEN mt.purchase_price
           WHEN mttt.label = 'Transfer - Outbound' THEN mt.purchase_price
           ELSE amount END                                                             AS activity_amount,
       CASE
           WHEN mttt.label = 'Redeem' AND ord.order_status_key = 8 THEN ord.master_order_id
           WHEN mttt.label = 'Redeem' THEN object_id
        END                                                                            AS redemption_order_id,
       IFF(mttt.label = 'Redeem', object_id, NULL)                                     AS source_redemption_order_id,
       ord.store_id                                                                    AS redemption_store_id,
       CASE
           WHEN st.store_country = 'CA' THEN 'CA'
           WHEN st.store_country = 'US' THEN 'US'
           WHEN a.country_code = 'GB' THEN 'UK'
           WHEN st.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
           WHEN st.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
           WHEN st.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
           WHEN st.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
           WHEN st.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
           WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
           WHEN st.store_country = 'EUREM' THEN 'NL'
           ELSE IFF(sttt.store_country = 'EUREM','NL', sttt.store_country) END                                                 AS vat_rate_ship_to_country
FROM shared.dim_credit dc
         JOIN edw_prod.data_model.dim_store sttt ON sttt.store_id = dc.store_id
         JOIN lake_consolidated.ultra_merchant.membership_token_transaction mtt
              ON mtt.membership_token_id = dc.credit_id
                  AND dc.credit_type = 'Token'
         LEFT JOIN edw_prod.data_model.fact_order ord ON ord.order_id = mtt.object_id
    AND mtt.object = 'order'
         LEFT JOIN lake_consolidated.ultra_merchant.address a ON a.address_id = ord.shipping_address_id
         LEFT JOIN edw_prod.data_model.dim_store st ON st.store_id = ord.store_id
         JOIN lake_consolidated.ultra_merchant.membership_token_transaction_type mttt
              ON mttt.membership_token_transaction_type_id = mtt.membership_token_transaction_type_id
         LEFT JOIN lake_consolidated.ultra_merchant.membership_token_transaction_reason mttr
                   ON mttr.membership_token_transaction_reason_id = mtt.membership_token_transaction_reason_id
         JOIN lake_consolidated.ultra_merchant.membership_token mt
              ON mt.membership_token_id = dc.credit_id
WHERE mtt.datetime_transaction IS NOT NULL
    AND dc.source_credit_id_type = 'Token'

UNION ALL

SELECT DISTINCT dc.credit_key,
       dc.credit_id,
       dc.credit_id AS source_credit_id,
       sct.administrator_id,
       CONVERT_TIMEZONE(sttt.store_time_zone, sct.datetime_transaction)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       'Store Credit Transaction Log'                                                  AS credit_activity_source,
       'Activity Captured Store Credit Transaction Log'                                AS credit_activity_source_reason,
       CASE
           WHEN sctt.label = 'Create' THEN 'Issued'
           WHEN sctt.label = 'Redeem' THEN 'Redeemed'
           WHEN sctt.label = 'Cancel' THEN 'Cancelled'
           WHEN sctt.label = 'Expire' THEN 'Expired'
           WHEN sctt.label = 'Transfer - Outbound' THEN 'Transferred'
           WHEN sctt.label = 'Transfer - Inbound' THEN 'Issued' -- transfer -inbound
           WHEN sctt.label = 'Convert To Variable' THEN 'Converted To Variable'
           WHEN sctt.label = 'Convert To Token' THEN 'Converted To Token'
           END                                                                         AS credit_activity_type,
       COALESCE(sctr.label, 'Unknown')                                                 AS credit_activity_type_reason,
       SUM(CASE
               WHEN sctt.label IN ('Create', 'Redeem', 'Cancel', 'Transfer - Inbound') THEN sct.amount
               WHEN sctt.label IN ('Transfer - Outbound', 'Expire', 'Convert To Variable', 'Convert To Token')
                   THEN sct.balance
               ELSE sct.amount
           END)                                                                        AS activity_amount,
       CASE
           WHEN sct.store_credit_transaction_type_id = 20 AND ord.order_status_key = 8 THEN ord.master_order_id
           WHEN sct.store_credit_transaction_type_id = 20 THEN ord.order_id
        END                                                                            AS redemption_order_id,
    IFF(sct.store_credit_transaction_type_id = 20, ord.order_id, NULL)                 AS source_redemption_order_id,
       ord.store_id                                                                    AS redemption_store_id,
       CASE
           WHEN store.store_country = 'CA' THEN 'CA'
           WHEN store.store_country = 'US' THEN 'US'
           WHEN a.country_code = 'GB' THEN 'UK'
           WHEN store.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
           WHEN store.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
           WHEN store.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
           WHEN store.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
           WHEN store.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
           WHEN store.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
           WHEN store.store_country = 'EUREM' THEN 'NL'
           ELSE IFF(sttt.store_country = 'EUREM','NL', sttt.store_country) END                                                 AS vat_rate_ship_to_country
FROM shared.dim_credit dc
         JOIN edw_prod.data_model.dim_store sttt ON sttt.store_id = dc.store_id
         JOIN lake_consolidated.ultra_merchant.store_credit_transaction sct
              ON sct.store_credit_id = dc.credit_id
                  AND dc.credit_type ILIKE '%credit%'
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit_transaction_reason sctr
                   ON sctr.store_credit_transaction_reason_id = sct.store_credit_transaction_reason_id
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit_transaction_type sctt
                   ON sctt.store_credit_transaction_type_id = sct.store_credit_transaction_type_id
         LEFT JOIN edw_prod.data_model.fact_order ord ON ord.order_id = sct.object_id
    AND sct.object = 'order'
         LEFT JOIN lake_consolidated.ultra_merchant.address a ON a.address_id = ord.shipping_address_id
         LEFT JOIN edw_prod.data_model.dim_store store ON store.store_id = ord.store_id
         JOIN lake_consolidated.ultra_merchant.store_credit sc
              ON sc.store_credit_id = sct.store_credit_id
         JOIN lake_consolidated.ultra_merchant.customer c
              ON c.customer_id = sc.customer_id
         LEFT JOIN lake_consolidated.ultra_merchant.membership_store_credit msc
                   ON msc.store_credit_id = sct.store_credit_id
         JOIN lake_consolidated.ultra_merchant.store_credit_reason scr
              ON scr.store_credit_reason_id = sc.store_credit_reason_id
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit_conversion scc
                   ON scc.converted_store_credit_id = sc.store_credit_id
WHERE sct.datetime_transaction IS NOT NULL
AND NOT (sct.datetime_transaction < '2017-06-01' and sct.store_credit_transaction_type_id = 30 and sct.amount = 0)
AND dc.source_credit_id_type = 'store_credit_id'
GROUP BY dc.credit_key,
         dc.credit_id,
         source_credit_id,
         sct.administrator_id,
         credit_activity_local_datetime,
         credit_activity_type,
         credit_activity_type_reason,
         credit_activity_source,
         credit_activity_source_reason,
         redemption_order_id,
         source_redemption_order_id,
         ord.store_id,
         vat_rate_ship_to_country

UNION ALL
-- add in historical credit activity before store_credit_transaction was created
-- see separate script used to create this
SELECT DISTINCT dcf.credit_key,
       dcf.credit_id,
       dcf.credit_id AS source_credit_id,
       h.administrator_id,
       CONVERT_TIMEZONE(sttt.store_time_zone, activity_datetime)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       activity_source,
       activity_source_reason,
       activity_type,
       NULL                                                                     AS activity_source_reason,
       activity_amount                                                          AS activity_amount,
       redemption_order_id,
       redemption_order_id                                                      AS source_redemption_order_id,
       redemption_store_id,
       COALESCE(vat_rate_ship_to_country, sttt.store_country)                   AS vat_rate_ship_to_country
FROM shared.dim_credit dcf
         JOIN edw_prod.data_model.dim_store sttt ON sttt.store_id = dcf.store_id
         JOIN edw_prod.reference.credit_historical_activity h
              ON h.store_credit_id = dcf.credit_id
                  AND credit_type ILIKE '%credit%'
WHERE dcf.source_credit_id_type = 'store_credit_id'
    AND NOT EXISTS (SELECT 1
                 FROM lake_consolidated.ultra_merchant.store_credit_transaction sct
                 WHERE sct.store_credit_transaction_type_id = 20
                   AND sct.object_id = h.redemption_order_id
                   AND sct.store_credit_id = h.store_credit_id
                   AND sct.datetime_transaction IS NOT NULL)
AND NOT EXISTS (SELECT
                    sct.store_credit_id,
                    sct.datetime_transaction,
                    CASE
                        WHEN sctt.label = 'Create' THEN 'Issued'
                        WHEN sctt.label = 'Redeem' THEN 'Redeemed'
                        WHEN sctt.label = 'Cancel' THEN 'Cancelled'
                        WHEN sctt.label = 'Expire' THEN 'Expired'
                        WHEN sctt.label = 'Transfer - Outbound' THEN 'Transferred'
                        WHEN sctt.label = 'Transfer - Inbound' THEN 'Issued' -- transfer -inbound
                        WHEN sctt.label = 'Convert To Variable' THEN 'Converted To Variable'
                        WHEN sctt.label = 'Convert To Token' THEN 'Converted To Token'
                    END AS activity_type_cast_stmt
                FROM lake_consolidated.ultra_merchant.store_credit_transaction sct
                      JOIN lake_consolidated.ultra_merchant.store_credit_transaction_type AS sctt
                           ON sctt.store_credit_transaction_type_id = sct.store_credit_transaction_type_id
                WHERE sct.store_credit_transaction_type_id <> 20
                    AND sct.datetime_transaction IS NOT NULL
                    AND NOT (sct.datetime_transaction < '2017-06-01' and sct.store_credit_transaction_type_id = 30 and sct.amount = 0)
                    AND sct.store_credit_id = h.store_credit_id
                    AND activity_type_cast_stmt = h.activity_type
                    AND sct.datetime_transaction::date = h.activity_datetime::date
              )

UNION ALL

SELECT DISTINCT dcf.credit_key,
       dcf.credit_id,
       dcf.credit_id AS source_credit_id,
       otd.object_id                                                                                    AS administrator_id,
       CONVERT_TIMEZONE(sttt.store_time_zone,
                        COALESCE(o.date_shipped, o.datetime_transaction, o.date_placed))::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       'Missing SCT - Successful Order',
       'successful in order credit',
       'Redeemed'                                                                                       AS activity_type,
       'Redeemed In Order'                                                                              AS activity_type_reason,
       oc.amount                                                                                        AS activity_amount,
       IFF(fo.order_status_key = 8, fo.master_order_id, oc.order_id)                                     AS redemption_order_id,
       oc.order_id                                                                                       AS source_redemption_order_id,
       fo.store_id                                                                                       AS redemption_store_id,
       CASE
           WHEN store.store_country = 'CA' THEN 'CA'
           WHEN store.store_country = 'US' THEN 'US'
           WHEN a.country_code = 'GB' THEN 'UK'
           WHEN store.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
           WHEN store.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
           WHEN store.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
           WHEN store.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
           WHEN store.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
           WHEN store.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
           WHEN store.store_country = 'EUREM' THEN 'NL'
           ELSE IFF(sttt.store_country = 'EUREM','NL', sttt.store_country) END                                                                  AS vat_rate_ship_to_country
FROM shared.dim_credit dcf
         JOIN edw_prod.data_model.dim_store sttt ON sttt.store_id = dcf.store_id
         JOIN lake_consolidated.ultra_merchant.order_credit oc ON oc.store_credit_id = dcf.credit_id
         --LEFT JOIN lake.ultra_merchant.order_credit_delete_log ocd ON ocd.order_credit_id = oc.order_credit_id
         JOIN lake_consolidated.ultra_merchant."ORDER" o ON o.order_id = oc.order_id
         LEFT JOIN edw_prod.data_model.fact_order AS fo ON fo.order_id = o.order_id
         LEFT JOIN lake_consolidated.ultra_merchant.address a ON a.address_id = o.shipping_address_id
         JOIN edw_prod.data_model.dim_store store ON store.store_id = o.store_id
         JOIN lake_consolidated.ultra_merchant.store st2 ON st2.store_id = o.store_id
         JOIN lake_consolidated.ultra_merchant.statuscode scc ON scc.statuscode = o.payment_statuscode
         JOIN lake_consolidated.ultra_merchant.statuscode scc2 ON scc2.statuscode = o.processing_statuscode
         JOIN lake_consolidated.ultra_merchant.store_credit sc ON sc.store_credit_id = oc.store_credit_id
         JOIN lake_consolidated.ultra_merchant.membership m ON m.customer_id = sc.customer_id
         JOIN lake_consolidated.ultra_merchant.store st ON st.store_id = m.store_id
         JOIN lake_consolidated.ultra_merchant.store_credit_reason scr ON scr.store_credit_reason_id = sc.store_credit_reason_id
         LEFT JOIN lake_consolidated.ultra_merchant.membership_store_credit msc ON msc.store_credit_id = sc.store_credit_id
         LEFT JOIN lake_consolidated.ultra_merchant.order_tracking_detail otd ON otd.order_tracking_id = o.order_tracking_id
    AND otd.object = 'administrator'
WHERE scc.label IN ('Paid', 'Refunded', 'Refunded (Partial)') -- COALESCE(o.DATE_SHIPPED,o.DATETIME_TRANSACTION, o.DATE_PLACED) >= '2017-06-01'
  AND oc.hvr_is_deleted = 0
  AND dcf.source_credit_id_type = 'store_credit_id'
  AND scc2.label IN ('Shipped', 'Complete')
  AND oc.amount > 0
  AND NOT EXISTS (SELECT 1
                 FROM edw_prod.reference.credit_historical_activity ha
                 WHERE ha.store_credit_id = dcf.credit_id
                   AND ha.activity_amount = oc.amount
                   AND ha.activity_type = 'Redeemed'
                   AND ha.redemption_order_id = oc.order_id)
  AND NOT EXISTS (SELECT 1
                 FROM lake_consolidated.ultra_merchant.store_credit_transaction sct
                 WHERE store_credit_transaction_type_id = 20
                   AND sct.object_id = o.order_id
                   AND sct.store_credit_id = oc.store_credit_id
                   AND sct.datetime_transaction IS NOT NULL)

UNION ALL
/* bounceback endowments */
SELECT DISTINCT
    dc.credit_key,
    dc.credit_id,
    dc.credit_id AS source_credit_id,
    IFF(be.administrator_id IS NOT NULL AND gctt.label = 'Create', be.administrator_id, -1) AS administrator_id,
     CONVERT_TIMEZONE(dsdc.store_time_zone, gct.datetime_transaction)::TIMESTAMP_NTZ        AS credit_activity_local_datetime,
    'Gift Certificate Transaction Log'                                                      AS credit_activity_source,
    'Activity Captured Gift Certificate Transaction Log'                                    AS credit_activity_source_reason,
    CASE
        WHEN gctt.label = 'Create' THEN 'Issued'
        WHEN gctt.label = 'Redeem' THEN 'Redeemed'
        WHEN gctt.label = 'Cancel' THEN 'Cancelled'
        WHEN gctt.label = 'Expire' THEN 'Expired'
        WHEN gctt.label = 'Transfer - Outbound' THEN 'Transferred'
        WHEN gctt.label = 'Transfer - Inbound' THEN 'Issued' -- transfer -inbound
        WHEN gctt.label = 'Convert To Store Credit' THEN 'Converted To Store Credit'
        END                                                                                AS credit_activity_type,
    COALESCE(gctr.label, 'Unknown')                                                        AS credit_activity_type_reason,
    SUM(CASE
           WHEN gctt.label IN ('Create', 'Redeem', 'Transfer - Inbound') THEN gct.amount
           WHEN gctt.label = 'Cancel' AND gct.datetime_transaction >= '2024-05-29 15:30:07.090' THEN gct.amount
           WHEN gctt.label IN ('Transfer - Outbound', 'Expire', 'Convert To Store Credit', 'Cancel')
               THEN gct.balance
           ELSE gct.amount
        END)                                                                               AS activity_amount,
    IFF(fo.order_status_key = 8, fo.master_order_id, fo.order_id)                          AS redemption_order_id,
    fo.order_id                                                                            AS source_redemption_order_id,
    fo.store_id                                                                            AS redemption_store_id,
    CASE
        WHEN st.store_country = 'CA' THEN 'CA'
        WHEN st.store_country = 'US' THEN 'US'
        WHEN a.country_code = 'GB' THEN 'UK'
        WHEN st.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
        WHEN st.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
        WHEN st.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
        WHEN st.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
        WHEN st.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
        WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
        WHEN st.store_country = 'EUREM' THEN 'NL'
        ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END              AS vat_rate_ship_to_country
FROM shared.dim_credit dc
     JOIN edw_prod.data_model.dim_store dsdc
        ON dsdc.store_id = dc.store_id
     JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction gct
        ON gct.gift_certificate_id = dc.credit_id
              AND dc.credit_type = 'Giftcard'
     JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction_type AS gctt
        ON gctt.gift_certificate_transaction_type_id = gct.gift_certificate_transaction_type_id
    LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction_reason AS gctr
        ON COALESCE(gctr.gift_certificate_transaction_type_id, -1) = COALESCE(gct.gift_certificate_transaction_type_id, -1)
    JOIN lake_consolidated.ultra_merchant.bounceback_endowment AS be
        ON be.object_id = gct.gift_certificate_id
        AND be.object = 'gift_certificate'
    LEFT JOIN edw_prod.stg.fact_order AS fo
        ON fo.order_id = gct.object_id
        AND gct.object = 'order'
    LEFT JOIN edw_prod.data_model.dim_store AS st
        ON st.store_id = fo.store_id
    LEFT JOIN lake_consolidated.ultra_merchant.address AS a
        ON a.address_id = fo.shipping_address_id
WHERE gct.datetime_transaction IS NOT NULL
    AND dc.source_credit_id_type = 'gift_certificate_id'
GROUP BY dc.credit_key,
         dc.credit_id,
         IFF(be.administrator_id IS NOT NULL AND gctt.label = 'Create', be.administrator_id, -1),
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         redemption_order_id,
         source_redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country

UNION ALL
--Gift Cards
SELECT DISTINCT
    dc.credit_key,
    dc.credit_id,
    dc.credit_id as source_credit_id,
    -1 AS administrator_id,
    CASE
        WHEN gctt.label = 'Create'
             AND CONVERT_TIMEZONE(dsdc.store_time_zone, gct.datetime_transaction)::DATE < fo.order_local_datetime::DATE
        THEN fo.order_local_datetime ::TIMESTAMP_NTZ
        ELSE CONVERT_TIMEZONE(dsdc.store_time_zone, gct.datetime_transaction)::TIMESTAMP_NTZ
    END                                                                                     AS credit_activity_local_datetime,
    'Gift Certificate Transaction Log'                                                      AS credit_activity_source,
    'Activity Captured Gift Certificate Transaction Log'                                    AS credit_activity_source_reason,
    CASE
        WHEN gctt.label = 'Create' THEN 'Issued'
        WHEN gctt.label = 'Redeem' THEN 'Redeemed'
        WHEN gctt.label = 'Cancel' THEN 'Cancelled'
        WHEN gctt.label = 'Expire' THEN 'Expired'
        WHEN gctt.label = 'Transfer - Outbound' THEN 'Transferred'
        WHEN gctt.label = 'Transfer - Inbound' THEN 'Issued' -- transfer -inbound
        WHEN gctt.label = 'Convert To Store Credit' THEN 'Converted To Store Credit'
        END                                                                                AS credit_activity_type,
    COALESCE(gctr.label, 'Gift Card Purchase')                                                        AS credit_activity_type_reason,
    SUM(CASE
           WHEN gctt.label IN ('Create', 'Redeem', 'Transfer - Inbound') THEN gct.amount
           WHEN gctt.label = 'Cancel' AND gct.datetime_transaction >= '2024-05-29 15:30:07.090' THEN gct.amount
           WHEN gctt.label IN ('Transfer - Outbound', 'Expire', 'Convert To Store Credit', 'Cancel')
               THEN gct.balance
           ELSE gct.amount
        END)                                                                               AS activity_amount,
    CASE
           WHEN fo2.order_status_key = 8 AND gct.gift_certificate_transaction_type_id = 20 THEN fo2.master_order_id
           WHEN gct.gift_certificate_transaction_type_id = 20 THEN fo2.order_id
           END                           AS redemption_order_id,
    IFF(gct.gift_certificate_transaction_type_id = 20, fo2.order_id, NULL)                  AS source_redemption_order_id,
    fo2.store_id                                                                            AS redemption_store_id,
    CASE
        WHEN st.store_country = 'CA' THEN 'CA'
        WHEN st.store_country = 'US' THEN 'US'
        WHEN a.country_code = 'GB' THEN 'UK'
        WHEN st.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
        WHEN st.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
        WHEN st.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
        WHEN st.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
        WHEN st.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
        WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
        WHEN st.store_country = 'EUREM' THEN 'NL'
        ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END              AS vat_rate_ship_to_country
FROM shared.dim_credit dc
         JOIN lake_consolidated.ultra_merchant.gift_certificate gc
              ON dc.credit_id = gc.gift_certificate_id
         JOIN edw_prod.data_model.dim_store dsdc
              ON dsdc.store_id = dc.store_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction gct
              ON gct.gift_certificate_id = dc.credit_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction_type AS gctt
              ON gctt.gift_certificate_transaction_type_id = gct.gift_certificate_transaction_type_id
         LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction_reason AS gctr
                   ON COALESCE(gctr.gift_certificate_transaction_type_id, -1) =
                      COALESCE(gct.gift_certificate_transaction_type_id, -1)
         LEFT JOIN edw_prod.stg.fact_order AS fo
                   ON fo.order_id = gc.order_id
         LEFT JOIN edw_prod.data_model.dim_store AS st
                   ON st.store_id = fo.store_id
         LEFT JOIN lake_consolidated.ultra_merchant.address AS a
                   ON a.address_id = fo.shipping_address_id
         LEFT JOIN edw_prod.stg.fact_order AS fo2
                   ON gct.object_id = fo2.order_id
WHERE gct.datetime_transaction IS NOT NULL
AND dc.source_credit_id_type = 'gift_certificate_id'
AND gc.gift_certificate_type_id != 9
AND gct.hvr_is_deleted = 0
  --Filter out any Transfer activity for gift certificate to gift certificate transactions through Giftco
  AND NOT (
    gctt.gift_certificate_transaction_type_id IN (50, 55)
    AND dc.credit_id IN (
        SELECT DISTINCT source_reference_number
        FROM lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
        WHERE gift_certificate_id IS NOT NULL
    )
)
  AND gc.order_id IS NOT NULL
GROUP BY dc.credit_key,
         dc.credit_id,
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         redemption_order_id,
         source_redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country

UNION ALL
--Everything from OC (Parent) Not in GCT
SELECT DISTINCT
    dc.credit_key,
    dc.credit_id,
    dc.credit_id as source_credit_id,
    -1 AS administrator_id,
     CONVERT_TIMEZONE(dsdc.store_time_zone, fo.order_local_datetime)::TIMESTAMP_NTZ        AS credit_activity_local_datetime,
    'Missing GCT - Successful Order'                                                      AS credit_activity_source,
    'successful in order credit'                                    AS credit_activity_source_reason,
    'Redeemed'                                                                               AS credit_activity_type,
    'Redeemed Gift Card'                                                        AS credit_activity_type_reason,
    SUM(oc.amount)                                                                              AS activity_amount,
    IFF(fo.order_status_key = 8, fo.master_order_id, fo.order_id)                           AS redemption_order_id,
    fo.order_id                  AS source_redemption_order_id,
    fo.store_id                                                                            AS redemption_store_id,
    CASE
        WHEN st.store_country = 'CA' THEN 'CA'
        WHEN st.store_country = 'US' THEN 'US'
        WHEN a.country_code = 'GB' THEN 'UK'
        WHEN st.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
        WHEN st.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
        WHEN st.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
        WHEN st.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
        WHEN st.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
        WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
        WHEN st.store_country = 'EUREM' THEN 'NL'
        ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END              AS vat_rate_ship_to_country
FROM shared.dim_credit dc
    JOIN lake_consolidated.ultra_merchant.order_credit AS oc
        ON dc.credit_id = oc.gift_certificate_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate gc
              ON dc.credit_id = gc.gift_certificate_id
         JOIN lake_consolidated.ultra_merchant."ORDER" AS o
                ON gc.order_id = o.order_id
             JOIN edw_prod.data_model.dim_store dsdc
              ON dsdc.store_id = o.store_id
         JOIN edw_prod.stg.fact_order AS fo
                   ON fo.order_id = oc.order_id
         LEFT JOIN edw_prod.data_model.dim_store AS st
                   ON st.store_id = fo.store_id
         LEFT JOIN lake_consolidated.ultra_merchant.address AS a
                   ON a.address_id = fo.shipping_address_id
WHERE dc.source_credit_id_type = 'gift_certificate_id'
  AND NOT EXISTS(SELECT 1
                 FROM lake_consolidated.ultra_merchant.gift_certificate_transaction AS gct
                 WHERE gct.gift_certificate_transaction_type_id = 20
                   AND gct.object_id = fo.order_id
                   AND gct.gift_certificate_id = dc.credit_id
                   AND gct.datetime_transaction IS NOT NULL
                   AND gct.hvr_is_deleted = 0)
 AND oc.hvr_is_deleted = 0
GROUP BY dc.credit_key,
         dc.credit_id,
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         redemption_order_id,
         source_redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country

UNION ALL
--managed gc
    SELECT DISTINCT
    dc.credit_key,
    dc.credit_id,
    gc2.gift_certificate_id AS source_credit_id,
    -1 AS administrator_id,
     CONVERT_TIMEZONE(dsdc.store_time_zone,
                        fo.order_local_datetime)::TIMESTAMP_NTZ        AS credit_activity_local_datetime,
    'Managed Gift Card - Successful Order'                                                      AS credit_activity_source,
    'successful in order credit'                                    AS credit_activity_source_reason,
    'Redeemed'                                                                               AS credit_activity_type,
    'Redeemed Managed Gift Card'                                                        AS credit_activity_type_reason,
    SUM(oc.amount)                                                                               AS activity_amount,
    IFF(fo.order_status_key = 8, fo.master_order_id, fo.order_id)                             AS redemption_order_id,
    fo.order_id                  AS source_redemption_order_id,
    fo.store_id                                                                            AS redemption_store_id,
    CASE
        WHEN st.store_country = 'CA' THEN 'CA'
        WHEN st.store_country = 'US' THEN 'US'
        WHEN a.country_code = 'GB' THEN 'UK'
        WHEN st.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
        WHEN st.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
        WHEN st.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
        WHEN st.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
        WHEN st.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
        WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
        WHEN st.store_country = 'EUREM' THEN 'NL'
        ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END              AS vat_rate_ship_to_country
FROM shared.dim_credit dc
    JOIN lake_consolidated.ultra_merchant.gift_certificate gc
        on dc.credit_id = gc.gift_certificate_id
    JOIN lake_consolidated.ultra_merchant."ORDER" o
        ON gc.order_id = o.order_id
    JOIN edw_prod.stg.dim_store dsdc
        ON dsdc.store_id = o.store_id
    JOIN lake_consolidated.ultra_merchant.managed_gift_certificate mgc
        ON gc.gift_certificate_id = mgc.gift_certificate_id
    JOIN (SELECT DISTINCT gift_certificate_id, code
          FROM lake_consolidated.ultra_merchant.credit_transfer_transaction
          WHERE credit_transfer_transaction_type_id IN (250, 260)) AS ctt
        ON ctt.code = COALESCE(mgc.code, gc.code)
    JOIN lake_consolidated.ultra_merchant.gift_certificate gc2
        ON ctt.gift_certificate_id = gc2.gift_certificate_id
    JOIN lake_consolidated.ultra_merchant.order_credit oc
        ON oc.gift_certificate_id = gc2.gift_certificate_id
    JOIN edw_prod.stg.fact_order AS fo
        ON fo.order_id = oc.order_id
    LEFT JOIN edw_prod.data_model.dim_store AS st
        ON st.store_id = fo.store_id
    LEFT JOIN lake_consolidated.ultra_merchant.address AS a
        ON a.address_id = fo.shipping_address_id
WHERE
    dc.source_credit_id_type = 'gift_certificate_id'
    AND oc.hvr_is_deleted = 0
GROUP BY dc.credit_key,
         dc.credit_id,
         gc2.gift_certificate_id,
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         redemption_order_id,
         source_redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country

UNION ALL
/* We converted a batch of variable credits to token that are not receiving a "Converted to Token" activity so we are creating one */
SELECT DISTINCT
    dco.credit_key,
    dco.credit_id,
    dco.credit_id AS source_credit_id,
    sctc.administrator_id,
    CONVERT_TIMEZONE(st.store_time_zone, sctc.datetime_added)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
    'Store Credit Token Conversion Log' AS credit_activity_source,
    'Activity Captured Store Credit Token Conversion Log' AS credit_activity_source_reason,
    'Converted To Token' AS credit_activity_type,
    'Converted to Variable To Token' AS credity_activity_type_reason,
    sc.balance AS activity_amount,
    NULL AS redemption_order_id,
    NULL AS source_redemption_order_id,
    NULL AS redemption_store_id,
    IFF(st.store_country = 'EUREM','NL', st.store_country) AS vat_rate_ship_to_country
FROM shared.dim_credit AS dc
    JOIN edw_prod.data_model.dim_store AS st
            ON st.store_id = dc.store_id
    JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion AS sctc
            ON sctc.converted_membership_token_id = dc.credit_id
    JOIN lake_consolidated.ultra_merchant.store_credit AS sc
            ON sc.store_credit_id = sctc.original_store_credit_id
    JOIN shared.dim_credit AS dco
            ON dco.credit_id = sc.store_credit_id
            AND dco.source_credit_id_type = 'store_credit_id'
WHERE dc.original_credit_match_reason = 'Converted To Variable To Token'
AND sctc.store_credit_conversion_type_id = 3 -- variable to token

UNION ALL
/* There was a cancellation bug in source. Explained in DA-28668 */
SELECT DISTINCT
    dc.credit_key,
    dc.credit_id,
    dc.credit_id AS source_credit_id,
    -1 AS administrator_id, /* setting as -1 because these cancellations were not captured in source */
    ccd.new_credit_cancellation_local_datetime::TIMESTAMP_NTZ AS credit_activity_local_datetime,
    'reference.credit_cancellation_datetime' AS credit_activity_source,
    'Uncaptured Credit Cancellations in Source' AS credit_activity_source_reason,
    'Cancelled' AS credit_activity_type,
    'Unknown' AS credity_activity_type_reason,
    ccd.amount,
    NULL AS redemption_order_id,
    NULL AS source_redemption_order_id,
    NULL AS redemption_store_id,
    IFF(st.store_country = 'EUREM','NL', st.store_country) AS vat_rate_ship_to_country
FROM shared.dim_credit AS dc
    JOIN edw_prod.reference.credit_cancellation_datetime AS ccd
        ON ccd.credit_id = dc.credit_id
        AND ccd.source_credit_id_type = dc.source_credit_id_type
    JOIN edw_prod.stg.dim_store AS st
        ON st.store_id = dc.store_id
WHERE ccd.is_ignore = FALSE;

/* Bounceback endowments - a customer can redeem just a portion of the endowment - but any unused balance would be immediately expired.
   Source tables do not record an expired transaction record, only redemption.
   This next section of code is creating a an expired transaction record for partially redeemed bb endowments to fully close out the credit */
INSERT INTO _fact_credit_event
(credit_key,
 credit_id,
 source_credit_id,
 administrator_id,
 credit_activity_local_datetime,
 credit_activity_source,
 credit_activity_source_reason,
 credit_activity_type,
 credit_activity_type_reason,
 activity_amount,
 redemption_order_id,
 source_redemption_order_id,
 redemption_store_id,
 vat_rate_ship_to_country)

SELECT DISTINCT
    dc.credit_key,
    dc.credit_id,
    dc.credit_id AS source_credit_id,
    fce.administrator_id,
    DATEADD(MILLISECOND, 1, fce.credit_activity_local_datetime) AS credit_acivity_local_datetime,
    'Missing GCT - Expired BB Endowment' AS credit_activity_source,
    'Uncaptured Expired BB Endowment in Source' AS credit_activity_source_reason,
    'Expired' AS credit_activity_type,
    'Unused BB Endowment' AS credit_activity_type_reason,
    IFNULL(gc.balance, 0) AS activity_amount,
    fce.redemption_order_id,
    fce.source_redemption_order_id,
    fce.redemption_store_id,
    fce.vat_rate_ship_to_country
FROM shared.dim_credit AS dc
    JOIN lake_consolidated.ultra_merchant.bounceback_endowment AS be
        ON be.object_id = dc.credit_id
        AND be.object = 'gift_certificate'
    JOIN _fact_credit_event AS fce
        ON fce.credit_key = dc.credit_key
        AND fce.credit_activity_type = 'Redeemed'
    JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
        ON gc.gift_certificate_id = dc.credit_id
WHERE
    dc.source_credit_id_type = 'gift_certificate_id'
    AND gc.statuscode = 3239
    AND gc.balance > 0;

-- Gift Certificates activity before gift_certificate_transaction table prior to 4/2017
INSERT INTO _fact_credit_event (credit_key,
 credit_id,
 source_credit_id,
 administrator_id,
 credit_activity_local_datetime,
 credit_activity_source,
 credit_activity_source_reason,
 credit_activity_type,
 credit_activity_type_reason,
 activity_amount,
 redemption_order_id,
 source_redemption_order_id,
 redemption_store_id,
 vat_rate_ship_to_country)

SELECT DISTINCT base.credit_key,
       base.credit_id,
       base.credit_id                                                                                                AS source_credit_id,
       NULL                                                                                                          AS administrator_id,
       CONVERT_TIMEZONE('America/Los_Angeles', stz.time_zone,
                        o.datetime_added)                                                                                                             AS credit_activity_local_datetime,
       IFF(credit_activity_local_datetime < '2017-05-01', 'Missing GCT - Before 2017',
           'Missing GCT - Check Source')                                                                             AS credit_activity_source,
       IFF(credit_activity_local_datetime < '2017-05-01', 'Historical Activity - Gift_Certificate',
           'Source Activity - Gift Certificate')                                                                     AS credit_activity_source_reason,
       'Issued'                                                                                                      AS credit_activity_type,
       'Issued'                                                                                                      AS credit_activity_type_reason,
       SUM(CASE
               WHEN sc.label IN ('Created', 'Redeemed', 'Transfered', 'Sent') THEN gc.amount
               WHEN sc.label IN ('Transfer - Outbound', 'Expire', 'Convert To Store Credit', 'Cancel')
                   THEN gc.balance
               ELSE gc.amount
           END)                                                                                                      AS activity_amount,
       NULL                                                                                                          AS redemption_order_id,
       NULL                                                                                                          AS source_redemption_order_id,
       NULL                                                                                                          AS redemption_store_id,
       CASE
           WHEN st.store_country = 'CA' THEN 'CA'
           WHEN st.store_country = 'US' THEN 'US'
           WHEN a.country_code = 'GB' THEN 'UK'
           WHEN st.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
           WHEN st.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
           WHEN st.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
           WHEN st.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
           WHEN st.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
           WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
           WHEN st.store_country = 'EUREM' THEN 'NL'
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END                                      AS vat_rate_ship_to_country
FROM shared.dim_credit AS base
         JOIN edw_prod.stg.dim_store dsdc
              ON dsdc.store_id = base.store_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
              ON gc.gift_certificate_id = base.credit_id
         JOIN lake_consolidated.ultra_merchant.statuscode AS sc
              ON gc.statuscode = sc.statuscode
         LEFT JOIN edw_prod.stg.dim_store AS st
                   ON base.store_id = st.store_id
         LEFT JOIN lake_consolidated.ultra_merchant."ORDER" o
                   ON gc.order_id = o.order_id
         LEFT JOIN lake_consolidated.ultra_merchant.address a
                   ON o.shipping_address_id = a.address_id
         LEFT JOIN edw_prod.reference.store_timezone stz
                   ON base.store_id = stz.store_id
WHERE base.source_credit_id_type = 'gift_certificate_id'
  AND NOT EXISTS (SELECT 1
                  FROM _fact_credit_event fe
                  WHERE fe.credit_activity_type = 'Issued'
                    AND fe.credit_key = base.credit_key)
  AND NOT EXISTS(SELECT 1
                 FROM lake_consolidated.ultra_merchant.gift_certificate_transaction gct
                 WHERE gift_certificate_transaction_type_id = 10
                   AND gct.gift_certificate_id = gc.gift_certificate_id)
GROUP BY base.credit_key,
         base.credit_id,
         base.credit_id,
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         vat_rate_ship_to_country
UNION ALL
SELECT DISTINCT base.credit_key,
       base.credit_id,
       base.credit_id                                                           AS source_credit_id,
       NULL                                                                     AS administrator_id,
       CONVERT_TIMEZONE('America/Los_Angeles', stz.time_zone, gc.datetime_modified)       AS credit_activity_local_datetime,
       IFF(credit_activity_local_datetime < '2017-05-01', 'Missing GCT - Before 2017',
           'Missing GCT - Check Source')                                              AS credit_activity_source,
       IFF(credit_activity_local_datetime < '2017-05-01', 'Historical Activity - Gift_Certificate',
           'Source Activity - Gift Certificate')                                 AS credit_activity_source_reason,
       'Cancelled'                               AS credit_activity_type,
       'Cancelled'                                 AS credit_activity_type_reason,
       SUM(CASE
               WHEN sc.label IN ('Created', 'Redeemed', 'Transfered', 'Sent') THEN gc.amount
               WHEN sc.label IN ('Transfer - Outbound', 'Expire', 'Convert To Store Credit', 'Cancel')
                   THEN gc.balance
               ELSE gc.amount
           END)                                                                 AS activity_amount,
       NULL                                                                     AS redemption_order_id,
       NULL                                                                     AS source_redemption_order_id,
       NULL                                                                     AS redemption_store_id,
       CASE
           WHEN st.store_country = 'CA' THEN 'CA'
           WHEN st.store_country = 'US' THEN 'US'
           WHEN a.country_code = 'GB' THEN 'UK'
           WHEN st.store_country = 'DE' AND a.country_code = 'AT' THEN 'AT'
           WHEN st.store_country = 'UK' AND a.country_code = 'IE' THEN 'IE'
           WHEN st.store_country = 'FR' AND a.country_code = 'BE' THEN 'BE'
           WHEN st.store_country = 'NL' AND a.country_code = 'BE' THEN 'BE'
           WHEN st.store_country = 'SE' AND a.country_code = 'PL' THEN 'PL'
           WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE') THEN a.country_code
           WHEN st.store_country = 'EUREM' THEN 'NL'
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END AS vat_rate_ship_to_country
FROM shared.dim_credit AS base
         JOIN edw_prod.stg.dim_store dsdc
              ON dsdc.store_id = base.store_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
              ON gc.gift_certificate_id = base.credit_id
         JOIN lake_consolidated.ultra_merchant.statuscode AS sc
              ON gc.statuscode = sc.statuscode
         LEFT JOIN edw_prod.stg.dim_store AS st
                   ON base.store_id = st.store_id
         LEFT JOIN lake_consolidated.ultra_merchant."ORDER" o
                   ON gc.order_id = o.order_id
         LEFT JOIN lake_consolidated.ultra_merchant.address a
                   ON o.shipping_address_id = a.address_id
         LEFT JOIN edw_prod.reference.store_timezone stz
                   ON base.store_id = stz.store_id
WHERE base.source_credit_id_type = 'gift_certificate_id'
  AND sc.label = 'Cancelled'
  AND NOT EXISTS (SELECT 1
                  FROM _fact_credit_event fe
                  WHERE fe.credit_activity_type = credit_activity_type
                    AND fe.credit_key = base.credit_key)
  AND NOT EXISTS(SELECT 1
                 FROM lake_consolidated.ultra_merchant.gift_certificate_transaction gct
                 WHERE gift_certificate_transaction_type_id = 30
                   AND gct.gift_certificate_id = gc.gift_certificate_id)
GROUP BY base.credit_key,
         base.credit_id,
         base.credit_id,
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         vat_rate_ship_to_country
;
------------------------------------------------------------------------
ALTER TABLE _fact_credit_event ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE;

/* we're going to delete credits wrongfully converted to token (DA-28668) */
UPDATE _fact_credit_event AS t
SET t.is_deleted = TRUE
FROM (
        SELECT fce.credit_key
        FROM _fact_credit_event AS fce
            JOIN shared.dim_credit AS dc
                ON dc.credit_key = fce.credit_key
            JOIN edw_prod.reference.credit_cancellation_datetime AS ccd
                ON ccd.credit_id = dc.credit_id
                AND ccd.source_credit_id_type = dc.source_credit_id_type
        WHERE ccd.is_ignore = TRUE
) AS s
WHERE t.credit_key = s.credit_key;

/* we're going to delete credit events that occurred
   after their new/corrected cancellation date (DA-28668) */

UPDATE _fact_credit_event AS t
SET t.is_deleted = TRUE
FROM (
        SELECT fce.credit_key, fce.credit_activity_type, fce.credit_activity_local_datetime
        FROM _fact_credit_event AS fce
            JOIN shared.dim_credit AS dc
                ON dc.credit_key = fce.credit_key
            JOIN edw_prod.reference.credit_cancellation_datetime AS ccd
                ON ccd.credit_id = dc.credit_id
                AND ccd.source_credit_id_type = dc.source_credit_id_type
        WHERE fce.credit_activity_local_datetime > ccd.new_credit_cancellation_local_datetime::TIMESTAMP_NTZ
) AS s
WHERE t.credit_key = s.credit_key
      AND t.credit_activity_local_datetime = s.credit_activity_local_datetime
      AND t.credit_activity_type = s.credit_activity_type;
------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _fact_credit_event_final AS
SELECT fce.credit_key,
       fce.credit_id,
       fce.source_credit_id,
       fce.administrator_id,
       credit_activity_local_datetime,
       credit_activity_source,
       credit_activity_source_reason,
       credit_activity_type,
       CASE
           WHEN credit_activity_type = 'Issued'
               AND credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion', 'Token To Credit Conversion', 'Gift Certificate')
               THEN 'Exclude'
           WHEN credit_activity_type IN ('Converted To Token', 'Converted To Variable', 'Transferred',
                                         'Converted To Store Credit', 'Converted To Credit') THEN 'Exclude'
           ELSE 'Include' END AS original_credit_activity_type_action,
       credit_activity_type_reason,
       activity_amount        AS activity_amount,
       redemption_order_id,
       source_redemption_order_id,
       redemption_store_id,
       vat_rate_ship_to_country
FROM _fact_credit_event AS fce
         JOIN shared.dim_credit dcf ON dcf.credit_key = fce.credit_key
WHERE fce.is_deleted = FALSE;

------------------------------------------------------------------------
-- putting exchange rates to convert source currency to USD by day into a table
-- doing in a temp table because when trying to do in the final join, the query was taking 30+ minutes
CREATE OR REPLACE TEMPORARY TABLE _exchange_rate AS
SELECT src_currency,
       dd.full_date,
       er.exchange_rate
FROM edw_prod.reference.currency_exchange_rate er
         JOIN edw_prod.data_model.dim_date dd ON er.effective_start_datetime <= dd.full_date
    AND er.effective_end_datetime > dd.full_date
WHERE dest_currency = 'USD'
  AND dd.full_date < CURRENT_DATE();

-- putting vat rates into a temp table
CREATE OR REPLACE TEMPORARY TABLE _vat_rate AS
SELECT REPLACE(country_code, 'GB', 'UK') AS country_code,
       dd.full_date,
       er.rate
FROM edw_prod.reference.vat_rate_history er
         JOIN edw_prod.data_model.dim_date dd ON er.start_date <= dd.full_date
    AND er.expires_date >= dd.full_date
WHERE dd.full_date < CURRENT_DATE() + 7;

/* Getting the value of a credit at the time of issuance to use to calculate equivalent counts */

CREATE OR REPLACE TEMP TABLE _fact_credit_event__membership_price AS
SELECT DISTINCT
    base.credit_key,
    dc.original_credit_issued_local_datetime,
    dc.store_id,
    dc.customer_id,
    m.price
FROM _fact_credit_event AS base
    JOIN shared.dim_credit AS dc
        ON dc.credit_key = base.credit_key
    LEFT JOIN lake_consolidated.ultra_merchant_history.membership AS m
        ON m.customer_id = dc.customer_id
        AND IFF(m.store_id = 41, 26, m.store_id) = dc.store_id
        AND CONVERT_TIMEZONE('America/Los_Angeles',dc.original_credit_issued_local_datetime) BETWEEN m.effective_start_datetime AND m.effective_end_datetime;
-- SELECT * FROM lake_consolidated.ultra_merchant_history.membership WHERE customer_id = 752198632;

CREATE OR REPLACE TEMP TABLE _fact_credit_event__membership_price_null AS
SELECT DISTINCT mp.credit_key,
                m.price,
                ABS(DATEDIFF('millisecond',
                             CONVERT_TIMEZONE('America/Los_Angeles', mp.original_credit_issued_local_datetime),
                             m.effective_start_datetime))                       AS date_diff,
                RANK() OVER (PARTITION BY mp.credit_key ORDER BY date_diff) AS closest_membership_date
FROM _fact_credit_event__membership_price AS mp
         JOIN lake_consolidated.ultra_merchant_history.membership AS m
              ON m.customer_id = mp.customer_id
                  AND IFF(m.store_id = 41, 26, m.store_id) = mp.store_id
WHERE mp.price IS NULL
    QUALIFY closest_membership_date = 1;


UPDATE _fact_credit_event__membership_price mp
SET mp.price = m.price
FROM _fact_credit_event__membership_price_null m
WHERE mp.credit_key = m.credit_key;


CREATE OR REPLACE TEMPORARY TABLE _credit_issued_vat_country AS
SELECT DISTINCT credit_key, IFNULL(vrh.rate, 0) AS credit_issuance_vat_rate
FROM _fact_credit_event_final fce
         LEFT JOIN _vat_rate vrh -- join so you can get the vat rate
                   ON vrh.country_code = fce.vat_rate_ship_to_country
                       AND vrh.full_date = CAST(fce.credit_activity_local_datetime AS DATE)
WHERE credit_key IN (SELECT credit_key
                     FROM _fact_credit_event_final
                     GROUP BY credit_key
                     HAVING COUNT(DISTINCT vat_rate_ship_to_country) > 1)
  AND credit_activity_type = 'Issued';

------------------------------------------------------------------------
/*  create FACT_CREDIT_EVENT_FINAL TABLE  */

CREATE OR REPLACE TEMPORARY TABLE _ready_to_output AS
SELECT fce.credit_key,
       fce.credit_id,
       fce.source_credit_id,
       fce.administrator_id,
       fce.credit_activity_type,
       fce.original_credit_activity_type_action,
       fce.credit_activity_type_reason,
       fce.credit_activity_local_datetime,
       fce.credit_activity_source,
       fce.credit_activity_source_reason,
       fce.redemption_order_id,
       fce.source_redemption_order_id,
       IFF(fce.redemption_store_id = 41, 26, fce.redemption_store_id) AS redemption_store_id,
       fce.vat_rate_ship_to_country,
       IFNULL(vrh.rate, 0)                                                                         AS credit_activity_vat_rate,
       exch.exchange_rate                                                                          AS credit_activity_usd_conversion_rate,
       IFF(dc.credit_type = 'Token', 1, IFNULL(fce.activity_amount / NULLIF(m.price,0),1))         AS credit_activity_equivalent_count,
       fce.activity_amount                                                                         AS credit_activity_gross_vat_local_amount,
       fce.activity_amount / (1 + IFNULL(vrh.rate, 0))                                             AS credit_activity_local_amount,
       fce.activity_amount / (1 + IFNULL(vrh2.rate, 0))                                            AS activity_amount_local_amount_issuance_date,
       IFNULL(fce.activity_amount / (1 + cv.credit_issuance_vat_rate), fce.activity_amount /
                                                                       (1 + IFNULL(vrh2.rate, 0))) AS credit_activity_local_amount_issuance_vat,
       CURRENT_TIMESTAMP()                                                                         AS meta_create_datetime,
       CURRENT_TIMESTAMP()                                                                         AS meta_update_datetime
FROM _fact_credit_event_final fce
         JOIN shared.dim_credit dc ON dc.credit_key = fce.credit_key
         JOIN edw_prod.data_model.dim_store st ON st.store_id = dc.store_id
         LEFT JOIN _fact_credit_event__membership_price AS m
                   ON m.credit_key = fce.credit_key
         LEFT JOIN _exchange_rate exch
                   ON exch.src_currency = st.store_currency -- add in exchange rate to convert to USD
                       AND CAST(fce.credit_activity_local_datetime AS DATE) = full_date
         LEFT JOIN _vat_rate vrh -- join so you can get the vat rate
                   ON vrh.country_code = fce.vat_rate_ship_to_country
                       AND vrh.full_date = CAST(fce.credit_activity_local_datetime AS DATE)
         LEFT JOIN _vat_rate vrh2 -- join so you can get the vat rate
                   ON vrh2.country_code = fce.vat_rate_ship_to_country
                       AND vrh2.full_date = CAST(dc.credit_issued_hq_datetime AS DATE)
         LEFT JOIN _credit_issued_vat_country AS cv
                   ON cv.credit_key = fce.credit_key;



TRUNCATE TABLE shared.fact_credit_event;

INSERT INTO shared.fact_credit_event (
     credit_key,
     source_credit_id,
     administrator_id,
     credit_activity_type,
     original_credit_activity_type_action,
     credit_activity_type_reason,
     credit_activity_local_datetime,
     credit_activity_source,
     credit_activity_source_reason,
     redemption_order_id,
     source_redemption_order_id,
     redemption_store_id,
     vat_rate_ship_to_country,
     credit_activity_vat_rate,
     credit_activity_usd_conversion_rate,
     credit_activity_equivalent_count,
     credit_activity_gross_vat_local_amount,
     credit_activity_local_amount,
     activity_amount_local_amount_issuance_date,
     credit_activity_local_amount_issuance_vat,
     meta_create_datetime,
     meta_update_datetime,
     activation_key,
     is_bop_vip
)
SELECT r.credit_key,
       r.source_credit_id,
       r.administrator_id,
       r.credit_activity_type,
       r.original_credit_activity_type_action,
       r.credit_activity_type_reason,
       r.credit_activity_local_datetime,
       r.credit_activity_source,
       r.credit_activity_source_reason,
       r.redemption_order_id,
       r.source_redemption_order_id,
       r.redemption_store_id,
       r.vat_rate_ship_to_country,
       r.credit_activity_vat_rate,
       r.credit_activity_usd_conversion_rate,
       r.credit_activity_equivalent_count,
       r.credit_activity_gross_vat_local_amount,
       r.credit_activity_local_amount,
       r.activity_amount_local_amount_issuance_date,
       r.credit_activity_local_amount_issuance_vat,
       r.meta_create_datetime,
       r.meta_update_datetime,
       COALESCE(fa.activation_key, -1)                                            AS activation_key,
       IFF(DATE_TRUNC(MONTH, CAST(r.credit_activity_local_datetime AS DATE)) >
           DATE_TRUNC(MONTH, CAST(fa.activation_local_datetime AS DATE)) AND
           DATE_TRUNC(MONTH, CAST(r.credit_activity_local_datetime AS DATE)) <=
           DATE_TRUNC(MONTH, CAST(fa.cancellation_local_datetime AS DATE)), 1, 0) AS is_bop_vip
FROM _ready_to_output r
         JOIN shared.dim_credit dc ON dc.credit_key = r.credit_key
         LEFT JOIN edw_prod.data_model.fact_activation fa
             ON fa.customer_id = dc.customer_id
                AND fa.source_activation_local_datetime <= r.credit_activity_local_datetime
                AND fa.source_next_activation_local_datetime > r.credit_activity_local_datetime;

ALTER TABLE shared.fact_credit_event SET DATA_RETENTION_TIME_IN_DAYS = 0;
