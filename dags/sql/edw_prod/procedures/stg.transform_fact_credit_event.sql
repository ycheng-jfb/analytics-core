SET target_table = 'stg.fact_credit_event';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

-- Switch warehouse if performing a full refresh
SET warehouse_to_be_used = IFF($is_full_refresh, 'da_wh_adhoc_large', current_warehouse());
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

/*
-- Initial load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_lake_ultra_merchant_membership_token = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_token'));
SET wm_lake_ultra_merchant_membership_token_transaction = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.membership_token_transaction'));
SET wm_lake_ultra_merchant_order = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant."ORDER"'));
SET wm_lake_ultra_merchant_address = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.address'));
SET wm_lake_ultra_merchant_store_credit_transaction = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_credit_transaction'));
SET wm_lake_ultra_merchant_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.store_credit'));
SET wm_lake_ultra_merchant_order_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_credit'));
SET wm_lake_ultra_merchant_order_tracking_detail = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.order_tracking_detail'));
SET wm_lake_ultra_merchant_customer = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.customer'));
SET wm_lake_ultra_merchant_gift_certificate = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_certificate'));
SET wm_lake_ultra_merchant_gift_certificate_transaction = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_certificate_transaction'));
SET wm_lake_ultra_merchant_bounceback_endowment = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.bounceback_endowment'));
SET wm_reference_credit_historical_activity = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.credit_historical_activity'));
SET wm_dim_credit = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.dim_credit'));
SET wm_fact_order = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order'));
SET wm_fact_activation = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_activation'));
SET wm_lake_history_ultra_merchant_membership = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant_history.membership'));
SET wm_reference_currency_exchange_rate = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.currency_exchange_rate'));
SET wm_lake_ultra_merchant_credit_transfer_transaction = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.credit_transfer_transaction'));
SET wm_lake_ultra_merchant_managed_gift_certificate = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.managed_gift_certificate'));
SET wm_lake_ultra_merchant_gift_certificate_store_credit = (SELECT stg.udf_get_watermark($target_table, 'lake_consolidated.ultra_merchant.gift_certificate_store_credit'));

/*
SELECT
    $wm_self,
    $wm_lake_ultra_merchant_membership_token,
    $wm_lake_ultra_merchant_membership_token_transaction,
    $wm_lake_ultra_merchant_order,
    $wm_lake_ultra_merchant_address,
    $wm_lake_ultra_merchant_store_credit_transaction,
    $wm_lake_ultra_merchant_store_credit,
    $wm_lake_ultra_merchant_order_credit,
    $wm_lake_ultra_merchant_order_tracking_detail,
    $wm_lake_ultra_merchant_customer,
    $wm_reference_credit_historical_activity,
    $wm_dim_credit,
    $wm_fact_activation;
*/

-- handle currency exchange watermark
CREATE OR REPLACE TEMP TABLE _currency_exchange_rate_watermark AS
SELECT er.*,
       CONVERT_TIMEZONE(dsdc.store_time_zone, dd.full_date)::DATE AS full_date,
       dsdc.store_id,
       dsdc.store_time_zone
FROM stg.dim_store dsdc
         JOIN reference.currency_exchange_rate er
              ON dsdc.store_currency = er.src_currency
         JOIN stg.dim_date dd
              ON er.effective_start_datetime <= dd.full_date
                  AND er.effective_end_datetime > dd.full_date
                  AND dd.full_date <= CURRENT_DATE()
WHERE dest_currency = 'USD'
  AND er.meta_update_datetime > $wm_reference_currency_exchange_rate
  AND NOT $is_full_refresh;

-- base table watermark
CREATE OR REPLACE TEMP TABLE _fact_credit_event__credit_base
(
    credit_id                 NUMBER(38, 0),
    credit_key                NUMBER(38, 0),
    credit_issuance_reason    VARCHAR(50),
    credit_issued_hq_datetime TIMESTAMP_TZ(9),
    credit_type               VARCHAR(50),
    store_id                  NUMBER(38, 0),
    source_credit_id_type     VARCHAR(50)
);

-- Full Refresh
INSERT INTO _fact_credit_event__credit_base (credit_id, credit_key, credit_issuance_reason, credit_issued_hq_datetime, credit_type, store_id, source_credit_id_type)
SELECT DISTINCT dc.credit_id, dc.credit_key, dc.credit_issuance_reason, dc.credit_issued_hq_datetime, dc.credit_type, dc.store_id, dc.source_credit_id_type
FROM stg.dim_credit AS dc
WHERE $is_full_refresh = TRUE
ORDER BY dc.credit_id;

-- Incremental Refresh
INSERT INTO _fact_credit_event__credit_base (credit_id, credit_key, credit_issuance_reason, credit_issued_hq_datetime, credit_type, store_id, source_credit_id_type)
SELECT DISTINCT incr.credit_id, incr.credit_key, incr.credit_issuance_reason, incr.credit_issued_hq_datetime, incr.credit_type, incr.store_id, incr.source_credit_id_type
FROM (
    /* Self-check for manual updates */
    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit AS dc
        JOIN stg.fact_credit_event AS fce
            ON dc.credit_key = fce.credit_key
    WHERE fce.meta_update_datetime > $wm_self

    UNION ALL

    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit dc
    WHERE meta_update_datetime > $wm_dim_credit

    UNION ALL

    SELECT dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit AS dc
        JOIN stg.fact_credit_event AS fce
            ON dc.credit_key = fce.credit_key
    WHERE fce.credit_activity_source = 'Missing SCT - Successful Order'

    UNION ALL

    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit dc
        LEFT JOIN lake_consolidated.ultra_merchant.membership_token mt
            ON mt.membership_token_id = dc.credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.membership_token_transaction mtt
            ON mtt.membership_token_id = dc.credit_id AND dc.credit_type = 'Token'
        LEFT JOIN lake_consolidated.ultra_merchant."ORDER" ord
            ON ord.order_id = mtt.object_id AND mtt.object = 'order'
        LEFT JOIN stg.fact_order AS fo
            ON fo.order_id = ord.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.address a
            ON a.address_id = ord.shipping_address_id
        LEFT JOIN _currency_exchange_rate_watermark cerw
            ON mtt.datetime_transaction::DATE = cerw.full_date
            AND dc.store_id = cerw.store_id
    WHERE (dc.meta_update_datetime > $wm_dim_credit
        OR mt.meta_update_datetime > $wm_lake_ultra_merchant_membership_token
        OR mtt.meta_update_datetime > $wm_lake_ultra_merchant_membership_token_transaction
        OR ord.meta_update_datetime > $wm_lake_ultra_merchant_order
        OR a.meta_update_datetime > $wm_lake_ultra_merchant_address
        OR fo.meta_update_datetime > $wm_fact_order
        OR cerw.meta_update_datetime > $wm_reference_currency_exchange_rate)
        AND dc.source_credit_id_type = 'Token'

    UNION ALL

    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit dc
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit_transaction sct
            ON sct.store_credit_id = dc.credit_id AND dc.credit_type ILIKE '%credit%'
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit sc
            ON sc.store_credit_id = sct.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.customer c
            ON c.customer_id = sc.customer_id
        LEFT JOIN _currency_exchange_rate_watermark cerw
            ON sct.datetime_transaction::DATE = cerw.full_date
            AND dc.store_id = cerw.store_id
    WHERE (sct.meta_update_datetime > $wm_lake_ultra_merchant_store_credit_transaction
        OR sc.meta_update_datetime > $wm_lake_ultra_merchant_store_credit
        OR c.meta_update_datetime > $wm_lake_ultra_merchant_customer
        OR cerw.meta_update_datetime > $wm_reference_currency_exchange_rate)
        AND dc.source_credit_id_type = 'store_credit_id'

    UNION ALL

    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit dc
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit_transaction sct
            ON sct.store_credit_id = dc.credit_id
        LEFT JOIN lake_consolidated.ultra_merchant."ORDER" ord
            ON ord.order_id = sct.object_id AND sct.object = 'order'
        LEFT JOIN stg.fact_order AS fo
            ON fo.order_id = ord.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.address a
            ON a.address_id = ord.shipping_address_id
        LEFT JOIN _currency_exchange_rate_watermark cerw
            ON sct.datetime_transaction::DATE = cerw.full_date
            AND dc.store_id = cerw.store_id
    WHERE (sct.meta_update_datetime > $wm_lake_ultra_merchant_store_credit_transaction
        OR ord.meta_update_datetime > $wm_lake_ultra_merchant_order
        OR a.meta_update_datetime > $wm_lake_ultra_merchant_address
        OR fo.meta_update_datetime > $wm_fact_order
        OR cerw.meta_update_datetime > $wm_reference_currency_exchange_rate)
        AND dc.source_credit_id_type = 'store_credit_id'

    UNION ALL

    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit dc
        JOIN reference.credit_historical_activity h
            ON h.store_credit_id = dc.credit_id AND dc.credit_type ILIKE '%credit%'
        LEFT JOIN _currency_exchange_rate_watermark cerw
            ON h.activity_datetime::DATE = cerw.full_date
            AND dc.store_id = cerw.store_id
    WHERE h.meta_update_datetime > $wm_reference_credit_historical_activity
        OR cerw.meta_update_datetime > $wm_reference_currency_exchange_rate

    UNION ALL

    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit dc
        LEFT JOIN lake_consolidated.ultra_merchant.order_credit oc
            ON oc.store_credit_id = dc.credit_id
        LEFT JOIN lake_consolidated.ultra_merchant."ORDER" o
            ON o.order_id = oc.order_id
        LEFT JOIN stg.fact_order AS fo
            ON fo.order_id = o.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.statuscode scc
            ON scc.statuscode = o.payment_statuscode
        LEFT JOIN lake_consolidated.ultra_merchant.store_credit sc
            ON sc.store_credit_id = oc.store_credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.address a
            ON a.address_id = o.shipping_address_id
        LEFT JOIN lake_consolidated.ultra_merchant.order_tracking_detail otd
            ON otd.order_tracking_id = o.order_tracking_id
        LEFT JOIN _currency_exchange_rate_watermark cerw
            ON COALESCE(o.date_shipped, o.datetime_transaction, o.date_placed)::DATE = cerw.full_date
            AND dc.store_id = cerw.store_id
    WHERE oc.meta_update_datetime > $wm_lake_ultra_merchant_order_credit
       OR o.meta_update_datetime > $wm_lake_ultra_merchant_order
       OR sc.meta_update_datetime > $wm_lake_ultra_merchant_store_credit
       OR a.meta_update_datetime > $wm_lake_ultra_merchant_address
       OR otd.meta_update_datetime > $wm_lake_ultra_merchant_order_tracking_detail
       OR fo.meta_update_datetime > $wm_fact_order
       OR cerw.meta_update_datetime > $wm_reference_currency_exchange_rate

    UNION ALL

    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit AS dc
        JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
            ON gc.gift_certificate_id = dc.credit_id
        LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction AS gct
            ON gct.gift_certificate_id = dc.credit_id
            AND dc.source_credit_id_type = 'gift_certificate_id'
        LEFT JOIN lake_consolidated.ultra_merchant.bounceback_endowment AS be
            ON be.object_id = gct.gift_certificate_id
            AND be.object = 'gift_certificate'
        LEFT JOIN stg.fact_order AS fo
            ON fo.order_id = gc.order_id
        LEFT JOIN lake_consolidated.ultra_merchant.address AS a
            ON a.address_id = fo.shipping_address_id
        LEFT JOIN _currency_exchange_rate_watermark cerw
            ON fo.order_local_datetime::DATE = cerw.full_date
            AND dc.store_id = cerw.store_id
        LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_store_credit gcsc
            ON gc.gift_certificate_id = gcsc.gift_certificate_id
        LEFT JOIN lake_consolidated.ultra_merchant.managed_gift_certificate mgc
            ON gc.gift_certificate_id = mgc.gift_certificate_id
        LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
            ON dc.credit_id = ctt.source_reference_number
    WHERE
        (gc.gift_certificate_type_id = 9 OR gc.order_id IS NOT NULL)
        AND (gct.meta_update_datetime > $wm_lake_ultra_merchant_gift_certificate_transaction
        OR be.meta_update_datetime > $wm_lake_ultra_merchant_bounceback_endowment
        OR fo.meta_update_datetime > $wm_fact_order
        OR a.meta_update_datetime > $wm_lake_ultra_merchant_address
        OR cerw.meta_update_datetime > $wm_reference_currency_exchange_rate
        OR gc.meta_update_datetime > $wm_lake_ultra_merchant_gift_certificate
        OR gcsc.meta_update_datetime > $wm_lake_ultra_merchant_gift_certificate_store_credit
        OR ctt.meta_update_datetime > $wm_lake_ultra_merchant_credit_transfer_transaction)
        AND dc.source_credit_id_type = 'gift_certificate_id'

    UNION ALL

    SELECT dc.credit_id,
           dc.credit_key,
           dc.credit_issuance_reason,
           dc.credit_issued_hq_datetime,
           dc.credit_type,
           dc.store_id,
           dc.source_credit_id_type
    FROM stg.dim_credit AS dc
             JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
                  ON dc.credit_id = gc.gift_certificate_id
             LEFT JOIN lake_consolidated.ultra_merchant.managed_gift_certificate mgc
                  ON mgc.gift_certificate_id = gc.gift_certificate_id
             LEFT JOIN lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
                  ON ctt.code = COALESCE(mgc.code, gc.code)
                      AND ctt.credit_transfer_transaction_type_id IN (250, 260)
             LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate gc2
                  ON ctt.gift_certificate_id = gc2.gift_certificate_id
             LEFT JOIN lake_consolidated.ultra_merchant.order_credit oc
                       ON oc.gift_certificate_id = gc2.gift_certificate_id
             LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction gct
                       ON gct.gift_certificate_id = gc2.gift_certificate_id
    WHERE (oc.meta_update_datetime > $wm_lake_ultra_merchant_order_credit
        OR gct.meta_update_datetime > $wm_lake_ultra_merchant_gift_certificate_transaction
        OR ctt.meta_update_datetime > $wm_lake_ultra_merchant_credit_transfer_transaction
        OR gc2.meta_update_datetime > $wm_lake_ultra_merchant_gift_certificate
        OR mgc.meta_update_datetime > $wm_lake_ultra_merchant_managed_gift_certificate)
      AND dc.source_credit_id_type = 'gift_certificate_id'

    UNION ALL

    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit dc
        JOIN stg.fact_activation fa
            ON fa.customer_id = dc.customer_id
    WHERE fa.meta_update_datetime > $wm_fact_activation

    UNION ALL

    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit AS dc
        JOIN lake_consolidated.ultra_merchant_history.membership AS m
            ON m.customer_id = dc.customer_id
    WHERE m.meta_update_datetime > $wm_lake_history_ultra_merchant_membership

    UNION ALL

    /* Previously errored rows */
    SELECT
        dc.credit_id,
        dc.credit_key,
        dc.credit_issuance_reason,
        dc.credit_issued_hq_datetime,
        dc.credit_type,
        dc.store_id,
        dc.source_credit_id_type
    FROM stg.dim_credit dc
        JOIN excp.fact_credit_event ec
            ON ec.credit_key = dc.credit_key AND ec.credit_id = dc.credit_id
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
ORDER BY incr.credit_id;
-- SELECT * FROM _fact_credit_event__credit_base;

-- Switch warehouse if the delta is more than medium warehouse can handle
SET warehouse_to_be_used = (SELECT IFF(COUNT(1) > 50000000, 'da_wh_adhoc_large', current_warehouse()) FROM _fact_credit_event__credit_base);
USE WAREHOUSE IDENTIFIER ($warehouse_to_be_used);

CREATE OR REPLACE TEMP TABLE _fact_credit_event AS
SELECT DISTINCT dc.credit_key,
                dc.credit_id,
                dc.credit_id                                                                    AS source_credit_id,
                COALESCE(mtt.administrator_id, -1)                                              AS administrator_id,
                CONVERT_TIMEZONE(dsdc.store_time_zone, mtt.datetime_transaction)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
                'Membership Token Transaction Log'                                              AS credit_activity_source,
                'Activity Captured Membership Token Transaction Log'                            AS credit_activity_source_reason,
                CASE
                    WHEN mttt.label = 'Create' THEN 'Issued'
                    WHEN mttt.label = 'Expire' THEN 'Expired'
                    WHEN mttt.label = 'Redeem' THEN 'Redeemed'
                    WHEN mttt.label = 'Cancel' THEN 'Cancelled'
                    WHEN mttt.label = 'Transfer - Outbound' THEN 'Transferred'
                    WHEN mttt.label = 'Convert To Credit' THEN 'Converted To Credit'
                    END                                                                         AS credit_activity_type,
                COALESCE(mttr.label, 'Unknown')                                                 AS credit_activity_type_reason,
                CASE
                    WHEN mttt.label = 'Create' THEN amount
                    WHEN mttt.label = 'Expire' THEN mt.purchase_price
                    WHEN mttt.label = 'Redeem' THEN amount
                    WHEN mttt.label = 'Cancel' THEN mt.purchase_price
                    WHEN mttt.label = 'Transfer - Outbound' THEN mt.purchase_price
                    ELSE amount END                                                             AS activity_amount,
                CASE
                    WHEN ord.order_status_key = 8 AND mttt.label = 'Redeem' THEN ord.master_order_id
                    WHEN mttt.label = 'Redeem' THEN mtt.object_id
                    END                                                                         AS redemption_order_id,
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
                    WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE')
                        THEN a.country_code
                    WHEN st.store_country = 'EUREM' THEN 'NL'
                    ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END        AS vat_rate_ship_to_country,
                CASE
                    WHEN credit_activity_type = 'Issued'
                        AND dc.credit_issuance_reason IN
                            ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                             'Fixed To Variable Conversion', 'Token To Credit Conversion', 'Gift Certificate')
                        THEN 'Exclude'
                    WHEN credit_activity_type IN ('Transferred', 'Converted To Credit') THEN 'Exclude'
                    ELSE 'Include' END                                                          AS original_credit_activity_type_action,
                dsdc.store_id                                                                   AS credit_store_id,
                dsdc.store_brand                                                                AS credit_store_brand,
                dsdc.store_country                                                              AS credit_store_country,
                dsdc.store_currency                                                             AS credit_store_currency,
                dc.credit_issued_hq_datetime,
                mtt.hvr_is_deleted::BOOLEAN                                                     AS is_deleted
FROM _fact_credit_event__credit_base dc
         JOIN lake_consolidated.ultra_merchant.membership_token mt
              ON mt.membership_token_id = dc.credit_id
         JOIN lake_consolidated.ultra_merchant.membership_token_transaction mtt
              ON mtt.membership_token_id = dc.credit_id AND
                 dc.credit_type = 'Token'
         JOIN lake_consolidated.ultra_merchant.membership_token_transaction_type mttt
              ON mttt.membership_token_transaction_type_id = mtt.membership_token_transaction_type_id
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = dc.store_id
         LEFT JOIN lake_consolidated.ultra_merchant.membership_token_transaction_reason mttr
                   ON mttr.membership_token_transaction_reason_id = mtt.membership_token_transaction_reason_id
         LEFT JOIN stg.fact_order ord
                   ON ord.order_id = mtt.object_id AND
                      mtt.object = 'order'
         LEFT JOIN lake_consolidated.ultra_merchant.address a
                   ON a.address_id = ord.shipping_address_id
         LEFT JOIN stg.dim_store st
                   ON st.store_id = ord.store_id
WHERE mtt.datetime_transaction IS NOT NULL

UNION ALL

SELECT DISTINCT dc.credit_key,
       dc.credit_id,
       dc.credit_id                                                                    AS source_credit_id,
       COALESCE(sct.administrator_id, -1)                                              AS administrator_id,
       CONVERT_TIMEZONE(dsdc.store_time_zone, sct.datetime_transaction)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
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
           WHEN ord.order_status_key = 8 AND sct.store_credit_transaction_type_id = 20 THEN ord.master_order_id
           WHEN sct.store_credit_transaction_type_id = 20 THEN sct.object_id
           END                                                                         AS redemption_order_id,
       IFF(sct.store_credit_transaction_type_id = 20, object_id, NULL)                 AS source_redemption_order_id,
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
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END        AS vat_rate_ship_to_country,
       CASE
           WHEN credit_activity_type = 'Issued'
               AND dc.credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion', 'Token To Credit Conversion', 'Gift Certificate')
               THEN 'Exclude'
           WHEN credit_activity_type IN ('Converted To Token', 'Converted To Variable', 'Transferred') THEN 'Exclude'
           ELSE 'Include' END                                                          AS original_credit_activity_type_action,
       dsdc.store_id                                                                   AS credit_store_id,
       dsdc.store_brand                                                                AS credit_store_brand,
       dsdc.store_country                                                              AS credit_store_country,
       dsdc.store_currency                                                             AS credit_store_currency,
       dc.credit_issued_hq_datetime,
       sct.hvr_is_deleted::BOOLEAN                                                     AS is_deleted
FROM _fact_credit_event__credit_base dc
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = dc.store_id
         JOIN lake_consolidated.ultra_merchant.store_credit_transaction sct
              ON sct.store_credit_id = dc.credit_id
                  AND dc.credit_type ILIKE '%credit%'
         JOIN lake_consolidated.ultra_merchant.store_credit sc
              ON sc.store_credit_id = sct.store_credit_id
         JOIN lake_consolidated.ultra_merchant.customer c
              ON c.customer_id = sc.customer_id
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit_transaction_reason sctr
                   ON sctr.store_credit_transaction_reason_id = sct.store_credit_transaction_reason_id
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit_transaction_type sctt
                   ON sctt.store_credit_transaction_type_id = sct.store_credit_transaction_type_id
         LEFT JOIN stg.fact_order ord
                   ON ord.order_id = sct.object_id AND
                      sct.object = 'order'
         LEFT JOIN lake_consolidated.ultra_merchant.address a
                   ON a.address_id = ord.shipping_address_id
         LEFT JOIN stg.dim_store store
                   ON store.store_id = ord.store_id
WHERE sct.datetime_transaction IS NOT NULL
  AND NOT (sct.datetime_transaction < '2017-06-01' AND sct.store_credit_transaction_type_id = 30 AND sct.amount = 0)
GROUP BY dc.credit_key,
         dc.credit_id,
         COALESCE(sct.administrator_id, -1),
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         original_credit_activity_type_action,
         redemption_order_id,
         source_redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country,
         dsdc.store_id,
         dsdc.store_brand,
         dsdc.store_country,
         dsdc.store_currency,
         dc.credit_issued_hq_datetime,
         sct.hvr_is_deleted::BOOLEAN

UNION ALL

SELECT DISTINCT dc.credit_key,
       dc.credit_id,
       dc.credit_id                                                                            AS source_credit_id,
       IFF(be.administrator_id IS NOT NULL AND gctt.label = 'Create', be.administrator_id, -1) AS administrator_id,
       CONVERT_TIMEZONE(dsdc.store_time_zone, gct.datetime_transaction)::TIMESTAMP_NTZ         AS credit_activity_local_datetime,
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
           END                                                                                 AS credit_activity_type,
       COALESCE(gctr.label, 'Unknown')                                                         AS credit_activity_type_reason,
       SUM(CASE
               WHEN gctt.label IN ('Create', 'Redeem', 'Transfer - Inbound') THEN gct.amount
               WHEN gctt.label = 'Cancel' AND gct.datetime_transaction >= '2024-05-29 15:30:07.090' THEN gct.amount
               WHEN gctt.label IN ('Transfer - Outbound', 'Expire', 'Convert To Store Credit', 'Cancel')
                   THEN gct.balance
               ELSE gct.amount
           END)                                                                                AS activity_amount,
       CASE
           WHEN fo.order_status_key = 8 AND gct.gift_certificate_transaction_type_id = 20 THEN fo.master_order_id
           WHEN gct.gift_certificate_transaction_type_id = 20 THEN fo.order_id
           END                                                                                 AS redemption_order_id,
       IFF(gct.gift_certificate_transaction_type_id = 20, fo.order_id, NULL)                   AS source_redemption_order_id,
       fo.store_id                                                                             AS redemption_store_id,
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
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END                AS vat_rate_ship_to_country,
       CASE
           WHEN credit_activity_type = 'Issued'
               AND dc.credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion')
               THEN 'Exclude'
           WHEN credit_activity_type IN
                ('Converted To Token', 'Converted To Variable', 'Transferred', 'Converted To Store Credit')
               THEN 'Exclude'
           ELSE 'Include' END                                                                  AS original_credit_activity_type_action,
       dsdc.store_id                                                                           AS credit_store_id,
       dsdc.store_brand                                                                        AS credit_store_brand,
       dsdc.store_country                                                                      AS credit_store_country,
       dsdc.store_currency                                                                     AS credit_store_currency,
       dc.credit_issued_hq_datetime,
       gct.hvr_is_deleted::BOOLEAN                                                             AS is_deleted
FROM _fact_credit_event__credit_base dc
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = dc.store_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction gct
              ON gct.gift_certificate_id = dc.credit_id
                  AND dc.credit_type = 'Giftcard'
         JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction_type AS gctt
              ON gctt.gift_certificate_transaction_type_id = gct.gift_certificate_transaction_type_id
         LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction_reason AS gctr
                   ON COALESCE(gctr.gift_certificate_transaction_type_id, -1) =
                      COALESCE(gct.gift_certificate_transaction_type_id, -1)
         JOIN lake_consolidated.ultra_merchant.bounceback_endowment AS be
              ON be.object_id = gct.gift_certificate_id
                  AND be.object = 'gift_certificate'
         LEFT JOIN stg.fact_order AS fo
                   ON fo.order_id = gct.object_id
                       AND gct.object = 'order'
         LEFT JOIN stg.dim_store AS st
                   ON st.store_id = fo.store_id
         LEFT JOIN lake_consolidated.ultra_merchant.address AS a
                   ON a.address_id = fo.shipping_address_id
WHERE gct.datetime_transaction IS NOT NULL
GROUP BY dc.credit_key,
         dc.credit_id,
         IFF(be.administrator_id IS NOT NULL AND gctt.label = 'Create', be.administrator_id, -1),
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         original_credit_activity_type_action,
         redemption_order_id,
         source_redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country,
         dsdc.store_id,
         dsdc.store_brand,
         dsdc.store_country,
         dsdc.store_currency,
         dc.credit_issued_hq_datetime,
         gct.hvr_is_deleted::BOOLEAN

UNION ALL

--Gift Cards
SELECT DISTINCT base.credit_key,
       base.credit_id,
       base.credit_id                                                                               AS source_credit_id,
       -1                                                                                           AS administrator_id,
       CASE
           WHEN gctt.label = 'Create'
                AND CONVERT_TIMEZONE(dsdc.store_time_zone, gct.datetime_transaction)::DATE < fo.order_local_datetime::DATE
           THEN fo.order_local_datetime ::TIMESTAMP_NTZ
           ELSE CONVERT_TIMEZONE(dsdc.store_time_zone, gct.datetime_transaction)::TIMESTAMP_NTZ
       END                                                                                          AS credit_activity_local_datetime,
       'Gift Certificate Transaction Log'                                                           AS credit_activity_source,
       'Activity Captured Gift Certificate Transaction Log'                                         AS credit_activity_source_reason,
       CASE
           WHEN gctt.label = 'Create' THEN 'Issued'
           WHEN gctt.label = 'Redeem' THEN 'Redeemed'
           WHEN gctt.label = 'Cancel' THEN 'Cancelled'
           WHEN gctt.label = 'Expire' THEN 'Expired'
           WHEN gctt.label = 'Transfer - Outbound' THEN 'Transferred'
           WHEN gctt.label = 'Transfer - Inbound' THEN 'Issued' -- transfer -inbound
           WHEN gctt.label = 'Convert To Store Credit' THEN 'Converted To Store Credit'
           END                                                                                      AS credit_activity_type,
       COALESCE(gctr.label, 'Gift Card Purchase')                                                   AS credit_activity_type_reason,
       SUM(CASE
               WHEN gctt.label IN ('Create', 'Redeem', 'Transfer - Inbound') THEN gct.amount
               WHEN gctt.label = 'Cancel' AND gct.datetime_transaction >= '2024-05-29 15:30:07.090' THEN gct.amount
               WHEN gctt.label IN ('Transfer - Outbound', 'Expire', 'Convert To Store Credit', 'Cancel')
                   THEN gct.balance
               ELSE gct.amount
           END)                                                                                     AS activity_amount,
       CASE
           WHEN fo2.order_status_key = 8 AND gct.gift_certificate_transaction_type_id = 20 THEN fo2.master_order_id
           WHEN gct.gift_certificate_transaction_type_id = 20 THEN fo2.order_id
           END                                                                                      AS redemption_order_id,
       IFF(gct.gift_certificate_transaction_type_id = 20, fo2.order_id,
           NULL)                                                                                    AS source_redemption_order_id,
       fo2.store_id                                                                                 AS redemption_store_id,
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
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END                     AS vat_rate_ship_to_country,
       CASE
           WHEN credit_activity_type = 'Issued'
               AND base.credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion')
               THEN 'Exclude'
           WHEN credit_activity_type IN
                ('Converted To Token', 'Converted To Variable', 'Transferred', 'Converted To Store Credit')
               THEN 'Exclude'
           ELSE 'Include' END                                                                       AS original_credit_activity_type_action,
       dsdc.store_id                                                                                AS credit_store_id,
       dsdc.store_brand                                                                             AS credit_store_brand,
       dsdc.store_country                                                                           AS credit_store_country,
       dsdc.store_currency                                                                          AS credit_store_currency,
       base.credit_issued_hq_datetime,
       gct.hvr_is_deleted::BOOLEAN                                                                  AS is_deleted
FROM _fact_credit_event__credit_base base
         JOIN lake_consolidated.ultra_merchant.gift_certificate gc
              ON base.credit_id = gc.gift_certificate_id
         JOIN stg.dim_credit dc
              ON base.credit_id = dc.credit_id
                  AND dc.source_credit_id_type = base.source_credit_id_type
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = base.store_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction gct
              ON gct.gift_certificate_id = base.credit_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction_type AS gctt
              ON gctt.gift_certificate_transaction_type_id = gct.gift_certificate_transaction_type_id
         LEFT JOIN lake_consolidated.ultra_merchant.gift_certificate_transaction_reason AS gctr
                   ON COALESCE(gctr.gift_certificate_transaction_reason_id, -1) =
                      COALESCE(gct.gift_certificate_transaction_reason_id, -1)
         LEFT JOIN stg.fact_order AS fo
                   ON fo.order_id = gc.order_id
         LEFT JOIN stg.dim_store AS st
                   ON st.store_id = fo.store_id
         LEFT JOIN lake_consolidated.ultra_merchant.address AS a
                   ON a.address_id = fo.shipping_address_id
         LEFT JOIN stg.fact_order AS fo2
                   ON gct.object_id = fo2.order_id
WHERE gct.datetime_transaction IS NOT NULL
  AND base.source_credit_id_type = 'gift_certificate_id'
  AND gc.gift_certificate_type_id != 9
  --Filter out any Transfer activity for gift certificate to gift certificate transactions through Giftco
  AND NOT (
    gctt.gift_certificate_transaction_type_id IN (50, 55)
        AND base.credit_id IN (SELECT DISTINCT source_reference_number
                               FROM lake_consolidated.ultra_merchant.credit_transfer_transaction ctt
                               WHERE gift_certificate_id IS NOT NULL)
    )
  AND gc.order_id IS NOT NULL
GROUP BY base.credit_key,
         base.credit_id,
         source_credit_id,
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         original_credit_activity_type_action,
         redemption_order_id,
         source_redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country,
         dsdc.store_id,
         dsdc.store_brand,
         dsdc.store_country,
         dsdc.store_currency,
         base.credit_issued_hq_datetime,
         gct.hvr_is_deleted::BOOLEAN

UNION ALL
--Everything from OC (Parent) Not in GCT
SELECT DISTINCT base.credit_key,
       base.credit_id,
       base.credit_id                                                           AS source_credit_id,
       -1                                                                       AS administrator_id,
       CONVERT_TIMEZONE(dsdc.store_time_zone,
                        fo.order_local_datetime)::TIMESTAMP_NTZ                 AS credit_activity_local_datetime,
       'Missing GCT - Successful Order'                                         AS credit_activity_source,
       'successful in order credit'                                             AS credit_activity_source_reason,
       'Redeemed'                                                               AS credit_activity_type,
       'Redeemed Gift Card'                                                     AS credit_activity_type_reason,
       SUM(oc.amount)                                                           AS activity_amount,
       IFF(fo.order_status_key = 8, fo.master_order_id, fo.order_id)            AS redemption_order_id,
       fo.order_id                                                              AS source_redemption_order_id,
       fo.store_id                                                              AS redemption_store_id,
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
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END AS vat_rate_ship_to_country,
       'Include'                                                                AS original_credit_activity_type_action,
       base.store_id                                                            AS credit_store_id,
       dsdc.store_brand                                                         AS credit_store_brand,
       dsdc.store_country                                                       AS credit_store_country,
       dsdc.store_currency                                                      AS credit_store_currency,
       base.credit_issued_hq_datetime,
       oc.hvr_is_deleted::BOOLEAN                                               AS is_deleted
FROM _fact_credit_event__credit_base base
         JOIN lake_consolidated.ultra_merchant.order_credit oc
              ON base.credit_id = oc.gift_certificate_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate gc
              ON base.credit_id = gc.gift_certificate_id
         JOIN stg.dim_credit dc
              ON base.credit_id = dc.credit_id
                  AND dc.source_credit_id_type = base.source_credit_id_type
         JOIN lake_consolidated.ultra_merchant."ORDER" o
              ON gc.order_id = o.order_id
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = o.store_id
         JOIN stg.fact_order AS fo
              ON oc.order_id = fo.order_id
         LEFT JOIN stg.dim_store AS st
                   ON st.store_id = fo.store_id
         LEFT JOIN lake_consolidated.ultra_merchant.address AS a
                   ON a.address_id = fo.shipping_address_id
WHERE base.source_credit_id_type = 'gift_certificate_id'
  AND NOT EXISTS(SELECT 1
                 FROM lake_consolidated.ultra_merchant.gift_certificate_transaction AS gct
                 WHERE gct.gift_certificate_transaction_type_id = 20
                   AND gct.object_id = fo.order_id
                   AND gct.gift_certificate_id = base.credit_id
                   AND gct.datetime_transaction IS NOT NULL
                   AND gct.hvr_is_deleted = 0)
GROUP BY base.credit_key,
         base.credit_id,
         source_credit_id,
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         original_credit_activity_type_action,
         redemption_order_id,
         source_redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country,
         base.store_id,
         dsdc.store_brand,
         dsdc.store_country,
         dsdc.store_currency,
         base.credit_issued_hq_datetime,
         oc.hvr_is_deleted::BOOLEAN

UNION ALL

--managed gift cards
SELECT DISTINCT base.credit_key,
       base.credit_id,
       gc2.gift_certificate_id                                                  AS source_credit_id,
       -1                                                                       AS administrator_id,
       CONVERT_TIMEZONE(dsdc.store_time_zone,
                        fo.order_local_datetime)::TIMESTAMP_NTZ                 AS credit_activity_local_datetime,
       'Managed Gift Card - Successful Order'                                   AS credit_activity_source,
       'successful in order credit'                                             AS credit_activity_source_reason,
       'Redeemed'                                                               AS credit_activity_type,
       'Redeemed Managed Gift Card'                                             AS credit_activity_type_reason,
       SUM(oc.amount)                                                           AS activity_amount,
       IFF(fo.order_status_key = 8, fo.master_order_id, fo.order_id)            AS redemption_order_id,
       fo.order_id                                                              AS source_redemption_order_id,
       fo.store_id                                                              AS redemption_store_id,
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
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END AS vat_rate_ship_to_country,
       'Include'                                                                AS original_credit_activity_type_action,
       base.store_id                                                            AS credit_store_id,
       dsdc.store_brand                                                         AS credit_store_brand,
       dsdc.store_country                                                       AS credit_store_country,
       dsdc.store_currency                                                      AS credit_store_currency,
       base.credit_issued_hq_datetime,
       oc.hvr_is_deleted::BOOLEAN                                               AS is_deleted
FROM _fact_credit_event__credit_base base
         JOIN lake_consolidated.ultra_merchant.gift_certificate gc
              ON base.credit_id = gc.gift_certificate_id
         JOIN stg.dim_credit dc
              ON base.credit_id = dc.credit_id
                  AND dc.source_credit_id_type = base.source_credit_id_type
         JOIN lake_consolidated.ultra_merchant."ORDER" o
              ON gc.order_id = o.order_id
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = o.store_id
         JOIN lake_consolidated.ultra_merchant.managed_gift_certificate mgc
              ON gc.gift_certificate_id = mgc.gift_certificate_id
         JOIN ( SELECT DISTINCT gift_certificate_id, code
                FROM lake_consolidated.ultra_merchant.credit_transfer_transaction
                WHERE credit_transfer_transaction_type_id IN (250, 260)) AS ctt
             ON ctt.code = COALESCE(mgc.code, gc.code)
         JOIN lake_consolidated.ultra_merchant.gift_certificate gc2
              ON ctt.gift_certificate_id = gc2.gift_certificate_id
         JOIN lake_consolidated.ultra_merchant.order_credit oc
              ON oc.gift_certificate_id = gc2.gift_certificate_id
         JOIN stg.fact_order AS fo
              ON oc.order_id = fo.order_id
         LEFT JOIN stg.dim_store AS st
                   ON st.store_id = fo.store_id
         LEFT JOIN lake_consolidated.ultra_merchant.address AS a
                   ON a.address_id = fo.shipping_address_id
WHERE base.source_credit_id_type = 'gift_certificate_id'
GROUP BY base.credit_key,
         base.credit_id,
         source_credit_id,
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         original_credit_activity_type_action,
         redemption_order_id,
         source_redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country,
         base.store_id,
         dsdc.store_brand,
         dsdc.store_country,
         dsdc.store_currency,
         base.credit_issued_hq_datetime,
         oc.hvr_is_deleted::BOOLEAN

UNION ALL

SELECT dc.credit_key,
       dc.credit_id,
       dc.credit_id                                                           AS source_credit_id,
       COALESCE(h.administrator_id, -1)                                       AS administrator_id,
       CONVERT_TIMEZONE(ds.store_time_zone, activity_datetime)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       activity_source                                                        AS credit_activity_source,
       activity_source_reason                                                 AS credit_activity_source_reason,
       activity_type                                                          AS credit_activity_type,
       ''                                                                     AS credit_activity_type_reason,
       SUM(activity_amount)                                                   AS activity_amount,
       redemption_order_id,
       redemption_order_id                                                    AS source_redemption_order_id,
       redemption_store_id,
       COALESCE(vat_rate_ship_to_country, ds.store_country)                   AS vat_rate_ship_to_country,
       CASE
           WHEN credit_activity_type = 'Issued'
               AND dc.credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion', 'Gift Certificate')
               THEN 'Exclude'
           WHEN credit_activity_type IN ('Converted To Token', 'Converted To Variable', 'Transferred') THEN 'Exclude'
           ELSE 'Include' END                                                 AS original_credit_activity_type_action,
       ds.store_id                                                            AS credit_store_id,
       ds.store_brand                                                         AS credit_store_brand,
       ds.store_country                                                       AS credit_store_country,
       ds.store_currency                                                      AS credit_store_currency,
       dc.credit_issued_hq_datetime,
       FALSE                                                                  AS is_deleted
FROM _fact_credit_event__credit_base dc
         JOIN stg.dim_store ds
              ON ds.store_id = dc.store_id
         JOIN reference.credit_historical_activity h
              ON h.store_credit_id = dc.credit_id
                  AND dc.credit_type ILIKE '%credit%'
WHERE NOT EXISTS(SELECT 1
                 FROM lake_consolidated.ultra_merchant.store_credit_transaction AS sct
                 WHERE sct.store_credit_transaction_type_id = 20
                   AND sct.object_id = h.redemption_order_id
                   AND sct.store_credit_id = h.store_credit_id
                   AND sct.datetime_transaction IS NOT NULL
                   AND sct.hvr_is_deleted = 0)
  AND NOT EXISTS(SELECT sct.store_credit_id,
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
                 WHERE sct.hvr_is_deleted = 0
                   AND sct.store_credit_transaction_type_id <> 20
                   AND sct.datetime_transaction IS NOT NULL
                   AND NOT (sct.datetime_transaction < '2017-06-01' AND sct.store_credit_transaction_type_id = 30 AND
                            sct.amount = 0)
                   AND sct.store_credit_id = h.store_credit_id
                   AND activity_type_cast_stmt = h.activity_type
                   AND sct.datetime_transaction::DATE = h.activity_datetime::DATE)
GROUP BY dc.credit_key, dc.credit_id, COALESCE(h.administrator_id, -1), credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         original_credit_activity_type_action,
         redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country,
         ds.store_id,
         ds.store_brand,
         ds.store_country,
         ds.store_currency,
         dc.credit_issued_hq_datetime

UNION ALL

SELECT dc.credit_key,
       dc.credit_id,
       dc.credit_id                                                                                     AS source_credit_id,
       COALESCE(otd.object_id, -1)                                                                      AS administrator_id,
       CONVERT_TIMEZONE(dsdc.store_time_zone,
                        COALESCE(o.date_shipped, o.datetime_transaction, o.date_placed))::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       'Missing SCT - Successful Order'                                                                 AS credit_activity_source,
       'successful in order credit'                                                                     AS credit_activity_source_reason,
       'Redeemed'                                                                                       AS credit_activity_type,
       'Redeemed In Order'                                                                              AS credit_activity_type_reason,
       SUM(oc.amount)                                                                                   AS activity_amount,
       IFF(fo.order_status_key = 8, fo.master_order_id, fo.order_id)                                    AS redemption_order_id,
       oc.order_id                                                                                      AS source_redemption_order_id,
       fo.store_id                                                                                      AS redemption_store_id,
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
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END                         AS vat_rate_ship_to_country,
       CASE
           WHEN credit_activity_type = 'Issued'
               AND dc.credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion', 'Gift Certificate')
               THEN 'Exclude'
           WHEN credit_activity_type IN ('Converted To Token', 'Converted To Variable', 'Transferred') THEN 'Exclude'
           ELSE 'Include' END                                                                           AS original_credit_activity_type_action,
       dsdc.store_id                                                                                    AS credit_store_id,
       dsdc.store_brand                                                                                 AS credit_store_brand,
       dsdc.store_country                                                                               AS credit_store_country,
       dsdc.store_currency                                                                              AS credit_store_currency,
       dc.credit_issued_hq_datetime,
       oc.hvr_is_deleted::BOOLEAN                                                                       AS is_deleted
FROM _fact_credit_event__credit_base dc
         JOIN lake_consolidated.ultra_merchant.order_credit oc
              ON oc.store_credit_id = dc.credit_id
         JOIN lake_consolidated.ultra_merchant."ORDER" AS o
              ON o.order_id = oc.order_id
         JOIN stg.fact_order AS fo
              ON fo.order_id = o.order_id
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = dc.store_id
         JOIN stg.dim_store store
              ON store.store_id = fo.store_id
         JOIN lake_consolidated.ultra_merchant.statuscode scc
              ON scc.statuscode = o.payment_statuscode
         JOIN lake_consolidated.ultra_merchant.statuscode scc2
              ON scc2.statuscode = o.processing_statuscode
         JOIN lake_consolidated.ultra_merchant.store_credit sc
              ON sc.store_credit_id = oc.store_credit_id
         LEFT JOIN lake_consolidated.ultra_merchant.address a
                   ON a.address_id = o.shipping_address_id
         LEFT JOIN (SELECT order_tracking_id, object_id
                    FROM lake_consolidated.ultra_merchant.order_tracking_detail
                    WHERE object = 'administrator'
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_tracking_id ORDER BY datetime_added, object_id) = 1) otd
                   ON otd.order_tracking_id = o.order_tracking_id
WHERE scc.label IN ('Paid', 'Refunded', 'Refunded (Partial)')
  AND dc.credit_type ILIKE '%credit%'
  AND scc2.label IN ('Shipped', 'Complete')
  AND oc.amount > 0
  AND NOT EXISTS(SELECT 1
                 FROM reference.credit_historical_activity ha
                 WHERE ha.store_credit_id = dc.credit_id
                   AND ha.activity_amount = oc.amount
                   AND ha.activity_type = 'Redeemed'
                   AND ha.redemption_order_id = oc.order_id)
  AND NOT EXISTS(SELECT 1
                 FROM lake_consolidated.ultra_merchant.store_credit_transaction sct
                 WHERE store_credit_transaction_type_id = 20
                   AND sct.object_id = o.order_id
                   AND sct.store_credit_id = oc.store_credit_id
                   AND sct.datetime_transaction IS NOT NULL)
GROUP BY dc.credit_key,
         dc.credit_id,
         otd.object_id,
         credit_activity_local_datetime,
         IFF(fo.order_status_key = 8, fo.master_order_id, fo.order_id),
         oc.order_id,
         fo.store_id,
         vat_rate_ship_to_country,
         original_credit_activity_type_action,
         dsdc.store_id,
         dsdc.store_brand,
         dsdc.store_country,
         dsdc.store_currency,
         dc.credit_issued_hq_datetime,
         oc.hvr_is_deleted::BOOLEAN

UNION ALL

/* We converted a batch of variable credits to token that are not receiving a "Converted to Token" activity so we are creating one */
SELECT dco.credit_key,
       dco.credit_id,
       dco.credit_id,
       COALESCE(sctc.administrator_id, -1)                                      AS administrator_id,
       CONVERT_TIMEZONE(st.store_time_zone, sctc.datetime_added)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       'Store Credit Token Conversion Log'                                      AS credit_activity_source,
       'Activity Captured Store Credit Token Conversion Log'                    AS credit_activity_source_reason,
       'Converted To Token'                                                     AS credit_activity_type,
       'Converted to Variable To Token'                                         AS credity_activity_type_reason,
       sc.balance                                                               AS activity_amount,
       NULL                                                                     AS redemption_order_id,
       NULL                                                                     AS source_redemption_order_id,
       NULL                                                                     AS redemption_store_id,
       IFF(st.store_country = 'EUREM', 'NL', st.store_country)                  AS vat_rate_ship_to_country,
       'Exclude'                                                                AS original_credit_activity_type_action,
       st.store_id                                                              AS credit_store_id,
       st.store_brand                                                           AS credit_store_brand,
       st.store_country                                                         AS credit_store_country,
       st.store_currency                                                        AS credit_store_currency,
       dco.credit_issued_hq_datetime,
       sctc.hvr_is_deleted::BOOLEAN                                             AS is_deleted
FROM _fact_credit_event__credit_base base
         JOIN stg.dim_credit AS dc
              ON base.credit_key = dc.credit_key
         JOIN stg.dim_store AS st
              ON st.store_id = dc.store_id
         JOIN lake_consolidated.ultra_merchant.store_credit_token_conversion AS sctc
              ON sctc.converted_membership_token_id = dc.credit_id
         JOIN lake_consolidated.ultra_merchant.store_credit AS sc
              ON sc.store_credit_id = sctc.original_store_credit_id
         JOIN stg.dim_credit AS dco
              ON dco.credit_id = sc.store_credit_id
                  AND dco.source_credit_id_type = 'store_credit_id'
WHERE dc.original_credit_match_reason = 'Converted To Variable To Token'
  AND sctc.store_credit_conversion_type_id = 3

UNION ALL
/* There was a cancellation bug in source. Explained in DA-28668 */
SELECT base.credit_key,
       base.credit_id,
       base.credit_id                                            AS source_credit_id,
       -1                                                        AS administrator_id,
       ccd.new_credit_cancellation_local_datetime::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       'reference.credit_cancellation_datetime'                  AS credit_activity_source,
       'Uncaptured Credit Cancellations in Source'               AS credit_activity_source_reason,
       'Cancelled'                                               AS credit_activity_type,
       'Unknown'                                                 AS credity_activity_type_reason,
       ccd.amount,
       NULL                                                      AS redemption_order_id,
       NULL                                                      AS source_redemption_order_id,
       NULL                                                      AS redemption_store_id,
       IFF(st.store_country = 'EUREM', 'NL', st.store_country)   AS vat_rate_ship_to_country,
       CASE
           WHEN credit_activity_type = 'Issued'
               AND dc.credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion', 'Gift Certificate')
               THEN 'Exclude'
           ELSE 'Include' END                                    AS original_credit_activity_type_action,
       st.store_id                                               AS credit_store_id,
       st.store_brand                                            AS credit_store_brand,
       st.store_country                                          AS credit_store_country,
       st.store_currency                                         AS credit_store_currency,
       dc.credit_issued_hq_datetime,
       FALSE                                                     AS is_deleted
FROM _fact_credit_event__credit_base base
         JOIN stg.dim_credit AS dc
              ON dc.credit_key = base.credit_key
         JOIN reference.credit_cancellation_datetime AS ccd
              ON ccd.credit_id = dc.credit_id
                  AND ccd.source_credit_id_type = dc.source_credit_id_type
         JOIN stg.dim_store AS st
              ON st.store_id = dc.store_id;

/* Bounceback endowments - a customer can redeem just a portion of the endowment - but any unused balance would be immediately expired.
   Source tables do not record an expired transaction record, only redemption.
   This next section of code is creating a an expired transaction record for partially redeemed bb endowments to fully close out the credit */
INSERT INTO _fact_credit_event (
    credit_key,
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
    vat_rate_ship_to_country,
    original_credit_activity_type_action,
    credit_store_id,
    credit_store_brand,
    credit_store_country,
    credit_store_currency,
    credit_issued_hq_datetime,
    is_deleted
)
SELECT
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
    fce.vat_rate_ship_to_country,
    fce.original_credit_activity_type_action,
    fce.credit_store_id,
    fce.credit_store_brand,
    fce.credit_store_country,
    fce.credit_store_currency,
    fce.credit_issued_hq_datetime,
    fce.is_deleted
FROM _fact_credit_event__credit_base AS dc
    JOIN _fact_credit_event AS fce
        ON fce.credit_key = dc.credit_key
        AND fce.credit_activity_type = 'Redeemed'
    JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
        ON gc.gift_certificate_id = dc.credit_id
    JOIN lake_consolidated.ultra_merchant.bounceback_endowment AS be
        ON be.object_id = gc.gift_certificate_id
        AND be.object = 'gift_certificate'
WHERE dc.credit_type = 'Giftcard'
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
                                vat_rate_ship_to_country,
                                original_credit_activity_type_action,
                                credit_store_id,
                                credit_store_brand,
                                credit_store_country,
                                credit_store_currency,
                                credit_issued_hq_datetime,
                                is_deleted)
SELECT base.credit_key,
       base.credit_id,
       base.credit_id                                                                                                AS source_credit_id,
       NULL                                                                                                          AS administrator_id,
       CONVERT_TIMEZONE('America/Los_Angeles', stz.time_zone, o.datetime_added)                                      AS credit_activity_local_datetime,
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
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END                                      AS vat_rate_ship_to_country,
       'Include'                                                                                                     AS original_credit_activity_type_action,
       base.store_id                                                                                                 AS credit_store_id,
       dsdc.store_brand                                                                                              AS credit_store_brand,
       dsdc.store_country                                                                                            AS credit_store_country,
       dsdc.store_currency                                                                                           AS credit_store_currency,
       o.datetime_added                                                                                              AS credit_issued_hq_datetime,
       FALSE                                                                                                         AS is_deleted
FROM _fact_credit_event__credit_base AS base
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = base.store_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
              ON gc.gift_certificate_id = base.credit_id
         JOIN lake_consolidated.ultra_merchant.statuscode AS sc
              ON gc.statuscode = sc.statuscode
         LEFT JOIN stg.dim_store AS st
                   ON base.store_id = st.store_id
         LEFT JOIN lake_consolidated.ultra_merchant."ORDER" o
                   ON gc.order_id = o.order_id
         LEFT JOIN lake_consolidated.ultra_merchant.address a
                   ON o.shipping_address_id = a.address_id
         LEFT JOIN reference.store_timezone stz
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
         vat_rate_ship_to_country,
         original_credit_activity_type_action,
         base.store_id,
         dsdc.store_brand,
         dsdc.store_country,
         dsdc.store_currency,
         o.datetime_added
UNION ALL
SELECT base.credit_key,
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
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END AS vat_rate_ship_to_country,
       'Include'                                                  AS original_credit_activity_type_action,
       base.store_id                                                            AS credit_store_id,
       dsdc.store_brand                                                         AS credit_store_brand,
       dsdc.store_country                                                       AS credit_store_country,
       dsdc.store_currency                                                      AS credit_store_currency,
       o.datetime_added                            AS credit_issued_hq_datetime,
       FALSE                                                                    AS is_deleted
FROM _fact_credit_event__credit_base AS base
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = base.store_id
         JOIN lake_consolidated.ultra_merchant.gift_certificate AS gc
              ON gc.gift_certificate_id = base.credit_id
         JOIN lake_consolidated.ultra_merchant.statuscode AS sc
              ON gc.statuscode = sc.statuscode
         LEFT JOIN stg.dim_store AS st
                   ON base.store_id = st.store_id
         LEFT JOIN lake_consolidated.ultra_merchant."ORDER" o
                   ON gc.order_id = o.order_id
         LEFT JOIN lake_consolidated.ultra_merchant.address a
                   ON o.shipping_address_id = a.address_id
         LEFT JOIN reference.store_timezone stz
                   ON base.store_id = stz.store_id
WHERE base.source_credit_id_type = 'gift_certificate_id'
  AND sc.label = 'Cancelled'
  AND NOT EXISTS (SELECT 1
                  FROM _fact_credit_event fe
                  WHERE fe.credit_activity_type = 'Cancelled'
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
         vat_rate_ship_to_country,
         original_credit_activity_type_action,
         base.store_id,
         dsdc.store_brand,
         dsdc.store_country,
         dsdc.store_currency,
         o.datetime_added
;


/* deleting tokens that were wrongfully converted (DA-28668) */
UPDATE _fact_credit_event AS fce
SET fce.is_deleted = TRUE
FROM stg.dim_credit AS dc
WHERE dc.credit_key = fce.credit_key
AND dc.is_cancelled_before_token_conversion = TRUE;

/* deleting events that occurred after the credit's corrected cancelled datetime (DA-28668) */
UPDATE _fact_credit_event AS fce
SET fce.is_deleted = TRUE
FROM (
        SELECT fce.credit_key, fce.credit_activity_type, fce.credit_activity_local_datetime
        FROM _fact_credit_event AS fce
            JOIN stg.dim_credit AS dc
                    ON dc.credit_key = fce.credit_key
            JOIN reference.credit_cancellation_datetime AS ccd
                ON ccd.credit_id = dc.credit_id
                AND ccd.source_credit_id_type = dc.source_credit_id_type
        WHERE fce.credit_activity_local_datetime > ccd.new_credit_cancellation_local_datetime::TIMESTAMP_NTZ
) AS ccd
WHERE ccd.credit_key = fce.credit_key
      AND ccd.credit_activity_local_datetime = fce.credit_activity_local_datetime
      AND ccd.credit_activity_type = fce.credit_activity_type;


CREATE OR REPLACE TEMP TABLE _fact_credit_event__membership_token_transaction_history AS
    SELECT mtth.* FROM _fact_credit_event__credit_base fce
        JOIN lake_consolidated.ultra_merchant.membership_token_transaction mtt
            ON fce.credit_id = mtt.membership_token_id
        AND fce.credit_type='Token'
        AND (mtt.datetime_transaction is null or mtt.hvr_is_deleted)
       JOIN lake_consolidated.ultra_merchant_history.membership_token_transaction mtth
        ON mtt.membership_token_transaction_id = mtth.membership_token_transaction_id
        and mtth.datetime_transaction IS NOT NULL;

-- Handle deletions - Membership Token Transaction
CREATE OR REPLACE TEMP TABLE _fact_credit_event__membership_transactions_delete AS
SELECT DISTINCT dc.credit_key,
                dc.credit_id,
                dc.credit_id                                                                    AS source_credit_id,
                mtt.administrator_id,
                CONVERT_TIMEZONE(dsdc.store_time_zone, mtt.datetime_transaction)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
                'Membership Token Transaction Log'                                              AS credit_activity_source,
                'Activity Captured Membership Token Transaction Log'                            AS credit_activity_source_reason,
                CASE
                    WHEN mttt.label = 'Create' THEN 'Issued'
                    WHEN mttt.label = 'Expire' THEN 'Expired'
                    WHEN mttt.label = 'Redeem' THEN 'Redeemed'
                    WHEN mttt.label = 'Cancel' THEN 'Cancelled'
                    WHEN mttt.label = 'Transfer - Outbound' THEN 'Transferred'
                    WHEN mttt.label = 'Convert To Credit' THEN 'Converted To Credit'
                    END                                                                         AS credit_activity_type,
                COALESCE(mttr.label, 'Unknown')                                                 AS credit_activity_type_reason,
                CASE
                    WHEN mttt.label = 'Create' THEN amount
                    WHEN mttt.label = 'Expire' THEN mt.purchase_price
                    WHEN mttt.label = 'Redeem' THEN amount
                    WHEN mttt.label = 'Cancel' THEN mt.purchase_price
                    WHEN mttt.label = 'Transfer - Outbound' THEN amount
                    ELSE amount END                                                             AS activity_amount,
                CASE
                    WHEN ord.order_status_key = 8 AND mttt.label = 'Redeem' THEN ord.master_order_id
                    WHEN mttt.label = 'Redeem' THEN mtt.object_id
                    END                                                                         AS redemption_order_id,
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
                    WHEN st.store_country = 'EUREM' AND a.country_code IN ('SE', 'NL', 'DK', 'IT', 'BE')
                        THEN a.country_code
                    WHEN st.store_country = 'EUREM' THEN 'NL'
                    ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END        AS vat_rate_ship_to_country,
                CASE
                    WHEN credit_activity_type = 'Issued'
                        AND dc.credit_issuance_reason IN
                            ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                             'Fixed To Variable Conversion', 'Token To Credit Conversion')
                        THEN 'Exclude'
                    WHEN credit_activity_type IN ('Transferred', 'Converted To Credit') THEN 'Exclude'
                    ELSE 'Include' END                                                          AS original_credit_activity_type_action,
                dsdc.store_id                                                                   AS credit_store_id,
                dsdc.store_brand                                                                AS credit_store_brand,
                dsdc.store_country                                                              AS credit_store_country,
                dsdc.store_currency                                                             AS credit_store_currency,
                dc.credit_issued_hq_datetime,
                TRUE                                                                            AS is_deleted
FROM _fact_credit_event__credit_base dc
         JOIN lake_consolidated.ultra_merchant.membership_token mt
              ON mt.membership_token_id = dc.credit_id
         JOIN _fact_credit_event__membership_token_transaction_history mtt
              ON mtt.membership_token_id = dc.credit_id AND
                 dc.credit_type = 'Token'
         JOIN lake_consolidated.ultra_merchant.membership_token_transaction_type mttt
              ON mttt.membership_token_transaction_type_id = mtt.membership_token_transaction_type_id
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = dc.store_id
         LEFT JOIN lake_consolidated.ultra_merchant.membership_token_transaction_reason mttr
                   ON mttr.membership_token_transaction_reason_id = mtt.membership_token_transaction_reason_id
         LEFT JOIN stg.fact_order ord
                   ON ord.order_id = mtt.object_id AND
                      mtt.object = 'order'
         LEFT JOIN lake_consolidated.ultra_merchant.address a
                   ON a.address_id = ord.shipping_address_id
         LEFT JOIN stg.dim_store st
                   ON st.store_id = ord.store_id;


INSERT INTO _fact_credit_event (
       credit_key,
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
       vat_rate_ship_to_country,
       original_credit_activity_type_action,
       credit_store_id,
       credit_store_brand,
       credit_store_country,
       credit_store_currency,
       credit_issued_hq_datetime,
       is_deleted
)
SELECT s.credit_key,
       s.credit_id,
       s.source_credit_id,
       s.administrator_id,
       s.credit_activity_local_datetime,
       s.credit_activity_source,
       s.credit_activity_source_reason,
       s.credit_activity_type,
       s.credit_activity_type_reason,
       s.activity_amount,
       s.redemption_order_id,
       s.source_redemption_order_id,
       s.redemption_store_id,
       s.vat_rate_ship_to_country,
       s.original_credit_activity_type_action,
       s.credit_store_id,
       s.credit_store_brand,
       s.credit_store_country,
       s.credit_store_currency,
       s.credit_issued_hq_datetime,
       TRUE AS is_deleted
FROM _fact_credit_event__membership_transactions_delete s
             JOIN stg.fact_credit_event fce
              ON fce.credit_id = s.credit_id AND
                 fce.credit_activity_type = s.credit_activity_type AND
                 fce.credit_activity_local_datetime = s.credit_activity_local_datetime AND
                 fce.credit_store_id = s.credit_store_id AND
                 IFNULL(fce.source_redemption_order_id, -1) = IFNULL(s.source_redemption_order_id, -1) AND
                 fce.credit_issued_hq_datetime = s.credit_issued_hq_datetime AND
                 fce.credit_activity_source = s.credit_activity_source
         LEFT JOIN _fact_credit_event fce_stg
                   ON fce_stg.credit_id = s.credit_id AND
                      fce_stg.credit_activity_type = s.credit_activity_type AND
                      fce_stg.credit_activity_local_datetime = s.credit_activity_local_datetime AND
                      fce_stg.credit_store_id = s.credit_store_id AND
                      IFNULL(fce_stg.source_redemption_order_id, -1) = IFNULL(s.source_redemption_order_id, -1) AND
                      fce_stg.credit_issued_hq_datetime = s.credit_issued_hq_datetime AND
                      fce_stg.credit_activity_source = s.credit_activity_source
WHERE fce_stg.credit_id IS NULL;

/*
-- Capture credit id changes so we can identify Missing SCT - Successful Order accurately
CREATE OR REPLACE TEMP TABLE _fact_credit_event__credit_id_changes AS
SELECT dc.credit_id, oc2.store_credit_id
FROM _fact_credit_event__credit_base dc
         JOIN lake_consolidated.ultra_merchant_history.order_credit oc
              ON dc.credit_id = oc.store_credit_id
         JOIN lake_consolidated.ultra_merchant_history.order_credit oc2
              ON oc.order_credit_id = oc2.order_credit_id
WHERE dc.credit_id <> oc2.store_credit_id;
*/

-- Handle deletions related to Missing SCT - Successful Order
CREATE OR REPLACE TEMP TABLE _fact_credit_event__missing_sct_stage AS
SELECT dc.credit_key,
       dc.credit_id,
       dc.credit_id                                                                                     AS source_credit_id,
       COALESCE(otd.object_id, -1)                                                                      AS administrator_id,
       CONVERT_TIMEZONE(dsdc.store_time_zone,
                        COALESCE(o.date_shipped, o.datetime_transaction, o.date_placed))::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       'Missing SCT - Successful Order'                                                                 AS credit_activity_source,
       'successful in order credit'                                                                     AS credit_activity_source_reason,
       'Redeemed'                                                                                       AS credit_activity_type,
       'Redeemed In Order'                                                                              AS credit_activity_type_reason,
       SUM(oc.amount)                                                                                   AS activity_amount,
       IFF(fo.order_status_key = 8, fo.master_order_id, fo.order_id)                                    AS redemption_order_id,
       oc.order_id                                                                                      AS source_redemption_order_id,
       fo.store_id                                                                                      AS redemption_store_id,
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
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END                         AS vat_rate_ship_to_country,
       CASE
           WHEN credit_activity_type = 'Issued'
               AND dc.credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion', 'Token To Credit Conversion')
               THEN 'Exclude'
           WHEN credit_activity_type IN
                ('Converted To Token', 'Converted To Variable', 'Transferred', 'Converted To Credit') THEN 'Exclude'
           ELSE 'Include' END                                                                           AS original_credit_activity_type_action,
       dsdc.store_id                                                                                    AS credit_store_id,
       dsdc.store_brand                                                                                 AS credit_store_brand,
       dsdc.store_country                                                                               AS credit_store_country,
       dsdc.store_currency                                                                              AS credit_store_currency,
       dc.credit_issued_hq_datetime
FROM _fact_credit_event__credit_base dc
         JOIN lake_consolidated.ultra_merchant.order_credit oc
              ON dc.credit_id = oc.store_credit_id
         JOIN lake_consolidated.ultra_merchant."ORDER" AS o
              ON o.order_id = oc.order_id
         JOIN stg.fact_order AS fo
              ON fo.order_id = o.order_id
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = dc.store_id
         JOIN stg.dim_store store
              ON store.store_id = fo.store_id
         JOIN lake_consolidated.ultra_merchant.statuscode scc
              ON scc.statuscode = o.payment_statuscode
         JOIN lake_consolidated.ultra_merchant.statuscode scc2
              ON scc2.statuscode = o.processing_statuscode
         JOIN lake_consolidated.ultra_merchant.store_credit sc
              ON sc.store_credit_id = oc.store_credit_id
         JOIN lake_consolidated.ultra_merchant.store_credit_transaction sct
              ON sct.store_credit_transaction_type_id = 20
                  AND sct.object_id = o.order_id
                  AND sct.store_credit_id = oc.store_credit_id
                  AND (CASE WHEN oc.hvr_is_deleted = 0 THEN sct.datetime_transaction IS NOT NULL ELSE TRUE END)
         LEFT JOIN lake_consolidated.ultra_merchant.address a
                   ON a.address_id = o.shipping_address_id
         LEFT JOIN (SELECT order_tracking_id, object_id
                    FROM lake_consolidated.ultra_merchant.order_tracking_detail
                    WHERE object = 'administrator'
                    QUALIFY
                        ROW_NUMBER() OVER (PARTITION BY order_tracking_id ORDER BY datetime_added, object_id) = 1) otd
                   ON otd.order_tracking_id = o.order_tracking_id
WHERE scc.label IN ('Paid', 'Refunded', 'Refunded (Partial)')
  AND dc.credit_type ILIKE '%credit%'
  AND scc2.label IN ('Shipped', 'Complete')
  AND oc.amount > 0
GROUP BY dc.credit_key,
         dc.credit_id,
         COALESCE(otd.object_id, -1),
         credit_activity_local_datetime,
         IFF(fo.order_status_key = 8, fo.master_order_id, fo.order_id),
         oc.order_id,
         fo.store_id,
         vat_rate_ship_to_country,
         original_credit_activity_type_action,
         dsdc.store_id,
         dsdc.store_brand,
         dsdc.store_country,
         dsdc.store_currency,
         dc.credit_issued_hq_datetime;

/* left join to make sure we are not adding anything to _fact_credit_event that already exists in _fact_credit_event
   also stg.fact_credit_event join makes sure we only process already existing data in FCE
*/
INSERT INTO _fact_credit_event (
       credit_key,
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
       vat_rate_ship_to_country,
       original_credit_activity_type_action,
       credit_store_id,
       credit_store_brand,
       credit_store_country,
       credit_store_currency,
       credit_issued_hq_datetime,
       is_deleted
)
SELECT s.credit_key,
       s.credit_id,
       s.source_credit_id,
       s.administrator_id,
       CAST(fce.credit_activity_local_datetime AS TIMESTAMP_NTZ(9)), -- using fce.credit_activity_local_datetime otherwise we produce multiple records
       s.credit_activity_source,
       s.credit_activity_source_reason,
       s.credit_activity_type,
       s.credit_activity_type_reason,
       s.activity_amount,
       s.redemption_order_id,
       s.source_redemption_order_id,
       s.redemption_store_id,
       s.vat_rate_ship_to_country,
       s.original_credit_activity_type_action,
       s.credit_store_id,
       s.credit_store_brand,
       s.credit_store_country,
       s.credit_store_currency,
       s.credit_issued_hq_datetime,
       TRUE AS is_deleted
FROM stg.fact_credit_event fce
         JOIN lake_consolidated.ultra_merchant."ORDER" AS o
              ON fce.redemption_order_id = o.order_id
         JOIN stg.dim_store ds
             ON fce.credit_store_id = ds.store_id
JOIN _fact_credit_event__missing_sct_stage s
              ON fce.credit_id = s.credit_id AND
                 fce.credit_activity_type = s.credit_activity_type AND
                 CAST(CONVERT_TIMEZONE(ds.store_time_zone,
                        COALESCE(o.date_shipped, o.datetime_transaction, o.date_placed)) AS TIMESTAMP_NTZ(9)) = CAST(s.credit_activity_local_datetime AS TIMESTAMP_NTZ(9)) AND
                 fce.credit_store_id = s.credit_store_id AND
                 fce.source_redemption_order_id = s.source_redemption_order_id AND
                 fce.credit_issued_hq_datetime = s.credit_issued_hq_datetime AND
                 fce.credit_activity_source = s.credit_activity_source AND
                 IFNULL(fce.administrator_id, -1) = IFNULL(s.administrator_id, -1)
    LEFT JOIN _fact_credit_event fce_stg
                   ON fce_stg.credit_id = s.credit_id AND
                      fce_stg.credit_activity_type = s.credit_activity_type AND
                      fce_stg.credit_activity_local_datetime = s.credit_activity_local_datetime AND
                      fce_stg.credit_store_id = s.credit_store_id AND
                      fce_stg.source_redemption_order_id = s.source_redemption_order_id AND
                      fce_stg.credit_issued_hq_datetime = s.credit_issued_hq_datetime AND
                      fce_stg.credit_activity_source = s.credit_activity_source
WHERE fce_stg.credit_id IS NULL;


-- Handle deletions related to Store Credit Transaction Log
CREATE OR REPLACE TEMP TABLE _fact_credit_event__store_credit_transaction_log_stg AS
SELECT dc.credit_key,
       dc.credit_id,
       dc.credit_id                                                                    AS source_credit_id,
       COALESCE(sct.administrator_id, -1)                                              AS administrator_id,
       CONVERT_TIMEZONE(dsdc.store_time_zone, sct.datetime_transaction)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
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
           WHEN ord.order_status_key = 8 AND sct.store_credit_transaction_type_id = 20 THEN ord.master_order_id
           WHEN sct.store_credit_transaction_type_id = 20 THEN sct.object_id
           END                                                                         AS redemption_order_id,
       IFF(sct.store_credit_transaction_type_id = 20, object_id, NULL)                 AS source_redemption_order_id,
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
           ELSE IFF(dsdc.store_country = 'EUREM', 'NL', dsdc.store_country) END        AS vat_rate_ship_to_country,
       CASE
           WHEN credit_activity_type = 'Issued'
               AND dc.credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion', 'Token To Credit Conversion')
               THEN 'Exclude'
           WHEN credit_activity_type IN ('Converted To Token', 'Converted To Variable', 'Transferred') THEN 'Exclude'
           ELSE 'Include' END                                                          AS original_credit_activity_type_action,
       dsdc.store_id                                                                   AS credit_store_id,
       dsdc.store_brand                                                                AS credit_store_brand,
       dsdc.store_country                                                              AS credit_store_country,
       dsdc.store_currency                                                             AS credit_store_currency,
       dc.credit_issued_hq_datetime,
       FALSE                                                                           AS is_deleted
FROM _fact_credit_event__credit_base dc
         JOIN stg.dim_store dsdc
              ON dsdc.store_id = dc.store_id
         JOIN lake_consolidated.ultra_merchant.store_credit_transaction sct
              ON sct.store_credit_id = dc.credit_id
                  AND dc.credit_type ILIKE '%credit%'
         JOIN lake_consolidated.ultra_merchant.store_credit sc
              ON sc.store_credit_id = sct.store_credit_id
         JOIN lake_consolidated.ultra_merchant.customer c
              ON c.customer_id = sc.customer_id
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit_transaction_reason sctr
                   ON sctr.store_credit_transaction_reason_id = sct.store_credit_transaction_reason_id
         LEFT JOIN lake_consolidated.ultra_merchant.store_credit_transaction_type sctt
                   ON sctt.store_credit_transaction_type_id = sct.store_credit_transaction_type_id
         LEFT JOIN stg.fact_order ord
                   ON ord.order_id = sct.object_id AND
                      sct.object = 'order'
         LEFT JOIN lake_consolidated.ultra_merchant.address a
                   ON a.address_id = ord.shipping_address_id
         LEFT JOIN stg.dim_store store
                   ON store.store_id = ord.store_id
WHERE NOT (sct.datetime_transaction IS NULL
    AND NOT (sct.datetime_transaction < '2017-06-01' AND sct.store_credit_transaction_type_id = 30 AND sct.amount = 0))
GROUP BY dc.credit_key,
         dc.credit_id,
         COALESCE(sct.administrator_id, -1),
         credit_activity_local_datetime,
         credit_activity_source,
         credit_activity_source_reason,
         credit_activity_type,
         credit_activity_type_reason,
         original_credit_activity_type_action,
         redemption_order_id,
         source_redemption_order_id,
         redemption_store_id,
         vat_rate_ship_to_country,
         dsdc.store_id,
         dsdc.store_brand,
         dsdc.store_country,
         dsdc.store_currency,
         dc.credit_issued_hq_datetime;

INSERT INTO _fact_credit_event (
       credit_key,
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
       vat_rate_ship_to_country,
       original_credit_activity_type_action,
       credit_store_id,
       credit_store_brand,
       credit_store_country,
       credit_store_currency,
       credit_issued_hq_datetime,
       is_deleted
)
SELECT s.credit_key,
       s.credit_id,
       s.source_credit_id,
       s.administrator_id,
       s.credit_activity_local_datetime,
       s.credit_activity_source,
       s.credit_activity_source_reason,
       s.credit_activity_type,
       s.credit_activity_type_reason,
       s.activity_amount,
       s.redemption_order_id,
       s.source_redemption_order_id,
       s.redemption_store_id,
       s.vat_rate_ship_to_country,
       s.original_credit_activity_type_action,
       s.credit_store_id,
       s.credit_store_brand,
       s.credit_store_country,
       s.credit_store_currency,
       s.credit_issued_hq_datetime,
       TRUE AS is_deleted
FROM _fact_credit_event__store_credit_transaction_log_stg s
             JOIN stg.fact_credit_event fce
              ON fce.credit_id = s.credit_id AND
                 fce.credit_activity_type = s.credit_activity_type AND
                 fce.credit_activity_local_datetime = s.credit_activity_local_datetime AND
                 fce.credit_store_id = s.credit_store_id AND
                 IFNULL(fce.source_redemption_order_id, -1) = IFNULL(s.source_redemption_order_id, -1) AND
                 fce.credit_issued_hq_datetime = s.credit_issued_hq_datetime AND
                 fce.credit_activity_source = s.credit_activity_source
         LEFT JOIN _fact_credit_event fce_stg
                   ON fce_stg.credit_id = s.credit_id AND
                      fce_stg.credit_activity_type = s.credit_activity_type AND
                      fce_stg.credit_activity_local_datetime = s.credit_activity_local_datetime AND
                      fce_stg.credit_store_id = s.credit_store_id AND
                      IFNULL(fce_stg.source_redemption_order_id, -1) = IFNULL(s.source_redemption_order_id, -1) AND
                      fce_stg.credit_issued_hq_datetime = s.credit_issued_hq_datetime AND
                      fce_stg.credit_activity_source = s.credit_activity_source
WHERE fce_stg.credit_id IS NULL;

-- handle deletes for bounceback endowments
INSERT INTO _fact_credit_event (
       credit_key,
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
       vat_rate_ship_to_country,
       original_credit_activity_type_action,
       credit_store_id,
       credit_store_brand,
       credit_store_country,
       credit_store_currency,
       credit_issued_hq_datetime,
       is_deleted
)
SELECT fce.credit_key,
       fce.credit_id,
       fce.source_credit_id,
       fce.administrator_id,
       fce.credit_activity_local_datetime::TIMESTAMP_NTZ,
       fce.credit_activity_source,
       fce.credit_activity_source_reason,
       fce.credit_activity_type,
       fce.credit_activity_type_reason,
       fce.credit_activity_gross_vat_local_amount,
       fce.redemption_order_id,
       fce.source_redemption_order_id,
       fce.redemption_store_id,
       fce.vat_rate_ship_to_country,
       fce.original_credit_activity_type_action,
       st.store_id,
       st.store_brand,
       st.store_country,
       st.store_currency,
       fce.credit_issued_hq_datetime,
       TRUE AS is_deleted
FROM stg.fact_credit_event AS fce
    JOIN _fact_credit_event__credit_base AS base
        ON base.credit_id = fce.credit_id
        AND base.credit_type = 'Giftcard'
    JOIN stg.dim_store AS st
        ON st.store_id = base.store_id
     LEFT JOIN _fact_credit_event fce_stg
               ON fce_stg.credit_id = fce.credit_id AND
                  fce_stg.credit_activity_type = fce.credit_activity_type AND
                  fce_stg.credit_activity_local_datetime = fce.credit_activity_local_datetime AND
                  fce_stg.credit_store_id = fce.credit_store_id AND
                  IFNULL(fce_stg.source_redemption_order_id, -1) = IFNULL(fce.source_redemption_order_id, -1) AND
                  fce_stg.credit_issued_hq_datetime = fce.credit_issued_hq_datetime AND
                  fce_stg.credit_activity_source = fce.credit_activity_source AND
                  fce_stg.source_credit_id = fce.source_credit_id
WHERE
    fce.credit_activity_source IN ('Gift Certificate Transaction Log', 'Missing GCT - Expired BB Endowment', 'Missing GCT - Check Source',
                                  'Managed Gift Card - Successful Order', 'Missing GCT - Successful Order', 'Missing GCT - Before 2017')
    AND fce_stg.credit_id IS NULL;

-- Handle historical activity related deletions and add them to the _fact_credit_event so that we can get accurate credit keys
CREATE OR REPLACE TEMP TABLE _fact_credit_event__historical_activity_stage AS
SELECT dc.credit_key,
       dc.credit_id,
       dc.credit_id                                                             AS source_credit_id,
       COALESCE(h.administrator_id, -1)                                         AS administrator_id,
       CONVERT_TIMEZONE(ds.store_time_zone, h.activity_datetime)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       h.activity_source                                                        AS credit_activity_source,
       h.activity_source_reason                                                 AS credit_activity_source_reason,
       h.activity_type                                                          AS credit_activity_type,
       ''                                                                       AS credit_activity_type_reason,
       SUM(activity_amount)                                                     AS activity_amount,
       h.redemption_order_id,
       h.redemption_order_id                                                    AS source_redemption_order_id,
       h.redemption_store_id,
       COALESCE(h.vat_rate_ship_to_country, ds.store_country)                   AS vat_rate_ship_to_country,
       CASE
           WHEN h.activity_type = 'Issued'
               AND dc.credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion')
               THEN 'Exclude'
           WHEN h.activity_type IN ('Converted To Token', 'Converted To Variable', 'Transferred') THEN 'Exclude'
           ELSE 'Include' END                                                   AS original_credit_activity_type_action,
       ds.store_id                                                              AS credit_store_id,
       ds.store_brand                                                           AS credit_store_brand,
       ds.store_country                                                         AS credit_store_country,
       ds.store_currency                                                        AS credit_store_currency,
       dc.credit_issued_hq_datetime,
       FALSE                                                                    AS is_deleted
FROM _fact_credit_event__credit_base dc
         JOIN stg.dim_store ds
              ON ds.store_id = dc.store_id
         JOIN reference.credit_historical_activity h
              ON h.store_credit_id = dc.credit_id
                  AND dc.credit_type ILIKE '%credit%'
         JOIN stg.fact_credit_event AS d
              ON d.credit_id = dc.credit_id
                  AND d.credit_store_id = ds.store_id
                  AND d.credit_activity_type = h.activity_type
                  AND IFNULL(d.redemption_order_id, -1) = IFNULL(h.redemption_order_id, -1)
                  AND d.credit_issued_hq_datetime = dc.credit_issued_hq_datetime
                  AND d.credit_activity_source = 'Store Credit Transaction Log'
         JOIN (SELECT sct.store_credit_id,
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
                 AND sct.hvr_is_deleted = 0
                 AND NOT (sct.datetime_transaction < '2017-06-01' AND sct.store_credit_transaction_type_id = 30 AND
                          sct.amount = 0)) AS a
              ON a.store_credit_id = h.store_credit_id
                  AND a.activity_type_cast_stmt = h.activity_type
                  AND a.datetime_transaction::DATE = h.activity_datetime::DATE
GROUP BY dc.credit_key,
         dc.credit_id,
         COALESCE(h.administrator_id, -1),
         CONVERT_TIMEZONE(ds.store_time_zone, h.activity_datetime)::TIMESTAMP_NTZ,
         h.activity_source,
         h.activity_source_reason,
         h.activity_type,
         COALESCE(h.vat_rate_ship_to_country, ds.store_country),
         h.redemption_order_id,
         h.redemption_store_id,
         h.vat_rate_ship_to_country,
         ds.store_id,
         ds.store_brand,
         ds.store_country,
         ds.store_currency,
         dc.credit_issued_hq_datetime,
         dc.credit_issuance_reason
UNION
SELECT dc.credit_key,
       dc.credit_id,
       dc.credit_id                                                             AS source_credit_id,
       COALESCE(h.administrator_id, -1)                                         AS administrator_id,
       CONVERT_TIMEZONE(ds.store_time_zone, h.activity_datetime)::TIMESTAMP_NTZ AS credit_activity_local_datetime,
       h.activity_source                                                        AS credit_activity_source,
       h.activity_source_reason                                                 AS credit_activity_source_reason,
       h.activity_type                                                          AS credit_activity_type,
       ''                                                                       AS credit_activity_type_reason,
       SUM(activity_amount)                                                     AS activity_amount,
       h.redemption_order_id,
       h.redemption_order_id                                                    AS source_redemption_order_id,
       h.redemption_store_id,
       COALESCE(h.vat_rate_ship_to_country, ds.store_country)                   AS vat_rate_ship_to_country,
       CASE
           WHEN h.activity_type = 'Issued'
               AND dc.credit_issuance_reason IN
                   ('Fixed To Variable Conversion', 'Giftco Roundtrip', 'Credit To Token Conversion',
                    'Fixed To Variable Conversion')
               THEN 'Exclude'
           WHEN h.activity_type IN ('Converted To Token', 'Converted To Variable', 'Transferred') THEN 'Exclude'
           ELSE 'Include' END                                                   AS original_credit_activity_type_action,
       ds.store_id                                                              AS credit_store_id,
       ds.store_brand                                                           AS credit_store_brand,
       ds.store_country                                                         AS credit_store_country,
       ds.store_currency                                                        AS credit_store_currency,
       dc.credit_issued_hq_datetime,
       FALSE                                                                    AS is_deleted
FROM _fact_credit_event__credit_base dc
         JOIN stg.dim_store ds
              ON ds.store_id = dc.store_id
         JOIN reference.credit_historical_activity h
              ON h.store_credit_id = dc.credit_id
                  AND dc.credit_type ILIKE '%credit%'
         JOIN stg.fact_credit_event AS d
              ON d.credit_id = dc.credit_id
                  AND d.credit_store_id = ds.store_id
                  AND d.credit_activity_type = h.activity_type
                  AND IFNULL(d.redemption_order_id, -1) = IFNULL(h.redemption_order_id, -1)
                  AND d.credit_issued_hq_datetime = dc.credit_issued_hq_datetime
                  AND d.credit_activity_source = 'Store Credit Transaction Log'
         JOIN lake_consolidated.ultra_merchant.store_credit_transaction AS sct
              ON sct.store_credit_transaction_type_id = 20
                  AND sct.object_id = h.redemption_order_id
                  AND sct.store_credit_id = h.store_credit_id
                  AND sct.datetime_transaction IS NOT NULL
                  AND sct.hvr_is_deleted = 0
GROUP BY dc.credit_key,
         dc.credit_id,
         COALESCE(h.administrator_id, -1),
         CONVERT_TIMEZONE(ds.store_time_zone, h.activity_datetime)::TIMESTAMP_NTZ,
         h.activity_source,
         h.activity_source_reason,
         h.activity_type,
         COALESCE(h.vat_rate_ship_to_country, ds.store_country),
         h.redemption_order_id,
         h.redemption_store_id,
         h.vat_rate_ship_to_country,
         ds.store_id,
         ds.store_brand,
         ds.store_country,
         ds.store_currency,
         dc.credit_issued_hq_datetime,
         dc.credit_issuance_reason;

-- left join to make sure we are not adding anything to _fact_credit_event that already exists in _fact_credit_event
INSERT INTO _fact_credit_event (
       credit_key,
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
       vat_rate_ship_to_country,
       original_credit_activity_type_action,
       credit_store_id,
       credit_store_brand,
       credit_store_country,
       credit_store_currency,
       credit_issued_hq_datetime,
       is_deleted
)
SELECT s.credit_key,
       s.credit_id,
       s.source_credit_id,
       s.administrator_id,
       s.credit_activity_local_datetime,
       s.credit_activity_source,
       s.credit_activity_source_reason,
       s.credit_activity_type,
       s.credit_activity_type_reason,
       s.activity_amount,
       s.redemption_order_id,
       s.source_redemption_order_id,
       s.redemption_store_id,
       s.vat_rate_ship_to_country,
       s.original_credit_activity_type_action,
       s.credit_store_id,
       s.credit_store_brand,
       s.credit_store_country,
       s.credit_store_currency,
       s.credit_issued_hq_datetime,
       TRUE AS is_deleted
FROM _fact_credit_event__historical_activity_stage s
         LEFT JOIN _fact_credit_event fce_stg
                   ON fce_stg.credit_id = s.credit_id AND
                      fce_stg.credit_activity_type = s.credit_activity_type AND
                      fce_stg.credit_activity_local_datetime = s.credit_activity_local_datetime AND
                      fce_stg.credit_store_id = s.credit_store_id AND
                      fce_stg.redemption_order_id = s.redemption_order_id AND
                      fce_stg.credit_issued_hq_datetime = s.credit_issued_hq_datetime
WHERE fce_stg.credit_id IS NULL;

-- putting exchange rates to convert source currency to USD by day into a table
CREATE OR REPLACE TEMPORARY TABLE _exchange_rate AS
SELECT er.src_currency,
       er.rate_date_pst,
       er.exchange_rate
FROM reference.currency_exchange_rate_by_date er
WHERE dest_currency = 'USD';

-- putting vat rates into a temp table
CREATE OR REPLACE TEMPORARY TABLE _vat_rate AS
SELECT REPLACE(country_code, 'GB', 'UK') AS country_code,
       dd.full_date,
       er.rate
FROM reference.vat_rate_history er
         JOIN data_model.dim_date dd ON er.start_date <= dd.full_date
    AND er.expires_date >= dd.full_date
WHERE dd.full_date <= CURRENT_DATE() + 7;


CREATE OR REPLACE TEMP TABLE _fact_credit_event__membership_price AS
SELECT DISTINCT
    base.credit_key,
    dc.original_credit_issued_local_datetime,
    dc.store_id,
    dc.customer_id,
    m.price
FROM _fact_credit_event AS base
    JOIN stg.dim_credit AS dc
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

CREATE OR REPLACE TEMPORARY TABLE _fact_credit_event_stg AS
SELECT
    fce.credit_key,
    fce.credit_id,
    fce.source_credit_id,
    dc.customer_id,
    fce.administrator_id,
    fce.credit_activity_type,
    fce.original_credit_activity_type_action,
    fce.credit_activity_type_reason,
    fce.credit_activity_local_datetime,
    fce.credit_activity_source,
    fce.credit_activity_source_reason,
    fce.credit_store_id,
    fce.redemption_order_id,
    fce.source_redemption_order_id,
    fce.redemption_store_id,
    fce.vat_rate_ship_to_country,
    IFNULL(vrh.rate, 0)                                                                            AS credit_activity_vat_rate,
    exch.exchange_rate                                                                             AS credit_activity_usd_conversion_rate,
    IFF(dc.credit_type = 'Token', 1, IFNULL(fce.activity_amount / NULLIF(m.price,0),1))            AS credit_activity_equivalent_count,
    fce.activity_amount                                                                            AS credit_activity_gross_vat_local_amount,
    fce.activity_amount / (1 + IFNULL(vrh.rate, 0))                                                AS credit_activity_local_amount,
    fce.activity_amount / (1 + IFNULL(vrh2.rate, 0))                                               AS activity_amount_local_amount_issuance_date,
    fce.credit_issued_hq_datetime,
    dc.is_test_customer,
    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY dc.credit_key ORDER BY fce.credit_activity_local_datetime DESC) = 1 THEN TRUE
        ELSE FALSE END AS is_current,
    fce.is_deleted
FROM _fact_credit_event AS fce
         JOIN stg.dim_credit AS dc
              ON dc.credit_key = fce.credit_key
    LEFT JOIN _fact_credit_event__membership_price AS m
        ON m.credit_key = fce.credit_key
    LEFT JOIN _exchange_rate exch
        ON exch.src_currency = fce.credit_store_currency -- add in exchange rate to convert to USD
        AND CAST(fce.credit_activity_local_datetime AS DATE) = rate_date_pst
    LEFT JOIN _vat_rate vrh -- join so you can get the vat rate
        ON vrh.country_code = fce.vat_rate_ship_to_country
        AND vrh.full_date = CAST(fce.credit_activity_local_datetime AS DATE)
    LEFT JOIN _vat_rate vrh2 -- join so you can get the vat rate
        ON vrh2.country_code = fce.vat_rate_ship_to_country
        AND vrh2.full_date = CAST(fce.credit_issued_hq_datetime AS DATE);
-- SELECT * FROM _fact_credit_event_stg WHERE credit_id = 13047933;

CREATE OR REPLACE TEMP TABLE _fact_credit_event__activation AS
SELECT
    stg.customer_id,
    fa.activation_key,
    fa.membership_event_key,
    fa.activation_local_datetime,
    fa.next_activation_local_datetime,
    fa.cancellation_local_datetime,
    fa.activation_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY stg.customer_id ORDER BY fa.activation_sequence_number, fa.activation_local_datetime) AS row_num
FROM (SELECT DISTINCT dc.customer_id FROM _fact_credit_event_stg AS fce JOIN stg.dim_credit AS dc ON dc.credit_key = fce.credit_key) AS stg
    JOIN stg.fact_activation AS fa
    	ON fa.customer_id = stg.customer_id
WHERE NOT NVL(fa.is_deleted, FALSE);

CREATE OR REPLACE TEMP TABLE _fact_credit_event__first_activation AS
SELECT
    stg.credit_key,
    stg.customer_id,
    a.activation_key,
    a.membership_event_key,
    a.activation_local_datetime,
    a.next_activation_local_datetime,
    a.activation_sequence_number,
    ROW_NUMBER() OVER (PARTITION BY stg.credit_key, stg.customer_id ORDER BY a.row_num) AS row_num
FROM _fact_credit_event_stg AS stg
    JOIN _fact_credit_event__activation AS a
		ON a.customer_id = stg.customer_id
        AND a.activation_local_datetime <= stg.credit_activity_local_datetime;

INSERT INTO stg.fact_credit_event_stg (
    credit_key,
    credit_id,
    source_credit_id,
    customer_id,
    activation_key,
    first_activation_key,
    is_bop_vip,
    administrator_id,
    credit_activity_type,
    original_credit_activity_type_action,
    credit_activity_type_reason,
    credit_activity_local_datetime,
    credit_activity_source,
    credit_activity_source_reason,
    credit_store_id,
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
    credit_issued_hq_datetime,
    is_test_customer,
    is_current,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
 )
SELECT
    r.credit_key,
    r.credit_id,
    r.source_credit_id,
    r.customer_id,
    COALESCE(fa.activation_key, -1) AS activation_key,
    COALESCE(ffa.activation_key, -1) AS first_activation_key,
       IFF(DATE_TRUNC(MONTH, CAST(r.credit_activity_local_datetime AS DATE)) > DATE_TRUNC(MONTH, CAST(fa.activation_local_datetime AS DATE)) AND
           DATE_TRUNC(MONTH, CAST(r.credit_activity_local_datetime AS DATE)) <= DATE_TRUNC(MONTH, CAST(fa.cancellation_local_datetime AS DATE)), 1, 0) AS is_bop_vip,
    COALESCE(r.administrator_id,-1)                                                                                                                    AS administrator_id,
    r.credit_activity_type,
    original_credit_activity_type_action,
    credit_activity_type_reason,
    r.credit_activity_local_datetime,
    credit_activity_source,
    credit_activity_source_reason,
    r.credit_store_id,
    IFNULL(r.redemption_order_id, -1) AS redemption_order_id,
    IFNULL(r.source_redemption_order_id, -1) AS source_redemption_order_id,
    IFF(IFNULL(r.redemption_store_id, -1) = 41, 26, IFNULL(r.redemption_store_id, -1))  AS redemption_store_id,
    vat_rate_ship_to_country,
    credit_activity_vat_rate,
    credit_activity_usd_conversion_rate,
    credit_activity_equivalent_count,
    credit_activity_gross_vat_local_amount,
    credit_activity_local_amount,
    activity_amount_local_amount_issuance_date,
    r.credit_issued_hq_datetime,
    r.is_test_customer,
    r.is_current,
    r.is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_credit_event_stg AS r
    LEFT JOIN _fact_credit_event__activation AS fa
        ON fa.customer_id = r.customer_id
        AND fa.activation_local_datetime <= r.credit_activity_local_datetime
        AND fa.next_activation_local_datetime > r.credit_activity_local_datetime
    LEFT JOIN _fact_credit_event__first_activation AS ffa
        ON ffa.credit_key = r.credit_key
        AND ffa.customer_id = r.customer_id
		AND ffa.row_num = 1
where credit_activity_type is not null;
-- Check for duplicates
-- SELECT credit_id, credit_activity_type, credit_activity_local_datetime, credit_store_id, redemption_order_id, credit_issued_hq_datetime FROM stg.fact_credit_event_stg GROUP BY 1,2,3,4,5,6 HAVING COUNT(1) > 1;

UPDATE stg.fact_credit_event AS fce
SET fce.is_deleted = TRUE,
    fce.meta_update_datetime = $execution_start_time
WHERE $is_full_refresh
    AND fce.is_deleted = FALSE
    AND NOT EXISTS (
        SELECT 1
        FROM stg.fact_credit_event_stg AS fces
        WHERE fce.credit_id = fces.credit_id
        AND fce.source_credit_id = fces.source_credit_id
        AND fce.credit_activity_type = fces.credit_activity_type
        AND fce.credit_activity_local_datetime = fces.credit_activity_local_datetime
        AND fce.credit_activity_source = fces.credit_activity_source
        AND fce.credit_store_id = fces.credit_store_id
        AND fce.source_redemption_order_id = fces.source_redemption_order_id
        AND fce.credit_issued_hq_datetime = fces.credit_issued_hq_datetime
        );
