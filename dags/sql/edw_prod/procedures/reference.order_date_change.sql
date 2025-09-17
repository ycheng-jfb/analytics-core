SET target_table  = 'reference.order_date_change';
SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

MERGE INTO stg.meta_table_dependency_watermark AS w
    USING (
        SELECT $target_table                                               AS table_name,
               NULLIF(dependent_table_name, 'reference.order_date_change') AS dependent_table_name,
               high_watermark_datetime                                     AS new_high_watermark_datetime
        FROM (
                 SELECT -- For self table
                        'reference.order_date_change' AS dependent_table_name,
                        '1900-01-01'                  AS high_watermark_datetime
                 UNION ALL

                 SELECT 'edw_prod.stg.fact_order'      AS dependent_table_name,
                        MAX(meta_update_datetime) AS high_watermark_datetime
                 FROM stg.fact_order
                 UNION ALL

                 SELECT 'edw_prod.stg.fact_refund'     AS dependent_table_name,
                        MAX(meta_update_datetime) AS high_watermark_datetime
                 FROM stg.fact_refund
             ) AS t
        ORDER BY COALESCE(t.dependent_table_name, '')
    ) AS s
    ON w.table_name = s.table_name
        AND EQUAL_NULL(w.dependent_table_name, s.dependent_table_name)
    WHEN NOT MATCHED THEN
        INSERT (
                table_name,
                dependent_table_name,
                high_watermark_datetime,
                new_high_watermark_datetime
            )
            VALUES (s.table_name,
                    s.dependent_table_name,
                    '1900-01-01', -- current high_watermark_datetime
                    s.new_high_watermark_datetime)
    WHEN MATCHED AND NOT EQUAL_NULL(w.new_high_watermark_datetime, s.new_high_watermark_datetime) THEN
        UPDATE
            SET w.new_high_watermark_datetime = s.new_high_watermark_datetime,
                w.meta_update_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

SET wm_fact_order = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_order'));
SET wm_fact_refund = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.stg.fact_refund'));

CREATE OR REPLACE TEMP TABLE _order_date_change__base AS
SELECT order_id,
       meta_original_order_id,
       NULL AS refund_id,
       customer_id,
       store_id,
       CAST(shipped_local_datetime AS DATE)             AS fo_shipped_local_date,
       CAST(payment_transaction_local_datetime AS DATE) AS fo_payment_transaction_local_date,
       NULL AS fr_refund_completion_date,
       o.order_sales_channel_key,
       'order' AS data_type
FROM stg.fact_order AS o
WHERE meta_update_datetime > $wm_fact_order
UNION ALL
SELECT f.source_order_id AS order_id,
       IFNULL(fo.meta_original_order_id,-1) AS meta_original_order_id,
       refund_id,
       f.customer_id,
       f.store_id,
       NULL AS fo_shipped_local_date,
       NULL AS fo_payment_transcation_local_date,
       CAST(refund_completion_local_datetime AS DATE)             AS fr_refund_completion_local_date,
       fo.order_sales_channel_key,
       'refund' AS data_type
FROM stg.fact_refund AS f
LEFT JOIN stg.fact_order fo
on f.order_id = fo.order_id
WHERE f.meta_update_datetime > $wm_fact_refund;


CREATE OR REPLACE TEMP TABLE _order_date_change__shipped_date_base AS
SELECT DISTINCT ob.order_id,
                oh.meta_original_order_id,
                ob.customer_id,
                ob.fo_shipped_local_date,
                CONVERT_TIMEZONE(stz.time_zone, oh.date_shipped::TIMESTAMP_TZ)::DATE     AS date_shipped_hist,
                CONVERT_TIMEZONE(stz.time_zone, oh.datetime_shipped::TIMESTAMP_TZ)::DATE AS datetime_shipped_hist
FROM _order_date_change__base AS ob
         JOIN lake_consolidated.ultra_merchant_history."ORDER" AS oh
              ON oh.order_id = ob.order_id
         LEFT JOIN reference.store_timezone stz
                   ON stz.store_id = oh.store_id
WHERE ob.data_type = 'order'
  AND oh.effective_end_datetime <> '9999-12-31 00:00:00.000000000 -08:00'-- the current record will flow into FSO from FO. This flag reduces processed data amount significantly
  AND (oh.datetime_shipped IS NOT NULL OR oh.date_shipped IS NOT NULL);


CREATE OR REPLACE TEMP TABLE _order_date_change__shipped_date AS
SELECT order_id,
       meta_original_order_id,
       customer_id,
       date_shipped_hist AS shipped_date,
       fo_shipped_local_date
FROM _order_date_change__shipped_date_base
WHERE date_shipped_hist IS NOT NULL
  AND date_shipped_hist <> fo_shipped_local_date

UNION

SELECT order_id,
       meta_original_order_id,
       customer_id,
       datetime_shipped_hist AS shipped_date,
       fo_shipped_local_date
FROM _order_date_change__shipped_date_base
WHERE datetime_shipped_hist IS NOT NULL
  AND datetime_shipped_hist <> fo_shipped_local_date;


CREATE OR REPLACE TEMP TABLE _order_date_change__transaction_date AS
SELECT ob.order_id,
       ob.meta_original_order_id,
       ob.customer_id,
       CONVERT_TIMEZONE(stz.time_zone, th.datetime_added::TIMESTAMP_TZ)::DATE AS transaction_date,
       ob.fo_payment_transaction_local_date
FROM lake_consolidated.ultra_merchant_history.payment_transaction_creditcard th
         JOIN _order_date_change__base ob ON th.order_id = ob.order_id
         LEFT JOIN reference.store_timezone stz
                   ON stz.store_id = ob.store_id
WHERE ob.data_type = 'order'
  AND LOWER(th.transaction_type) IN ('prior_auth_capture', 'sale_redirect')
  AND th.statuscode = 4001
  AND th.effective_end_datetime <> '9999-12-31 00:00:00.000000000 -08:00'
  AND th.datetime_added IS NOT NULL
  AND CONVERT_TIMEZONE(stz.time_zone, th.datetime_added::TIMESTAMP_TZ)::DATE <> ob.fo_payment_transaction_local_date

UNION

SELECT ob.order_id,
       ob.meta_original_order_id,
       ob.customer_id,
       CONVERT_TIMEZONE(stz.time_zone, pt.datetime_added::TIMESTAMP_TZ)::DATE AS transaction_date,
       ob.fo_payment_transaction_local_date
FROM lake_consolidated.ultra_merchant_history.payment_transaction_psp pt
         JOIN _order_date_change__base ob ON pt.order_id = ob.order_id
         LEFT JOIN reference.store_timezone stz
                   ON stz.store_id = ob.store_id
WHERE ob.data_type = 'order'
  AND LOWER(pt.transaction_type) IN ('prior_auth_capture', 'sale_redirect')
  AND pt.statuscode IN (4001, 4040)
  AND pt.effective_end_datetime <> '9999-12-31 00:00:00.000000000 -08:00'
  AND CONVERT_TIMEZONE(stz.time_zone, pt.datetime_added::TIMESTAMP_TZ)::DATE <> ob.fo_payment_transaction_local_date

UNION

SELECT ob.order_id,
       ob.meta_original_order_id,
       ob.customer_id,
       CONVERT_TIMEZONE(stz.time_zone, ptc.datetime_added::TIMESTAMP_TZ)::DATE AS transaction_date,
       ob.fo_payment_transaction_local_date
FROM lake_consolidated.ultra_merchant_history.payment_transaction_cash ptc
         JOIN _order_date_change__base ob
              ON ob.order_id = ptc.order_id
         LEFT JOIN reference.store_timezone stz
                   ON stz.store_id = ob.store_id
WHERE ob.data_type = 'order'
  AND LOWER(ptc.transaction_type) = 'sale'
  AND ptc.statuscode = 4001
  AND ptc.effective_end_datetime <> '9999-12-31 00:00:00.000000000 -08:00'
  AND CONVERT_TIMEZONE(stz.time_zone, ptc.datetime_added::TIMESTAMP_TZ)::DATE <> ob.fo_payment_transaction_local_date;


CREATE OR REPLACE TEMP TABLE _order_date_change__refund_date AS
SELECT ob.order_id,
       ob.meta_original_order_id,
       ob.refund_id,
       ob.customer_id,
       CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(r.datetime_refunded, 'America/Los_Angeles')) :: DATE  AS refund_completion_date
FROM _order_date_change__base ob
    JOIN lake_consolidated.ultra_merchant_history.refund r
        ON ob.refund_id = r.refund_id
    LEFT JOIN reference.store_timezone stz
        ON stz.store_id = ob.store_id
WHERE data_type = 'refund'
  AND r.effective_end_datetime <> '9999-12-31 00:00:00.000000000 -08:00'
  AND CONVERT_TIMEZONE(stz.time_zone, public.udf_to_timestamp_tz(r.datetime_refunded, 'America/Los_Angeles')) :: DATE <> IFNULL(ob.fr_refund_completion_date,'1900-01-01') ;

/* collecting date changes for dropship split master orders */
CREATE OR REPLACE TEMP TABLE _order_date_change__dropship_master_base AS
SELECT base.order_id,
       base.meta_original_order_id,
       base.customer_id,
       base.fo_shipped_local_date AS shipped_date_current,
       base.fo_payment_transaction_local_date AS payment_transaction_date_current,
       tt.shipped_local_datetime::DATE AS shipped_date_time_travel,
       tt.payment_transaction_local_datetime::DATE payment_transaction_date_time_travel
FROM _order_date_change__base AS base
         JOIN stg.dim_order_sales_channel AS osc
              ON osc.order_sales_channel_key = base.order_sales_channel_key
         JOIN lake_consolidated.ultra_merchant.order_classification AS oc
              ON oc.order_id = base.order_id
        LEFT JOIN stg.fact_order AT(OFFSET => -60*60*5) AS tt
            ON tt.order_id = base.order_id
WHERE osc.is_third_party = TRUE
  AND oc.order_type_id = 37 -- split master
  AND base.data_type = 'order'
  AND (base.fo_payment_transaction_local_date IS NOT NULL OR base.fo_shipped_local_date IS NOT NULL);


CREATE OR REPLACE TEMP TABLE _order_date_change__dropship_master AS
SELECT DISTINCT order_id, meta_original_order_id, customer_id, old_date
FROM (SELECT order_id, meta_original_order_id, customer_id, shipped_date_time_travel AS old_date
      FROM _order_date_change__dropship_master_base
      WHERE shipped_date_time_travel IS NOT NULL
        AND shipped_date_current IS NOT NULL
        AND shipped_date_current <> shipped_date_time_travel
      UNION ALL
      SELECT order_id, meta_original_order_id, customer_id, payment_transaction_date_time_travel AS old_date
      FROM _order_date_change__dropship_master_base
      WHERE payment_transaction_date_time_travel IS NOT NULL
        AND payment_transaction_date_current IS NOT NULL
        AND payment_transaction_date_current <> payment_transaction_date_time_travel);


CREATE OR REPLACE TEMP TABLE _order_date_change__stg AS
SELECT order_id, meta_original_order_id, customer_id, shipped_date AS date, 'order' as data_type
FROM _order_date_change__shipped_date
UNION
SELECT order_id, meta_original_order_id, customer_id, transaction_date, 'order' AS data_type
FROM _order_date_change__transaction_date
UNION
SELECT order_id, meta_original_order_id, customer_id, refund_completion_date, 'refund' AS data_type
FROM _order_date_change__refund_date
UNION
SELECT order_id, meta_original_order_id, customer_id, old_date, 'order' AS data_type
FROM _order_date_change__dropship_master;


MERGE INTO reference.order_date_change t
    USING _order_date_change__stg s ON
            s.order_id = t.order_id AND
            s.date = t.date AND
            s.data_type = t.data_type
    WHEN MATCHED THEN UPDATE SET
        t.meta_update_datetime = $execution_start_time
    WHEN NOT MATCHED THEN INSERT
        (
         order_id,
         meta_original_order_id,
         customer_id,
         date,
         data_type,
         meta_create_datetime,
         meta_update_datetime
            )
        VALUES (order_id,
                meta_original_order_id,
                customer_id,
                date,
                data_type,
                $execution_start_time,
                $execution_start_time);

UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = IFF(dependent_table_name IS NOT NULL, new_high_watermark_datetime,
        (SELECT MAX(meta_update_datetime) AS new_high_watermark_datetime FROM reference.order_date_change)),
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
