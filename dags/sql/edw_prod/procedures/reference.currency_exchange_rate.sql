SET meta_datetime = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY  TABLE  _exchange_rate_base
AS
SELECT src_currency,
    dest_currency,
    exchange_rate,
    rate_date AS rate_datetime,
    DATEADD('day',1,rate_date::DATE)::TIMESTAMP_LTZ AS effective_start_datetime,
    hash(src_currency,dest_currency,rate_date,exchange_rate) as meta_row_hash
FROM (SELECT DISTINCT 'USD' AS src_currency,
        'USD' AS dest_currency,
        1 AS exchange_rate,
        Exch_Date::DATE AS rate_date
    FROM lake.merlin.Fx_Rate_Dtl
    UNION ALL
    SELECT DISTINCT 'EUR' AS src_currency,
        'EUR' AS dest_currency,
        1 AS exchange_rate,
        Exch_Date::DATE AS rate_date
    FROM lake.merlin.Fx_Rate_Dtl
    UNION ALL
        SELECT src_currency,
        dest_currency,
        exchange_rate,
        rate_date
    FROM (SELECT src.ISO AS src_currency,
            dest.ISO AS dest_currency,
            bid AS exchange_rate,
            Exch_Date::DATETIME AS rate_date,
            ROW_NUMBER() OVER (PARTITION BY src.ISO, dest.ISO, Exch_Date::DATE ORDER BY Exch_Date DESC) rnk
        FROM lake.merlin.Currency src
        JOIN lake.merlin.FX_RATE_CURRENCY b ON src.oid=b.Source_Currency
        JOIN lake.merlin.Currency dest ON dest.oid=b.Dest_Currency
        JOIN lake.merlin.FX_RATE_DTL rates ON b.oid=FX_RATE_CURRENCY
    ) t1
    WHERE t1.rnk = 1);

CREATE OR REPLACE TEMPORARY TABLE _exchange_rate_final AS
SELECT src_currency,
    dest_currency,
    rate_datetime,
    exchange_rate,
    effective_start_datetime,
    effective_end_datetime,
    hash(src_currency,dest_currency,rate_datetime,exchange_rate,effective_start_datetime,effective_end_datetime) AS meta_row_hash
FROM (SELECT src_currency,
        dest_currency,
        rate_datetime,
        exchange_rate,
        effective_start_datetime,
        COALESCE (dateadd(ms, -1, LEAD(effective_start_datetime) OVER ( PARTITION BY src_currency,dest_currency ORDER BY rate_datetime)),'9999-12-31') AS effective_end_datetime
    FROM _exchange_rate_base) erbt;

MERGE INTO REFERENCE.CURRENCY_EXCHANGE_RATE t
using _exchange_rate_final s
    on t.SRC_CURRENCY = s.src_currency
    and t.DEST_CURRENCY=s.dest_currency
    and t.rate_datetime=s.rate_datetime
WHEN NOT MATCHED THEN INSERT (src_currency,dest_currency,exchange_rate,rate_datetime,effective_start_datetime,effective_end_datetime,meta_row_hash,meta_create_datetime,meta_update_datetime)
VALUES (src_currency,dest_currency,exchange_rate,rate_datetime,effective_start_datetime,effective_end_datetime,meta_row_hash,$meta_datetime,$meta_datetime)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN UPDATE SET
    t.effective_start_datetime = s.effective_start_datetime,
    t.effective_end_datetime = s.effective_end_datetime,
    t.EXCHANGE_RATE = s.exchange_rate,
    t.META_UPDATE_DATETIME = $meta_datetime,
    t.meta_row_hash = s.meta_row_hash;

----Temporary fix that needs to be removed after adding permanent fix
DELETE
FROM reference.currency_exchange_rate
WHERE rate_datetime = '2024-07-19 00:00:00.000'
  AND NOT ((src_currency = 'USD' AND dest_currency = 'USD') OR (src_currency = 'EUR' AND dest_currency = 'EUR'));
