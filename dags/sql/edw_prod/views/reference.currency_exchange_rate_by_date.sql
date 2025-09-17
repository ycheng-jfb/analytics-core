CREATE VIEW IF NOT EXISTS reference.currency_exchange_rate_by_date
AS
SELECT
    cer.src_currency,
    cer.dest_currency,
    dates.date::date AS rate_date_pst,
    cer.rate_datetime,
    cer.exchange_rate
FROM TABLE (stg.get_date_range_tz('2010-01-01'::DATE, '2050-01-01'::DATE, 'America/Los_Angeles')) AS dates
JOIN reference.currency_exchange_rate AS cer
    ON dates.date BETWEEN cer.effective_start_datetime AND cer.effective_end_datetime
    AND dates.date <= dateadd(day, 1, current_timestamp());
