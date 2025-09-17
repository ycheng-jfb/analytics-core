create function stg.GET_DATE_RANGE_TZ(START_DATE DATE, END_DATE DATE, TZ VARCHAR)
    returns TABLE (DATE TIMESTAMP_TZ)
as
$$
    SELECT
        DATEADD(DAY, seq4(), sd.start_date_tz) AS date
    FROM TABLE (GENERATOR(ROWCOUNT => 65535)) AS t
    CROSS JOIN (SELECT
                    public.udf_to_timestamp_tz(start_date, tz) AS start_date_tz
        ) sd
    WHERE
        date <= public.udf_to_timestamp_tz(end_date, tz)
$$;
