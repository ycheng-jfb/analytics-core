CREATE OR REPLACE FUNCTION lake.public.udf_to_timestamp_tz(dt TIMESTAMP_NTZ, tz VARCHAR)
    RETURNS TIMESTAMP_TZ
AS
$$
    timestamp_tz_from_parts(
            year(dt),
            month(dt),
            day(dt),
            hour(dt),
            minute(dt),
            second(dt),
            date_part(NANOSECOND, dt),
            tz
        )
$$;
