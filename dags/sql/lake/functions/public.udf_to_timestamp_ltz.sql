CREATE OR REPLACE FUNCTION lake.public.udf_to_timestamp_ltz(dt TIMESTAMP_NTZ, tz VARCHAR)
    RETURNS TIMESTAMP_LTZ
AS
$$
    lake.public.udf_to_timestamp_tz(dt, tz)::TIMESTAMP_LTZ
$$;
