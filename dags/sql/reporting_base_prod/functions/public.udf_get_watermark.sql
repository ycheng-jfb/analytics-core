create function public.udf_get_watermark(table_name VARCHAR, dependent_table_name VARCHAR)
    returns TIMESTAMP_LTZ
as
$$
    SELECT NVL(tdw.high_watermark_datetime, '1900-01-01':: TIMESTAMP_LTZ) AS watermark_datetime
    FROM public.meta_table_dependency_watermark tdw
    WHERE lower(tdw.table_name) = lower(table_name)
    AND EQUAL_NULL(lower(tdw.dependent_table_name), lower(dependent_table_name))
$$;
