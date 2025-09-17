create function stg.GET_DATE_RANGE(START_DATE DATE, END_DATE DATE)
    returns TABLE (DATE DATE)
as
$$
    WITH dt AS (
        SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY NULL) - 1, '1900-01-01'::date) AS date
        FROM TABLE(GENERATOR(ROWCOUNT => 65535)) AS t
        )
    SELECT date
    FROM dt
	WHERE date BETWEEN start_date AND end_date
	$$;
