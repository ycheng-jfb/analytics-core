CREATE OR REPLACE TEMPORARY TABLE _final_table_for_fin AS
SELECT YEAR ((SELECT credit_active_as_of_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))::TEXT || '-' || UPPER (MONTHNAME((SELECT credit_active_as_of_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))) ||
    DAY ((SELECT credit_active_as_of_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))::TEXT AS credit_outstanding_date, YEAR ((SELECT credit_issued_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))::TEXT || '-' || UPPER (MONTHNAME((SELECT credit_issued_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))) ||
    DAY ((SELECT credit_issued_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))::TEXT AS credit_issued_max_date, YEAR ((SELECT vip_cohort_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))::TEXT || '-' || UPPER (MONTHNAME((SELECT vip_cohort_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))) ||
    DAY ((SELECT vip_cohort_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))::TEXT AS vip_cohort_max_date, YEAR ((SELECT member_status_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))::TEXT || '-' || UPPER (MONTHNAME((SELECT member_status_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))) ||
    DAY ((SELECT member_status_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=1))::TEXT AS member_status_classification_date, store_brand, store_country, store_region, member_status, first_vip_cohort, date_flag, CASE
    WHEN outstanding_count >= 10 THEN '10+'
    ELSE CAST (outstanding_count AS VARCHAR)
END AS outstanding_count_bucket,
       COUNT(DISTINCT customer_id)                     AS customer_count,
       SUM(outstanding_amount)                         AS outstanding_amount,
       SUM(outstanding_count)                          AS outstanding_count
FROM month_end.fin008_by_customer
WHERE date_flag=1
GROUP BY credit_outstanding_date,
         credit_issued_max_date,
         vip_cohort_max_date,
         member_status_classification_date,
         store_brand,
         store_country,
         store_region,
         member_status,
         first_vip_cohort,
         date_flag,
         outstanding_count_bucket
UNION ALL
SELECT
    YEAR ((SELECT credit_active_as_of_date --
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))::TEXT || '-' || UPPER (MONTHNAME((SELECT credit_active_as_of_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))) ||
    DAY ((SELECT credit_active_as_of_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))::TEXT AS credit_outstanding_date, YEAR ((SELECT credit_issued_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))::TEXT || '-' || UPPER (MONTHNAME((SELECT credit_issued_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))) ||
    DAY ((SELECT credit_issued_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))::TEXT AS credit_issued_max_date, YEAR ((SELECT vip_cohort_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))::TEXT || '-' || UPPER (MONTHNAME((SELECT vip_cohort_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))) ||
    DAY ((SELECT vip_cohort_max_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))::TEXT AS vip_cohort_max_date, YEAR ((SELECT member_status_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))::TEXT || '-' || UPPER (MONTHNAME((SELECT member_status_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))) ||
    DAY ((SELECT member_status_date
    FROM month_end.dates_for_fin008 WHERE num_for_recursion=2))::TEXT AS member_status_classification_date, store_brand, store_country, store_region, member_status, first_vip_cohort, date_flag, CASE
    WHEN outstanding_count >= 10 THEN '10+'
    ELSE CAST (outstanding_count AS VARCHAR)
END AS outstanding_count_bucket,
       COUNT(DISTINCT customer_id)                     AS customer_count,
       SUM(outstanding_amount)                         AS outstanding_amount,
       SUM(outstanding_count)                          AS outstanding_count
FROM month_end.fin008_by_customer
WHERE date_flag=2
GROUP BY credit_outstanding_date,
         credit_issued_max_date,
         vip_cohort_max_date,
         member_status_classification_date,
         store_brand,
         store_country,
         store_region,
         member_status,
         first_vip_cohort,
         date_flag,
         outstanding_count_bucket;


MERGE INTO month_end.fin008_outstanding_credit_count_by_status foc
    USING _final_table_for_fin ft
    ON foc.credit_outstanding_date = ft.credit_outstanding_date
        AND foc.credit_issued_max_date = ft.credit_issued_max_date
        AND foc.vip_cohort_max_date = ft.vip_cohort_max_date
        AND foc.member_status_classification_date = ft.member_status_classification_date
        AND foc.store_brand = ft.store_brand
        AND foc.store_country = ft.store_country
        AND foc.store_region = ft.store_region
        AND foc.member_status = ft.member_status
        AND foc.first_vip_cohort = ft.first_vip_cohort
        AND foc.outstanding_count_bucket = ft.outstanding_count_bucket
    WHEN MATCHED THEN
        UPDATE SET foc.customer_count = ft.customer_count,
            foc.outstanding_amount = ft.outstanding_amount,
            foc.outstanding_count = ft.outstanding_count
    WHEN NOT MATCHED THEN
        INSERT (credit_outstanding_date,
                credit_issued_max_date,
                vip_cohort_max_date,
                member_status_classification_date,
                store_brand,
                store_country,
                store_region,
                member_status,
                first_vip_cohort,
                outstanding_count_bucket,
                customer_count,
                outstanding_amount,
                outstanding_count)
            VALUES (ft.credit_outstanding_date,
                    ft.credit_issued_max_date,
                    ft.vip_cohort_max_date,
                    ft.member_status_classification_date,
                    ft.store_brand,
                    ft.store_country,
                    ft.store_region,
                    ft.member_status,
                    ft.first_vip_cohort,
                    ft.outstanding_count_bucket,
                    ft.customer_count,
                    ft.outstanding_amount,
                    ft.outstanding_count);
