CREATE OR REPLACE TEMPORARY TABLE _global_fpa_forecast AS
SELECT CASE
           WHEN store_name = 'JFUS' then 'JF US'
           WHEN store_name = 'JFCA' then 'JF CA'
           WHEN store_name = 'ShoeDazzle' then 'SD US'
           WHEN store_name = 'FLUS' then 'FL US'
           WHEN store_name = 'FLCA' then 'FL CA'
           WHEN store_name = 'Fabkids' then 'FK US'
           WHEN store_name = 'Savage X NA' then 'SX US'
           WHEN store_name IN  ('Lingerie', 'Lingerie NA') then 'SX US'
           WHEN store_name = 'Shapewear NA' then 'FL US' -- per Damon's request: add YT NA into FL US until DB50 has YT breakout
           ELSE store_name
           END AS store_name,
           billing_month,
           sum(total_credit_billings) as total_credit_billings,
           sum(m2_tenure_vips) as m2_tenure_vips,
           sum(m2_plus_vips) as m2_plus_vips,
           sum(m2_credit_billings) as m2_credit_billings,
           sum(m2_plus_credit_billings) as m2_plus_credit_billings,
           iff(sum(m2_tenure_vips) = 0, 0, sum(m2_credit_billings) / sum(m2_tenure_vips))  as m2_billing_rate,
           iff(sum(M2_PLUS_VIPS) = 0, 0, sum(m2_plus_credit_billings) / sum(m2_plus_vips)) as m2_plus_billing_rate
FROM lake.fpa.na_fpa_forecast
WHERE store_name in ('JFUS', 'JFCA', 'ShoeDazzle', 'Fabkids', 'FLUS', 'FLCA',
                          'Lingerie', 'Savage X NA', 'Lingerie NA', 'Shapewear NA')
group by 1, 2
UNION
SELECT CASE
           WHEN store_name = 'JustFab DE' THEN 'JF DE'
           WHEN store_name = 'JustFab FR' THEN 'JF FR'
           WHEN store_name = 'JustFab UK' THEN 'JF UK'
           WHEN store_name = 'JustFab ES' THEN 'JF ES'
           WHEN store_name = 'JustFab NL' THEN 'JF NL'
           WHEN store_name = 'JustFab DK' THEN 'JF DK'
           WHEN store_name = 'JustFab SE' THEN 'JF SE'
           WHEN store_name = 'Fabletics DE' THEN 'FL DE'
           WHEN store_name = 'Fabletics FR' THEN 'FL FR'
           WHEN store_name = 'Fabletics UK' THEN 'FL UK'
           WHEN store_name = 'Fabletics ES' THEN 'FL ES'
           WHEN store_name = 'Fabletics NL' THEN 'FL NL'
           WHEN store_name = 'Fabletics DK' THEN 'FL DK'
           WHEN store_name = 'Fabletics SE' THEN 'FL SE'
           WHEN store_name = 'SavageX DE' THEN 'SX DE'
           WHEN store_name = 'SavageX FR' THEN 'SX FR'
           WHEN store_name = 'SavageX UK' THEN 'SX UK'
           WHEN store_name = 'SavageX ES' THEN 'SX ES'
           WHEN store_name = 'SavageX NL' THEN 'SX EU'
           WHEN store_name = 'SavageX NL (.eu)' THEN 'SX EU'
           --else STORE_NAME
           END AS store_name,
       billing_month,
       total_credit_billings,
       m2_tenure_vips,
       m2_plus_vips,
       m2_credit_billings,
       m2_plus_credit_billings,
       m2_billing_rate,
       m2_plus_billing_rate
FROM lake.fpa.eu_fpa_forecast
WHERE  ((store_name IN
           ('JustFab DE', 'JustFab FR', 'JustFab UK', 'JustFab ES', 'JustFab NL', 'JustFab DK', 'JustFab SE')
        OR store_name LIKE '%%Fabletics DE%%' OR store_name LIKE '%%Fabletics FR%%' OR store_name LIKE '%%Fabletics UK%%'
        OR store_name LIKE '%%Fabletics ES%%' OR store_name LIKE '%%Fabletics NL%%' OR store_name LIKE 'Fabletics DK%%'
        OR store_name LIKE '%%Fabletics SE%%'
        OR store_name LIKE '%%SavageX DE%%'
        OR store_name LIKE '%%SavageX FR%%'
        OR store_name LIKE '%%SavageX UK%%'
        OR store_name LIKE '%%SavageX ES%%'
        OR store_name LIKE '%%SavageX NL%%'));

CREATE OR REPLACE TEMPORARY TABLE _bor AS
SELECT
        month AS billing_month,
        iff(store_name = 'SX NL', 'SX EU', store_name) as store_name,
        m1_new_vips,
        first_successful_new_vips,
        m1_grace_vips,
        first_successful_grace_vips,
        billed_credit,
        first_successful_billed_credit_rate_trailing_6_months,
        retry_rate,
        retry_rate_trailing_6_months
FROM reporting_prod.gms.gms_operational_billing_history;

CREATE OR REPLACE TEMPORARY TABLE _fpa_and_bor_calc1 AS
SELECT
        b.billing_month,
        b.store_name,
        b.m1_new_vips,
        --to_number(b.FIRST_SUCCESSFUL_NEW_VIPS) as FIRST_SUCCESSFUL_NEW_VIPS,
        to_decimal(to_number(b.m1_new_vips * m2_billing_rate)) AS first_successful_new_vips,
        b.m1_grace_vips,
        --to_number(b.FIRST_SUCCESSFUL_GRACE_VIPS) as FIRST_SUCCESSFUL_GRACE_VIPS,
        to_decimal(to_number(b.m1_grace_vips * m2_billing_rate)) AS first_successful_grace_vips,
        b.billed_credit,
        b.first_successful_billed_credit_rate_trailing_6_months,
        b.retry_rate,
        b.retry_rate_trailing_6_months,
        f.store_name AS store_name1,
        f.m2_tenure_vips,
        f.m2_plus_vips,
        f.m2_credit_billings,
        f.m2_plus_credit_billings,
        f.m2_billing_rate AS m2_billing_credit_rate_fpa,
        f.m2_plus_billing_rate,
        f.billing_month AS billing_month1,
        f.total_credit_billings,
        to_number(b.m1_new_vips * m2_billing_rate) AS first_successful_new_vips_fixed,
        to_number(b.m1_grace_vips * m2_billing_rate) AS first_successful_grace_vips_fixed
FROM _bor b
JOIN _global_fpa_forecast f ON f.billing_month=b.billing_month AND f.store_name=b.store_name;

CREATE OR REPLACE TEMPORARY TABLE _fpa_and_bor_filter1 AS
SELECT
        billing_month,
        store_name AS storefront,
        m1_new_vips,
        first_successful_new_vips,
        m1_grace_vips,
        first_successful_grace_vips,
        billed_credit AS first_successful_billed_credit_rate,
        first_successful_billed_credit_rate_trailing_6_months AS first_successful_billed_credit_rate_trailing,
        retry_rate,
        retry_rate_trailing_6_months,
        m2_tenure_vips,
        m2_plus_vips,
        m2_credit_billings AS fpa_m2_credit_billings,
        m2_plus_credit_billings AS fpa_m2_plus_credit_billings,
        m2_billing_credit_rate_fpa,
        m2_plus_billing_rate,
        m2_credit_billings + m2_plus_credit_billings AS total_billing_fpa_forecast
    FROM _fpa_and_bor_calc1
    WHERE store_name NOT IN ('FL US', 'FL CA');
    --where (trim(STORE_NAME) != 'FL US' or trim(STORE_NAME) != 'FL CA');

CREATE OR REPLACE TEMPORARY TABLE _fpa_and_bor_filter2_flna AS
SELECT
        billing_month,
        store_name,
        m1_new_vips,
        m1_grace_vips,
        billed_credit,
        first_successful_billed_credit_rate_trailing_6_months,
        retry_rate,
        retry_rate_trailing_6_months,
        total_credit_billings,
        m2_tenure_vips,
        m2_plus_vips
    FROM _fpa_and_bor_calc1
    WHERE store_name = 'FL US' OR store_name = 'FL CA';

CREATE OR REPLACE TEMPORARY TABLE _anaplan_vip_tenure AS
SELECT
    row_number() OVER (ORDER BY store,vip_cohort,month) AS id,
    store,
    vip_cohort,
    month,
    tenure,
    value,
    credit_billers
    FROM (
        SELECT
            CASE report_mapping
                WHEN 'FL+SC-TREV-NA' THEN 'FLNorth America'
                WHEN 'FL+SC-TREV-CA' THEN 'FLCanada'
            END AS store,
            vip_cohort,
            month_date AS month,
            tenure,
            SUM(metric_count) AS value,
            SUM(credit_billers) AS credit_billers
        FROM edw_prod.reporting.vip_tenure_final_output
        WHERE report_mapping IN ('FL+SC-TREV-NA', 'FL+SC-TREV-CA')
        GROUP BY
            CASE report_mapping
                WHEN 'FL+SC-TREV-NA' THEN 'FLNorth America'
                WHEN 'FL+SC-TREV-CA' THEN 'FLCanada'
            END,
            vip_cohort,
            month_date,
            tenure
    ) a
WHERE value <> 0;

CREATE OR REPLACE TEMPORARY TABLE _fl_na_vip_tenure_calc1 AS
SELECT
        --store,
        CASE WHEN store = 'FLNorth America' THEN 'FL US'
            WHEN store = 'FLCanada' THEN 'FL CA'
            --else store
        END AS store,
        vip_cohort,
        month,
        tenure,
        SUM(value) as metric_value,
        SUM(credit_billers) as credit_billers
    FROM _anaplan_vip_tenure
    WHERE store in ('FLNorth America', 'FLCanada')
        AND tenure = 'M2'
    GROUP BY
        store,
        vip_cohort,
        month,
        tenure;

CREATE OR REPLACE TEMPORARY TABLE _fl_na_vip_tenure_calc2 AS
SELECT
        *,
        dateadd(month, 2, month) AS month_plus1_billing_month,
        credit_billers / metric_value AS m2_billing_credit_rate_fpa
    from _fl_na_vip_tenure_calc1;

CREATE OR REPLACE TEMPORARY TABLE _fl_na_vip_tenure_calc3 AS
select
        store,
        month_plus1_billing_month,
        m2_billing_credit_rate_fpa
    from _fl_na_vip_tenure_calc2;

CREATE OR REPLACE TEMPORARY TABLE _fpa_and_bor_filter3 AS
SELECT * FROM _fpa_and_bor_filter2_flna f
    JOIN _fl_na_vip_tenure_calc3 a ON a.MONTH_PLUS1_BILLING_MONTH=f.BILLING_MONTH
        AND a.store=f.STORE_NAME;

CREATE OR REPLACE TEMPORARY TABLE _fpa_and_bor_filter4 AS
SELECT
        billing_month,
        store_name AS storefront,
        m1_new_vips,
        m1_new_vips * m2_billing_credit_rate_fpa as first_successful_new_vips,
        m1_grace_vips,
        m1_grace_vips * m2_billing_credit_rate_fpa as first_successful_grace_vips,
        billed_credit as first_successful_billed_credit_rate,
        first_successful_billed_credit_rate_trailing_6_months as first_successful_billed_credit_rate_trailing,
        retry_rate,
        retry_rate_trailing_6_months,
        m2_tenure_vips,
        m2_plus_vips,
        null as fpa_m2_credit_billings,
        null as fpa_m2_plus_credit_billings,
        m2_billing_credit_rate_fpa,
        null as m2_plus_billing_rate,
        total_credit_billings as total_billing_fpa_forecast
    from _fpa_and_bor_filter3;

CREATE OR REPLACE TEMPORARY TABLE _bos_forecast_final AS
SELECT * FROM _fpa_and_bor_filter1
UNION
SELECT * FROM _fpa_and_bor_filter4;

BEGIN;

DELETE FROM reporting_prod.gms.bos_boss_forecast;

MERGE INTO reporting_prod.gms.bos_boss_forecast t
    USING (SELECT *
                FROM (select *,
                        HASH(*) as meta_row_hash,
                        row_number()
                            OVER (PARTITION BY billing_month,storefront
                                order by NULL
                                 ) AS rn
                        FROM _bos_forecast_final)
                where rn = 1) s
        ON equal_null(t.billing_month, s.billing_month)
        AND equal_null(t.storefront, s.storefront)
    WHEN NOT MATCHED THEN INSERT
        (billing_month,storefront,m1_new_vips,first_successful_new_vips,m1_grace_vips,first_successful_grace_vips,first_successful_billed_credit_rate,
        first_successful_billed_credit_rate_trailing,retry_rate,retry_rate_trailing_6_months,m2_tenure_vips,m2_plus_vips,fpa_m2_credit_billings,fpa_m2_plus_credit_billings,
        m2_billing_credit_rate_fpa, m2_plus_billing_rate,total_billing_fpa_forecast, meta_row_hash)
        VALUES  (billing_month,storefront,m1_new_vips,first_successful_new_vips,m1_grace_vips,first_successful_grace_vips,first_successful_billed_credit_rate,
        first_successful_billed_credit_rate_trailing,retry_rate,retry_rate_trailing_6_months,m2_tenure_vips,m2_plus_vips,fpa_m2_credit_billings,fpa_m2_plus_credit_billings,
        m2_billing_credit_rate_fpa, m2_plus_billing_rate,total_billing_fpa_forecast,meta_row_hash)
    WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
        THEN
        UPDATE
            SET t.m1_new_vips = s.m1_new_vips
                ,t.first_successful_new_vips = s.first_successful_new_vips
                ,t.m1_grace_vips = s.m1_grace_vips
                ,t.first_successful_grace_vips = s.first_successful_grace_vips
                ,t.first_successful_billed_credit_rate = s.first_successful_billed_credit_rate
                ,t.first_successful_billed_credit_rate_trailing = s.first_successful_billed_credit_rate_trailing
                ,t.retry_rate = s.retry_rate
                ,t.retry_rate_trailing_6_months = s.retry_rate_trailing_6_months
                ,t.m2_tenure_vips = s.m2_tenure_vips
                ,t.m2_plus_vips = s.m2_plus_vips
                ,t.fpa_m2_credit_billings = s.fpa_m2_credit_billings
                ,t.fpa_m2_plus_credit_billings = s.fpa_m2_plus_credit_billings
                ,t.m2_billing_credit_rate_fpa = s.m2_billing_credit_rate_fpa
                ,t.m2_plus_billing_rate = s.m2_plus_billing_rate
                ,t.total_billing_fpa_forecast = s.total_billing_fpa_forecast
                ,t.meta_row_hash = s.meta_row_hash
                ,t.meta_update_datetime = CURRENT_TIMESTAMP;

COMMIT;
