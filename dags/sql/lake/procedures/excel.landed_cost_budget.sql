DELETE FROM lake_stg.excel.landed_cost_budget_stg;
COPY INTO lake_stg.excel.landed_cost_budget_stg (business_unit, gender, category, class, origin_country, destination_country, fc, po_incoterms, shipping_mode, metric_value, metric_name, budget_date)
FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.landed_cost_budget/v2/'
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%';


INSERT INTO lake.excel.landed_cost_budget
(
    business_unit, gender, category, class, origin_country, destination_country, fc, po_incoterms,
    shipping_mode, budget_date, primary_freight, first_cost_rate, duty_rate, tariff_rate, unit_volume,
    total_budget_first_cost, total_budget_primary_cost, total_budget_duty_cost, total_budget_tariff_cost,
    meta_row_hash, meta_create_datetime, meta_update_datetime
)
SELECT
    business_unit, gender, category, class, origin_country, destination_country, fc, po_incoterms,
    shipping_mode, budget_date, primary_freight, first_cost_rate, duty_rate, tariff_rate, unit_volume,
    total_budget_first_cost, total_budget_primary_cost, total_budget_duty_cost, total_budget_tariff_cost,
    meta_row_hash, meta_create_datetime, meta_update_datetime
FROM
    (
        SELECT
            a.*
        FROM (
            SELECT
                *,
                hash(*) AS meta_row_hash,
                current_timestamp AS meta_create_datetime,
                current_timestamp AS meta_update_datetime,
                row_number() OVER ( PARTITION BY meta_row_hash ORDER BY NULL) AS rn
            FROM (
                     select
                        business_unit,
                        gender,
                        category,
                        class,
                        origin_country,
                        destination_country,
                        fc,
                        po_incoterms,
                        shipping_mode,
                        budget_date,
                        "'primary_freight'" AS  primary_freight,
                        "'first_cost_rate'" AS first_cost_rate,
                        "'duty_rate'" AS duty_rate,
                        "'tariff_rate'" AS tariff_rate,
                        "'unit_volume'" AS unit_volume,
                        "'total_budget_first_cost'" AS total_budget_first_cost,
                        "'total_budget_primary_cost'" AS total_budget_primary_cost,
                        "'total_budget_duty_cost'" AS total_budget_duty_cost,
                        "'total_budget_tariff_cost'" AS total_budget_tariff_cost
                    from lake_stg.excel.landed_cost_budget_stg
                    pivot(sum(metric_value) for metric_name in ('primary_freight','first_cost_rate','duty_rate',
                    'tariff_rate','unit_volume','total_budget_first_cost','total_budget_primary_cost','total_budget_duty_cost','total_budget_tariff_cost'))as p
                )
         ) a
        LEFT JOIN lake.excel.landed_cost_budget b
            ON a.meta_row_hash = b.meta_row_hash
        WHERE b.meta_row_hash IS NULL
        AND a.rn = 1
) s;
