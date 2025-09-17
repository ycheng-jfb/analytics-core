SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET target_table = 'stg.fact_product_ref_cost_history';

SET wm_lake_great_plains_iv_fifo = (SELECT stg.udf_get_watermark($target_table,'lake.great_plains.iv_fifo'));
SET wm_lake_evolve01_ssrs_reports_jfportal_poskus = (SELECT stg.udf_get_watermark($target_table,'lake.evolve01_ssrs_reports.jfportal_poskus'));

/*
Historical data before 2020-06-01 is loaded from reference.fact_product_ref_cost_history_archive.
This data is copied from archive_edw01_edw.dbo.fact_product_ref_cost_history_first_time.
*/

CREATE OR REPLACE TEMPORARY TABLE _fact_product_ref_cost_history_first_time_delta AS
SELECT
    nvl(dim_geography_geography_key.geography_key, -1) AS geography_key,
    nvl(dim_currency_currency_key.currency_key, -1) AS currency_key,
    s.sku AS sku,
    s.cost_start_date AS cost_start_datetime,
    s.cost_end_date AS cost_end_datetime,
    s.cost_type AS cost_type,
    s.is_current_cost AS is_current_cost,
    s.total_cost_amount AS total_cost_amount,
    s.product_cost_amount AS product_cost_amount,
    s.cmt_cost_amount AS cmt_cost_amount,
    s.freight_cost_amount AS freight_cost_amount,
    s.duty_cost_amount AS duty_cost_amount,
    s.commission_cost_amount AS commission_cost_amount,
    hash(nvl(dim_geography_geography_key.geography_key, -1), nvl(dim_currency_currency_key.currency_key, -1), s.sku, s.cost_start_date, s.cost_end_date, s.cost_type, s.is_current_cost, s.total_cost_amount, s.product_cost_amount, s.cmt_cost_amount, s.freight_cost_amount, s.duty_cost_amount, s.commission_cost_amount) AS meta_row_hash,
    s.meta_create_datetime AS meta_create_datetime,
    s.meta_update_datetime AS meta_update_datetime
FROM reference.fact_product_ref_cost_history_archive s
LEFT JOIN stg.dim_geography AS dim_geography_geography_key
    ON s.sub_region = dim_geography_geography_key.sub_region
    AND nvl(s.cost_start_date, current_timestamp)
    BETWEEN dim_geography_geography_key.effective_start_datetime AND dim_geography_geography_key.effective_end_datetime
LEFT JOIN stg.dim_currency AS dim_currency_currency_key
    ON s.currency_code = dim_currency_currency_key.iso_currency_code
AND s.exchange_rate_type = dim_currency_currency_key.currency_exchange_rate_type
    AND nvl(s.cost_start_date, current_timestamp)
    BETWEEN dim_currency_currency_key.effective_start_datetime AND dim_currency_currency_key.effective_end_datetime
WHERE s.meta_data_quality IS DISTINCT FROM 'error'
    AND (SELECT MIN(high_watermark_datetime)
            FROM stg.meta_table_dependency_watermark
            WHERE table_name = $target_table AND high_watermark_datetime > '2020-06-01') IS NULL;

MERGE INTO stg.fact_product_ref_cost_history t
USING _fact_product_ref_cost_history_first_time_delta s
    ON equal_null(t.geography_key, s.geography_key)
    AND equal_null(t.sku, s.sku)
    AND equal_null(t.cost_start_datetime, s.cost_start_datetime)
WHEN NOT MATCHED THEN INSERT (
    geography_key, currency_key, sku, cost_start_datetime, cost_end_datetime, cost_type, is_current_cost, total_cost_amount, product_cost_amount, cmt_cost_amount, freight_cost_amount, duty_cost_amount, commission_cost_amount, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    geography_key, currency_key, sku, cost_start_datetime, cost_end_datetime, cost_type, is_current_cost, total_cost_amount, product_cost_amount, cmt_cost_amount, freight_cost_amount, duty_cost_amount, commission_cost_amount, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN
UPDATE SET
    t.geography_key = s.geography_key,
    t.currency_key = s.currency_key,
    t.sku = s.sku,
    t.cost_start_datetime = s.cost_start_datetime,
    t.cost_end_datetime = s.cost_end_datetime,
    t.cost_type = s.cost_type,
    t.is_current_cost = s.is_current_cost,
    t.total_cost_amount = s.total_cost_amount,
    t.product_cost_amount = s.product_cost_amount,
    t.cmt_cost_amount = s.cmt_cost_amount,
    t.freight_cost_amount = s.freight_cost_amount,
    t.duty_cost_amount = s.duty_cost_amount,
    t.commission_cost_amount = s.commission_cost_amount,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;

CREATE OR REPLACE TEMPORARY TABLE _base AS
SELECT DISTINCT
    sku,
    sub_region
FROM (
        SELECT
            sku,
            sub_region
        FROM excp.fact_product_ref_cost_history e
        WHERE e.meta_is_current_excp AND e.meta_data_quality = 'error'

        UNION ALL

        SELECT
            rtrim(ltrim(itemnmbr)) AS sku,
            l.region AS sub_region
        FROM lake.great_plains.iv_fifo i
        JOIN reference.iv_fifo_region l
          ON rtrim(ltrim(i.trxloctn)) = l.trxloctn
          AND i.interid = l.interid
        WHERE i.meta_update_datetime > $wm_lake_great_plains_iv_fifo
            AND i.meta_update_datetime > '2020-06-01'
            AND rtrim(ltrim(i.pordnmbr)) NOT IN ('INV XFR', 'Sales Entry')

        UNION ALL

        SELECT
            sku,
            region_id AS sub_region
        FROM lake.evolve01_ssrs_reports.jfportal_poskus
        WHERE lower(po_status_text) IN ('received', 'released', 'awaiting executive approval',
                                        'changed rejected', 'asn','released no hts')
            AND lower(line_status) <> 'line canceled'
            AND dw_datetime_modified > $wm_lake_evolve01_ssrs_reports_jfportal_poskus
            AND dw_datetime_modified > '2020-06-01'
     ) s;

CREATE OR REPLACE TEMPORARY TABLE _product_base AS
SELECT *
FROM (
        SELECT
            rtrim(ltrim(itemnmbr)) AS sku,
            'GP Inventory Cost' AS cost_type,
            l.region AS sub_region,
            lcr.dest_currency AS iso_currency_code,
            'Daily Close' AS currency_type,
            cast(daterecd AS DATE) AS cost_start_datetime,
            sum(lcr.exchange_rate * lc_freight * qtyrecvd) AS freight,
            sum(lcr.exchange_rate * lc_duty * qtyrecvd) AS duty,
            sum(lcr.exchange_rate * lc_cmt * qtyrecvd) AS cmt,
            sum(lcr.exchange_rate * orcptcost * qtyrecvd) AS cost,
            sum(lcr.exchange_rate * lc_comm * qtyrecvd) AS commission,
            sum(lcr.exchange_rate * fifo_unitcost * qtyrecvd) AS total_cost,
            iff(sum(nvl(qtyrecvd, 0)) = 0, 1, sum(nvl(qtyrecvd, 0))) AS totalqty,
            2 AS rnk
        FROM lake.great_plains.iv_fifo po1
        JOIN reference.iv_fifo_region l
            ON rtrim(ltrim(po1.trxloctn)) = l.trxloctn
            AND po1.interid = l.interid
        JOIN _base e
            ON rtrim(ltrim(po1.itemnmbr)) = e.sku
            AND l.region = e.sub_region
        LEFT JOIN reference.gp_db_currency_mapping c
            ON po1.interid = c.db
        LEFT JOIN reference.currency_exchange_rate_by_date lcr
            ON c.iso_currency_code = lcr.src_currency
            AND upper(lcr.dest_currency) = iff(l.region = 'EU', 'EUR', 'USD')
            AND daterecd::DATE = lcr.rate_date_pst
        WHERE landed_cost = 1
            AND rtrim(ltrim(pordnmbr)) NOT IN ('INV ADJ', 'INV XFR', 'Sales Entry')
        GROUP BY
            cast(daterecd AS DATE),
            itemnmbr,
            l.region,
            lcr.dest_currency

        UNION ALL

        SELECT
            rtrim(ltrim(itemnmbr)) AS sku,
            'GP Inventory Cost' AS cost_type,
            l.region AS sub_region,
            lcr.dest_currency AS iso_currency_code,
            'Daily Close' AS currency_type,
            cast(daterecd AS DATE) AS cost_start_datetime,
            sum(lcr.exchange_rate * lc_freight * qtyrecvd) AS freight,
            sum(lcr.exchange_rate * lc_duty * qtyrecvd) AS duty,
            sum(lcr.exchange_rate * lc_cmt * qtyrecvd) AS cmt,
            sum(lcr.exchange_rate * orcptcost * qtyrecvd) AS cost,
            sum(lcr.exchange_rate * lc_comm * qtyrecvd) AS commission,
            sum(lcr.exchange_rate * fifo_unitcost * qtyrecvd) AS total_cost,
            iff(sum(qtyrecvd) IS NULL OR sum(qtyrecvd) = 0, 1, sum(qtyrecvd)) AS totalqty,
            4 AS rnk
        FROM lake.great_plains.iv_fifo po1
        JOIN reference.iv_fifo_region l
            ON rtrim(ltrim(po1.trxloctn)) = l.trxloctn
            AND po1.interid = l.interid
        JOIN _base e
            ON rtrim(ltrim(po1.itemnmbr)) = e.sku
            AND l.region = e.sub_region
        LEFT JOIN reference.gp_db_currency_mapping c
            ON po1.interid = c.db
        LEFT JOIN reference.currency_exchange_rate_by_date lcr
            ON c.iso_currency_code = lcr.src_currency
            AND upper(lcr.dest_currency) =  iff(l.region = 'EU', 'EUR', 'USD')
            AND daterecd::DATE = lcr.rate_date_pst
        WHERE landed_cost = 0
        AND (rtrim(ltrim(pordnmbr)) NOT IN ('INV ADJ', 'INV XFR', 'Sales Entry') OR
            (rtrim(ltrim(pordnmbr)) NOT IN ('INV XFR', 'Sales Entry') AND l.region = 'EU'))
        GROUP BY
            CAST(daterecd AS DATE),
            itemnmbr,
            l.region,
            lcr.dest_currency

        UNION ALL

        SELECT
            po1.sku,
            'PO Cost' AS cost_type,
            region_id AS sub_region,
            'USD' AS iso_currency_code,
            'Daily Close' AS currency_type,
            cast(coalesce(fc_delivery, xfd, delivery, '1900-01-01') AS DATE) AS cost_start_datetime,
            sum(freight * qty) AS freight,
            sum(duty * qty) AS duty,
            sum(cmt * qty) AS cmt,
            sum(cost * qty) AS cost,
            0 AS commission,
            sum((nvl(po1.freight, 0) + nvl(po1.duty, 0) + nvl(po1.cmt, 0) + nvl(po1.cost, 0)) * qty) AS total_cost,
            iff(sum(qty) IS NULL OR sum(qty) = 0, 1, sum(qty)) AS totalqty,
            3 AS rnk
        FROM lake.evolve01_ssrs_reports.jfportal_poskus po1
        JOIN _base e
            ON po1.sku = e.sku
            AND po1.region_id = e.sub_region
        WHERE lower(po1.po_status_text) IN ('received', 'released', 'awaiting executive approval',
                                        'changed rejected', 'asn','released no hts')
            AND lower(line_status) <> 'line canceled'
        GROUP BY
            cast(coalesce(fc_delivery, xfd, delivery, '1900-01-01') AS DATE),
            po1.sku,
            region_id
     ) s;

CREATE OR REPLACE TEMPORARY TABLE _metrics AS
SELECT
    sku,
    cost_type,
    sub_region,
    iso_currency_code,
    currency_type,
    cost_start_datetime,
    rnk,
    to_number(nvl(freight / totalqty, 0), 18, 2) AS freight,
    to_number(nvl(duty / totalqty, 0), 18, 2) AS duty,
    to_number(nvl(cmt / totalqty, 0), 18, 2) AS cmt,
    to_number(nvl(cost / totalqty, 0), 18, 2) AS product_cost,
    to_number(nvl(commission / totalqty, 0), 18, 2) AS commission,
    to_number(nvl(total_cost / totalqty, 0), 18, 2) AS total_cost
FROM _product_base;


CREATE OR REPLACE TEMPORARY TABLE _base_final AS
SELECT
    sku,
    sub_region,
    cost_start_datetime::TIMESTAMP_LTZ(3) AS cost_start_datetime,
    cost_type AS cost_type_name,
    iso_currency_code,
    currency_type AS currency_exchange_rate_type,
    freight,
    duty,
    cmt,
    product_cost,
    commission,
    total_cost,
    rnk,
    hash(freight, duty, cmt, product_cost, commission, total_cost) AS row_hash
FROM _metrics;

CREATE OR REPLACE TEMPORARY TABLE _fact AS
SELECT
    f.product_ref_cost_history_key,
    f.sku,
    dg.sub_region,
    f.cost_start_datetime,
    f.cost_end_datetime,
    f.cost_type AS cost_type_name,
    dc.iso_currency_code,
    dc.currency_exchange_rate_type,
    f.freight_cost_amount,
    f.duty_cost_amount,
    f.cmt_cost_amount,
    f.product_cost_amount,
    f.commission_cost_amount,
    f.total_cost_amount,
    1 AS rnk,
    hash(freight_cost_amount, duty_cost_amount, cmt_cost_amount, product_cost_amount,
         commission_cost_amount, total_cost_amount) AS row_hash
FROM stg.fact_product_ref_cost_history f
JOIN stg.dim_geography dg
    ON f.geography_key = dg.geography_key
JOIN stg.dim_currency dc
    ON f.currency_key = dc.currency_key
JOIN _base e
    ON f.sku = e.sku
    AND dg.sub_region = e.sub_region;


CREATE OR REPLACE TEMPORARY TABLE _unit_cost_rank AS
SELECT
    *
FROM _base_final

UNION ALL

SELECT
    sku,
    sub_region,
    cost_start_datetime,
    cost_type_name,
    iso_currency_code,
    currency_exchange_rate_type,
    freight_cost_amount,
    duty_cost_amount,
    cmt_cost_amount,
    product_cost_amount,
    commission_cost_amount,
    total_cost_amount,
    rnk,
    row_hash
FROM _fact;

CREATE OR REPLACE TEMPORARY TABLE _metrics_lag AS
WITH cte_rank
AS
(
    SELECT
        sku,
        sub_region,
        cost_start_datetime,
        cost_type_name,
        iso_currency_code,
        currency_exchange_rate_type,
        freight,
        duty,
        cmt,
        product_cost,
        commission,
        total_cost,
        row_hash,
        rnk,
        row_number() OVER (PARTITION BY sku, sub_region, cost_start_datetime ORDER BY rnk) AS rn
    FROM _unit_cost_rank
)
SELECT
    sku,
    sub_region,
    cost_start_datetime,
    cost_type_name,
    iso_currency_code,
    currency_exchange_rate_type,
    freight,
    duty,
    cmt,
    product_cost,
    commission,
    total_cost,
    row_hash,
    iff(rnk = 1, NULL, lag(row_hash) OVER (PARTITION BY sku, sub_region ORDER BY cost_start_datetime, rnk)) AS row_hash_lag
FROM cte_rank
WHERE rn = 1;

CREATE OR REPLACE TEMPORARY TABLE _cost_end_datetime AS
SELECT
    sku,
    sub_region,
    cost_start_datetime,
    nvl(dateadd(ms, -1, lead(m.cost_start_datetime) OVER (PARTITION BY m.sku, m.sub_region
                                                       ORDER BY m.cost_start_datetime)), '9999-12-31') AS cost_end_datetime,
    cost_type_name,
    iso_currency_code,
    currency_exchange_rate_type,
    freight,
    duty,
    cmt,
    product_cost,
    commission,
    total_cost,
    row_hash
FROM _metrics_lag m
WHERE row_hash <> nvl(row_hash_lag, 1);

CREATE OR REPLACE TEMPORARY TABLE _row_action AS
SELECT
    c.*,
    f.row_hash AS fact_row_hash,
    f.cost_start_datetime AS fact_cost_start_date,
    f.cost_end_datetime AS fact_cost_end_date,
    f.product_ref_cost_history_key,
    f.freight_cost_amount,
    f.duty_cost_amount,
    f.cmt_cost_amount,
    f.product_cost_amount,
    f.commission_cost_amount,
    f.total_cost_amount,
    iff(c.row_hash = f.row_hash AND c.cost_start_datetime = f.cost_start_datetime AND c.cost_end_datetime = f.cost_end_datetime, 1, 0) AS is_ignore
FROM _cost_end_datetime c
LEFT JOIN _fact f
    ON c.sku = f.sku
    AND c.sub_region = f.sub_region
    AND c.cost_start_datetime BETWEEN f.cost_start_datetime AND f.cost_end_datetime;


--Insert into staging table
INSERT INTO stg.fact_product_ref_cost_history_stg
(
    sku,
    cost_type,
    sub_region,
    is_current_cost,
    currency_code,
    exchange_rate_type,
    cost_start_datetime,
    cost_end_datetime,
    total_cost_amount,
    product_cost_amount,
    cmt_cost_amount,
    freight_cost_amount,
    duty_cost_amount,
    commission_cost_amount,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    sku,
    cost_type_name,
    sub_region,
    iff(cost_end_datetime = '9999-12-31', TRUE, FALSE) AS is_current_cost,
    iso_currency_code,
    currency_exchange_rate_type,
    cost_start_datetime,
    cost_end_datetime,
    total_cost,
    product_cost,
    cmt,
    freight,
    duty,
    commission,
    $execution_start_time,
    $execution_start_time
FROM _row_action
WHERE is_ignore = 0;
