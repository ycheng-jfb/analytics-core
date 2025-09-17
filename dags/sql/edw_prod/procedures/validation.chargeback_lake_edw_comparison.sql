--For FL and YTY store_brand we see the discrepancy in laje vs edw_prod, as we are pulling store_id in fact_chargeback_ebs from fact_order and not from the lake data.
--For few orders in fact_order it is mapped to FL, but in chargeback file it is mapped to YTY and vice versa.
--For validation purpose, Bucketing all YTY records to FL

CREATE OR REPLACE TEMP TABLE _lake_oracle_ebs_chargeback_base_eu AS
SELECT ebs.settlement_date,
       ebs.creation_date,
       ebs.chargeback_amt,
       ebs.store_id,
       ebs.vat_gl_account,
       ebs.sale_gl_account,
       ebs.receivables_trx_name
FROM lake.oracle_ebs.chargeback_eu ebs
JOIN stg.fact_order fo
    ON fo.meta_original_order_id = ebs.order_id
WHERE ebs.order_id IS NOT NULL
UNION ALL
SELECT ebs.settlement_date,
       ebs.creation_date,
       ebs.chargeback_amt,
       ebs.store_id,
       ebs.vat_gl_account,
       ebs.sale_gl_account,
       ebs.receivables_trx_name
FROM lake.oracle_ebs.chargeback_eu ebs
WHERE order_id IS NULL;

CREATE OR REPLACE TEMP TABLE _lake_oracle_ebs_chargeback_base_us AS
SELECT ebs.order_id,
       ebs.settlement_date,
       ebs.creation_date,
       ebs.chargeback_amt,
       ebs.store_id,
       ebs.gl_account,
       ebs.receivables_trx_name
FROM lake.oracle_ebs.chargeback_us ebs
JOIN stg.fact_order fo
    ON fo.meta_original_order_id = ebs.order_id
WHERE ebs.order_id IS NOT NULL
UNION ALL
SELECT ebs.order_id,
       ebs.settlement_date,
       ebs.creation_date,
       ebs.chargeback_amt,
       ebs.store_id,
       ebs.gl_account,
       ebs.receivables_trx_name
FROM lake.oracle_ebs.chargeback_us ebs
WHERE order_id IS NULL;

CREATE OR REPLACE TEMP TABLE _lake_chargeback_data_data_including_null_pre AS
SELECT
    DATE_TRUNC('month', nvl(ebs.settlement_date, ebs.creation_date)::DATE) AS gl_period,
    CASE
        WHEN COALESCE(ods.store_brand_abbr,gsb.reporting_store_brand, gts.store_brand) = 'YTY' THEN 'FL'
        ELSE COALESCE(ods.store_brand_abbr,gsb.reporting_store_brand, gts.store_brand)
    END AS store_brand,
    COALESCE(ods.store_country,gsc.reporting_store_country, gts.store_country) AS store_country,
    SUM(chargeback_amt) AS chargeback_amt
FROM _lake_oracle_ebs_chargeback_base_eu ebs
LEFT JOIN stg.dim_store ods
    ON ods.store_id = IFF(ebs.store_id = 41, 26, ebs.store_id)
LEFT JOIN reference.gl_store_brand gsb
    ON NVL(split_part(ebs.sale_gl_account,'.',1), split_part(ebs.vat_gl_account,'.',1)) = gsb.gl_company_code
LEFT JOIN reference.gl_trx_store gts
    ON LOWER(ebs.receivables_trx_name) = LOWER(gts.transaction_name)
LEFT JOIN reference.gl_store_country gsc
    ON NVL(split_part(ebs.sale_gl_account,'.',3), split_part(ebs.vat_gl_account,'.',3)) = gsc.gl_country_code
GROUP BY 1,2,3

UNION

SELECT
    DATE_TRUNC('month', NVL(ebs.settlement_date, ebs.creation_date)::DATE) AS GL_PERIOD,
    CASE
        WHEN COALESCE(ods.store_brand_abbr,gsb.reporting_store_brand, gts.store_brand) = 'YTY'  THEN 'FL'
        ELSE COALESCE(ods.store_brand_abbr,gsb.reporting_store_brand, gts.store_brand)
    END AS store_brand,
    COALESCE(ods.store_country,gsc.reporting_store_country, gts.store_country) AS store_country,
    SUM(chargeback_amt) AS chargeback_amt
FROM _lake_oracle_ebs_chargeback_base_us ebs
LEFT JOIN stg.dim_store ods
    ON ods.store_id = IFF(ebs.store_id = 41, 26, ebs.store_id)
LEFT JOIN reference.gl_store_brand gsb
    ON split_part(ebs.gl_account,'.',1) = gsb.gl_company_code
LEFT JOIN reference.gl_trx_store gts
    ON LOWER(ebs.receivables_trx_name) = LOWER(gts.transaction_name)
LEFT JOIN reference.gl_store_country gsc
    ON split_part(ebs.gl_account,'.',3) = gsc.gl_country_code
WHERE nvl(ebs.order_id, -1) NOT IN (1459191904, 164362616)
GROUP BY 1,2,3;
--1459191904 Tripos system processor sent payment transcation id as order id
--164362616 Mismatch company id mapped, please refer to da-34299 for more details

CREATE OR REPLACE TEMP TABLE _lake_chargeback_data_data_including_null AS
SELECT gl_period,
       store_brand,
       store_country,
       SUM(chargeback_amt) AS chargeback_amt
FROM _lake_chargeback_data_data_including_null_pre
GROUP BY 1,2,3;

CREATE OR REPLACE TEMP TABLE _fact_chargeback_data_including_null AS
SELECT
    DATE_TRUNC('month',  CONVERT_TIMEZONE('America/Los_Angeles', f.chargeback_datetime))::DATE AS chargeback_month,
    CASE
        when ds.store_brand_abbr = 'YTY' THEN 'FL'
        ELSE ds.store_brand_abbr
        END AS store_brand,
    ds.store_country,
    SUM(chargeback_payment_transaction_local_amount) AS chargeback_payment_transaction_local_amount
FROM stg.fact_chargeback f
JOIN stg.dim_store ds
    ON ds.store_id = f.store_id
WHERE chargeback_month > '2022-08-01'
    AND NOT f.is_deleted
GROUP BY 1,2,3;

CREATE OR REPLACE TEMP TABLE _chargeback_lake_edw_comparsion AS
SELECT
    a.gl_period AS chargeback_month,
    a.store_brand,
    a.store_country,
    a.chargeback_amt as lake_chargeback_amt,
    (-1 * b.chargeback_payment_transaction_local_amount) AS edw_chargeback_amt
FROM _lake_chargeback_data_data_including_null a
FULL JOIN _fact_chargeback_data_including_null b
    ON a.gl_period = b.chargeback_month
    AND a.store_brand = b.store_brand
    AND a.store_country = b.store_country
WHERE NOT equal_null(a.chargeback_amt,edw_chargeback_amt);

TRUNCATE TABLE validation.chargeback_lake_edw_comparison;

INSERT INTO validation.chargeback_lake_edw_comparison
SELECT
    chargeback_month,
    store_brand,
    store_country,
    lake_chargeback_amt,
    edw_chargeback_amt
FROM _chargeback_lake_edw_comparsion;
