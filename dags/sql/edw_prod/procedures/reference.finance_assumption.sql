SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET low_watermark = (SELECT IFNULL(MAX(meta_update_datetime), '1900-01-01')
                     FROM reference.finance_assumption);

-- pull forecasts and actuals only from existing ingestion
-- If we have forecasts and actuals for a same bu, month then pick actuals discard forecasts
CREATE OR REPLACE TEMPORARY TABLE _finance_assumption__forecast_actual AS
SELECT f.version,
       f.version_date,
       f.financial_date,
       f.hyperion_store_rg,
       m.store_brand AS brand,
       f.bu,
       m.region_type,
       m.region_type_mapping,
       m.store_type,
       m.local_currency,
       f.membership_credits_charged_count,
       f.gross_cash_revenue,
       f.activating_order_count,
       f.repeat_order_count,
       f.merchant_fees,
       f.gms_variable,
       f.fc_variable,
       f.freight_out_cost,
       f.outbound_shipping_supplies,
       f.product_cost_markdown,
       f.reship_exch_orders_shipped
FROM reference.finance_monthly_budget_forecast_actual f
         LEFT JOIN reference.finance_bu_mapping m ON f.bu = m.bu
    AND $execution_start_time::DATE BETWEEN m.start_date AND m.end_date
    AND m.report_mapping <> 'SX-OREV-US'
WHERE f.version IN ('Actuals', 'Forecast')
  AND f.bu IN ('FLNA-OREV', 'FLNA-RREV',
               'FLDE-OREV', 'FLDE-RREV',
               'FLDK-TREV', 'FLES-TREV', 'FLFR-TREV', 'FLNL-TREV', 'FLSE-TREV', 'FLUK-RREV', 'FLUK-OREV',
               'YTNA-OREV',
               'SXNA-OREV', 'SXNA-RREV',
               'SX FR', 'SX GER', 'SX NED', 'SX SP', 'SX DK', 'SX SE', 'SX UK', 'SXUK-RREV',
               'FKNA',
               'JFNA', 'SDNA',
               'JF DK', 'JF FR', 'JF GER', 'JF NED', 'JF SE', 'JF SP', 'JF UK')
QUALIFY ROW_NUMBER() OVER(PARTITION BY f.bu, f.financial_date ORDER BY f.version) = 1
;


CREATE OR REPLACE TEMP TABLE _finance_assumption__next_three_months AS
SELECT DISTINCT dd.month_date AS financial_date
FROM data_model.dim_date dd
         LEFT JOIN _finance_assumption__forecast_actual fa
                   ON fa.financial_date = dd.month_date
WHERE dd.month_date BETWEEN DATE_TRUNC(MONTH, CURRENT_DATE) AND DATEADD(MM, 3, DATE_TRUNC(MONTH, CURRENT_DATE))
  AND fa.financial_date IS NULL;


--insert latest month - 3 months in future is not available
INSERT INTO _finance_assumption__forecast_actual
SELECT DISTINCT fa.version,
                fa.version_date,
                d.financial_date,
                fa.hyperion_store_rg,
                fa.brand,
                fa.bu,
                fa.region_type,
                fa.region_type_mapping,
                fa.store_type,
                fa.local_currency,
                0 AS membership_credits_charged_count,
                0 AS gross_cash_revenue,
                0 AS activating_order_count,
                0 AS repeat_order_count,
                0 AS merchant_fees,
                0 AS gms_variable,
                0 AS fc_variable,
                0 AS freight_out_cost,
                0 AS outbound_shipping_supplies,
                0 AS product_cost_markdown,
                0 AS reship_exch_orders_shipped
FROM _finance_assumption__forecast_actual fa
         CROSS JOIN _finance_assumption__next_three_months d
WHERE fa.financial_date = (SELECT MAX(financial_date) FROM _finance_assumption__forecast_actual)
ORDER BY financial_date, bu;


--average monthly exchange rates
CREATE OR REPLACE TEMPORARY TABLE _finance_assumption__avg_exch_rates AS
SELECT dest_currency,
       DATE_TRUNC(MONTH, rate_date_pst) AS rate_month,
       AVG(exchange_rate)               AS avg_exchange_rate
FROM reference.currency_exchange_rate_by_date
WHERE src_currency = 'USD'
  AND dest_currency IN ('USD', 'EUR', 'GBP', 'SEK', 'DKK')
GROUP BY dest_currency, DATE_TRUNC(MONTH, rate_date_pst);

---insert current month exchange rate for future months
INSERT INTO _finance_assumption__avg_exch_rates
SELECT dest_currency,
       rate_month,
       avg_exchange_rate
FROM (SELECT dest_currency, avg_exchange_rate
      FROM _finance_assumption__avg_exch_rates
      WHERE rate_month = (SELECT MAX(rate_month) FROM _finance_assumption__avg_exch_rates)) max_exch
         CROSS JOIN (SELECT DISTINCT financial_date AS rate_month
                     FROM _finance_assumption__forecast_actual
                     WHERE financial_date > (SELECT MAX(rate_month) FROM _finance_assumption__avg_exch_rates)) dates;

--convert amounts to local
CREATE OR REPLACE TEMPORARY TABLE _finance_assumption__forecast_actual_local AS
SELECT f.bu,
       f.financial_date,
       f.brand,
       'None'                                           AS gender,
       f.region_type,
       f.region_type_mapping,
       f.store_type,
       f.local_currency,
       f.membership_credits_charged_count               AS membership_credits_charged_count,
       e.avg_exchange_rate * gross_cash_revenue         AS gross_cash_revenue,
       f.activating_order_count,
       f.repeat_order_count,
       e.avg_exchange_rate * merchant_fees              AS merchant_fees,
       e.avg_exchange_rate * gms_variable               AS gms_variable,
       e.avg_exchange_rate * fc_variable                AS fc_variable,
       e.avg_exchange_rate * freight_out_cost           AS freight_out_cost,
       e.avg_exchange_rate * outbound_shipping_supplies AS outbound_shipping_supplies,
       product_cost_markdown,
       e.avg_exchange_rate * reship_exch_orders_shipped AS reship_exch_orders_shipped
FROM _finance_assumption__forecast_actual f
         JOIN _finance_assumption__avg_exch_rates e ON f.local_currency = e.dest_currency
    AND f.financial_date = e.rate_month
ORDER BY financial_date, bu;

-- rolling amounts calcs
CREATE OR REPLACE TEMP TABLE _finance_assumption__forecast_actual_rolling AS
SELECT bu,
       financial_date,
       brand,
       gender,
       region_type,
       region_type_mapping,
       store_type,
       local_currency,
       membership_credits_charged_count,
       SUM(membership_credits_charged_count) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS membership_credits_charged_count_rolling,
       SUM(membership_credits_charged_count) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS membership_credits_charged_count_rolling_12,
       gross_cash_revenue,
       SUM(gross_cash_revenue) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS gross_cash_revenue_rolling,
       SUM(gross_cash_revenue) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING ) AS gross_cash_revenue_rolling_12,
       activating_order_count,
       SUM(activating_order_count) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS activating_order_count_rolling,
       SUM(activating_order_count) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING ) AS activating_order_count_rolling_12,
       repeat_order_count,
       SUM(repeat_order_count) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS repeat_order_count_rolling,
       SUM(repeat_order_count) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING ) AS repeat_order_count_rolling_12,
       merchant_fees,
       SUM(merchant_fees) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS merchant_fees_rolling,
       SUM(merchant_fees) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING ) AS merchant_fees_rolling_12,
       gms_variable,
       SUM(gms_variable) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS gms_variable_rolling,
       SUM(gms_variable) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING ) AS gms_variable_rolling_12,
       fc_variable,
       SUM(fc_variable) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS fc_variable_rolling,
       SUM(fc_variable) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING ) AS fc_variable_rolling_12,
       freight_out_cost,
       SUM(freight_out_cost) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS freight_out_cost_rolling,
       SUM(freight_out_cost) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING ) AS freight_out_cost_rolling_12,
       outbound_shipping_supplies,
       SUM(outbound_shipping_supplies) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS outbound_shipping_supplies_rolling,
       SUM(outbound_shipping_supplies) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING ) AS outbound_shipping_supplies_rolling_12,
       product_cost_markdown,
       SUM(product_cost_markdown) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS product_cost_markdown_rolling,
       SUM(product_cost_markdown) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING ) AS product_cost_markdown_rolling_12,
       reship_exch_orders_shipped,
       SUM(reship_exch_orders_shipped) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING ) AS reship_exch_orders_shipped_rolling,
       SUM(reship_exch_orders_shipped) OVER (PARTITION BY bu
           ORDER BY financial_date ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING ) AS reship_exch_orders_shipped_rolling_12
FROM _finance_assumption__forecast_actual_local
ORDER BY bu,
         financial_date,
         brand,
         gender,
         region_type;

---combine jfbna
CREATE OR REPLACE TEMPORARY TABLE _finance_assumption__jfb_na_agg AS
SELECT financial_date,
       SUM(membership_credits_charged_count)            AS membership_credits_charged_count,
       SUM(membership_credits_charged_count_rolling)    AS membership_credits_charged_count_rolling,
       SUM(membership_credits_charged_count_rolling_12) AS membership_credits_charged_count_rolling_12,
       SUM(gross_cash_revenue)                          AS gross_cash_revenue,
       SUM(gross_cash_revenue_rolling)                  AS gross_cash_revenue_rolling,
       SUM(gross_cash_revenue_rolling_12)               AS gross_cash_revenue_rolling_12,
       SUM(activating_order_count)                      AS activating_order_count,
       SUM(activating_order_count_rolling)              AS activating_order_count_rolling,
       SUM(activating_order_count_rolling_12)           AS activating_order_count_rolling_12,
       SUM(repeat_order_count)                          AS repeat_order_count,
       SUM(repeat_order_count_rolling)                  AS repeat_order_count_rolling,
       SUM(repeat_order_count_rolling_12)               AS repeat_order_count_rolling_12,
       SUM(merchant_fees)                               AS merchant_fees,
       SUM(merchant_fees_rolling)                       AS merchant_fees_rolling,
       SUM(merchant_fees_rolling_12)                    AS merchant_fees_rolling_12,
       SUM(gms_variable)                                AS gms_variable,
       SUM(gms_variable_rolling)                        AS gms_variable_rolling,
       SUM(gms_variable_rolling_12)                     AS gms_variable_rolling_12,
       SUM(fc_variable)                                 AS fc_variable,
       SUM(fc_variable_rolling)                         AS fc_variable_rolling,
       SUM(fc_variable_rolling_12)                      AS fc_variable_rolling_12,
       SUM(freight_out_cost)                            AS freight_out_cost,
       SUM(freight_out_cost_rolling)                    AS freight_out_cost_rolling,
       SUM(freight_out_cost_rolling_12)                 AS freight_out_cost_rolling_12,
       SUM(outbound_shipping_supplies)                  AS outbound_shipping_supplies,
       SUM(outbound_shipping_supplies_rolling)          AS outbound_shipping_supplies_rolling,
       SUM(outbound_shipping_supplies_rolling_12)       AS outbound_shipping_supplies_rolling_12,
       SUM(product_cost_markdown)                       AS product_cost_markdown,
       SUM(product_cost_markdown_rolling)               AS product_cost_markdown_rolling,
       SUM(product_cost_markdown_rolling_12)            AS product_cost_markdown_rolling_12,
       SUM(reship_exch_orders_shipped)                  AS reship_exch_orders_shipped,
       SUM(reship_exch_orders_shipped_rolling)          AS reship_exch_orders_shipped_rolling,
       SUM(reship_exch_orders_shipped_rolling_12)       AS reship_exch_orders_shipped_rolling_12
FROM _finance_assumption__forecast_actual_rolling
WHERE bu IN ('JFNA', 'SDNA', 'FKNA')
GROUP BY financial_date;


CREATE OR REPLACE TEMPORARY TABLE _finance_assumption__jfb_na_agg_cross AS
SELECT m.bu,
       a.financial_date,
       m.brand,
       'None'            AS gender,
       m.region_type,
       m.region_type_mapping,
       m.store_type,
       m.local_currency,
       a.membership_credits_charged_count,
       a.membership_credits_charged_count_rolling,
       a.membership_credits_charged_count_rolling_12,
       a.gross_cash_revenue,
       a.gross_cash_revenue_rolling,
       a.gross_cash_revenue_rolling_12,
       a.activating_order_count,
       a.activating_order_count_rolling,
       a.activating_order_count_rolling_12,
       a.repeat_order_count,
       a.repeat_order_count_rolling,
       a.repeat_order_count_rolling_12,
       a.merchant_fees,
       a.merchant_fees_rolling,
       a.merchant_fees_rolling_12,
       a.gms_variable,
       a.gms_variable_rolling,
       a.gms_variable_rolling_12,
       a.fc_variable,
       a.fc_variable_rolling,
       a.fc_variable_rolling_12,
       a.freight_out_cost,
       a.freight_out_cost_rolling,
       a.freight_out_cost_rolling_12,
       a.outbound_shipping_supplies,
       a.outbound_shipping_supplies_rolling,
       a.outbound_shipping_supplies_rolling_12,
       a.product_cost_markdown,
       a.product_cost_markdown_rolling,
       a.product_cost_markdown_rolling_12,
       a.reship_exch_orders_shipped,
       a.reship_exch_orders_shipped_rolling,
       a.reship_exch_orders_shipped_rolling_12
FROM _finance_assumption__jfb_na_agg a
         CROSS JOIN (SELECT distinct store_brand as brand, bu, region_type, region_type_mapping, store_type, local_currency
                     FROM reference.finance_bu_mapping
                     WHERE bu IN ('JFNA', 'SDNA', 'FKNA')) m
ORDER BY m.bu, a.financial_date, m.brand, m.region_type_mapping;

---set jfbna to aggregate metrics
CREATE OR REPLACE TEMPORARY TABLE _finance_monthly_forecast_actual__rolling_final AS
SELECT r.bu,
       r.financial_date,
       r.brand,
       r.gender,
       r.region_type,
       r.region_type_mapping,
       r.store_type,
       r.local_currency,
       NVL(jfb.membership_credits_charged_count,
           r.membership_credits_charged_count)                                 AS membership_credits_charged_count,
       NVL(jfb.membership_credits_charged_count_rolling,
           r.membership_credits_charged_count_rolling)                         AS membership_credits_charged_count_rolling,
       NVL(jfb.membership_credits_charged_count_rolling_12,
           r.membership_credits_charged_count_rolling_12)                      AS membership_credits_charged_count_rolling_12,
       NVL(jfb.gross_cash_revenue, r.gross_cash_revenue)                       AS gross_cash_revenue,
       NVL(jfb.gross_cash_revenue_rolling, r.gross_cash_revenue_rolling)       AS gross_cash_revenue_rolling,
       NVL(jfb.gross_cash_revenue_rolling_12, r.gross_cash_revenue_rolling_12) AS gross_cash_revenue_rolling_12,
       NVL(jfb.activating_order_count, r.activating_order_count)               AS activating_order_count,
       NVL(jfb.activating_order_count_rolling,
           r.activating_order_count_rolling)                                   AS activating_order_count_rolling,
       NVL(jfb.activating_order_count_rolling_12,
           r.activating_order_count_rolling_12)                                AS activating_order_count_rolling_12,
       NVL(jfb.repeat_order_count, r.repeat_order_count)                       AS repeat_order_count,
       NVL(jfb.repeat_order_count_rolling, r.repeat_order_count_rolling)       AS repeat_order_count_rolling,
       NVL(jfb.repeat_order_count_rolling_12, r.repeat_order_count_rolling_12) AS repeat_order_count_rolling_12,
       NVL(jfb.merchant_fees, r.merchant_fees)                                 AS merchant_fees,
       NVL(jfb.merchant_fees_rolling, r.merchant_fees_rolling)                 AS merchant_fees_rolling,
       NVL(jfb.merchant_fees_rolling_12, r.merchant_fees_rolling_12)           AS merchant_fees_rolling_12,
       NVL(jfb.gms_variable, r.gms_variable)                                   AS gms_variable,
       NVL(jfb.gms_variable_rolling, r.gms_variable_rolling)                   AS gms_variable_rolling,
       NVL(jfb.gms_variable_rolling_12, r.gms_variable_rolling_12)             AS gms_variable_rolling_12,
       NVL(jfb.fc_variable, r.fc_variable)                                     AS fc_variable,
       NVL(jfb.fc_variable_rolling, r.fc_variable_rolling)                     AS fc_variable_rolling,
       NVL(jfb.fc_variable_rolling_12, r.fc_variable_rolling_12)               AS fc_variable_rolling_12,
       NVL(jfb.freight_out_cost, r.freight_out_cost)                           AS freight_out_cost,
       NVL(jfb.freight_out_cost_rolling, r.freight_out_cost_rolling)           AS freight_out_cost_rolling,
       NVL(jfb.freight_out_cost_rolling_12, r.freight_out_cost_rolling_12)     AS freight_out_cost_rolling_12,
       NVL(jfb.outbound_shipping_supplies, r.outbound_shipping_supplies)       AS outbound_shipping_supplies,
       NVL(jfb.outbound_shipping_supplies_rolling,
           r.outbound_shipping_supplies_rolling)                               AS outbound_shipping_supplies_rolling,
       NVL(jfb.outbound_shipping_supplies_rolling_12,
           r.outbound_shipping_supplies_rolling_12)                            AS outbound_shipping_supplies_rolling_12,
       NVL(jfb.product_cost_markdown, r.product_cost_markdown)                 AS product_cost_markdown,
       NVL(jfb.product_cost_markdown_rolling,
           r.product_cost_markdown_rolling)                                    AS product_cost_markdown_rolling,
       NVL(jfb.product_cost_markdown_rolling_12,
           r.product_cost_markdown_rolling_12)                                 AS product_cost_markdown_rolling_12,
       NVL(jfb.reship_exch_orders_shipped, r.reship_exch_orders_shipped)       AS reship_exch_orders_shipped,
       NVL(jfb.reship_exch_orders_shipped,
           r.reship_exch_orders_shipped_rolling)                               AS reship_exch_orders_shipped_rolling,
       NVL(jfb.reship_exch_orders_shipped_rolling_12,
           r.reship_exch_orders_shipped_rolling_12)                            AS reship_exch_orders_shipped_rolling_12
FROM _finance_assumption__forecast_actual_rolling r
         LEFT JOIN _finance_assumption__jfb_na_agg_cross jfb
                   ON r.bu = jfb.bu
                       AND r.financial_date = jfb.financial_date
                       AND r.brand = jfb.brand
                       AND r.gender = jfb.gender
                       AND r.region_type = jfb.region_type
                       AND r.region_type_mapping = jfb.region_type_mapping
                       AND r.store_type = jfb.store_type
                       AND r.local_currency = jfb.local_currency;


CREATE OR REPLACE TEMPORARY TABLE _finance_assumption__calcs AS
SELECT bu,
       financial_date,
       brand,
       gender,
       region_type,
       region_type_mapping,
       store_type,
       local_currency,
       IFF((activating_order_count + repeat_order_count + reship_exch_orders_shipped) = 0, 0,
           (outbound_shipping_supplies) /
           (activating_order_count + repeat_order_count + reship_exch_orders_shipped))                                                                                    AS shipping_supplies_cost_per_order,
       IFF((activating_order_count_rolling + repeat_order_count_rolling + reship_exch_orders_shipped_rolling) = 0, 0,
           (outbound_shipping_supplies_rolling) / (activating_order_count_rolling + repeat_order_count_rolling +
                                                   reship_exch_orders_shipped_rolling))                                                                                   AS shipping_supplies_cost_per_order_rolling,
       IFF((activating_order_count_rolling_12 + repeat_order_count_rolling_12 + reship_exch_orders_shipped_rolling_12) = 0, 0,
           (outbound_shipping_supplies_rolling_12) / (activating_order_count_rolling_12 + repeat_order_count_rolling_12 +
                                                   reship_exch_orders_shipped_rolling_12))                                                                                AS shipping_supplies_cost_per_order_rolling_12,
       IFF((activating_order_count + repeat_order_count + reship_exch_orders_shipped) = 0, 0, (freight_out_cost) /
                                                                                              (activating_order_count + repeat_order_count + reship_exch_orders_shipped)) AS shipping_cost_per_order,
       IFF((activating_order_count_rolling + repeat_order_count_rolling + reship_exch_orders_shipped_rolling) = 0, 0,
           (freight_out_cost_rolling) / (activating_order_count_rolling + repeat_order_count_rolling +
                                         reship_exch_orders_shipped_rolling))                                                                                             AS shipping_cost_per_order_rolling,

       IFF((activating_order_count_rolling_12 + repeat_order_count_rolling_12 + reship_exch_orders_shipped_rolling_12) = 0, 0,
           (freight_out_cost_rolling_12) / (activating_order_count_rolling_12 + repeat_order_count_rolling_12 +
                                         reship_exch_orders_shipped_rolling_12))                                                                                          AS shipping_cost_per_order_rolling_12,
       IFF((activating_order_count + repeat_order_count + reship_exch_orders_shipped) = 0, 0, (fc_variable) /
                                                                                              (activating_order_count + repeat_order_count + reship_exch_orders_shipped)) AS variable_warehouse_cost_per_order,
       IFF((activating_order_count_rolling + repeat_order_count_rolling + reship_exch_orders_shipped_rolling) = 0, 0,
           (fc_variable_rolling) / (activating_order_count_rolling + repeat_order_count_rolling +
                                    reship_exch_orders_shipped_rolling))                                                                                                  AS variable_warehouse_cost_per_order_rolling,
       IFF((activating_order_count_rolling + repeat_order_count_rolling + reship_exch_orders_shipped_rolling) = 0, 0,
           (fc_variable_rolling_12) / (activating_order_count_rolling_12 + repeat_order_count_rolling_12 +
                                    reship_exch_orders_shipped_rolling_12))                                                                                               AS variable_warehouse_cost_per_order_rolling_12,

       IFF((activating_order_count + repeat_order_count + reship_exch_orders_shipped +
            membership_credits_charged_count) = 0, 0, (gms_variable) / (activating_order_count + repeat_order_count +
                                                                        reship_exch_orders_shipped +
                                                                        membership_credits_charged_count))                                                                AS variable_gms_cost_per_order,
       IFF((activating_order_count_rolling + repeat_order_count_rolling + reship_exch_orders_shipped_rolling +
            membership_credits_charged_count_rolling) = 0, 0, (gms_variable_rolling) / (activating_order_count_rolling +
                                                                                        repeat_order_count_rolling +
                                                                                        reship_exch_orders_shipped_rolling +
                                                                                        membership_credits_charged_count_rolling))                                        AS variable_gms_cost_per_order_rolling,
       IFF((activating_order_count_rolling_12 + repeat_order_count_rolling_12 + reship_exch_orders_shipped_rolling_12 +
            membership_credits_charged_count_rolling_12) = 0, 0, (gms_variable_rolling_12) / (activating_order_count_rolling_12 +
                                                                                        repeat_order_count_rolling_12 +
                                                                                        reship_exch_orders_shipped_rolling_12 +
                                                                                        membership_credits_charged_count_rolling_12))                                     AS variable_gms_cost_per_order_rolling_12,
       IFF((activating_order_count + repeat_order_count + reship_exch_orders_shipped) = 0, 0, (freight_out_cost) /
                                                                                              (activating_order_count + repeat_order_count + reship_exch_orders_shipped)) AS return_shipping_cost_per_order,
       IFF((activating_order_count_rolling + repeat_order_count_rolling + reship_exch_orders_shipped_rolling) = 0, 0,
           (freight_out_cost_rolling) / (activating_order_count_rolling + repeat_order_count_rolling +
                                         reship_exch_orders_shipped_rolling))                                                                                             AS return_shipping_cost_per_order_rolling,
       IFF((activating_order_count_rolling_12 + repeat_order_count_rolling_12 + reship_exch_orders_shipped_rolling_12) = 0, 0,
           (freight_out_cost_rolling_12) / (activating_order_count_rolling_12 + repeat_order_count_rolling_12 +
                                         reship_exch_orders_shipped_rolling_12))                                                                                          AS return_shipping_cost_per_order_rolling_12,
       IFF((gross_cash_revenue) = 0, 0, (merchant_fees) /
                                        (gross_cash_revenue))                                                                                                             AS variable_payment_processing_pct_cash_revenue,
       IFF((gross_cash_revenue_rolling) = 0, 0, (merchant_fees_rolling) /
                                                (gross_cash_revenue_rolling))                                                                                             AS variable_payment_processing_pct_cash_revenue_rolling,
       IFF((gross_cash_revenue_rolling_12) = 0, 0, (merchant_fees_rolling_12) /
                                                (gross_cash_revenue_rolling_12))                                                                                          AS variable_payment_processing_pct_cash_revenue_rolling_12,
       IFF((activating_order_count + repeat_order_count + reship_exch_orders_shipped) = 0, 0, (product_cost_markdown) /
                                                                                              (activating_order_count + repeat_order_count + reship_exch_orders_shipped)) AS product_markdown_cost_per_order,
       IFF((activating_order_count_rolling + repeat_order_count_rolling + reship_exch_orders_shipped_rolling) = 0, 0,
           (product_cost_markdown_rolling) / (activating_order_count_rolling + repeat_order_count_rolling +
                                              reship_exch_orders_shipped_rolling))                                                                                        AS product_markdown_cost_per_order_rolling,
       IFF((activating_order_count_rolling_12 + repeat_order_count_rolling_12 + reship_exch_orders_shipped_rolling_12) = 0, 0,
           (product_cost_markdown_rolling_12) / (activating_order_count_rolling_12 + repeat_order_count_rolling_12 +
                                              reship_exch_orders_shipped_rolling_12))                                                                                     AS product_markdown_cost_per_order_rolling_12
FROM _finance_monthly_forecast_actual__rolling_final;


CREATE OR REPLACE TEMPORARY TABLE _finance_assumption__return_resaleable AS
SELECT DISTINCT IFF(bu = 'FLUK-TREV', 'FLUK-OREV', bu) AS bu
              , returned_product_resaleable_percent
FROM reference.finance_assumption_history
WHERE month >= '2022-01-01';


CREATE
OR REPLACE TEMPORARY TABLE _finance_assumption__historical AS
SELECT DISTINCT iff(bu = 'FLUK-TREV', 'FLUK-OREV', bu) AS bu,
                '1999-12-31'                           AS financial_date,
                brand,
                gender,
                region_type,
                region_type_mapping,
                store_type,
                local_currency,
                shipping_supplies_cost_per_order,
                shipping_cost_per_order,
                variable_warehouse_cost_per_order,
                variable_gms_cost_per_order,
                variable_payment_processing_pct_cash_revenue,
                return_shipping_cost_per_order,
                product_markdown_percent,
                returned_product_resaleable_percent
FROM reference.finance_assumption_history h
WHERE h.month >= '2022-01-01';


CREATE OR REPLACE TEMPORARY TABLE _finance_assumption__substitution AS
SELECT ac.bu,
       ac.financial_date,
       ac.brand,
       ac.gender,
       ac.region_type,
       ac.region_type_mapping,
       ac.store_type,
       ac.local_currency,
       CASE
           WHEN ac.store_type = 'Retail' THEN 0
           WHEN ac.shipping_supplies_cost_per_order_rolling_12 > 0 THEN ac.shipping_supplies_cost_per_order_rolling_12
           ELSE NULL END AS shipping_supplies_cost_per_order,
       CASE
           WHEN ac.store_type = 'Retail' THEN 0
           WHEN ac.shipping_cost_per_order > 0 THEN ac.shipping_cost_per_order
           WHEN ac.shipping_cost_per_order_rolling > 0 THEN ac.shipping_cost_per_order_rolling
           WHEN ac.shipping_cost_per_order_rolling_12 > 0 THEN ac.shipping_cost_per_order_rolling_12
           ELSE NULL END AS shipping_cost_per_order,
       CASE
           WHEN ac.variable_warehouse_cost_per_order > 0 THEN ac.variable_warehouse_cost_per_order
           WHEN ac.variable_warehouse_cost_per_order_rolling > 0 THEN ac.variable_warehouse_cost_per_order_rolling
           WHEN ac.variable_warehouse_cost_per_order_rolling_12 > 0 THEN ac.variable_warehouse_cost_per_order_rolling_12
           ELSE NULL END AS variable_warehouse_cost_per_order,
       CASE
           WHEN ac.store_type = 'Retail' THEN 0
           WHEN ac.variable_gms_cost_per_order > 0 THEN ac.variable_gms_cost_per_order
           WHEN ac.variable_gms_cost_per_order_rolling > 0 THEN ac.variable_gms_cost_per_order_rolling
           WHEN ac.variable_gms_cost_per_order_rolling_12 > 0 THEN ac.variable_gms_cost_per_order_rolling_12
           ELSE NULL END AS variable_gms_cost_per_order,
       CASE
           WHEN ac.variable_payment_processing_pct_cash_revenue > 0 THEN ac.variable_payment_processing_pct_cash_revenue
           WHEN ac.variable_payment_processing_pct_cash_revenue_rolling > 0
               THEN ac.variable_payment_processing_pct_cash_revenue_rolling
           WHEN ac.variable_payment_processing_pct_cash_revenue_rolling_12 > 0
               THEN ac.variable_payment_processing_pct_cash_revenue_rolling_12
           ELSE NULL END AS variable_payment_processing_pct_cash_revenue,
       CASE
           WHEN ac.return_shipping_cost_per_order > 0 THEN ac.return_shipping_cost_per_order
           WHEN ac.return_shipping_cost_per_order_rolling > 0 THEN ac.return_shipping_cost_per_order_rolling
           WHEN ac.return_shipping_cost_per_order_rolling_12 > 0 THEN ac.return_shipping_cost_per_order_rolling_12
           ELSE NULL END AS return_shipping_cost_per_order,
       CASE
           WHEN ac.product_markdown_cost_per_order > 0 THEN ac.product_markdown_cost_per_order
           WHEN ac.product_markdown_cost_per_order_rolling > 0 THEN ac.product_markdown_cost_per_order_rolling
           WHEN ac.product_markdown_cost_per_order_rolling_12 > 0 THEN ac.product_markdown_cost_per_order_rolling_12
           ELSE NULL END AS product_markdown_percent,
       rr.returned_product_resaleable_percent
FROM _finance_assumption__calcs ac
         LEFT JOIN _finance_assumption__return_resaleable rr ON ac.bu = rr.bu

UNION ALL

SELECT bu,
       financial_date,
       brand,
       gender,
       region_type,
       region_type_mapping,
       store_type,
       local_currency,
       shipping_supplies_cost_per_order,
       shipping_cost_per_order,
       variable_warehouse_cost_per_order,
       variable_gms_cost_per_order,
       variable_payment_processing_pct_cash_revenue,
       return_shipping_cost_per_order,
       product_markdown_percent,
       returned_product_resaleable_percent
FROM _finance_assumption__historical
ORDER BY bu,
         financial_date,
         brand,
         gender,
         region_type,
         region_type_mapping,
         store_type,
         local_currency
;


CREATE OR REPLACE TEMPORARY TABLE _finance_assumption__final_output AS
SELECT bu,
       financial_date,
       brand,
       gender,
       region_type,
       region_type_mapping,
       store_type,
       local_currency,
       iff(shipping_supplies_cost_per_order IS NULL,
           lag(shipping_supplies_cost_per_order, 1, 0) IGNORE NULLS OVER (ORDER BY bu, financial_date),
           shipping_supplies_cost_per_order)                               AS shipping_supplies_cost_per_order,
       iff(shipping_cost_per_order IS NULL,
           lag(shipping_cost_per_order, 1, 0) IGNORE NULLS OVER (ORDER BY bu, financial_date),
           shipping_cost_per_order)                                        AS shipping_cost_per_order,
       iff(variable_warehouse_cost_per_order IS NULL,
           lag(variable_warehouse_cost_per_order, 1, 0) IGNORE NULLS OVER (ORDER BY bu, financial_date),
           variable_warehouse_cost_per_order)                              AS variable_warehouse_cost_per_order,
       iff(variable_gms_cost_per_order IS NULL,
           lag(variable_gms_cost_per_order, 1, 0) IGNORE NULLS OVER (ORDER BY bu, financial_date),
           variable_gms_cost_per_order)                                    AS variable_gms_cost_per_order,
       iff(variable_payment_processing_pct_cash_revenue IS NULL,
           lag(variable_payment_processing_pct_cash_revenue, 1, 0) IGNORE NULLS OVER (ORDER BY bu, financial_date),
           variable_payment_processing_pct_cash_revenue)                   AS variable_payment_processing_pct_cash_revenue,
       iff(return_shipping_cost_per_order IS NULL,
           lag(return_shipping_cost_per_order, 1, 0) IGNORE NULLS OVER (ORDER BY bu, financial_date),
           return_shipping_cost_per_order)                                 AS return_shipping_cost_per_order,
       iff(product_markdown_percent IS NULL,
           lag(product_markdown_percent, 1, 0) IGNORE NULLS OVER (ORDER BY bu, financial_date),
           product_markdown_percent)                                       AS product_markdown_percent,
       returned_product_resaleable_percent
FROM _finance_assumption__substitution;


CREATE OR REPLACE TEMP TABLE _finance_assumption__stg AS
SELECT bu,
       month as financial_date,
       brand,
       gender,
       region_type,
       region_type_mapping,
       store_type,
       local_currency,
       shipping_supplies_cost_per_order,
       shipping_cost_per_order,
       variable_warehouse_cost_per_order,
       variable_gms_cost_per_order,
       variable_payment_processing_pct_cash_revenue,
       return_shipping_cost_per_order,
       product_markdown_percent,
       returned_product_resaleable_percent,
       HASH(bu, month, brand, gender, region_type, region_type_mapping, store_type, local_currency,
            shipping_supplies_cost_per_order, shipping_cost_per_order, variable_warehouse_cost_per_order,
            variable_gms_cost_per_order, variable_payment_processing_pct_cash_revenue, return_shipping_cost_per_order,
            product_markdown_percent, returned_product_resaleable_percent) AS meta_row_hash,
       meta_create_datetime,
       meta_update_datetime
FROM reference.finance_assumption_history
WHERE month < '2022-07-01'
  AND meta_update_datetime > $low_watermark

UNION ALL

SELECT bu,
       financial_date,
       brand,
       gender,
       region_type,
       region_type_mapping,
       store_type,
       local_currency,
       shipping_supplies_cost_per_order,
       shipping_cost_per_order,
       variable_warehouse_cost_per_order,
       variable_gms_cost_per_order,
       variable_payment_processing_pct_cash_revenue,
       return_shipping_cost_per_order,
       product_markdown_percent,
       returned_product_resaleable_percent,
       hash(bu, financial_date, brand, gender, region_type, region_type_mapping, store_type, local_currency,
            shipping_supplies_cost_per_order, shipping_cost_per_order, variable_warehouse_cost_per_order,
            variable_gms_cost_per_order, variable_payment_processing_pct_cash_revenue, return_shipping_cost_per_order,
            product_markdown_percent, returned_product_resaleable_percent) AS meta_row_hash,
       $execution_start_time                                               AS meta_create_datetime,
       $execution_start_time                                               AS meta_update_datetime
FROM _finance_assumption__final_output
WHERE financial_date >= '2022-07-01';


MERGE INTO reference.finance_assumption t
    USING _finance_assumption__stg s
    ON EQUAL_NULL(t.financial_date, s.financial_date)
        AND EQUAL_NULL(t.brand, s.brand)
        AND EQUAL_NULL(t.gender, s.gender)
        AND EQUAL_NULL(t.region_type, s.region_type)
        AND EQUAL_NULL(t.region_type_mapping, s.region_type_mapping)
        AND EQUAL_NULL(t.store_type, s.store_type)
    WHEN NOT MATCHED THEN INSERT (bu, financial_date, brand, gender, region_type, region_type_mapping, store_type,
                                  local_currency, shipping_supplies_cost_per_order, shipping_cost_per_order,
                                  variable_warehouse_cost_per_order, variable_gms_cost_per_order,
                                  variable_payment_processing_pct_cash_revenue, return_shipping_cost_per_order,
                                  product_markdown_percent, returned_product_resaleable_percent,
                                  meta_row_hash, meta_create_datetime, meta_update_datetime)
        VALUES (bu, financial_date, brand, gender, region_type, region_type_mapping, store_type,
                local_currency, shipping_supplies_cost_per_order, shipping_cost_per_order,
                variable_warehouse_cost_per_order, variable_gms_cost_per_order,
                variable_payment_processing_pct_cash_revenue, return_shipping_cost_per_order,
                product_markdown_percent, returned_product_resaleable_percent,
                meta_row_hash, meta_create_datetime, meta_update_datetime)
    WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
        THEN
        UPDATE SET
            t.bu = s.bu,
            t.local_currency = s.local_currency,
            t.shipping_supplies_cost_per_order = s.shipping_supplies_cost_per_order,
            t.shipping_cost_per_order = s.shipping_cost_per_order,
            t.variable_warehouse_cost_per_order = s.variable_warehouse_cost_per_order,
            t.variable_gms_cost_per_order = s.variable_gms_cost_per_order,
            t.variable_payment_processing_pct_cash_revenue = s.variable_payment_processing_pct_cash_revenue,
            t.return_shipping_cost_per_order = s.return_shipping_cost_per_order,
            t.product_markdown_percent = s.product_markdown_percent,
            t.returned_product_resaleable_percent = s.returned_product_resaleable_percent,
            t.meta_row_hash = s.meta_row_hash,
            t.meta_create_datetime = s.meta_create_datetime,
            t.meta_update_datetime = s.meta_update_datetime;
