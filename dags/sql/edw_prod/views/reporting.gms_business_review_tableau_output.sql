CREATE OR REPLACE VIEW reporting.gms_business_review_tableau_output(
	DATE,
	REPORT_MAPPING,
	DATA_SOURCE,
	STORE_BRAND,
	BUSINESS_UNIT,
	CASH_NET_REVENUE,
	NEW_VIPS,
	NONACTIVATING_PRODUCT_ORDER_COUNT,
    CANCELS
) as
WITH _base_data AS (
    SELECT date,
           'shipped'                                                                       AS date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           IFF(date_object = 'shipped', cash_net_revenue, 0)                               AS cash_net_revenue,
           IFF(date_object = 'placed', new_vips, 0)                                        AS new_vips,
           IFF(date_object = 'shipped', nonactivating_product_order_count, 0)              AS nonactivating_product_order_count,
           IFF(date_object = 'placed', cancels, 0)                                         AS cancels,
           IFF(date_object = 'shipped', cash_net_revenue_mtd, 0)                           AS cash_net_revenue_mtd,
           IFF(date_object = 'placed', new_vips_mtd, 0)                                    AS new_vips_mtd,
           IFF(date_object = 'shipped', nonactivating_product_order_count_mtd, 0)          AS nonactivating_product_order_count_mtd,
           IFF(date_object = 'placed', cancels_mtd, 0)                                     AS cancels_mtd,
           IFF(date_object = 'shipped', cash_net_revenue_mtd_ly, 0)                        AS cash_net_revenue_mtd_ly,
           IFF(date_object = 'placed', new_vips_mtd_ly, 0)                                 AS new_vips_mtd_ly,
           IFF(date_object = 'shipped', nonactivating_product_order_count_mtd_ly,
               0)                                                                          AS nonactivating_product_order_count_mtd_ly,
           IFF(date_object = 'placed', cancels_mtd_ly, 0)                                  AS cancels_mtd_ly,
           IFF(date_object = 'shipped', cash_net_revenue_month_tot_ly, 0)                  AS cash_net_revenue_month_tot_ly,
           IFF(date_object = 'placed', new_vips_month_tot_ly, 0)                           AS new_vips_month_tot_ly,
           IFF(date_object = 'shipped', nonactivating_product_order_count_month_tot_ly,
               0)                                                                          AS nonactivating_product_order_count_month_tot_ly,
           IFF(date_object = 'placed', cancels_month_tot_ly, 0)                            AS cancels_month_tot_ly,
           IFF(date_object = 'shipped', cash_net_revenue_forecast, 0)                      AS cash_net_revenue_forecast,
           IFF(date_object = 'placed', new_vips_forecast, 0)                               AS new_vips_forecast,
           IFF(date_object = 'shipped', nonactivating_product_order_count_forecast,
               0)                                                                          AS nonactivating_product_order_count_forecast,
           IFF(date_object = 'placed', cancels_forecast, 0)                                AS cancels_forecast,
           IFF(date_object = 'shipped', cash_net_revenue_budget, 0)                        AS cash_net_revenue_budget,
           IFF(date_object = 'placed', new_vips_budget, 0)                                 AS new_vips_budget,
           IFF(date_object = 'shipped', nonactivating_product_order_count_budget,
               0)                                                                          AS nonactivating_product_order_count_budget,
           IFF(date_object = 'placed', cancels_budget, 0)                                  AS cancels_budget
    FROM reporting.daily_cash_final_output
    WHERE date_object IN ('shipped', 'placed')
        AND currency_object = 'usd'
        AND currency_type = 'USD'
        AND is_daily_cash_usd = TRUE
        AND report_date_type = 'To Date'
        AND report_mapping IN ('FK-TREV-NA',
            'FL-W-TREV-EU',
            'FL-M-TREV-EU',
            'FL-TREV-EU',
            'FL+SC-TREV-NA',
            'FL+SC-W-OREV-NA',
            'FL+SC-M-OREV-NA',
            'FL+SC-RREV-NA',
            'JF-TREV-EU',
            'JF-TREV-NA',
            'SD-TREV-NA',
            'SX-TREV-EU',
            'SX-TREV-NA',
            'SX-OREV-US',
            'SX-RREV-US',
            'YT-OREV-NA'
        )
)
   , _daily_cash_final_output AS (
    SELECT date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           SUM(COALESCE(cash_net_revenue, 0))                               AS cash_net_revenue,
           SUM(COALESCE(new_vips, 0))                                       AS new_vips,
           SUM(COALESCE(nonactivating_product_order_count, 0))              AS nonactivating_product_order_count,
           SUM(COALESCE(cancels,0))                                         AS cancels,
           SUM(COALESCE(cash_net_revenue_mtd, 0))                           AS cash_net_revenue_mtd,
           SUM(COALESCE(new_vips_mtd, 0))                                   AS new_vips_mtd,
           SUM(COALESCE(nonactivating_product_order_count_mtd, 0))          AS nonactivating_product_order_count_mtd,
           SUM(COALESCE(cancels_mtd,0))                                     AS cancels_mtd,
           SUM(COALESCE(cash_net_revenue_mtd_ly, 0))                        AS cash_net_revenue_mtd_ly,
           SUM(COALESCE(new_vips_mtd_ly, 0))                                AS new_vips_mtd_ly,
           SUM(COALESCE(nonactivating_product_order_count_mtd_ly, 0))       AS nonactivating_product_order_count_mtd_ly,
           SUM(COALESCE(cancels_mtd_ly,0))                                  AS cancels_mtd_ly,
           SUM(COALESCE(cash_net_revenue_month_tot_ly, 0))                  AS cash_net_revenue_month_tot_ly,
           SUM(COALESCE(new_vips_month_tot_ly, 0))                          AS new_vips_month_tot_ly,
           SUM(COALESCE(nonactivating_product_order_count_month_tot_ly, 0)) AS nonactivating_product_order_count_month_tot_ly,
           SUM(COALESCE(cancels_month_tot_ly,0))                            AS cancels_month_tot_ly,
           SUM(COALESCE(cash_net_revenue_forecast, 0))                      AS cash_net_revenue_forecast,
           SUM(COALESCE(new_vips_forecast, 0))                              AS new_vips_forecast,
           SUM(COALESCE(nonactivating_product_order_count_forecast, 0))     AS nonactivating_product_order_count_forecast,
           SUM(COALESCE(cancels_forecast,0))                                AS cancels_forecast,
           SUM(COALESCE(cash_net_revenue_budget, 0))                        AS cash_net_revenue_budget,
           SUM(COALESCE(new_vips_budget, 0))                                AS new_vips_budget,
           SUM(COALESCE(nonactivating_product_order_count_budget, 0))       AS nonactivating_product_order_count_budget,
           SUM(COALESCE(cancels_budget,0))                                  AS cancels_budget
    FROM _base_data
    GROUP BY date,
             date_object,
             currency_object,
             currency_type,
             business_unit,
             store_brand,
             report_mapping,
             is_daily_cash_usd,
             is_daily_cash_eur
)
   , _max_date AS (
    SELECT MAX(date) AS max_date
    FROM _daily_cash_final_output
)
   , _variables AS (
    SELECT (SELECT max_date FROM _max_date) AS report_date,
           'Report Date'                    AS source,
           (SELECT max_date FROM _max_date) AS full_date
    UNION ALL
    SELECT full_date AS report_date, 'daily' AS source, (SELECT max_date FROM _max_date) AS full_date
    FROM data_model.dim_date
    WHERE full_date BETWEEN CAST('2013-01-01' AS DATE) AND (SELECT max_date FROM _max_date)
    UNION ALL
    SELECT DISTINCT DATE_TRUNC('month', full_date)   AS report_date,
                    'Month_date'                     AS source,
                    (SELECT max_date FROM _max_date) AS full_date
    FROM data_model.dim_date
    WHERE full_date BETWEEN CAST('2013-01-01' AS DATE) AND (SELECT max_date FROM _max_date)
)
   , _daily AS (
    SELECT date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           'daily'                                AS data_source,
           SUM(cash_net_revenue)                  AS cash_net_revenue,
           SUM(new_vips)                          AS new_vips,
           SUM(nonactivating_product_order_count) AS nonactivating_product_order_count,
           SUM(cancels)                           AS cancels
    FROM _daily_cash_final_output dcfo
             JOIN _variables v ON v.report_date = dcfo.date
        AND v.source IN ('daily')
    GROUP BY date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           'daily'
)
   , _report_months AS (
    SELECT DISTINCT report_mapping,
                    CASE
                        WHEN DATE_TRUNC('month', date) = DATE_TRUNC('month', CURRENT_DATE())
                            THEN (SELECT max_date FROM _max_date)
                        ELSE LAST_DAY(date) END AS last_day_of_month
    FROM _daily_cash_final_output
    WHERE date <= (
        SELECT report_date
        FROM _variables
        WHERE source = 'Report Date')
      AND date >= '2013-01-01'
)
   , _eom AS (
    SELECT DATE_TRUNC('month', date)                  AS date,
           date_object,
           currency_object,
           currency_type,
           dcfo.business_unit,
           store_brand,
           dcfo.report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           'EOM'                                      AS data_source,
           SUM(cash_net_revenue_mtd)                  AS cash_net_revenue,
           SUM(new_vips_mtd)                          AS new_vips,
           SUM(nonactivating_product_order_count_mtd) AS nonactivating_product_order_count,
           SUM(cancels_mtd)                           AS cancels
    FROM _daily_cash_final_output dcfo
             JOIN _report_months rm ON dcfo.date = rm.last_day_of_month
        AND rm.report_mapping = dcfo.report_mapping
    GROUP BY DATE_TRUNC('month', date),
           date_object,
           currency_object,
           currency_type,
           dcfo.business_unit,
           store_brand,
           dcfo.report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           'EOM'
)
   , _run_rate AS (
    SELECT DATE_TRUNC('month', date)                                            AS date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           dcfo.report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           'RR'                                                                 AS data_source,
           CASE
               WHEN DATE_TRUNC('month', date) = DATE_TRUNC('month', CURRENT_DATE()) THEN SUM(
                           COALESCE(cash_net_revenue_mtd, 0) *
                           (COALESCE(cash_net_revenue_month_tot_ly, 0) / NULLIFZERO(cash_net_revenue_mtd_ly)))
               ELSE SUM(COALESCE(cash_net_revenue_mtd, 0)) END                  AS cash_net_revenue,

           CASE
               WHEN DATE_TRUNC('month', date) = DATE_TRUNC('month', CURRENT_DATE()) THEN SUM(COALESCE(new_vips_mtd, 0) *
                                                                                             (COALESCE(new_vips_month_tot_ly, 0) / NULLIFZERO(new_vips_mtd_ly)))
               ELSE SUM(COALESCE(new_vips_mtd, 0)) END                          AS new_vips,

           CASE
               WHEN DATE_TRUNC('month', date) = DATE_TRUNC('month', CURRENT_DATE()) THEN SUM(
                           COALESCE(nonactivating_product_order_count_mtd, 0) *
                           (COALESCE(nonactivating_product_order_count_month_tot_ly, 0) /
                            NULLIFZERO(nonactivating_product_order_count_mtd_ly)))
               ELSE SUM(COALESCE(nonactivating_product_order_count_mtd, 0)) END AS nonactivating_product_order_count,
           CASE
               WHEN DATE_TRUNC('month', date) = DATE_TRUNC('month', CURRENT_DATE()) THEN SUM(COALESCE(cancels_mtd, 0) *
                                                                                             (COALESCE(cancels_month_tot_ly, 0) / NULLIFZERO(cancels_mtd_ly)))
               ELSE SUM(COALESCE(cancels_mtd, 0)) END                           AS cancels
    FROM _daily_cash_final_output dcfo
             JOIN _report_months rm ON dcfo.date = rm.last_day_of_month
        AND rm.report_mapping = dcfo.report_mapping

    GROUP BY DATE_TRUNC('month', date),
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           dcfo.report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           'RR'
)
   , _forecast AS (
    SELECT date                                            AS date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           'FC'                                            AS data_source,
           SUM(cash_net_revenue_forecast)                  AS cash_net_revenue,
           SUM(new_vips_forecast)                          AS new_vips_forecast,
           SUM(nonactivating_product_order_count_forecast) AS nonactivating_product_order_count,
           SUM(cancels_forecast)                           AS cancels
    FROM _daily_cash_final_output
    WHERE date IN (
        SELECT report_date
        FROM _variables
        WHERE source = 'Month_date'
          AND report_date >= '2018-01-01'
    )
    GROUP BY date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           'FC'
)
   , _budget AS (
    SELECT date                                          AS date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           'BT'                                          AS data_source,
           SUM(cash_net_revenue_budget)                  AS cash_net_revenue,
           SUM(new_vips_budget)                          AS new_vips_budget,
           SUM(nonactivating_product_order_count_budget) AS nonactivating_product_order_count,
           SUM(cancels_budget)                           AS cancels
    FROM _daily_cash_final_output
    WHERE date IN (
        SELECT report_date
        FROM _variables
        WHERE source = 'Month_date'
          AND report_date >= '2018-01-01'
    )
    GROUP BY date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           'BT'
)


   , _output AS (
    SELECT date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           data_source,
           cash_net_revenue,
           new_vips,
           nonactivating_product_order_count,
           cancels
FROM _daily

    UNION ALL

    SELECT date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           data_source,
           cash_net_revenue,
           new_vips,
           nonactivating_product_order_count,
           cancels
FROM _eom

    UNION ALL

    SELECT date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           data_source,
           cash_net_revenue,
           new_vips,
           nonactivating_product_order_count,
           cancels
FROM _run_rate

    UNION ALL

    SELECT date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           data_source,
           cash_net_revenue,
           new_vips_forecast,
           nonactivating_product_order_count,
           cancels
FROM _forecast

    UNION ALL

    SELECT date,
           date_object,
           currency_object,
           currency_type,
           business_unit,
           store_brand,
           report_mapping,
           is_daily_cash_usd,
           is_daily_cash_eur,
           data_source,
           cash_net_revenue,
           new_vips_budget,
           nonactivating_product_order_count,
           cancels
FROM _budget
)
SELECT date,
       report_mapping,
       data_source,
       store_brand,
       business_unit,
       SUM(COALESCE(cash_net_revenue, 0))                  AS cash_net_revenue,
       SUM(COALESCE(new_vips, 0))                          AS new_vips,
       SUM(COALESCE(nonactivating_product_order_count, 0)) AS nonactivating_product_order_count,
       SUM(COALESCE(cancels, 0))                           AS cancels
FROM _output o
GROUP BY  date,
       report_mapping,
       data_source,
       store_brand,
       business_unit;
