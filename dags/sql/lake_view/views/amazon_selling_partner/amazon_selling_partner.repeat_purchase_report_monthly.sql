CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.repeat_purchase_report_monthly
            (
                        asin,
                        end_date,
                        marketplace_id,
                        start_date,
                        orders,
                        unique_customers,
                        repeat_customers_pct_total,
                        repeat_purchase_revenue_amount,
                        repeat_purchase_revenue_currency_code,
                        repeat_purchase_revenue_pct_total,
                        meta_update_datetime
            )
            AS
SELECT asin,
       end_date,
       marketplace_id,
       start_date,
       orders,
       unique_customers,
       repeat_customers_pct_total,
       repeat_purchase_revenue_amount,
       repeat_purchase_revenue_currency_code,
       repeat_purchase_revenue_pct_total,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.repeat_purchase_report_monthly;
