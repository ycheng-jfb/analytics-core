CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.market_basket_analysis_report_monthly
            (
                        asin,
                        end_date,
                        marketplace_id,
                        purchased_with_asin,
                        start_date,
                        purchased_with_rank,
                        combination_pct,
                        meta_update_datetime
            )
            AS
SELECT asin,
       end_date,
       marketplace_id,
       purchased_with_asin,
       start_date,
       purchased_with_rank,
       combination_pct,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.market_basket_analysis_report_monthly;
