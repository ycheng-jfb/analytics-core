CREATE OR REPLACE VIEW lake_view.amazon_selling_partner.search_terms_report_quarterly
            (
                        clicked_asin,
                        department_name,
                        end_date,
                        marketplace_id,
                        search_frequency_rank,
                        search_term,
                        start_date,
                        click_share_rank,
                        click_share,
                        conversion_share,
                        meta_update_datetime
            )
            AS
SELECT clicked_asin,
       department_name,
       end_date,
       marketplace_id,
       search_frequency_rank,
       search_term,
       start_date,
       click_share_rank,
       click_share,
       conversion_share,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_amazon_selling_partner_v1.search_terms_report_quarterly;
