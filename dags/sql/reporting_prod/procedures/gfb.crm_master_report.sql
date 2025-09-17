SET start_date =(
    SELECT IFNULL(
        DATEADD('day',-7,(SELECT MAX(send_date)::DATE
        from gfb.crm_master_report
    )), '1990-01-01')
);

SET daily_cash_date=IFF($start_date='1990-01-01','2018-05-01',$start_date);

DELETE FROM gfb.crm_master_report
WHERE send_date>$start_date;

INSERT INTO gfb.crm_master_report
/*
   !!! UNCOMMENT IF YOU NEED TO LOAD THE TABLE FIRST TIME!!!
SELECT CURRENT_TIMESTAMP() AS update_time,
       store_name,
       campaign_id,
       campaign_name,
       subject,
       email_list,
       abtest_segment,
       send_date,
       revenue,
       unique_clicks,
       opened_user_count,
       estimated_opens,
       sent,
       opt_in,
       opt_out,
       purchases,
       hard_bounce,
       soft_bounce,
       spam_reports,
       activating_orders,
       repeat_orders,
       activating_rev,
       repeat_rev,
       type,
       data_table
FROM gfb.sailthru_campaign_data

UNION ALL
*/

SELECT CURRENT_TIMESTAMP() AS update_time,
       store_name,
       campaign_id,
       campaign_name,
       subject,
       email_list,
       abtest_segment,
       send_date,
       revenue,
       unique_clicks,
       opened_user_count,
       estimated_opens,
       sent,
       0                            AS opt_in,
       opt_out,
       purchases,
       hard_bounce,
       soft_bounce,
       spam_reports,
       activating_orders,
       repeat_orders,
       activating_rev,
       repeat_rev,
       type,
       'campaign'                  AS data_table
FROM gfb.crm_weekly_kpi_report
WHERE type='Email'
    AND send_date>$start_date

UNION ALL

/*
    !!! UNCOMMENT IF YOU NEED TO LOAD THE TABLE FIRST TIME!!!
SELECT CURRENT_TIMESTAMP() AS update_time,
       store_name,
       template_id,
       template_name,
       subject,
       email_list,
       abtest_segment,
       date,
       rev,
       unique_clicks,
       opened_user_count,
       estopens,
       delivered,
       opt_in,
       optout,
       purchase,
       hard_bounce,
       soft_bounce,
       spam_reports,
       activating_orders,
       repeat_orders,
       activating_rev,
       repeat_rev,
       type,
       data_table
FROM gfb.sailthru_trigger_data

UNION ALL
*/

SELECT CURRENT_TIMESTAMP()         AS update_time,
       store_name,
       campaign_id,
       campaign_name,
       subject,
       email_list,
       abtest_segment,
       send_date,
       revenue,
       unique_clicks,
       opened_user_count,
       estimated_opens,
       sent,
       0                            AS opt_in,
       opt_out,
       purchases,
       hard_bounce,
       soft_bounce,
       spam_reports,
       activating_orders,
       repeat_orders,
       activating_rev,
       repeat_rev,
       type,
       'mobile'                     AS data_table
FROM gfb.crm_weekly_kpi_report
WHERE type='SMS'
    AND send_date>$start_date

UNION ALL

SELECT CURRENT_TIMESTAMP()         AS update_time,
       store_name,
       campaign_id,
       campaign_name,
       subject,
       email_list,
       abtest_segment,
       send_date,
       revenue,
       unique_clicks,
       opened_user_count,
       estimated_opens,
       sent,
       0                            AS opt_in,
       opt_out,
       purchases,
       hard_bounce,
       soft_bounce,
       spam_reports,
       activating_orders,
       repeat_orders,
       activating_rev,
       repeat_rev,
       type,
       'mobile_push'               AS data_table
FROM gfb.crm_weekly_kpi_report
WHERE type='Push'
    AND send_date>$start_date

UNION ALL

SELECT CURRENT_TIMESTAMP()                                                                     AS update_time,
       IFF(store_group IN ('JustFab', 'ShoeDazzle', 'FabKids'), CONCAT(LOWER(store_group), '_', 'us'),
           REPLACE(LOWER(store_group), ' ', '_'))                                              AS store_name,
       0::INT                                                                                  AS template_id,
       'TEMPLATE_NAME'                                                                         AS template_name,
       'SUBJECT'                                                                               AS subject,
       'EMAIL_LIST'                                                                            AS email_list,
       'ABTEST_SEGMENT'                                                                        AS abtest_segment,
       date,
       0                                                                                       AS rev,
       0                                                                                       AS unique_clicks,
       0                                                                                       AS opened_user_count,
       0                                                                                       AS estopens,
       0                                                                                       AS delivered,
       0                                                                                       AS opt_in,
       0                                                                                       AS optout,
       0                                                                                       AS purchase,
       0                                                                                       AS hard_bounce,
       0                                                                                       AS soft_bounce,
       0                                                                                       AS spam_reports,
       SUM(IFF(mc.membership_order_type_l1 = 'Activating VIP', fso.product_order_count, 0))    AS activating_orders,
       SUM(IFF(mc.membership_order_type_l1 != 'Activating VIP', fso.product_order_count, 0))   AS repeat_orders,
       SUM(IFF(mc.membership_order_type_l1 = 'Activating VIP', fso.product_gross_revenue, 0))  AS activating_rev,
       SUM(IFF(mc.membership_order_type_l1 != 'Activating VIP', fso.product_gross_revenue, 0)) AS repeat_rev,
       'Daily_Cash '                                                                           AS type,
       'daily_cash'                                                                            AS data_table
FROM edw_prod.analytics_base.finance_sales_ops fso
     JOIN edw_prod.data_model_jfb.dim_store s
          ON s.store_id = fso.store_id
     JOIN edw_prod.data_model_jfb.dim_order_membership_classification mc
          ON mc.order_membership_classification_key = fso.order_membership_classification_key
WHERE store_brand_abbr IN ('JF', 'FK', 'SD')
  AND currency_object = 'usd'
  AND date_object = 'placed'
  AND store_type = 'Online'
  AND date >= $daily_cash_date
  AND date <> '9999-12-31'
GROUP BY update_time,
         IFF(store_group IN ('JustFab', 'ShoeDazzle', 'FabKids'), CONCAT(LOWER(store_group), '_', 'us'),
             REPLACE(LOWER(store_group), ' ', '_')),
         template_id,
         template_name,
         subject,
         email_list,
         abtest_segment,
         date;
