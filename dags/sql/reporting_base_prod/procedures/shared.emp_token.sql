/*
 converted from foundry team's code on identifying correct expiration date of token
 original table: ultramerchant.[dbo].[vw_emp_token_expire_v2]
 */

CREATE OR REPLACE TRANSIENT TABLE shared.emp_token AS
SELECT vw.*,
       (
           DATEADD(MONTH, vw.extension_months,
                   CASE
                       WHEN vw.scenario = 'Manager Discretion Tokens'
                           THEN LAST_DAY(DATEADD(MONTH, 12, vw.datetime_added))
                       WHEN vw.scenario = 'Opted-in'
                           THEN
                           (CASE
                               -- US & CA & UK
                                WHEN vw.country_code IN ('US', 'CA', 'GB') THEN
                                    LAST_DAY(DATEADD(MONTH, 12,
                                                     IFF(vw.datetime_optin > vw.datetime_added, vw.datetime_optin,
                                                         vw.datetime_added)))
                               -- FR & ES & DK & SE & NL
                                WHEN vw.country_code IN ('FR', 'ES', 'DK', 'SE', 'NL') THEN
                                    LAST_DAY(DATEADD(MONTH, 36,
                                                     IFF(vw.datetime_optin > vw.datetime_added, vw.datetime_optin,
                                                         vw.datetime_added)))
                               -- DE
                                WHEN vw.country_code = 'DE' THEN
                                    DATE_FROM_PARTS(YEAR(DATEADD(MONTH, 36, IFF(vw.datetime_optin > vw.datetime_added,
                                                                                vw.datetime_optin, vw.datetime_added))),
                                                    12, 31)
                               -- Unexpected Scenario
                                ELSE
                                    NULL
                               END)
                       WHEN vw.scenario = 'No Opt-In & Grandfathered VIPs'
                           THEN NULL
                       -- Grandfathered Downgrade has activation since migration - converted token
                       WHEN vw.scenario = 'No Opt-In & Grandfathered Downgrades - Token converted from Member Credit'
                           THEN
                           (CASE
                               -- VIP activation recorded on Level Group Modification Log after EMP conversion
                                WHEN EXISTS(SELECT 1 --top 1 1
                                            FROM lake_consolidated_view.ultra_merchant.membership_level_group_modification_log mlgml
                                            WHERE mlgml.membership_id = vw.membership_id
                                              AND vw.date_migrated_payg <= mlgml.datetime_added
                                              AND mlgml.new_value IN (500, 1000, 2000))
                                    THEN (
                                    CASE
                                        WHEN vw.country_code IN ('US', 'CA', 'GB') THEN
                                            LAST_DAY(DATEADD(MONTH, 12, (SELECT MIN(mlgml.datetime_added)
                                                                         FROM lake_consolidated_view.ultra_merchant.membership_level_group_modification_log mlgml
                                                                         WHERE mlgml.membership_id = vw.membership_id
                                                                           AND vw.date_migrated_payg <= mlgml.datetime_added
                                                                           AND mlgml.new_value IN (500, 1000, 2000))))
                                        WHEN vw.country_code IN ('FR', 'ES', 'DK', 'SE', 'NL') THEN
                                            LAST_DAY(DATEADD(MONTH, 36, (SELECT MIN(mlgml.datetime_added)
                                                                         FROM lake_consolidated_view.ultra_merchant.membership_level_group_modification_log mlgml
                                                                         WHERE mlgml.membership_id = vw.membership_id
                                                                           AND vw.date_migrated_payg <= mlgml.datetime_added
                                                                           AND mlgml.new_value IN (500, 1000, 2000))))
                                        WHEN vw.country_code = 'DE' THEN
                                            DATE_FROM_PARTS(YEAR(DATEADD(MONTH, 36, (SELECT MIN(mlgml.datetime_added)
                                                                                     FROM lake_consolidated_view.ultra_merchant.membership_level_group_modification_log mlgml
                                                                                     WHERE mlgml.membership_id = vw.membership_id
                                                                                       AND vw.date_migrated_payg <= mlgml.datetime_added
                                                                                       AND mlgml.new_value IN (500, 1000, 2000)))),
                                                            12, 31)
                                        ELSE
                                            NULL
                                        END
                                    )
                               -- VIP activation NOT recorded on Level Group Modification Log after EMP conversion but currently a VIP                                                                                            )
                                WHEN vw.membership_statuscode = 3930
                                    THEN (
                                    CASE
                                        WHEN vw.country_code IN ('US', 'CA', 'GB') THEN
                                            LAST_DAY(DATEADD(MONTH, 12, vw.datetime_activated))
                                        WHEN vw.country_code IN ('FR', 'ES', 'DK', 'SE', 'NL') THEN
                                            LAST_DAY(DATEADD(MONTH, 36, vw.datetime_activated))
                                        WHEN vw.country_code = 'DE' THEN
                                            DATE_FROM_PARTS(YEAR(DATEADD(MONTH, 36, vw.datetime_activated)), 12, 31)
                                        ELSE
                                            NULL
                                        END
                                    )
                               -- VIP activation NOT recorded on Level Group Modification Log after EMP conversion and NOT currently a VIP
                                ELSE
                                    NULL
                               END)
                       -- Grandfathered Downgrade has activation since migration - NOT converted token
                       WHEN vw.scenario =
                            'No Opt-In & Grandfathered Downgrades & NOT Token converted from Member Credit'
                           THEN
                           (CASE
                               -- US & CA & UK
                                WHEN vw.country_code IN ('US', 'CA', 'GB') THEN
                                    LAST_DAY(DATEADD(MONTH, 12, vw.datetime_added))
                               -- FR & ES & DK & SE & NL
                                WHEN vw.country_code IN ('FR', 'ES', 'DK', 'SE', 'NL') THEN
                                    LAST_DAY(DATEADD(MONTH, 36, vw.datetime_added))
                               -- DE
                                WHEN vw.country_code = 'DE' THEN
                                    DATE_FROM_PARTS(YEAR(DATEADD(MONTH, 36, vw.datetime_added)), 12, 31)
                               -- Unexpected Scenario
                                ELSE
                                    NULL
                               END)
                       WHEN vw.scenario = 'No Opt-In & New EMP customers'
                           THEN
                           (CASE
                               -- US & CA & UK
                                WHEN vw.country_code IN ('US', 'CA', 'GB') THEN
                                    LAST_DAY(DATEADD(MONTH, 12, vw.datetime_added))
                               -- FR & ES & DK & SE & NL
                                WHEN vw.country_code IN ('FR', 'ES', 'DK', 'SE', 'NL') THEN
                                    LAST_DAY(DATEADD(MONTH, 36, vw.datetime_added))
                               -- DE
                                WHEN vw.country_code = 'DE' THEN
                                    DATE_FROM_PARTS(YEAR(DATEADD(MONTH, 36, vw.datetime_added)), 12, 31)
                               -- Unexpected Scenario
                                ELSE
                                    NULL
                               END)
                       WHEN vw.scenario = 'Unexpected Scenario'
                           THEN NULL
                       END)
           )   AS date_expires
        ,
       CASE
           WHEN scenario = 'No Opt-In & New EMP customers' AND vw.datetime_optin IS NULL
               THEN (SELECT MIN(fa.activation_local_datetime)
                     FROM edw_prod.data_model.fact_activation fa -- not using membership table as it only tracks the first activation
                     JOIN edw_prod.data_model.dim_store st ON fa.store_id = st.store_id
                     WHERE fa.customer_id = vw.customer_id
                     AND
                     ((st.store_brand = 'Fabletics' AND st.store_region = 'NA' AND fa.activation_local_datetime::DATE > '2021-01-01')
                   OR (st.store_brand = 'Fabletics' AND st.store_region = 'EU' AND fa.activation_local_datetime::DATE > '2021-11-01')
                   OR (st.store_brand = 'Savage X' AND st.store_region = 'NA' AND fa.activation_local_datetime::DATE > '2022-01-01')
                   OR (st.store_brand = 'JustFab' AND st.store_country = 'US' AND fa.activation_local_datetime::DATE > '2023-07-18')
                   OR (st.store_brand = 'JustFab' AND st.store_country = 'US' AND fa.activation_local_datetime::DATE >= '2023-10-26')
                   OR (st.store_brand = 'ShoeDazzle' AND st.store_country = 'US' AND fa.activation_local_datetime::DATE > '2023-09-21')
                   OR (st.store_brand = 'FabKids' AND st.store_country = 'US' AND fa.activation_local_datetime::DATE > '2023-09-26')))
           WHEN scenario LIKE 'No Opt-In & Grandfathered Downgrades%' AND vw.datetime_optin IS NULL
               THEN (SELECT MIN(fa.activation_local_datetime)
                     FROM edw_prod.data_model.fact_activation fa
                     WHERE fa.customer_id = vw.customer_id
                       AND vw.date_migrated_payg <= fa.activation_local_datetime)
           WHEN scenario = 'No Opt-In & Grandfathered VIPs' AND vw.datetime_optin IS NULL
               THEN (SELECT MIN(fa.activation_local_datetime)
                     FROM edw_prod.data_model.fact_activation fa -- not using membership table as it only tracks the first activation
                     JOIN edw_prod.data_model.dim_store st ON fa.store_id = st.store_id
                     WHERE fa.customer_id = vw.customer_id
                     AND
                     ((st.store_brand = 'Fabletics' AND st.store_region = 'NA' AND fa.activation_local_datetime::DATE > '2021-01-01')
                   OR (st.store_brand = 'Fabletics' AND st.store_region = 'EU' AND fa.activation_local_datetime::DATE > '2021-11-01')
                   OR (st.store_brand = 'Savage X' AND st.store_region = 'NA' AND fa.activation_local_datetime::DATE > '2022-01-01')
                   OR (st.store_brand = 'JustFab' AND st.store_country = 'US' AND fa.activation_local_datetime::DATE > '2023-07-18')
                   OR (st.store_brand = 'JustFab' AND st.store_country = 'US' AND fa.activation_local_datetime::DATE >= '2023-10-26')
                   OR (st.store_brand = 'ShoeDazzle' AND st.store_country = 'US' AND fa.activation_local_datetime::DATE > '2023-09-21')
                   OR (st.store_brand = 'FabKids' AND st.store_country = 'US' AND fa.activation_local_datetime::DATE > '2023-09-26')))
           ELSE vw.datetime_optin
           END AS adjusted_datetime_optin
FROM (SELECT mt.membership_token_id,
             mt.membership_id,
             m.store_id,
             m.customer_id,
             m.datetime_activated,
             m.statuscode                         AS membership_statuscode,
             mt.membership_token_reason_id,
             mtr.label                            AS token_reason_label,
             IFF(mtr.cash = 1, 'Cash', 'NonCash') AS credit_tender,
             sg.country_code,
             sg.store_group_id,
             mt.purchase_price,
             mt.currency_code,
             mt.statuscode,
             mt.datetime_added,
             cd.max_optin_date                    AS datetime_optin,     -- nmp_opt_in_true
             dd.full_date                         AS date_migrated_vip,  -- nmp_migration
             dd2.full_date                        AS date_migrated_payg, -- nmp_migration_payg
             mt.extension_months,
             (
                 CASE -- !!!! ORDER OF CASES MATTER  !!!!
                 -- Manager Discretion Tokens
                     WHEN
                         mt.membership_token_reason_id = 15
                         THEN
                         'Manager Discretion Tokens'
                     -- Opted-in
                     WHEN cd.customer_detail_id IS NOT NULL
                         THEN 'Opted-in'
                     -- No Opt-In & Grandfathered VIPs
                     WHEN
                             cd.customer_detail_id IS NULL
                             AND md.membership_detail_id IS NOT NULL
                         THEN
                         'No Opt-In & Grandfathered VIPs'
                     -- No Opt-In & Grandfathered Downgrades
                     WHEN
                             cd.customer_detail_id IS NULL -- !Opt-in
                             AND md.membership_detail_id IS NULL -- !Migrated as VIP
                             AND
                             md2.membership_detail_id IS NOT NULL -- Migrated as NonVIP
                         THEN
                         -- No Opt-In & Grandfathered Downgrades
                         -- Token converted from Member Credit
                         (CASE
                              WHEN
                                  mt.membership_token_reason_id = 51
                                  THEN
                                  'No Opt-In & Grandfathered Downgrades - Token converted from Member Credit'
                             -- No Opt-In & Grandfathered Downgrades
                             -- NOT a Token Converted After EMP Migration
                              ELSE
                                  'No Opt-In & Grandfathered Downgrades & NOT Token converted from Member Credit'
                             END)
                     -- No Opt-In & New EMP customers
                     WHEN
                             cd.customer_detail_id IS NULL
                             AND md.membership_detail_id IS NULL
                             AND
                             md2.membership_detail_id IS NULL
                         THEN
                         'No Opt-In & New EMP customers'
                     -- Unexpected Scenario
                     ELSE 'Unexpected Scenario'
                     END
                 )                                AS scenario
      FROM lake_consolidated_view.ultra_merchant.membership_token mt
               JOIN lake_consolidated_view.ultra_merchant.membership m
                    ON mt.membership_id = m.membership_id
               JOIN lake_consolidated_view.ultra_merchant.membership_plan mp
                    ON mp.membership_plan_id = m.membership_plan_id
               JOIN lake_consolidated_view.ultra_merchant.store_group sg
                    ON sg.store_group_id = mp.store_group_id
          --  AND sg.active = 1
               JOIN lake_consolidated_view.ultra_merchant.membership_token_reason mtr
                    ON mtr.membership_token_reason_id = mt.membership_token_reason_id
          -- FYI: Chance for a couple duplicate customer_detail records to appear from EMP Migrations.
          -- Current data has been cleaned up and we'll look out for this with any future migrations jobs.
          -- Otherwise future workaround would be to change these to OUTER APPLY ( SELECT TOP(1) .. )
               LEFT JOIN (SELECT customer_id,
                                 name,
                                 MAX(customer_detail_id) AS customer_detail_id,
                                 MAX(datetime_added)     AS max_optin_date
                          FROM lake_consolidated_view.ultra_merchant.customer_detail
                          WHERE name = 'nmp_opt_in_true'
                                -- AND value = TRUE
                          GROUP BY customer_id,
                                   name) cd
                         ON cd.customer_id = m.customer_id
          -- AND cd.name = 'nmp_opt_in_true'
          --AND cd.[value] = 'true'
          --AND cd.[value] IN ('1','yes','true')
          -- For VIP members who migrated to EMP
               LEFT JOIN lake_consolidated_view.ultra_merchant.membership_detail md
                         ON mt.membership_id = md.membership_id
                             AND md.name = 'nmp_migration'
               LEFT JOIN edw_prod.data_model.dim_date dd ON md.value = dd.date_key
          -- For Lead members who migrated to EMP
               LEFT JOIN lake_consolidated_view.ultra_merchant.membership_detail md2
                         ON mt.membership_id = md2.membership_id
                             AND md2.name = 'nmp_migration_payg'
               LEFT JOIN edw_prod.data_model.dim_date dd2 ON md2.value = dd2.date_key
      WHERE mt.statuscode in (
          3540 -- Active
          , 3546 -- Expired
     )) vw;
