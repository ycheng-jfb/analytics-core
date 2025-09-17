CREATE OR REPLACE TRANSIENT TABLE reporting_media_base_prod.influencers.daily_spend AS
-- FabKids
SELECT media_partner_id,
       post_id AS shared_id,
       media_partner_id || '|' || COALESCE(post_id, 'nosid') AS media_partner_id_shared_id,
       src.business_unit_abbr,
       st.store_id,
       src.creator_name AS influencer_name,
       src.influencer_classification AS channel,
       d.full_date AS date,
       src.post_status,
       0 AS mens_flag,
       0 AS is_scrubs_flag,
       IFF(LOWER(cpa_report_input) IN ('post with spend', 'spend only'), 1, 0) AS include_in_cpa_report_flag,
       SUM(COALESCE(upfront_cost, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                           (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS upfront_spend,
       SUM(COALESCE(cogs, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                   (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS cogs
    FROM lake_view.sharepoint.med_influencer_master_organic_spend_dimensions_fk src
             JOIN edw_prod.data_model.dim_date d ON d.full_date >= src.spend_date_start
        AND d.full_date <= src.spend_date_end
             JOIN edw_prod.data_model.dim_store st ON st.store_full_name = 'FabKids ' || src.spend_country
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
UNION ALL
-- Fabletics
SELECT media_partner_id,
       post_id AS shared_id,
       media_partner_id || '|' || COALESCE(post_id, 'nosid') AS media_partner_id_shared_id,
       src.business_unit_abbr,
       st.store_id,
       src.creator_name AS influencer_name,
       src.influencer_classification AS channel,
       d.full_date AS date,
       src.post_status,
       0 AS mens_flag,
       0 AS is_scrubs_flag,
       IFF(LOWER(cpa_report_input) IN ('post with spend', 'spend only'), 1, 0) AS include_in_cpa_report_flag,
       SUM(COALESCE(upfront_cost, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                           (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS upfront_spend,
       SUM(COALESCE(cogs, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                   (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS cogs
    FROM lake_view.sharepoint.med_INFLUENCER_MASTER_ORGANIC_SPEND_DIMENSIONS_FL src
             JOIN edw_prod.data_model.dim_date d ON d.full_date >= src.spend_date_start
        AND d.full_date <= src.spend_date_end
             JOIN edw_prod.data_model.dim_store st ON st.store_full_name = 'Fabletics ' || src.spend_country
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
UNION ALL
-- JustFab + ShoeDazzle
SELECT media_partner_id,
       post_id AS shared_id,
       media_partner_id || '|' || COALESCE(post_id, 'nosid') AS media_partner_id_shared_id,
       src.business_unit_abbr,
       st.store_id,
       src.creator_name AS influencer_name,
       src.influencer_classification AS channel,
       d.full_date AS date,
       src.post_status,
       0 AS mens_flag,
       0 AS is_scrubs_flag,
       IFF(LOWER(cpa_report_input) IN ('post with spend', 'spend only'), 1, 0) AS include_in_cpa_report_flag,
       SUM(COALESCE(upfront_cost, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                           (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS upfront_spend,
       SUM(COALESCE(cogs, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                   (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS cogs
    FROM lake_view.sharepoint.med_influencer_master_organic_spend_dimensions_gfb src
             JOIN edw_prod.data_model.dim_date d ON d.full_date >= src.spend_date_start
        AND d.full_date <= src.spend_date_end
             JOIN edw_prod.data_model.dim_store st
                  ON st.store_full_name = CASE WHEN src.business_unit_abbr = 'JF' THEN 'JustFab ' || src.spend_country
                                               WHEN src.business_unit_abbr = 'SD'
                                                   THEN 'ShoeDazzle ' || src.spend_country
                                          END
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
UNION ALL
-- Savage X
SELECT media_partner_id,
       post_id AS shared_id,
       media_partner_id || '|' || COALESCE(post_id, 'nosid') AS media_partner_id_shared_id,
       src.business_unit_abbr,
       st.store_id,
       src.creator_name AS influencer_name,
       src.influencer_classification AS channel,
       d.full_date AS date,
       src.post_status,
       0 AS mens_flag,
       0 AS is_scrubs_flag,
       IFF(LOWER(cpa_report_input) IN ('post with spend', 'spend only'), 1, 0) AS include_in_cpa_report_flag,
       SUM(COALESCE(upfront_cost, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                           (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS upfront_spend,
       SUM(COALESCE(cogs, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                   (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS cogs
    FROM lake_view.sharepoint.med_influencer_master_organic_spend_dimensions_sx src
             JOIN edw_prod.data_model.dim_date d ON d.full_date >= src.spend_date_start
        AND d.full_date <= src.spend_date_end
             JOIN edw_prod.data_model.dim_store st ON st.store_full_name = 'Savage X ' || src.spend_country
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
UNION ALL
-- Fabletics Men
SELECT media_partner_id,
       post_id AS shared_id,
       media_partner_id || '|' || COALESCE(post_id, 'nosid') AS media_partner_id_shared_id,
       src.business_unit_abbr,
       st.store_id,
       src.creator_name AS influencer_name,
       src.influencer_classification AS channel,
       d.full_date AS date,
       src.post_status,
       1 AS mens_flag,
       0 AS is_scrubs_flag,
       IFF(LOWER(cpa_report_input) IN ('post with spend', 'spend only'), 1, 0) AS include_in_cpa_report_flag,
       SUM(COALESCE(upfront_cost, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                           (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS upfront_spend,
       SUM(COALESCE(cogs, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                   (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS cogs
    FROM lake_view.sharepoint.med_INFLUENCER_MASTER_ORGANIC_SPEND_DIMENSIONS_FL_MEN src
             JOIN edw_prod.data_model.dim_date d ON d.full_date >= src.spend_date_start
        AND d.full_date <= src.spend_date_end
             JOIN edw_prod.data_model.dim_store st ON st.store_full_name = 'Fabletics ' || src.spend_country
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
UNION ALL
-- Yitty
SELECT media_partner_id,
       post_id AS shared_id,
       media_partner_id || '|' || COALESCE(post_id, 'nosid') AS media_partner_id_shared_id,
       src.business_unit_abbr,
       st.store_id,
       src.creator_name AS influencer_name,
       src.influencer_classification AS channel,
       d.full_date AS date,
       src.post_status,
       0 AS mens_flag,
       0 AS is_scrubs_flag,
       IFF(LOWER(cpa_report_input) IN ('post with spend', 'spend only'), 1, 0) AS include_in_cpa_report_flag,
       SUM(COALESCE(upfront_cost, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                           (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS upfront_spend,
       SUM(COALESCE(cogs, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                   (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS cogs
    FROM lake_view.sharepoint.med_influencer_master_organic_spend_dimensions_yty src
             JOIN edw_prod.data_model.dim_date d ON d.full_date >= src.spend_date_start
        AND d.full_date <= src.spend_date_end
             JOIN edw_prod.data_model.dim_store st ON st.store_full_name = 'Yitty US'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
UNION ALL
-- Fabletics Scrubs
SELECT media_partner_id,
       post_id AS shared_id,
       media_partner_id || '|' || COALESCE(post_id, 'nosid') AS media_partner_id_shared_id,
       src.business_unit_abbr,
       st.store_id,
       src.creator_name AS influencer_name,
       src.influencer_classification AS channel,
       d.full_date AS date,
       src.post_status,
       0 AS mens_flag,
       1 AS is_scrubs_flag,
       IFF(LOWER(cpa_report_input) IN ('post with spend', 'spend only'), 1, 0) AS include_in_cpa_report_flag,
       SUM(COALESCE(upfront_cost, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                           (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS upfront_spend,
       SUM(COALESCE(cogs, 0) / IFF((DATEDIFF(DAY, spend_date_start, spend_date_end) + 1) = 0, 1,
                                   (DATEDIFF(DAY, spend_date_start, spend_date_end) + 1))) AS cogs
    FROM lake_view.sharepoint.med_influencer_master_organic_spend_dimensions_scb src
             JOIN edw_prod.data_model.dim_date d ON d.full_date >= src.spend_date_start
        AND d.full_date <= src.spend_date_end
             JOIN edw_prod.data_model.dim_store st ON st.store_full_name = 'Fabletics ' || src.spend_country
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
-- MicroInfluencers
UNION ALL
SELECT CASE WHEN src.business_unit_abbr = 'JF-BM' AND src.country_abbr = 'US' THEN 'JFBMT32024'
            WHEN src.business_unit_abbr = 'JF-N8' AND src.country_abbr = 'US' THEN 'JFN8T32024'
            WHEN src.business_unit_abbr = 'JF-DP' AND src.country_abbr = 'US' THEN 'JFDPT32024'
            WHEN src.business_unit_abbr = 'JF' AND src.country_abbr = 'US' THEN 'JFT32017'
            WHEN src.business_unit_abbr = 'SD' AND src.country_abbr = 'US' THEN 'SDT32017'
            WHEN src.business_unit_abbr = 'FL' AND src.country_abbr = 'US' THEN '2003347'
            WHEN src.business_unit_abbr = 'FK' AND src.country_abbr = 'US' THEN 'FKT32019'
            WHEN src.business_unit_abbr = 'YTY' AND src.country_abbr = 'US' THEN 'YTYT32022'
            WHEN src.business_unit_abbr = 'SCB' AND src.country_abbr = 'US' THEN 'SCB0001'
            WHEN src.BUSINESS_UNIT_ABBR = 'SX' AND src.COUNTRY_ABBR = 'US' THEN 'SXF0001'
            ELSE 'No ID'
       END AS media_partner_id,
       'None' AS shared_id,
       media_partner_id || '|nosid' AS media_partner_id_shared_id,
       case when src.business_unit_abbr ilike 'JF%' then 'JF' else src.business_unit_abbr end as business_unit_abbr,
       st.store_id,
       case when src.business_unit_abbr = 'JF-BM' then 'BM MicroInfluencers'
            when src.business_unit_abbr = 'JF-N8' then 'N8 MicroInfluencers'
            when src.business_unit_abbr = 'JF-DP' then 'DP MicroInfluencers'
            else 'MicroInfluencers'
            end as influencer_name,
       src.subchannel AS channel,
       d.full_date AS date,
       'Live' AS post_status,
       CASE WHEN src.business_unit_abbr = 'FLM' THEN 1 ELSE 0 END AS mens_flag,
       CASE WHEN src.business_unit_abbr = 'SCB' THEN 1 ELSE 0 END AS is_scrubs_flag,
       CASE WHEN ((src.business_unit_abbr LIKE 'JF%') OR (src.business_unit_abbr IN ('SD', 'FK')))
            AND YEAR(date) >= 2025
            AND st.store_region IN ('NA')
            THEN 0 ELSE 1
           END AS include_in_cpa_report_flag,
       0 AS upfront_spend,
       SUM(COALESCE(cogs, 0) / DAY(LAST_DAY(src.month_start_date))) AS cogs
    FROM lake_view.sharepoint.med_microinfluencer_product_cost src
             JOIN edw_prod.data_model.dim_date d ON d.full_date >= src.month_start_date
        AND d.full_date <= LAST_DAY(src.month_start_date)
             JOIN edw_prod.data_model.dim_store st
                  ON st.store_full_name = CASE WHEN src.business_unit_abbr = 'SX' THEN 'Savage X ' || src.country_abbr
                                               WHEN src.business_unit_abbr in ('JF','JF-BM','JF-N8','JF-DP') THEN 'JustFab ' || src.country_abbr
                                               WHEN src.business_unit_abbr = 'SD' THEN 'ShoeDazzle ' || src.country_abbr
                                               WHEN src.business_unit_abbr IN ('FL', 'FLM', 'SCB') THEN 'Fabletics ' || src.country_abbr
                                               WHEN src.business_unit_abbr = 'FK' THEN 'FabKids ' || src.country_abbr
                                               WHEN src.business_unit_abbr = 'YTY' THEN 'Yitty US'
                                          END
    WHERE src.cogs > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;

------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _fix_irmp
(
    new_unique_id VARCHAR(55),
    old_unique_id VARCHAR(55)
);

INSERT
    INTO _fix_irmp
    VALUES ('2209184', '1939855|snitchery'),
           ('2209179', '1939855|loeylane'),
           ('2210584', '1304717|AshleyNichole'),
           ('2210584', '1304717|ashleynicholeYT'),
           ('2210595', '1304717|vivianvo'),
           ('2210595', '1304717|VivianVo'),
           ('2216316', '1810916|danielleebrownn'),
           ('2216302', '1810916|michelleinfusino'),
           ('2214856', '1810916|whitneyfransway'),
           ('2209463', '1810916|uchenwosu'),
           ('2210524', '1458838|sierranielsen'),
           ('2454658', '1723547|CarissaStanton'),
           ('2454658', '1723547|nosid'),
           ('2454658', '1723547| '),
           ('2549373', '2504979|nosid');

UPDATE reporting_media_base_prod.influencers.daily_spend i
SET media_partner_id = new_unique_id
    FROM _fix_irmp
    WHERE LOWER(_fix_irmp.old_unique_id) = LOWER(i.media_partner_id_shared_id);


CREATE OR REPLACE TEMPORARY TABLE _fix_spend
(
    new_unique_id VARCHAR(55),
    old_unique_id VARCHAR(55)
);

INSERT
    INTO _fix_spend
    VALUES ('1723353|nosid', '1723353|allychen'),
           ('1305321|nosid', '1305321|andidorfman'),
           ('224636|minimarley', '224636|breannachevolleau'),
           ('2454658|nosid', '1723547|CarissaStanton'),
           ('2454658|nosid', '1723547|nosid'),
           ('2454658|nosid', '1723547| '),
           ('420783|choosingchelsea', '420783|chelseaculbertson'),
           ('1457197|beautychickee', '1457197|christinamarieharris'),
           ('1457197|beautychickee', '1457197|nosid'),
           ('1806086|ayeciara', '1806086|ciaraanderson'),
           ('1931663|claudiasulewski', '1931663|claudiasulewski'),
           ('1358723|nosid', '1358723|graceborst'),
           ('1357009|nosid', '1357009|jeanineamapola'),
           ('1357009|nosid', '1357009|jeanineamapola'),
           ('1457730|nosid', '1457730|karissaduncan'),
           ('1401480|nosid', '1401480|maddieziegler'),
           ('1454575|nosid', '1454575|hannahgodwin'),
           ('1454567|nosid', '1454567|stassischroeder'),
           ('1806128|nosid', '1806128|aliyajanell'),
           ('1815819|nosid', '1815819|brittanycartwright'),
           ('1877411|nosid', '1877411|kaylenzahara'),
           ('1903394|nosid', '1903394|kenyascott'),
           ('1381812|nosid', '1381812|sarahraevargas'),
           ('1459923|nosid', '1459923|saffikarina'),
           ('1455920|nosid', '1455920|morganyates'),
           ('1345964|nosid', '1345964|massyarias'),
           ('1907143|nosid', '1907143|astridloch'),
           ('1790399|SierraSchultzzie', '1790399|SierraShultzzie'),
           ('1242537|loeylane', '1242537|nosid'),
           ('1458838|sierranielsen', '1458838|nosid'),
           ('xx1804284|makaylalondon', '1804284|makaylalondon'),
           ('2209184|snitchery', '1939855|snitchery'),
           ('2209179|loeylane', '1939855|loeylane'),
           ('2210584|AshleyNichole', '1304717|AshleyNichole'),
           ('2210584|ashleynicholeYT', '1304717|ashleynicholeYT'),
           ('2210595|vivianvo', '1304717|vivianvo'),
           ('2210595|VivianVo', '1304717|VivianVo'),
           ('2216316|danielleebrownn', '1810916|danielleebrownn'),
           ('2216302|michelleinfusino', '1810916|michelleinfusino'),
           ('2214856|whitneyfransway', '1810916|whitneyfransway'),
           ('2209463|uchenwosu', '1810916|uchenwosu'),
           ('2210524|sierranielsen', '1458838|sierranielsen'),
           ('1458838|meowmeix', '1458838|amandameixner'),
           ('2549373|nosid', '2504979|nosid');

UPDATE reporting_media_base_prod.influencers.daily_spend i
SET media_partner_id_shared_id = new_unique_id
    FROM _fix_spend
    WHERE LOWER(_fix_spend.old_unique_id) = LOWER(i.media_partner_id_shared_id);
