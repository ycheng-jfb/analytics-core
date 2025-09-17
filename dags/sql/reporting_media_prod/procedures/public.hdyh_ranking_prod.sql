set update_datetime = current_timestamp();
--record what is in hdyh now

INSERT INTO reporting_media_base_prod.influencers.hdyh_answers_historical
WITH _ds           AS (SELECT DISTINCT store_group_id, store_group, store_country FROM edw_prod.data_model.dim_store)
   , _global_lists AS (SELECT hdyh_global_list_id, customer_referrer_id
                       FROM lake_consolidated.ultra_merchant.hdyh_global_list_referrer
                       UNION ALL
                       --analytics algo plus assigned doesn't come through the same way. is_influencer indicates if it came from the algo
                       SELECT 720 AS hdyh_global_list_id, customer_referrer_id
                       FROM lake_consolidated.ultra_merchant.customer_referrer
                       WHERE store_group_id = 16
                         AND active = 1
                         AND is_influencer = TRUE)

SELECT _ds.store_group,
       _ds.store_country,
       CASE WHEN c.meta_company_id = 20 AND hdyh_global_list_id = 220 THEN 'foundry test'
            WHEN c.meta_company_id = 20 AND hdyh_global_list_id = 420 THEN 'yty'
            WHEN c.meta_company_id = 20 AND hdyh_global_list_id = 520 THEN 'scb'
            WHEN c.meta_company_id = 20 AND hdyh_global_list_id IN (620, 920, 1020, 1320, 1620, 2020) THEN 'flm'
            WHEN c.meta_company_id = 20 AND hdyh_global_list_id IN (720, 820, 1120, 1220, 1420, 1520, 1720, 1820, 1920) THEN 'flw'
            WHEN c.meta_company_id = 30 THEN 'sx'
            WHEN c.meta_company_id = 10 AND store_group ILIKE 'ShoeDazzle%' THEN 'sd'
            WHEN c.meta_company_id = 10 AND store_group ILIKE 'FabKids%' THEN 'fk'
            WHEN c.meta_company_id = 10 AND store_group ILIKE 'JustFab%' THEN 'jf'
            WHEN c.meta_company_id = 10 THEN store_group END AS hdyh_list,
       c.*,
       current_timestamp()                                   AS timestamp
FROM lake_consolidated.ultra_merchant.customer_referrer c
     LEFT JOIN _ds ON _ds.store_group_id = c.store_group_id
     LEFT JOIN _global_lists g ON c.customer_referrer_id = g.customer_referrer_id
WHERE c.active = 1
  AND hdyh_list IS NOT NULL
  AND store_country = 'US'
ORDER BY hdyh_list, label DESC;

create or replace transient table reporting_media_prod.public.hdyh_ranking_prod_sxna (
    hdyh_name varchar,
    media_partner_id varchar,
    update_datetime datetime,
    store_group_id int,
    group_name varchar
) as
    select hdyh_name ,
            media_partner_id,
           $update_datetime as update_datetime,
           (select distinct STORE_GROUP_ID from edw_prod.data_model.dim_store where store_brand_abbr = 'SX'and store_country = 'US') as store_group_id,
           group_name
    from
        (select distinct influencer_cleaned_name as hdyh_name,
                         cast(MEDIA_PARTNER_ID as VARCHAR) as media_partner_id,
                        'influencers' as group_name
        from reporting_media_base_prod.public.HDYH_INFLUENCER_RANKING_SXNA

        union all

        select distinct influencer_name as hdyh_name,  media_partner_id, 'influencers' as group_name
        from lake_view.sharepoint.med_hardcoded_influencer_hdyh_answers
        WHERE media_partner_id is not null
        and media_partner_id not ilike '#N/A (Did not find value % in VLOOKUP evaluation.)'
        and media_partner_id not in (select distinct cast(media_partner_id as VARCHAR) as media_partner_id from reporting_media_base_prod.public.HDYH_INFLUENCER_RANKING_SXNA)
        and lower(store_brand_abbr) = 'sx'

        union all

        select hdyh_name,
               NULL as media_partner_id,
               case when hdyh_name in ('Influencer/Blogger') then 'influencers' else 'other' end as group_name
        from (values ('Influencer/Blogger'),
                     ('Rihanna'),
                     ('TikTok'),
                     ('Facebook'),
                     ('Podcast'),
                     ('Pinterest'),
                     ('Banner Ad'),
                     ('Twitter'),
                     ('TV/Streaming'),
                     ('Nordstrom'),
                     ('YouTube'),
                     ('Instagram'),
                     ('#SavageXAmbassador'),
                     ('Online Magazine'),
                     ('Friend'),
                     ('Amazon'),
                     ('Search Engine')) as v1 (hdyh_name)

        );

create or replace transient table reporting_media_prod.public.hdyh_ranking_prod_flna (
    hdyh_name varchar,
    media_partner_id varchar,
    update_datetime datetime,
    store_group_id int,
    group_name varchar
) as
    select hdyh_name ,
            media_partner_id,
           $update_datetime as update_datetime,
           (select distinct STORE_GROUP_ID from edw_prod.data_model.dim_store where store_brand_abbr = 'FL'and store_country = 'US') as store_group_id,
           group_name
    from
        (select distinct influencer_cleaned_name as hdyh_name,
                         cast(MEDIA_PARTNER_ID as VARCHAR) as media_partner_id,
                        'influencers' as group_name
        from reporting_media_base_prod.public.hdyh_influencer_ranking_flna

        union all

        select distinct influencer_name as hdyh_name, media_partner_id, 'influencers' as group_name
        from lake_view.sharepoint.med_hardcoded_influencer_hdyh_answers
        WHERE media_partner_id is not null
        and media_partner_id not ilike '#N/A (Did not find value % in VLOOKUP evaluation.)'
        and media_partner_id not in (select distinct cast(media_partner_id as VARCHAR) as media_partner_id from reporting_media_base_prod.public.HDYH_INFLUENCER_RANKING_FLNA)
        and lower(store_brand_abbr) = 'fl'
        );

CREATE OR REPLACE TRANSIENT TABLE
    reporting_media_prod.public.hdyh_ranking_prod (
        hdyh_name varchar,
        media_partner_id varchar,
        update_datetime datetime,
        store_group_id int,
        group_name varchar
        )
AS
SELECT DISTINCT
    hdyh_name,
    media_partner_id,
    update_datetime,
    store_group_id,
    group_name
FROM
    reporting_media_prod.public.hdyh_ranking_prod_sxna
UNION ALL
SELECT DISTINCT
    hdyh_name,
    media_partner_id,
    update_datetime,
    store_group_id,
    group_name
FROM
    reporting_media_prod.public.hdyh_ranking_prod_flna
ORDER BY
    store_group_id,
    group_name;
