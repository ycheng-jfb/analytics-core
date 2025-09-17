SET start_date = IFNULL((SELECT MAX(date) FROM gfb.gfb_segment_pageviews),
                                    DATEADD('year',-2,DATE_TRUNC('year',CURRENT_TIMESTAMP()))); 
                                    
SET last_update_datetime = CURRENT_TIMESTAMP();

DELETE FROM gfb.gfb_segment_pageviews WHERE date = $start_date;

CREATE OR REPLACE TEMPORARY TABLE _fact_activations AS
SELECT DISTINCT customer_id,
        session_id,
        membership_event_type 
FROM edw_prod.data_model_jfb.fact_membership_event
WHERE membership_event_type='Activation'
AND TO_DATE(event_start_local_datetime)>=$start_date;

CREATE OR REPLACE TEMPORARY TABLE _jf_identity AS
SELECT userid                                                                       AS user_id,
       TO_TIMESTAMP(timestamp)                                                      AS memb_start,
       CASE
           WHEN LOWER(traits_membership_status) = 'lead' THEN 'lead'
           WHEN LOWER(traits_membership_status) IN
                ('vip', 'vip elite', 'sd classic vip', 'vip classic',
                 'elite') THEN 'vip'
           ELSE 'other' END                                                         AS membership_status,
       IFF(traits_email LIKE '%@test.com' OR
           traits_email LIKE '%@example.com', 'test','')                            AS test_account,
       COALESCE(LEAD(TO_TIMESTAMP(timestamp))
                     OVER (PARTITION BY userid ORDER BY TO_TIMESTAMP(timestamp)),
                '9999-12-31')                                                       AS memb_end
FROM lake.segment_gfb.javascript_justfab_identify
WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', memb_start) >= dateadd('month',-1,$start_date);

CREATE OR REPLACE TEMPORARY TABLE _jf_pageview_counts AS
SELECT CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(p.timestamp))::DATE                              AS date
     , 'JF'                                                                                                         AS brand
     , CASE WHEN p.country ILIKE ANY ('US', 'CA') THEN 'NA'
            WHEN p.country IS NULL THEN NULL
            ELSE 'EU' END                                                                                           AS region
     , UPPER(p.country)                                                                                             AS country
     ,CASE WHEN i.membership_status ='vip' THEN 'Repeat'
            WHEN fa.membership_event_type IS NOT NULL AND fa.membership_event_type='Activation' THEN 'Activating'
            ELSE 'Ecom' END                                                                                         AS membership_state
     , COUNT(1)                                                                                                     AS pv_count
FROM lake.segment_gfb.javascript_justfab_page p
     JOIN _jf_identity i
          ON p.userid = i.user_id
              AND TO_TIMESTAMP(p.timestamp) > CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', memb_start)
              AND TO_TIMESTAMP(p.timestamp) < CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', memb_end)
              AND IFNULL(i.test_account, '') <> 'test'
              AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', p.timestamp) >= $start_date
     LEFT JOIN _fact_activations fa
          ON fa.customer_id= COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id)
              AND TRY_TO_NUMBER(fa.session_id)=TRY_TO_NUMBER(p.properties_session_id) 
GROUP BY CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(p.timestamp))::DATE                             
     , 'JF'                                                                                                           
     , CASE WHEN p.country ILIKE ANY ('US', 'CA') THEN 'NA'                       
            WHEN p.country IS NULL THEN NULL                       
            ELSE 'EU' END                                                                                             
     , UPPER(p.country)                                                                                               
     ,CASE WHEN i.membership_status ='vip' THEN 'Repeat'
            WHEN fa.membership_event_type IS NOT NULL AND fa.membership_event_type='Activation' THEN 'Activating'
            ELSE 'Ecom' END;

CREATE OR REPLACE TEMPORARY TABLE _sd_identity AS
SELECT userid                                                                       AS user_id,
       TO_TIMESTAMP(timestamp)                                                      AS memb_start,
       CASE
           WHEN LOWER(traits_membership_status) = 'lead' THEN 'lead'
           WHEN LOWER(traits_membership_status) IN
                ('vip', 'vip elite', 'sd classic vip', 'vip classic',
                 'elite') THEN 'vip'
           ELSE 'other' END                                                         AS membership_status,
       IFF(traits_email LIKE '%@test.com' OR
           traits_email LIKE '%@example.com', 'test','')                            AS test_account,
       COALESCE(LEAD(TO_TIMESTAMP(timestamp))
                     OVER (PARTITION BY userid ORDER BY TO_TIMESTAMP(timestamp)),
                '9999-12-31')                                                       AS memb_end
FROM lake.segment_gfb.javascript_shoedazzle_identify
WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', memb_start) >= dateadd('month',-1,$start_date);

CREATE OR REPLACE TEMPORARY TABLE _sd_pageview_counts AS
SELECT CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(p.timestamp))::DATE                              AS date
 , 'SD'                                                                                                             AS brand
     , CASE WHEN p.country ILIKE ANY ('US', 'CA') THEN 'NA'
            WHEN p.country IS NULL THEN NULL
            ELSE 'EU' END                                                                                           AS region
     , UPPER(p.country)                                                                                             AS country
     ,CASE WHEN i.membership_status ='vip' THEN 'Repeat'
            WHEN fa.membership_event_type IS NOT NULL AND fa.membership_event_type='Activation' THEN 'Activating'
            ELSE 'Ecom' END                                                                                         AS membership_state
     , COUNT(1)                                                                                                     AS pv_count
FROM lake.segment_gfb.javascript_shoedazzle_page p
     JOIN _sd_identity i
          ON p.userid = i.user_id
              AND TO_TIMESTAMP(p.timestamp) > CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', memb_start)
              AND TO_TIMESTAMP(p.timestamp) < CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', memb_end)
              AND IFNULL(i.test_account, '') <> 'test'
              AND p.country = 'us'
              AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', p.timestamp) >= $start_date
     LEFT JOIN _fact_activations fa
          ON fa.customer_id= COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id)
              AND TRY_TO_NUMBER(fa.session_id)=TRY_TO_NUMBER(p.properties_session_id)  
GROUP BY CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(p.timestamp))::DATE                             
     , 'SD'                                                                                                           
     , CASE WHEN p.country ILIKE ANY ('US', 'CA') THEN 'NA'                       
            WHEN p.country IS NULL THEN NULL                       
            ELSE 'EU' END                                                                                             
     , UPPER(p.country)                                                                                               
     ,CASE WHEN i.membership_status ='vip' THEN 'Repeat'
            WHEN fa.membership_event_type IS NOT NULL AND fa.membership_event_type='Activation' THEN 'Activating'
            ELSE 'Ecom' END;

CREATE OR REPLACE TEMPORARY TABLE _fk_identity AS
SELECT userid                                                                       AS user_id,
       TO_TIMESTAMP(timestamp)                                                      AS memb_start,
       CASE
           WHEN LOWER(traits_membership_status) = 'lead' THEN 'lead'
           WHEN LOWER(traits_membership_status) IN
                ('vip', 'vip elite', 'sd classic vip', 'vip classic',
                 'elite') THEN 'vip'
           ELSE 'other' END                                                         AS membership_status,
       IFF(traits_email LIKE '%@test.com' OR
           traits_email LIKE '%@example.com', 'test','')                            AS test_account,
       COALESCE(LEAD(TO_TIMESTAMP(timestamp))
                     OVER (PARTITION BY userid ORDER BY TO_TIMESTAMP(timestamp)),
                '9999-12-31')                                                       AS memb_end
FROM lake.segment_gfb.javascript_fabkids_identify
WHERE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', memb_start) >= dateadd('month',-1,$start_date);

CREATE OR REPLACE TEMPORARY TABLE _fk_pageview_counts AS
SELECT CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(p.timestamp))::DATE                                AS date
     , 'FK'                                                                                                           AS brand
     , CASE WHEN p.country ILIKE ANY ('US', 'CA') THEN 'NA'                       
            WHEN p.country IS NULL THEN NULL                       
            ELSE 'EU' END                                                                                             AS region
     , UPPER(p.country)                                                                                               AS country
     ,CASE WHEN i.membership_status ='vip' THEN 'Repeat'
            WHEN fa.membership_event_type IS NOT NULL AND fa.membership_event_type='Activation' THEN 'Activating'
            ELSE 'Ecom' END                                                                                           AS membership_state
     , COUNT(1)                                                                                                       AS pv_count
FROM lake.segment_gfb.javascript_fabkids_page p
     JOIN _fk_identity i
          ON p.userid = i.user_id
              AND TO_TIMESTAMP(p.timestamp) > CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', memb_start)
              AND TO_TIMESTAMP(p.timestamp) < CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', memb_end)
              AND IFNULL(i.test_account, '') <> 'test'
              AND p.country = 'us'
              AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', p.timestamp) >= $start_date
      LEFT JOIN _fact_activations fa
          ON fa.customer_id= COALESCE(TRY_TO_NUMERIC(userid), properties_customer_id)
              AND TRY_TO_NUMBER(fa.session_id)=TRY_TO_NUMBER(p.properties_session_id)  
GROUP BY CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(p.timestamp))::DATE                             
     , 'FK'                                                                                                           
     , CASE WHEN p.country ILIKE ANY ('US', 'CA') THEN 'NA'                       
            WHEN p.country IS NULL THEN NULL                       
            ELSE 'EU' END                                                                                             
     , UPPER(p.country)                                                                                               
     ,CASE WHEN i.membership_status ='vip' THEN 'Repeat'
            WHEN fa.membership_event_type IS NOT NULL AND fa.membership_event_type='Activation' THEN 'Activating'
            ELSE 'Ecom' END;

INSERT INTO gfb.gfb_segment_pageviews
SELECT date,
       brand,
       region,
       country,
       pv_count,
       $last_update_datetime AS last_update_datetime,
       membership_state
FROM _jf_pageview_counts
UNION ALL
SELECT date,
       brand,
       region,
       country,
       pv_count,
       $last_update_datetime AS last_update_datetime,
       membership_state
FROM _sd_pageview_counts
UNION ALL
SELECT date,
       brand,
       region,
       country,
       pv_count,
       $last_update_datetime AS last_update_datetime,
       membership_state
FROM _fk_pageview_counts;
