SET max_date = (SELECT  NVL(MAX(date), '2018-06-09')  FROM REPORTING_PROD.SXF.DAILY_BOP_EOP_COUNTS);

CREATE OR REPLACE TEMPORARY TABLE _date AS
SELECT
	d.date_key
	,d.full_date
FROM EDW_PROD.DATA_MODEL_SXF.DIM_DATE d
WHERE
	d.full_date >= DATEADD(DAY, -60, $max_date)
	AND d.full_date <= CURRENT_TIMESTAMP :: DATE;

SET bop_date = (SELECT TOP 1 full_date FROM _date ORDER BY date_key ASC);
SET bop_date_key = (SELECT TOP 1 date_key FROM _date ORDER BY date_key ASC);
SET bop_date_max = (SELECT TOP 1 full_date FROM _date ORDER BY date_key DESC);
SET bop_date_key_max = (SELECT TOP 1 date_key FROM _date ORDER BY date_key DESC);

CREATE OR REPLACE TEMPORARY TABLE _omni_classification AS
(
    SELECT customer_id
        ,activation_key
        ,order_channel
        ,ROW_NUMBER() OVER(PARTITION BY customer_id,activation_key ORDER BY order_local_datetime_start DESC) row_num
    FROM REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION
    WHERE activation_key IS NOT NULL
         qualify row_num = 1
);

-- Daily Total VIPs by VIP type
CREATE OR REPLACE TEMPORARY TABLE _bop_mee AS
	SELECT mee.customer_id
        ,max_event.store_full_name AS store_reporting_name
        ,st1.store_full_name AS vip_activation_store_name
        ,st1.store_type AS vip_activation_store_type
        ,CASE
                 WHEN omc.order_channel ILIKE 'Omni' THEN 'Omni Channel'
                 WHEN omc.order_channel ILIKE 'Online' THEN 'Online Purchaser Only'
                 WHEN omc.order_channel ILIKE 'Retail' THEN 'Retail Purchaser Only'
                ELSE 'Unsure'
            END AS omni_channel
		,max_event.bop_date
		,max_event.bop_date_key
        ,fa.vip_cohort_month_date AS vip_cohort
        ,'M' || TO_VARCHAR(DATEDIFF(month, vip_cohort, DATE(max_event.bop_date)) + 1) as Tenure
	    ,CASE WHEN is_reactivated_vip = 'FALSE' THEN 'First Time Activation' ELSE 'Reactivation' END Activation_Type
        ,CASE
            WHEN membership_type = 'Membership Fee' THEN 'Annual'
            ELSE membership_type
            END AS membership_type
	FROM EDW_PROD.DATA_MODEL_SXF.FACT_MEMBERSHIP_EVENT mee
	JOIN (
         SELECT
			mee.customer_id
           ,st.store_full_name
           ,d.full_date AS bop_date
           ,d.date_key AS bop_date_key
           ,MAX(to_timestamp_ntz(mee.event_start_local_datetime)) AS max_event_local_datetime
           ,MAX(to_timestamp_ntz(mee.event_end_local_datetime)) AS max_event_end_local_datetime
		FROM EDW_PROD.DATA_MODEL_SXF.FACT_MEMBERSHIP_EVENT mee
		JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE st ON st.store_id = mee.store_id
		JOIN EDW_PROD.DATA_MODEL_SXF.DIM_DATE d ON d.full_date >  DATE(mee.event_start_local_datetime)
			AND  d.date_key >= $bop_date_key
            AND d.date_key <= $bop_date_key_max
       WHERE st.store_brand_abbr = 'SX'
             AND st.store_full_name NOT IN ('Savage X CA','Savage X NL','Savage X DK','Savage X SE')
	         AND st.store_full_name NOT LIKE '%(DM)%'
      GROUP BY
			mee.customer_id
           ,st.store_full_name
           ,d.full_date
           ,d.date_key
	) max_event ON max_event.max_event_local_datetime = to_timestamp_ntz(mee.event_start_local_datetime)
		AND max_event.customer_id = mee.customer_id
	JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.MEMBERSHIP m ON m.customer_id = mee.customer_id
            AND mee.membership_state IN ('VIP')
    JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TYPE mt ON mt.membership_type_id = m.membership_type_id
    JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ACTIVATION fa ON fa.membership_event_key = mee.membership_event_key
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE st1 ON st1.store_id = fa.sub_store_id
    LEFT JOIN _omni_classification omc ON omc.customer_id = fa.customer_id AND fa.activation_key = omc.activation_key;

CREATE OR REPLACE TEMPORARY TABLE _daily_bop_vip_count AS
	SELECT
		bm.bop_date AS bop_date
		,bm.store_reporting_name
        ,bm.vip_cohort
        ,COALESCE(bm.Tenure , 'Unsure') AS Tenure
        ,COALESCE(bm.Activation_Type ,'Unsure' ) AS Activation_Type
        ,COALESCE(bm.membership_type, 'Unsure') AS membership_type
        ,COALESCE(bm.vip_activation_store_name , 'Unsure') AS vip_activation_store_name
        ,COALESCE(bm.vip_activation_store_type ,'Unsure' ) AS vip_activation_store_type
        ,COALESCE(bm.omni_channel, 'Unsure') AS omni_channel
		,COUNT(bm.customer_id) AS bop_vip_count
		FROM _bop_mee bm
	GROUP BY bop_date,
        store_reporting_name,
        vip_cohort,
        Tenure,
        Activation_Type,
        membership_Type,
        vip_activation_store_name,
        vip_activation_store_type,
        omni_channel;

CREATE OR REPLACE TEMPORARY TABLE _daily_eop_vip_count AS
	SELECT
		DATEADD(DAY, -1, bm.bop_date) AS eop_date
		,bm.store_reporting_name
        ,bm.vip_cohort
        ,COALESCE(bm.Tenure , 'Unsure') AS Tenure
        ,COALESCE(bm.Activation_Type ,'Unsure' ) AS Activation_Type
        ,COALESCE(bm.membership_type, 'Unsure') AS membership_type
        ,COALESCE(bm.vip_activation_store_name , 'Unsure') AS vip_activation_store_name
        ,COALESCE(bm.vip_activation_store_type ,'Unsure' ) AS vip_activation_store_type
        ,COALESCE(bm.omni_channel, 'Unsure') AS omni_channel
		,COUNT(bm.customer_id) AS eop_vip_count
		FROM _bop_mee bm
	GROUP BY eop_date,
        store_reporting_name,
        vip_cohort,
        Tenure,
        Activation_Type,
        membership_Type,
        vip_activation_store_name,
        vip_activation_store_type,
        omni_channel;

DELETE FROM REPORTING_PROD.SXF.DAILY_BOP_EOP_COUNTS
    WHERE date >= DATEADD(DAY, -60, $max_date);

INSERT INTO REPORTING_PROD.SXF.DAILY_BOP_EOP_COUNTS(
        date,
        store_reporting_name,
        store_brand_name,
        store_name,
        store_region_abbr,
        store_country_abbr,
        vip_activation_store_name,
        vip_activation_store_type,
        omni_channel,
        vip_cohort,
        Tenure,
        Activation_Type,
        membership_Type,
        bop_vip_count,
        eop_vip_count
    )
        SELECT COALESCE(bop.bop_date,eop.eop_date) AS date
		,COALESCE(bop.store_reporting_name,eop.store_reporting_name) AS store_reporting_name
        ,st.store_brand
        ,st.store_name
        ,st.store_region
        ,st.store_country
        ,COALESCE(bop.vip_activation_store_name ,eop.vip_activation_store_name) AS vip_activation_store_name
        ,COALESCE(bop.vip_activation_store_type ,eop.vip_activation_store_type ) AS vip_activation_store_type
        ,COALESCE(bop.omni_channel, eop.omni_channel) AS omni_channel
        ,COALESCE(bop.vip_cohort,eop.vip_cohort) vip_cohort
        ,COALESCE(bop.Tenure,eop.Tenure) Tenure
        ,COALESCE(bop.Activation_Type,eop.Activation_Type) Activation_Type
        ,COALESCE(bop.membership_Type,eop.membership_Type) membership_Type
		--bop vip count
		,COALESCE(bop.bop_vip_count, 0) AS bop_vip_count
		--eop vip count
		,COALESCE(eop.eop_vip_count, 0) AS eop_vip_count
        FROM _daily_bop_vip_count bop
        FULL OUTER JOIN _daily_eop_vip_count eop
            ON bop.bop_date = eop.eop_date
            AND bop.store_reporting_name = eop.store_reporting_name
            AND bop.vip_cohort = eop.vip_cohort
            AND bop.Tenure = eop.Tenure
            AND bop.Activation_Type = eop.Activation_Type
            AND bop.membership_Type = eop.membership_Type
            AND bop.vip_activation_store_name  = eop.vip_activation_store_name
            AND bop.vip_activation_store_type = eop.vip_activation_store_type
            AND bop.omni_channel = eop.omni_channel
        JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE st
            ON (st.store_full_name = bop.store_reporting_name
            OR st.store_full_name = eop.store_reporting_name)
         WHERE date >= DATEADD(DAY, -60, $max_date);
