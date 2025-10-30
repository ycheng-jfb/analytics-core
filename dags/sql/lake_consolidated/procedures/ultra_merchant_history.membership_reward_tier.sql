--Full refresh
create or replace TRANSIENT TABLE lake_consolidated.ultra_merchant_history.MEMBERSHIP_REWARD_TIER (
	DATA_SOURCE_ID NUMBER(38,0),
	META_COMPANY_ID NUMBER(38,0),
	MEMBERSHIP_REWARD_TIER_ID NUMBER(38,0),
	MEMBERSHIP_REWARD_PLAN_ID NUMBER(38,0),
	MEMBERSHIP_LEVEL_GROUP_ID NUMBER(38,0),
	LABEL VARCHAR(50),
	REQUIRED_POINTS_EARNED NUMBER(38,0),
	REQUIRED_MONTHS NUMBER(38,0),
	PURCHASE_POINT_MULTIPLIER NUMBER(38,0),
	TIER_HIDDEN BOOLEAN,
	MINIMUM_LIFETIME_SPEND NUMBER(19,4),
	META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID NUMBER(38,0),
	META_ROW_SOURCE VARCHAR(40),
	HVR_CHANGE_OP NUMBER(38,0),
	EFFECTIVE_START_DATETIME TIMESTAMP_LTZ(3),
	EFFECTIVE_END_DATETIME TIMESTAMP_LTZ(3),
	META_UPDATE_DATETIME TIMESTAMP_LTZ(3)
);


--SET lake_jfb_watermark = (
--    select min(last_update)
--    from (
--        select cast(min(META_SOURCE_CHANGE_DATETIME) as TIMESTAMP_LTZ(3)) AS last_update
--        FROM lake_jfb.ultra_merchant_history.membership_reward_tier
--    ) AS A
--);


create or replace TEMP TABLE _lake_jfb_membership_reward_tier_history AS
select distinct
    MEMBERSHIP_REWARD_TIER_ID,
    MEMBERSHIP_REWARD_PLAN_ID,
    MEMBERSHIP_LEVEL_GROUP_ID,
    LABEL,
    REQUIRED_POINTS_EARNED,
    REQUIRED_MONTHS,
    PURCHASE_POINT_MULTIPLIER,
    TIER_HIDDEN,
    MINIMUM_LIFETIME_SPEND,
    META_ROW_SOURCE,
    HVR_CHANGE_OP,
    cast(META_SOURCE_CHANGE_DATETIME as TIMESTAMP_LTZ(3))  as effective_start_datetime,
    10 as data_source_id
FROM lake_jfb.ultra_merchant_history.membership_reward_tier
--WHERE META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
QUALIFY ROW_NUMBER() OVER(PARTITION BY MEMBERSHIP_REWARD_TIER_ID, META_SOURCE_CHANGE_DATETIME
    ORDER BY HVR_CHANGE_SEQUENCE DESC) = 1;


--CREATE OR REPLACE TEMP TABLE _lake_jfb_company_membership_reward AS
--SELECT MEMBERSHIP_REWARD_TIER_ID, company_id as meta_company_id
--FROM (
--     SELECT DISTINCT
--         L.MEMBERSHIP_REWARD_TIER_ID,
--         DS.COMPANY_ID
--     FROM lake_jfb.REFERENCE.DIM_STORE AS DS
--     INNER JOIN lake_jfb.ultra_merchant_history.membership_reward_tier AS L
--     ON DS.STORE_ID = L.STORE_ID
--    WHERE l.META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, $lake_jfb_watermark)
--    ) AS l
--QUALIFY ROW_NUMBER() OVER(PARTITION BY MEMBERSHIP_REWARD_TIER_ID ORDER BY company_id ASC) = 1;


INSERT INTO lake_consolidated.ultra_merchant_history.membership_reward_tier (
    DATA_SOURCE_ID,
	META_COMPANY_ID,
	MEMBERSHIP_REWARD_TIER_ID,
	MEMBERSHIP_REWARD_PLAN_ID,
	MEMBERSHIP_LEVEL_GROUP_ID,
	LABEL,
	REQUIRED_POINTS_EARNED,
	REQUIRED_MONTHS,
	PURCHASE_POINT_MULTIPLIER,
	TIER_HIDDEN,
	MINIMUM_LIFETIME_SPEND,
	META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID,
	META_ROW_SOURCE,
	HVR_CHANGE_OP,
	EFFECTIVE_START_DATETIME,
	EFFECTIVE_END_DATETIME,
	META_UPDATE_DATETIME
)
SELECT DISTINCT
    s.data_source_id,
    null as meta_company_id,

--    CASE WHEN NULLIF(s.MEMBERSHIP_REWARD_TIER_ID, '0') IS NOT NULL THEN CONCAT(s.MEMBERSHIP_REWARD_TIER_ID, null) ELSE NULL END as MEMBERSHIP_REWARD_TIER_ID,
    IFF(s.MEMBERSHIP_REWARD_TIER_ID = 0, NULL, s.MEMBERSHIP_REWARD_TIER_ID) AS MEMBERSHIP_REWARD_TIER_ID,
    MEMBERSHIP_REWARD_PLAN_ID,
	MEMBERSHIP_LEVEL_GROUP_ID,
	LABEL,
	REQUIRED_POINTS_EARNED,
	REQUIRED_MONTHS,
	PURCHASE_POINT_MULTIPLIER,
	TIER_HIDDEN,
	MINIMUM_LIFETIME_SPEND,
    s.MEMBERSHIP_REWARD_TIER_ID as META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID,
    s.meta_row_source,
    s.hvr_change_op,
    s.effective_start_datetime,
    NULL AS effective_end_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime
FROM (
    SELECT
        A.*
--        ,COALESCE(CJ.meta_company_id, 40) AS meta_company_id
    FROM _lake_jfb_membership_reward_tier_history AS A
--    LEFT JOIN _lake_jfb_company_membership_reward AS CJ
--        ON cj.MEMBERSHIP_REWARD_TIER_ID = a.MEMBERSHIP_REWARD_TIER_ID
--    WHERE COALESCE(CJ.meta_company_id, 40) IN (10)
    ) AS s
WHERE NOT EXISTS (
    SELECT
        1
    FROM lake_consolidated.ultra_merchant_history.membership_reward_tier AS t
    WHERE
        s.data_source_id = t.data_source_id
        AND          s.MEMBERSHIP_REWARD_TIER_ID = t.META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID
        AND s.effective_start_datetime = t.effective_start_datetime
    )
ORDER BY s.effective_start_datetime ASC;


CREATE OR REPLACE temp table _membership_reward_tier_updates as
SELECT s.*,
    row_number() over(partition by s.data_source_id,

        s.META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID
ORDER BY s.effective_start_datetime ASC) AS rnk
FROM (
SELECT DISTINCT
    s.data_source_id,

        s.META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID,
    s.effective_start_datetime,
    s.effective_end_datetime
FROM lake_consolidated.ultra_merchant_history.membership_reward_tier as s
INNER JOIN (
    SELECT
        data_source_id,

        META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID,
        effective_start_datetime
    FROM lake_consolidated.ultra_merchant_history.membership_reward_tier
    WHERE effective_end_datetime is NULL
    ) AS t
    ON s.data_source_id = t.data_source_id
    AND     s.META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID = t.META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID
WHERE (
    s.effective_start_datetime >= t.effective_start_datetime
    OR s.effective_end_datetime = '9999-12-31 00:00:00.000 -0800'
)) s;


CREATE OR REPLACE TEMP TABLE _membership_reward_tier_delta AS
SELECT
    s.data_source_id,

        s.META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID,
    s.effective_start_datetime,
    COALESCE(dateadd(MILLISECOND, -1, t.effective_start_datetime), '9999-12-31 00:00:00.000 -0800') AS new_effective_end_datetime
FROM _membership_reward_tier_updates AS s
    LEFT JOIN _membership_reward_tier_updates AS t
    ON s.data_source_id = t.data_source_id
    AND     s.META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID = t.META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID
    AND S.rnk = T.rnk - 1
WHERE (S.effective_end_datetime <> dateadd(MILLISECOND, -1, T.effective_start_datetime)
    OR S.effective_end_datetime IS NULL) ;


UPDATE lake_consolidated.ultra_merchant_history.membership_reward_tier AS t
SET t.effective_end_datetime = s.new_effective_end_datetime,
    META_UPDATE_DATETIME = current_timestamp()
FROM _membership_reward_tier_delta AS s
WHERE s.data_source_id = t.data_source_id
    AND     s.META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID = t.META_ORIGINAL_MEMBERSHIP_REWARD_TIER_ID
    AND s.effective_start_datetime = t.effective_start_datetime
    AND COALESCE(t.effective_end_datetime, '1900-01-01 00:00:00 -0800') <> s.new_effective_end_datetime
