

MERGE INTO lake.tatari.streaming_spend_and_impression t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            *,
            iff(abs(FILENAME_DATE-END_DATE) < 32, 1,0) as is_mature,
            hash(date, platform, measured_spend, booked_cpm, impressions, effective_spend, effective_cpm, creative_code, campaign_id, creative_name, account_name, filename_date, end_date, is_mature) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY date, platform, creative_code, campaign_id, account_name ORDER BY is_mature desc, coalesce(filename_date, '1900-01-01') DESC ) AS rn
        FROM lake.tatari.streaming_spend_and_impression_stg
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.date, s.date)
    AND equal_null(t.platform, s.platform)
    AND equal_null(t.creative_code, s.creative_code)
    AND equal_null(t.campaign_id, s.campaign_id)
    AND equal_null(t.account_name, s.account_name)
WHEN NOT MATCHED THEN INSERT (
    date, platform, measured_spend, booked_cpm, impressions, effective_spend, effective_cpm, creative_code, campaign_id, creative_name, account_name, filename_date, meta_row_hash, meta_create_datetime, meta_update_datetime, end_date, is_mature
)
VALUES (
    date, platform, measured_spend, booked_cpm, impressions, effective_spend, effective_cpm, creative_code, campaign_id, creative_name, account_name, filename_date, meta_row_hash, meta_create_datetime, meta_update_datetime, end_date, is_mature
)
WHEN MATCHED AND t.is_mature = 0 and ((s.is_mature = 1) or (t.meta_row_hash != s.meta_row_hash
    AND coalesce(s.filename_date, '1900-01-01') > coalesce(t.filename_date, '1900-01-01'))) THEN UPDATE
SET t.date = s.date,
    t.platform = s.platform,
    t.measured_spend = s.measured_spend,
    t.booked_cpm = s.booked_cpm,
    t.impressions = s.impressions,
    t.effective_spend = s.effective_spend,
    t.effective_cpm = s.effective_cpm,
    t.creative_code = s.creative_code,
    t.campaign_id = s.campaign_id,
    t.creative_name = s.creative_name,
    t.account_name = s.account_name,
    t.filename_date = s.filename_date,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime,
    t.end_date = s.end_date,
    t.is_mature = s.is_mature;

CREATE OR REPLACE TEMPORARY TABLE _mature_combos AS
select distinct ACCOUNT_NAME,FILENAME_DATE, END_DATE
from lake.tatari.streaming_spend_and_impression
where IS_MATURE = 1;

delete from lake.tatari.streaming_spend_and_impression t
    using _mature_combos mc
    where mc.ACCOUNT_NAME = t.ACCOUNT_NAME and t.DATE >= mc.FILENAME_DATE
            and t.DATE <  mc.END_DATE  and t.is_mature = 0;

DELETE FROM lake.tatari.streaming_spend_and_impression_stg;
