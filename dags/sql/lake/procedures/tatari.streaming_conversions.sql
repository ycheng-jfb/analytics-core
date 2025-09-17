delete
from lake.tatari.streaming_conversions_stg
where methodology is null or conversion_metric is null;

MERGE INTO lake.tatari.streaming_conversions t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            *,
            iff(abs(FILENAME_DATE-END_DATE)= 1, 1,0) as is_mature,
            hash(campaign_id, impression_date, creative_code, publisher, spend, methodology, conversion_metric, lift, creative_name, account_name, filename_date, end_date, is_mature) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY campaign_id, conversion_metric, methodology, impression_date, creative_code, account_name ORDER BY is_mature desc, coalesce(filename_date, '1900-01-01') DESC ) AS rn
        FROM lake.tatari.streaming_conversions_stg
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.campaign_id, s.campaign_id)
    AND equal_null(t.conversion_metric, s.conversion_metric)
    AND equal_null(t.methodology, s.methodology)
    AND equal_null(t.impression_date, s.impression_date)
    AND equal_null(t.creative_code, s.creative_code)
    AND equal_null(t.account_name, s.account_name)
WHEN NOT MATCHED THEN INSERT (
    campaign_id, impression_date, creative_code, publisher, spend, methodology, conversion_metric, lift, creative_name, account_name, filename_date, meta_row_hash, meta_create_datetime, meta_update_datetime, is_mature, end_date
)
VALUES (
    campaign_id, impression_date, creative_code, publisher, spend, methodology, conversion_metric, lift, creative_name, account_name, filename_date, meta_row_hash, meta_create_datetime, meta_update_datetime, is_mature, end_date
)
WHEN MATCHED AND t.is_mature = 0 and ((s.is_mature = 1) or (t.meta_row_hash != s.meta_row_hash
    AND coalesce(s.filename_date, '1900-01-01') > coalesce(t.filename_date, '1900-01-01'))) THEN UPDATE
SET t.campaign_id = s.campaign_id,
    t.impression_date = s.impression_date,
    t.creative_code = s.creative_code,
    t.publisher = s.publisher,
    t.spend = s.spend,
    t.methodology = s.methodology,
    t.conversion_metric = s.conversion_metric,
    t.lift = s.lift,
    t.creative_name = s.creative_name,
    t.account_name = s.account_name,
    t.filename_date = s.filename_date,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime,
    t.is_mature  = s.is_mature,
    t.end_date = s.end_date;

CREATE OR REPLACE TEMPORARY TABLE _mature_combos AS
select distinct ACCOUNT_NAME,FILENAME_DATE, end_date
from lake.tatari.streaming_conversions
where IS_MATURE = 1;

delete from lake.tatari.streaming_conversions t
    using _mature_combos mc
    where mc.ACCOUNT_NAME = t.ACCOUNT_NAME and t.impression_date >= mc.FILENAME_DATE
            and t.impression_date < mc.end_date and t.is_mature = 0;

DELETE FROM lake.tatari.streaming_conversions_stg;
