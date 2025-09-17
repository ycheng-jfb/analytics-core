MERGE INTO lake.tatari.linear_spend_and_impressions t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            *,
            iff(abs(FILENAME_DATE-END_DATE) < 32, 1,0) as is_mature,
            hash(spot_datetime,creative_name,creative_code,network,program,spend,impressions,lift,is_deadzoned,spot_id,account_name,filename_date,end_date,is_mature) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY spot_datetime, spot_id, account_name ORDER BY is_mature desc, coalesce(filename_date, '1900-01-01') DESC ) AS rn
        FROM lake.tatari.linear_spend_and_impressions_stg
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.spot_datetime, s.spot_datetime)
    AND equal_null(t.spot_id, s.spot_id)
    AND equal_null(t.account_name, s.account_name)
WHEN NOT MATCHED THEN INSERT (
    spot_datetime, creative_name, creative_code, network, program, spend, impressions, lift, is_deadzoned, spot_id, account_name, filename_date, meta_row_hash, meta_create_datetime, meta_update_datetime,end_date,is_mature
)
VALUES (
    spot_datetime, creative_name, creative_code, network, program, spend, impressions, lift, is_deadzoned, spot_id, account_name, filename_date, meta_row_hash, meta_create_datetime, meta_update_datetime,end_date,is_mature
)
WHEN MATCHED AND t.is_mature = 0 and ((s.is_mature = 1) or (t.meta_row_hash != s.meta_row_hash
    AND coalesce(s.filename_date, '1900-01-01') > coalesce(t.filename_date, '1900-01-01'))) THEN UPDATE
SET t.spot_datetime = s.spot_datetime,
    t.creative_name = s.creative_name,
    t.creative_code = s.creative_code,
    t.network = s.network,
    t.program = s.program,
    t.spend = s.spend,
    t.impressions = s.impressions,
    t.lift = s.lift,
    t.is_deadzoned = s.is_deadzoned,
    t.spot_id = s.spot_id,
    t.account_name = s.account_name,
    t.filename_date = s.filename_date,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime,
    t.end_date = s.end_date,
    t.is_mature = s.is_mature;

CREATE OR REPLACE TEMPORARY TABLE _mature_combos AS
SELECT DISTINCT account_name, filename_date, end_date
    FROM lake.tatari.linear_spend_and_impressions
    WHERE is_mature = 1;

DELETE
    FROM lake.tatari.linear_spend_and_impressions t
        USING _mature_combos mc
    WHERE mc.account_name = t.account_name
      AND t.spot_datetime >= mc.filename_date
      AND t.spot_datetime < mc.end_date
      AND t.is_mature = 0;

DELETE FROM lake.tatari.linear_spend_and_impressions_stg;
