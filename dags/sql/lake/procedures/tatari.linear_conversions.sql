MERGE INTO lake.tatari.linear_conversions t
USING (
    SELECT
        a.*
    FROM (
        SELECT
            *,
            iff(abs(filename_date-end_date) < 3,1,0) as is_mature,
            hash(spot_id,broadcast_week,spot_datetime,creative_name,creative_code,network,rotation,spend,conversion_metric,lift,account_name,filename_date, end_date, is_mature) AS meta_row_hash,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime,
            row_number() OVER ( PARTITION BY spot_id, conversion_metric, account_name ORDER BY is_mature desc, coalesce(filename_date, '1900-01-01') DESC ) AS rn
        FROM lake.tatari.linear_conversions_stg
     ) a
    WHERE a.rn = 1
) s ON equal_null(t.spot_id, s.spot_id)
    AND equal_null(t.conversion_metric, s.conversion_metric)
    AND equal_null(t.account_name, s.account_name)
WHEN NOT MATCHED THEN INSERT (
    spot_id, broadcast_week, spot_datetime, creative_name, creative_code, network, rotation, spend, conversion_metric, lift, account_name, filename_date, end_date, is_mature, meta_row_hash, meta_create_datetime, meta_update_datetime
)
VALUES (
    spot_id, broadcast_week, spot_datetime, creative_name, creative_code, network, rotation, spend, conversion_metric, lift, account_name, filename_date, end_date, is_mature, meta_row_hash, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.is_mature = 0 and ((s.is_mature = 1) or (t.meta_row_hash != s.meta_row_hash
    AND coalesce(s.filename_date, '1900-01-01') > coalesce(t.filename_date, '1900-01-01'))) THEN UPDATE
SET t.spot_id = s.spot_id,
    t.broadcast_week = s.broadcast_week,
    t.spot_datetime = s.spot_datetime,
    t.creative_name = s.creative_name,
    t.creative_code = s.creative_code,
    t.network = s.network,
    t.rotation = s.rotation,
    t.spend = s.spend,
    t.conversion_metric = s.conversion_metric,
    t.lift = s.lift,
    t.account_name = s.account_name,
    t.filename_date = s.filename_date,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime,
    t.end_date = s.end_date,
    t.is_mature = s.is_mature;

CREATE OR REPLACE TEMPORARY TABLE _mature_combos AS
SELECT DISTINCT account_name, filename_date, end_date
    FROM lake.tatari.linear_conversions
    WHERE is_mature = 1;

DELETE
    FROM lake.tatari.linear_conversions t USING _mature_combos mc
    WHERE mc.account_name = t.account_name
      AND t.spot_datetime >= mc.filename_date
      AND t.spot_datetime < mc.end_date
      AND t.is_mature = 0;

DELETE FROM lake.tatari.linear_conversions_stg;
