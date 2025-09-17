SET low_watermark_ltz = %(low_watermark)s :: TIMESTAMP_LTZ;

CREATE OR REPLACE TEMP TABLE _session_base AS
SELECT DISTINCT session_id, visitor_id
FROM lake_consolidated_view.ultra_merchant.visitor_session
WHERE meta_update_datetime > $low_watermark_ltz
ORDER BY session_id;

CREATE OR REPLACE TEMP TABLE _visitor_hist AS
SELECT DISTINCT
    s.session_id,
    s.visitor_id
FROM staging.visitor_session s
where s.visitor_id IN (SELECT visitor_id FROM _session_base WHERE visitor_id IS NOT NULL);

CREATE OR REPLACE TEMP TABLE _visitor_session_base AS
SELECT DISTINCT
    session_id,
    visitor_id
FROM (
    SELECT
        vb.session_id,
        vb.visitor_id
    FROM _session_base vb
    UNION ALL
    SELECT
        vh.session_id,
        vh.visitor_id
    FROM _visitor_hist vh
  ) AS A;

CREATE OR REPLACE TEMP TABLE _visitor_session AS
SELECT
    session_id,
    visitor_id,
    previous_visitor_session_id,
    previous_visitor_session_datetime,
    next_visitor_session_id,
    next_visitor_session_datetime,
    HASH(session_id, visitor_id,
        previous_visitor_session_id, previous_visitor_session_datetime,
        next_visitor_session_id, next_visitor_session_datetime) as meta_row_hash
FROM (
    SELECT
        session_id,
        visitor_id,
        previous_visitor_session_id,
        previous_visitor_session_datetime,
        next_visitor_session_id,
        next_visitor_session_datetime,
        row_number() over(partition by session_id order by datetime_modified DESC) as rn
    FROM (
        SELECT
            s.session_id,
            vs.visitor_id,
            IFF(vs.visitor_id IS NOT NULL, LAG(vs.session_id, 1) OVER (PARTITION BY vs.visitor_id, ds.store_brand_abbr ORDER BY ums.datetime_added ASC, ums.session_id ASC), NULL) as previous_visitor_session_id,
            IFF(vs.visitor_id IS NOT NULL, LAG(ums.datetime_added, 1) OVER (PARTITION BY vs.visitor_id, ds.store_brand_abbr ORDER BY ums.datetime_added ASC, ums.session_id ASC), NULL) as previous_visitor_session_datetime,
            IFF(vs.visitor_id IS NOT NULL, LEAD(vs.session_id, 1) OVER (PARTITION BY vs.visitor_id, ds.store_brand_abbr ORDER BY ums.datetime_added ASC, ums.session_id ASC), NULL) as next_visitor_session_id,
            IFF(vs.visitor_id IS NOT NULL, LEAD(ums.datetime_added, 1) OVER (PARTITION BY vs.visitor_id, ds.store_brand_abbr ORDER BY ums.datetime_added ASC, ums.session_id ASC), NULL) as next_visitor_session_datetime,
            vs.datetime_modified
        FROM _visitor_session_base AS s
        JOIN lake_consolidated_view.ultra_merchant.visitor_session vs
            ON s.session_id = vs.session_id
        JOIN lake_consolidated_view.ultra_merchant.session ums
            ON ums.session_id = vs.session_id
        JOIN edw_prod.data_model.dim_store ds
            ON ums.store_id = ds.store_id
        ) AS A
    ) AS A
WHERE rn = 1
ORDER BY session_id;

MERGE INTO staging.visitor_session tgt
USING (SELECT *, edw_prod.stg.udf_unconcat_brand(session_id) AS meta_original_session_id
       FROM _visitor_session
       ) AS src
    ON src.session_id = tgt.session_id
    WHEN NOT MATCHED THEN
        INSERT (session_id, visitor_id,
            previous_visitor_session_id, previous_visitor_session_datetime,
            next_visitor_session_id, next_visitor_session_datetime, meta_row_hash, meta_original_session_id,
            meta_create_datetime, meta_update_datetime)
        VALUES
        (
            src.session_id, src.visitor_id,
            src.previous_visitor_session_id, src.previous_visitor_session_datetime,
            src.next_visitor_session_id, src.next_visitor_session_datetime, src.meta_row_hash, src.meta_original_session_id,
            current_timestamp::TIMESTAMP_LTZ, current_timestamp::timestamp_ltz
         )
    WHEN MATCHED AND src.meta_row_hash != tgt.meta_row_hash
        THEN UPDATE SET
        tgt.visitor_id = src.visitor_id,
        tgt.previous_visitor_session_id = src.previous_visitor_session_id,
        tgt.previous_visitor_session_datetime = src.previous_visitor_session_datetime,
        tgt.next_visitor_session_id = src.next_visitor_session_id,
        tgt.next_visitor_session_datetime = src.next_visitor_session_datetime,
        tgt.meta_row_hash = src.meta_row_hash,
        tgt.meta_original_session_id = src.meta_original_session_id,
        tgt.meta_update_datetime = current_timestamp;
