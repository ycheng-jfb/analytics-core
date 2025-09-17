MERGE INTO reference.store_timezone t
    USING (
        SELECT
            store_id,
            country_abbr,
            country_name,
            time_zone,
            meta_row_hash
        FROM (
            SELECT
                a.store_id,
                a.country_abbr,
                CASE WHEN a.country_abbr = 'EU' THEN 'Europe'
                    WHEN a.country_abbr = 'DD' THEN 'German Democratic Republic'
                    WHEN a.country_abbr = 'UK' THEN 'United Kingdom'
                    ELSE ct.name
                END AS country_name,
                CASE
                    WHEN a.country_abbr = 'UK' THEN 'Europe/London'
                    WHEN a.country_abbr = 'DD' THEN 'Europe/Berlin'
                    WHEN a.country_abbr = 'EU' THEN 'Europe/Berlin'
                    ELSE ct.default_timezone
                END AS time_zone,
                hash(a.country_abbr, country_name, ct.default_timezone) AS meta_row_hash,
                row_number() OVER (PARTITION BY a.store_id ORDER BY (SELECT NULL)) AS rn
            FROM (
                SELECT
                    s.store_id,
                    CASE
                        WHEN RIGHT(sg.label, 3) LIKE ' %' THEN RIGHT(sg.label, 2)
                        WHEN sg.label IN
                            ('Just Fabulous', 'JustFabulous', 'JustFab', 'FabKids', 'Fabletics',
                             'ShoeDazzle') THEN 'US'
                        ELSE 'US'
                    END AS country_abbr
                FROM lake_consolidated.ultra_merchant.store s
                JOIN lake_consolidated.ultra_merchant.store_group sg ON sg.store_group_id = s.store_group_id
            ) a
            LEFT JOIN reference.country_timezone ct ON ct.alpha2_code = a.country_abbr
        ) a
        WHERE
            a.rn = 1
    ) s ON s.store_id = t.store_id
    WHEN NOT MATCHED THEN INSERT (store_id, country_abbr, country_name, time_zone, meta_row_hash)
        VALUES
        (
            store_id, country_abbr, country_name, time_zone, meta_row_hash)
    WHEN MATCHED AND s.meta_row_hash != t.meta_row_hash
        THEN UPDATE
        SET country_abbr = s.country_abbr,
            country_name = s.country_name,
            time_zone = s.time_zone,
            meta_row_hash = s.meta_row_hash,
            meta_update_datetime = current_timestamp
;
-- todo: Below code snipped will only run once.
--  I have added it to the script to represent the manual insertion we did for the dummy YITTY mobile store id(24101).
--  Once we have a real YITTY mobile store, we should remove the snippet and the record from the reference.store_timezone

INSERT INTO reference.store_timezone
    (store_id,
     country_abbr,
     time_zone,
     country_name,
     meta_row_hash)
SELECT 24101,
       country_abbr,
       time_zone,
       country_name,
       HASH(country_abbr, country_name, time_zone) AS meta_row_hash
FROM reference.store_timezone
WHERE store_id = 151 and  1 <> (SELECT COUNT(*) FROM reference.store_timezone WHERE store_id = 24101);
