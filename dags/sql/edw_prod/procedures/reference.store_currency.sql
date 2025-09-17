SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _country_currency
(
    country        VARCHAR(100),
    country_abbr   VARCHAR(6),
    currency_code  VARCHAR(3),
    effective_start_datetime TIMESTAMP_LTZ,
    effective_end_datetime TIMESTAMP_LTZ
);

--Populate temp table with existing country details
INSERT INTO _country_currency
VALUES
    ('US', 'US', 'USD', '1900-01-01', '9999-12-31'),
    ('Germany', 'DE', 'EUR', '1900-01-01', '9999-12-31'),
    ('UK', 'UK', 'GBP', '1900-01-01', '9999-12-31'),
    ('Canada', 'CA', 'CAD', '1900-01-01', '2014-12-31 23:59:59.999'),
    ('Canada', 'CA', 'USD', '2015-01-01', '9999-12-31'),
    ('France', 'FR', 'EUR', '1900-01-01', '9999-12-31'),
    ('Spain', 'ES', 'EUR', '1900-01-01', '9999-12-31'),
    ('Italy', 'IT', 'EUR', '1900-01-01', '9999-12-31'),
    ('Netherlands', 'NL', 'EUR', '1900-01-01', '9999-12-31'),
    ('Denmark', 'DK', 'DKK', '1900-01-01', '9999-12-31'),
    ('Sweden', 'SE', 'SEK', '1900-01-01', '9999-12-31'),
    ('EU', 'EU', 'EUR', '1900-01-01', '9999-12-31');

CREATE OR REPLACE TEMP TABLE _store_country AS
SELECT
    s.store_id,
    CASE
        WHEN sg.label IN
            ('Just Fabulous', 'JustFabulous', 'JustFab', 'FabKids', 'Fabletics',
             'ShoeDazzle', 'Savage X') THEN 'US'
        WHEN RIGHT(sg.label, 3) LIKE ' %' THEN RIGHT(sg.label, 2)
        ELSE 'US'
    END AS country_abbr,
    s.datetime_added::TIMESTAMP_LTZ AS datetime_added
FROM lake_consolidated.ultra_merchant.store s
JOIN lake_consolidated.ultra_merchant.store_group sg
    ON sg.store_group_id = s.store_group_id;

INSERT INTO _store_country (store_id, country_abbr, datetime_added)
SELECT
    24101 as store_id,
    country_abbr,
    datetime_added
FROM _store_country
WHERE store_id = 241;

CREATE OR REPLACE TEMP TABLE _stg_store_currency AS
SELECT
    scm.store_id,
    scm.country_abbr AS country,
    cd.currency_code,
    hash(scm.store_id, scm.country_abbr, cd.currency_code) AS meta_row_hash,
    nvl(cd.effective_start_datetime, '1900-01-01')::TIMESTAMP_LTZ AS effective_start_date,
    nvl(cd.effective_end_datetime, '9999-12-31')::TIMESTAMP_LTZ AS effective_end_date,
    row_number() over(partition by scm.store_id ORDER BY nvl(cd.effective_start_datetime, '1900-01-01')::TIMESTAMP_LTZ ) AS rno
FROM _store_country scm
LEFT JOIN _country_currency cd
    ON scm.country_abbr = cd.country_abbr
LEFT JOIN reference.store_currency sc
    ON sc.store_id = scm.store_id
    AND sc.is_current
WHERE scm.datetime_added <= nvl(cd.effective_end_datetime, '9999-12-31');

BEGIN;

TRUNCATE TABLE reference.store_currency;

INSERT INTO reference.store_currency
(
     store_id,
     country,
     currency_code,
     effective_start_date,
     effective_end_date,
     is_current,
     meta_row_hash,
     meta_create_datetime,
     meta_update_datetime
)
SELECT
    store_id,
    country,
    currency_code,
    IFF(rno = 1, '1900-01-01', effective_start_date) AS effective_start_date,
    effective_end_date,
    IFF(effective_end_date = '9999-12-31', TRUE, FALSE) AS is_current,
    meta_row_hash,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _stg_store_currency
ORDER BY store_id;

COMMIT;
