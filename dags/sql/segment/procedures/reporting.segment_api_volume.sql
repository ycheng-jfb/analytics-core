--Segment API Volume - for all brands all environments

SET report_date = dateadd(day, -2, %(low_watermark)s) :: TIMESTAMP_NTZ::VARCHAR;

TRUNCATE TABLE SEGMENT.REPORTING.SEGMENT_API_VOLUME_STG;

CALL SEGMENT.REPORTING.SEGMENT_METADATA_COUNTS('SEGMENT_GFB', $report_date, '');
CALL SEGMENT.REPORTING.SEGMENT_METADATA_COUNTS('SEGMENT_FL', $report_date, '');
CALL SEGMENT.REPORTING.SEGMENT_METADATA_COUNTS('SEGMENT_SXF', $report_date, '');


INSERT INTO SEGMENT.REPORTING.SEGMENT_API_VOLUME_STG (REPORT_DATE, SOURCE, STORE_BRAND, STORE_COUNTRY, ENVIRONMENT, API_CALL, VOLUME)
select report_date,source, store_brand, store_country, environment, API_call, sum(volume) as volume
from (

--environment - Dev_QA

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_us_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_us_dev_qa.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_us_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_us_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Java' as Source, 'Fabletics' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.java_fabletics_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by  1, PG.EVENT


union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_ca_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_ca_dev_qa.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_ca_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_ca_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'DE' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_de_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'DE' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_de_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'DE' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_de_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'DK' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_dk_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1




Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'DK' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_dk_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'DK' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_dk_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'ES' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_es_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1




Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'ES' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_es_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Fabletics' as store_brand,
'ES' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_es_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


 union all

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'FR' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_fr_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1



Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'FR' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_fr_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'FR' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_fr_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'NL' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_nl_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'NL' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_nl_dev_qa.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'NL' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_nl_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'NL' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_nl_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

 union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'SE' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_se_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1



Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'SE' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_se_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'SE' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_se_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_uk_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_uk_dev_qa.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_uk_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_uk_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

--env - Staging

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'US' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_us_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'US' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_us_staging.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'US' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_us_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'US' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_us_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT



union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'CA' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_ca_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'CA' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_ca_staging.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'CA' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_ca_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'CA' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_ca_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'DE' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_de_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1





Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'DE' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_de_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Fabletics' as store_brand,
'DE' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_de_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'DK' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_dk_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1




Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'DK' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_dk_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Fabletics' as store_brand,
'DK' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_dk_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'ES' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_es_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1




Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'ES' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_es_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Fabletics' as store_brand,
'ES' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_es_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


 union all

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'FR' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_fr_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'FR' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_fr_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Fabletics' as store_brand,
'FR' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_fr_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'NL' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_nl_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'NL' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_nl_staging.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'NL' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_nl_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Fabletics' as store_brand,
'NL' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_nl_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

 union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'SE' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_se_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'SE' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_se_staging.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'SE' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_se_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Fabletics' as store_brand,
'SE' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_se_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'UK' as  store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_uk_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Fabletics' as store_brand,
'UK' as  store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_uk_staging.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Fabletics' as store_brand,
'UK' as  store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_uk_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Fabletics' as store_brand,
'UK' as  store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_fabletics_uk_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all


select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'React native' as Source, 'Fabletics' as store_brand,
'US' as store_country, 'Staging'as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.react_native_fabletics_fitness_app_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by  1, PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'React native' as Source, 'Fabletics' as store_brand,
'US' as store_country, 'Staging'as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.react_native_fabletics_fitness_app_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by  1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'React native' as Source, 'Fabletics' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.react_native_fabletics_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by  1, PG.EVENT


union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'React native' as Source, 'Fabletics' as store_brand,
'US' as store_country, 'Dev_qa'as environment,
'identifies' as API_call, count(PG.ID) as volume
from  segment.react_native_fabletics_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by  1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'React native' as Source, 'JustFab' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from  segment.react_native_justfab_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by  1, PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'React native' as Source, 'JustFab' as store_brand,
'US' as store_country, 'Dev_qa'as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.react_native_justfab_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by  1



union all
--environment - Dev_QA



select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_ca_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_ca_dev_qa.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_ca_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_ca_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'DE' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_de_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'DE' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_de_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'DE' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_de_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'DK' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_dk_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1




Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'DK' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_dk_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'DK' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_dk_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'ES' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_es_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1




Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'ES' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_es_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'ES' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_es_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


 union all

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'FR' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_fr_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1



Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'FR' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_fr_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'FR' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_fr_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'NL' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_nl_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'NL' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_nl_dev_qa.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'NL' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_nl_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'NL' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_nl_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

 union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'SE' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_se_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1



Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'SE' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_se_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'SE' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_se_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_uk_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_uk_dev_qa.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_uk_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_uk_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

--env - Staging



select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'CA' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_ca_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'CA' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_ca_staging.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'CA' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_ca_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'CA' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_ca_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'DE' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_de_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1





Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'DE' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_de_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'DE' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_de_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'DK' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_dk_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1




Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'DK' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_dk_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'DK' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_dk_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'ES' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_es_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1




Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'ES' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_es_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'ES' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_es_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


 union all

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'FR' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_fr_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'FR' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_fr_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'FR' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_fr_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'NL' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_nl_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'NL' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_nl_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'NL' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_nl_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

 union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'SE' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_se_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'JustFab' as store_brand,
'SE' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_se_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'SE' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_se_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT



union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'JustFab' as store_brand,
'UK' as  store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_justfab_uk_staging.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'JustFab' as store_brand,
'UK' as  store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_justfab_uk_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


--Savage X

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_us_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1



Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_us_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_us_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT



union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_ca_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_ca_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'CA' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_ca_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'DE' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_de_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'DE' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_de_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'DE' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_de_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'ES' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_es_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'ES' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_es_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'ES' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_es_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


union all

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'FR' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_fr_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'FR' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_fr_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'FR' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_fr_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_uk_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1




Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_uk_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'UK' as  store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_uk_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

--env - Staging

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'US' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_us_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'US' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_us_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'US' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_us_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT



union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'CA' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_ca_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1



Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'CA' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_ca_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'CA' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_ca_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'DE' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_de_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1





Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'DE' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_de_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'DE' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_de_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'ES' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_es_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'ES' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_es_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'ES' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_es_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT


 union all

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'FR' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_fr_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'FR' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_fr_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'FR' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_fr_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Savage X' as store_brand,
'UK' as  store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_uk_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1



Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Savage X' as store_brand,
'UK' as  store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_sxf_uk_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Savage X' as store_brand,
'UK' as  store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_sxf_uk_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all


select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Java' as Source, 'Savage X' as store_brand,
'US' as store_country, 'Dev_qa'as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.java_sxf_us_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by  1, PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Java' as Source, 'Savage X' as store_brand,
'UK' as store_country, 'Dev_qa'as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.java_sxf_uk_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by  1, PG.EVENT

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Java' as Source, 'Savage X' as store_brand,
'FR' as store_country, 'Dev_qa'as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.java_sxf_fr_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by  1, PG.EVENT

  --ShoeDazzle

Union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_dev_qa.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_dev_qa.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_dev_qa.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Dev_qa' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT



union all

--ShoeDazzle Staging

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_staging.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

  select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Staging' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_staging.hello PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Staging' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_staging.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Staging' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_staging.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

 --Shoedazzle env development

 select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Development' as environment,
'page' as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_development.pages PG
where PG.RECEIVED_AT >= $report_date
Group by 1


Union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Javascript' as Source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Development' as environment,
'identifies' as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_development.identifies PG
where PG.RECEIVED_AT >= $report_date
Group by 1

union all

select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date,'Javascript' as source, 'Shoedazzle' as store_brand,
'US' as store_country, 'Development' as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.javascript_shoedazzle_development.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by 1,PG.EVENT

union all

 --Fabkids store_brand SQL Statements from Segment database


select convert_timezone('UTC','America/Los_Angeles',PG.RECEIVED_AT)::date as report_date, 'Java' as Source, 'Fabkids' as store_brand,
'US' as store_country, 'Dev_qa'as environment,
PG.EVENT as API_call, count(PG.ID) as volume
from segment.java_fabkids_dev_qa.tracks PG
where PG.RECEIVED_AT >= $report_date
Group by  1, PG.EVENT
)
Group by
report_date,
source,
store_brand,
store_country,
environment,
API_call;


-- removing current day's records as they are incomplete
DELETE from SEGMENT.REPORTING.SEGMENT_API_VOLUME_STG
where report_date = current_date;

MERGE INTO SEGMENT.REPORTING.SEGMENT_API_VOLUME sav
USING(
         SELECT *,
                hash(*) AS meta_row_hash,
                current_timestamp()::TIMESTAMP_LTZ AS meta_create_datetime,
                current_timestamp()::TIMESTAMP_LTZ AS meta_update_datetime
        FROM SEGMENT.REPORTING.SEGMENT_API_VOLUME_STG
    ) s ON equal_null(sav.report_date, s.report_date)
        AND equal_null(sav.source, s.source)
        AND equal_null(sav.store_brand, s.store_brand)
        AND equal_null(sav.store_country, s.store_country)
        AND equal_null(sav.environment, s.environment)
        AND equal_null(sav.API_call, s.API_call)
WHEN NOT MATCHED
    THEN INSERT (report_date, source, store_brand, store_country, environment, API_call, volume, meta_row_hash, meta_create_datetime, meta_update_datetime )
        VALUES  (report_date, source, store_brand, store_country, environment, API_call, volume, meta_row_hash, meta_create_datetime, meta_update_datetime )
WHEN MATCHED AND sav.meta_row_hash != s.meta_row_hash
    THEN UPDATE
    SET sav.report_date = s.report_date,
        sav.source = s.source,
        sav.store_brand = s.store_brand,
        sav.store_country = s.store_country,
        sav.environment = s.environment,
        sav.API_call = s.API_call,
        sav.volume = s.volume,
        sav.META_ROW_HASH = s.META_ROW_HASH,
        sav.meta_update_datetime = CURRENT_TIMESTAMP;
