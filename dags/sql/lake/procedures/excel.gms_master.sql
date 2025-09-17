BEGIN;

DELETE FROM lake_stg.excel.gms_master_stg;

COPY INTO lake_stg.excel.gms_master_stg (agent_id, ecom_id,genesys_id,agent_name,team_name,team_id,attrition_date,sheet_name)
FROM '@lake_stg.public.tsos_da_int_inbound/lake/excel.gms_master/v3/'
FILE_FORMAT=(
    TYPE = CSV,
    FIELD_DELIMITER = '|',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    ESCAPE_UNENCLOSED_FIELD = NONE,
    NULL_IF = ('')
)
ON_ERROR = 'SKIP_FILE_1%'
;

CREATE or REPLACE TEMP TABLE _gms_master_stg AS
(
select * from lake_stg.excel.gms_master_stg where sheet_name in ('DVO', 'DVO NH',  'MIA','MIA NH', 'BCD', 'BCD NH',  'UK','UK NH','EU',
'EU NH','Casablanca', 'Casablanca NH','Tier 2', 'GMS Training', 'GMS MEA','GMS EU Training & Quality','MIA GMS Training','FL Retail',
 'SMM_MSG','BackOffice','MEX', 'MEX NH')
union all
select * from lake_stg.excel.gms_master_stg where sheet_name in ('DVO ResignedTerminated', 'MIA ResignedTerminated', 'BCD ResignedTerminated',
'UK ResignedTerminated','EU ResignedTerminated','Casablanca ResignedTerminated','Tier ResignedTerminated', 'GMS Training ResignedTerminated'
,'GMS MEA ResignedTerminated','GMS EU Training&Quality Resign.','FL Retail ResignedTerminated','SMM_MSG ResignedTerminate',
'BackOffice ResignedTerminate','MEX ResignedTerminate')
and month(attrition_date)=month(current_timestamp)
);

set stg_row_count = (select count(*) from _gms_master_stg);
DELETE FROM lake.excel.gms_master WHERE $stg_row_count > 0;

INSERT INTO lake.excel.gms_master
(
    agent_id,ecom_id,genesys_id,agent_name,team_name,team_id,attrition_date,sheet_name, meta_row_hash, meta_create_datetime, meta_update_datetime
)

SELECT
    agent_id,ecom_id,genesys_id,agent_name,team_name,team_id,attrition_date,sheet_name, meta_row_hash, meta_create_datetime, meta_update_datetime
FROM
    (
        SELECT
            a.*
        FROM (
            SELECT
                *,
                hash(*) AS meta_row_hash,
                current_timestamp AS meta_create_datetime,
                current_timestamp AS meta_update_datetime,
                row_number() OVER ( PARTITION BY meta_row_hash ORDER BY NULL) AS rn
            FROM _gms_master_stg
         ) a
        WHERE a.rn = 1
) s
WHERE $stg_row_count > 0;

COMMIT;
