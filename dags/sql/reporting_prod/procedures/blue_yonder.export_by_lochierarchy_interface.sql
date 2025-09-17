use reporting_prod;
create or replace temp table _final (
MEMBERID varchar,
MEMBERNAME varchar,
LEVELID varchar,
PARENTACTIONCODE number(5,0),
PARENTMEMBERID varchar,
PARENTLEVELID varchar
);

insert into _final
(
MEMBERID,
MEMBERNAME,
LEVELID,
PARENTACTIONCODE,
PARENTMEMBERID,
PARENTLEVELID
)
select distinct
CHANNEL,
CHANNEL_NAME,
'CHANNEL',
NULL,
REGION,
'REGION'
from reporting_prod.blue_yonder.export_by_loc_interface_stg;

insert into _final
(
MEMBERID,
MEMBERNAME,
LEVELID,
PARENTACTIONCODE,
PARENTMEMBERID,
PARENTLEVELID
)
select distinct
REGION,
REGION_NAME,
'REGION',
NULL,
COMPANY,
'TOTAL'
from reporting_prod.blue_yonder.export_by_loc_interface_stg;

insert into _final
(
MEMBERID,
MEMBERNAME,
LEVELID,
PARENTACTIONCODE,
PARENTMEMBERID,
PARENTLEVELID
)
select distinct
COMPANY,
COMPANY,
'TOTAL',
NULL,
TOTAL_COMPANY,
'TOTORG'
from reporting_prod.blue_yonder.export_by_loc_interface_stg;

insert into _final
(
MEMBERID,
MEMBERNAME,
LEVELID,
PARENTACTIONCODE,
PARENTMEMBERID,
PARENTLEVELID
)
select distinct
TOTAL_COMPANY,
TOTAL_COMPANY,
'TOTORG',
NULL,
NULL,
NULL
from reporting_prod.blue_yonder.export_by_loc_interface_stg;

DELETE FROM _final WHERE memberid is null;

MERGE INTO reporting_prod.blue_yonder.export_by_lochierarchy_interface_stg t
USING _final s
ON t.MEMBERID = s.MEMBERID
    AND t.LEVELID = s.LEVELID
WHEN MATCHED
    AND t.PARENTMEMBERID <> s.PARENTMEMBERID
THEN
    UPDATE
        SET t.PARENTMEMBERID = s.PARENTMEMBERID,
            t.PARENTLEVELID = s.PARENTLEVELID,
            t.PARENTACTIONCODE = 2,
            t.DATE_ADDED = current_timestamp()
WHEN MATCHED
    AND t.PARENTMEMBERID = s.PARENTMEMBERID
    AND t.PARENTACTIONCODE = 2
THEN
    UPDATE
        SET
            t.PARENTACTIONCODE = NULL,
            t.DATE_ADDED = current_timestamp()
WHEN NOT MATCHED
THEN INSERT
(
MEMBERID,
MEMBERNAME,
LEVELID,
PARENTACTIONCODE,
PARENTMEMBERID,
PARENTLEVELID
)
VALUES
(
MEMBERID,
MEMBERNAME,
LEVELID,
PARENTACTIONCODE,
PARENTMEMBERID,
PARENTLEVELID
);

DELETE FROM reporting_prod.blue_yonder.export_by_lochierarchy_interface_stg
WHERE (memberid,membername,levelid) NOT IN (SELECT memberid,membername,levelid FROM _final);
