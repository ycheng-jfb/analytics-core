set changedate = (select dateadd('day', -1, NVL(max(Change_Date), '2019-01-01')) from reporting_prod.gsc.po_audit_trail_DATASET);

create or replace temporary table _tbl(
    table_name varchar(25),
    table_type varchar(25)
);

insert into _tbl
    (table_name, table_type)
values
    ('poHdr','Main'),
    ('podtl','Details'),
    ('pohts','HTS'),
    ('pomisccost','Miscellaneous'),
    ('pomilestone','Milestone'),
    ('pocolordtl','VendorInfo');

CREATE OR REPLACE TEMPORARY TABLE _CHANGE_DATA
AS
SELECT DISTINCT
    Doc_id,
    Log_Hdr_Id,
    Table_name,
    Change_Reason,
    User_Name,
    Owner,
    Old_Value,
    New_Value,
    Date_Create,
    Column_name,
    th_table
FROM (
         SELECT h.Doc_Id,
                h.Change_Log_Hdr_Id as Log_Hdr_Id,
                t.table_type        as Table_name,
                Crc.Reason          as Change_Reason,
                h.User_Name         as User_Name,
                pd.sku              as Owner,
                td.Old_Value        as Old_Value,
                td.New_Value        as New_Value,
                h.Date_Create,
                td.Column_Name      as Column_name,
                th.table_name       as th_table
         FROM lake_view.JF_Portal.Change_Log_Hdr2 h
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Hdr th
                       ON h.Change_Log_Hdr_Id = th.Change_Log_Hdr_Id
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Dtl td
                       ON th.Change_Log_Tbl_Hdr_Id = td.Change_Log_Tbl_Hdr_Id
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON CR
                            ON H.DOC_ID = CR.PO_ID
                                AND H.DOC_VERSION = CR.VERSION
                                and to_varchar(cr.date_create::timestamp, 'mm/dd/yyyy, hh24:mi') =
                                    to_varchar(h.date_create::timestamp, 'mm/dd/yyyy, hh24:mi')
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON_CODE CRC
                            ON CR.PO_CHANGE_REASON_CODE_ID = CRC.PO_CHANGE_REASON_CODE_ID
                  JOIN lake_view.JF_Portal.Po_Dtl pd
                       ON UPPER(th.Table_Name) = 'PODTL' --'PoDtl'
                           AND to_varchar(pd.Po_Dtl_Id) = td.Tbl_Id
                  left join _tbl t
                            on UPPER(th.table_name) = UPPER(t.table_name)
         WHERE Doc_Type_Id = 2
           and h.date_create > $changedate

         UNION ALL

         SELECT h.Doc_Id,
                h.Change_Log_Hdr_Id as Log_Hdr_Id,
                t.table_type        as Table_name,
                Crc.Reason          as Change_Reason,
                h.User_Name         as User_Name,
                NULL                as Owner
             ,
                PSO.DESCRIPTION     AS OLD_VALUE --td.old_value
             ,
                PSN.DESCRIPTION     AS NEW_VALUE --td.new_value
             ,
                h.Date_Create
             ,
                td.Column_Name      as Column_name,
                th.table_name       as th_table
         FROM lake_view.JF_Portal.Change_Log_Hdr2 h
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Hdr th
                       ON h.Change_Log_Hdr_Id = th.Change_Log_Hdr_Id
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Dtl td
                       ON th.Change_Log_Tbl_Hdr_Id = td.Change_Log_Tbl_Hdr_Id
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON CR
                            ON H.DOC_ID = CR.PO_ID
                                AND H.DOC_VERSION = CR.VERSION
                                and to_varchar(cr.date_create::timestamp, 'mm/dd/yyyy, hh24:mi') =
                                    to_varchar(h.date_create::timestamp, 'mm/dd/yyyy, hh24:mi')
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON_CODE CRC
                            ON CR.PO_CHANGE_REASON_CODE_ID = CRC.PO_CHANGE_REASON_CODE_ID
                  JOIN lake_view.JF_Portal.Po_Hdr ph
                       ON upper(th.Table_Name) = 'POHDR'--'PoHdr'
                           AND to_VARCHAR(ph.Po_Id) = td.Tbl_Id
                  left join _tbl t
                            on upper(th.table_name) = upper(t.table_name)
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_STATUS PSO
                            ON TD.OLD_VALUE = PSO.PO_STATUS_ID
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_STATUS PSN
                            ON TD.NEW_VALUE = PSN.PO_STATUS_ID
         WHERE Doc_Type_Id = 2
           AND UPPER(TD.COLUMN_NAME) = 'POSTATUSID'
           and h.date_create > $changedate

         UNION ALL

         SELECT h.Doc_Id,
                h.Change_Log_Hdr_Id as Log_Hdr_Id,
                t.table_type        as Table_name,
                Crc.Reason          as Change_Reason,
                h.User_Name         as User_Name,
                NULL                as Owner
             ,
                FMO.NAME            AS OLD_VALUE --td.old_value
             ,
                FMN.NAME            AS NEW_VALUE --td.new_value
             ,
                h.Date_Create
             ,
                td.Column_Name      as Column_name,
                th.table_name       as th_table
         FROM lake_view.JF_Portal.Change_Log_Hdr2 h
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Hdr th
                       ON h.Change_Log_Hdr_Id = th.Change_Log_Hdr_Id
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Dtl td
                       ON th.Change_Log_Tbl_Hdr_Id = td.Change_Log_Tbl_Hdr_Id
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON CR
                            ON H.DOC_ID = CR.PO_ID
                                AND H.DOC_VERSION = CR.VERSION
                                and to_varchar(cr.date_create::timestamp, 'mm/dd/yyyy, hh24:mi') =
                                    to_varchar(h.date_create::timestamp, 'mm/dd/yyyy, hh24:mi')
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON_CODE CRC
                            ON CR.PO_CHANGE_REASON_CODE_ID = CRC.PO_CHANGE_REASON_CODE_ID
                  JOIN lake_view.JF_Portal.Po_Hdr ph
                       ON upper(th.Table_Name) = 'POHDR'--'PoHdr'
                           AND to_VARCHAR(ph.Po_Id) = td.Tbl_Id
                  left join _tbl t
                            on upper(th.table_name) = upper(t.table_name)
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.Freight_Method FMO
                            ON TD.OLD_VALUE = FMO.FREIGHT_METHOD_ID
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.Freight_Method FMN
                            ON TD.NEW_VALUE = FMN.FREIGHT_METHOD_ID
         WHERE Doc_Type_Id = 2
           AND UPPER(TD.COLUMN_NAME) = 'FREIGHTMETHODID' --'FreightMethodId'
           and h.date_create > $changedate

         UNION ALL

         SELECT h.Doc_Id,
                h.Change_Log_Hdr_Id as Log_Hdr_Id,
                t.table_type        as Table_name,
                Crc.Reason          as Change_Reason,
                h.User_Name         as User_Name,
                NULL                as Owner
             ,
                ITO.NAME            AS OLD_VALUE --td.old_value
             ,
                ITN.NAME            AS NEW_VALUE --td.new_value
             ,
                h.Date_Create
             ,
                td.Column_Name      as Column_name,
                th.table_name       as th_table
         FROM lake_view.JF_Portal.Change_Log_Hdr2 h
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Hdr th
                       ON h.Change_Log_Hdr_Id = th.Change_Log_Hdr_Id
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Dtl td
                       ON th.Change_Log_Tbl_Hdr_Id = td.Change_Log_Tbl_Hdr_Id
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON CR
                            ON H.DOC_ID = CR.PO_ID
                                AND H.DOC_VERSION = CR.VERSION
                                and to_varchar(cr.date_create::timestamp, 'mm/dd/yyyy, hh24:mi') =
                                    to_varchar(h.date_create::timestamp, 'mm/dd/yyyy, hh24:mi')
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON_CODE CRC
                            ON CR.PO_CHANGE_REASON_CODE_ID = CRC.PO_CHANGE_REASON_CODE_ID
                  JOIN lake_view.JF_Portal.Po_Hdr ph
                       ON upper(th.Table_Name) = 'POHDR'--'PoHdr'
                           AND to_VARCHAR(ph.Po_Id) = td.Tbl_Id
                  left join _tbl t
                            on upper(th.table_name) = upper(t.table_name)
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.Inco_Term ITO
                            ON TD.OLD_VALUE = ITO.INCO_TERM_ID
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.Inco_Term ITN
                            ON TD.NEW_VALUE = ITN.INCO_TERM_ID
         WHERE Doc_Type_Id = 2
           AND UPPER(TD.COLUMN_NAME) = 'INCOTERMID' --'IncoTermId'
           and h.date_create > $changedate

         UNION ALL

         SELECT h.Doc_Id,
                h.Change_Log_Hdr_Id                           as Log_Hdr_Id,
                t.table_type                                  as Table_name,
                Crc.Reason                                    as Change_Reason,
                h.User_Name                                   as User_Name,
                NULL                                          as Owner,
                SPO.City_Origin || ', ' || SPO.Country_Origin AS OLD_VALUE, --td.old_value
                SPN.City_Origin || ', ' || SPN.Country_Origin AS NEW_VALUE, --td.new_value
                h.Date_Create,
                td.Column_Name                                as Column_name,
                th.table_name                                 as th_table
         FROM lake_view.JF_Portal.Change_Log_Hdr2 h
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Hdr th
                       ON h.Change_Log_Hdr_Id = th.Change_Log_Hdr_Id
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Dtl td
                       ON th.Change_Log_Tbl_Hdr_Id = td.Change_Log_Tbl_Hdr_Id
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON CR
                            ON H.DOC_ID = CR.PO_ID
                                AND H.DOC_VERSION = CR.VERSION
                                and to_varchar(cr.date_create::timestamp, 'mm/dd/yyyy, hh24:mi') =
                                    to_varchar(h.date_create::timestamp, 'mm/dd/yyyy, hh24:mi')
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON_CODE CRC
                            ON CR.PO_CHANGE_REASON_CODE_ID = CRC.PO_CHANGE_REASON_CODE_ID
                  JOIN lake_view.JF_Portal.Po_Hdr ph
                       ON upper(th.Table_Name) = 'POHDR'--'PoHdr'
                           AND to_VARCHAR(ph.Po_Id) = td.Tbl_Id
                  left join _tbl t
                            on upper(th.table_name) = upper(t.table_name)
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.SHIPPING_PORT SPO
                            ON TD.OLD_VALUE = SPO.SHIPPING_PORT_ID
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.SHIPPING_PORT SPN
                            ON TD.NEW_VALUE = SPN.SHIPPING_PORT_ID
         WHERE Doc_Type_Id = 2
           AND UPPER(TD.COLUMN_NAME) = 'SHIPPINGPORTID'
           and h.date_create > $changedate

         UNION ALL

         SELECT h.Doc_Id,
                h.Change_Log_Hdr_Id as Log_Hdr_Id,
                t.table_type        as Table_name,
                Crc.Reason          as Change_Reason,
                h.User_Name         as User_Name,
                NULL                as Owner,
                td.Old_Value        as Old_Value,
                td.New_Value        as New_Value,
                h.Date_Create,
                td.Column_Name      as Column_name,
                th.table_name       as th_table
         FROM lake_view.JF_Portal.Change_Log_Hdr2 h
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Hdr th
                       ON h.Change_Log_Hdr_Id = th.Change_Log_Hdr_Id
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Dtl td
                       ON th.Change_Log_Tbl_Hdr_Id = td.Change_Log_Tbl_Hdr_Id
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON CR
                            ON H.DOC_ID = CR.PO_ID
                                AND H.DOC_VERSION = CR.VERSION
                                and to_varchar(cr.date_create::timestamp, 'mm/dd/yyyy, hh24:mi') =
                                    to_varchar(h.date_create::timestamp, 'mm/dd/yyyy, hh24:mi')
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON_CODE CRC
                            ON CR.PO_CHANGE_REASON_CODE_ID = CRC.PO_CHANGE_REASON_CODE_ID
                  JOIN lake_view.JF_Portal.Po_Hdr ph
                       ON upper(th.Table_Name) = 'POHDR'--'PoHdr'
                           AND to_VARCHAR(ph.Po_Id) = td.Tbl_Id
                  left join _tbl t
                            on upper(th.table_name) = upper(t.table_name)
         WHERE Doc_Type_Id = 2
           AND UPPER(TD.COLUMN_NAME) NOT IN ('SHIPPINGPORTID', 'INCOTERMID', 'FREIGHTMETHODID', 'POSTATUSID')
           and h.date_create > $changedate

         UNION ALL

         SELECT h.Doc_Id,
                h.Change_Log_Hdr_Id as Log_Hdr_Id,
                t.table_type        as Table_name,
                Crc.Reason          as Change_Reason,
                h.User_Name         as User_Name,
                ph.color            as Owner,
                td.Old_Value        as Old_Value,
                td.New_Value        as New_Value,
                h.Date_Create,
                td.Column_Name      as Column_name,
                th.table_name       as th_table
         FROM lake_view.JF_Portal.Change_Log_Hdr2 h
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Hdr th
                       ON h.Change_Log_Hdr_Id = th.Change_Log_Hdr_Id
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Dtl td
                       ON th.Change_Log_Tbl_Hdr_Id = td.Change_Log_Tbl_Hdr_Id
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON CR
                            ON H.DOC_ID = CR.PO_ID
                                AND H.DOC_VERSION = CR.VERSION
                                and to_varchar(cr.date_create::timestamp, 'mm/dd/yyyy, hh24:mi') =
                                    to_varchar(h.date_create::timestamp, 'mm/dd/yyyy, hh24:mi')
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON_CODE CRC
                            ON CR.PO_CHANGE_REASON_CODE_ID = CRC.PO_CHANGE_REASON_CODE_ID
                  JOIN lake_view.JF_Portal.Po_Hts ph
                       ON upper(th.Table_Name) = 'POHTS' --'PoHts'
                           AND to_VARCHAR(ph.Po_Hts_Id) = td.Tbl_Id
                  left join _tbl t
                            on upper(th.table_name) = upper(t.table_name)
         WHERE Doc_Type_Id = 2
           and h.date_create > $changedate
           --AND h.DocId = @PoId

         UNION ALL

         SELECT h.Doc_Id,
                h.Change_Log_Hdr_Id as Log_Hdr_Id,
                t.table_type        as Table_name,
                Crc.Reason          as Change_Reason,
                h.User_Name         as User_Name,
                null                as Owner,
                td.Old_Value        as Old_Value,
                td.New_Value        as New_Value,
                h.Date_Create,
                td.Column_Name      as Column_name,
                th.table_name       as th_table
         FROM lake_view.JF_Portal.Change_Log_Hdr2 h
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Hdr th
                       ON h.Change_Log_Hdr_Id = th.Change_Log_Hdr_Id
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Dtl td
                       ON th.Change_Log_Tbl_Hdr_Id = td.Change_Log_Tbl_Hdr_Id
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON CR
                            ON H.DOC_ID = CR.PO_ID
                                AND H.DOC_VERSION = CR.VERSION
                                and to_varchar(cr.date_create::timestamp, 'mm/dd/yyyy, hh24:mi') =
                                    to_varchar(h.date_create::timestamp, 'mm/dd/yyyy, hh24:mi')
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON_CODE CRC
                            ON CR.PO_CHANGE_REASON_CODE_ID = CRC.PO_CHANGE_REASON_CODE_ID
                  JOIN lake_view.JF_Portal.Po_Hts ph
                       ON upper(th.Table_Name) NOT IN ('PODTL', 'POHDR', 'POHTS') --('PoDtl','PoHdr','PoHts')
                           AND to_VARCHAR(ph.Po_Hts_Id) = td.Tbl_Id
                  left join _tbl t
                            on upper(th.table_name) = upper(t.table_name)
         WHERE Doc_Type_Id = 2
           and h.date_create > $changedate
           --AND h.DocId = @PoId

         UNION ALL

         SELECT h.Doc_Id,
                h.Change_Log_Hdr_Id as Log_Hdr_Id,
                t.table_type        as Table_name,
                Crc.Reason          as Change_Reason,
                h.User_Name         as User_Name,
                m.name              as Owner,
                td.Old_Value        as Old_Value,
                td.New_Value        as New_Value,
                h.Date_Create,
                td.Column_Name      as Column_name,
                th.table_name       as th_table
         FROM lake_view.JF_Portal.Change_Log_Hdr2 h
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Hdr th
                       ON h.Change_Log_Hdr_Id = th.Change_Log_Hdr_Id
                  JOIN lake_view.JF_Portal.Change_Log_Tbl_Dtl td
                       ON th.Change_Log_Tbl_Hdr_Id = td.Change_Log_Tbl_Hdr_Id
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON CR
                            ON H.DOC_ID = CR.PO_ID
                                AND H.DOC_VERSION = CR.VERSION
                                and to_varchar(cr.date_create::timestamp, 'mm/dd/yyyy, hh24:mi') =
                                    to_varchar(h.date_create::timestamp, 'mm/dd/yyyy, hh24:mi')
                  LEFT JOIN LAKE_VIEW.JF_PORTAL.PO_CHANGE_REASON_CODE CRC
                            ON CR.PO_CHANGE_REASON_CODE_ID = CRC.PO_CHANGE_REASON_CODE_ID
                  JOIN lake_view.JF_Portal.Po_Milestone m
                       ON td.Tbl_Id = to_VARCHAR(m.Po_Milestone_Id)
                  left join _tbl t
                            on upper(th.table_name) = upper(t.table_name)
         WHERE th.Change_Log_Tbl_Hdr_Id IN (
             SELECT Change_Log_Tbl_Hdr_Id
             FROM lake_view.JF_Portal.Change_Log_Tbl_Hdr
             WHERE upper(Table_Name) = 'POMILESTONE' -- 'PoMilestone'
         )
           and h.date_create > $changedate
     ) AS A;

delete from reporting_prod.gsc.po_audit_trail_dataset where change_date > $changedate;

insert into REPORTING_PROD.GSC.PO_AUDIT_TRAIL_DATASET
(
  doc_id
  ,division
  ,showroom
  ,po_number
  ,vendor
  ,class
  ,po_status
  ,department
  ,total_quantity
  ,table_NAME
  ,column_name
  ,user_name
  ,line
  ,old_value
  ,new_value
  ,change_date
  ,change_reason
)
Select distinct
 P.PO_Id
 ,P.Division_Id
 ,P.Show_room
 ,P.PO_Number
 ,P.Vend_Name
 ,d.Style_Type as Class
 ,s.Description as PO_Status
 ,p.Department
 ,d.Total_Quantity
 ,cd.Table_NAME
 ,cd.Column_Name
 ,cd.User_Name
 ,cd.owner as Line
 ,cd.Old_Value
 ,cd.New_Value
 ,cd.date_create as Change_Date
 ,cd.Change_Reason
from _change_data cd
join LAKE_VIEW.jf_portal.PO_hdr P
    on cd.doc_id = p.po_id
Left Join (
    Select
        PO_id,
        Style_Type,
        sum(qty) as total_Quantity
    from LAKE_VIEW.jf_portal.PO_Dtl
    group by PO_Id, Style_Type
    ) d
on P.Po_Id = d.PO_Id
left join lake_view.jf_portal.po_status s
    on p.po_status_id = s.po_status_id
where p.po_id in (select distinct doc_id from _CHANGE_DATA where date_create > $changedate)
order by p.show_room ASC, p.PO_Number ASC;
