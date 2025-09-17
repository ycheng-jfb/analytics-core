from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='jf_portal',
    schema='dbo',
    table='Division',
    watermark_column='DateUpdate',
    schema_version_prefix='v3',
    column_list=[
        Column('division_id', 'VARCHAR(15)', uniqueness=True, source_name='DivisionId'),
        Column('incotermid_def', 'INT', source_name='IncoTermId_Def'),
        Column('inter_id', 'VARCHAR(15)', source_name='InterId'),
        Column('abbr', 'VARCHAR(2)', source_name='Abbr'),
        Column('from_addr', 'VARCHAR(100)', source_name='FromAddr'),
        Column('address1', 'VARCHAR(61)', source_name='ADDRESS1'),
        Column('address2', 'VARCHAR(61)', source_name='ADDRESS2'),
        Column('address3', 'VARCHAR(61)', source_name='ADDRESS3'),
        Column('city', 'VARCHAR(35)', source_name='CITY'),
        Column('state', 'VARCHAR(29)', source_name='STATE'),
        Column('zipcode', 'VARCHAR(11)', source_name='ZIPCODE'),
        Column('country', 'VARCHAR(61)', source_name='COUNTRY'),
        Column('phone1', 'VARCHAR(21)', source_name='PHONE1'),
        Column('phone2', 'VARCHAR(21)', source_name='PHONE2'),
        Column('phone3', 'VARCHAR(21)', source_name='PHONE3'),
        Column('faxnumbr', 'VARCHAR(21)', source_name='FAXNUMBR'),
        Column('store_group_id', 'INT'),
        Column('date_create', 'TIMESTAMP_NTZ(3)', delta_column=1, source_name='DateCreate'),
        Column('date_update', 'TIMESTAMP_NTZ(3)', delta_column=0, source_name='DateUpdate'),
        Column('legal_entity_name', 'VARCHAR(50)', source_name='LegalEntityName'),
    ],
)
