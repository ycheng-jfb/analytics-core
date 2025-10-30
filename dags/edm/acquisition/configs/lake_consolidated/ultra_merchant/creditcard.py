from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='creditcard',
    company_join_sql="""
     SELECT DISTINCT
         L.CREDITCARD_ID,
         DS.company_id
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.CUSTOMER AS C
     ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
     INNER JOIN {database}.{source_schema}.creditcard AS L
     ON L.CUSTOMER_ID=C.CUSTOMER_ID """,
    column_list=[
        Column('creditcard_id', 'INT', uniqueness=True, key=True),
        Column('customer_id', 'INT', key=True),
        Column('address_id', 'INT', key=True),
        Column('cnh', 'VARCHAR(32)'),
        Column('card_num', 'VARCHAR(255)'),
        Column('card_type', 'VARCHAR(15)'),
        Column('exp_month', 'VARCHAR(2)'),
        Column('exp_year', 'VARCHAR(2)'),
        Column('name_on_card', 'VARCHAR(50)'),
        Column('last_four_digits', 'VARCHAR(4)'),
        Column('method', 'VARCHAR(25)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('datetime_last_used', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
    ],
    watermark_column='datetime_modified',
)
