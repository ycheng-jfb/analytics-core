from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table='endowment',
    company_join_sql="""SELECT DISTINCT
           L.endowment_id,
           DS.company_id
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}.customer AS C
           ON DS.store_id = C.store_id
       INNER JOIN {database}.{source_schema}.endowment AS L
           ON L.customer_id = C.customer_id""",
    column_list=[
        Column('endowment_id', 'INT', uniqueness=True, key=True),
        Column('endowment_type_id', 'INT'),
        Column('endowment_batch_id', 'INT'),
        Column('customer_id', 'INT', key=True),
        Column('store_credit_id', 'INT', key=True),
        Column('statuscode', 'INT'),
    ],
)
