from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultracms',
    schema='dbo',
    table='audit',
    column_list=[
        Column('audit_id', 'INT', uniqueness=True),
        Column('administrator_id', 'INT'),
        Column('db_name', 'VARCHAR(100)'),
        Column('table_name', 'VARCHAR(100)'),
        Column('table_column', 'VARCHAR(100)'),
        Column('id_value', 'VARCHAR(50)'),
        Column('action', 'VARCHAR(50)'),
        Column('before_value', 'VARCHAR'),
        Column('after_value', 'VARCHAR'),
        Column('date_added', 'TIMESTAMP_NTZ(3)'),
    ],
)
