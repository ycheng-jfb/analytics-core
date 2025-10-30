from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table='partner',
    column_list=[
        Column('partner_id', 'INT', uniqueness=True),
        Column('code', 'VARCHAR(50)'),
        Column('label', 'VARCHAR(50)'),
        Column('ftp_host', 'VARCHAR(75)'),
        Column('ftp_username', 'VARCHAR(75)'),
        Column('ftp_password', 'VARCHAR(75)'),
        Column('ftp_cert_key', 'VARCHAR(75)'),
        Column('ftp_root_directory', 'VARCHAR(100)'),
        Column('ftp_protocol', 'VARCHAR(20)'),
        Column('encryption_key', 'VARCHAR(50)'),
        Column('encryption_type', 'VARCHAR(50)'),
        Column('console_username', 'VARCHAR(75)'),
        Column('console_password', 'VARCHAR(75)'),
        Column(
            'datetime_added',
            'TIMESTAMP_NTZ(3)',
        ),
        Column(
            'datetime_modified',
            'TIMESTAMP_NTZ(3)',
        ),
        Column('statuscode', 'INT'),
    ],
    watermark_column='datetime_modified',
)
