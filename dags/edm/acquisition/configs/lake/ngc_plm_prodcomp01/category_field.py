from include.airflow.operators.mssql_acquisition import HighWatermarkMaxVarcharDatetimeOffset
from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ngc_plm_prodcomp01',
    schema='dbo',
    table='CategoryField',
    high_watermark_cls=HighWatermarkMaxVarcharDatetimeOffset,
    watermark_column='ModifiedOn',
    schema_version_prefix='v2',
    column_list=[
        Column('id_category_field', 'INT', uniqueness=True, source_name='ID_CategoryField'),
        Column('created_by', 'VARCHAR(256)', source_name='CreatedBy'),
        Column(
            'created_on',
            'TIMESTAMP_LTZ(7)',
            delta_column=1,
            source_name='convert(VARCHAR, CreatedOn, 127)',
        ),
        Column('modified_by', 'VARCHAR(256)', source_name='ModifiedBy'),
        Column(
            'modified_on',
            'TIMESTAMP_LTZ(7)',
            delta_column=0,
            source_name='convert(VARCHAR, ModifiedOn, 127)',
        ),
        Column('row_version', 'INT', source_name='RowVersion'),
        Column('name', 'VARCHAR(50)', source_name='Name'),
        Column('type', 'INT', source_name='Type'),
        Column('size', 'INT', source_name='Size'),
        Column('decimals', 'INT', source_name='Decimals'),
        Column('is_active', 'BOOLEAN', source_name='isActive'),
        Column('physical_name', 'VARCHAR(50)', source_name='PhysicalName'),
        Column('label', 'VARCHAR(50)', source_name='Label'),
        Column('value_type', 'INT', source_name='ValueType'),
        Column('is_apply_values_to_entities', 'BOOLEAN', source_name='isApplyValuesToEntities'),
    ],
)
