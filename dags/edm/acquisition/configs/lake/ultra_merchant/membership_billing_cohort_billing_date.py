from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column

table_config = TableConfig(
    database='ultramerchant',
    schema='dbo',
    table='membership_billing_cohort_billing_date',
    schema_version_prefix='v2',
    column_list=[
        Column('membership_billing_cohort_billing_date_id', 'INT', uniqueness=True),
        Column('membership_billing_cohort_id', 'INT'),
        Column('percentage', 'DOUBLE'),
        Column('date_due', 'TIMESTAMP_NTZ(0)'),
    ],
)
