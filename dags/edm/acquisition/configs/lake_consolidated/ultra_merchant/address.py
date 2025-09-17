from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table='address',
    table_type=TableType.REGULAR_GLOBAL,
    company_join_sql="""
SELECT l.address_id, ds.company_id
FROM {database}.{schema}.address l
JOIN (
SELECT l.address_id,
                ds.company_id
FROM {database}.REFERENCE.dim_store AS ds
         INNER JOIN {database}.{schema}.customer AS c
                    ON ds.store_group_id = c.store_group_id
         INNER JOIN {database}.{schema}.address AS l
                    ON l.customer_id = c.customer_id
UNION

SELECT l.address_id,
                ds.company_id
FROM {database}.REFERENCE.dim_store AS ds
         INNER JOIN {database}.{schema}.customer AS c
                    ON ds.store_group_id = c.store_group_id
         INNER JOIN {database}.{schema}.address AS l
                    ON l.address_id = c.default_address_id
UNION

SELECT cc.address_id,
       ds.company_id
FROM {database}.REFERENCE.dim_store AS ds
    INNER JOIN {database}.{schema}.customer AS c
        ON ds.store_id = c.store_id
    INNER JOIN {database}.{schema}.creditcard AS cc
        ON cc.customer_id = c.customer_id

UNION
SELECT l.address_id,
       ds.company_id
FROM {database}.REFERENCE.dim_store AS ds
         INNER JOIN {database}.{schema}."ORDER" AS o
                    ON ds.store_id = o.store_id
         INNER JOIN {database}.{schema}.address AS l
                    ON o.billing_address_id = l.address_id
UNION
SELECT l.address_id,
        ds.company_id
FROM {database}.REFERENCE.dim_store AS ds
         INNER JOIN {database}.{schema}."ORDER" AS o
                    ON ds.store_id = o.store_id
         INNER JOIN {database}.{schema}.address AS l
                    ON o.shipping_address_id = l.address_id) as ds
ON l.address_id = ds.address_id""",
    column_list=[
        Column('address_id', 'INT', uniqueness=True, key=True),
        Column('customer_id', 'INT', key=True),
        Column('firstname', 'VARCHAR(25)'),
        Column('lastname', 'VARCHAR(25)'),
        Column('name', 'VARCHAR(50)'),
        Column('company', 'VARCHAR(100)'),
        Column('address1', 'VARCHAR(50)'),
        Column('address2', 'VARCHAR(35)'),
        Column('city', 'VARCHAR(35)'),
        Column('state', 'VARCHAR(25)'),
        Column('zip', 'VARCHAR(25)'),
        Column('country_code', 'VARCHAR(2)'),
        Column('phone', 'VARCHAR(25)'),
        Column('us_phone_areacode', 'VARCHAR(3)'),
        Column('us_phone_prefix', 'VARCHAR(3)'),
        Column('us_phone_linenumber', 'VARCHAR(4)'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('is_validated', 'INT'),
        Column('datetime_modified', 'TIMESTAMP_NTZ(3)'),
        Column('phone_digits', 'VARCHAR(25)'),
        Column('address_hash', 'BINARY(20)'),
        Column('address3', 'VARCHAR(255)'),
        Column('address4', 'VARCHAR(255)'),
        Column('address5', 'VARCHAR(255)'),
        Column('address6', 'VARCHAR(255)'),
        Column('address7', 'VARCHAR(255)'),
        Column('address8', 'VARCHAR(255)'),
        Column('address_type_id', 'INT'),
        Column('email', 'VARCHAR(75)'),
        Column('is_customer_validated', 'INT'),
    ],
    watermark_column='datetime_modified',
)
