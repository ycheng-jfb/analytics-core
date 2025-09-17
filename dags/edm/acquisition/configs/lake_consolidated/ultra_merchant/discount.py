from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table="discount",
    table_type=TableType.REGULAR_GLOBAL,
    company_join_sql="""
    SELECT DISTINCT
        L.discount_id,
        DS.company_id
    FROM {database}.{source_schema}.discount AS L
    JOIN (
        SELECT DISTINCT company_id
        FROM {database}.REFERENCE.DIM_STORE
        WHERE company_id IS NOT NULL
        ) AS DS""",
    column_list=[
        Column("discount_id", "INT", uniqueness=True, key=True),
        Column("applied_to", "VARCHAR(15)"),
        Column("calculation_method", "VARCHAR(25)"),
        Column("label", "VARCHAR(50)"),
        Column("percentage", "DOUBLE"),
        Column("rate", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("date_expires", "TIMESTAMP_NTZ(0)"),
        Column("statuscode", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
    post_sql="""
        UPDATE {database}.{schema}.discount
        SET discount_id = meta_company_id
        WHERE discount_id IS NULL;

        DELETE FROM {database}.ultra_merchant.discount
        WHERE discount_id IN (SELECT discount_id
            FROM {database}.ultra_merchant.discount
            GROUP BY discount_id
            HAVING COUNT(1) > 1)
        AND (data_source_id = 10 AND meta_company_id != 10)
            OR (data_source_id = 40 AND meta_company_id NOT IN (20, 30, 40));
    """,
)
