from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table="brand",
    table_type=TableType.REGULAR_GLOBAL,
    company_join_sql="""
   SELECT l.brand_id, ds.company_id
    FROM {database}.{schema}.product l
    JOIN {database}.reference.dim_store ds
        ON l.store_group_id = ds.store_group_id
    UNION
    SELECT l.brand_id, ds.company_id
    FROM {database}.{source_schema}.brand l
        JOIN {database}.reference.dim_store AS ds
            ON l.store_group_id = ds.store_group_id """,
    column_list=[
        Column("brand_id", "INT ", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("code", "VARCHAR(25)"),
        Column("label", "VARCHAR(100)"),
        Column("meta_keywords", "VARCHAR(255)"),
        Column("meta_description", "VARCHAR(255)"),
        Column("disclaimer", "VARCHAR(255)"),
        Column("country_code", "VARCHAR(2)"),
        Column("image_file_type", "VARCHAR(25)"),
        Column("call_for_price", "INT"),
        Column("international_is_ok", "INT"),
        Column("no_online_sales", "INT"),
        Column("msrp_required", "INT"),
        Column("login_required", "INT"),
        Column("is_internal", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
        Column("new_membership_promo_allowed", "INT"),
    ],
    watermark_column="datetime_added",
    post_sql="""
    DELETE FROM {database}.ultra_merchant.brand
    WHERE brand_id IN (SELECT brand_id
        FROM {database}.ultra_merchant.brand
        GROUP BY brand_id
        HAVING COUNT(1) > 1)
    AND (data_source_id = 10 AND meta_company_id != 10)
        OR (data_source_id = 40 AND meta_company_id NOT IN (20, 30, 40));
    """,
)
