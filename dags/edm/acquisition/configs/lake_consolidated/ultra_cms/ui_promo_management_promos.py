from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    schema="ultra_cms",
    table="ui_promo_management_promos",
    company_join_sql="""
        SELECT DISTINCT
            L.ui_promo_management_promo_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.ui_promo_management_promos AS L
            ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("ui_promo_management_promo_id", "INT", uniqueness=True, key=True),
        Column("promo_name", "VARCHAR(200)"),
        Column("ui_promo_management_campaign_id", "INT", key=True),
        Column("promo_type_id", "INT"),
        Column("store_group_id", "INT"),
        Column("short_label", "VARCHAR(50)"),
        Column("long_label", "VARCHAR(255)"),
        Column("start_date", "DATE"),
        Column("start_time", "TIME"),
        Column("end_date", "DATE"),
        Column("end_time", "TIME"),
        Column("ongoing_promotion", "INT"),
        Column("code", "VARCHAR(255)"),
        Column("statuscode", "INT"),
        Column("administrator_id", "INT"),
        Column("promo_id", "INT", key=True),
        Column("isactive", "INT"),
        Column("setup_json", "VARCHAR"),
        Column("target_user_json", "VARCHAR"),
        Column("qualifications_json", "VARCHAR"),
        Column("benefits_json", "VARCHAR"),
        Column("additional_settings_json", "VARCHAR"),
        Column("delivery_method_json", "VARCHAR"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("display_settings_json", "VARCHAR"),
    ],
    watermark_column="datetime_modified",
)
