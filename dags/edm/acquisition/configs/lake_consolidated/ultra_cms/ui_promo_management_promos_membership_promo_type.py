from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    schema="ultra_cms",
    table="ui_promo_management_promos_membership_promo_type",
    company_join_sql="""
      SELECT DISTINCT
          L.ui_promo_management_promos_membership_promo_type_id,
          DS.company_id
      FROM {database}.REFERENCE.DIM_STORE AS DS
      INNER JOIN {database}.{source_schema}.ui_promo_management_promos AS UPMP
      ON DS.STORE_GROUP_ID = UPMP.STORE_GROUP_ID
      INNER JOIN {database}.{source_schema}.ui_promo_management_promos_membership_promo_type AS L
          ON L.ui_promo_management_promo_id = UPMP.ui_promo_management_promo_id """,
    column_list=[
        Column(
            "ui_promo_management_promos_membership_promo_type_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
        Column("ui_promo_management_promo_id", "INT", key=True),
        Column("membership_promo_type_id", "INT"),
    ],
)
