from include.airflow.operators.snowflake import SnowflakeEdwProcedureOperator


def get_mart_operator():
    return SnowflakeEdwProcedureOperator(
        procedure='reference.gfc_po_sku_line_lpn_mapping.sql', database='edw_prod'
    )
