from include.airflow.operators.snowflake import SnowflakeEdwProcedureOperator


def get_mart_operator():
    return SnowflakeEdwProcedureOperator(
        procedure="reference.gsc_po_detail_dataset_history.sql", database="edw_prod"
    )
