from include.airflow.operators.snowflake import SnowflakeEdwProcedureOperator


def get_mart_operator():
    return SnowflakeEdwProcedureOperator(
        procedure='reference.gsc_landed_cost_history.sql', database='edw_prod'
    )
