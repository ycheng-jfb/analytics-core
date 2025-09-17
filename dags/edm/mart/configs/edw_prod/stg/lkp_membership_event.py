from include.airflow.operators.snowflake import SnowflakeEdwProcedureOperator


def get_mart_operator():
    return SnowflakeEdwProcedureOperator(
        procedure="stg.transform_lkp_membership_event.sql", database="edw_prod"
    )
