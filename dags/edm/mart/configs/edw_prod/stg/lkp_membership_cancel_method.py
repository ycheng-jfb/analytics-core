from include.airflow.operators.snowflake import SnowflakeEdwProcedureOperator


def get_mart_operator():
    return SnowflakeEdwProcedureOperator(
        procedure="stg.transform_lkp_membership_cancel_method.sql", database="edw_prod"
    )
