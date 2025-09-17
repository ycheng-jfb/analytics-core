from include.airflow.operators.snowflake import SnowflakeEdwProcedureOperator


def get_mart_operator():
    return SnowflakeEdwProcedureOperator(
        procedure="stg.transform_lkp_membership_event_type.sql", database="edw_prod"
    )
