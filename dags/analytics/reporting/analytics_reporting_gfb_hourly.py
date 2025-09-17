import pendulum
from airflow.models import DAG

from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import owners
from include.config.email_lists import analytics_support

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 5, 15, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.jfb_analytics,
    "email": analytics_support,
}


dag = DAG(
    dag_id="analytics_reporting_gfb_hourly",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
)


with dag:
    gfb044_holiday_data_set = SnowflakeProcedureOperator(
        procedure="gfb.gfb044_holiday_data_set.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb044_holiday_data_set = TableauRefreshOperator(
        task_id="tableau_refresh_gfb044_holiday_data_set",
        data_source_name="GFB044_HOLIDAY_DATA_SET",
    )

    gfb044_holiday_data_set >> tableau_refresh_gfb044_holiday_data_set
