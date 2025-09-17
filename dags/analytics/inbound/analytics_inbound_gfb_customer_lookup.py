import pendulum
from airflow.models import DAG

from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import owners
from include.config.email_lists import analytics_support

default_args = {
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.jfb_analytics,
    "email": analytics_support,
}


dag = DAG(
    dag_id="analytics_inbound_gfb_customer_lookup",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
)

with dag:
    dos_110_customer_lookup = SnowflakeProcedureOperator(
        procedure="gfb.dos_110_customer_lookup.sql",
        database="reporting_prod",
        autocommit=False,
        warehouse="DA_WH_ETL_LIGHT",
    )

    tableau_refresh_dos_110_customer_lookup = TableauRefreshOperator(
        task_id="tableau_refresh_dos_110_customer_lookup",
        data_source_name="DOS_110_CUSTOMER_LOOKUP",
    )

    (dos_110_customer_lookup >> tableau_refresh_dos_110_customer_lookup)
