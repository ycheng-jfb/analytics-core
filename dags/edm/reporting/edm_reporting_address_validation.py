import datetime

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeAlertOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2022, 6, 1, 0, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_reporting_address_validation",
    default_args=default_args,
    schedule="0 1 24 * *",
    catchup=False,
    max_active_runs=1,
)

sql_or_path = """
    SELECT c.store_group_id AS SG_Id,
        sc.statuscode AS StatusCode,
        sc.label AS Label,
        COUNT(1) AS Count
    FROM lake_consolidated.ultra_merchant.address_validation_request_log avrl
    JOIN lake_consolidated.ultra_merchant.customer c ON avrl.customer_id = c.customer_id
        AND c.store_group_id IN (10, 22)
    JOIN lake_consolidated.ultra_merchant.statuscode sc ON sc.statuscode = avrl.statuscode
    JOIN lake_consolidated.ultra_merchant.session s ON s.session_id = avrl.session_id
        AND s.datetime_added >= DATEADD(DAY, 20, DATE_TRUNC('MONTH',DATEADD(MONTH, -1, CURRENT_DATE())))
        AND s.datetime_added < DATEADD(DAY, 20, DATE_TRUNC('MONTH',CURRENT_DATE()))
    GROUP BY c.store_group_id,
        sc.statuscode,
        sc.label
    ORDER BY 1, 2
"""

with dag:
    address_validation = SnowflakeAlertOperator(
        task_id="address_validation",
        sql_or_path=sql_or_path,
        distribution_list=[
            "lars@techstyle.com",
            "cfernandes@techstyle.com",
            "dataanalyticsoperations@techstyle.com",
        ],
        subject=f"Address validation calls / DE - > FL & JF {{ macros.datetime.today().strftime('%Y-%m-%d') }}",
        body="Address validation calls from last month 21 to current month 20",
    )
