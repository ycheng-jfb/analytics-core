import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import BaseSensorOperator

from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import conn_ids, owners
from include.config.email_lists import analytics_support
from include.utils.context_managers import ConnClosing
from include.utils.snowflake import generate_query_tag_cmd

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.jfb_analytics,
    "email": analytics_support,
}


def check_the_time(next_execution_date: pendulum.datetime, **kwargs):
    run_time = next_execution_date.in_timezone("America/Los_Angeles")
    if run_time.hour < 12:
        return [gfb059_finance_daily_sales.task_id]
    elif run_time.hour >= 12:
        return [gfb055_02_outlook_gsheet_hist.task_id]
    else:
        return []


class check_total_cac_output(BaseSensorOperator):
    def __init__(
        self,
        snowflake_conn_id=conn_ids.Snowflake.default,
        poke_interval=60 * 5,
        mode="reschedule",
        timeout=60 * 60,
        **kwargs,
    ):
        self.snowflake_conn_id = snowflake_conn_id
        super().__init__(
            **kwargs, poke_interval=poke_interval, mode=mode, timeout=timeout
        )

    def snowflake_hook(self):
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
        )

    def poke(self, context):
        cmd = """
            select distinct
                1 as result
            from reporting_media_prod.attribution.total_cac_output a
            where
                a.STORE_NAME in ('JustFab NA', 'JustFab EU', 'ShoeDazzle NA', 'FabKids NA')
                and a.SOURCE = 'DailyTY'
                and a.CURRENCY = 'USD'
                and a.DATE = dateadd(day, -1, current_date())"""
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        with ConnClosing(snowflake_hook.get_conn()) as conn:
            cur = conn.cursor()
            cur.execute(query_tag)
            cur.execute(cmd)
            result = cur.fetchone()

        if result is None or result[0] != 1:
            print("Retrying")
            return False

        print("Job Executed")
        return True


class check_daily_cash_output(BaseSensorOperator):
    def __init__(
        self,
        snowflake_conn_id=conn_ids.Snowflake.default,
        poke_interval=60 * 5,
        mode="reschedule",
        timeout=60 * 60,
        **kwargs,
    ):
        self.snowflake_conn_id = snowflake_conn_id
        super().__init__(
            **kwargs, poke_interval=poke_interval, mode=mode, timeout=timeout
        )

    def snowflake_hook(self):
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
        )

    def poke(self, context):
        cmd = """
            select distinct
                1 as result
            from EDW_PROD.REPORTING.DAILY_CASH_FINAL_OUTPUT a
            where
                a.REPORT_MAPPING in ('JFNA-TREV', 'JFEU-TREV', 'SDNA-TREV', 'FKNA-TREV')
                and a.CURRENCY_TYPE = 'USD'
                and a.DATE_OBJECT = 'placed'
                and a.DATE = dateadd(day, -1, current_date())"""
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        with ConnClosing(snowflake_hook.get_conn()) as conn:
            cur = conn.cursor()
            cur.execute(query_tag)
            cur.execute(cmd)
            result = cur.fetchone()

        if result is None or result[0] != 1:
            print("Retrying")
            return False

        print("Job Executed")
        return True


dag = DAG(
    dag_id="analytics_reporting_gfb_finance",
    default_args=default_args,
    schedule="0 8,12,16 * * *",
    max_active_tasks=15,
    catchup=False,
)


with dag:
    gfb059_finance_daily_sales = SnowflakeProcedureOperator(
        procedure="gfb.gfb059_finance_daily_sales.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb059_finance_daily_sales = TableauRefreshOperator(
        task_id="tableau_refresh_gfb059_finance_daily_sales",
        data_source_name="GFB059_FINANCE_DAILY_SALES",
    )

    gfb055_02_outlook_gsheet_hist = SnowflakeProcedureOperator(
        procedure="gfb.gfb055_02_outlook_gsheet_hist.sql",
        database="reporting_prod",
        autocommit=False,
    )

    gfb055_01_outlook_metrics = SnowflakeProcedureOperator(
        procedure="gfb.gfb055_01_outlook_metrics.sql",
        database="reporting_prod",
        autocommit=False,
    )

    gfb055_month_end_kpi = SnowflakeProcedureOperator(
        procedure="gfb.gfb055_month_end_kpi.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb055_month_end_kpi = TableauRefreshOperator(
        task_id="tableau_refresh_gfb055_month_end_kpi",
        data_source_name="GFB055_MONTH_END_KPI",
    )

    # check_the_time_task = BranchPythonOperator(
    #     task_id="check_the_time",
    #     python_callable=check_the_time,
    # )

    gfb055_01_01_outlook_supplement = SnowflakeProcedureOperator(
        procedure="gfb.gfb055_01_01_outlook_supplement.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb055_01_01_outlook_supplement = TableauRefreshOperator(
        task_id="tableau_refresh_gfb055_01_01_outlook_supplement",
        data_source_name="GFB055_01_01_OUTLOOK_SUPPLEMENT",
    )

    gfb076_daily_sales_refresh = SnowflakeProcedureOperator(
        procedure="gfb.gfb076_daily_sales_refresh.sql",
        database="reporting_prod",
        autocommit=False,
    )

    tableau_refresh_gfb076_daily_sales_refresh = TableauRefreshOperator(
        task_id="tableau_refresh_gfb076_daily_sales_refresh",
        data_source_name="GFB076_DAILY_SALES_REFRESH",
    )

    # layer 1 joins
    #    join_branch_0 = EmptyOperator(task_id="join_branch_0", trigger_rule="none_failed")

    # layer_2_joins
    join_branch_1 = EmptyOperator(task_id="join_branch_1", trigger_rule="none_failed")

    # layer 0
    join_branch_1 >> gfb055_02_outlook_gsheet_hist

    # layer 3
    (
        gfb055_02_outlook_gsheet_hist
        >> gfb059_finance_daily_sales
        >> gfb055_01_outlook_metrics
        >> [
            gfb055_month_end_kpi,
            gfb055_01_01_outlook_supplement,
        ]
    )

    # layer 4
    gfb059_finance_daily_sales >> tableau_refresh_gfb059_finance_daily_sales

    gfb055_month_end_kpi >> tableau_refresh_gfb055_month_end_kpi

    gfb055_01_01_outlook_supplement >> tableau_refresh_gfb055_01_01_outlook_supplement

    gfb076_daily_sales_refresh >> tableau_refresh_gfb076_daily_sales_refresh
