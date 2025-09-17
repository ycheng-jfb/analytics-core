from typing import Any

import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.base import BaseSensorOperator
from functools import cached_property
from snowflake.connector import DictCursor

from include.airflow.callbacks.slack import SlackFailureCallback
from include.airflow.dag_helpers import chain_tasks
from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.mssql import MsSqlToS3Operator
from include.airflow.operators.snowflake import (
    SnowflakeProcedureOperator,
    SnowflakeSqlOperator,
)
from include.airflow.operators.sqlagent import RunSqlAgent
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils import snowflake
from include.utils.context_managers import ConnClosing
from include.utils.decorators import retry_wrapper

default_args = {
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": SlackFailureCallback("slack_alert_edm"),
}

dag = DAG(
    dag_id="edm_reporting_passive_cancels_monthly",
    default_args=default_args,
    schedule="0 10 3,20 * *",
    catchup=False,
    max_active_tasks=20,
)

mssql_conn_id_server_dict = {
    "jfb": {"conn_id": conn_ids.MsSql.justfab_app_airflow_rw, "brand_id": "10"},
    "fl": {"conn_id": conn_ids.MsSql.fabletics_app_airflow_rw, "brand_id": "20"},
    "sxf": {"conn_id": conn_ids.MsSql.savagex_app_airflow_rw, "brand_id": "30"},
}


class SnowflakeToMSSQL(BaseOperator):
    def __init__(
        self,
        snowflake_cmd,
        pre_sql_cmd,
        post_sql_cmd,
        mssql_target_table,
        mssql_conn_id,
        snowflake_conn_id=conn_ids.Snowflake.default,
        **kwargs,
    ):
        self.snowflake_conn_id = snowflake_conn_id
        self.pre_sql_cmd = pre_sql_cmd
        self.post_sql_cmd = post_sql_cmd
        self.mssql_target_table = mssql_target_table
        self.snowflake_cmd = snowflake_cmd
        self.mssql_conn_id = mssql_conn_id
        super().__init__(**kwargs)

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
        )

    @cached_property
    def mssql_hook(self):
        return MsSqlOdbcHook(
            mssql_conn_id=self.mssql_conn_id,
            database="ultramerchant",
        )

    def execute(self, context):
        with ConnClosing(self.mssql_hook.get_conn()) as con:
            cur = con.cursor()
            cur.execute(self.pre_sql_cmd)
        with ConnClosing(self.snowflake_hook.get_conn()) as conn:
            query_tag = snowflake.generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = conn.cursor(cursor_class=DictCursor)
            cur.execute(query_tag)
            for rows in pd.read_sql_query(
                sql=self.snowflake_cmd, con=conn, chunksize=20000
            ):  # snowflake
                with self.mssql_hook.get_sqlalchemy_connection() as conn_mssql:
                    min_value = rows["MEMBERSHIP_ID"].min()
                    max_value = rows["MEMBERSHIP_ID"].max()
                    filter_condition = f"{min_value} and {max_value}"
                    rows.to_sql(
                        name=self.mssql_target_table,
                        con=conn_mssql,
                        chunksize=20000,
                        if_exists="append",
                        index=False,
                    )
                    cur = conn.cursor()
                    final_update_statement = f"{self.post_sql_cmd} {filter_condition};"
                    cur.execute(final_update_statement)


class CheckEcomJobCompletionSensor(BaseSensorOperator):
    def __init__(
        self,
        mssql_conn_id,
        poke_interval=60 * 10,
        mode="reschedule",
        timeout=60 * 60,
        **kwargs,
    ):
        self.mssql_conn_id = mssql_conn_id
        super().__init__(
            **kwargs, poke_interval=poke_interval, mode=mode, timeout=timeout
        )

    def poke(self, context):
        cmd = """
            SELECT TOP 1 success
            FROM [ultramerchant].dbo.syn_process_log_master WITH(NOLOCK)
            WHERE [process_master_name] = 'monthly_passive_cancels_process'
                AND CAST(datetime_start AS DATE) = CAST(GETDATE() AS DATE)
            ORDER BY 1 DESC"""
        mssql_hook = MsSqlOdbcHook(mssql_conn_id=self.mssql_conn_id)
        with ConnClosing(mssql_hook.get_conn()) as conn:
            cur = conn.cursor()
            cur.execute(cmd)
            result = cur.fetchone()

        if result is None or result[0] != 1:
            print("Retrying")
            return False

        print("Job Executed")
        return True


def check_day(**kwargs):
    execution_date = kwargs["data_interval_end"]
    run_date = execution_date
    if run_date.day == 20:
        return crm_passive_cancel.task_id
    elif run_date.day == 3:
        return ecom_passive_cancel.task_id
    else:
        return []


@retry_wrapper(3, Exception, sleep_time=1)
def fetch_cursor_result_mssql(cur: Any):
    try:
        return cur.fetchone()
    except Exception as ex:
        print(ex)
        cur.nextset()
        raise Exception


@retry_wrapper(3, Exception, sleep_time=60)
def run_ecom_mssql_proc_fn(mssql_conn_id):
    mssql_hook = MsSqlOdbcHook(mssql_conn_id=mssql_conn_id, database="ultramerchant")
    sql = """DECLARE @success int
        EXEC [dbo].[pr_monthly_ecom_passive_cancels_process] @success = @success OUTPUT
        SELECT @success as N'@success'"""
    try:
        with ConnClosing(mssql_hook.get_conn()) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            result = fetch_cursor_result_mssql(cur)
            print(result)
            if result[0] == 1:
                return membership_passive_cancels_to_s3.task_id
            else:
                return []
    except Exception as ex:
        print(ex)
        raise Exception


with dag:
    day_check = BranchPythonOperator(
        python_callable=check_day,
        task_id="check_day_branch",
    )

    crm_passive_cancel = SnowflakeProcedureOperator(
        procedure="membership.monthly_crm_passive_cancels.sql",
        database="reporting_prod",
    )

    ecom_passive_cancel = SnowflakeProcedureOperator(
        procedure="membership.monthly_ecom_passive_cancels.sql",
        database="reporting_prod",
    )

    for brand, brand_db_details in mssql_conn_id_server_dict.items():
        mssql_conn_id = brand_db_details["conn_id"]
        company_ids = brand_db_details["brand_id"]

        pre_sql_cmd_crm = "[dbo].[pr_monthly_CRM_eligible_passive_cancels_init]"

        export_cmd_crm = f"""
        select
            edw_prod.stg.udf_unconcat_brand(membership_id) as membership_id,
            true AS is_current_month,
            current_timestamp::timestamp_ntz AS datetime_added
        FROM reporting_prod.membership.monthly_crm_passive_cancels
        WHERE IS_PUSHED_DB50 IS NULL
            AND datetime_added IN (
                SELECT max(datetime_added)
                FROM reporting_prod.membership.monthly_crm_passive_cancels
                )
            AND RIGHT(membership_id, 2) IN ({company_ids})
        ORDER BY 1 DESC
        """

        post_sql_cmd_crm = f"""
            update reporting_prod.membership.monthly_crm_passive_cancels
            set IS_PUSHED_DB50=True
            where RIGHT(membership_id, 2) IN ({company_ids})
            AND membership_id between """

        export_cmd_db50_insert_crm = SnowflakeToMSSQL(
            pre_sql_cmd=pre_sql_cmd_crm,
            snowflake_cmd=export_cmd_crm,
            task_id=f"insert_task_crm_{brand}",
            mssql_target_table="monthly_CRM_eligible_passive_cancels",
            mssql_conn_id=mssql_conn_id,
            post_sql_cmd=post_sql_cmd_crm,
        )

        pre_sql_cmd_ecom = """select 1"""

        export_cmd_ecom = f"""
        SELECT
            edw_prod.stg.udf_unconcat_brand(membership_id) as membership_id,
            concat('monthly_passive_cancels_',
                to_varchar(current_date()::date,
                'yyyy_mm')) AS source_table
        FROM reporting_prod.membership.monthly_ecom_passive_cancels
        WHERE is_pushed_db50 IS NULL
            AND datetime_added in (
                SELECT max(datetime_added)
                FROM reporting_prod.membership.monthly_ecom_passive_cancels
                )
            AND RIGHT(membership_id, 2) IN ({company_ids})
        ORDER BY 1 DESC
        """

        post_sql_cmd_ecom = f"""
            update reporting_prod.membership.monthly_ecom_passive_cancels
            set IS_PUSHED_DB50=True
            where RIGHT(membership_id, 2) IN ({company_ids})
            AND membership_id between """

        export_cmd_db50_insert_ecom = SnowflakeToMSSQL(
            pre_sql_cmd=pre_sql_cmd_ecom,
            snowflake_cmd=export_cmd_ecom,
            task_id=f"insert_task_ecom_{brand}",
            mssql_target_table="membership_passive_cancels",
            mssql_conn_id=mssql_conn_id,
            post_sql_cmd=post_sql_cmd_ecom,
        )

        s3_key = (
            "lake/lake_consolidated.ultra_merchant.membership_passive_cancels/v2"
            f"/downgrade_status_{brand}/downgrade_status.csv.gz"
        )

        membership_passive_cancels_to_s3 = MsSqlToS3Operator(
            sql="""
            SELECT
                membership_id,
                downgrade_status
            FROM [ultramerchant].[dbo].[membership_passive_cancels] WITH (NOLOCK)
            WHERE [source_table] = 'monthly_passive_cancels_'
                + CAST(DATEPART(yyyy, GETDATE()) AS varchar(4))
                + '_' + RIGHT('00' + CAST(DATEPART(mm, GETDATE()) AS varchar(2)), 2);""",
            bucket=s3_buckets.tsos_da_int_inbound,
            key=s3_key,
            mssql_conn_id=mssql_conn_id,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            s3_replace=True,
            task_id=f"membership_passive_cancels_{brand}_to_s3",
        )

        update_downgrade_status_query = f"""
            CREATE OR REPLACE TEMP TABLE _monthly_passive_cancels
            (
                membership_id NUMBER(38,0),
                downgrade_status STRING
            );

            COPY INTO _monthly_passive_cancels (membership_id, downgrade_status)
                from (select $1, $2
                FROM '{stages.tsos_da_int_inbound}/{s3_key}')
                FILE_FORMAT=(
                    TYPE = CSV,
                    FIELD_DELIMITER = '\t',
                    RECORD_DELIMITER = '\n',
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
                    SKIP_HEADER = 0,
                    ESCAPE_UNENCLOSED_FIELD = NONE,
                    NULL_IF = ('')
                )
                ON_ERROR = 'SKIP_FILE_1%';

            UPDATE reporting_prod.membership.monthly_ecom_passive_cancels mecp
                SET mecp.downgrade_status = mpc.downgrade_status
            FROM _monthly_passive_cancels mpc
            WHERE right(mecp.membership_id, 2) in ({company_ids})
                AND mpc.membership_id = edw_prod.stg.udf_unconcat_brand(mecp.membership_id)
                AND mecp.is_current_month = 1;"""

        update_downgrade_status = SnowflakeSqlOperator(
            task_id=f"update_downgrade_status_{brand}",
            sql_or_path=update_downgrade_status_query,
        )

        run_ecom_mssql_proc_job = RunSqlAgent(
            task_id=f"ecom_mssql_job_{brand}",
            mssql_conn_id=mssql_conn_id,
            mssql_job_name="Membership Passive Cancels",
            wrapper_proc="sp_start_job",
        )
        check_job_completion = CheckEcomJobCompletionSensor(
            task_id=f"check_job_completion_{brand}",
            mssql_conn_id=mssql_conn_id,
        )

        day_check >> [crm_passive_cancel, ecom_passive_cancel]
        crm_passive_cancel >> export_cmd_db50_insert_crm

        chain_tasks(
            ecom_passive_cancel,
            export_cmd_db50_insert_ecom,
            run_ecom_mssql_proc_job,
            check_job_completion,
            membership_passive_cancels_to_s3,
            update_downgrade_status,
        )
