from datetime import timedelta

import pandas as pd
import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_to_mssql import SnowflakeSqlToMSSqlOperatorTruncateAndLoad
from include.config import email_lists, owners, snowflake_roles, conn_ids
from include.utils import snowflake
from include.utils.context_managers import ConnClosing

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.global_apps_analytics,
    'email': email_lists.global_applications,
    'on_failure_callback': slack_failure_gsc,
    'execution_timeout': timedelta(hours=3),
}


dag = DAG(
    dag_id='global_apps_gsc_reporting_daily',
    default_args=default_args,
    schedule='7 5 * * *',
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    doc_md=__doc__,
)


class SnowflakeToMSSql(SnowflakeSqlToMSSqlOperatorTruncateAndLoad):
    def execute(self, context=None):
        with ConnClosing(self.snowflake_hook.get_conn()) as conn:
            query_tag = snowflake.generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = conn.cursor()
            cur.execute(query_tag)
            sql = self.get_sql_cmd(self.sql_or_path)
            with self.mssql_hook.get_sqlalchemy_connection() as conn_mssql:
                conn_mssql.execute(self.sql_truncate)
            for rows in pd.read_sql_query(sql=sql, con=conn, chunksize=20000):
                with self.mssql_hook.get_sqlalchemy_connection() as conn_mssql:
                    rows.to_sql(
                        name=self.tgt_table,
                        schema=self.tgt_schema,
                        con=conn_mssql,
                        chunksize=20000,
                        if_exists=self.if_exists,
                        index=False,
                    )


with dag:
    gsc_sku_origin_po = SnowflakeProcedureOperator(
        procedure='gsc.sku_origin_po.sql',
        database='reporting_base_prod',
        autocommit=False,
    )

    gsc_shipped_items = SnowflakeProcedureOperator(
        procedure='gsc.shipped_items_data.sql',
        database='reporting_base_prod',
        autocommit=False,
    )

    gsc_style_material_content = SnowflakeProcedureOperator(
        procedure='gsc.style_material_content.sql',
        database='reporting_base_prod',
        autocommit=False,
    )

    gfc_fc_order_analysis = SnowflakeProcedureOperator(
        procedure='gfc.fc_order_analysis.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfc_fpa_report_dataset = SnowflakeProcedureOperator(
        procedure='gfc.fpa_reporting_dataset.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gsc_pol_pod_transit_dataset = SnowflakeProcedureOperator(
        procedure='gsc.pol_pod_transit_dataset.sql',
        database='reporting_base_prod',
        autocommit=False,
    )

    gfc_uturn_dataset = SnowflakeProcedureOperator(
        procedure='gfc.uturn_dataset.sql',
        database='reporting_prod',
        autocommit=False,
    )

    pulse_comment_data_to_mssql = SnowflakeToMSSql(
        task_id='pulse_comment_data_to_mssql',
        sql_or_path='select * from reporting_base_prod.gsc.pulse_comment_data',
        tgt_database='ssrs_reports',
        tgt_schema='gsc',
        tgt_table='pulse_comment_data',
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow_rw,
        role=snowflake_roles.etl_service_account,
        if_exists='append',
    )

    [
        gsc_style_material_content,
        gsc_sku_origin_po,
        gsc_shipped_items,
        gfc_fc_order_analysis,
        gfc_fpa_report_dataset,
        gsc_pol_pod_transit_dataset,
        gfc_uturn_dataset,
        pulse_comment_data_to_mssql,
    ]
