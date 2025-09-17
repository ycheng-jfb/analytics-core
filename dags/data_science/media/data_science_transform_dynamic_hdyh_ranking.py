from dataclasses import dataclass

import pendulum
from airflow.models import DAG, BaseOperator

from include.airflow.callbacks.slack import slack_failure_data_science
from include.airflow.dag_helpers import chain_tasks
from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_to_mssql import SnowflakeSqlToMSSqlOperator
from include.config import conn_ids, owners

default_args = {
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    'retries': 1,
    'owner': owners.data_science,
    "email": 'datascience@techstyle.com',
    "on_failure_callback": slack_failure_data_science,
}

dag = DAG(
    dag_id="data_science_transform_dynamic_hdyh_ranking",
    default_args=default_args,
    schedule="0 */2 * * *",
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)


class SqlOp(BaseOperator):
    def __init__(self, mssql_conn_id, **kwargs):
        super().__init__(**kwargs)
        self.mssql_conn_id = mssql_conn_id

    def execute(self, context=None):
        ms_hook = MsSqlOdbcHook(mssql_conn_id=self.mssql_conn_id, database='ultraimport')
        truncate_statement = """execute pr_snowflake_hdyh_ranking_trunc"""
        with ms_hook.get_conn() as cnx:
            cnx.execute(truncate_statement)


@dataclass
class ExportConfig:
    """
    Config for push-to-mssql component
    """

    server: str
    mssql_conn_id: str

    @property
    def to_mssql_operator(self):
        return SnowflakeSqlToMSSqlOperator(
            task_id=f"push_to_{self.server}",
            sql_or_path="""
                        SELECT
                            hdyh_name,
                            media_partner_id,
                            update_datetime,
                            store_group_id,
                            group_name
                        FROM
                            reporting_media_prod.public.hdyh_ranking_prod
                        ORDER BY
                            store_group_id,
                            group_name
                    """,
            mssql_conn_id=self.mssql_conn_id,
            tgt_table="snowflake_hdyh_ranking",
            tgt_schema="dbo",
            tgt_database="ultraimport",
            if_exists="append",
        )


with dag:
    campaign_activity_aggregate = SnowflakeProcedureOperator(
        procedure="creatoriq.campaign_activity_aggregate.sql",
        database="reporting_media_base_prod",
        autocommit=False,
    )
    campaign_activity_modeled = SnowflakeProcedureOperator(
        procedure="influencers.campaign_activity_modeled.sql",
        database="reporting_media_base_prod",
    )

    daily_performance_by_influencer_with_post_data = SnowflakeProcedureOperator(
        database='reporting_media_base_prod',
        procedure='influencers.daily_performance_by_influencer_with_post_data.sql',
    )

    influencer_impact_score_daily_sxna = SnowflakeProcedureOperator(
        procedure="public.influencer_impact_score_daily_sxna.sql",
        database="reporting_media_base_prod",
    )

    hdyh_dynamic_ranking_sxna = SnowflakeProcedureOperator(
        procedure="public.hdyh_dynamic_ranking_sxna.sql",
        database="reporting_media_base_prod",
    )

    influencer_impact_score_daily_flna = SnowflakeProcedureOperator(
        procedure="public.influencer_impact_score_daily_flna.sql",
        database="reporting_media_base_prod",
    )

    hdyh_dynamic_ranking_flna = SnowflakeProcedureOperator(
        procedure="public.hdyh_dynamic_ranking_flna.sql",
        database="reporting_media_base_prod",
    )

    hdyh_answers_historical = SnowflakeProcedureOperator(
        procedure="influencers.hdyh_answers_historical.sql",
        database="reporting_media_base_prod",
    )
    hdyh_ranking_prod = SnowflakeProcedureOperator(
        procedure="public.hdyh_ranking_prod.sql",
        database="reporting_media_prod",
    )

    truncate_table_db166 = SqlOp(
        task_id='truncate_db166', mssql_conn_id=conn_ids.MsSql.db166_app_airflow_rw
    )

    copy_to_sql_db166 = ExportConfig(
        server='db166', mssql_conn_id=conn_ids.MsSql.db166_app_airflow_rw
    )

    truncate_table_db50 = SqlOp(
        task_id='truncate_db50', mssql_conn_id=conn_ids.MsSql.db50_app_airflow_rw
    )

    copy_to_sql_db50 = ExportConfig(server='db50', mssql_conn_id=conn_ids.MsSql.db50_app_airflow_rw)

    truncate_table_fabletics = SqlOp(
        task_id='truncate_fabletics', mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow_rw
    )
    copy_to_sql_fabletics = ExportConfig(
        server='fabletics', mssql_conn_id=conn_ids.MsSql.fabletics_app_airflow_rw
    )

    truncate_table_justfab = SqlOp(
        task_id='truncate_justfab', mssql_conn_id=conn_ids.MsSql.justfab_app_airflow_rw
    )

    copy_to_sql_justfab = ExportConfig(
        server='justfab', mssql_conn_id=conn_ids.MsSql.justfab_app_airflow_rw
    )

    truncate_table_savagex = SqlOp(
        task_id='truncate_savagex', mssql_conn_id=conn_ids.MsSql.savagex_app_airflow_rw
    )

    copy_to_sql_savagex = ExportConfig(
        server='savagex', mssql_conn_id=conn_ids.MsSql.savagex_app_airflow_rw
    )

    chain_tasks(
        campaign_activity_aggregate,
        campaign_activity_modeled,
        daily_performance_by_influencer_with_post_data,
        [influencer_impact_score_daily_sxna, influencer_impact_score_daily_flna],
        [hdyh_dynamic_ranking_sxna, hdyh_dynamic_ranking_flna],
        hdyh_answers_historical,
        hdyh_ranking_prod,
        truncate_table_db50,
        copy_to_sql_db50.to_mssql_operator,
        truncate_table_db166,
        copy_to_sql_db166.to_mssql_operator,
        truncate_table_fabletics,
        copy_to_sql_fabletics.to_mssql_operator,
        truncate_table_justfab,
        copy_to_sql_justfab.to_mssql_operator,
        truncate_table_savagex,
        copy_to_sql_savagex.to_mssql_operator,
    )
