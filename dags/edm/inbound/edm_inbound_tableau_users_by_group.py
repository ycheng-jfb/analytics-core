import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake_load import SnowflakeTruncateAndLoadOperator
from include.airflow.operators.tableau import TableauUsersByGroupsToS3Operator
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages
from include.utils.snowflake import Column, CopyConfigCsv

default_args = {
    'start_date': pendulum.datetime(2020, 12, 11, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id='edm_inbound_tableau_users_by_group',
    default_args=default_args,
    schedule='0 10,14 * * *',
    catchup=False,
    max_active_runs=1,
)

database = 'lake'
schema = 'tableau'
table = 'users_by_group'
yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
s3_prefix = f'lake/{database}.{schema}.{table}/v2'

column_list = [
    Column('group_name', 'VARCHAR', source_name='grp_name'),
    Column('group_id', 'VARCHAR', source_name='grp_id', uniqueness=True),
    Column('user_name', 'VARCHAR', source_name='usr_name'),
    Column('user_id', 'VARCHAR', source_name='usr_id', uniqueness=True),
    Column('user_auth_setting', 'VARCHAR', source_name='usr_authSetting'),
    Column('user_last_login', 'TIMESTAMP_LTZ(3)', source_name='usr_lastLogin'),
    Column('user_site_role', 'VARCHAR', source_name='usr_siteRole'),
    Column('user_language', 'VARCHAR', source_name='usr_language'),
    Column('user_locale', 'VARCHAR', source_name='usr_locale'),
    Column('user_external_auth_user_id', 'VARCHAR', source_name='usr_externalAuthUserId'),
    Column('updated_at', 'TIMESTAMP_LTZ(3)', source_name='updated_at', delta_column=True),
]

with dag:
    tableau_to_s3 = TableauUsersByGroupsToS3Operator(
        task_id='tableau_users_by_group_tableau',
        key=f'{s3_prefix}/{yr_mth}/{schema}_{table}_{{{{ ts_nodash }}}}.csv.gz',
        bucket=s3_buckets.tsos_da_int_inbound,
        column_list=[x.source_name for x in column_list],
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
    )
    to_snowflake = SnowflakeTruncateAndLoadOperator(
        task_id='tableau_users_by_group_snowflake',
        database=database,
        schema=schema,
        table=table,
        staging_database='lake_stg',
        view_database='lake_view',
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list,
        files_path=f'{stages.tsos_da_int_inbound}/{s3_prefix}/',
        copy_config=CopyConfigCsv(field_delimiter='\t'),
    )

    tableau_to_s3 >> to_snowflake
