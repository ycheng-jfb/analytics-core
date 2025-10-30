import datetime
import os
import re
import warnings
from copy import deepcopy
from datetime import date
from functools import cached_property
from inspect import cleandoc
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pendulum
import yaml
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import BaseOperator, Variable
from airflow.utils.email import send_email
from airflow.utils.file import list_py_file_paths
from include.config import snowflake_roles
from dateutil.relativedelta import relativedelta
from include import CONFIG_DIR, DAGS_DIR, SQL_DIR
from include.airflow.callbacks.slack import send_slack_message
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.watermark import (
    BaseProcessWatermarkOperator,
    BaseTaskWatermarkOperator,
)
from include.config import stages, conn_ids
from include.utils.snowflake import (
    generate_query_tag_cmd,
    get_snowflake_warehouse,
    set_database,
    split_statements,
)
from include.utils.string import indent, unindent_auto
from tabulate import tabulate

DATABASE_CONFIG_KEY = 'database_config'
ENV_ENABLED_KEY = 'env_config_enabled'
DAG_TO_DB_CONFIG_FILE = CONFIG_DIR / "dag_to_db_map.yml"
FOLDER_TO_DB_CONFIG_FILE = CONFIG_DIR / "folder_to_db_map.yml"
dag_locs: Dict[str, str] = {}


def get_dag_location(dag_id):
    global dag_locs
    if dag_id in dag_locs:
        return dag_locs[dag_id]
    for path in list_py_file_paths(DAGS_DIR, False, False):
        path_obj = Path(path)
        pattern = rf"dag_id=('|\"){dag_id}('|\")"
        if re.search(pattern, path_obj.read_text()):
            return path_obj.as_posix()
    raise Exception(f"no dag file found for dag_id '{dag_id}'")


def get_database_config(dag_id: str, dag_folder: Optional[str]):
    """
    Merges all database config files and returns the final config

    Follows the below priority to pick database value if it is exists in more than one file
        1. Variable config
        2. dag_to_db_map config
        3. folder_to_db_map config

    Args:
        dag_id: dag_id to get dag_id specific database config
        dag_folder: Folder name of the dag. Relative path from ROOT DIR. E.g. 'dags/edm/mart'
    """
    dag_config: Dict[str, Any] = {}
    folder_config: Dict[str, Any] = {}
    variable_config = Variable.get(DATABASE_CONFIG_KEY, deserialize_json=True, default_var={})
    if dag_id:
        with open(DAG_TO_DB_CONFIG_FILE, "r") as file:
            dag_yaml = yaml.load(stream=file, Loader=yaml.FullLoader)
        if dag_id in dag_yaml:
            dag_config = dag_yaml[dag_id]
        if not dag_folder or not Path(dag_folder).exists():
            warnings.warn("no valid dag folder supplied to `get_database_config`")
            dag_folder = get_dag_location(dag_id)
        with open(FOLDER_TO_DB_CONFIG_FILE, "r") as file:
            for rel_path, config in yaml.load(stream=file, Loader=yaml.FullLoader).items():
                if rel_path == '*':
                    folder_config.update(**config)
                elif not dag_folder:
                    continue
                else:
                    config_path = (DAGS_DIR / rel_path).as_posix()
                    dag_path = Path(dag_folder).as_posix()
                    if dag_path.startswith(config_path):
                        folder_config.update(**config)
    db_config = {**folder_config, **dag_config, **variable_config}
    return db_config


def _get_effective_database(database, dag_file_path, dag_id):
    env_enabled = str(Variable.get(key=ENV_ENABLED_KEY, default_var='true')).lower() == 'true'
    if env_enabled:
        if not dag_file_path:
            dag_folder = None
        else:
            dag_folder = os.path.exists(dag_file_path) and os.path.dirname(dag_file_path) or None
        config = get_database_config(dag_id, dag_folder)
        return config.get(database, f"{database}")
    else:
        return database


def get_effective_database(database, task):
    """
    Given the target `database` this function returns the configured database.

    When the target is not configured, the default behavior is to append `_dev`.

    Args:
        database: name of snowflake production database
        task: instance of the class
    """
    if database is None:
        return
    dag_id = None
    dag_file_path = None
    if task.has_dag():
        dag_id = task.dag_id
        dag_file_path = task.dag.fileloc
    return _get_effective_database(database=database, dag_file_path=dag_file_path, dag_id=dag_id)


class BaseSnowflakeOperator(BaseOperator):
    """
    Abstract base class for interacting with snowflake computing.
    Has helper methods and allows setting of parameters to override those in the connection.
    """

    ui_color = "#ededed"

    def __init__(
        self,
        snowflake_conn_id=conn_ids.Snowflake.default,
        parameters=None,
        autocommit=True,
        warehouse=None,
        database=None,
        schema=None,
        role=None,
        timezone=None,
        max_active_tis_per_dag=1,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.timezone = timezone
        self.max_active_tis_per_dag = max_active_tis_per_dag
        self.cnx = None

    def get_sql_cmd(self, sql_or_path):
        try:
            if Path(sql_or_path).exists() or isinstance(sql_or_path, Path):
                with open(sql_or_path.as_posix(), "rt") as f:
                    sql = f.read()
            else:
                sql = sql_or_path
        except OSError as e:
            if e.strerror == "File name too long":
                sql = sql_or_path
            else:
                raise e
        return sql

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=get_effective_database(self.database, self),
            role=self.role,
            schema=self.schema,
            timezone=self.timezone,
        )

    def run_sql_or_path(self, sql_or_path, autocommit=None, parameters=None):
        query_tag = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        sql = self.get_sql_cmd(sql_or_path=sql_or_path)
        sql = query_tag + sql
        with self.snowflake_hook.get_conn() as cnx:
            self.cnx = cnx  # we store under self.cnx so that we can use on_kill
            self.cnx.autocommit(autocommit or self.autocommit)
            self.snowflake_hook.execute_multiple_with_cnx(
                cnx=cnx, sql=sql, parameters=parameters or self.parameters
            )

    def run_sql_or_path_with_cnx(self, cnx, sql_or_path, parameters=None):
        query_tag = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        sql = self.get_sql_cmd(sql_or_path=sql_or_path)
        sql = query_tag + sql
        self.log.info('Executing: %s', sql)
        self.snowflake_hook.execute_multiple_with_cnx(
            cnx=cnx, sql=sql, parameters=parameters or self.parameters
        )

    def on_kill(self):
        if self.cnx:
            self.cnx.execute_string(f"SELECT SYSTEM$CANCEL_ALL_QUERIES({self.cnx.session_id})")
            self.cnx.execute_string("ROLLBACK")

    def execute(self, context=None):
        raise NotImplementedError


class SnowflakeSqlOperator(BaseSnowflakeOperator):
    """
    Executes sql cmd or file on snowflake
    Args:
        snowflake_conn_id: reference to specific snowflake connection id
        sql_or_path: the sql code to be executed or path to sql file.
    """

    template_fields = ["sql_or_path", "parameters", "snowflake_conn_id"]
    template_ext = (".sql",)

    def __init__(self, sql_or_path: Union[str, Path], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_or_path = sql_or_path

    def execute(self, context=None):
        self.run_sql_or_path(self.sql_or_path)

    def dry_run(self) -> None:
        print(self.get_sql_cmd(self.sql_or_path))


class SnowflakeAlertOperator(BaseSnowflakeOperator):
    """
    Used to send alerts to different channels.
    Args:
        sql_or_path: the sql code to be executed or path to sql file. (templated)
        subject: subject of the message.
        body: body of the message.
        distribution_list: distribution list for email alerts.
        slack_conn_id: slack connection id.
        slack_channel_name: slack channel name to post the alerts message.
        snowflake_conn_id: Reference to specific snowflake connection id
        alert_type: alert type [mail,user_notification and slack]. default alert type is "mail".
        database: name of database, e.g. 'reporting'
        pass_sql_to_message: set to true, if the sql should also be printed in the alter message along with the sql result.
        downstream_action: downstream action [skip, fail, None]. default downstream_action is None which will continue downstream tasks on alerts

    Example:
        Sample SQL query which will throw defined alert when we receive any row in result
        ::
            SELECT *
            FROM (
            SELECT 'order' AS tbl_name, MAX(order_local_datetime) AS max_date FROM edw_prod.data_model.fact_order
            UNION
            SELECT 'order_line' AS tbl_name, MAX(order_local_datetime) AS max_date FROM edw_prod.data_model.fact_order_line
            )
            WHERE max_date < GETDATE()::date;
    """

    template_fields = ("sql", "subject")
    template_ext = (".sql",)

    def __init__(
        self,
        sql_or_path: Union[str, Path],
        subject: str,
        body: str,
        distribution_list: List[str],
        slack_conn_id: str = conn_ids.SlackAlert.media,
        slack_channel_name: str = "airflow-alerts-media",
        snowflake_conn_id: str = conn_ids.Snowflake.default,
        alert_type: str | list = "mail",
        database: str = None,
        pass_sql_to_message: bool = False,
        downstream_action: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql_or_path
        self.subject = subject
        self.body = body
        self.distribution_list = distribution_list
        self.alert_type = alert_type
        self.database = database
        self.slack_conn_id = slack_conn_id
        self.slack_channel_name = slack_channel_name
        self.pass_sql_to_message = pass_sql_to_message
        self.downstream_action = downstream_action

    @staticmethod
    def _includes(required_alert_type, operator_alert_type):
        if isinstance(operator_alert_type, list):
            return required_alert_type in operator_alert_type
        elif isinstance(operator_alert_type, str):
            return required_alert_type == operator_alert_type
        else:
            raise TypeError(f"{operator_alert_type} is not a instance of 'str' or 'list'")

    def check_for_downstream_action(self):
        if self.downstream_action == "fail":
            raise AirflowFailException("Alert sent failing task")
        elif self.downstream_action == "skip":
            raise AirflowSkipException("Alert sent skipping current and downstream tasks")
        else:
            print("Alert sent continuing downstream tasks")

    def execute(self, context=None):
        self.log.info("Executing: %s", self.sql)
        with self.snowflake_hook.get_conn() as cnx:
            query_tag = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
            set_db_query = set_database(database=self.database)
            sql = self.get_sql_cmd(sql_or_path=self.sql)
            if set_db_query is not None:
                query = query_tag + set_db_query + sql
            else:
                query = query_tag + sql
            cur = cnx.cursor()
            for cmd in split_statements(query):
                cur.execute(cmd)
            last_rows = cur.fetchall()
            self.log.info(last_rows)
            if len(last_rows) > 0:
                self.log.info("found rows; sending alert.")
                column_list = [x[0] for x in cur.description]
                df = pd.DataFrame(data=last_rows, columns=column_list)
                html_content = f"{self.body}<br><br>{df.to_html(index=False)}"
                if self._includes("mail", self.alert_type):
                    send_email(
                        to=self.distribution_list, subject=self.subject, html_content=html_content
                    )
                if self._includes("user_notification", self.alert_type):
                    list_of_emails = []
                    for index, row in df.iterrows():
                        email = str(row['USER_NAME'])
                        if email not in list_of_emails:
                            list_of_emails.append(email)
                    for email in list_of_emails:
                        to_email = email
                        subject = self.subject.replace('USERNAME', to_email)
                        html_content = f"{self.body.replace('USERNAME', to_email)}"
                        user_df = df.loc[df['USER_NAME'] == to_email]
                        html_content += f'<br><br>{user_df.to_html(index=False)}'
                        to_email = to_email.lower()
                        print(to_email)
                        print(subject)
                        print(html_content)
                        send_email(to=to_email, subject=subject, html_content=html_content)
                if self._includes("slack", self.alert_type):
                    sql_result = tabulate(
                        df.iloc[:10],
                        headers=[i[0] for i in cur.description],
                        tablefmt='orgtbl',
                        showindex=False,
                    )
                    if not self.pass_sql_to_message:
                        sql = ''
                    message = f"""
                            :no_entry: Task *{self.task_id}* <!here>
*DAG*: {self.dag_id}
*{self.subject}*
_{self.body} : {len(last_rows)}_\n```{sql}\n{sql_result}```
                        """
                    send_slack_message(message=cleandoc(message), conn_id=self.slack_conn_id)

                self.check_for_downstream_action()
            else:
                self.log.info("no rows retrieved; skipping alert.")


class SnowflakeAlertComplexEmailOperator(BaseSnowflakeOperator):
    template_fields = ("sql",)
    template_ext = (".sql",)

    def __init__(
        self,
        sql_or_path_list: List[Union[str, Path]],
        subject: str,
        body_list: List[str],
        distribution_list: List[str],
        snowflake_conn_id: str = conn_ids.Snowflake.default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql_or_path_list
        self.subject = subject
        self.body = body_list
        self.distribution_list = distribution_list

    def execute(self, context=None):
        df_list = []
        for sql in self.sql:
            self.log.info("Executing: %s", sql)
            sql_cmd = self.get_sql_cmd(sql_or_path=sql)
            df = self.snowflake_hook.get_pandas_df(sql_cmd)
            df_list.append(df)
            self.log.info(df.to_string)

        html_content = ''
        for i in range(len(df_list)):
            if not df_list[i].empty:
                self.log.info(f"found rows in table {i}; sending alert.")
            html_content += f"{self.body[i]}<br><br>{df_list[i].to_html(index=False)}<br><br><br>"

        if html_content != '':
            send_email(to=self.distribution_list, subject=self.subject, html_content=html_content)
        else:
            self.log.info("no rows retrieved; skipping alert.")


class BaseDependency:
    @property
    def high_watermark_command(self) -> str:
        raise NotImplementedError


class TableDependencyTzLtz(BaseDependency):
    """
    Use when source table has data type TIMESTAMP_TZ or TIMESTAMP_LTZ
    """

    def __init__(self, table_name: str, column_name: str = "meta_update_datetime"):
        self.table_name = table_name
        self.column_name = column_name

    @property
    def high_watermark_command(self) -> str:
        """
        Must return zoned timestamp
        """
        hwm_command = f"""
            SELECT
                max({self.column_name}) AS high_watermark
            FROM {self.table_name}
        """
        return unindent_auto(hwm_command)

    def __eq__(self, other):
        return self.table_name == other.table_name and self.column_name == other.column_name


class TableDependencyNtz(BaseDependency):
    """
    Use when source table has data type TIMESTAMP_NTZ
    """

    def __init__(self, table_name: str, timezone: str, column_name: str = "meta_update_datetime"):
        self.table_name = table_name
        self.column_name = column_name
        self.timezone = timezone

    @property
    def high_watermark_command(self) -> str:
        """
        Must return zoned timestamp
        """
        hwm_command = f"""
            SELECT
                lake.public.udf_to_timestamp_tz(max({self.column_name}), '{self.timezone}') AS high_watermark
            FROM {self.table_name}
        """
        return unindent_auto(hwm_command)

    def __eq__(self, other):
        return (
            self.table_name == other.table_name
            and self.column_name == other.column_name
            and self.timezone == other.timezone
        )


class SnowflakeWatermarkSqlOperator(BaseTaskWatermarkOperator, SnowflakeSqlOperator):
    """
    Deprecated -- use :class:`~.SnowflakeProcedureOperator`.
    Used for watermark-based incremental processes.
    Given a list of dependencies, will calculate high_watermark value at start of run, and store
    to metastore after successful run.
    In each run, high watermark from last successful run is passed as parameter ``low_watermark``.
    Args:
        sql_or_path: the sql code to be executed or path to sql file. (templated)
        snowflake_conn_id: Reference to specific snowflake connection id
        watermark_tables: List of dependent snowflake tables to get the high_watermark value for each run.
        initial_load_value: value used as ``low_watermark`` on first run.
        snapshot_enabled: if a snapshot is required for the table -True else False
        snapshot_period: Interval for which the snapshot is to be maintained in months.(default value = 6)
    """

    def __init__(
        self,
        sql_or_path: Union[str, Path],
        snowflake_conn_id=conn_ids.Snowflake.default,
        parameters=None,
        autocommit=True,
        warehouse=None,
        database=None,
        schema=None,
        role=None,
        max_active_tis_per_dag=1,
        watermark_tables: List[Union[str, dict, TableDependencyTzLtz, TableDependencyNtz]] = None,
        snapshot_enabled: bool = False,
        snapshot_period: int = None,
        initial_load_value: str = "1900-01-01 00:00:00+00:00",
        **kwargs,
    ):
        super().__init__(
            sql_or_path=sql_or_path,
            snowflake_conn_id=snowflake_conn_id,
            parameters=parameters,
            autocommit=autocommit,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            max_active_tis_per_dag=max_active_tis_per_dag,
            initial_load_value=initial_load_value,
            **kwargs,
        )
        self._watermark_tables = watermark_tables
        self.snapshot_enabled = snapshot_enabled
        self.snapshot_period = snapshot_period
        if self.snapshot_enabled is False and self.snapshot_period is not None:
            raise ValueError("'snapshot_period' only works when 'snapshot_enabled' is True ")

    @cached_property
    def watermark_tables(self):
        if not self._watermark_tables:
            return None
        dep_list = []
        for dep in self._watermark_tables:
            if isinstance(dep, str):
                dep_list.append(TableDependencyTzLtz(dep))
            elif isinstance(dep, dict):
                if 'timezone' in dep:
                    dep_list.append(TableDependencyNtz(**dep))
                else:
                    dep_list.append(TableDependencyTzLtz(**dep))
            elif issubclass(dep.__class__, BaseDependency):
                dep_list.append(dep)
            else:
                raise ValueError(f"Invalid watermark table provided: {dep}")
        return dep_list

    def update_parameters(self, new_parameters: dict):
        if not self.parameters:
            self.parameters = new_parameters
        else:
            self.parameters.update(new_parameters)

    def build_high_watermark_command(self):
        command_list = []
        for dep in deepcopy(self.watermark_tables):
            database, schema, table_name = dep.table_name.split('.')
            effective_database = get_effective_database(database, self)
            dep.table_name = f"{effective_database}.{schema}.{table_name}"
            command_list.append(dep.high_watermark_command)
        union = "\nUNION\n".join(command_list)
        cmd = f"""
            SELECT min($1)
            FROM ({indent(union, 4)}
        ) a
        """
        return unindent_auto(cmd)

    def get_high_watermark(self):
        hwm_command = self.build_high_watermark_command()
        cur = self.snowflake_hook.get_cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        cur.execute(hwm_command)
        result = cur.fetchall()
        val = result[0][0]
        if isinstance(val, datetime.datetime):
            print(val)
            if val.utcoffset() is None:
                raise Exception("watermark query must return object with tzinfo")
            val_pendulum = pendulum.instance(val)
            return val_pendulum.isoformat()
        else:
            raise Exception(
                "at present only datetime return values supported by watermark sql operator"
            )

    def generate_sql_for_snapshot(self) -> str:
        schema_name = self.procedure.split('.')[0]
        table_name = self.procedure.split('.')[1]
        cmd = rf"""
        CREATE TRANSIENT TABLE IF NOT EXISTS {self.database}.{schema_name}.{table_name}_snapshot
            LIKE {self.database}.{schema_name}.{table_name};

        ALTER TABLE {self.database}.{schema_name}.{table_name}_snapshot
            ADD COLUMN IF NOT EXISTS snapshot_date date;

        INSERT INTO {self.database}.{schema_name}.{table_name}_snapshot
            SELECT
                *,
                '{date.today().strftime('%Y-%m-%d')}' as snapshot_date
            FROM {self.database}.{schema_name}.{table_name};

        DELETE FROM {self.database}.{schema_name}.{table_name}_snapshot
        WHERE snapshot_date < '{date.today() - relativedelta(months=self.snapshot_period)}';"""
        return unindent_auto(cmd)

    def watermark_execute(self, context=None):
        self.run_sql_or_path(self.sql_or_path)

    def snapshot_execute(self):
        sql_snapshot = self.generate_sql_for_snapshot()
        self.run_sql_or_path(sql_snapshot)

    def dry_run(self) -> None:
        if self.watermark_tables:
            hwm_command = self.build_high_watermark_command()
            print(hwm_command)
        print(self.get_sql_cmd(self.sql_or_path))
        if self.snapshot_enabled:
            print(self.generate_sql_for_snapshot())
        else:
            print("snapshot not enabled")

    def execute(self, context=None):
        if self.watermark_tables:
            self.watermark_pre_execute()
            self.update_parameters({"low_watermark": self.low_watermark})
        self.watermark_execute(context=context)
        if self.snapshot_enabled:
            self.snapshot_execute()
        if self.watermark_tables:
            self.watermark_post_execute()


class SnowflakeProcedureOperator(BaseProcessWatermarkOperator, SnowflakeWatermarkSqlOperator):
    """

    In our airflow implementation, we store "procedures" in scripts located as follows:

    .. code:: text

        sql/<database>/procedures/<procedure name>

    This operator aims to simplify the dag writing process when calling a sproc.
    You just need to give it the database name and procedure name, and dependency list if
    applicable.
    For example:

    .. code:: python

        genesys_segment = SnowflakeProcedureOperator(
            database='reporting',
            procedure='gms.genesys_segment.sql',
            watermark_tables=[
                TableDependencyTzLtz(
                    table_name="lake.genesys.conversations", column_name='meta_update_datetime',
                )
            ],
        )

    In this example, the operator will call the script located here:

    .. code:: text

        sql/reporting/procedures/gms.genesys_segment.sql

    And the task_id will be ``reporting.gms.genesys_segment.sql``.

    Args:
        procedure: the script name in the procedures folder in database.
            e.g. 'sps.carrier_milestone.sql'

        database: name of database, e.g. 'reporting'
        watermark_tables: list of :class:`~.BaseDependency`.  If list of string table names
            is provided, will instantiate as :class:`~.TableDependencyTzLtz`.
        snapshot_enabled: if a snapshot is required for the table -True else False
        snapshot_period: Interval for which the snapshot is to be maintained in months.(default value = 6)
        role: snowflake role

    """

    ui_fgcolor = '#000000'
    ui_color = '#F6D6BF'

    def __init__(
        self,
        procedure,
        database,
        warehouse=None,
        schema='public',
        watermark_tables: List[Union[str, dict, TableDependencyTzLtz, TableDependencyNtz]] = None,
        role=snowflake_roles.etl_service_account,
        **kwargs,
    ):
        process_name = f"{database}.{procedure}"
        namespace = "snowflake-procedure"
        script_filename = Path(SQL_DIR, database, 'procedures', procedure)
        self.procedure = procedure
        super().__init__(
            task_id=f"{database}.{script_filename.name}".lower(),
            sql_or_path=script_filename,
            watermark_tables=watermark_tables,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            namespace=namespace,
            process_name=process_name,
            **kwargs,
        )


class SnowflakeEdwProcedureOperator(SnowflakeProcedureOperator):
    def execute(self, context=None):
        self.warehouse = get_snowflake_warehouse(context)
        if self.watermark_tables:
            self.watermark_pre_execute()
            self.update_parameters({"low_watermark": self.low_watermark})
        self.watermark_execute(context=context)
        if self.watermark_tables:
            self.watermark_post_execute()


class SnowflakeBackupTableToS3(BaseSnowflakeOperator):
    """
    Backup table to S3 storage. Default location is defined if no file_path is passed.

    Args:
        database: database for table back up
        schema: schema for table back up
        table: table to back up
        delete_after_backup: flag to delete records after they are backed up
        file_type: file format, options are (CSV | JSON | PARQUET)
        field_delimiter: delimiter for fields
        record_delimiter: delimiter for records
        overwrite: flag to allow overwriting an already existing file at s3_key location
        header: flag to include header record in file
        custom_column_list: list of column names only to include in file from table
        where_clause: where clause to filter records in table, needs to begin with 'WHERE'
        custom_file_format: dictionary to completely replace format params
        custom_copy_params: dictionary to completely replace copy params
    """

    template_fields = ["where_clause", "snowflake_conn_id"]
    TRANS_DELIM = str.maketrans({'\t': r'\t', '\n': r'\n', '\r': r'\r'})

    def __init__(
        self,
        database,
        schema,
        table,
        delete_after_backup=False,
        file_type='CSV',
        field_delimiter='\t',
        record_delimiter='\n',
        overwrite=False,
        header=True,
        column_list: List = None,
        where_clause: str = '',
        custom_file_format: Dict = None,
        custom_copy_params: Dict = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.database = database
        self.schema = schema
        self.table = table
        self.delete_after_backup = delete_after_backup
        self.file_type = file_type
        self.column_list = column_list
        self.field_delimiter = field_delimiter.translate(self.TRANS_DELIM)
        self.record_delimiter = record_delimiter.translate(self.TRANS_DELIM)
        self.overwrite = overwrite
        self.header = header
        self.where_clause = where_clause
        self.custom_file_format = custom_file_format
        self.custom_file_format = custom_copy_params

    @staticmethod
    def timestamp_filter_where_clause(timestamp_col, days_back):
        now = datetime.datetime.now()
        past = now - datetime.timedelta(days=days_back)
        return f"WHERE {timestamp_col} < '{past.strftime('%Y-%m-%d %H:%M:%S')}'"

    @property
    def file_format_params_str(self):
        if self.custom_file_format:
            params = self.custom_file_format
        else:
            params = {
                "TYPE": f"{self.file_type}",
                "FIELD_DELIMITER": f"'{self.field_delimiter}'",
                "RECORD_DELIMITER": f"'{self.record_delimiter}'",
                "FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'",
                "ESCAPE_UNENCLOSED_FIELD": None,
                "NULL_IF": "('')",
            }

        params_list = [f"{k} = {v}" for k, v in params.items()]
        params_str = ',\n            '.join(params_list)
        return params_str

    @property
    def copy_params_str(self):
        if self.custom_file_format:
            params = self.custom_file_format
        else:
            params = {"OVERWRITE": True, "HEADER": True, "MAX_FILE_SIZE": 5000000000}

        params_list = [f"{k} = {v}" for k, v in params.items()]
        params_str = ',\n'.join(params_list)
        return params_str

    @property
    def col_names_str(self):
        return f"{', '.join(self.column_list)}" if self.column_list else "*"

    @property
    def copy_into_sql(self) -> str:
        date_formated = datetime.datetime.now().strftime("%Y%m%d")
        file_path = (
            f'{stages.tsos_da_int_backup_archive}/backup/'
            f'{self.database}.{self.schema}.{self.table}/{date_formated}/'
            f'{self.table}_{date_formated}.'
            f'{self.file_type.lower()}'
        )

        cmd = rf"""
        COPY INTO {file_path}
        FROM (
            SELECT
                {self.col_names_str}
            FROM {self.database}.{self.schema}.{self.table}
            {self.where_clause}
        )
        FILE_FORMAT=(
            {self.file_format_params_str}
        )
        {self.copy_params_str}
        ;
        """
        return unindent_auto(cmd)

    @property
    def delete_sql(self):
        return rf'DELETE FROM {self.database}.{self.schema}.{self.table} {self.where_clause};'

    def execute(self, context=None):
        self.run_sql_or_path(self.copy_into_sql)
        if self.delete_after_backup:
            self.run_sql_or_path(self.delete_sql)

    def dry_run(self) -> None:
        print(self.copy_into_sql)
        if self.delete_after_backup:
            print(self.delete_sql)
