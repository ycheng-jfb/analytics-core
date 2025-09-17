from typing import List, Optional

from airflow.utils.email import send_email
from snowflake.connector import DictCursor

from include.airflow.hooks.slack import SlackHook
from include.airflow.operators.snowflake import BaseSnowflakeOperator, SnowflakeSqlOperator
from include.utils.snowflake import (
    Column,
    CopyConfigCsv,
    LoadJobInsertIfNotExists,
    LoadJobInsertUpdate,
    LoadJobInsertUpdateBrand,
    LoadJobInsertUpdateDelete,
    LoadJobScd,
    LoadJobTruncate,
    generate_query_tag_cmd,
)
from include.utils.string import unindent_auto

DEFAULT_COPY_CONFIG = CopyConfigCsv(
    field_delimiter=',',
    force_copy=False,
    header_rows=0,
    record_delimiter='\n',
    skip_pct=1,
    timestamp_format=None,
)


class BaseSnowflakeLoadOperator(BaseSnowflakeOperator):
    """
    Base operator for loading data to snowflake incrementally.

    Args:
        database: database of base table
        schema: schema of base table
        table: name of base table
        column_list: column list
        files_path: this goes in the FROM clause
        pattern: if provided, regex pattern for files
        copy_config: either :class:`~.CopyConfigJson` or :class:`~.CopyConfigCsv`
        default_timezone: valid IANA timezone for session
        staging_database: if you want _stg table on a database other than that of base table
        archive_database: if you want to append deltas to archive table
        view_database: if you want a view to be created
        cluster_by: if you want a clustering key specified
        initial_load: if you want the tables to be created at runtime
        stg_column_naming: choose if stage table follows 'source' or 'target' column names
    """

    template_fields = ("files_path", "pattern", "custom_select")

    def __init__(
        self,
        database,
        schema,
        table,
        column_list: List[Column],
        files_path,
        pattern=None,
        copy_config=DEFAULT_COPY_CONFIG,
        default_timezone="UTC",
        warehouse=None,
        staging_database=None,
        staging_schema=None,
        archive_database=None,
        view_database=None,
        cluster_by: str = None,
        initial_load: bool = False,
        stg_column_naming: str = 'target',
        custom_select: str = None,
        **kwargs,
    ):
        for param in ('base_table', 'target_database'):
            if param in kwargs:
                raise ValueError(
                    f'{param} is no longer a valid param.  you must specify database,'
                    f'schema, and table'
                )
        super().__init__(
            database=database,
            schema=schema,
            warehouse=warehouse,
            timezone=default_timezone,
            **kwargs,
        )
        self.table = table
        self.files_path = files_path
        self.pattern = pattern
        self.column_list = column_list
        self.initial_load = True
        self.staging_database = staging_database
        self.staging_schema = staging_schema
        self.archive_database = archive_database
        self.view_database = view_database
        self.copy_config = copy_config
        self.cluster_by = cluster_by
        self.initial_load = initial_load
        self.stg_column_naming = stg_column_naming
        self.custom_select = custom_select

    @property
    def job_kwargs(self):
        return dict(
            database=self.database,
            schema=self.schema,
            table=self.table,
            warehouse=self.warehouse,
            files_path=self.files_path,
            column_list=self.column_list,
            pattern=self.pattern,
            copy_config=self.copy_config,
            staging_database=self.staging_database,
            staging_schema=self.staging_schema,
            archive_database=self.archive_database,
            view_database=self.view_database,
            cluster_by=self.cluster_by,
            stg_column_naming=self.stg_column_naming,
        )

    def get_job(self, cnx):
        raise NotImplementedError

    def execute(self, context=None):
        self.copy_config.custom_select = self.custom_select
        with self.snowflake_hook.get_conn() as cnx:
            self.snowflake_hook.set_autocommit(cnx, False)
            job = self.get_job(cnx)
            job.execute(initial_load=self.initial_load)

    def create_all_objects(self, dry_run=False):
        if dry_run is True:
            self.get_job(None).run_ddls(dryrun=dry_run)
        else:
            with self.snowflake_hook.get_conn() as cnx:
                self.get_job(cnx).run_ddls(dryrun=dry_run)

    def dry_run(self, initial_load=True):
        job = self.get_job(None)
        job.execute(initial_load=initial_load, dryrun=True)


class SnowflakeIncrementalLoadOperator(BaseSnowflakeLoadOperator):
    """
    For insert-update processes.

    See :class:`~.BaseSnowflakeLoadOperator` for more info.

    :param pre_merge_command: arbitrary SQL to execute before the merge.
    """

    template_fields = (
        "files_path",
        "pattern",
        "pre_merge_command",
        "snowflake_conn_id",
        "custom_select",
    )  # type: ignore

    def __init__(
        self,
        pre_merge_command=None,
        warehouse: str = 'DA_WH_ETL_LIGHT',
        **kwargs,
    ):
        self.warehouse = warehouse
        super().__init__(warehouse=warehouse, **kwargs)
        self.pre_merge_command = pre_merge_command

    @property
    def job_kwargs(self):
        return dict(**super().job_kwargs, pre_merge_command=self.pre_merge_command)

    def get_job(self, cnx):
        return LoadJobInsertUpdate(
            cnx=cnx, **self.job_kwargs, task_id=self.task_id, dag_id=self.dag_id
        )


class SnowflakeBrandIncrementalLoadOperator(BaseSnowflakeLoadOperator):

    template_fields = (
        "files_path",
        "pattern",
        "pre_merge_command",
        "snowflake_conn_id",
        "custom_select",
    )  # type: ignore

    def __init__(
        self,
        brand: str,
        pre_merge_command: Optional[str] = None,
        warehouse: Optional[str] = 'DA_WH_ETL',
        **kwargs,
    ):
        self.warehouse = warehouse
        self.brand = brand
        super().__init__(warehouse=warehouse, **kwargs)
        self.pre_merge_command = pre_merge_command

    @property
    def job_kwargs(self):
        return dict(**super().job_kwargs, pre_merge_command=self.pre_merge_command)

    def get_job(self, cnx):
        return LoadJobInsertUpdateBrand(
            cnx=cnx,
            **self.job_kwargs,
            task_id=self.task_id,
            dag_id=self.dag_id,
            brand=self.brand,
        )


class SnowflakeTruncateAndLoadOperator(SnowflakeIncrementalLoadOperator):
    """
    For truncate-and-load processes.

    See parent classes :class:`~.BaseSnowflakeLoadOperator` and
    :class:`~.SnowflakeIncrementalLoadOperator`  for more info.

    Even though it's truncate and load, it still dedupes in the merge, according to uniqueness
    specified in column list.
    """

    template_fields = ("files_path", "pattern", "snowflake_conn_id")  # type: ignore

    def get_job(self, cnx):
        return LoadJobTruncate(cnx=cnx, **self.job_kwargs, task_id=self.task_id, dag_id=self.dag_id)


class SnowflakeInsertUpdateDeleteOperator(SnowflakeIncrementalLoadOperator):
    """
    For insert-update-delete processes.

    .. note::

        You probably don't want to use this operator.  It's only good for small tables, because you
        have to have the full source table loaded in every delta.

    See parent classes :class:`~.BaseSnowflakeLoadOperator` and
    :class:`~.SnowflakeIncrementalLoadOperator`  for more info.
    """

    def get_job(self, cnx):
        return LoadJobInsertUpdateDelete(
            cnx=cnx, **self.job_kwargs, task_id=self.task_id, dag_id=self.dag_id
        )


class SnowflakeScdOperator(BaseSnowflakeLoadOperator):
    """
    For type-2 SCD loads.

    See parent class :class:`~.BaseSnowflakeLoadOperator` for more parameter documentation.
    """

    def get_job(self, cnx):
        return LoadJobScd(cnx=cnx, **self.job_kwargs, task_id=self.task_id, dag_id=self.dag_id)


class CopyUnsuccessfulError(Exception):
    pass


class SnowflakeCopyOperator(SnowflakeSqlOperator):
    """
    Use when running arbitrary copy commands.

    It will parse the load results.

    If every file is either 'LOADED' or 'PARTIALLY LOADED', the task is successful.
    Otherwise, the task fails.

    You can control your the partial load threshold with parameters in your copy command.

    :param email_to_when_no_file: email address or list of addresses you want alerts sent to if no files are copied.

    :param slack_id_when_no_file: slack_conn_id for slack channel you want alerts sent to if no files are copied.

    """

    def __init__(
        self,
        warehouse='DA_WH_ETL_LIGHT',
        default_timezone='UTC',
        email_to_when_no_file=None,
        slack_id_when_no_file=None,
        **kwargs,
    ):
        super().__init__(timezone=default_timezone, warehouse=warehouse, **kwargs)
        self.email_to_when_no_file = email_to_when_no_file
        self.slack_id_when_no_file = slack_id_when_no_file

    def run_command(self):
        with self.snowflake_hook.get_conn() as cnx:
            self.cnx = cnx  # for self.on_kill()
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            with cnx.cursor(cursor_class=DictCursor) as cur:
                cur.execute(query_tag)
                cur.execute(self.sql_or_path)
                load_result = cur.fetchall()
        return load_result

    def process_load_result(self, load_result):
        total_rows_loaded = 0
        total_rows_parsed = 0
        error_file_count = 0
        error_message = []
        no_file_flag = True
        for row in load_result:
            if row['status'] == 'Copy executed with 0 files processed.':
                break
            no_file_flag = False
            total_rows_parsed += row["rows_parsed"]
            if 'LOADED' in row["status"]:
                total_rows_loaded += row["rows_loaded"]
                self.log.info(f"Load successful: {row}")
            else:
                error_file_count += 1
                error_message.append(row)
                self.log.error(f"Load unsuccessful: {row}")
        self.log.info(f"Loaded total {total_rows_loaded} of {total_rows_parsed} records")
        if error_file_count > 0:
            raise CopyUnsuccessfulError(
                f"Load process was unsuccessful. Please check log below: \n {error_message}"
            )
        elif no_file_flag:
            if self.email_to_when_no_file:
                send_email(
                    to=self.email_to_when_no_file,
                    subject=f'WARNING - No new files processed for {self.dag_id}.{self.task_id}',
                    html_content=f'Snowflake-Copy-Operator did not process any new files for '
                    f'the task <b>{self.dag_id}.{self.task_id}</b>',
                )
            if self.slack_id_when_no_file:
                hook = SlackHook(slack_conn_id=self.slack_id_when_no_file)
                hook.send_message(
                    message=f'*WARNING* - No new files processed for *{self.dag_id}'
                    f'.{self.task_id}*'
                )

    def execute(self, context=None):
        self.log.info(f"Executing: {self.sql_or_path}")
        load_result = self.run_command()
        self.process_load_result(load_result=load_result)

    def dry_run(self):
        print(self.sql_or_path)


class SnowflakeCopyTableOperator(SnowflakeCopyOperator):
    """
    Simple copy operator.  Takes copy config and column list, just like :class:`~.SnowflakeIncrementalLoadOperator`
    and can generate table ddl but is just append only and only single table.

    Args:
        table: target table name (excluding schema and database)
        files_path: inbound path
        column_list: list of snowflake columns (:class:`~snowflake.Column`) excluding metas
        copy_config: copy config class that manages copy params and file format
        pattern: optional regex pattern for additional filtering beyond files path

    """

    template_fields = ("files_path", "pattern", "snowflake_conn_id", "warehouse")  # type: ignore

    def __init__(
        self,
        table,
        files_path,
        column_list: List[Column],
        warehouse=None,
        copy_config=DEFAULT_COPY_CONFIG,
        pattern=None,
        cluster_by=None,
        **kwargs,
    ):
        super().__init__(warehouse=warehouse, sql_or_path='', **kwargs)
        self.colunm_list = column_list
        self.table = table
        self.copy_config = copy_config
        self.files_path = files_path
        self.pattern = pattern
        self.cluster_by = cluster_by

    @property
    def full_table_name(self):
        name_parts = [x for x in (self.database, self.schema, self.table) if x]
        return '.'.join(name_parts)

    @property
    def ddl_base_table(self):
        cluster_by = f"\nCLUSTER BY ({self.cluster_by})\n" if self.cluster_by else ''
        colunm_list = self.colunm_list
        column_ddls = ',\n            '.join([f"{x.name} {x.type}" for x in colunm_list])
        cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.full_table_name} (
            {column_ddls},
            meta_create_datetime TIMESTAMP_LTZ(3) DEFAULT current_timestamp(3)
        ){cluster_by};
        """
        return unindent_auto(cmd)

    @property
    def dml_copy_into_table(self) -> str:
        return self.copy_config.dml_copy_into_table(  # type: ignore
            table=self.full_table_name,
            files_path=self.files_path,
            pattern=self.pattern,
            column_names=[x.name for x in self.colunm_list],
        )

    def execute(self, context=None):
        self.log.info(f"Executing: {self.dml_copy_into_table}")
        with self.snowflake_hook.get_conn() as cnx:
            self.cnx = cnx  # for self.on_kill()
            with cnx.cursor(cursor_class=DictCursor) as cur:
                query_tag_sql = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
                cur.execute(query_tag_sql)
                cur.execute(self.dml_copy_into_table)
                load_result = cur.fetchall()
        self.process_load_result(load_result=load_result)

    def dry_run(self):
        print(self.ddl_base_table)
        print(self.dml_copy_into_table)

    def create_all_objects(self, dry_run=False):
        if dry_run:
            print(self.ddl_base_table)
        else:
            with self.snowflake_hook.get_conn() as cnx:
                with cnx.cursor(cursor_class=DictCursor) as cur:
                    cur.execute(self.ddl_base_table)


class SnowflakeInsertOperator(SnowflakeIncrementalLoadOperator):
    """
    For inserting the row if not exists in target table.

    Even though it's insert operator, it still dedupes the data based meta_row_hash of all columns
    """

    def get_job(self, cnx):
        return LoadJobInsertIfNotExists(
            cnx=cnx, **self.job_kwargs, task_id=self.task_id, dag_id=self.dag_id
        )
