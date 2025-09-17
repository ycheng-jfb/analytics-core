import inspect
import logging
from collections import namedtuple
from datetime import timedelta
from functools import cached_property
from pathlib import Path
from typing import List, Optional, Type, Union

from include import DAGS_DIR
from include.airflow.hooks.mssql import MsSqlOdbcHook, get_column_list_definition
from include.airflow.operators.mssql_acquisition import (
    BaseHighWatermarkQuery,
    HighWatermarkMaxRowVersion,
    HighWatermarkMaxVarcharSS,
    MsSqlBcpTableToS3Operator,
    MsSqlTableToS3CsvChunkOperator,
    MsSqlTableToS3CsvOperator,
)
from include.airflow.operators.snowflake_load import (
    SnowflakeCopyTableOperator,
    SnowflakeIncrementalLoadOperator,
    SnowflakeInsertUpdateDeleteOperator,
    SnowflakeTruncateAndLoadOperator,
)
from include.config import conn_ids, s3_buckets, snowflake_roles, stages
from include.config.pools import Pool, get_pool
from include.utils.context_managers import ConnClosing
from include.utils.functions import get_default_args
from include.utils.path import ensure_dir
from include.utils.snowflake import Column, CopyConfigCsv, CopyConfigJson
from include.utils.string import camel_to_snake

class_mapping = {
    class_.__name__: class_
    for class_ in (
        MsSqlBcpTableToS3Operator,
        MsSqlTableToS3CsvChunkOperator,
        MsSqlTableToS3CsvOperator,
    )
}
"""
Use to retrieve class object given class name.
"""

cdc_databases = {"um_replicated"}
"""List of databases that are cdc sources"""

conn_database_mapping = {
    conn_ids.MsSql.evolve01_app_airflow: [
        "jf_portal",
        "ultrawarehouse",
        "merlin",
        "ssrs_reports",
        "eplmv14prod",
        "ngc_plm_prodcomp01",
        "countwise",
        "ultraidentity",
    ],
    conn_ids.MsSql.bento_prod_reader_app_airflow: [
        "gdpr",
        "ultracms",
        "ultramerchant",
        "ultrarollup",
        "ultracart",
    ],
    conn_ids.MsSql.jfgcdb01_app_airflow: ["jfgc"],
    conn_ids.MsSql.giftco_app_airflow: ["jfgc"],  # added for 2019
    conn_ids.MsSql.dbp61_app_airflow: ["um_replicated"],
    conn_ids.MsSql.dbd05_app_airflow: ["datafbpr"],
}
"""
List of databases available for each connection.  Used only to create a reverse lookup in
:obj:`database_conn_mapping`.
"""

database_conn_mapping = {
    database: conn_id
    for conn_id, databases in conn_database_mapping.items()
    for database in databases
}
brand_database_conn_mapping = {
    "fl": {
        i: conn_ids.MsSql.fabletics_app_airflow
        for i in [
            "gdpr",
            "ultracms",
            "ultramerchant",
            "ultrarollup",
            "ultracart",
        ]
    },
    "jfb": {
        i: conn_ids.MsSql.justfab_app_airflow
        for i in [
            "gdpr",
            "ultracms",
            "ultramerchant",
            "ultrarollup",
            "ultracart",
        ]
    },
    "sxf": {
        i: conn_ids.MsSql.savagex_app_airflow
        for i in [
            "gdpr",
            "ultracms",
            "ultramerchant",
            "ultrarollup",
            "ultracart",
        ]
    },
}
"""
Mapping between database name and connection id to use when pulling from that database.

Use :func:`~.get_conn_id` instead of accessing this variable directly, so that an informative
exception will be thrown if database not mapped.
"""


def get_conn_id(database: str, server: str = None) -> str:
    """
    Get connection id to use, given database name.

    If database not yet mapped, through informative exception.
    Args:
        database: the database for which you want the connection id

    Returns: connection id

    """
    try:
        if server is not None:
            return brand_database_conn_mapping[server][database]
        else:
            return database_conn_mapping[database.lower()]
    except KeyError:
        raise Exception(f"database {database} is not mapped in `database_conn_mapping`")


"""
Mapping between database name and connection id to use when pulling from that database for each brand group.
"""

database_schema_mapping = {
    "ultracms": "ultra_cms",
    "ultramerchant": "ultra_merchant",
    "ultrawarehouse": "ultra_warehouse",
    "ultrarollup": "ultra_rollup",
    "ssrs_reports": "evolve01_ssrs_reports",
    "ultracart": "ultra_cart",
    "ultraidentity": "ultra_identity",
    "datafbpr": "bluecherry",
}
"""
Map source database --> target schema when there is custom mapping.
"""


class ColumnList(list):
    @property
    def has_uniqueness(self):
        for col in self:
            if col.uniqueness is True:
                return True
        return False

    @property
    def delta_column_list(self):
        return sorted(
            [x for x in self if x.delta_column is not False],
            key=lambda x: x.delta_column,
        )

    @property
    def candidate_watermark_column(self):
        return self.delta_column_list and self.delta_column_list[0]

    @property
    def has_delta_column(self):
        for col in self:
            if col.delta_column is not False:
                return True
        return False

    @property
    def column_name_list(self):
        return [x.name for x in self]

    @property
    def select_list(self):
        return "    " + ",\n    ".join(self.column_name_list)

    @property
    def uniqueness_cols_str(self) -> List[str]:
        return [x.name for x in self if x.uniqueness is True]


def remove_all_quoting(table_name):
    return table_name.replace('"', "").replace("[", "").replace("]", "").lower()


CDC_ALL = "all"
CDC_DEL = "del"
CDC_LAST = "last"


class TableConfig:
    """
    Config class to manage lake acquisition for sql server databases on our ecom platform.

    Args:
        database: source database
        schema: source schema
        table: the name of the table as it appears in the source. camel case table names will be
            converted to snake.
        column_list: List of Column objects.  The names given here are for the target.
        watermark_column: column used for incremental load watermarking
        initial_load_value: on first run, what will be lower bound
        high_watermark_cls: subclass of BaseHighWatermarkQuery that takes watermark_col
            (e.g. 'datetime_added') and returns e.g. ``max(datetime_added)`` or
            ``max(convert(varchar, datetime_added, 2))``
        strict_inequality: controls whether where clause is ``>`` or ``>=``.
        fetch_batch_size: when pulling data we do cur.fetch_many. this controls rows per fetch.
        schema_version_prefix: because we are writing to csv, if schema changes, we want to write to
            a different directory.
        to_s3_operator_class_name: if you want to use a to_s3 class other than
            MsSqlTableToS3CsvOperator
        skip_param_validation: if you want to skip column list validation
        cluster_by: for target snowflake table, if we want to add a clustering key
        partition_cols: if source table is partitioned, we query it differently
        enable_archive: True or False enables or disables archive append; if None, default logic
            is applied
        skip_downstream_if_no_rows: True or False, it will skip the downstream task if no data is
            returned from mssql source table.
        server: (Optional) linked server name
    """

    UTCNOW_TEMPLATE = "{{ macros.tfgdt.utcnow_nodash() }}"

    def __init__(
        self,
        database: str,
        schema: str,
        table: str,
        column_list: List[Column],
        server: str = None,
        linked_server: Optional[str] = None,
        warehouse: str = "DA_WH_ETL_LIGHT",
        table_hints: List[str] = None,
        watermark_column: Optional[str] = None,
        initial_load_value: Optional[str] = "1900-01-01",
        high_watermark_cls: Type[BaseHighWatermarkQuery] = HighWatermarkMaxVarcharSS,
        strict_inequality=True,
        fetch_batch_size: int = 100000,
        schema_version_prefix: str = "v1",
        to_s3_operator_class_name: str = "MsSqlTableToS3CsvOperator",
        skip_param_validation=False,
        cluster_by: str = None,
        partition_cols: List[str] = None,
        enable_archive=None,
        skip_downstream_if_no_rows=False,
        is_full_upsert=False,
    ):
        self.server = server
        self.linked_server = linked_server
        self.database = database
        self.schema = schema
        self.table = table
        self.column_list = ColumnList(column_list)
        self.is_cdc_db = database in cdc_databases
        self.cdc_type = (
            self.is_cdc_db
            and self.get_cdc_parts(table_name=self.table).cdc_type
            or None
        )
        self.table_hints = table_hints
        if self.cdc_type in (CDC_ALL, CDC_DEL):
            self.target_database = "lake_archive"
            self.staging_database = None
            self.view_database = None
        elif self.server is not None:
            self.target_database = "lake_" + self.server
            self.staging_database = "lake_stg"
            self.view_database = "lake_" + self.server + "_view"
        else:
            self.target_database = "lake"
            self.staging_database = "lake_stg"
            self.view_database = "lake_view"
        self.watermark_column = watermark_column
        self.initial_load_value = initial_load_value
        self.high_watermark_cls = high_watermark_cls
        self.stage_name = stages.tsos_da_int_inbound
        self.bucket_name = s3_buckets.tsos_da_int_inbound
        self.strict_inequality = strict_inequality
        self.fetch_batch_size = fetch_batch_size
        self.schema_version_prefix = schema_version_prefix
        self.full_view_name = (
            f"{self.view_database}.{self.target_schema}.{self.target_table}"
        )
        self.to_s3_operator_class_name = to_s3_operator_class_name
        self.skip_param_validation = skip_param_validation
        self.cluster_by = cluster_by
        self.partition_cols = partition_cols
        self.s3_conn_id = conn_ids.S3.tsos_da_int_prod
        self.enable_archive = enable_archive
        self.skip_downstream_if_no_rows = skip_downstream_if_no_rows
        self.is_full_upsert = is_full_upsert
        self.warehouse = warehouse
        if not self.skip_param_validation:
            self.validate_column_list()

    @property
    def append_to_archive_table(self):
        """
        If ``enable_archive`` is defined, use it; otherwise apply default logic.

        """
        if self.enable_archive in (True, False):
            return self.enable_archive
        else:
            return (
                self.watermark_column
                and self.watermark_column.lower()
                not in ("datetime_added", "date_create", "created_on")
                and "delete_log" not in self.full_table_name.lower()
            )

    @staticmethod
    def get_cdc_parts(table_name):
        return namedtuple(
            "CDCTable", ["source_database", "source_table_name", "cdc_type"]
        )(*table_name.split("__"))

    @property
    def archive_database(self):
        return (
            "lake_archive"
            if self.append_to_archive_table and self.cdc_type not in (CDC_ALL, CDC_DEL)
            else None
        )

    @cached_property
    def cdc_table_name_parts(self):
        return self.get_cdc_parts(self.table)

    @property
    def cdc_source_database(self):
        return self.cdc_table_name_parts.source_database

    @property
    def cdc_table_name(self):
        return self.cdc_table_name_parts.source_table_name

    @property
    def cdc_table_type(self):
        return self.cdc_table_name_parts.cdc_type

    @property
    def target_table(self):
        table = camel_to_snake(self.table.strip("[").strip("]"))
        if table in {"order"}:
            return '"' + table.upper() + '"'
        if self.cdc_type in (CDC_ALL, CDC_DEL):
            table = f"{self.cdc_table_name}__{self.cdc_table_type}"
        elif self.cdc_type == CDC_LAST:
            table = self.cdc_table_name
        return table

    @cached_property
    def target_schema(self):
        database = self.database
        if self.cdc_type in (CDC_ALL, CDC_DEL):
            schema_base = database_schema_mapping.get(
                self.cdc_source_database.lower(), self.cdc_source_database.lower()
            )
            return f"{schema_base}_cdc"
        elif self.cdc_type == CDC_LAST:
            database = self.cdc_source_database
        target_schema = database_schema_mapping.get(database.lower(), database.lower())
        return target_schema

    @property
    def mssql_conn_id(self):
        return get_conn_id(self.database, self.server)

    @property
    def pool(self):
        return get_pool(self.mssql_conn_id)

    @property
    def full_table_name(self):
        return f"{self.database}.{self.schema}.{self.table}".lower()

    @property
    def filename(self):
        table = remove_all_quoting(self.full_table_name)
        suffix = f"_{self.UTCNOW_TEMPLATE}" if self.watermark_column else ""
        return f"{table}{suffix}.csv.gz".lower()

    @property
    def s3_prefix(self):
        table = remove_all_quoting(self.target_table)
        table_path = f"{table}/{self.schema_version_prefix}"
        if self.server is not None:
            base_path = f"{self.target_database}/{self.target_database}_{self.server}.{self.target_schema}"
        else:
            base_path = (
                f"{self.target_database}/{self.target_database}.{self.target_schema}"
            )
        return f"{base_path}.{table_path}".lower()

    @property
    def key(self):
        return f"{self.s3_prefix}/{self.filename}".lower()

    @property
    def full_target_table_name(self):
        if self.server is not None:
            return f"{self.target_database}_{self.server}.{self.target_schema}.{self.target_table}"
        else:
            return f"{self.target_database}.{self.target_schema}.{self.target_table}"

    @property
    def base_task_id(self):
        return self.full_target_table_name.replace('"', "").lower()

    @property
    def to_snowflake_task_id(self):
        return f"{self.base_task_id}.to_snowflake"

    @property
    def to_s3_task_id(self):
        return f"{self.base_task_id}.to_s3"

    @property
    def load_operator(
        self,
    ) -> Type[
        Union[
            SnowflakeCopyTableOperator,
            SnowflakeInsertUpdateDeleteOperator,
            SnowflakeIncrementalLoadOperator,
            SnowflakeTruncateAndLoadOperator,
        ]
    ]:
        if self.cdc_type in (CDC_ALL, CDC_DEL):
            return SnowflakeCopyTableOperator
        if self.is_full_upsert:
            return SnowflakeInsertUpdateDeleteOperator
        if self.watermark_column is None:
            return SnowflakeTruncateAndLoadOperator
        else:
            return SnowflakeIncrementalLoadOperator

    @property
    def source_column_name_list(self):
        return [x.source_name for x in self.column_list]

    def validate_column_list(self):
        if self.watermark_column:
            if not self.column_list.has_uniqueness:
                raise Exception(
                    "Column list must have uniqueness cols if using watermark column"
                )
            if not self.column_list.has_delta_column:
                raise Exception(
                    "Column list must have delta col if using watermark column"
                )

    @property
    def watermark_namespace(self):
        if self.is_cdc_db:
            return "acquisition_cdc"
        else:
            return "acquisition_ecom"

    @property
    def watermark_process_name(self):
        return self.full_target_table_name

    @property
    def to_s3_operator(
        self,
    ) -> Union[
        MsSqlBcpTableToS3Operator,
        MsSqlTableToS3CsvChunkOperator,
        MsSqlTableToS3CsvOperator,
    ]:
        class_ = class_mapping[self.to_s3_operator_class_name]
        return class_(  # type: ignore
            task_id=self.to_s3_task_id,
            src_table=self.table,
            src_schema=self.schema,
            src_database=self.database,
            linked_server=self.linked_server,
            mssql_conn_id=self.mssql_conn_id,
            bucket=self.bucket_name,
            s3_conn_id=self.s3_conn_id,
            key=self.key,
            column_list=self.source_column_name_list,
            table_hints=self.table_hints,
            watermark_column=self.watermark_column,
            initial_load_value=self.initial_load_value,
            high_watermark_cls=self.high_watermark_cls,
            strict_inequality=self.strict_inequality,
            pool=self.pool,
            batch_size=self.fetch_batch_size,
            watermark_namespace=self.watermark_namespace,
            watermark_process_name=self.watermark_process_name,
            priority_weight=2,
            partition_cols=self.partition_cols,
            skip_downstream_if_no_rows=self.skip_downstream_if_no_rows,
            # is_full_upsert=self.is_full_upsert,
        )

    @property
    def to_snowflake_operator(
        self,
    ) -> Union[
        SnowflakeCopyTableOperator,
        SnowflakeTruncateAndLoadOperator,
        SnowflakeIncrementalLoadOperator,
        SnowflakeInsertUpdateDeleteOperator,
    ]:
        params = dict(
            task_id=self.to_snowflake_task_id,
            role=snowflake_roles.etl_service_account,
            database=self.target_database,
            schema=self.target_schema,
            table=self.target_table,
            column_list=self.column_list,
            warehouse=self.warehouse,
            files_path=f"{self.stage_name}/{self.s3_prefix}/",
            copy_config=CopyConfigCsv(
                field_delimiter=MsSqlTableToS3CsvOperator.FIELD_DELIMITER,
                header_rows=0,
                skip_pct=0,
            ),
            default_timezone="America/Los_Angeles",
            cluster_by=self.cluster_by,
            execution_timeout=timedelta(hours=1),
            pool=Pool.snowflake_lake_load,
        )
        if self.cdc_type not in (CDC_ALL, CDC_DEL):
            params.update(
                staging_database=self.staging_database,
                archive_database=self.archive_database,
                view_database=self.view_database,
            )
        if self.server is not None:
            params.update(
                database=self.target_database + "_" + self.server,
                schema=self.target_schema,
                staging_database=self.staging_database,
                staging_schema=self.target_schema + "_" + self.server,
                archive_database=self.archive_database,
                view_database=self.view_database.replace(
                    "_view", "_" + self.server + "_view"
                ),
            )
        return self.load_operator(**params)

    def debug_info(self):
        msg = f"""
        source table: {self.full_table_name}
        target table: {self.full_target_table_name}
        staging database: {self.staging_database}
        operator: {self.load_operator.__name__}
        s3 bucket: {self.bucket_name}
        s3 prefix: {self.s3_prefix}
        s3 filename: {self.filename}
        s3 key: {self.key}
        initial load value: {self.initial_load_value}
        snowflake stage name: {self.stage_name}
        """
        print(msg)

    @property
    def primary_key_def(self):
        pk_cols = ", ".join([x.name for x in self.column_list if x.uniqueness is True])
        return f"ALTER TABLE {self.full_target_table_name} ADD PRIMARY KEY ({pk_cols});"

    def __repr__(self):
        default_args = get_default_args(self.__init__)
        cmd = "TableConfig(\n"
        signature = inspect.signature(self.__init__)

        param_names = [k for k, v in signature.parameters.items()]
        sort_map = {name: idx for idx, name in enumerate(signature.parameters)}
        sort_map["column_list"] = 10000

        for p in sorted(param_names, key=lambda x: sort_map.get(x)):
            if p == "kwargs":
                continue
            actual_attr = getattr(self, p)
            if p not in default_args or actual_attr != default_args[p]:
                try:
                    cmd += f"    {p}={actual_attr.__repr__()},\n"
                except TypeError:
                    cmd += f"    {p}={actual_attr.__name__},\n"

        cmd += ")"
        return cmd

    @classmethod
    def generate_config(cls, database: str, schema: str, table: str, **kwargs):
        def update_col(col_name, update_dict):
            col = [x for x in column_list if x.name == col_name][0]
            for k, v in update_dict.items():
                setattr(col, k, v)

        log = logging.getLogger()
        log.setLevel(logging.WARNING)

        is_cdc_db = database in cdc_databases
        cols_db = database
        cols_table = table
        if is_cdc_db:
            cdc_parts = cls.get_cdc_parts(table)
            if cdc_parts.cdc_type == CDC_LAST:
                cols_db = cdc_parts.source_database
                cols_table = cdc_parts.source_table_name
        cols_conn_id = get_conn_id(cols_db)
        mssql_hook = MsSqlOdbcHook(cols_conn_id)
        with ConnClosing(mssql_hook.get_conn()) as cnx:
            cur = cnx.cursor()
            column_list = list(
                get_column_list_definition(
                    cur=cur, database=cols_db, schema=schema, table=cols_table
                )
            )
        if not column_list:
            raise Exception(f"table {table} not found.  check spelling.")
        cl = ColumnList(column_list)
        kwargs.update(
            database=database,
            schema=schema,
            table=table,
            column_list=column_list,
        )
        delta_column_list = cl.delta_column_list
        if delta_column_list:
            kwargs.update(watermark_column=delta_column_list[0].source_name)
        if is_cdc_db:
            kwargs.update(high_watermark_cls=HighWatermarkMaxRowVersion)
            kwargs.update(watermark_column="repl_timestamp")
            kwargs.update(initial_load_value="0x0")
            update_col("repl_timestamp", dict(uniqueness=True))
            if not delta_column_list:
                update_col("repl_timestamp", dict(delta_column=True))

        log.setLevel(logging.INFO)
        return cls(**kwargs)  # type: ignore

    def write_out_config(self):
        config_text = ""
        if self.high_watermark_cls != HighWatermarkMaxVarcharSS:
            config_text += (
                f"from include.airflow.operators.mssql_acquisition import "
                f"{self.high_watermark_cls.__name__}\n"
            )
        config_text += (
            "from include.utils.acquisition.table_config import TableConfig\n"
        )
        config_text += "from include.utils.snowflake import Column\n\ntable_config = "
        config_text += self.__repr__()
        config_text += "\n"
        table_path = Path(
            Path(
                DAGS_DIR,
                "edm",
                "acquisition",
                "configs",
                "lake",
                self.target_schema,
                self.target_table.replace('"', "").lower(),
            )
        ).with_suffix(".py")
        ensure_dir(table_path.parent)
        with open(table_path, "wt") as f:
            f.write(config_text)
        from subprocess import PIPE, Popen

        with Popen(["black", table_path], stdout=PIPE):
            pass

    @classmethod
    def generate_configs_from_list(
        cls, table_name_list: List[str], create_objects: bool = False
    ):
        """
        Takes list of table names, generates acquisition configs.

        .. note::

            You must also enable this job by adding to a dag, e.g.
            ``dags/edm/acquisition/edm_acquisition_ultra_merchant.py``.

        Args:
            table_name_list: list of fully-qualified table names as defined in source server.
            create_objects: if True, will create all snowflake objects

        Examples:
            >>> from include.utils.acquisition.table_config import TableConfig
            >>> table_name_list = [
            >>>     'ultramerchant.dbo.case_intent_log',
            >>>     'ultramerchant.dbo.case_intent',
            >>> ]
            >>> TableConfig.generate_configs_from_list(table_name_list)  # generate configs

        """
        for table_name in table_name_list:
            database, schema, table = (
                table_name.replace("[", "").replace("]", "").split(".")
            )

            table_config = cls.generate_config(
                database=database,
                schema=schema,
                table=table,
            )
            table_config.write_out_config()
            if create_objects:
                table_config.to_snowflake_operator.create_all_objects()

    def dedupe_lake_archive(self):
        if self.cdc_type in ("all", "del"):
            raise Exception("Not applicable to cdc ingestions")
        """
        Helper function to print out command to dedupe a lake_archive table

        """
        uniqueness_list = ", ".join(self.column_list.uniqueness_cols_str)
        watermark_column = self.watermark_column
        column_list = ", ".join(self.column_list.column_name_list)
        lake_archive_table = f"lake_archive.{self.target_schema}.{self.target_table}"
        cmd = f"""
        CREATE TABLE {lake_archive_table}__new__ AS
        SELECT {column_list}
        FROM (
            SELECT *,
                row_number() over (PARTITION BY {uniqueness_list}, {watermark_column} ORDER BY {watermark_column} ASC) AS rn
            FROM {lake_archive_table} l
        ) a
        WHERE a.rn = 1;

        DROP TABLE {lake_archive_table};

        ALTER TABLE {lake_archive_table}__new__
            RENAME TO {lake_archive_table};
        """  # noqa: E501
        print(cmd)

    def merge_into_lake_archive(self):
        if self.cdc_type in (CDC_ALL, CDC_DEL):
            raise Exception("Not applicable to cdc ingestions")
        """
        Helper function to print out command to merge lake records into lake_archive

        """
        uniqueness_list = ", ".join(self.column_list.uniqueness_cols_str)
        watermark_column = self.watermark_column
        column_list = ", ".join(self.column_list.column_name_list)
        lake_archive_table = f"lake_archive.{self.target_schema}.{self.target_table}"
        merge_cond = "\n    AND ".join(
            [
                f"equal_null(s.{x}, t.{x})"
                for x in [*self.column_list.uniqueness_cols_str, watermark_column]
            ]
        )
        cmd = f"""
        MERGE INTO {lake_archive_table} t
        USING (
            SELECT {column_list}
            FROM (
                SELECT *,
                    row_number() over (PARTITION BY {uniqueness_list}, {watermark_column} ORDER BY {watermark_column} ASC) AS rn
                FROM {self.full_target_table_name} l
            ) a
            WHERE a.rn = 1
        ) s ON {merge_cond}
        WHEN NOT MATCHED THEN INSERT ({column_list})
        VALUES ({column_list});
        """  # noqa: E501
        print(cmd)


class KafkaTableConfig(TableConfig):
    def __init__(
        self,
        custom_select_sql: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.custom_select_sql = custom_select_sql

    @property
    def full_target_table_name(self):
        return f"{self.database}.{self.schema}.{self.table}"

    @property
    def to_s3_operator(self):
        raise NotImplementedError

    @property
    def to_snowflake_operator(self):
        op_kwargs = dict(
            task_id=f"{self.full_target_table_name}.s3_to_snowflake",
            database=self.target_database,
            schema=self.target_schema,
            table=self.table,
            warehouse=self.warehouse,
            staging_database="lake_stg",
            view_database="lake_view",
            files_path=None,
            column_list=self.column_list,
            copy_config=CopyConfigJson(
                custom_select=self.custom_select_sql,
                skip_pct=0,
                match_by_column_name=False,
            ),
            pool=Pool.snowflake_lake_load,
        )
        return self.load_operator(**op_kwargs)
