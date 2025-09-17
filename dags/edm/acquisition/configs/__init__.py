from pathlib import Path

from airflow.utils.module_loading import import_string

from include import DAGS_DIR
from include.airflow.hooks.snowflake import SnowflakeHook
from include.config import snowflake_roles
from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig,
)
from include.utils.acquisition.table_config import TableConfig
from include.utils.context_managers import ConnClosing
from include.utils.string import camel_to_snake


class TableNotFoundError(Exception):
    pass


def get_table_config(table_name) -> TableConfig:
    try:
        return import_string(f"edm.acquisition.configs.{table_name}.table_config")  # type: ignore
    except ModuleNotFoundError:
        raise TableNotFoundError(f"table name {table_name} not found")


def get_lake_consolidated_table_config(table_name) -> LakeConsolidatedTableConfig:
    try:
        lake_consolidated_table_name = table_name[table_name.find(".") :]
        return import_string(  # type: ignore
            f"edm.acquisition.configs.lake_consolidated{lake_consolidated_table_name}.table_config"
        )
    except ModuleNotFoundError:
        raise TableNotFoundError(f"table name {table_name} not found")


def get_all_configs(schema=None, table=None):
    for elem in Path(f"{DAGS_DIR}/edm/acquisition/configs/lake").rglob("*.py"):
        if elem.name == "__init__.py":
            continue

        table_name = path_to_table_name(elem)
        config = get_table_config(table_name=table_name)
        if schema and table:
            if schema == config.target_schema and table == config.target_table:
                yield config
        elif schema:
            if schema == config.target_schema:
                yield config
        else:
            yield config


def get_all_lake_consolidated_configs(schema=None, table=None):
    for elem in Path(f"{DAGS_DIR}/edm/acquisition/configs/lake_consolidated").rglob(
        "*.py"
    ):
        if elem.name == "__init__.py":
            continue

        table_name = path_to_table_name(elem)
        config = get_lake_consolidated_table_config(table_name=table_name)
        if schema and table:
            if schema == config.schema and table == config.table:
                yield config
        elif schema:
            if schema == config.schema:
                yield config
        else:
            yield config


def to_import_path(file_path):
    return file_path.as_posix().replace("/", ".").replace(".py", "")


def path_to_table_name(file_path):
    table_name = (
        file_path.as_posix()
        .replace(f"{DAGS_DIR}/edm/acquisition/configs/", "")
        .replace("/", ".")
        .replace(".py", "")
    )
    return table_name


def create_all_views(schema=None, table=None):
    hook = SnowflakeHook(role=snowflake_roles.etl_service_account)
    with ConnClosing(hook.get_conn()) as cnx, cnx.cursor() as cur:
        for tbl in get_all_configs(schema=schema, table=table):
            try:
                print(f"creating view for table {tbl.full_target_table_name}")
                cur.execute(tbl.view_ddl)
            except Exception as e:
                print(f"view creation failed for table {tbl.full_target_table_name}")
                print(e)


def get_lake_history_tables(schema=None, table=None):
    table_list = []
    for x in get_all_configs(schema, table):
        if (
            x.append_to_archive_table
            and x.cdc_type not in ("all", "del")
            and x.target_table[-11:].lower() != "_delete_log"
        ):
            if x.cdc_type == "last":
                table = x.target_table.strip('"').lower()
            else:
                table = camel_to_snake(x.table.strip("[").strip("]"))

            table_list.append(f"lake.{x.target_schema}.{table}")

    return table_list


def get_lake_history_tables_with_one_delta_column(schema=None, table=None):
    table_list = []
    for x in get_all_configs(schema, table):
        if (
            x.append_to_archive_table
            and x.cdc_type not in ("all", "del")
            and x.target_table[-11:].lower() != "_delete_log"
        ):
            if x.cdc_type == "last":
                table = x.target_table.strip('"').lower()
            else:
                table = camel_to_snake(x.table.strip("[").strip("]"))

            if len(x.column_list.delta_column_list) < 2:
                table_list.append(f"lake.{x.target_schema}.{table}")

    return table_list
