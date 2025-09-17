import warnings
from importlib import import_module
from pathlib import Path

from include import DAGS_DIR, SQL_DIR
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.snowflake_mart_base import (
    MART_DATABASE,
    MART_SCHEMA,
    MissingViewException,
)
from include.config import snowflake_roles, conn_ids
from include.utils.context_managers import ConnClosing
from include.utils.path import ensure_dir
from include.utils.string import unindent_auto


def get_mart_operator(table_name):
    """
    Returns mart operator could be either subclass of BaseSnowflakeMartOperator
    or SnowflakeProcedureOperator.

    Args:
        table_name: 3-part name of edw table

    Returns: Union[BaseSnowflakeMartOperator, SnowflakeProcedureOperator]
    """
    parts = table_name.split('.')
    if len(parts) != 3:
        raise Exception('`table_name` param must use 3-part naming')
    mod = import_module(f"edm.mart.configs.{table_name}")
    op = mod.get_mart_operator()
    return op


def get_all_mart_operators(table_list=None):
    """
    Get all mart operators or a subset of mart operators if ``table_list`` provided.

    Args:
        table_list: if is provided, only retrieve operators for table list.

    Returns: Iterable[Union[BaseSnowflakeMartOperator, SnowflakeProcedureOperator]]

    """
    if not table_list:
        path = f"{DAGS_DIR}/edm/mart/configs/edw"
        table_list = []
        for elem in Path(path).rglob('*.py'):
            if elem.name == '__init__.py':
                continue
            table_name = path_to_table_name(elem)
            table_list.append(table_name)
    for table in table_list:
        yield get_mart_operator(table)


def path_to_table_name(file_path):
    table_name = (
        file_path.as_posix()
        .replace(f"{DAGS_DIR}/edm/mart/configs/", '')
        .replace('/', '.')
        .replace('.py', '')
    )
    return table_name


def create_views(table_list=None, dryrun=False):
    """
    Create views for all edw base tables.

    Args:
        table_list: if provided, only views for table list will be created. if not, all views
            will be created.  use 3-part naming.
        dryrun: if true, will only print out view definitions.
    """
    op_list = list(get_all_mart_operators(table_list))
    if dryrun:
        for op in op_list:
            if not hasattr(op, 'get_view_ddl'):
                continue
            try:
                cmd = op.get_view_ddl()
            except MissingViewException:
                warnings.warn(f"view '{op.table_name}' missing from source control. adding.")
                cmd = op.get_view_ddl()
            print(cmd)
    else:
        hook = SnowflakeHook(
            snowflake_conn_id=conn_ids.Snowflake.default, role=snowflake_roles.etl_service_account
        )
        with ConnClosing(hook.get_conn()) as cnx, cnx.cursor() as cur:
            for op in op_list:
                if not hasattr(op, 'get_view_ddl'):
                    continue
                cmd = op.get_view_ddl()
                print(f"creating view for table {op.target_full_table_name}")
                print(cmd)
                cur.execute(cmd)


def generate_ddls(table_list=None):
    """
    This method is to generate tables scripts for target and staging tables and write
    them into corresponding sql script directory.
    Args:
        table_list: if we provide table values, table scripts will be generated for these tables
        E.g. ['edw.dbo.dim_address', 'edw.dbo.dim_bundle']
    """
    for op in get_all_mart_operators(table_list):
        if not hasattr(op, 'create_stg_table'):
            continue
        yield f"{op.table_name}_stg", op.create_stg_table()
        yield f"{op.table_name}_excp", op.create_excp_table()
        yield op.table_name, op.create_base_table()


def print_ddls(table_list=None):
    for table_name, cmd in generate_ddls(table_list=table_list):
        print(cmd)


def drop_and_create_all_tables(cur, table_list=None, are_you_sure=None):
    """
    Only run this if you are trying to drop and recreate every table in edw.

    """

    if are_you_sure != "Yes, I'm 100% sure":
        return
    else:
        for table_name, cmd in generate_ddls(table_list=table_list):
            print(table_name)
            cur.execute(f"drop table if exists edw.dbo.{table_name}")
            print(cur.fetchall())
            cur.execute(cmd)
            print(cur.fetchall())


# todo: add check to ensure that there is no overlap between static dims and mart configs


def write_out_ddls(table_list=None):
    for table_name, cmd in generate_ddls(table_list=table_list):
        write_out_script(
            database=MART_DATABASE,
            folder='tables',
            script=cmd,
            schema=MART_SCHEMA,
            table=table_name,
        )


def write_out_script(database, folder, schema, table, script):
    path = Path(f"{SQL_DIR}/{database}/{folder}/{schema}.{table.lower()}.sql")
    ensure_dir(path.parent)
    with open(path, 'wt') as f:
        f.write(script)


def update_meta_row_hash(table_list=None, dryrun=False):
    """
    This method is to update meta row hash for all records in given table list.
    Args:
        table_list: if we provide table values, meta row hash will update for these tables
        E.g. ['edw.dbo.dim_address', 'edw.dbo.dim_bundle']
    """
    schema = 'dbo'
    config_list = []
    if table_list:
        for tbl in table_list:
            config_list.append(get_mart_operator(tbl))
    else:
        for config in get_all_mart_operators(schema=schema):
            config_list.append(config)

    if dryrun:
        for config in config_list:
            cmd = get_meta_row_hash_cmd(config)
            print(cmd)
    else:
        hook = SnowflakeHook(
            snowflake_conn_id=conn_ids.Snowflake.default, role=snowflake_roles.etl_service_account
        )
        with ConnClosing(hook.get_conn()) as cnx, cnx.cursor() as cur:
            for config in config_list:
                cmd = get_meta_row_hash_cmd(config)
                try:
                    print(f"Updating meta row hash for table {config.target_full_table_name}")
                    print(cmd)
                    cur.execute(cmd)
                except Exception as e:
                    print(
                        f"Updating meta row hash failed for table {config.target_full_table_name}"
                    )
                    print(e)


def get_meta_row_hash_cmd(table_name):
    mart_operator = get_mart_operator(table_name)
    if mart_operator.column_list.is_type_2_load:
        cmd = f"{table_name} is a type 2 table. Use this method is for Type1 Dims and Facts."
    else:
        cmd = f"""
                UPDATE {table_name} s
                    SET s.meta_row_hash = {mart_operator.meta_row_hash};
            """
    return unindent_auto(cmd)
