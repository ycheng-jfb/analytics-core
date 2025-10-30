import time

import logging
from typing import Callable

from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import QueryStatus

from snowflake.snowpark import DataFrame, Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import *

# noinspection PyProtectedMember
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import CopyIntoTableNode

# noinspection PyProtectedMember
from snowflake.snowpark._internal.utils import parse_table_name


LOGGER = logging.getLogger("snowpark_utils")


class RawSqlExpression:
    def __init__(self, expression):
        self.expression = expression

    def __str__(self):
        return self.expression


def copy_json_into_existing_table(
    session: Session,
    table_name: str,
    src_file_path: str,
    src_pattern: str = None,
    src_files: Optional[list] = None,
    include_metadata: Optional[dict] = None,
    format_type_options: Optional[dict] = None,
    copy_options: Optional[dict] = None,
    block: bool = True,
):

    # TODO validate that copy options doesn't include explicitly defined options below
    # Create a local copy of copy_options to avoid mutating input dictionary
    local_copy_options = dict(copy_options) if copy_options is not None else {}
    local_copy_options['MATCH_BY_COLUMN_NAME'] = 'CASE_INSENSITIVE'
    if include_metadata:
        formatted_string = ', '.join(f"{key}={value}" for key, value in include_metadata.items())
        formatted_string = f"({formatted_string})"
        local_copy_options['INCLUDE_METADATA'] = RawSqlExpression(formatted_string)

    # noinspection PyTypeChecker
    df = DataFrame(
        session,
        CopyIntoTableNode(
            table_name=parse_table_name(table_name),
            file_path=src_file_path,
            files=src_files,
            file_format="json",
            format_type_options=format_type_options,
            pattern=src_pattern,
            column_names=None,
            transformations=None,
            copy_options=local_copy_options,
            validation_mode=None,
            user_schema=None,
            cur_options=None,
            create_table_from_infer_schema=False,
        ),
    )

    if block:
        return df.collect()
    else:
        return df.collect_nowait()


def unquote_columns(input_df: DataFrame) -> DataFrame:
    # Create a dictionary to map old column names to new column names without single quotes
    renamed_columns = {column: column.strip("\"").strip("'") for column in input_df.columns}
    # Use the rename method to update column names
    return input_df.rename(renamed_columns)


def columns_to_uppercase(input_df: DataFrame) -> DataFrame:
    return input_df.select([F.col(column).as_(column.upper()) for column in input_df.columns])


def wait_for_async_jobs(session, job_id_async_job_tuples):
    failed_job_ids = []
    running_jobs = {job_id: async_job for job_id, async_job in job_id_async_job_tuples}
    while running_jobs:
        for job_id, async_job in list(running_jobs.items()):
            if async_job.is_done():
                LOGGER.info(f"Job {job_id} is complete, query_id: {async_job.query_id}")
                del running_jobs[job_id]
                status = get_query_status(session, async_job.query_id)
                if is_query_error(status):
                    LOGGER.error(
                        f"Job {job_id} failed with status: {status}, query_id: {async_job.query_id}"
                    )
                    failed_job_ids.append(job_id)
                else:
                    LOGGER.info(f"Job {job_id} succeeded, query_id: {async_job.query_id}")
            else:
                LOGGER.info(f"Job {job_id} is still running, query_id: {async_job.query_id}")
        time.sleep(1)
    LOGGER.info("All async jobs have finished")
    return failed_job_ids


def get_query_status(session, query_id):
    return session.connection.get_query_status(query_id)


def is_query_successful(status):
    return QueryStatus.SUCCESS == status


def is_query_error(status):
    return SnowflakeConnection.is_an_error(status)


def table_exists(session: Session, database, schema, table):
    schema = schema.strip('"').upper()
    table = table.strip('"').upper()
    query = f"SELECT EXISTS (SELECT * FROM {database.upper()}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}') AS TABLE_EXISTS"
    return session.sql(query).collect()[0].as_dict().get('TABLE_EXISTS')


def create_table_with_schema(session, table_name, schema: StructType):
    session.create_dataframe([[None] * len(schema.names)], schema=schema).na.drop().write.mode(
        'overwrite'
    ).save_as_table(table_name)


def struct_typemap(c):
    if isinstance(c, DecimalType):
        return f"NUMBER({c.precision},{c.scale})"
    elif isinstance(c, LongType):
        return "NUMBER"
    elif isinstance(c, DoubleType):
        return "DOUBLE"
    elif isinstance(c, StringType):
        return "VARCHAR"
    elif isinstance(c, TimestampType):
        return "TIMESTAMP"
    elif isinstance(c, VariantType):
        return "VARIANT"
    elif isinstance(c, BinaryType):
        return "BINARY"
    elif isinstance(c, MapType):
        return "OBJECT"
    elif isinstance(c, ArrayType):
        return "ARRAY"
    elif isinstance(c, GeographyType):
        return "GEOGRAPHY"
    elif isinstance(c, BooleanType):
        return "BOOLEAN"
    elif isinstance(c, IntegerType):
        return "NUMBER"


def evolve_schema_if_necessary(session: Session, target_table: str, src_df: DataFrame) -> bool:
    target_table_parts = target_table.split('.')
    if not table_exists(
        session, target_table_parts[0], target_table_parts[1], target_table_parts[2]
    ):
        return False
    cols = session.table(target_table).columns
    results_cols = src_df.columns
    diff_col_names = [x for x in results_cols if x not in cols]
    if len(diff_col_names):
        LOGGER.info(f"Altering target table: {target_table}, adding columns: {diff_col_names}")
        diff_cols = [x for x in src_df.schema.fields if x.name in diff_col_names]
        for x in diff_cols:
            LOGGER.info(f"Adding column: {x.name}")
            sql = f"ALTER TABLE {target_table} ADD COLUMN {x.name} {struct_typemap(x.datatype)}"
            session.sql(sql).show()
        return True
    else:
        LOGGER.info(f"No changes needed for target table: {target_table}")
        return False


def flatten_struct(
    struct, prefix=None, explode_column_list=None, normalize_name_func: Callable = None
):
    output = []
    if isinstance(struct, StructType):
        fields = struct.fields
    elif isinstance(struct, ArrayType):
        fields = struct.element_type

    for field in fields:
        name = prefix + '_' + field.name if prefix else field.name
        dtype = field.datatype
        if isinstance(dtype, StructType):
            output += flatten_struct(dtype, prefix=name, explode_column_list=explode_column_list)
        else:
            if normalize_name_func:
                name = normalize_name_func(name)
            if (
                isinstance(dtype, ArrayType)
                and explode_column_list
                and (name in explode_column_list)
            ):
                output += flatten_struct(
                    dtype, prefix=name, explode_column_list=explode_column_list
                )
            else:
                output.append((name, dtype))
    return output


def struct_select(
    df: DataFrame,
    struct_type: StructType,
    unmapped_fields_col_name: Optional[str] = None,
    explode_column_list: list = None,
    normalize_name_func: Callable = None,
    use_safe_cast: bool = True,
):
    input_field_names = [field.name for field in df.schema.fields]
    mapped_field_names = []
    expressions = []

    flattened_fields = flatten_struct(
        struct_type,
        explode_column_list=explode_column_list,
        normalize_name_func=normalize_name_func,
    )

    for field in flattened_fields:
        # only add fields from structure that appear in df
        if field[0] not in input_field_names:
            continue
        field_name = field[0]
        field_type = field[1]
        LOGGER.debug(f"Adding field: {field_name}, with type: {field_type}")
        if (
            use_safe_cast
            and not isinstance(field_type, StringType)
            and not isinstance(field_type, ArrayType)
            and not isinstance(field_type, MapType)
        ):
            expressions.append(
                F.try_cast(F.col(field_name).cast(StringType()), field_type).as_(field_name)
            )
        else:
            expressions.append(F.col(field_name).cast(field_type).as_(field_name))
        mapped_field_names.append(field_name)
    if unmapped_fields_col_name:
        LOGGER.info(f"Storing unmapped fields in column: {unmapped_fields_col_name}")
        unmapped_field_names = [
            field_name for field_name in input_field_names if field_name not in mapped_field_names
        ]
        rescue_kv = []
        for field_name in unmapped_field_names:
            rescue_kv.extend([F.lit(field_name), F.col(field_name)])
        expressions.append(
            F.object_construct_keep_null(*rescue_kv)
            .cast(StringType())
            .as_(unmapped_fields_col_name)
        )
    else:
        LOGGER.info("Not storing unmapped fields")
    return df.select(expressions)
