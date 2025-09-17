"""
test get sql to parquet table
>>> from pathlib import Path
>>> from include.airflow.hooks.mssql import MsSqlOdbcHook
>>> from binascii import hexlify
>>> from base64 import b64encode
>>> hook = MsSqlOdbcHook()
>>> cnx = hook.get_conn()
>>> cur = cnx.cursor()
>>>
>>> filename = Path('~/temp/test.parquet').expanduser().as_posix()
>>> table_schema = get_parquet_schema_from_table(cur, 'edw', 'dbo', 'dim_store')
>>> sql = "select * from edw.dbo.dim_store (nolock)"
>>> write_sql_to_parquet(cnx, sql, table_schema, filename, bin_encoder=b64encode)

"""

from typing import List

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

from include.utils.snowflake import Column


def sql_to_arrow_mapper(
    type_name, column_size, decimal_digits, timezone=None, **kwargs
):
    def dt_prec_map(num):
        if num > 6:
            return "ns"
        if num > 3:
            return "us"
        if num > 0:
            return "ms"
        return "s"

    try:
        if "varchar" in type_name:
            return pa.string()
        if type_name in ("bigint", "bigint identity"):
            return pa.int64()
        if type_name in ("int", "int identity"):
            return pa.int32()
        if type_name in ("bit", "boolean"):
            return pa.bool_()
        if type_name in ("money", "number", "numeric", "decimal"):
            return pa.decimal128(column_size, decimal_digits)
        if type_name in ("float",):
            return pa.float32()
        if type_name in ("double",):
            return pa.float64()
        if type_name in ("binary", "varbinary", "uniqueidentifier"):
            return pa.string()
        if type_name == "date":
            return pa.date32()
        if type_name in (
            "smalldatetime",
            "datetime",
            "datetime2",
            "timestamp_ntz",
            "timestamp_ltz",
            "timestamp_tz",
            "timestamp",
        ):
            return pa.timestamp(unit=dt_prec_map(column_size), tz=timezone)
    except Exception:
        print(f"something wrong: {(type_name, column_size, decimal_digits)}")
        raise
    raise Exception(f"no mapping defined for type {type_name}")


def get_table_schema(cur, database, schema, table):
    col_list = cur.columns(catalog=database, schema=schema, table=table).fetchall()
    col_names = [x[0] for x in cur.description]
    col_list = [dict(zip(col_names, row)) for row in col_list]
    attr_subset = {"column_name", "type_name", "column_size", "decimal_digits"}
    col_list = [{k: v for k, v in row.items() if k in attr_subset} for row in col_list]
    return col_list


def get_parquet_schema_from_table(
    cur, database, schema, table, column_subset=None, timezone=None
):
    col_list = get_table_schema(cur, database, schema, table)
    type_list = []
    for row in col_list:
        type_list.append(
            (row["column_name"], sql_to_arrow_mapper(**row, timezone=timezone))
        )

    if not column_subset:
        return pa.schema(type_list)
    else:
        type_dict = dict(type_list)
        schema_list = [(col, type_dict[col]) for col in column_subset]
        return pa.schema(schema_list)


def get_parquet_schema_from_snowflake_columns(column_list: List[Column]):
    type_list = []
    for col in column_list:
        type_list.append(
            (
                col.name,
                sql_to_arrow_mapper(
                    col.type_base_name.lower(), col.column_size, col.decimal_digits
                ),
            )
        )
    return pa.schema(type_list)


class BatchedParquetWriter(pq.ParquetWriter):
    @staticmethod
    def raise_stop_iteration():
        raise StopIteration

    def write_all(self, cur, batch_size=30000):
        batch_num = 0
        while True:
            try:
                table = pa.Table.from_arrays(
                    arrays=np.transpose(
                        cur.fetchmany(batch_size) or self.raise_stop_iteration()
                    ),
                    schema=self.schema,
                )
                batch_num += 1
                print("batch num ", batch_num)
                self.write_table(table)
            except StopIteration:
                print("no more rows")
                break


class BatchedPandasParquetWriter(pq.ParquetWriter):
    def write_all(self, cnx, sql, batch_size=30000):
        batch_num = 0
        for df in pd.read_sql_query(
            sql=sql, con=cnx, coerce_float=False, chunksize=batch_size
        ):
            batch_num += 1
            print(f"batch_num: {batch_num}")
            self.write_table(pa.Table.from_pandas(df=df, schema=self.schema))


class BatchedTurbodbcParquetWriter:
    def __init__(self, cur, filename, flavor="spark", version="2.0"):
        self.cur = cur
        self.filename = filename
        self.flavor = flavor
        self.version = version
        self.writer = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self.writer, "close"):
            self.writer.close()

    def write_all(self, cur):
        arr_data = cur.fetcharrowbatches()  # type: List[pa.Table]
        batch_num = 0
        for batch in arr_data:
            batch_num += 1
            print("batch num: ", batch_num)
            if not self.writer:
                self.writer = pq.ParquetWriter(
                    self.filename,
                    batch.schema,
                    flavor=self.flavor,
                    version=self.version,
                )
            self.writer.write_table(batch)
