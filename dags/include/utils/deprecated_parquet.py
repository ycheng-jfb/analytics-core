from typing import Callable, Optional
from typing.io import BinaryIO

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from pyarrow.lib import RecordBatch
from pyodbc import SQL_VARBINARY, Connection


def write_sql_to_parquet(
    cnx: Connection,
    sql: str,
    table_schema: pa.Schema,
    filename: str,
    bin_encoder: Optional[Callable] = None,
    fetch_size=10000,
    batches_per_table=5,
):
    with open(filename, "wb") as f:
        write_sql_to_parquet_fileobj(
            cnx=cnx,
            sql=sql,
            table_schema=table_schema,
            fileobj=f,
            bin_encoder=bin_encoder,
            fetch_size=fetch_size,
            batches_per_table=batches_per_table,
        )


def chunk_batches(batches, batches_per_table=10):
    batch_list = []
    batch_count = 0
    for batch in batches:
        batch_count += 1
        batch_list.append(batch)
        if batch_count == batches_per_table:
            yield batch_list
            batch_count = 0
            batch_list = []
    if batch_list:
        yield batch_list


def write_sql_to_parquet_fileobj(
    cnx: Connection,
    sql: str,
    table_schema: pa.Schema,
    fileobj: BinaryIO,
    bin_encoder: Optional[Callable] = None,
    fetch_size=10000,
    batches_per_table=5,
):
    if bin_encoder:
        cnx.add_output_converter(SQL_VARBINARY, bin_encoder)
    cur = cnx.cursor()
    cur.execute(sql)
    batch_iterator = yield_record_batches_from_cursor(
        cur, table_schema, batch_size=fetch_size
    )
    with pq.ParquetWriter(
        where=fileobj,
        schema=table_schema,
        flavor="spark",
        version="2.0",
        compression="snappy",
    ) as writer:
        for batch_list in chunk_batches(
            batch_iterator, batches_per_table=batches_per_table
        ):
            print("writing batches to file")
            table = pa.Table.from_batches(batch_list)
            writer.write_table(table)
    if bin_encoder:
        cnx.remove_output_converter(SQL_VARBINARY)


def yield_record_batches_from_cursor(cur, table_schema, batch_size=10000):
    batch_count = 0
    while True:
        batch_count += 1
        print(f"pulling batch {batch_count}")
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        trows = list(np.transpose(rows))
        batch = RecordBatch.from_arrays(trows, schema=table_schema)
        yield batch


def rewrite_parquet_file(
    filename_in,
    filename_out,
    compression="snappy",
    version="2.0",
    flavor="spark",
    row_group_size=None,
):
    table = pq.read_table(filename_in)
    with pq.ParquetWriter(
        where=filename_out,
        schema=table.schema,
        compression=compression,
        version=version,
        flavor=flavor,
    ) as writer:
        writer.write_table(table, row_group_size=row_group_size)


def write_sql_to_parquet_pandas(cnx, sql, filename, chunk_size):
    writer = None
    schema = None
    batch_num = 0
    try:
        for df in pd.read_sql(sql=sql, con=cnx, chunksize=chunk_size):
            batch_num += 1
            print(f"pulling batch num {batch_num}")
            table = pa.Table.from_pandas(df=df, preserve_index=False, schema=schema)
            if not schema:
                schema = table.schema
            if not writer:
                writer = pq.ParquetWriter(
                    where=filename,
                    schema=schema,
                    flavor="spark",
                    version="2.0",
                    compression="snappy",
                )
            writer.write_table(table=table)
    finally:
        if hasattr(writer, "close"):
            writer.close()
