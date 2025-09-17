import csv
import gzip
import os
import tempfile
from binascii import hexlify
from pathlib import Path
from typing import List, Union

import pandas as pd
from airflow.models import BaseOperator, SkipMixin
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.email import send_email
from functools import cached_property
from pyodbc import SQL_BINARY, SQL_VARBINARY

from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.config import conn_ids
from include.airflow.hooks.smb import SMBHook
from include.utils.context_managers import ConnClosing


def bin_to_hex(val):
    return hexlify(val).decode("utf-8")


class MsSqlToS3Operator(BaseOperator, SkipMixin):
    FIELD_DELIMITER = "\t"
    RECORD_DELIMITER = "\n"
    FILE_EXTENSION = "tsv"
    template_fields = ["sql", "key"]

    def __init__(
        self,
        sql,
        bucket,
        key,
        mssql_conn_id=conn_ids.MsSql.default,
        s3_conn_id=conn_ids.AWS.tfg_default,
        s3_replace=True,
        batch_size=100000,
        file_split_size=5000000,
        skip_downstream_if_no_rows=False,
        **kwargs,
    ):
        """

        :param sql: sql command to execute to retrieve data
        :param bucket: s3 bucket to send the data
        :param key: s3 key to use.  in order to use splitting, put ``*`` where you want part id
        :param mssql_conn_id:
        :param s3_conn_id:
        :param s3_replace: should we replace existing s3 files with same name.
        :param batch_size: how many rows do e pull from sql server at a time.
        :param skip_downstream_if_no_rows: it will skip the downstream task if no data is returned.
        :param kwargs:
        """
        self.sql = sql
        self.bucket = bucket
        self.key = key
        self.mssql_conn_id = mssql_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_replace = s3_replace
        self.batch_size = batch_size
        self.file_split_size = file_split_size
        self.skip_downstream_if_no_rows = skip_downstream_if_no_rows
        super().__init__(**kwargs)

    def copy_to_s3(self, filename, key):
        s3_hook = S3Hook(self.s3_conn_id)
        print(f"uploading to {self.bucket}/{key}")
        s3_hook.load_file(
            filename=filename, key=key, bucket_name=self.bucket, replace=self.s3_replace
        )

    @staticmethod
    def part_id(num):
        return f"000000{num}"[-6:]

    def build_key(self, part_num):
        return self.key.replace("*", self.part_id(part_num))

    @staticmethod
    def get_row_count(cur):
        cur.execute("select @@rowcount")
        rowcount = cur.fetchall()[0][0]
        return rowcount

    def write_sql_to_s3_split(self, cnx, sql, temp_dir):
        temp_file_name = Path(temp_dir, "tmpfile").as_posix()
        cur = cnx.cursor()
        batch_count = 0
        file_count = 0
        cur.execute(sql)
        is_finished = False
        while not is_finished:
            with gzip.open(temp_file_name, "wt") as f:
                file_count += 1
                rows_written = 0
                writer = csv.writer(
                    f,
                    dialect="unix",
                    delimiter=self.FIELD_DELIMITER,
                    quoting=csv.QUOTE_MINIMAL,
                    escapechar="\\",
                )
                while True:
                    batch_count += 1
                    print(f"fetching rows: batch {batch_count}")
                    rows = cur.fetchmany(self.batch_size)
                    if rows:
                        writer.writerows(rows)
                        rows_written += self.batch_size
                    else:
                        is_finished = True
                        break
                    if rows_written >= self.file_split_size:
                        break
            if rows_written > 0:
                curr_key = self.build_key(part_num=file_count)
                self.copy_to_s3(filename=temp_file_name, key=curr_key)
                os.remove(temp_file_name)
            else:
                print("no rows written. skipping upload.")
                break

        return self.get_row_count(cur)

    def write_sql_to_s3(self, cnx, sql, temp_dir):
        temp_file_name = Path(temp_dir, "tmpfile").as_posix()
        print(sql)
        cur = cnx.cursor()
        batch_count = 0
        cur.execute(sql)
        with gzip.open(temp_file_name, "wt") as f:
            writer = csv.writer(
                f,
                dialect="unix",
                delimiter=self.FIELD_DELIMITER,
                quoting=csv.QUOTE_MINIMAL,
                escapechar="\\",
            )
            rows = cur.fetchmany(self.batch_size)
            while rows:
                writer.writerows(rows)
                batch_count += 1
                print(f"fetching more rows: batch {batch_count}")
                rows = cur.fetchmany(self.batch_size)
        if batch_count > 0:
            self.copy_to_s3(temp_file_name, self.key)

        return self.get_row_count(cur)

    @staticmethod
    def split_statements(val):
        sql_statements = [x.strip() for x in val.split(";") if x.strip()]
        last_stmt = sql_statements.pop()
        return sql_statements, last_stmt

    def dry_run(self):
        print(f"dry_run:\n\nquery:{self.sql}\n\nkey: {self.key}\n")

    def execute(self, context):
        mssqlhook = MsSqlOdbcHook(mssql_conn_id=self.mssql_conn_id)
        file_split = "*" in self.key
        with tempfile.TemporaryDirectory() as td, ConnClosing(
            mssqlhook.get_conn()
        ) as cnx:
            cnx.add_output_converter(SQL_VARBINARY, bin_to_hex)
            cnx.add_output_converter(SQL_BINARY, bin_to_hex)

            prep_stmts, last_stmt = self.split_statements(self.sql)
            if prep_stmts:
                cur = cnx.cursor()
                for stmt in prep_stmts:
                    cur.execute(stmt)

            if file_split:
                print("found '*' in s3 key; implementing file splitting")
                rowcount = self.write_sql_to_s3_split(
                    cnx=cnx, sql=last_stmt, temp_dir=td
                )
            else:
                rowcount = self.write_sql_to_s3(cnx=cnx, sql=last_stmt, temp_dir=td)

        if rowcount < 1 and self.skip_downstream_if_no_rows:
            print("skipping downstream immediate task")
            if context:
                downstream_tasks = context["task"].get_direct_relatives(upstream=False)
                if downstream_tasks:
                    self.skip(
                        context["dag_run"],
                        context["ti"].execution_date,
                        downstream_tasks,
                    )
        else:
            print(f"row count: {rowcount}")


class MsSqlOperator(BaseOperator):
    """
    This operator will execute a statement in MsSql

    Args:
        sql: sql statement to execute
        mssql_conn_id: mssql connection
    """

    def __init__(
        self,
        sql,
        mssql_conn_id,
        **kwargs,
    ):
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        super().__init__(**kwargs)

    @cached_property
    def mssql_hook(self):
        return MsSqlOdbcHook(mssql_conn_id=self.mssql_conn_id)

    def execute(self, context=None):
        with ConnClosing(self.mssql_hook.get_conn()) as conn:
            cur = conn.cursor()
            cur.execute(self.sql)

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


class MsSqlToSmbCsvOperator(MsSqlOperator):
    """
    This operator will execute a statement in MsSql and upload the results to a CSV on an SMB share

    Args:
        sql: sql query or procedure to execute
        mssql_conn_id: connection ID for MsSql
        smb_conn_id: connection ID for SMB
        share_name: name of share to upload to
        remote_path: remote file path to upload to
        compress: boolean to turn file into a gzip file
    """

    def __init__(
        self,
        smb_conn_id,
        share_name,
        remote_path,
        compress=False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.smb_conn_id = smb_conn_id
        self.share_name = share_name
        self.remote_path = remote_path
        self.compress = compress

        if self.compress and not self.remote_path.endswith(".gz"):
            raise ValueError(
                "remote_path should end with .gz when compress is set to True"
            )
        elif not self.compress and not self.remote_path.endswith(".csv"):
            raise ValueError(
                "remote_path should end with .csv when no compression is set"
            )

    @property
    def open(self):
        return gzip.open if self.compress else open

    @cached_property
    def smb_hook(self):
        return SMBHook(self.smb_conn_id)

    def write_query_results_to_file(self, fp):
        with ConnClosing(self.mssql_hook.get_conn()) as conn, self.open(
            fp.name, "wt"
        ) as f:
            cur = conn.cursor()
            cur.execute(self.sql)
            result = cur.fetchall()
            column_names = [i[0] for i in cur.description]
            writer = csv.writer(f)
            writer.writerow(column_names)
            writer.writerows(result)

    def execute(self, context=None):
        with tempfile.NamedTemporaryFile() as tf:
            self.write_query_results_to_file(tf)
            tf.flush()
            self.smb_hook.upload(self.share_name, self.remote_path, tf.name)


class MsSqlAlertOperator(MsSqlOperator):
    template_fields = ("sql",)
    template_ext = (".sql",)

    def __init__(
        self,
        sql_or_path: Union[str, Path],
        subject: str,
        body: str,
        distribution_list: List[str],
        mssql_conn_id: str,
        alert_type: str = "mail",
        priority: str = "P5",
        database: str = None,
        *args,
        **kwargs,
    ):
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql_or_path
        super().__init__(mssql_conn_id=self.mssql_conn_id, sql=self.sql, **kwargs)
        self.subject = subject
        self.body = body
        self.distribution_list = distribution_list
        self.alert_type = alert_type
        self.database = database
        self.priority = priority

    def execute(self, context=None):
        self.log.info("Executing: %s", self.sql)
        with self.mssql_hook.get_conn() as cnx:
            sql = self.get_sql_cmd(sql_or_path=self.sql)
            cur = cnx.execute(sql)
            last_rows = cur.fetchall()
            self.log.info(last_rows)
            if len(last_rows) > 0:
                self.log.info("found rows; sending alert.")
                column_list = [x[0] for x in cur.description]
                df = pd.DataFrame(data=last_rows, columns=column_list)
                html_content = f"{self.body}<br><br>{df.to_html(index=False)}"
                if self.alert_type == "mail":
                    send_email(
                        to=self.distribution_list,
                        subject=self.subject,
                        html_content=html_content,
                    )
                elif self.alert_type == "user_notification":
                    for index, row in df.iterrows():
                        to_email = str(row["NAME"])
                        subject = self.subject.replace("USERNAME", to_email)
                        html_content = f"{self.body.replace('USERNAME', to_email)}"
                        to_email = to_email.lower()
                        print(to_email)
                        print(subject)
                        print(html_content)
                        send_email(
                            to=to_email, subject=subject, html_content=html_content
                        )
            else:
                self.log.info("no rows retrieved; skipping alert.")
