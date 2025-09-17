import csv
import tempfile
from pathlib import Path

import pendulum

from include.airflow.operators.snowflake_export import SnowflakeToSFTPOperator
from include.utils.snowflake import get_result_column_names, generate_query_tag_cmd


class SalesfloorExportToSFTP(SnowflakeToSFTPOperator):
    template_fields = ["sql_or_path", "timestamp", "sftp_dir"]

    def __init__(self, batch_size, timestamp, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_size = batch_size
        self.timestamp = timestamp

    def get_filename(self, batch_count):
        dt = pendulum.parse(self.timestamp)
        new_dt = dt.add(seconds=30 * batch_count)
        str_dt = str(new_dt.replace(tzinfo=None))
        final_dt = str_dt.replace('-', '').replace(':', '').replace('T', '-')
        return self.filename.replace('.csv', '-' + final_dt + '.csv')

    def export_to_files(self, sql, local_path) -> list:
        batch_count = 1
        rows_exported = 0
        file_list = []
        self.log.info("Executing: %s", sql)
        query_tag_cmd = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        with self.snowflake_hook.get_conn() as cnx:
            cur = cnx.cursor()
            cur.execute(query_tag_cmd)
            cur.execute(sql)
            rows = cur.fetchmany(self.batch_size)  # type: ignore
            while rows:
                filename = Path(local_path, self.get_filename(batch_count))
                with open(filename, "wt") as f:  # type: ignore
                    writer = csv.writer(
                        f,
                        dialect=self.dialect,
                        delimiter=self.field_delimiter,
                        quoting=csv.QUOTE_MINIMAL,
                        escapechar='\\',
                    )
                    if self.header:
                        column_list = self.get_effective_column_list(
                            self.lowercase_column_names, get_result_column_names(cur)
                        )
                        writer.writerow(column_list)
                    writer.writerows(rows)
                rows_exported += len(rows)
                batch_count += 1
                self.log.info(f"fetching more rows: batch {batch_count}")
                file_list.append(filename)
                rows = cur.fetchmany(self.batch_size)  # type: ignore

        self.log.info(f"{rows_exported}: rows exported")
        return file_list

    def watermark_execute(self, context=None):
        with tempfile.TemporaryDirectory() as td:
            sql = self.get_sql_cmd(self.sql_or_path)
            files_exported = self.export_to_files(sql=sql, local_path=td)
            for file in files_exported:
                remote_path = Path(self.sftp_dir, file.name).as_posix()
                self.upload_to_sftp(local_path=file, remote_path=remote_path)
                self.log.info(f"{file}: exported to {remote_path}")
