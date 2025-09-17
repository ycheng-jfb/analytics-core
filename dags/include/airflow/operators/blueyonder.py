from airflow.providers.sftp.hooks.sftp import SFTPHook
from include.airflow.operators.snowflake import SnowflakeSqlOperator
from include.utils.context_managers import ConnClosing
from include.config import conn_ids
from tempfile import TemporaryDirectory
from datetime import datetime
import pandas as pd
from pathlib import Path
import datetime
from pytz import timezone
import pytz
import os


class BlueYonderExportFeed(SnowflakeSqlOperator):
    def __init__(
        self,
        table_name,
        sftp_conn_id,
        file_name,
        column_list,
        generate_empty_file,
        remote_filepath="/",
        append=True,
        snowflake_conn_id=conn_ids.Snowflake.default,
        database="reporting_prod",
        schema="blue_yonder",
        run_procedure=True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.table_name = table_name
        self.generate_empty_file = generate_empty_file
        self.remote_filepath = remote_filepath
        self.append = append
        self.sftp_conn_id = sftp_conn_id
        self.file_name = file_name
        self.max_file_size = 150 * 1024 * 1024
        self.run_procedure = run_procedure
        """int: max_file_size documented inline.

        This is restriction enforced in terms of maximum file size possible, that can be shared in
        SFTP. The maximum file size is actually 200MB , but its kept as 150 MB to handle exceptional
        errors due
        """
        self.chunk_size = 500000
        self.file_name_list = []
        self.no_of_files = 0
        self.column_list = column_list
        self.record_count = 0

    def write_to_local(self, temp_dir):
        select_list = (
            ", ".join([f"{x.name}" for x in self.column_list])
            if self.column_list
            else "*"
        )
        select_query = f"select {select_list} from {self.database}.{self.schema}.{self.table_name}_stg"
        self.file_name = f"{self.file_name}_{datetime.datetime.now(tz=pytz.utc).astimezone(timezone('US/Pacific')).strftime('%Y%m%d_%H%M%S')}"
        part_no = 0
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        with sftp_hook.get_conn() as sftp_client:
            with ConnClosing(self.snowflake_hook.get_conn()) as conn:
                query_tag = (
                    f"ALTER SESSION SET QUERY_TAG='{self.dag_id},{self.task_id}';"
                )
                cur = conn.cursor()
                cur.execute(query_tag)
                for df_batch in pd.read_sql_query(
                    sql=select_query, con=conn, chunksize=self.chunk_size
                ):
                    print("part no", part_no)
                    local_path = f"{Path(temp_dir)}/{self.file_name}-{part_no:03}.csv"
                    df_batch.to_csv(local_path, header=True, sep="|", index=False)
                    print(f"{self.remote_filepath}/{self.file_name}-{part_no:03}.csv")
                    sftp_client.put(
                        localpath=local_path,
                        remotepath=f"{self.remote_filepath}/{self.file_name}-{part_no:03}.csv",
                    )
                    os.remove(local_path)
                    part_no += 1
                if part_no == 1:
                    sftp_client.rename(
                        oldpath=f"{self.remote_filepath}/{self.file_name}-000.csv",
                        newpath=f"{self.remote_filepath}/{self.file_name}.csv",
                    )

    def split_file(self, temp_dir):
        if self.no_of_files > 1:
            part_no = 0
            df = pd.read_csv(f"{temp_dir}/{self.file_name}.csv", sep="|")
            row_count = len(df)
            rows_per_file = row_count // self.no_of_files + 1
            for i in range(self.no_of_files):
                max_row = (i + 1) * rows_per_file
                if max_row > row_count:
                    max_row = row_count
                df_batch = df.iloc[i * rows_per_file : max_row]
                local_path = f"{Path(temp_dir)}/{self.file_name}-{part_no:03}.csv"
                df_batch.to_csv(local_path, header=True, sep="|", index=False)
                self.file_name_list.append(local_path)
                part_no += 1
            os.remove(f"{temp_dir}/{self.file_name}.csv")
        else:
            self.file_name_list.append(f"{temp_dir}/{self.file_name}.csv")

    def upload_to_sftp(self):
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        with sftp_hook.get_conn() as sftp_client:
            for file_name in self.file_name_list:
                sftp_client.put(
                    localpath=file_name,
                    remotepath=f"{self.remote_filepath}/{Path(file_name).name}",
                )

    def file_size_exceed_limit(self, td):
        file_size_bytes = os.path.getsize(f"{td}/{self.file_name}.csv")
        self.no_of_files = file_size_bytes // self.max_file_size
        if file_size_bytes % self.max_file_size >= 0:
            self.no_of_files += 1
        if self.no_of_files > 1:
            self.log.info(f"File size exceeds {self.max_file_size}")

    def create_tables(self, dry_run=False):
        create_table_cols = ""
        for col in self.column_list:
            create_table_cols += f"""\n    {col.name} {col.type},\n """
        create_table_cmd_stg = f"""CREATE TRANSIENT TABLE IF NOT EXISTS
        {self.database}.{self.schema}.{self.table_name}_stg ({create_table_cols}\ndate_added TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_NTZ(3));"""
        create_table_cmd_main = f"""CREATE TRANSIENT TABLE IF NOT EXISTS
        {self.database}.{self.schema}.{self.table_name} ({create_table_cols}\ndate_added TIMESTAMP_NTZ(3) DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_NTZ(3));"""
        if dry_run:
            print(create_table_cmd_stg)
            print(create_table_cmd_main)
        else:
            with ConnClosing(self.snowflake_hook.get_conn()) as sf_cnx:
                sf_cursor = sf_cnx.cursor()
                sf_cursor.execute(create_table_cmd_stg)
                sf_cursor.execute(create_table_cmd_main)
                sf_cursor.close()

    def data_completeness_qa_check(self):
        # compare the no of records in total record count to record count after file split
        df = pd.concat(
            map(lambda x: pd.read_csv(x, delimiter="|"), self.file_name_list)
        )
        if self.record_count != len(df):
            raise Exception(
                "Number of records fetched and after splitting doesnt match."
            )

    def create_snapshot(self, dry_run=False):
        select_list = (
            ", ".join([f"{x.name}" for x in self.column_list])
            if self.column_list
            else "*"
        )
        insert_query = f"""INSERT INTO {self.database}.{self.schema}.{self.table_name}  ({select_list},date_added)
        select {select_list},date_added from {self.database}.{self.schema}.{self.table_name}_stg """
        print(insert_query)
        delete_sql = f"""DELETE FROM {self.database}.{self.schema}.{self.table_name}
        where date_added < current_date() - 15;"""  # delete 15 days older snapshots
        print(delete_sql)
        if not dry_run:
            with ConnClosing(self.snowflake_hook.get_conn()) as sf_cnx:
                query_tag = (
                    f"ALTER SESSION SET QUERY_TAG='{self.dag_id},{self.task_id}';"
                )
                sf_cursor = sf_cnx.cursor()
                sf_cursor.execute(query_tag)
                sf_cursor.execute(insert_query)
                sf_cursor.execute(delete_sql)

    def dry_run(self):
        super().dry_run()
        self.create_tables(dry_run=True)
        self.create_snapshot(dry_run=True)

    def execute(self, context=None):
        if self.run_procedure:
            # self.create_tables()
            super().execute(context=context)
            if self.append:
                self.create_snapshot()
        with TemporaryDirectory() as td:
            self.write_to_local(td)
