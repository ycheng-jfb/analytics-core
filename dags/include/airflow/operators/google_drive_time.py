import tempfile
import time
from pathlib import Path
from threading import Thread

import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.utils import snowflake
from include.utils.context_managers import ConnClosing


class DrivingTime(BaseOperator):
    """
    This operator will get distance of stores from customer zip location
    """

    def __init__(
        self,
        number_of_threads,
        file_key,
        bucket_name,
        sql_query,
        s3_conn_id,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.API_KEY = ""
        self.number_of_threads = number_of_threads
        self.file_key = file_key
        self.bucket_name = bucket_name
        self.sql_query = sql_query
        self.s3_conn_id = s3_conn_id

    def google_map_get_distance_duration(self, origin_zip, destination_zip):
        request_url = (
            f"https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial"
            f"&origins={origin_zip}&destinations={destination_zip}&key={self.API_KEY}"
        )
        response_data = requests.get(request_url).json()
        for row in response_data["rows"]:
            if row["elements"][0]["status"] == "OK" and response_data["status"] == "OK":
                minutes = 0
                drive_distance = row["elements"][0]["distance"]["text"]
                drive_duration = row["elements"][0]["duration"]["text"]
                # Data type conversion
                drive_distance = float(drive_distance.split(" ")[0].replace(",", ""))
                if "day" in drive_duration:
                    days = int(drive_duration.split(" ")[0])
                    hours = int(drive_duration.split(" ")[2])
                    minutes = days * 24 * 60 + hours * 60
                elif "hour" in drive_duration:
                    hours = int(drive_duration.split(" ")[0])
                    api_minutes = int(drive_duration.split(" ")[2])
                    minutes = hours * 60 + api_minutes
                elif "min" in drive_duration:
                    minutes = int(drive_duration.split(" ")[0])
                return drive_distance, minutes
        return 9999, 9999

    def thread_loop(self, df_all, start, end, thread_number):
        df_thread = df_all[start:end]
        for store in df_thread.itertuples():
            records_processed = store[0] - start
            if records_processed % 1000 == 0 and records_processed != 0:
                print(f"thread {thread_number} {records_processed} rows processed")
                time.sleep(1.0)
            try:
                distance, duration = self.google_map_get_distance_duration(
                    store.vip_state_zip, store.store_state_zip
                )
                df_all.loc[store[0], "duration"] = duration
            except Exception as ex:
                print(f"Error: {ex} {store.vip_state_zip}  {store.store_state_zip}")
                time.sleep(1.0)

    def process_google_api(self):
        self.API_KEY = BaseHook.get_connection("google_map_api_default").password
        snowflake_hook = SnowflakeHook()

        with ConnClosing(snowflake_hook.get_conn()) as conn, conn.cursor() as cur:
            query_tag = snowflake.generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)
            fl_vip_zip = pd.read_sql(self.sql_query, con=conn)
        fl_vip_zip.columns = fl_vip_zip.columns.str.lower()
        total_records = fl_vip_zip.shape[0]
        print(f"Processing {total_records} rows")
        record_slice = int(total_records / self.number_of_threads)
        thread_list = []

        for thread_number in range(1, self.number_of_threads + 1):
            start = (thread_number - 1) * record_slice
            end = (
                total_records
                if thread_number == self.number_of_threads
                else start + record_slice
            )
            threads = Thread(
                target=self.thread_loop, args=(fl_vip_zip, start, end, thread_number)
            )
            threads.start()
            thread_list.append(threads)
        for threads in thread_list:
            threads.join()

        with tempfile.TemporaryDirectory() as td:
            local_path = Path(td, "tmp_file")
            fl_vip_zip.to_csv(local_path, index=False, compression="gzip")
            s3_hook = S3Hook(self.s3_conn_id)
            s3_hook.load_file(
                filename=local_path.as_posix(),
                key=self.file_key,
                bucket_name=self.bucket_name,
                replace=True,
            )

    def execute(self, context=None):
        self.process_google_api()
