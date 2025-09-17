import tempfile
import time
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd
import pendulum
from functools import cached_property

from include.airflow.hooks.lithium import LithiumHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3JsonOperator
from include.utils.exponential_wait import exponential_backoff_wait_times
from include.config import conn_ids


class LithiumToS3Operator(BaseRowsToS3JsonOperator):
    template_fields = ["key", "report_params"]

    def __init__(
        self,
        report_params: dict,
        endpoint: str,
        lithium_conn_id: str = conn_ids.Lithium.default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.lithium_conn_id = lithium_conn_id
        self.report_params = report_params
        self.base_url = "https://analytics-api.app.lithium.com"
        self.url = f"{self.base_url}/api/public/reports/report/{endpoint}"

    @cached_property
    def hook(self):
        return LithiumHook(lithium_conn_id=self.lithium_conn_id)

    def time_range(self, date):
        dt = pendulum.parse(date)
        start_time = round(dt.float_timestamp * 1000)
        end_time = start_time + 86400000

        now = round(pendulum.DateTime.now().float_timestamp * 1000)
        if end_time > now:
            end_time = now
        return start_time, end_time

    def build_params(self, start_time, end_time) -> dict:
        config = {**self.report_params, "startTime": start_time, "endTime": end_time}
        return config

    def report_date_list(self):
        report_date_start = self.report_params["startTime"]
        report_date_end = self.report_params["endTime"]
        date_dt_list = pd.date_range(
            start=report_date_start, end=report_date_end, freq="D"
        )
        date_list = [x.isoformat()[0:10] for x in date_dt_list]
        return date_list

    def await_report(self, status_url):
        time.sleep(1)
        waiter = exponential_backoff_wait_times(
            growth_param=1.2,
            initial_wait=15,
            max_wait_time_seconds=60 * 60,
            max_sleep_interval=2 * 60,
        )
        while True:
            s = self.hook.make_request(status_url)
            status_response = s.json().get("result")
            status = status_response.get("result").get("detail")
            if status == "COMPLETED":
                return status_response
            else:
                wait_seconds = next(waiter)
                print(f"report is not ready; waiting {wait_seconds} seconds")
                time.sleep(wait_seconds)

    def create_report_request(self):
        r = self.hook.make_request(self.url, params=self.report_params)
        data = r.json()

        status_url = data.get("result").get("statusUrl")

        s = self.hook.make_request(status_url)
        status_response = s.json().get("result")
        status = status_response.get("result").get("detail")

        if status != "COMPLETED":
            status_response = self.await_report(status_url)

        report_url = status_response.get("jobInfo").get("downloadUrl")
        return report_url

    def get_rows(self) -> Iterable[Dict]:
        report_url = self.create_report_request()

        response = self.hook.make_request(url=report_url)
        data = response.json()

        utcnow = pendulum.DateTime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+0000")
        extra_cols = dict(updated_at=utcnow)
        for row in data:
            yield {**row, **extra_cols}

    def execute(self, context=None):
        for date in self.report_date_list():
            start_time, end_time = self.time_range(date)
            self.report_params = self.build_params(start_time, end_time)

            if "*" not in self.key:
                raise ValueError("key must contain '*' to split report per each day")
            key = self.key.replace("*", date)

            with tempfile.TemporaryDirectory() as td:
                full_local_path = Path(td, date)
                rows = self.get_rows()
                self.write_dict_rows_to_file(rows=rows, filename=full_local_path)
                self.upload_to_s3(
                    full_local_path.as_posix(), bucket=self.bucket, key=key
                )
