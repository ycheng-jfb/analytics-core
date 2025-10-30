from typing import Iterator
from datetime import datetime, timedelta

import pendulum
import requests
from airflow.hooks.base import BaseHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator


class CognigyToS3Operator(BaseRowsToS3CsvOperator):
    template_fields = ["req_params", "key"]

    def __init__(
        self,
        req_params,
        cognigy_conn_id='cognigy_default',
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cognigy_conn_id = cognigy_conn_id
        self.req_params = req_params

    def split_dates(self, start_time, end_time):
        start_time = datetime.fromisoformat(start_time)
        end_time = datetime.fromisoformat(end_time)
        split_times = []
        interval = timedelta(hours=self.req_params['interval'])
        current_time = start_time
        while current_time < end_time:
            split_times.append(current_time)
            current_time += interval
        return split_times + [end_time]

    def yield_rows_from_data(self, data) -> Iterator[dict]:
        updated_at = pendulum.DateTime.utcnow().isoformat()
        for row in data["value"]:
            row["updated_at"] = updated_at
            yield row

    def get_rows(self) -> Iterator[dict]:
        conn = BaseHook.get_connection(self.cognigy_conn_id)
        url = (
            f"https://{conn.host}/{conn.extra_dejson.get('version')}/{self.req_params['endpoint']}/"
        )
        self.log.info(f"Passed params are: {self.req_params}")
        split_times_full = self.split_dates(
            self.req_params['start_time'], self.req_params['end_time']
        )
        for i in range(len(split_times_full) - 1):
            self.log.info(
                f"Processing the data from {split_times_full[i]} to {split_times_full[i+1]}"
            )
            par = {}
            if self.req_params["date_column"]:
                par['$filter'] = (
                    f"{self.req_params['date_column']}%20gt%20{str(split_times_full[i]).replace(' ', 'T')}Z%20and%20{self.req_params['date_column']}%20lt%20{str(split_times_full[i+1]).replace(' ', 'T')}Z%20and%20projectId%20eq%20'{self.req_params['project_id']}'",
                )
            par['apikey'] = f'{conn.password}'
            response = requests.get(url=url, params=par)
            data = response.json()
            if response.status_code != 200:
                print(f"headers: {response.headers}")
                print(f"content: {response.content}")
                print(f"status_code: {response.status_code}")
                response.raise_for_status()
                raise Exception(response.content)
            yield from self.yield_rows_from_data(data)
