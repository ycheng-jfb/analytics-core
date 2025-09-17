from typing import Iterator
from datetime import datetime, timedelta

import requests
import pendulum
from airflow.hooks.base import BaseHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator


class ApplovinToS3Operator(BaseRowsToS3CsvOperator):
    template_fields = ["time_period", "key"]

    def __init__(
        self,
        config: dict,
        advertiser_id: str,
        time_period: dict,
        applovin_conn_id='applovin_default',
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.applovin_conn_id = applovin_conn_id
        self.config = config
        self.advertiser_id = advertiser_id
        self.time_period = time_period

    def split_dates(self, start_time, end_time):
        start_time = datetime.strptime(start_time, '%Y-%m-%d')
        end_time = datetime.strptime(end_time, '%Y-%m-%d')
        split_times = set()
        interval = timedelta(days=1)
        current_time = start_time
        while current_time < end_time:
            split_times.add(current_time.strftime('%Y-%m-%d'))
            current_time += interval
        split_times.add(end_time.strftime('%Y-%m-%d'))
        return split_times

    def get_rows(self) -> Iterator[dict]:
        conn = BaseHook.get_connection(self.applovin_conn_id)
        url = f"https://{conn.host}/report"
        self.log.info(f"Passed params are: {self.config}")
        split_times_full = self.split_dates(self.time_period['start'], self.time_period['end'])
        self.config['api_key'] = f'{conn.password}'
        for date in split_times_full:
            self.log.info(f"Processing the data for {date}")
            self.config['start'] = self.config['end'] = date
            response = requests.get(url=url, params=self.config)
            if response.status_code == 200:
                data = response.json()
                for rec in data['results']:
                    rec['advertiser_id'] = self.advertiser_id
                    rec['api_call_timestamp'] = pendulum.DateTime.utcnow().isoformat()
                    yield rec
            else:
                print(f"headers: {response.headers}")
                print(f"content: {response.content}")
                print(f"status_code: {response.status_code}")
                response.raise_for_status()
                raise Exception(response.content)
