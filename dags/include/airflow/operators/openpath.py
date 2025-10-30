from datetime import datetime, timezone
from typing import Dict, Iterator

import pendulum
from include.airflow.hooks.openpath import OpenpathHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator
from include.config import conn_ids


class OpenpathActivityEventsToS3Operator(BaseRowsToS3CsvOperator):
    """
    Operator for loading Openpath API activity events report data into S3.
    """

    template_fields = ["key", "req_params"]

    def __init__(
        self,
        req_params: dict,
        openpath_conn_id=conn_ids.Openpath.default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.base_url = "https://api.openpath.com"
        self.req_params = req_params
        self.openpath_conn_id = openpath_conn_id

    def yield_rows_from_data(self, data) -> Iterator[dict]:
        updated_at = pendulum.DateTime.utcnow().isoformat()
        for row in data["data"]:
            row = {
                **{key: value for key, value in row['uiData'].items() if key in self.column_list},
                "updated_at": updated_at,
            }
            yield row

    @staticmethod
    def get_epochtime(input_time):
        initial_time = datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)
        conv_input_time = datetime.strptime(input_time, '%Y-%m-%dT%H:%M:%S%z')
        req_time = conv_input_time.replace(tzinfo=timezone.utc)
        epoch_time = (req_time - initial_time).total_seconds()
        return epoch_time

    def get_rows(self) -> Iterator[dict]:
        hook = OpenpathHook(openpath_conn_id=self.openpath_conn_id)
        params = {
            'filter': f"uiData.time:({self.get_epochtime(self.req_params['start_time'])}-<{self.get_epochtime(self.req_params['end_time'])})"
        }
        request_url = f"{self.base_url}/orgs/{self.req_params['org_id']}/reports/{self.req_params['endpoint']}"
        response = hook.session.get(request_url, params=params)
        print(f"request_url: {response.url}")
        if response.status_code != 200:
            print(f"headers: {response.headers}")
            print(f"content: {response.content}")
            print(f"status_code: {response.status_code}")
            response.raise_for_status()
            raise Exception(response.content)
        data = response.json()
        while True:
            yield from self.yield_rows_from_data(data)
            if data['cursors']["hasNextPage"]:
                params['cursor'] = data['cursors']['nextCursor']
                response = hook.session.get(request_url, params=params)
                response.raise_for_status()
                data = response.json()
            else:
                break
