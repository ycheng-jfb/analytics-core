from typing import Iterator

import base64
import pendulum
import requests
from airflow.hooks.base import BaseHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator
from include.config import conn_ids


class StoreforceToS3Operator(BaseRowsToS3CsvOperator):
    template_fields = ["key"]

    def __init__(
        self,
        endpoint,
        storeforce_conn_id="storeforce_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.store_force_conn_id = storeforce_conn_id
        self.endpoint = endpoint

    def yield_rows_from_data(self, data) -> Iterator[dict]:
        updated_at = pendulum.DateTime.utcnow().isoformat()
        for row in data["result"]["storeKPIs"]:
            row = {
                **{key: value for key, value in row.items() if key in self.column_list},
                "updated_at": updated_at,
            }
            yield row

    def get_rows(self) -> Iterator[dict]:
        conn = BaseHook.get_connection(self.store_force_conn_id)
        url = f"https://{conn.host}/exportapi/v1/api/Export/{self.endpoint}/"
        auth_creds = base64.b64encode(f"{conn.login}:{conn.password}".encode())
        headers = {
            "Ocp-Apim-Subscription-Key": conn.extra_dejson.get("subscription_key"),
            "Authorization": f"Basic {auth_creds.decode()}",
        }

        response = requests.get(url=url, headers=headers)
        data = response.json()
        if response.status_code != 200:
            print(f"headers: {response.headers}")
            print(f"content: {response.content}")
            print(f"status_code: {response.status_code}")
            response.raise_for_status()
            raise Exception(response.content)
        yield from self.yield_rows_from_data(data)
