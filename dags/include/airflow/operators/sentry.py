from airflow.hooks.base import BaseHook
from functools import cached_property
from typing import Iterator

import pendulum
import re
import requests
import time

from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.rows_to_s3 import (
    BaseRowsToS3CsvOperator,
    BaseRowsToS3JsonWatermarkOperator,
)
from include.config import conn_ids
from include.utils.snowflake import generate_query_tag_cmd


class SentryJsonToS3Operator(BaseRowsToS3JsonWatermarkOperator):
    template_fields = ["key"]

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook()

    def get_high_watermark(self) -> str:
        cur = self.snowflake_hook.get_cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        sql = """
            SELECT MAX(meta_update_datetime)
            FROM lake.sentry.events;
        """
        cur.execute(sql)
        result = cur.fetchall()
        return str(result[0][0])

    def get_rows(self) -> Iterator[dict]:
        conn = BaseHook.get_connection(conn_ids.Sentry.default)
        cur = self.snowflake_hook.get_cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        query = f"""
            SELECT id, project_name
            from lake.sentry.events
            where meta_update_datetime > '{self.low_watermark}'
            """
        cur.execute(query)
        response = cur.fetchall()
        for val in response:
            url = f"https://{conn.host}/api/0/projects/fabletics/{val[1]}/events/{val[0]}/"
            headers = {"Authorization": f"Bearer {conn.password}"}
            response = requests.get(url=url, headers=headers)
            if response.status_code != 200:
                print(f"headers: {response.headers}")
                print(f"content: {response.content}")
                print(f"status_code: {response.status_code}")
                response.raise_for_status()
                raise Exception(response.content)
            yield response.json()


class SentryToS3Operator(BaseRowsToS3CsvOperator):
    template_fields = ["key"]

    def __init__(
        self,
        config: dict,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.config = config

    def yield_rows_from_data(self, data) -> Iterator[dict]:
        updated_at = pendulum.DateTime.utcnow().isoformat()
        for row in data["data"]:
            row = {
                **{key: value for key, value in row.items() if key in self.column_list},
                "updated_at": updated_at,
            }
            yield row

    def get_rows(self) -> Iterator[dict]:
        conn = BaseHook.get_connection(conn_ids.Sentry.default)
        url = f"https://{conn.host}/api/0/organizations/fabletics/events/"
        headers = {"Authorization": f"Bearer {conn.password}"}
        response = requests.get(url=url, headers=headers, params=self.config)
        if response.status_code != 200:
            print(f"headers: {response.headers}")
            print(f"content: {response.content}")
            print(f"status_code: {response.status_code}")
            response.raise_for_status()
            raise Exception(response.content)

        while True:
            if int(response.headers["x-sentry-rate-limit-remaining"]) == 0:
                current_time = int(time.time())
                rate_limit_reset_time = int(
                    response.headers["x-sentry-rate-limit-reset"]
                )
                wait_time = abs(rate_limit_reset_time - current_time)
                self.log.info(
                    f"Rate limit exceeded and it will reset in {wait_time/60} minute(s)"
                )
                time.sleep(wait_time)

            data = response.json()
            yield from self.yield_rows_from_data(data)
            has_next_page = re.findall(r'results="([^"]+)"', response.headers["link"])[
                1
            ]
            if has_next_page == "true":
                print("Pulling the next 100 records")
                self.config["cursor"] = re.findall(
                    r'cursor="([^"]+)"', response.headers["link"]
                )[1]
                response = requests.get(url=url, headers=headers, params=self.config)
            else:
                break
