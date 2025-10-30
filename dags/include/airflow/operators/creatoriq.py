import asyncio
import tempfile
import time
from abc import abstractmethod
from pathlib import Path
from typing import Any, Dict, List

import aiohttp
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from functools import cached_property

from include.airflow.hooks.creatoriq import CreatorIQHook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.utils.exponential_wait import exponential_backoff_wait_times
from include.utils.s3 import get_filename
from include.utils.snowflake import generate_query_tag_cmd
from include.config import conn_ids


class CreatorIQToS3Operator(BaseOperator):
    """
    This operator needs to be inherited and get_rows method should be implemented to
    specify the request url and process the json data returned.
    """

    template_fields = ("key",)

    def __init__(
        self,
        bucket: str,
        key: str,
        column_list: List[str],
        creatoriq_conn_id: str = conn_ids.CreatorIQ.creatoriq_default,
        s3_conn_id: str = conn_ids.AWS.tfg_default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.key = key
        self.column_list = column_list
        self.s3_conn_id = s3_conn_id
        self.creatoriq_conn_id = creatoriq_conn_id
        self.hook = None

    def upload_file_to_s3(self, file):
        """
        Uploads file to the S3
        Args:
            file: filename
        """
        s3_hook = S3Hook(self.s3_conn_id)
        if file:
            s3_hook.load_file(filename=file, key=self.key, bucket_name=self.bucket, replace=True)
            print(f"uploaded to {self.key}")

    def get_sql_data(self):
        """
        Executes the sql command.
        Returns: Returns the results of sql query execution.
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        with snowflake_hook.get_conn() as cnx:
            self.log.info("Executing: %s", self.sql_cmd)
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = cnx.cursor()
            cur.execute(query_tag)
            cur.execute(self.sql_cmd)
            rows = cur.fetchall()
        return rows

    @staticmethod
    def merge_dicts(*dicts):
        """
        Takes multiple dictionaries and combine them and returns a single dictionary
        Args:
            *dicts:
        Returns:
            dict
        """
        d = dict()
        for x in dicts:
            d.update(x)
        return d

    @abstractmethod
    def get_rows(self):
        """
        Returns: a list of dicts that can be used to write into the file.
        """
        pass

    def get_hook(self, **kwargs):
        return CreatorIQHook(creatoriq_conn_id=self.creatoriq_conn_id, **kwargs)

    def execute(self, context=None):
        self.hook = self.get_hook()
        with tempfile.TemporaryDirectory() as td:
            local_file_path = Path(td, get_filename(self.key))
            self.hook.get_dict_rows_to_file(
                filename=local_file_path.as_posix(),
                column_list=self.column_list,
                rows=self.get_rows(),
            )
            self.upload_file_to_s3(file=local_file_path.as_posix())


class CreatorIQPatchOperator(BaseOperator):
    """
    This operator performs patch on the campaign ids returned from the given sql.
    """

    template_fields = ("sql_cmd",)

    def __init__(
        self,
        sql_cmd: str,
        creatoriq_conn_id: str = conn_ids.CreatorIQ.creatoriq_default,
        snowflake_conn_id: str = conn_ids.Snowflake.default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.creatoriq_conn_id = creatoriq_conn_id
        self.sql_cmd = sql_cmd

    @cached_property
    def hook(self):
        return CreatorIQHook(creatoriq_conn_id=self.creatoriq_conn_id, key_name="x-api-key")

    async def patch(
        self, session: aiohttp.ClientSession, semaphore, id: str, **kwargs
    ) -> Dict[Any, Any]:
        """
        raises patch requests in async
        Args:
            session: asyncio session
            semaphore: this defines the number of requests that can run in parallel
            id: campaign id for the patch

        Returns:
            returns the json response.
        """
        url = f'https://apis.creatoriq.com/crm/v1/api/campaign/{id}/updateActivity'
        async with semaphore:
            print(f"Requesting {url}")
            resp = await session.request('PATCH', url=url, **kwargs)
            response = await resp.json()
            response['CampaignId'] = id
        return dict(response)

    async def make_request_asnyc(self, id_list, **kwargs):
        """
        Creates the async requests
        Args:
            id_list: list of all campaign ids

        Returns:
            returns the responses from async requests
        """
        semaphore = asyncio.BoundedSemaphore(value=10)
        session = self.hook.get_async_conn()
        async with session:
            tasks = []
            for id_ in id_list:
                tasks.append(self.patch(session=session, semaphore=semaphore, id=id_, **kwargs))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            return responses

    def get_id_list(self):
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        with snowflake_hook.get_conn() as cnx:
            self.log.info("Executing: %s", self.sql_cmd)
            cur = cnx.cursor()
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)
            cur.execute(self.sql_cmd)
            id_list = [x[0] for x in cur.fetchall()]
        self.log.info(id_list)
        return id_list

    def execute(self, context):
        id_list = self.get_id_list()
        waiter = exponential_backoff_wait_times(
            growth_param=2.2, initial_wait=1.5, max_wait_time_seconds=60 * 60 * 1.2
        )
        completed = []
        while True:
            try:
                responses = asyncio.run(self.make_request_asnyc(id_list))
                for response in responses:
                    status = response.get('TaskStatus', None)
                    if status == "DONE":
                        completed.extend([response['CampaignId']])
                    if status is None:
                        print("request failed")
                        print(response)
                print(f"completed campaign list {completed}")
                id_list = list(set(id_list) - set(completed))
                if id_list:
                    wait_seconds = next(waiter)
                    print(f"campaigns {id_list} not done; waiting {wait_seconds} seconds")
                    time.sleep(wait_seconds)
                else:
                    break
            except Exception as e:
                print(f"unable to complete patch for {id_list} campaign ids in 1 hour")
                raise e(f"unable to complete patch for {id_list} campaign ids in 1 hour")
