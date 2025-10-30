import gzip
import json
import tempfile
import time
from datetime import datetime
from functools import cached_property, reduce
from pathlib import Path
from typing import Iterator
from urllib.parse import urlencode

import pandas as pd
import pendulum
import requests
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.airflow.hooks.genesys import GenesysPureCloudHook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.rows_to_s3 import (
    BaseRowsToS3CsvOperator,
    BaseRowsToS3CsvWatermarkOperator,
)
from include.airflow.utils.utils import flatten_json
from include.utils import snowflake
from include.config import conn_ids
from include.utils.decorators import retry_wrapper


class TooMuchDataException(Exception):
    """Raise exception when Genesys API errors due to a result set that is too large"""


class RetryException(Exception):
    """Raise exception to retry function call"""


class GenesysConversationsToS3Operator(BaseOperator):
    template_fields = ["key", "from_date", "to_date"]

    def __init__(
        self,
        from_date: str,
        to_date: str,
        key: str,
        bucket: str,
        fail_on_no_rows: bool = False,
        genesys_conn_id: str = conn_ids.Genesys.genesys_bond,
        s3_conn_id: str = conn_ids.AWS.tfg_default,
        next_page: str = None,
        *args,
        **kwargs,
    ):
        self.from_date = from_date
        self.to_date = to_date
        self.key = key
        self.bucket = bucket
        self.fail_on_no_rows = fail_on_no_rows
        self.genesys_conn_id = genesys_conn_id
        self.s3_conn_id = s3_conn_id
        self._session = None
        self.rows_exported = 0
        self.next_page = next_page
        super().__init__(*args, **kwargs)

    @cached_property
    def session(self):
        creds = BaseHook.get_connection(self.genesys_conn_id)  # type: Connection
        params = {"grant_type": "client_credentials"}
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        r = requests.post(
            "https://login.mypurecloud.com/oauth/token",
            data=params,
            headers=headers,
            auth=(creds.login, creds.password),
        )
        r.raise_for_status()

        access_token = r.json()["access_token"]

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        session = requests.session()
        session.headers = headers
        return session

    def get_params(self, page_number, page_size=10):
        params = {
            "interval": f"{self.from_date}/{self.to_date}",
            "order": "asc",
            "orderBy": "conversationStart",
            "paging": {"pageSize": page_size, "pageNumber": page_number},
        }
        return params

    def write_to_file(self, filename):

        if not filename.endswith('.gz'):
            raise ValueError(f"File will be gzipped but filename {filename} does not end in .gz")

        with gzip.open(filename, "wt") as f:
            page_number = 0
            req_number = 0
            retry_after = 0
            curr_hour = datetime.now().strftime("%H")
            curr_min = datetime.now().strftime("%M")
            while True:
                page_number += 1
                print(page_number)
                req_number += 1

                if (
                    datetime.now().strftime("%H") != curr_hour
                    or datetime.now().strftime("%M") != curr_min
                ):
                    curr_hour = datetime.now().strftime("%H")
                    curr_min = datetime.now().strftime("%M")
                    req_number = 0

                # Handle API Rate Limits
                if retry_after > 0:
                    print("Sleeping for ", retry_after, " seconds to handle Rate Limits.")
                    time.sleep(retry_after)

                params = self.get_params(page_number)
                r = self.session.post(
                    url="https://api.mypurecloud.com/api/v2/analytics/conversations/details/query",
                    json=params,
                )

                data = r.json()
                if r.status_code == 200:
                    retry_after = 0
                # Don't fail on 429 or 50X, resolve Retry-After to work-around API Rate Limits
                elif r.status_code in [429, 502, 503, 504]:
                    print(r.headers)
                    retry_after = int(r.headers['Retry-After']) + 1
                    page_number += -1
                    req_number += -1
                    continue
                else:
                    r.raise_for_status()
                if not data:
                    print("no data")
                    break
                if "conversations" in data:
                    for row in data["conversations"]:
                        self.rows_exported += 1
                        f.write(json.dumps(row))
                        f.write("\n")
                else:
                    print(data)
                    print("no conversations in data")
                    break

        print(f"rows exported: {self.rows_exported}")

    def execute(self, context=None):
        with tempfile.TemporaryDirectory() as td:
            local_path = (Path(td) / Path(self.key).name).as_posix()
            self.write_to_file(local_path)
            if self.rows_exported == 0 and self.fail_on_no_rows:
                raise ValueError("no rows exported; must retry later")

            if self.rows_exported:
                s3_hook = S3Hook(self.s3_conn_id)
                s3_hook.load_file(
                    filename=local_path,
                    key=self.key,
                    bucket_name=self.bucket,
                    replace=True,
                )


class GenesysEntitiesToS3Operator(BaseRowsToS3CsvOperator):
    template_fields = ["key"]

    def __init__(
        self,
        endpoint: str,
        extra_params: dict = None,
        api_version="v2",
        genesys_conn_id=conn_ids.Genesys.default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.api_version = api_version
        self.base_url = "https://api.mypurecloud.com"
        self.endpoint = endpoint
        self.extra_params = extra_params
        self.page_size = 100
        self.genesys_conn_id = genesys_conn_id

    def yield_rows_from_data(self, data) -> Iterator[dict]:
        updated_at = pendulum.DateTime.utcnow().isoformat()
        for entity in data["entities"]:
            row = {
                **{key: value for key, value in entity.items() if key in self.column_list},
                "updated_at": updated_at,
            }
            yield row

    def get_rows(self) -> Iterator[dict]:
        hook = GenesysPureCloudHook(genesys_conn_id=self.genesys_conn_id)
        request_url = (
            f"{self.base_url}/api/{self.api_version}/{self.endpoint}?"
            f"pageSize={self.page_size}&"
            f"pageNumber=1"
        )
        if self.extra_params:
            request_url = f"{request_url}&{urlencode(self.extra_params)}"
        print(f"request_url: {request_url}")
        print(f"endpoint: {self.endpoint}")
        response = hook.session.get(request_url)
        if response.status_code != 200:
            print(f"headers: {response.headers}")
            print(f"content: {response.content}")
            print(f"status_code: {response.status_code}")
            response.raise_for_status()
            raise Exception(response.content)
        data = response.json()
        while True:
            yield from self.yield_rows_from_data(data)
            if data.get("nextUri") and data.get("pageNumber") != data.get("pageCount"):
                print(f'found next page {data["nextUri"]}')
                response = hook.session.get(f'{self.base_url}{data["nextUri"]}')
                response.raise_for_status()
                data = response.json()
            else:
                break


class GenesysFlowOutcomesToS3Operator(GenesysConversationsToS3Operator):
    minute_intervals = [60, 30, 10, 1]

    def __init__(
        self,
        key,
        bucket,
        from_date=str(pendulum.DateTime.utcnow().add(days=-3)),
        to_date=str(pendulum.DateTime.utcnow()),
        *args,
        **kwargs,
    ):
        super().__init__(
            from_date=from_date, to_date=to_date, key=key, bucket=bucket, *args, **kwargs
        )
        self.interval_size = self.minute_intervals[0]
        self.current_position = self.from_date_dt
        self.failed_position = None

    @cached_property
    def from_date_dt(self):
        return pendulum.parse(self.from_date)

    @cached_property
    def to_date_dt(self):
        return pendulum.parse(self.to_date)

    @staticmethod
    def get_params_flow_outcomes(start_interval, end_interval):
        params = {
            "interval": f"{start_interval}/{end_interval}",
            "groupBy": [
                "conversationID",
                "flowOutcome",
                "flowOutcomeID",
                "flowOutcomeValue",
                "flowName",
            ],
            "views": [],
            "metrics": ["nFlowOutcome"],
        }
        return params

    def decrease_interval_size(self):
        current_index = self.minute_intervals.index(self.interval_size)
        self.interval_size = self.minute_intervals[current_index + 1]

    def reset_interval_size_if_ok(self):
        passed_enough_time = self.current_position.diff(self.failed_position).in_hours() >= 1
        interval_size_is_not_default = self.interval_size != self.minute_intervals[0]
        if interval_size_is_not_default and passed_enough_time:
            print("Resetting to original interval size.")
            self.interval_size = self.minute_intervals[0]

    def mark_failure(self, current_position):
        self.failed_position = current_position

    @retry_wrapper(3, RetryException, sleep_time=30)
    def make_request(self, params):
        """
        Will hit endpoint and retry up to 3 times status_code == 504
        No retries will be performed for any other error; other errors will simply be raised.
        """
        url = "https://api.mypurecloud.com/api/v2/analytics/flows/aggregates/query"
        r = self.session.post(url=url, json=params)
        try:
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as e:
            if 'limit of 30000 results' or 'Result set is larger than result limit' in r.text:
                raise TooMuchDataException
            elif r.status_code == 504:  # Internal Server Error
                raise RetryException
            else:
                print(r.text)
                raise e

    def get_rows(self):
        """
        Consume all rows from API and yield them.
        If a TooMuchDataException is encountered, decrease interval size.
        If no error, increment current position and check if interval size can be reset.
        """
        while (
            self.current_position.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
            < self.to_date_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        ):
            try:
                interval_end = min(
                    [self.current_position.add(minutes=self.interval_size), self.to_date_dt]
                )
                print(f"Running {self.current_position} to {interval_end}")
                params = self.get_params_flow_outcomes(self.current_position, interval_end)
                r = self.make_request(params)
                data = r.json()
                results = data.get('results', [])
                for row in results:
                    yield row
                print(f"exported rows: {len(data)}")
                self.current_position = interval_end
                self.reset_interval_size_if_ok()
            except TooMuchDataException:
                print("Too much data. Decreasing interval size.")
                self.mark_failure(self.current_position)
                self.decrease_interval_size()

    def write_to_file(self, filename):
        if not filename.endswith('.gz'):
            raise ValueError(f"File will be gzipped but filename {filename} does not end in .gz")

        with gzip.open(filename, "wt") as f:
            updated_at = str(pendulum.DateTime.utcnow())
            data = self.get_rows()
            for row in data:
                self.rows_exported += 1
                row['group']['updated_at'] = updated_at
                f.write(json.dumps(row['group']))
                f.write("\n")
        print(f"total rows exported: {self.rows_exported}")


class GenesysParticipantsToS3Operator(GenesysConversationsToS3Operator):
    def get_payload(self):
        payload = {
            "interval": f"{self.from_date}/{self.to_date}",
            "orderBy": "conversationStart",
            "order": "asc",
        }
        return payload

    def create_job(self):
        payload = self.get_payload()
        r = self.session.post(
            url="https://api.mypurecloud.com/api/v2/analytics/conversations/details/jobs",
            json=payload,
        )
        r.raise_for_status()
        resp = r.json()
        if "jobId" in resp:
            return resp["jobId"]
        else:
            return None

    def check_job_status(self, job_id):
        max_timeout = 1800
        poke_interval = 60
        duration = 0
        job_status_list = ['FAILED', 'CANCELLED', 'FULFILLED', 'EXPIRED']
        while True:
            time.sleep(poke_interval)
            duration = duration + poke_interval
            r = self.session.get(
                url=f"https://api.mypurecloud.com/api/v2/analytics/conversations/details/jobs/{job_id}"
            )
            r.raise_for_status()
            resp = r.json()

            if resp['state'] in job_status_list:
                job_status = resp['state']
                break
            elif duration > max_timeout:
                job_status = "MaxTimeout"
                break
            else:
                pass
        return job_status

    @staticmethod
    def get_value(data, keys, default=None):
        return reduce(
            lambda d, key: d.get(key, default) if isinstance(d, dict) else default,
            keys.split("/"),
            data,
        )

    def get_participant_attributes(self, data):
        p_attributes_list = []
        for row in data:
            if self.get_value(row, 'purpose') == 'customer':
                p_attributes = dict(
                    participant_id=self.get_value(row, 'participantId'),
                    participant_name=self.get_value(row, 'participantName'),
                    purpose=self.get_value(row, 'purpose'),
                    first_name=self.get_value(row, 'attributes/context.firstName'),
                    last_name=self.get_value(row, 'attributes/context.lastName'),
                    email=self.get_value(row, 'attributes/context.email'),
                    phone_number=self.get_value(row, 'attributes/context.phoneNumber'),
                    address_street=self.get_value(row, 'attributes/context.addressStreet'),
                    address_city=self.get_value(row, 'attributes/context.addressCity'),
                    address_state=self.get_value(row, 'attributes/context.addressState'),
                    address_postal_code=self.get_value(row, 'attributes/context.addressPostalCode'),
                    legacy_routing_target_queue_address=self.get_value(
                        row, 'attributes/context.genesys.legacyRoutingTargetQueueAddress'
                    ),
                    gdf_intent_par_data=self.get_value(row, 'attributes/Flow.GDFIntentParData'),
                    queue_id=self.get_value(row, 'attributes/queueId'),
                    brand_url_set=self.get_value(row, 'attributes/BRAND_URL_SET'),
                    parameters=self.get_value(row, 'attributes/Parameters'),
                    queue_default=self.get_value(row, 'attributes/queueDefault'),
                    cancellation_reason=self.get_value(row, 'attributes/cancellationReason'),
                    endpoint_url_token=self.get_value(row, 'attributes/endpointUrlToken'),
                    default_queue=self.get_value(row, 'attributes/defaultQueue'),
                    language=self.get_value(row, 'attributes/language'),
                    session_id=self.get_value(row, 'attributes/sessionId'),
                    locale=self.get_value(row, 'attributes/locale'),
                    intent=self.get_value(row, 'attributes/intent'),
                    user_id=self.get_value(row, 'attributes/userId'),
                    script_id=self.get_value(row, 'attributes/scriptId'),
                    store_group_id=self.get_value(row, 'attributes/storeGroupId'),
                    customer_id=self.get_value(row, 'attributes/customerId'),
                    genesys_language=self.get_value(row, 'attributes/genesysLanguage'),
                    name=self.get_value(row, 'attributes/name'),
                    brand=self.get_value(row, 'attributes/brand'),
                    attribute_email=self.get_value(row, 'attributes/email'),
                    goals=self.get_value(row, 'attributes/goals'),
                    purecloud=self.get_value(row, 'attributes/context.purecloud'),
                    custom_field1=self.get_value(row, 'attributes/context.customField1'),
                    custom_field2=self.get_value(row, 'attributes/context.customField2'),
                    custom_field3=self.get_value(row, 'attributes/context.customField3'),
                    custom_field4=self.get_value(row, 'attributes/context.customField4'),
                    custom_field1_label=self.get_value(row, 'attributes/context.customField1Label'),
                    custom_field2_label=self.get_value(row, 'attributes/context.customField2Label'),
                    custom_field3_label=self.get_value(row, 'attributes/context.customField3Label'),
                    custom_field4_label=self.get_value(row, 'attributes/context.customField4Label'),
                )
                p_attributes_list.append(p_attributes)
        return p_attributes_list

    def get_transformed_data(self, data):
        participant_data_list = []
        for row in data:
            participant_data = dict(
                conversation_id=self.get_value(row, 'conversationId'),
                conversation_start=self.get_value(row, 'conversationStart'),
                conversation_end=self.get_value(row, 'conversationEnd'),
                participants=self.get_value(row, 'participants'),
            )
            p_attributes = (
                self.get_participant_attributes(participant_data['participants'])
                if participant_data['participants']
                else None
            )
            participant_data.pop('participants')
            if p_attributes:
                for item in p_attributes:
                    p_attributes_data = participant_data.copy()
                    p_attributes_data['participant_id'] = item['participant_id']
                    p_attributes_data['participant_name'] = item['participant_name']
                    p_attributes_data['purpose'] = item['purpose']
                    p_attributes_data['first_name'] = item['first_name']
                    p_attributes_data['last_name'] = item['last_name']
                    p_attributes_data['email'] = item['email']
                    p_attributes_data['phone_number'] = item['phone_number']
                    p_attributes_data['address_street'] = item['address_street']
                    p_attributes_data['address_city'] = item['address_city']
                    p_attributes_data['address_state'] = item['address_state']
                    p_attributes_data['address_postal_code'] = item['address_postal_code']
                    p_attributes_data['legacy_routing_target_queue_address'] = item[
                        'legacy_routing_target_queue_address'
                    ]
                    p_attributes_data['gdf_intent_par_data'] = item['gdf_intent_par_data']
                    p_attributes_data['queue_id'] = item['queue_id']
                    p_attributes_data['brand_url_set'] = item['brand_url_set']
                    p_attributes_data['parameters'] = item['parameters']
                    p_attributes_data['queue_default'] = item['queue_default']
                    p_attributes_data['cancellation_reason'] = item['cancellation_reason']
                    p_attributes_data['endpoint_url_token'] = item['endpoint_url_token']
                    p_attributes_data['default_queue'] = item['default_queue']
                    p_attributes_data['language'] = item['language']
                    p_attributes_data['session_id'] = item['session_id']
                    p_attributes_data['locale'] = item['locale']
                    p_attributes_data['intent'] = item['intent']
                    p_attributes_data['user_id'] = item['user_id']
                    p_attributes_data['script_id'] = item['script_id']
                    p_attributes_data['store_group_id'] = item['store_group_id']
                    p_attributes_data['customer_id'] = item['customer_id']
                    p_attributes_data['genesys_language'] = item['genesys_language']
                    p_attributes_data['name'] = item['name']
                    p_attributes_data['brand'] = item['brand']
                    p_attributes_data['attribute_email'] = item['attribute_email']
                    p_attributes_data['goals'] = item['goals']
                    p_attributes_data['purecloud'] = item['purecloud']
                    p_attributes_data['custom_field1'] = item['custom_field1']
                    p_attributes_data['custom_field2'] = item['custom_field2']
                    p_attributes_data['custom_field3'] = item['custom_field3']
                    p_attributes_data['custom_field4'] = item['custom_field4']
                    p_attributes_data['custom_field1_label'] = item['custom_field1_label']
                    p_attributes_data['custom_field2_label'] = item['custom_field2_label']
                    p_attributes_data['custom_field3_label'] = item['custom_field3_label']
                    p_attributes_data['custom_field4_label'] = item['custom_field4_label']
                    participant_data_list.append(p_attributes_data)
        return participant_data_list

    def write_to_file(self, filename):
        batch_size = "1000"
        batch_num = 1
        job_id = self.create_job()
        # job_id = "809d0083-5dd1-41c8-b4d0-79ceab1970fd"
        print(f"job Id: {job_id}")
        if job_id:
            job_status = self.check_job_status(job_id)
            print(f"job status: {job_status}")
            if job_status == 'FULFILLED':
                if self.next_page:
                    params = dict(cursor=self.next_page, pageSize=batch_size)
                else:
                    params = dict(pageSize=batch_size)
                while True:
                    print(f"Loading batch: {batch_num}")
                    s3_key = f"{filename}-{batch_num}.ndjson.gz"
                    with tempfile.TemporaryDirectory() as td:
                        local_path = (Path(td) / Path(s3_key).name).as_posix()
                        r = self.session.get(
                            url="https://api.mypurecloud.com/api/v2/analytics/"
                            f"conversations/details/jobs/{job_id}/results",
                            # noqa: E501
                            params=params,
                        )
                        r.raise_for_status()
                        data = r.json()
                        if not data:
                            print("no data")
                            break
                        if "conversations" in data:
                            with gzip.open(local_path, "wt") as f:
                                for row in self.get_transformed_data(data["conversations"]):
                                    self.rows_exported += 1
                                    f.write(json.dumps(row))
                                    f.write("\n")
                        else:
                            print("no conversations in data")
                            break
                        self.upload_to_s3(local_path, s3_key)
                        if 'cursor' in data:
                            print(f"cursor: {data['cursor']}")
                            params = dict(cursor=data['cursor'], pageSize=batch_size)
                        else:
                            break
                        batch_num = batch_num + 1

                print(f"rows exported: {self.rows_exported}")
            else:
                print(f"Job is not completed. Here is the job status {job_status}")
        else:
            print("Job creation is failed")

    @cached_property
    def s3_hook(self):
        return S3Hook(self.s3_conn_id)

    def upload_to_s3(self, filename, s3_key):
        self.s3_hook.load_file(
            filename=filename,
            key=s3_key,
            bucket_name=self.bucket,
            replace=True,
        )
        print(f"Uploaded file: {s3_key}")

    def execute(self, context=None):
        self.write_to_file(self.key)


class GenesysConversationAnalyticsToS3(BaseRowsToS3CsvWatermarkOperator):
    def get_high_watermark(self) -> str:
        sql = (
            "select MAX(META_UPDATE_DATETIME) "
            "from REPORTING_PROD.GMS.GENESYS_CONVERSATION "
            "where MEDIA_TYPE = 'voice';"
        )
        conn = self.snowflake_hook.get_conn()
        cur = conn.cursor()
        query_tag = snowflake.generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        cur.execute(sql)
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=['META_UPDATE_DATETIME'])
        return str(df.iat[0, 0].isoformat())

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook()

    @cached_property
    def genesys_hook(self):
        return GenesysPureCloudHook(genesys_conn_id=self.hook_conn_id)

    def get_rows(self):
        updated_at_d = {"updated_at": pendulum.DateTime.utcnow()}
        sql = (
            "select distinct CONVERSATION_ID "
            "from REPORTING_PROD.GMS.GENESYS_CONVERSATION "
            f"where meta_update_datetime > '{self.low_watermark}' "
            f"and meta_update_datetime < '{str(pendulum.parse(self.low_watermark).add(days=5))}' "
            "and MEDIA_TYPE = 'voice';"
        )
        conn = self.snowflake_hook.get_conn()
        cur = conn.cursor()
        query_tag = snowflake.generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        cur.execute(sql)
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=['CONVERSATION_ID'])
        conversation_ids = list(df['CONVERSATION_ID'])
        for conversation_id in conversation_ids:
            try:
                r = self.genesys_hook.session.get(
                    f'https://api.mypurecloud.com/api/v2/'
                    f'speechandtextanalytics/conversations/{conversation_id}'
                )
                r.raise_for_status()

                yield {**flatten_json(r.json()), **updated_at_d}

            except requests.exceptions.HTTPError as err:
                if err.response.status_code != 404 and err.response.status_code != 500:
                    raise


class GenesysRoutingQueuesToS3(BaseRowsToS3CsvOperator):
    def __init__(
        self,
        api_version: str,
        genesys_conn_id: str = "genesys_bond",
        base_url: str = "https://api.mypurecloud.com",
        *args,
        **kwargs,
    ):
        self.genesys_conn_id = genesys_conn_id
        self.api_version = api_version
        self.base_url = base_url
        super().__init__(*args, **kwargs)

    @cached_property
    def session(self):
        creds = BaseHook.get_connection(self.genesys_conn_id)  # type: Connection
        params = {"grant_type": "client_credentials"}
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        r = requests.post(
            "https://login.mypurecloud.com/oauth/token",
            data=params,
            headers=headers,
            auth=(creds.login, creds.password),
        )
        r.raise_for_status()

        access_token = r.json()["access_token"]

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        session = requests.session()
        session.headers = headers
        return session

    def get_routing_queues(self):
        params = {"pageNumber": 1}
        while True:
            response = self.session.get(
                f"{self.base_url}/api/{self.api_version}/routing/queues", params=params
            )
            response.raise_for_status()

            if not response.json().get("entities"):
                break
            for queue in response.json().get("entities"):
                yield queue
            params["pageNumber"] += 1

    def get_rows(self):
        for queue in self.get_routing_queues():
            yield queue
