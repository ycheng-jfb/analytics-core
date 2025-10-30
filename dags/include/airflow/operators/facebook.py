import json
import pendulum
import requests
import time

from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from typing import Dict, Iterable

from include.airflow.hooks.facebook import FacebookAdsHook
from include.airflow.operators.rows_to_s3 import BaseRowsToS3CsvOperator
from include.utils.exponential_wait import exponential_backoff_wait_times
from include.airflow.hooks.snowflake import SnowflakeHook
from include.utils.exponential_wait import Waiter
from include.utils.snowflake import generate_query_tag_cmd
from include.config import conn_ids


class FacebookAdvancedAnalyticsToS3Operator(BaseRowsToS3CsvOperator):
    template_fields = ["macros", "key", "sql_cmd"]

    def __init__(
        self,
        macros,
        template_id,
        sql_cmd,
        facebook_conn_id=conn_ids.Facebook.default,
        snowflake_conn_id: str = conn_ids.Snowflake.default,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.macros = macros
        self.template_id = template_id
        self.sql_cmd = sql_cmd
        self.facebook_conn_id = facebook_conn_id
        self.snowflake_conn_id = snowflake_conn_id
        self.base_url = 'https://graph.facebook.com'
        self.payload = {}

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

    def get_result_id(self, api_version):
        self.macros.update(self.get_macros(self.get_sql_data()))
        print(f"Macros are: {self.macros}")
        self.payload['macros'] = json.dumps(self.macros)
        url = f"{self.base_url}/{api_version}/{self.template_id}/execute_template"
        response = requests.post(url=url, data=self.payload)
        if response.status_code != 200:
            raise Exception(f'Received unexpected response:{response.text}')
        json_response = response.json()
        print(json_response)
        return json_response['result_id']

    def get_macros(self, account_ids):
        result = {}
        for i in range(1, 11):
            try:
                account_id = account_ids[i - 1][0]
            except IndexError:
                account_id = '0'
            result[f'ID_ACCOUNT_{i}'] = str(account_id)
        result['STRING_LIST_ACCOUNT_IDS'] = ",".join([f"'{t[0]}'" for t in account_ids])
        return result

    def get_response(self, result_id, hook):
        url = f"{self.base_url}/{hook.api_version}/{result_id}?access_token={hook.access_token}"
        waiter = exponential_backoff_wait_times(
            growth_param=1.2, initial_wait=15, max_wait_time_seconds=60 * 60
        )
        while True:
            response = requests.get(url)
            if response.status_code != 200:
                raise Exception(f'Received unexpected response:{response.text}')
            json_response = response.json()
            if json_response['status'] == 'SUCCESS':
                break
            else:
                wait_seconds = next(waiter)
                print(
                    f"Response for result_id:{result_id} not ready; waiting {wait_seconds} seconds"
                )
                print(f"response is {json_response}")
                time.sleep(wait_seconds)
        return json_response

    def get_rows(self) -> Iterable[dict]:
        hook = FacebookAdsHook(facebook_ads_conn_id=self.facebook_conn_id)
        self.payload['access_token'] = hook.access_token
        result_id = self.get_result_id(hook.api_version)
        final_response = self.get_response(result_id, hook)
        curr_time = pendulum.DateTime.utcnow().isoformat()
        manual_data = {
            'date_start': self.macros['DATE_START'],
            'date_end': self.macros['DATE_END'],
            'api_call_timestamp': curr_time,
        }
        header_row = [i[0] for i in final_response['header']]
        for item in final_response['result']:
            yield {**self.process_reocrd(dict(zip(header_row, item))), **manual_data}

    def process_reocrd(self, record):
        for key in record.keys():
            if 'Account' in key and record[key] == '1':
                record[key] = self.macros[key.replace('Account', 'ID_ACCOUNT_')]
        return record


class FacebookAdvancedAnalyticsToS3OperatorNew(FacebookAdvancedAnalyticsToS3Operator):

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

    def get_macros(self, account_ids):
        result = {
            'LISTID_ACCOUNT_IDS_1': '1,2,3',
            'LISTID_ACCOUNT_IDS_2': '1,2,3',
            'LISTID_ACCOUNT_IDS_3': '1,2,3',
            'LISTID_ACCOUNT_IDS_4': '1,2,3',
            'LISTID_ACCOUNT_IDS_5': '1,2,3',
            'LISTID_ACCOUNT_IDS_6': '1,2,3',
            'LISTID_ACCOUNT_IDS_7': '1,2,3',
            'LISTID_ACCOUNT_IDS_8': '1,2,3',
            'LISTID_ALL_ACCOUNTS': '',
        }
        for store, listid, account_id in account_ids:
            result['LISTID_ALL_ACCOUNTS'] = result['LISTID_ALL_ACCOUNTS'] + ',' + str(account_id)
            if result[listid] == '1,2,3':
                result[listid] = str(account_id)
            else:
                result[listid] = result[listid] + ',' + str(account_id)

        if result['LISTID_ALL_ACCOUNTS'][0] == ',':
            result['LISTID_ALL_ACCOUNTS'] = result['LISTID_ALL_ACCOUNTS'].replace(',', '', 1)
        return result

    def process_reocrd(self, record):
        for key in record.keys():
            if record[key] == '':
                record[key] = 0
        record['string_list_store_name'] = self.macros['STRING_LIST_STORE_BRAND_NAME']
        return record


class FacebookAdCreativeNew(BaseRowsToS3CsvOperator):
    template_fields = ("key",)

    def __init__(
        self,
        account_id: str,
        facebook_ads_conn_id: str = conn_ids.Facebook.default,
        s3_conn_id: str = conn_ids.AWS.tfg_default,
        snowflake_conn_id: str = conn_ids.Snowflake.default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.facebook_ads_conn_id = facebook_ads_conn_id
        self.snowflake_conn_id = snowflake_conn_id
        self.account_id = account_id
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
        sql_cmd = f"""
        select ad.account_id,campaign_id,ad_id,creative_id,effective_status as configured_status
        from lake_view.facebook.ad_creative ad
        JOIN lake_view.sharepoint.med_account_mapping_media am ON TO_VARCHAR(ad.account_id)=am.source_id
            AND am.source='Facebook' AND am.reference_column='account_id'
        JOIN edw_prod.data_model.dim_store ds ON ds.store_id=am.store_id
        where updated_time >= '2023-01-01' and lower(ad.account_id) = '{self.account_id.replace("act_", "")}'
        order by creative_id
"""
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        with snowflake_hook.get_conn() as cnx:
            self.log.info("Executing: %s", sql_cmd)
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = cnx.cursor()
            cur.execute(query_tag)
            cur.execute(sql_cmd)
            rows = cur.fetchall()
        return rows

    def get_hook(self):
        return BaseHook()

    def get_rows(self):
        print("into the getrows")
        hook = self.get_hook()
        conn = hook.get_connection(self.facebook_ads_conn_id)
        api_version = conn.extra_dejson['api_version']
        rows = self.get_sql_data()
        print(len(rows))
        for idx, row in enumerate(rows):
            print(row)
            if 'sxf' in self.facebook_ads_conn_id and (idx + 1) % 100 == 0:
                time.sleep(5)
            elif (idx + 1) % 1000 == 0:
                time.sleep(5)
            url = f"https://graph.facebook.com/{api_version}/{row[3]}?fields=thumbnail_url,object_story_spec,image_url,image_hash,effective_object_story_id,instagram_permalink_url&access_token={conn.password}&thumbnail_width=1500&thumbnail_height=1200"
            response = requests.request("GET", url)
            row_data = {}
            if response.status_code == 200:
                data = response.json()
                row_data['account_id'] = row[0]
                row_data['campaign_id'] = row[1]
                row_data['ad_id'] = row[2]
                row_data['creative_id'] = row[3]
                # data['image_hash'] = ''
                # data['image_url'] = ''
                row_data['configured_status'] = row[4]
                # row_data['post_type'] = ''
                if 'thumbnail_url' in data:
                    row_data['thumbnail_url'] = data['thumbnail_url']
                else:
                    row_data['thumbnail_url'] = ''

                if 'image_url' in data:
                    row_data['image_url'] = data['image_url']
                    row_data['image_hash'] = data['image_hash']

                elif "object_story_spec" in data:
                    (
                        row_data["image_url"],
                        row_data["image_hash"],
                        row_data["post_type"],
                    ) = self.get_image_data(data, row_data['account_id'])
                else:
                    permalink_url = None
                    if "image_hash" in data:
                        permalink_url = self.get_permalink_url(
                            data["image_hash"], row_data['account_id']
                        )
                    row_data["image_url"] = permalink_url or data.get("image_url")
                    row_data["image_hash"] = data.get("image_hash")
                    row_data["post_type"] = None
                row_data['effective_object_story_id'] = data.get('effective_object_story_id')
                row_data['object_story_spec'] = data.get('object_story_spec')
                row_data['instagram_permalink_url'] = data.get('instagram_permalink_url')
                yield row_data

    def get_image_data(self, data, account_id):
        if data["object_story_spec"].get("video_data"):
            data_type = "video_data"
        elif data["object_story_spec"].get("link_data"):
            data_type = "link_data"
        elif data["object_story_spec"].get("photo_data"):
            data_type = "photo_data"
        elif data["object_story_spec"].get("template_data"):
            data_type = "template_data"
        else:
            data_type = None
        if data_type:
            if "child_attachments" in data["object_story_spec"][data_type]:
                print(
                    "process_child_attachments"
                    + str(self.process_child_attachments(data, data_type, account_id))
                )
                return self.process_child_attachments(data, data_type, account_id)

            if (
                "image_url" not in data["object_story_spec"][data_type]
                and "image_hash" in data["object_story_spec"][data_type]
            ):
                image_url = self.get_permalink_url(
                    data["object_story_spec"][data_type]["image_hash"], account_id
                )
                if image_url is None:
                    image_url = data.get("image_url")  # type: ignore
                image_hash = data["object_story_spec"][data_type]["image_hash"]  # type: ignore
            else:
                image_url = data["object_story_spec"][data_type].get("image_url")  # type: ignore
                image_hash = data["object_story_spec"][data_type].get("image_hash")
            return image_url, image_hash, data_type
        else:
            return None, None, "other"

    def process_child_attachments(self, data, data_type, account_id):
        child_attachments = data["object_story_spec"][data_type]["child_attachments"]
        image_url = None
        image_hash = None
        for dic in child_attachments:
            permalink_url = None
            if "image_hash" in dic:
                permalink_url = self.get_permalink_url(dic["image_hash"], account_id)
            image_url = permalink_url or dic.get("picture")
            image_hash = dic.get("image_hash")
            print("image_url" + str(image_url))
            if image_url:
                break
        return image_url, image_hash, "child_attachments"

    def get_permalink_url(self, image_hash, account_id):
        hook = self.get_hook()
        conn = hook.get_connection(self.facebook_ads_conn_id)
        api_version = conn.extra_dejson['api_version']

        url = f"https://graph.facebook.com/{api_version}/act_{account_id}/adimages?fields=id,permalink_url,hash&hashes=[\"{image_hash}\"]&access_token={conn.password}"

        response = requests.request("GET", url)
        permalink_url = None
        if response.status_code == 200:
            images = response.json()
            if len(images['data']) > 0:
                print("Image data: " + str(images))
                if 'permalink_url' in images['data'][0]:
                    permalink_url = images['data'][0].get('permalink_url')

        return permalink_url


class FacebookAdsPixelStats(BaseRowsToS3CsvOperator):
    template_fields = ("key",)

    def __init__(
        self,
        pixel_id: str,
        facebook_ads_conn_id: str = conn_ids.Facebook.default,
        s3_conn_id: str = conn_ids.AWS.tfg_default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.facebook_ads_conn_id = facebook_ads_conn_id
        self.pixel_id = pixel_id
        self.hook = None

    def get_hook(self):
        return BaseHook()

    def get_rows(self):
        conn = self.get_hook().get_connection(self.facebook_ads_conn_id)
        api_version = conn.extra_dejson['api_version']
        start_time = int(time.time()) - 60 * 60 * 24 * 7
        base_url = f"https://graph.facebook.com/{api_version}/{self.pixel_id}/stats"
        params = {
            'access_token': conn.password,
            'aggregation': 'event_source',
            'start_time': start_time,
        }

        events = [
            'PageView',
            'ViewContent',
            'AddToCart',
            'CompleteRegistration',
            'Subscribe',
            'Purchase',
        ]

        with requests.Session() as session:
            for event in events:
                params['event'] = event
                try:
                    response = session.get(base_url, params=params)
                    response.raise_for_status()
                    rows = response.json()
                except requests.RequestException as e:
                    print(f"Request failed for event {event}: {e}")
                    continue

                data = {'pixel_id': self.pixel_id, 'event': event}
                for row in rows.get('data', []):
                    data['start_time'] = datetime.strptime(row['start_time'], "%Y-%m-%dT%H:%M:%S%z")
                    count_dict = {item['value']: item['count'] for item in row['data']}
                    yield {**data, **count_dict}


class FacebookAdInsightsToS3(BaseRowsToS3CsvOperator):
    template_fields = ("key", "config")

    def __init__(
        self,
        account_id: str,
        config: dict,
        rows_per_req: int,
        facebook_ads_conn_id: str = "facebook_default",
        s3_conn_id: str = "tfg_aws_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.facebook_ads_conn_id = facebook_ads_conn_id
        self.account_id = account_id
        self.config = config
        self.rows_per_req = rows_per_req
        self.hook = None
        self.base_url = 'https://graph.facebook.com'

    def get_hook(self):
        return BaseHook()

    def get_status(self, response):
        if response.status_code == 200:
            return True
        else:
            print(f"headers: {response.headers}")
            print(f"content: {response.content}")
            print(f"status_code: {response.status_code}")
            response.raise_for_status()
            raise Exception(response.content)

    def get_rows(self) -> Iterable[Dict]:
        print("Configuration :", self.config)
        hook = self.get_hook()
        conn = hook.get_connection(self.facebook_ads_conn_id)
        api_version = conn.extra_dejson['api_version']
        access_token = {'access_token': conn.password}
        req_url = f"{self.base_url}/{api_version}/{self.account_id}/insights"
        response = requests.post(url=req_url, json=self.config | access_token)
        if self.get_status(response):
            report_run_id = response.json()['report_run_id']
            print(f"Generated the report for {self.account_id} with the run id: {report_run_id}")
            report_run_url = f"{self.base_url}/{api_version}/{report_run_id}"
            waiter = Waiter(
                growth_param=1.2,
                initial_wait=10,
                max_wait_time_seconds=60 * 60,
            )
            while True:
                status_req = requests.get(url=report_run_url, params=access_token)
                status = status_req.json()['async_status']
                per_complete = status_req.json()['async_percent_completion']
                if status != "Job Completed":
                    wait_time = waiter.next()
                    self.log.info(
                        f"""'{report_run_id}' has status '{status}' with {per_complete}% completed.
                                                                    Waiting for {wait_time} seconds"""
                    )
                    time.sleep(wait_time)
                elif status == "Job Completed" and per_complete == 100:
                    self.log.info(f"Report with run_id:{report_run_id} is generated")
                    break
                else:
                    raise Exception(
                        f"{report_run_id} has status {status} with {per_complete}% completed"
                    )

            res = requests.get(
                url=f"{report_run_url}/insights", params=access_token | {"limit": self.rows_per_req}
            )
            if self.get_status(res):
                while True:
                    data = res.json()
                    if data['data']:
                        for rec in data['data']:
                            rec['api_call_timestamp'] = pendulum.DateTime.utcnow().isoformat()
                            yield rec
                    if data['paging'].get('next'):
                        res = requests.get(url=data['paging']['next'])
                    else:
                        break
