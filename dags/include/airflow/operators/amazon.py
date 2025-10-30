import time
import tempfile
import pendulum

from airflow.models import BaseOperator
from datetime import datetime, timedelta
from functools import cached_property
from pathlib import Path
from urllib import request

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.airflow.hooks.amazon import AmazonHook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.utils.exponential_wait import Waiter
from include.airflow.operators.rows_to_s3 import (
    BaseRowsToS3CsvWatermarkOperator,
    BaseRowsToS3CsvOperator,
)
from include.utils.snowflake import generate_query_tag_cmd
from include.config import conn_ids


class AmazonToS3BaseOperator(BaseOperator):
    def __init__(
        self,
        amazon_conn_id: str = conn_ids.AWS.amazon_default,
        s3_conn_id: str = conn_ids.AWS.default,
        version: str = "2021-06-30",
        *args,
        **kwargs,
    ):
        self.amazon_conn_id = amazon_conn_id
        self.s3_conn_id = s3_conn_id
        self.version = version
        super().__init__(*args, **kwargs)

    @cached_property
    def hook(self):
        return AmazonHook(self.amazon_conn_id)

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook()

    def make_request(
        self, method, url, endpoint, params=None, json=None, data=None, recursion_size=5
    ):
        endpoint = endpoint[1:] if endpoint[0] == "/" else endpoint
        res = ''
        try:
            res = self.hook.session.request(
                method=method,
                url=f"{url}/{endpoint}",
                params=params,
                json=json,
                data=data,
            )
            if res.status_code in (200, 202):
                return res
            else:
                print(f"headers: {res.headers}")
                print(f"content: {res.content}")
                print(f"status_code: {res.status_code}")
                res.raise_for_status()
                raise Exception(res.content)
        except Exception as e:
            if e.response.status_code == 404:
                print(e.response.content.decode("utf-8"))
                return res
            elif e.response.status_code in (429, 403):
                print(e.response.content.decode("utf-8"))
                if e.response.status_code == 429:
                    wait_seconds = 60 * 5 if 'reports' in endpoint else 5
                    print(f"Waiting for {wait_seconds} seconds")
                    time.sleep(wait_seconds)
                else:
                    print("Deleting the cached session")
                    del self.hook.session
                if recursion_size > 0:
                    res = self.make_request(
                        method=method,
                        url=url,
                        endpoint=endpoint,
                        params=params,
                        json=json,
                        data=data,
                        recursion_size=recursion_size - 1,
                    )
                elif recursion_size == 0:
                    raise Exception(e.response.content)
            else:
                print(f"headers: {e.response.headers}")
                print(f"content: {e.response.content}")
                print(f"status_code: {e.response.status_code}")
                e.response.raise_for_status()
                raise Exception(e.response.content)
        return res


class AmazonToS3Operator(AmazonToS3BaseOperator):

    template_fields = ["start_time"]

    def __init__(
        self,
        bucket: str,
        s3_prefix: str,
        report_type: str,
        market_place_ids: list,
        is_request: bool,
        start_time: str,
        file_extension: str = 'gz',
        amazon_conn_id: str = conn_ids.AWS.amazon_default,
        s3_conn_id: str = conn_ids.AWS.default,
        version: str = "2021-06-30",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.s3_prefix = s3_prefix
        self.report_type = report_type
        self.market_place_ids = market_place_ids
        self.is_request = is_request
        self.start_time = start_time
        self.file_extension = file_extension
        self.amazon_conn_id = amazon_conn_id
        self.s3_conn_id = s3_conn_id
        self.base_url = f"https://sellingpartnerapi-na.amazon.com/reports/{version}"

    def upload_document_to_s3(self, report):
        self.log.info(f"processing the report: {report['reportId']}")
        date_start = datetime.strptime(report['dataStartTime'], '%Y-%m-%dT%H:%M:%S%z').strftime(
            '%Y%m%dT%H%M%S'
        )
        date_end = datetime.strptime(report['dataEndTime'], '%Y-%m-%dT%H:%M:%S%z').strftime(
            '%Y%m%dT%H%M%S'
        )
        file_name = (
            f"amazon.sp_{self.report_type.lower()}_{date_start}_to_{date_end}.{self.file_extension}"
            if self.report_type != 'GET_MERCHANT_LISTINGS_ALL_DATA'
            else f"amazon.sp_{self.report_type.lower()}_{self.market_place_ids[0]}_{date_start}_to_{date_end}.{self.file_extension}"
        )
        self.log.info(f"pulling the file url for the document: {report['reportDocumentId']}")
        req_report_url = self.make_request(
            method="GET", url=self.base_url, endpoint=f"documents/{report['reportDocumentId']}"
        )
        response = req_report_url.json()
        report_url = response['url']
        dest_s3 = S3Hook(self.s3_conn_id)
        with tempfile.TemporaryDirectory() as td:
            local_file_path = Path(td, file_name)
            request.urlretrieve(report_url, filename=local_file_path.as_posix())
            self.log.info(f"Uploading the file {file_name} to S3")
            dest_s3.load_file(
                filename=local_file_path.as_posix(),
                key=f"{self.s3_prefix}/{file_name}",
                bucket_name=self.bucket,
                replace=True,
            )

    def execute(self, context=None):
        if self.is_request:
            report_body = {
                "reportType": self.report_type,
                "marketplaceIds": self.market_place_ids,
                "dateStartTime": f"{datetime.strptime(self.start_time, '%Y-%m-%dT%H:%M:%S%z')+timedelta(days=-30)}",
            }
            report_request = self.make_request(
                method='POST', url=self.base_url, endpoint='reports', json=report_body
            )

            if report_request.status_code in (200, 202):
                res = report_request.json()
                report_id = res['reportId']

                self.log.info(f"Generated the report: {report_id}")
                waiter = Waiter(
                    growth_param=1.2,
                    initial_wait=60 * 5,
                    max_wait_time_seconds=60 * 60,
                )
                while True:
                    self.log.info('pulling report status')
                    status = self.make_request(
                        method="GET", url=self.base_url, endpoint=f"reports/{report_id}"
                    )
                    if status.status_code in (200, 202):
                        status_dt = status.json()
                        self.log.info(f'report status {status_dt}')
                        if status_dt['processingStatus'] in ('IN_QUEUE', 'IN_PROGRESS'):
                            wait_seconds = waiter.next()
                            self.log.info(
                                f"report {report_id} has status '{status_dt['processingStatus']}'; "
                                f"waiting {wait_seconds} seconds"
                            )
                            time.sleep(wait_seconds)
                        elif status_dt['processingStatus'] == 'DONE':
                            self.log.info(f"report {report_id} has completed successfully")
                            self.upload_document_to_s3(status_dt)
                            break
                        else:
                            raise Exception(f"report request failed with response {status_dt}")
        else:
            report_params = {
                "reportTypes": self.report_type,
            }
            self.log.info('pulling the available settlement reports')
            report_request = self.make_request(
                method='GET', url=self.base_url, endpoint='reports', params=report_params
            )
            res = report_request.json()
            for report in res['reports']:
                if datetime.strptime(
                    report['createdTime'], '%Y-%m-%dT%H:%M:%S%z'
                ) > datetime.strptime(self.start_time, '%Y-%m-%dT%H:%M:%S%z'):
                    self.upload_document_to_s3(report)


class AmazonRefundEventListToS3Operator(AmazonToS3BaseOperator, BaseRowsToS3CsvWatermarkOperator):

    def __init__(
        self,
        amazon_conn_id: str = conn_ids.AWS.amazon_default,
        s3_conn_id: str = conn_ids.AWS.default,
        version: str = "v0",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.amazon_conn_id = amazon_conn_id
        self.s3_conn_id = s3_conn_id
        self.base_url = f"https://sellingpartnerapi-na.amazon.com/finances/{version}/orders"

    def get_high_watermark(self) -> str:
        cur = self.snowflake_hook.get_cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        sql = """
            SELECT MAX(meta_create_datetime)
            FROM lake.amazon.selling_partner_settlement_report_data;
        """
        cur.execute(sql)
        result = cur.fetchall()
        return str(result[0][0])

    def yield_rows_from_data(self, data):
        updated_at = pendulum.DateTime.utcnow().isoformat()
        row = {
            **{
                key: value
                for key, value in data["payload"]["FinancialEvents"]["RefundEventList"][0].items()
                if key in self.column_list
            },
            "updated_at": updated_at,
        }
        return row

    def get_rows(self):
        print("Connecting to Snowflake")
        cur = self.snowflake_hook.get_cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        query = f"""
        SELECT DISTINCT order_id
        FROM lake.amazon.selling_partner_settlement_report_data
        WHERE transaction_type ilike '%refund%'
            AND meta_create_datetime > '{self.low_watermark}';
        """
        cur.execute(query)
        response = cur.fetchall()
        for val in response:
            time.sleep(1)
            print(f"Processing for Order: {val[0]}")
            res = self.make_request(
                method='GET', url=self.base_url, endpoint=f"{val[0]}/financialEvents"
            )
            if res.status_code == 200:
                data = res.json()
                if len(data["payload"]["FinancialEvents"]["RefundEventList"]) > 0:
                    yield self.yield_rows_from_data(data)


class AmazonCatalogToS3Operator(AmazonToS3BaseOperator, BaseRowsToS3CsvOperator):

    def __init__(
        self,
        market_place_ids: list,
        amazon_conn_id: str = conn_ids.AWS.amazon_default,
        s3_conn_id: str = conn_ids.AWS.default,
        version: str = "2022-04-01",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.market_place_ids = market_place_ids
        self.amazon_conn_id = amazon_conn_id
        self.s3_conn_id = s3_conn_id
        self.base_url = f"https://sellingpartnerapi-na.amazon.com/catalog/{version}/items"
        self.parent_asins = set()

    def yield_rows_from_data(self, data):
        if data['relationships'][0]['relationships'] and data['relationships'][0]['relationships'][
            0
        ].get('parentAsins'):
            self.parent_asins.add(
                tuple(data['relationships'][0]['relationships'][0].get('parentAsins'))
            )
        updated_at = pendulum.DateTime.utcnow().isoformat()
        row = {
            **{key: value for key, value in data.items() if key in self.column_list},
            "updated_at": updated_at,
        }
        return row

    def get_rows(self):
        cur = self.snowflake_hook.get_cursor()
        query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
        cur.execute(query_tag)
        cur.execute("SELECT DISTINCT asin FROM lake.amazon.selling_partner_inventory_data;")
        response = cur.fetchall()
        for asins in [response, self.parent_asins]:
            for val in asins:
                time.sleep(1)
                self.log.info(f"Pulling data for ASIN: {val[0]}")
                for market_place_id in self.market_place_ids:
                    req_params = {
                        'marketplaceIds': market_place_id,
                        'includedData': 'attributes,dimensions,identifiers,images,productTypes,relationships,salesRanks,summaries',
                    }
                    print(f"Trying with marketplaceId: {market_place_id}")
                    res = self.make_request(
                        method='GET', url=self.base_url, endpoint=val[0], params=req_params
                    )
                    if res.status_code == 200:
                        yield self.yield_rows_from_data(res.json())
                        break
