import time

import pendulum
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.airflow.hooks.roku import RokuHook
from include.utils.exponential_wait import Waiter
from include.config import conn_ids


class RokuToS3Operator(BaseOperator):
    """
    This operator needs to be inherited and get_rows method should be implemented to
    specify the request url and process the json data returned.
    """

    template_fields = ["key", "report_params"]

    def __init__(
        self,
        bucket: str,
        key: str,
        report_params: dict,
        roku_conn_id: str = conn_ids.Roku.default,
        s3_conn_id: str = conn_ids.AWS.tfg_default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.key = key
        self.report_params = report_params
        self.s3_conn_id = s3_conn_id
        self.roku_conn_id = roku_conn_id
        self.hook = None

    def execute(self, context=None):
        hook = RokuHook(roku_conn_id=self.roku_conn_id)
        s3 = S3Hook(aws_conn_id=self.s3_conn_id)
        url = s3.generate_presigned_url(
            client_method="put_object",
            params={"Bucket": self.bucket, "Key": self.key, "ContentType": "text/csv"},
            expires_in=18000,
            http_method="PUT",
        )
        start_time = pendulum.parse(self.report_params["reports"][0]["start_at"])
        end_time = pendulum.parse(self.report_params["reports"][0]["end_at"])
        self.report_params["reports"][0]["start_at"] = start_time.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        self.report_params["reports"][0]["end_at"] = end_time.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        self.report_params["reports"][0]["delivery_address"] = url
        response = hook.make_request(
            method="POST", endpoint="reports", json=self.report_params
        )
        print(response.json())
        data = response.json()
        uid = data["reports"][0]["uid"]
        waiter = Waiter(
            growth_param=1.2,
            initial_wait=60,
            max_wait_time_seconds=60 * 60,
        )
        while True:
            print("pulling report status")
            status = hook.make_request(
                method="GET", endpoint="reports", params={"uid": uid}
            )
            status_dt = status.json()
            print(f"report status {status_dt}")
            if status_dt["reports"][0]["status"] in ("pending", "running"):
                wait_seconds = waiter.next()
                print(
                    f"report {uid} has status '{status_dt['reports'][0]['status']}'; waiting {wait_seconds} seconds"
                )
                time.sleep(wait_seconds)
            elif status_dt["reports"][0]["status"] == "done":
                print(f"report {uid} has completed successfully")
                break
            else:
                raise Exception(f"report request failed with response {status_dt}")
