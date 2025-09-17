from airflow.models import BaseOperator

from include.airflow.hooks.smartly import SmartlyHook
from include.config import conn_ids


class SmartlyToS3Operator(BaseOperator):
    template_fields = ["key", "report_params"]

    def __init__(
        self,
        report_params: dict,
        bucket: str,
        key: str,
        account_ids: list,
        smartly_conn_id: str = conn_ids.Smartly.default,
        s3_conn_id: str = conn_ids.AWS.tfg_default,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.key = key
        self.s3_conn_id = s3_conn_id
        self.smartly_conn_id = smartly_conn_id
        self.report_params = report_params
        self.account_ids = account_ids

    def execute(self, context=None):
        hook = SmartlyHook(smartly_conn_id=self.smartly_conn_id, s3_conn_id=self.s3_conn_id)
        self.report_params["account_id"] = self.account_ids
        endpoint = f"https://stats-api.smartly.io/api/{hook.version}/stats"
        hook.get_report_to_s3(
            bucket=self.bucket,
            key=self.key,
            endpoint=endpoint,
            params=self.report_params,
        )
