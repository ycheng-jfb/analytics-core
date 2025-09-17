import tempfile

import requests
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class OmnishopInventoryS3ObjectToS3(BaseOperator):
    template_fields = ["key"]

    def __init__(
        self,
        store_file_location: str,
        s3_conn_id: str,
        key: str,
        bucket: str,
        omnishop_conn_id="omnishop_s3_conn",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.key = key
        self.bucket = bucket
        self.omnishop_conn_id = omnishop_conn_id
        self.s3_conn_id = s3_conn_id
        self.store_file_location = store_file_location

    def dry_run(self):
        print(f"File uploaded to {self.key}")

    def execute(self, context=None):
        omnishop_conn = BaseHook.get_connection(self.omnishop_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        url = f"{omnishop_conn.get_uri()}/{self.store_file_location}"

        response = requests.get(url)
        response.raise_for_status()

        if response.status_code == 200:
            with tempfile.NamedTemporaryFile() as temp_file:
                temp_file.write(response.content)
                s3_hook.load_file(
                    filename=temp_file.name,
                    key=self.key,
                    bucket_name=self.bucket,
                    replace=True,
                )
            print("File uploaded successfully")
        else:
            print(f"Failed to download file. Status code: {response.status_code}")
