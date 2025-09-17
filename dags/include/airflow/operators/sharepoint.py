from functools import cached_property

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.airflow.hooks.msgraph import MsGraphSharePointHook


class SharepointToS3Operator(BaseOperator):
    def __init__(
        self,
        sharepoint_conn_id,
        source_folder_name,
        file_path,
        site_id,
        drive_id,
        s3_conn_id,
        bucket,
        s3_key_prefix,
        is_archive_file: bool = False,
        archive_folder: str = "archive",
        **kwargs,
    ):
        self.sharepoint_conn_id = sharepoint_conn_id
        self.source_folder_name = source_folder_name
        self.file_path = file_path
        self.site_id = site_id
        self.drive_id = drive_id
        self.s3_conn_id = s3_conn_id
        self.bucket = bucket
        self.s3_key_prefix = s3_key_prefix
        self.is_archive_file = is_archive_file
        self.archive_folder = archive_folder
        super().__init__(**kwargs)

    @cached_property
    def ms_graph_sharepoint_hook(self):
        return MsGraphSharePointHook(sharepoint_conn_id=self.sharepoint_conn_id)

    @cached_property
    def s3_hook(self) -> S3Hook:
        return S3Hook(self.s3_conn_id)

    def execute(self, context=None):
        res = self.ms_graph_sharepoint_hook.list_file(
            self.site_id, self.drive_id, self.source_folder_name, self.file_path
        )

        # filter files
        files = list(filter(lambda obj: obj.get('file') is not None, res['value']))

        for file in files:
            download_url = file['@microsoft.graph.downloadUrl']
            file_name = file['name']
            file_id = file['id']

            # download_file
            file_response = self.ms_graph_sharepoint_hook.make_request(download_url)

            # Upload to S3
            self.s3_hook.load_bytes(
                bytes_data=file_response.content,
                key=f"{self.s3_key_prefix}/{file_name}",
                bucket_name=self.bucket,
                replace=True,
            )

            if self.is_archive_file:
                self.ms_graph_sharepoint_hook.move_file(
                    site_id=self.site_id,
                    drive_id=self.drive_id,
                    file_id=file_id,
                    file_name=file_name,
                    destination_folder_name=self.archive_folder,
                )
