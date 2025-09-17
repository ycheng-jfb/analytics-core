from pprint import pprint
from typing import List

from airflow.providers.amazon.aws.hooks.s3 import S3Hook as BaseS3Hook
from functools import cached_property

from include.utils.data_structures import chunk_list


class S3Hook(BaseS3Hook):
    """
    Helper class for using boto3 s3 client

    """

    @cached_property
    def client(self):
        return self.get_conn()

    @staticmethod
    def raise_for_status(response):
        try:
            response_code = response["ResponseMetadata"]["HTTPStatusCode"]
        except Exception as e:
            print("unexpected result")
            print(response)
            raise e
        if response_code != 200:
            raise Exception(f"copy failed: {response}")

    def delete_objects(self, bucket: str, keys: List[str]):
        """
        Will delete ``keys``.   Parses response code for error.

        Will chunk into batches of 500

        Args:
            bucket: s3 bucket
            keys: keys to delete

        """
        for chunk in chunk_list(keys, size=500):
            object_list = [{"Key": key} for key in chunk]
            response = self.client.delete_objects(
                Bucket=bucket, Delete={"Objects": object_list}
            )
            if response:
                pprint(response)
                self.raise_for_status(response=response)

    def copy(
        self,
        source_bucket,
        target_bucket,
        source_key,
        target_key,
        acl="bucket-owner-full-control",
    ):
        """
        Copies key from ``source_key`` to ``target_key`` and raises if unsuccessful

        Uses :meth:`S3.Client.copy` which is higher-level than :meth:`S3.Client.copy_object`.

        It can handle multipart transfers.
        """

        copy_source = {
            "Bucket": source_bucket,
            "Key": source_key,
        }
        response = self.client.copy(
            Bucket=target_bucket,
            Key=target_key,
            CopySource=copy_source,
            ExtraArgs={"ACL": acl},
        )
        if response:
            self.raise_for_status(response=response)

    def load_file(
        self,
        filename,
        key,
        bucket_name=None,
        replace=False,
        encrypt=False,
        acl="bucket-owner-full-control",
    ):
        """
        Loads a local file to S3

        Args:
            filename: name of the file to load.
            key: S3 key that will point to the file
            bucket_name: Name of the bucket in which to store the file
            replace: A flag to decide whether or not to overwrite the key if it already exists. If
                replace is False and the key exists, an error will be raised.
            encrypt: If True, the file will be encrypted on the server-side by S3 and will be stored
                in an encrypted form while at rest in S3.
            acl: optional canned acl to apply to file after uploading
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)

        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError("The key {key} already exists.".format(key=key))

        extra_args = {}
        if encrypt:
            extra_args["ServerSideEncryption"] = "AES256"
        if acl:
            extra_args["ACL"] = acl

        client = self.get_conn()
        client.upload_file(filename, bucket_name, key, ExtraArgs=extra_args)
