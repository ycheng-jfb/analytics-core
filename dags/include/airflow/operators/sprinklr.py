import gzip
import json
import re
import tempfile
from pathlib import Path

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from functools import cached_property
from pendulum.parsing.exceptions import ParserError

from include.airflow.hooks.sprinklr import (
    RateLimitException,
    SprinklrHook,
    TooMuchDataException,
)
from include.airflow.operators.watermark import BaseProcessWatermarkOperator
from include.utils.data_structures import chunk_list
from include.config import conn_ids


class SprinklrToS3Operator(BaseProcessWatermarkOperator):
    """
    API Docs: https://developer.sprinklr.com/docs/read/API_Overview
    Utils are found in `include/utils/sprinklr.py` to help with report payloads and column configs

    Args:
        key: Name of S3 key
        bucket: Name of S3 bucket
        endpoint: Corresponds to the report to run. There should be an associated payload in the
            `PAYLOADS` variable
        extract_case_id: Optional boolean to extract "CASE_ID" from a column named "CASE TEXT".
            This is required for some reports that cannot pull CASE_ID on their own.
        sprinklr_conn_id: Name of Sprinklr connection id
        s3_conn_id: Name of S3 connection id
        initial_load_value: Unix ts in milliseconds. Defaults to equivalent of 2020-09-01.
        from_date: Optionally pass a date to pull from. If not passed, will use low_watermark
        to_date: Optionally pass a date to pull to. If not passed, will use high_watermark
        look_back_period: Optionally pass a number of milliseconds to look back from.
            Default is 0
        time_increment: Optionally pass a number of milliseconds to increment.
            Default is 24 hours
    """

    template_fields = ["from_date", "to_date", "key"]
    api_base_url = "https://api2.sprinklr.com/api/v2"

    def __init__(
        self,
        key,
        bucket,
        endpoint,
        extract_case_id: bool = False,
        sprinklr_conn_id=conn_ids.Sprinklr.default,
        s3_conn_id=conn_ids.AWS.tfg_default,
        initial_load_value: str = "1598918400000",
        from_date: str = None,
        to_date: str = None,
        look_back_period: int = 0,
        time_increment: int = 24 * 60 * 60 * 1000,  # 24 hours in milliseconds
        **kwargs,
    ):
        super().__init__(
            process_name=endpoint,
            namespace="sprinklr",
            initial_load_value=initial_load_value,
            **kwargs,
        )
        self.key = key
        self.bucket = bucket
        self.endpoint = endpoint
        if self.endpoint not in PAYLOADS:
            raise Exception(
                f"Received endpoint {endpoint}; allowable endpoint values: {PAYLOADS.keys()}"
            )
        self.extract_case_id = extract_case_id
        self.sprinklr_conn_id = sprinklr_conn_id
        self.s3_conn_id = s3_conn_id
        self.exported_rows = 0
        self.from_date = from_date
        self.to_date = to_date
        self.cancelled_unix = None
        self.tmp_s3_prefix = "tmp/" + "/".join(key.split("/")[:-1])
        self.look_back_period = look_back_period
        self.time_increment = time_increment

    @cached_property
    def s3_hook(self):
        return S3Hook(self.s3_conn_id)

    @cached_property
    def from_unix(self):
        if self.from_date:
            try:
                return (
                    int(pendulum.parse(self.from_date).timestamp() * 1000)
                    - self.look_back_period
                )

            except ParserError:
                print(
                    f"Error parsing from_date '{self.from_date}', defaulting to low_watermark"
                )
        return int(self.get_low_watermark()) - self.look_back_period

    @cached_property
    def to_unix(self):
        if self.to_date:
            try:
                return int(pendulum.parse(self.to_date).timestamp() * 1000)
            except ParserError:
                print(
                    f"Error parsing to_date '{self.to_date}', defaulting to high_watermark"
                )
        return int(self.get_high_watermark())

    @property
    def from_unix_as_date(self):
        return str(pendulum.from_timestamp(self.from_unix / 1000))

    @property
    def to_unix_as_date(self):
        return str(pendulum.from_timestamp(self.to_unix / 1000))

    def get_high_watermark(self) -> str:
        return str((pendulum.DateTime.utcnow().timestamp() * 1000).__round__())

    @cached_property
    def sprinklr_hook(self):
        return SprinklrHook(self.sprinklr_conn_id)

    @property
    def payload(self):
        return PAYLOADS[self.endpoint]

    def dump_rows_to_tmp(self, local_path, existing_rows):
        # If tmp s3 file existed, append existing rows and delete existing s3 file
        if self.tmp_s3_keys:
            self.append_existing_rows_to_file(local_path, existing_rows)
            self.s3_hook.delete_objects(self.bucket, self.tmp_s3_keys)

        # Upload new s3 file
        file_name = f"{self.from_unix}-{self.cancelled_unix}.ndjson.gz"
        key = f"{self.tmp_s3_prefix}/{file_name}"
        self.s3_hook.load_file(
            filename=local_path,
            key=key,
            bucket_name=self.bucket,
            replace=True,
        )

    def get_rows(self, payload):
        """
        Documentation for creating a report payload:
        https://developer.sprinklr.com/docs/read/api_tutorials/Reporting_Widget_Payload_Extraction_Tool

        NOTE: When using the converting tool, every custom field comes back with a heading of "UNIVERSAL_CASE_CUSTOM_PROPERTY". Make sure to change each heading name to be unique.
        - function `modify_report_payload` can be used to ensure this.
        """  # noqa
        # This runs in daily increments per Sprinklr's recommendation
        url = f"{self.api_base_url}/reports/query"
        current_time = self.cancelled_unix or self.from_unix
        time_increment = self.time_increment
        load_time = str(pendulum.DateTime.utcnow())
        while current_time < self.to_unix:
            # Report time running
            to_time = min([self.to_unix, current_time + time_increment])
            ctime_str = str(
                pendulum.from_timestamp(current_time / 1000).replace(microsecond=0)
            )
            to_time_str = str(
                pendulum.from_timestamp(to_time / 1000).replace(microsecond=0)
            )
            print(f"Running {ctime_str} to {to_time_str}")

            # Adjust time in payload and start at page 0
            payload["startTime"] = current_time
            payload["endTime"] = to_time
            payload["page"] = 0

            while True:
                print(f"Requesting page {payload['page']}")
                try:
                    response = self.sprinklr_hook.make_request(
                        "POST", url, payload=payload
                    )
                except RateLimitException as e:
                    self.cancelled_unix = to_time
                    raise e
                data = response.json()["data"]
                if "rows" not in data:
                    break
                headings = data["headings"]
                for row in data["rows"]:
                    row_of_dict = {
                        heading: value for heading, value in zip(headings, row)
                    }
                    row_of_dict["load_time"] = load_time
                    if self.extract_case_id:
                        case_id = int(
                            re.search(r"^(\d+)", row_of_dict["CASE_TEXT"].strip())[1]
                        )
                        row_of_dict["CASE_ID"] = case_id
                    yield row_of_dict
                payload["page"] += 1
            current_time += time_increment

    @staticmethod
    def append_existing_rows_to_file(local_path, existing_rows: bytes):
        with gzip.open(local_path, "a") as f:
            f.write(existing_rows)

    def write_to_file(self, local_path, data):
        with gzip.open(local_path, "wt") as f:
            for row in data:
                self.exported_rows += 1
                f.write(json.dumps(row))
                f.write("\n")
        print(f"exported rows: {self.exported_rows}")

    @cached_property
    def tmp_s3_keys(self):
        return self.s3_hook.list_keys(self.bucket, self.tmp_s3_prefix) or []

    def get_existing_rows(self) -> bytes:  # type: ignore
        for key in self.tmp_s3_keys:
            # Existing tmp keys might be from a different failed run. Only use this data if the
            # starting unix matches self.from_unix
            regex = r"^(\d+)-(\d+)\."
            matches = re.search(regex, Path(key).name)
            try:
                starting_unix, cancelled_unix = int(matches[1]), int(matches[2])  # type: ignore
            except TypeError:
                raise Exception(
                    f"S3 key '{key}' does not follow the regex pattern. This file may need to be "
                    f"deleted from the S3 bucket '{self.bucket}'"
                )
            if starting_unix == self.from_unix:
                print(f"Found existing tmp key '{key}'")
                self.cancelled_unix = cancelled_unix  # type: ignore
                obj = self.s3_hook.get_key(key, self.bucket)
                with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
                    return gzipfile.read()

    def watermark_execute(self, context=None):
        print(f"Api endpoint: {self.endpoint}")
        print(f"Running report from {self.from_unix_as_date} to {self.to_unix_as_date}")
        existing_rows = self.get_existing_rows()

        with tempfile.TemporaryDirectory() as td:
            local_path = (Path(td) / Path(self.key).name).as_posix()
            rows = self.get_rows(self.payload)
            try:
                self.write_to_file(local_path, rows)
            except RateLimitException as e:
                # If we hit the rate limit, dump the file and pick up in next retry
                self.dump_rows_to_tmp(local_path, existing_rows)
                raise e

            if existing_rows:
                self.append_existing_rows_to_file(local_path, existing_rows)
                self.s3_hook.delete_objects(self.bucket, self.tmp_s3_keys)
            if self.exported_rows:
                self.s3_hook.load_file(
                    filename=local_path,
                    key=self.key,
                    bucket_name=self.bucket,
                    replace=True,
                )


PAYLOADS = {
    "retention": {
        "reportingEngine": "PLATFORM",
        "report": "INBOUND_CASE",
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "pageSize": 1000,
        "page": 0,
        "groupBys": [
            {
                "heading": "DATE_TYPE_CASE_CREATION_TIME",
                "dimensionName": "DATE_TYPE_CASE_CREATION_TIME",
                "groupType": "FIELD",
                "details": {"isDateTypeDimension": True, "interval": "1d"},
            },
            {
                "heading": "CASE_ID",
                "dimensionName": "CASE_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f3c39ff6329316be3564325",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "_c_5f639dcf56752e4223d4ac86",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY2",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "ACCOUNT_ID",
                "dimensionName": "ACCOUNT_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY3",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413ca4",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY4",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f47ea905a6bbf3fc1a2e5da",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "CASE_COUNT",
                "measurementName": "CASE_COUNT",
                "aggregateFunction": "SUM",
            },
            {
                "heading": "RETENTION_CASE_COUNT2063",
                "measurementName": "RETENTION_CASE_COUNT2063",
                "aggregateFunction": "SUM",
            },
            {
                "heading": "CASE_COUNT_SAVED1634",
                "measurementName": "CASE_COUNT_SAVED1634",
                "aggregateFunction": "SUM",
            },
        ],
        "filters": None,
    },
    "closed_case": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "CASE_MACRO_USAGE",
        "filters": [
            {
                "dimensionName": "MACRO_ID",
                "filterType": "IN",
                "values": [
                    "5f2c0a0a8551567d14423ae1",
                    "5f2c15230781ea3d3ba5e0a6",
                    "5f2c14df0781ea3d3ba59766",
                    "5f2c0a0a8551567d14423aea",
                    "5f2c15450781ea3d3ba60b8a",
                ],
                "details": {},
            },
        ],
        "groupBys": [
            {
                "heading": "CASE_MACRO_APPLY_TIME",
                "dimensionName": "CASE_MACRO_APPLY_TIME",
                "groupType": "FIELD",
                "details": {"isDateTypeDimension": True},
            },
            {
                "heading": "CASE_ID",
                "dimensionName": "CASE_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "_c_5f639dcf56752e4223d4ac86",
                    "isSecureField": False,
                },
            },
            {
                "heading": "ACCOUNT_ID",
                "dimensionName": "ACCOUNT_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY2",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413ca4",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY3",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413c95",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY4",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c13830781ea3d3ba3b31d",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY5",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "spr_uc_status",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY6",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f3c39ff6329316be3564325",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY7",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f47ea905a6bbf3fc1a2e5da",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "CASE_MACRO_USAGE_COUNT",
                "measurementName": "CASE_MACRO_USAGE_COUNT",
                "aggregateFunction": "SUM",
                "details": None,
            }
        ],
        "sorts": [{"heading": "CASE_MACRO_USAGE_COUNT", "order": "DESC"}],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "new_case": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "INBOUND_CASE",
        "filters": None,
        "groupBys": [
            {
                "heading": "DATE_TYPE_CASE_CREATION_HISTOGRAM",
                "dimensionName": "DATE_TYPE_CASE_CREATION_HISTOGRAM",
                "groupType": "DATE_HISTOGRAM",
                "details": {"isDateTypeDimension": True},
            },
            {
                "heading": "CASE_ID",
                "dimensionName": "CASE_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "_c_5f639dcf56752e4223d4ac86",
                    "isSecureField": False,
                },
            },
            {
                "heading": "ACCOUNT_ID",
                "dimensionName": "ACCOUNT_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY2",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413ca4",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY3",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413c95",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY4",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c13830781ea3d3ba3b31d",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY5",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "spr_uc_status",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY6",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f3c39ff6329316be3564325",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY7",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f47ea905a6bbf3fc1a2e5da",
                    "isSecureField": False,
                },
            },
            {
                "heading": "HAS_BRAND_RESPONDED",
                "dimensionName": "HAS_BRAND_RESPONDED",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY8",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c13830781ea3d3ba3b283",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "HIGH_PRIORITY_CASE1183",
                "measurementName": "HIGH_PRIORITY_CASE1183",
                "aggregateFunction": "SUM",
                "details": None,
            },
            {
                "heading": "CASE_COUNT",
                "measurementName": "CASE_COUNT",
                "aggregateFunction": "SUM",
                "details": None,
            },
        ],
        "sorts": [{"heading": "CASE_COUNT", "order": "DESC"}],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "smm_first_response": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "CaseSLAReport",
        "filters": [
            {
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "filterType": "NIN",
                "values": ["Bot"],
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f3c39ff6329316be3564325",
                    "clientId": None,
                },
            },
            {
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "filterType": "NIN",
                "values": ["Yes"],
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f47ea905a6bbf3fc1a2e5da",
                    "clientId": None,
                },
            },
            {
                "dimensionName": "ACCOUNT_ID",
                "filterType": "IN",
                "values": [
                    "590134",
                    "590138",
                    "513016",
                    "590116",
                    "513029",
                    "513019",
                    "513020",
                    "513055",
                    "513053",
                    "586309",
                    "586311",
                    "603044",
                    "513054",
                    "586307",
                    "586310",
                    "586308",
                    "513023",
                    "513027",
                    "513030",
                    "586664",
                    "513026",
                    "513025",
                    "513051",
                    "513024",
                    "513021",
                    "586667",
                    "513041",
                    "586666",
                    "513022",
                    "513056",
                    "513057",
                    "513062",
                    "513063",
                    "513059",
                    "513060",
                    "513061",
                    "513058",
                ],
                "details": {},
            },
        ],
        "groupBys": [
            {
                "heading": "FIRST_BRAND_RESPONSE_SN_CREATED_TIME",
                "dimensionName": "FIRST_BRAND_RESPONSE_SN_CREATED_TIME",
                "groupType": "FIELD",
                "details": {"isDateTypeDimension": True},
            },
            {
                "heading": "CASE_TEXT",
                "dimensionName": "CASE",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "ACCOUNT_ID",
                "dimensionName": "ACCOUNT_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413ca4",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY2",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413c95",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY3",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c13830781ea3d3ba3b31d",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "CASE_FIRST_USER_RESPONSE_SLA",
                "measurementName": "CASE_FIRST_USER_RESPONSE_SLA",
                "aggregateFunction": "AVG",
                "details": None,
            },
            {
                "heading": "CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
                "measurementName": "CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
                "aggregateFunction": "AVG",
                "details": None,
            },
        ],
        "sorts": [{"heading": "FIRST_BRAND_RESPONSE_SN_CREATED_TIME", "order": "DESC"}],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "smm_handle_time": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "CaseProcessingSLAReport",
        "filters": [
            {
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "filterType": "NIN",
                "values": ["Yes"],
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f47ea905a6bbf3fc1a2e5da",
                    "clientId": None,
                },
            },
            {
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "filterType": "IN",
                "values": ["Agent", "Bot & Agent"],
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f3c39ff6329316be3564325",
                    "clientId": None,
                },
            },
            {
                "dimensionName": "ACCOUNT_ID",
                "filterType": "IN",
                "values": [
                    "590134",
                    "590138",
                    "513016",
                    "590116",
                    "513029",
                    "513019",
                    "513020",
                    "513055",
                    "513053",
                    "586309",
                    "586311",
                    "603044",
                    "513054",
                    "586307",
                    "586310",
                    "586308",
                    "513023",
                    "513027",
                    "513030",
                    "586664",
                    "513026",
                    "513025",
                    "513051",
                    "513024",
                    "513021",
                    "586667",
                    "513041",
                    "586666",
                    "513022",
                    "513056",
                    "513057",
                    "513062",
                    "513063",
                    "513059",
                    "513060",
                    "513061",
                    "513058",
                ],
                "details": {},
            },
        ],
        "groupBys": [
            {
                "heading": "DATE_TYPE_CASE_CREATION_TIME",
                "dimensionName": "DATE_TYPE_CASE_CREATION_TIME",
                "groupType": "FIELD",
                "details": {"isDateTypeDimension": True},
            },
            {
                "heading": "CASE_TEXT",
                "dimensionName": "CASE",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "ACCOUNT_ID",
                "dimensionName": "ACCOUNT_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413ca4",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY2",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413c95",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY3",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c13830781ea3d3ba3b31d",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "CaseProcessingSLA",
                "measurementName": "CaseProcessingSLA",
                "aggregateFunction": "SUM",
                "details": None,
            }
        ],
        "sorts": [{"heading": "CaseProcessingSLA", "order": "DESC"}],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "sam_first_response": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "CaseSLAReport",
        "filters": [
            {
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "filterType": "IN",
                "values": ["Yes"],
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f47ea905a6bbf3fc1a2e5da",
                    "clientId": None,
                },
            },
            {
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "filterType": "NIN",
                "values": ["Bot"],
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f3c39ff6329316be3564325",
                    "clientId": None,
                },
            },
            {
                "dimensionName": "ACCOUNT_ID",
                "filterType": "IN",
                "values": [
                    "590134",
                    "590138",
                    "513016",
                    "590116",
                    "513029",
                    "513019",
                    "513020",
                    "513055",
                    "513053",
                    "586309",
                    "586311",
                    "603044",
                    "513054",
                    "586307",
                    "586310",
                    "586308",
                    "513023",
                    "513027",
                    "513030",
                    "586664",
                    "513026",
                    "513025",
                    "513051",
                    "513024",
                    "513021",
                    "586667",
                    "513041",
                    "586666",
                    "513022",
                    "513056",
                    "513057",
                    "513062",
                    "513063",
                    "513059",
                    "513060",
                    "513061",
                    "513058",
                ],
                "details": {},
            },
        ],
        "groupBys": [
            {
                "heading": "FIRST_BRAND_RESPONSE_SN_CREATED_TIME",
                "dimensionName": "FIRST_BRAND_RESPONSE_SN_CREATED_TIME",
                "groupType": "FIELD",
                "details": {"isDateTypeDimension": True},
            },
            {
                "heading": "CASE_TEXT",
                "dimensionName": "CASE",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "ACCOUNT_ID",
                "dimensionName": "ACCOUNT_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413ca4",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY2",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413c95",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY3",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c13830781ea3d3ba3b31d",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "CASE_FIRST_USER_RESPONSE_SLA",
                "measurementName": "CASE_FIRST_USER_RESPONSE_SLA",
                "aggregateFunction": "AVG",
                "details": None,
            },
            {
                "heading": "CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
                "measurementName": "CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
                "aggregateFunction": "AVG",
                "details": None,
            },
        ],
        "sorts": [{"heading": "FIRST_BRAND_RESPONSE_SN_CREATED_TIME", "order": "DESC"}],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "sam_handle_time": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "CaseProcessingSLAReport",
        "filters": [
            {
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "filterType": "IN",
                "values": ["Agent", "Bot & Agent"],
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f3c39ff6329316be3564325",
                    "clientId": None,
                },
            },
            {
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "filterType": "IN",
                "values": ["Yes"],
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f47ea905a6bbf3fc1a2e5da",
                    "clientId": None,
                },
            },
            {
                "dimensionName": "ACCOUNT_ID",
                "filterType": "IN",
                "values": [
                    "590134",
                    "590138",
                    "513016",
                    "590116",
                    "513029",
                    "513019",
                    "513020",
                    "513055",
                    "513053",
                    "586309",
                    "586311",
                    "603044",
                    "513054",
                    "586307",
                    "586310",
                    "586308",
                    "513023",
                    "513027",
                    "513030",
                    "586664",
                    "513026",
                    "513025",
                    "513051",
                    "513024",
                    "513021",
                    "586667",
                    "513041",
                    "586666",
                    "513022",
                    "513056",
                    "513057",
                    "513062",
                    "513063",
                    "513059",
                    "513060",
                    "513061",
                    "513058",
                ],
                "details": {},
            },
        ],
        "groupBys": [
            {
                "heading": "DATE_TYPE_CASE_CREATION_TIME",
                "dimensionName": "DATE_TYPE_CASE_CREATION_TIME",
                "groupType": "FIELD",
                "details": {"isDateTypeDimension": True},
            },
            {
                "heading": "CASE_TEXT",
                "dimensionName": "CASE",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "ACCOUNT_ID",
                "dimensionName": "ACCOUNT_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413ca4",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY2",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413c95",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY3",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c13830781ea3d3ba3b31d",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "CaseProcessingSLA",
                "measurementName": "CaseProcessingSLA",
                "aggregateFunction": "SUM",
                "details": None,
            }
        ],
        "sorts": [{"heading": "CaseProcessingSLA", "order": "DESC"}],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "case_properties": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "INBOUND_CASE",
        "filters": None,
        "groupBys": [
            {
                "heading": "CASE_ID",
                "dimensionName": "CASE_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "spr_uc_priority",
                    "isSecureField": False,
                },
            },
            {
                "heading": "SENTIMENT",
                "dimensionName": "SENTIMENT",
                "groupType": "FIELD",
                "details": None,
            },
        ],
        "projections": [
            {
                "heading": "CASE_COUNT",
                "measurementName": "CASE_COUNT",
                "aggregateFunction": "SUM",
                "details": None,
            }
        ],
        "sorts": [{"heading": "CASE_COUNT", "order": "DESC"}],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "agent_case_assignment": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "CaseSLAReport",
        "filters": None,
        "groupBys": [
            {
                "heading": "date",
                "dimensionName": "date",
                "groupType": "DATE_HISTOGRAM",
                "details": {"isDateTypeDimension": True},
            },
            {
                "heading": "AFFECTED_USER_ID",
                "dimensionName": "AFFECTED_USER_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "spr_uc_priority",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "UNIQUE_CASES_ASSIGNED",
                "measurementName": "UNIQUE_CASES_ASSIGNED",
                "aggregateFunction": "SUM",
                "details": None,
            }
        ],
        "sorts": [{"heading": "date", "order": "DESC"}],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "agent_response_volume": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "POST_INSIGHTS",
        "filters": [
            {
                "dimensionName": "USER_ID",
                "filterType": "NIN",
                "values": ["0", "-100"],
                "details": {},
            }
        ],
        "groupBys": [
            {
                "heading": "date",
                "dimensionName": "date",
                "groupType": "DATE_HISTOGRAM",
                "details": {"isDateTypeDimension": True},
            },
            {
                "heading": "ACCOUNT_CUSTOM_PROPERTY",
                "dimensionName": "ACCOUNT_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "USER_ID",
                "dimensionName": "USER_ID",
                "groupType": "FIELD",
                "details": None,
            },
        ],
        "projections": [
            {
                "heading": "PUBLISHED_MESSAGE_COUNT",
                "measurementName": "PUBLISHED_MESSAGE_COUNT",
                "aggregateFunction": "SUM",
                "details": None,
            }
        ],
        "sorts": [{"heading": "PUBLISHED_MESSAGE_COUNT", "order": "DESC"}],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "case_disposition_survey": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "INBOUND_CASE",
        "filters": None,
        "groupBys": [
            {
                "heading": "CASE_ID",
                "dimensionName": "CASE_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "_c_5f74f5b613516e3b4b2f08af",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413c77",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY2",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "_c_5f69276a28bebd2607ed3385",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY3",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "_c_6019a6e6414d687c6561c85c",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "CASE_COUNT",
                "measurementName": "CASE_COUNT",
                "aggregateFunction": "SUM",
                "details": None,
            }
        ],
        "sorts": [{"heading": "CASE_COUNT", "order": "DESC"}],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "agent_response_sla": {
        "timeField": "date",
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "CaseSLAReport",
        "filters": None,
        "groupBys": [
            {
                "heading": "date",
                "dimensionName": "date",
                "groupType": "DATE_HISTOGRAM",
                "details": {"isDateTypeDimension": True, "interval": "1d"},
            },
            {
                "heading": "BRAND_RESPONSE_BY_USER",
                "dimensionName": "BRAND_RESPONSE_BY_USER",
                "groupType": "FIELD",
                "details": None,
            },
        ],
        "projections": [
            {
                "heading": "CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
                "measurementName": "CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
                "aggregateFunction": "AVG",
                "details": None,
            },
            {
                "heading": "CASE_RESPONSE_SLA_CUSTOMMETRIC_7664",
                "measurementName": "CASE_RESPONSE_SLA_CUSTOMMETRIC_7664",
                "aggregateFunction": "AVG",
                "details": None,
            },
        ],
        "sorts": [
            {"heading": "CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446", "order": "DESC"}
        ],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "case_queue_sla": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "CaseSLAReport",
        "filters": None,
        "groupBys": [
            {
                "heading": "CASE_DATA",
                "dimensionName": "CASE",
                "groupType": "FIELD",
                "details": None,
            }
        ],
        "projections": [
            {
                "heading": "AVG_CASE_QUEUE_SLA_AWAITING_RESPONSE_CUSTOMMETRIC_8344",
                "measurementName": "CASE_QUEUE_SLA_AWAITING_RESPONSE_CUSTOMMETRIC_8344",
                "aggregateFunction": "AVG",
                "details": None,
            },
            {
                "heading": "AVG_CASE_QUEUE_SLA_ASSIGNED_CUSTOMMETRIC_7516",
                "measurementName": "CASE_QUEUE_SLA_ASSIGNED_CUSTOMMETRIC_7516",
                "aggregateFunction": "AVG",
                "details": None,
            },
            {
                "heading": "AVG_CASE_QUEUE_SLA_AWAITING_ASSIGNMENT_CUSTOMMETRIC_4364",
                "measurementName": "CASE_QUEUE_SLA_AWAITING_ASSIGNMENT_CUSTOMMETRIC_4364",
                "aggregateFunction": "AVG",
                "details": None,
            },
            {
                "heading": "AVG_CASE_QUEUE_SLA_NEW_CUSTOMMETRIC_575",
                "measurementName": "CASE_QUEUE_SLA_NEW_CUSTOMMETRIC_575",
                "aggregateFunction": "AVG",
                "details": None,
            },
            {
                "heading": "AVG_CASE_QUEUE_SLA_TOTAL_WORKED_TIME_CUSTOMMETRIC_6269",
                "measurementName": "CASE_QUEUE_SLA_TOTAL_WORKED_TIME_CUSTOMMETRIC_6269",
                "aggregateFunction": "AVG",
                "details": None,
            },
            {
                "heading": "CASE_QUEUE_SLA_AWAITING_RESPONSE_CUSTOMMETRIC_8344",
                "measurementName": "CASE_QUEUE_SLA_AWAITING_RESPONSE_CUSTOMMETRIC_8344",
                "aggregateFunction": "SUM",
                "details": None,
            },
            {
                "heading": "CASE_QUEUE_SLA_ASSIGNED_CUSTOMMETRIC_7516",
                "measurementName": "CASE_QUEUE_SLA_ASSIGNED_CUSTOMMETRIC_7516",
                "aggregateFunction": "SUM",
                "details": None,
            },
            {
                "heading": "CASE_QUEUE_SLA_AWAITING_ASSIGNMENT_CUSTOMMETRIC_4364",
                "measurementName": "CASE_QUEUE_SLA_AWAITING_ASSIGNMENT_CUSTOMMETRIC_4364",
                "aggregateFunction": "SUM",
                "details": None,
            },
            {
                "heading": "CASE_QUEUE_SLA_NEW_CUSTOMMETRIC_575",
                "measurementName": "CASE_QUEUE_SLA_NEW_CUSTOMMETRIC_575",
                "aggregateFunction": "SUM",
                "details": None,
            },
            {
                "heading": "CASE_QUEUE_SLA_TOTAL_WORKED_TIME_CUSTOMMETRIC_6269",
                "measurementName": "CASE_QUEUE_SLA_TOTAL_WORKED_TIME_CUSTOMMETRIC_6269",
                "aggregateFunction": "SUM",
                "details": None,
            },
        ],
        "sorts": [
            {
                "heading": "SUM_CASE_QUEUE_SLA_AWAITING_RESPONSE_CUSTOMMETRIC_8344",
                "order": "DESC",
            }
        ],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "agent_scorecard_process": {
        "report": "CaseProcessingSLAReport",
        "reportingEngine": "PLATFORM",
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "filters": None,
        "groupBys": [
            {
                "heading": "date_0",
                "dimensionName": "date",
                "groupType": "DATE_HISTOGRAM",
                "details": {"isDateTypeDimension": True, "interval": "1d"},
                "namedFilters": None,
            },
            {
                "heading": "USER_ID_1",
                "dimensionName": "USER_ID",
                "groupType": "FIELD",
                "details": {},
                "namedFilters": None,
            },
            {
                "heading": "ACCOUNT_ID_2",
                "dimensionName": "ACCOUNT_ID",
                "groupType": "FIELD",
                "details": {},
                "namedFilters": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY_3",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413c95",
                    "isSecureField": False,
                },
                "namedFilters": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY_4",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413c26",
                },
                "namedFilters": None,
            },
        ],
        "projections": [
            {
                "heading": "M_CaseProcessingSLAReport_CASE_COUNT_0",
                "measurementName": "CASE_COUNT",
                "aggregateFunction": "SUM",
                "details": {},
            },
            {
                "heading": "M_CaseProcessingSLAReport_PROCESSING_SLA_UNIQUE_CASES_1",
                "measurementName": "PROCESSING_SLA_UNIQUE_CASES",
                "aggregateFunction": "SUM",
                "details": {},
            },
            {
                "heading": "M_CaseProcessingSLAReport_CASEPROCESSINGSLA_2",
                "measurementName": "CaseProcessingSLA",
                "aggregateFunction": "SUM",
                "details": {"workingHours": {}, "holidayHours": []},
            },
            {
                "heading": "M_CaseProcessingSLAReport_CASEPROCESSINGSLA_3",
                "measurementName": "CaseProcessingSLA",
                "aggregateFunction": "AVG",
                "details": {"workingHours": {}, "holidayHours": []},
            },
        ],
        "projectionDecorations": [],
        "projectionFilters": None,
        "sorts": [{"heading": "date_0", "order": "ASC"}],
        "streamRequestInfo": None,
        "additional": {
            "translateResponse": "false",
            "widgetId": "6077490627278e7af2421a61",
            "exportInfo": "false",
            "MARGIN": "false",
            "showForecast": "false",
            "dashboardId": "6077490627278e7af2421a2f",
            "engine": "PLATFORM",
            "showTotal": "true",
            "chartType": "TABLE",
            "showRolloverTrends": "false",
            "TABULAR": "true",
        },
        "skipResolve": False,
        "jsonResponse": False,
    },
    "agent_scorecard_response": {
        "timeField": None,
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "page": 0,
        "pageSize": 1000,
        "reportingEngine": "PLATFORM",
        "report": "POST_RESPONSE_TIME",
        "filters": None,
        "groupBys": [
            {
                "heading": "date",
                "dimensionName": "date",
                "groupType": "DATE_HISTOGRAM",
                "details": {"isDateTypeDimension": True, "interval": "1d"},
            },
            {
                "heading": "USER_ID",
                "dimensionName": "USER_ID",
                "groupType": "FIELD",
                "details": None,
            },
        ],
        "projections": [
            {
                "heading": "MESSAGE_RESPONSE_TIME",
                "measurementName": "MESSAGE_RESPONSE_TIME",
                "aggregateFunction": "AVG",
                "details": None,
            }
        ],
        "sorts": [
            {"heading": "CASE_COUNT__SUM__CaseProcessingSLAReport", "order": "DESC"}
        ],
        "projectionDecorations": None,
        "additional": None,
        "skipResolve": False,
        "jsonResponse": False,
    },
    "all_first_response": {
        "reportingEngine": "PLATFORM",
        "report": "CaseSLAReport",
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "pageSize": 1000,
        "page": 0,
        "groupBys": [
            {
                "heading": "DATE_TYPE_CASE_CREATION_TIME",
                "dimensionName": "DATE_TYPE_CASE_CREATION_TIME",
                "groupType": "FIELD",
                "details": {"isDateTypeDimension": True, "interval": "1d"},
            },
            {
                "heading": "CASE_TEXT",
                "dimensionName": "CASE",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "ACCOUNT_ID",
                "dimensionName": "ACCOUNT_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413c95",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY2",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c13830781ea3d3ba3b31d",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "CASE_FIRST_USER_RESPONSE_SLA",
                "measurementName": "CASE_FIRST_USER_RESPONSE_SLA",
                "aggregateFunction": "AVG",
            },
            {
                "heading": "CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
                "measurementName": "CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
                "aggregateFunction": "AVG",
            },
            {
                "heading": "CASE_USER_RESPONSE_SLA",
                "measurementName": "CASE_USER_RESPONSE_SLA",
                "aggregateFunction": "SUM",
            },
            {
                "heading": "AVG_CASE_USER_RESPONSE_SLA",
                "measurementName": "CASE_USER_RESPONSE_SLA",
                "aggregateFunction": "AVG",
            },
        ],
        "filters": [
            {
                "filterType": "NIN",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "values": ["Bot"],
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f3c39ff6329316be3564325",
                    "clientId": None,
                    "ASSET_CLASS": "UNIVERSAL_CASE",
                },
            },
        ],
    },
    "all_handle_time": {
        "reportingEngine": "PLATFORM",
        "report": "CaseProcessingSLAReport",
        "startTime": 0,
        "endTime": 0,
        "timeZone": "America/Los_Angeles",
        "pageSize": 1000,
        "page": 0,
        "groupBys": [
            {
                "heading": "DATE_TYPE_CASE_CREATION_TIME",
                "dimensionName": "DATE_TYPE_CASE_CREATION_TIME",
                "groupType": "FIELD",
                "details": {"isDateTypeDimension": True, "interval": "1d"},
            },
            {
                "heading": "CASE_TEXT",
                "dimensionName": "CASE",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08f98551567d14413a37",
                    "isSecureField": False,
                },
            },
            {
                "heading": "ACCOUNT_ID",
                "dimensionName": "ACCOUNT_ID",
                "groupType": "FIELD",
                "details": None,
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c08fa8551567d14413c95",
                    "isSecureField": False,
                },
            },
            {
                "heading": "UNIVERSAL_CASE_CUSTOM_PROPERTY2",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "groupType": "FIELD",
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f2c13830781ea3d3ba3b31d",
                    "isSecureField": False,
                },
            },
        ],
        "projections": [
            {
                "heading": "CaseProcessingSLA",
                "measurementName": "CaseProcessingSLA",
                "aggregateFunction": "SUM",
            },
            {
                "heading": "AvgCaseProcessingSLA",
                "measurementName": "CaseProcessingSLA",
                "aggregateFunction": "AVG",
            },
        ],
        "filters": [
            {
                "filterType": "NIN",
                "dimensionName": "UNIVERSAL_CASE_CUSTOM_PROPERTY",
                "values": ["Bot"],
                "details": {
                    "srcType": "CUSTOM",
                    "fieldName": "5f3c39ff6329316be3564325",
                    "ASSET_CLASS": "UNIVERSAL_CASE",
                },
            },
        ],
        "sorts": {"heading": "CaseProcessingSLA", "order": "DESC"},
    },
}
