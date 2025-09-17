# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


from airflow.hooks.dbapi import DbApiHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from functools import cached_property
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from include.config import conn_ids


class GoogleAnalyticsHook(GoogleBaseHook, DbApiHook, LoggingMixin):
    """
    Interact with Google Analytics. This hook uses the Google Cloud Platform
    connection.
    """

    def __init__(
        self,
        bigquery_conn_id=conn_ids.BigQuery.default,
        delegate_to=None,
        use_legacy_sql=False,
        location=None,
    ):
        super(GoogleAnalyticsHook, self).__init__(
            gcp_conn_id=bigquery_conn_id, delegate_to=delegate_to
        )
        self.use_legacy_sql = use_legacy_sql
        self.location = location

    @cached_property
    def service(self):
        http_authorized = self._authorize()
        service = build("analytics", "v3", http=http_authorized, cache_discovery=False)
        return service

    def upload_media_file(self, account_id, property_id, dataset_id, file):
        """
        Uploads a file into Google analytics using Data Import APIs.
        """
        media = MediaFileUpload(
            file, mimetype="application/octet-stream", resumable=False
        )
        self.service.management().uploads().uploadData(
            accountId=account_id,
            webPropertyId=property_id,
            customDataSourceId=dataset_id,
            media_body=media,
        ).execute()
