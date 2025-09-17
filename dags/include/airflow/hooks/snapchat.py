import csv
import gzip
import json

import requests
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.config import conn_ids

print_ = lambda x: print(json.dumps(x, indent=2))


class SnapchatAdsHook(BaseHook):
    def __init__(
        self,
        snapchat_conn_id=conn_ids.Snapchat.default,
        s3_conn_id=conn_ids.AWS.tfg_default,
    ):
        self.snapchat_conn_id = snapchat_conn_id
        self.s3_conn_id = s3_conn_id
        self.session = None
        self.connection = self.get_connection(snapchat_conn_id)
        self.client_id = self.connection.login
        self.client_secret = self.connection.password
        self.refresh_token = self.connection.extra_dejson.get("refresh_token")

    def get_conn_for_conversions_api(self):
        headers = {
            "Authorization": "Bearer " + self.refresh_token,
            "Content-Type": "application/json",
        }
        session = requests.Session()
        session.headers = headers
        return session

    def get_conn(self, force=False):
        if not force and self.session:
            return self.session

        params = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        r = requests.post(
            "https://accounts.snapchat.com/login/oauth2/access_token", params=params
        )
        r.raise_for_status()
        access_token = r.json()["access_token"]
        headers = {"Authorization": "Bearer " + access_token}
        session = requests.Session()
        session.headers = headers
        return session

    def make_request(self, method, rel_url, params=None):
        base_url = "https://adsapi.snapchat.com/v1"
        url = f"{base_url}/{rel_url}"
        self.session = self.get_conn()
        r = self.session.request(method=method, url=url, params=params)
        if r.status_code == 401:
            self.session = self.get_conn(force=True)
            r = self.session.request(method=method, url=url, params=params)
        if r.status_code != 200:
            print(f"headers: {r.headers}")
            print(f"content: {r.content}")
            print(f"status_code: {r.status_code}")
            r.raise_for_status()
            raise Exception(r.content)
        else:
            return r

    def get_organization_ids(self):
        r = self.make_request("GET", "me/organizations")
        org_ids = [x["organization"]["id"] for x in r.json()["organizations"]]
        return org_ids

    def get_account_ids(self, organization_id, get_timezone=False):
        rel_url = f"organizations/{organization_id}/adaccounts"
        r = self.make_request("GET", rel_url)
        if get_timezone:
            ad_accounts = [
                {"id": x["adaccount"]["id"], "timezone": x["adaccount"]["timezone"]}
                for x in r.json()["adaccounts"]
            ]
        else:
            ad_accounts = [x["adaccount"]["id"] for x in r.json()["adaccounts"]]
        return ad_accounts

    def get_campaign_ids_for_account(self, account_id):
        rel_url = f"adaccounts/{account_id}/campaigns"
        r = self.make_request("GET", rel_url)
        rdict = r.json()
        campaigns = rdict["campaigns"]
        campaign_ids = [x["campaign"]["id"] for x in campaigns]

        return campaign_ids

    def get_all_campaign_ids(self):
        print("getting organization ids")
        organization_id_list = self.get_organization_ids()

        account_list = []

        print("getting account lists")
        for oid in organization_id_list:
            account_list.extend(self.get_account_ids(oid))

        campaign_list = []

        print("getting campaign list")
        for aid in account_list:
            campaign_list.extend(self.get_campaign_ids_for_account(aid))

        return campaign_list

    def get_all_account_ids(self, get_timezone=False):
        print("getting organization ids")
        organization_id_list = self.get_organization_ids()

        account_list = []

        print("getting account lists")
        for oid in organization_id_list:
            account_list.extend(self.get_account_ids(oid, get_timezone))

        return account_list

    def get_paginated_request(self, url, params=None):
        r = self.session.get(url, params=params)
        r.raise_for_status()
        rdict = r.json()
        yield rdict
        while True:
            print("1")
            paging = rdict.get("paging", {})
            next_link = paging.get("next_link")
            if next_link:
                r = self.session.get(next_link)
                r.raise_for_status()
                rdict = r.json()
                yield rdict
            else:
                break

    def write_dict_rows_to_file(
        self, rows, column_list, filename: str, compression="gzip"
    ):
        open_func = gzip.open if compression == "gzip" else open
        with open_func(filename, "wt") as f:  # type: ignore
            writer = csv.DictWriter(
                f,
                delimiter="\t",
                dialect="unix",
                extrasaction="ignore",
                fieldnames=column_list,
                quoting=csv.QUOTE_MINIMAL,
            )
            print(filename)
            for row in rows:
                writer.writerow(row)

    def upload_to_s3(self, filename, bucket, key, s3_replace=True):
        s3_hook = S3Hook(self.s3_conn_id)
        print(f"uploading to {key}")
        s3_hook.load_file(
            filename=filename.as_posix(),
            key=key,
            bucket_name=bucket,
            replace=s3_replace,
        )
        print(f"uploaded to {key}")
