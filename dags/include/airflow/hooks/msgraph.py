import tempfile
from datetime import datetime
from functools import cached_property
from pathlib import Path

import requests
from airflow.hooks.base import BaseHook
from include.utils.decorators import retry_wrapper
from O365 import Account, FileSystemTokenBackend


class MsGraphMailHook(BaseHook):
    """
    Interaction with MSGraph protocol

    Args:
        msgraph_conn_id: MSGraph connection which have client_id, secret_id and tenant_id for the connection
        resource_address: source mail address in which we are going to search mails with required attachments
        from_address: from which mail address we are going to get the attachments
        subjects: (optional) list of mail subject for searching
        attachments_list: (optional) list of file names to be downloaded from mails.
                if None, all the attachments will be downloaded
        file_extensions: (optional) list of file types to be downloaded. Default values [xls, xlsx]
    """

    def __init__(
        self,
        msgraph_conn_id: str,
        resource_address: str,
        from_address: str,
        subjects: list = None,
        attachments_list: list = None,
        file_extensions: list = None,
    ):
        self.msgraph_conn_id = msgraph_conn_id
        self.from_address = from_address
        self.subjects = subjects
        self.attachments_list = attachments_list
        self.resource_address = resource_address
        self.file_extensions = file_extensions

    def get_conn(self):
        """
        Connection establishment for MSGraph using connection_id and resource_address
        """
        conn = self.get_connection(self.msgraph_conn_id)
        credentials = (conn.extra_dejson["client_id"], conn.extra_dejson["secret_id"])
        with tempfile.TemporaryDirectory() as td:
            token_backend = FileSystemTokenBackend(
                token_path=td, token_filename="tmp_token.txt"
            )
            account = Account(
                credentials,
                auth_flow_type="credentials",
                tenant_id=conn.extra_dejson["tenant_id"],
                token_backend=token_backend,
            )
            if account.authenticate():
                client = account.mailbox(resource=self.resource_address)
            FileSystemTokenBackend(
                token_path=td, token_filename="tmp_token.txt"
            ).delete_token()
        return client

    def download_attachments(self, file_path: str, add_timestamp: bool = True):
        """
        searches for the particular unread mails with provided from_address and subject
        Downloads attachments from the filtered mails
        """
        client = self.get_conn()
        queries = []
        if "," in self.from_address:
            search_filter = ""
            for sender_mail in self.from_address.split(","):
                search_filter += "from:" + sender_mail + " or "
            search_filter = search_filter.rstrip("or ")
        else:
            search_filter = "from:" + self.from_address

        print(f"search_filter - {search_filter}")
        if self.subjects and len(self.subjects) != 0:
            for subject in self.subjects:
                search_filter = "from:" + self.from_address + " AND subject:" + subject
                queries.append(client.q().search(search_filter))
        else:
            queries.append(client.q().search(search_filter))

        result = list(
            client.get_messages(query=query, download_attachments=True, limit=None)
            for query in queries
        )

        for messages in result:
            for message in messages:
                if message.has_attachments and not message.is_read:
                    mail_time = datetime.strftime(message.received, "_%Y%m%d_%H%M%S")
                    for att in message.attachments:
                        file_name = str(att).replace("Attachment: ", "")
                        file_parts = file_name.rsplit(".", 1)
                        new_file_name = f"{file_parts[0]}{mail_time if add_timestamp else ''}.{file_parts[1]}"

                        if self.file_extensions is not None and file_parts[
                            1
                        ].lower() in [x.lower() for x in self.file_extensions]:
                            if self.attachments_list:
                                if file_name.lower() in [
                                    x.lower() for x in self.attachments_list
                                ]:
                                    att.save(
                                        location=file_path, file_name=new_file_name
                                    )
                                    print(f"Moved file...{new_file_name}")
                            else:
                                att.save(location=file_path, custom_name=new_file_name)
                                print(f"Moved file...{new_file_name}")

                message.mark_as_read()


class MsGraphSharePointHook(BaseHook):
    """
    Interaction with MSGraph protocol

    Args:
        sharepoint_conn_id: MSGraph connection which have client_id, secret_id and tenant_id for the connection
    """

    def __init__(
        self,
        sharepoint_conn_id: str,
    ):
        self.sharepoint_conn_id = sharepoint_conn_id

    @cached_property
    def get_token(self):
        """
        MSGraph token for sharepoint upload
        """
        conn = self.get_connection(self.sharepoint_conn_id)
        client_id = conn.extra_dejson["client_id"]
        client_secret = conn.extra_dejson["client_secret"]
        tenant_id = conn.extra_dejson["tenant_id"]
        auth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
        }
        response = requests.post(auth_url, data=data)
        access_token = response.json()["access_token"]
        if response.status_code != 200:
            print("Failed with Status Code:")
            raise Exception(f"{str(response.status_code)} - {response.reason}")
        return access_token

    @cached_property
    def session(self):
        headers = {
            "Authorization": f"Bearer {self.get_token}",
            "content-type": "application/json",
        }
        session = requests.session()
        session.headers = headers
        return session

    @retry_wrapper(3, Exception, sleep_time=5)
    def make_request(
        self,
        url: str,
        method: str = "GET",
        params: dict = None,
        data: dict = None,
        files: dict = None,
    ):
        response = self.session.request(
            method=method, url=url, params=params, data=data, files=files
        )
        response.raise_for_status()
        return response

    @retry_wrapper(3, Exception, sleep_time=5)
    def upload(self, site_id: str, drive_id: str, folder_name: str, file_path: Path):
        """
        Upload file to SharePoint
        """
        access_token = self.get_token
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/octet-stream",
        }
        upload_url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/root:/"
            f"{folder_name}/{file_path.name}:/content"
        )
        self.log.info(f"upload to sharepoint: {file_path.name}")
        with open(file_path, "rb") as file:
            response = requests.put(upload_url, headers=headers, data=file)
            if response.status_code != 200:
                self.log.info("Failed with Status Code:")
                raise Exception(f"{str(response.status_code)} - {response.reason}")

    @retry_wrapper(3, Exception, sleep_time=5)
    def list_file(
        self, site_id: str, drive_id: str, source_folder_name: str, file_path: str
    ):
        """
        List file in SharePoint
        """
        file_url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/root:/"
            f"{source_folder_name}/{file_path}:/children"
        )
        response = self.session.get(file_url)
        response.raise_for_status()
        return response.json()

    @retry_wrapper(3, Exception, sleep_time=5)
    def move_file(
        self,
        site_id: str,
        drive_id: str,
        file_id: str,
        file_name: str,
        destination_folder_name: str,
    ):
        """
        Move file in SharePoint
        """
        # get archive folder_id
        destination_folder_url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/root:/"
            f"{destination_folder_name}"
        )
        destination_folder_metadata_response = self.session.get(destination_folder_url)
        destination_folder_metadata_response.raise_for_status()
        destination_folder_id = destination_folder_metadata_response.json()["id"]

        move_file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}"
        data = {"parentReference": {"id": destination_folder_id}, "name": file_name}
        response = self.session.patch(move_file_url, json=data)
        response.raise_for_status()
        self.log.info(f"archive file: {file_name}")
