import os
import tempfile
from datetime import datetime

from airflow.models import BaseOperator

from include.airflow.hooks.imap import ImapHook
from include.airflow.hooks.msgraph import MsGraphMailHook
from include.airflow.hooks.smb import SMBHook


class DepreciatedEmailToSMBOperator(BaseOperator):
    def __init__(
        self,
        remote_path: str,
        smb_conn_id: str,
        filter_queries: list,
        smb_file_name: str = '',
        imap_conn_id: str = "imap_default",
        share_name: str = "",
        file_extensions=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if file_extensions is None:
            file_extensions = ['xls', 'xlsx']
        self.remote_path = remote_path
        self.smb_conn_id = smb_conn_id
        self.imap_conn_id = imap_conn_id
        self.share_name = share_name
        self.filter_queries = filter_queries
        self.smb_file_name = smb_file_name
        self.file_extensions = file_extensions

    def execute(self, context=None):
        imap_hook = ImapHook(imap_conn_id=self.imap_conn_id)
        queries = self.filter_queries
        results = [imap_hook.search_messages(search_query=q) for q in queries]
        with tempfile.TemporaryDirectory() as td:
            for messages in results:
                for message in messages:
                    mail_time = datetime.strptime(
                        message['Date'], '%a, %d %b %Y %H:%M:%S %z'
                    ).strftime("_%Y%m%d_%H%M%S")
                    for message_part in imap_hook.iterate_attachments(message):
                        data_bytes = message_part.get_payload(decode=True)
                        file_name = (
                            message_part.get_filename()
                            if self.smb_file_name == ''
                            else self.smb_file_name
                        )
                        if file_name:
                            file_parts = file_name.rsplit('.', 1)
                            if file_parts[1] in self.file_extensions:
                                file_name = f"{file_parts[0]}{mail_time}.{file_parts[1]}"
                                local_path = os.path.join(td, file_name)
                                with open(local_path, "wb") as f:
                                    f.write(data_bytes)

                                smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
                                smb_hook.upload(
                                    share_name=self.share_name,
                                    remote_path=os.path.join(self.remote_path, file_name),
                                    local_path=local_path,
                                )


class EmailToSMBOperator(BaseOperator):
    """
    Downloads mail attachments and upload them to SMB location

    Args:
        remote_path: smb remote directory path to upload the files
        smb_conn_id: smb connection string
        share_name: smb share name
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
        remote_path: str,
        smb_conn_id: str,
        resource_address: str,
        from_address: str,
        msgraph_conn_id: str = 'msgraph_default',
        share_name: str = "",
        subjects: list = None,
        attachments_list: list = None,
        file_extensions: list = None,
        add_timestamp_to_file: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if file_extensions is None:
            file_extensions = ['xls', 'xlsx']
        self.remote_path = remote_path
        self.smb_conn_id = smb_conn_id
        self.resource_address = resource_address
        self.from_address = from_address
        self.msgraph_conn_id = msgraph_conn_id
        self.share_name = share_name
        self.subjects = subjects
        self.attachments_list = attachments_list
        self.file_extensions = file_extensions
        self.add_timestamp_to_file = add_timestamp_to_file

    def execute(self, context=None):
        msgraph_hook = MsGraphMailHook(
            msgraph_conn_id=self.msgraph_conn_id,
            resource_address=self.resource_address,
            from_address=self.from_address,
            subjects=self.subjects,
            file_extensions=self.file_extensions,
            attachments_list=self.attachments_list,
        )
        with tempfile.TemporaryDirectory() as td:
            msgraph_hook.download_attachments(
                file_path=td, add_timestamp=self.add_timestamp_to_file
            )
            for filename in os.listdir(td):
                smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
                smb_hook.upload(
                    share_name=self.share_name,
                    remote_path=os.path.join(self.remote_path, filename),
                    local_path=os.path.join(td, filename),
                )
