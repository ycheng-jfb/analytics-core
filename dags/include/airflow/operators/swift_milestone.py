import os
import tempfile

import pendulum
from airflow.models import BaseOperator

from include.airflow.hooks.imap import ImapHook
from include.airflow.hooks.msgraph import MsGraphMailHook
from include.airflow.hooks.smb import SMBHook


class DepreciatedSwiftMilestoneToSMBOperator(BaseOperator):
    def __init__(
        self,
        remote_path: str,
        smb_conn_id: str,
        imap_conn_id: str = "imap_swift_milestones",
        share_name: str = "",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.remote_path = remote_path
        self.smb_conn_id = smb_conn_id
        self.imap_conn_id = imap_conn_id
        self.share_name = share_name

    def execute(self, context=None):
        imap_hook = ImapHook(imap_conn_id=self.imap_conn_id)
        subjects = ["File For Bill To: 835033", "Swift / Just Fab tracking report"]
        queries = [
            f'(FROM "christine_kloos@swifttrans.com" SUBJECT "{sub}" UNSEEN)'
            for sub in subjects
        ]
        results = [imap_hook.search_messages(search_query=q) for q in queries]

        with tempfile.TemporaryDirectory() as td:
            for messages in results:
                for message in messages:
                    mail_time = pendulum.parse(message["Date"]).strftime("_%Y%m%d_%H%M")
                    file_name = f"CustomerShipments{mail_time}.CSV"
                    for message_part in imap_hook.iterate_attachments(message):
                        data_bytes = message_part.get_payload(decode=True)
                        data = data_bytes.decode("utf-8")
                        local_path = os.path.join(td, file_name)
                        with open(local_path, "w") as f:
                            f.write(data)

                    smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
                    smb_hook.upload(
                        share_name=self.share_name,
                        remote_path=os.path.join(self.remote_path, file_name),
                        local_path=local_path,
                    )


class SwiftMilestoneToSMBOperator(BaseOperator):
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
        msgraph_conn_id: str = "msgraph_default",
        share_name: str = "",
        subjects: list = None,
        attachments_list: list = None,
        file_extensions: list = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if file_extensions is None:
            file_extensions = ["xls", "xlsx"]
        self.remote_path = remote_path
        self.smb_conn_id = smb_conn_id
        self.resource_address = resource_address
        self.from_address = from_address
        self.msgraph_conn_id = msgraph_conn_id
        self.share_name = share_name
        self.subjects = subjects
        self.attachments_list = attachments_list
        self.file_extensions = file_extensions

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
            msgraph_hook.download_attachments(file_path=td)
            for filename in os.listdir(td):
                file_parts = filename.rsplit("_", 2)
                remotefilename = f"CustomerShipments_{file_parts[1]}_{file_parts[2]}"
                smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
                smb_hook.upload(
                    share_name=self.share_name,
                    remote_path=os.path.join(self.remote_path, remotefilename),
                    local_path=os.path.join(td, filename),
                )
