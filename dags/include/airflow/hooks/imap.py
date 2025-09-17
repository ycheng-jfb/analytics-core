import email
import imaplib
from email.message import Message
from typing import Iterable

from airflow.hooks.base import BaseHook


class ImapHook(BaseHook):
    def __init__(self, imap_conn_id: str = "imap_default", mailbox="INBOX"):
        self.imap_conn_id = imap_conn_id
        self.mailbox = mailbox

    def get_conn(self):
        conn = self.get_connection(self.imap_conn_id)
        client = imaplib.IMAP4_SSL(conn.host, 993)
        client.login(conn.login, conn.password)
        client.select(mailbox=self.mailbox)
        return client

    def search_messages(self, search_query, mailbox="INBOX") -> Iterable[Message]:
        client = self.get_conn()
        client.select(mailbox)
        search_status, search_response = client.search(None, search_query)
        id_list_string = search_response[0]
        id_list = id_list_string.split()
        for id in id_list:
            fetch_status, fetch_response = client.fetch(id, "(RFC822)")
            email_body = fetch_response[0][1]
            message = email.message_from_bytes(email_body)
            yield message

    @staticmethod
    def is_attachment(message: Message) -> bool:
        if message.get_content_maintype() == "multipart":
            return False
        elif message.get("Content-Disposition") is None:
            return False
        else:
            return True

    def iterate_attachments(self, message: Message) -> Iterable[Message]:
        for message_part in message.walk():
            if self.is_attachment(message_part):
                yield message_part

    def get_message_body(self, message: Message):
        for message_part in message.walk():
            if message_part.get_content_maintype() == "multipart":
                continue
            return message_part
