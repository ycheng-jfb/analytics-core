import tempfile
from airflow.hooks.base import BaseHook
import gnupg


class GPGHook(BaseHook):
    """
    Interaction with gpg protocol

    Args:
        gpg_conn_id: gpg connection which have client_id, secret_id and tenant_id for the connection
    """

    def __init__(
        self,
        gpg_conn_id: str,
    ):
        self.gpg_conn_id = gpg_conn_id

    def get_conn(self):
        """
        Connection establishment for gpg using connection_id and resource_address
        """
        conn = self.get_connection(self.gpg_conn_id)
        credentials = conn.extra_dejson["extra__keyfile_dict"]
        return credentials

    def decrypt_file(self, encrypted_content):
        with tempfile.TemporaryDirectory() as gnupghome:
            gpg = gnupg.GPG(gnupghome=gnupghome)
            private_key = self.get_conn()
            import_result = gpg.import_keys(key_data=private_key)
            gpg.trust_keys(
                [x["fingerprint"] for x in import_result.results], "TRUST_ULTIMATE"
            )
            return gpg.decrypt(encrypted_content)
