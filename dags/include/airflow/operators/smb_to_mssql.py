import tempfile
from pathlib import Path

import pandas as pd
from airflow.models import BaseOperator

from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.airflow.hooks.smb import SMBHook


class BaseSMBToMsSqlOperator(BaseOperator):
    """Moves filesystem data to MSSQL tables.

    Attributes:
        smb_conn_id: environment defined connection string's label.
        mssql_conn_id: environment defined connection string's label.
        remote_path: file path from the share_name including file name.
        share_name: remote server folder share name.
        database: target database.
        schema: target schema.
        table: target table.
        if_exists: How to behave if the table already exists.

            * ``fail``: Raises a ValueError.
            * ``replace``: Drops the table before inserting new values.
            * ``append``: Inserts new values to the existing table.
        truncate_table: default True.  if True, truncates target table before data is inserted.
    """

    def __init__(
        self,
        smb_conn_id,
        mssql_conn_id,
        remote_path,
        share_name,
        database,
        schema,
        table,
        if_exists,
        truncate_table=True,
        **kwargs,
    ):
        self.smb_conn_id = smb_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.database = database
        self.remote_path = remote_path
        self.share_name = share_name
        self.schema = schema
        self.table = table
        self.if_exists = if_exists
        self.truncate_table = truncate_table
        super().__init__(**kwargs)

    def get_as_df(self, filename):
        """method to be implemented by user.

        method should return data frame.
        """
        raise NotImplementedError

    def execute(self, context):
        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        mssql_hook = MsSqlOdbcHook(
            mssql_conn_id=self.mssql_conn_id, database=self.database
        )
        with tempfile.TemporaryDirectory() as td:
            filename = Path(td, "temp_file").as_posix()
            with open(file=filename, mode="wb") as f, smb_hook.get_conn() as smb_client:
                smb_client.retrieveFile(
                    service_name=self.share_name, path=self.remote_path, file_obj=f
                )
            df = self.get_as_df(filename=filename)
            with mssql_hook.get_sqlalchemy_connection() as mssql_cnx:
                if self.truncate_table:
                    cmd = "truncate table " + self.schema + "." + self.table
                    mssql_cnx.execute(cmd)
                df.to_sql(
                    name=self.table,
                    con=mssql_cnx,
                    schema=self.schema,
                    if_exists=self.if_exists,
                    index=False,
                )


class SMBExcelToMsSqlOperator(BaseSMBToMsSqlOperator):
    def __init__(self, sheet_name=0, **kwargs):
        self.sheet_name = sheet_name
        super().__init__(**kwargs)

    def get_as_df(self, filename):
        df = pd.read_excel(filename, sheet_name=self.sheet_name)
        return df


class SMBCSVToMsSqlOperator(BaseSMBToMsSqlOperator):
    def __init__(self, header="infer", skiprows=None, names=None, **kwargs):
        self.header = header
        self.skiprows = skiprows
        self.names = names
        super().__init__(**kwargs)

    def get_as_df(self, filename):
        df = pd.read_csv(
            filename, header=self.header, skiprows=self.skiprows, names=self.names
        )
        return df
