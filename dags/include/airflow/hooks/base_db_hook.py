from abc import abstractmethod
from contextlib import closing

import pandas as pd
from airflow.hooks.base import BaseHook


class BaseDbHook(BaseHook):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    @abstractmethod
    def hook_copy_to_file(self, sql, conn, file_obj):
        raise NotImplementedError

    def copy_to_file(self, sql, file_obj):
        with closing(self.get_conn()) as conn:
            self.hook_copy_to_file(sql, conn, file_obj)
            conn.commit()

    def pandas_copy_to_file(self, sql, conn, file_obj, sep='\t'):
        df = pd.read_sql_query(sql, conn)
        df.to_csv(file_obj.filename, index=False, sep=sep)

    def pandas_read_sql_query(self, sql):
        with closing(self.get_conn()) as conn:
            return pd.read_sql_query(sql, conn)
