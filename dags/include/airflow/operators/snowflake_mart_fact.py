from typing import List

from include.airflow.operators.snowflake_mart_base import BaseSnowflakeMartUpsertOperator, Column
from include.utils.string import unindent_auto


class SnowflakeMartFactOperator(BaseSnowflakeMartUpsertOperator):

    ui_fgcolor = '#000000'
    ui_color = '#C8EDF5'

    @property
    def base_table_meta_cols(self) -> List[Column]:
        base_table_meta_cols = [
            Column('meta_row_hash', 'INT'),
            Column('meta_create_datetime', 'TIMESTAMP_LTZ'),
            Column('meta_update_datetime', 'TIMESTAMP_LTZ'),
        ]
        return base_table_meta_cols

    def load_delta_table(self):
        cmd = f"""
                    INSERT INTO {self.delta_table_name}
                    SELECT
                        {self.delta_table_col_str},
                        {self.meta_row_hash} AS meta_row_hash,
                        s.meta_create_datetime AS meta_create_datetime,
                        s.meta_update_datetime AS meta_update_datetime
                    FROM {self.stg_table_name} s
                    {self.key_lookup_join_str}
                    WHERE s.meta_data_quality IS DISTINCT FROM 'error';
                    """
        return unindent_auto(cmd)
