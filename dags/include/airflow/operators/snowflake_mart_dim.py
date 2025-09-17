from typing import List

from include.airflow.operators.snowflake_mart_base import BaseSnowflakeMartUpsertOperator, Column
from include.utils.string import unindent_auto


class SnowflakeMartDimOperator(BaseSnowflakeMartUpsertOperator):
    INITIAL_FROM_TIMESTAMP = "to_timestamp_ltz('1900-01-01')"
    INITIAL_TO_TIMESTAMP = "to_timestamp_ltz('9999-12-31')"

    ui_fgcolor = '#000000'
    ui_color = '#EBF6BF'

    @property
    def base_table_meta_cols(self) -> List[Column]:
        base_table_meta_cols = [
            Column('effective_start_datetime', f"{self.EFF_START_TIMESTAMP_COL_TYPE}"),
            Column('effective_end_datetime', f"{self.EFF_START_TIMESTAMP_COL_TYPE}"),
            Column('is_current', 'BOOLEAN'),
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
                        {self.INITIAL_FROM_TIMESTAMP} AS effective_start_datetime,
                        {self.INITIAL_TO_TIMESTAMP} AS effective_end_datetime,
                        TRUE AS is_current,
                        {self.meta_row_hash} AS meta_row_hash,
                        s.meta_create_datetime AS meta_create_datetime,
                        s.meta_update_datetime AS meta_update_datetime
                    FROM {self.stg_table_name} s
                    {self.key_lookup_join_str}
                    WHERE s.meta_data_quality IS DISTINCT FROM 'error';
                    """
        return unindent_auto(cmd)
