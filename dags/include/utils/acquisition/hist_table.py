from edm.acquisition.configs import TableNotFoundError, get_table_config
from include.utils.string import unindent_auto


class HistTable:
    """
    This helper class provides a way to create historical tables that track changes in our
    ecom tables using effective dates.

    Args:
        table_name

    Examples:
        >>> hist_table = HistTable(
        >>>     table_name='lake.ultra_merchant.order',
        >>> )
        >>> hist_table.create_hist_table(dry_run=True)

    """

    def __init__(self, table_name):
        self.table_config = get_table_config(table_name=table_name)
        self.column_name_list = self.table_config.column_list.column_name_list
        if self.table_config.column_list.delta_column_list:
            self.delta_column = self.table_config.column_list.delta_column_list[0].name
        else:
            self.delta_column = None
        self.uniqueness_cols_str = self.table_config.column_list.uniqueness_cols_str
        self.select_list = self.table_config.column_list.select_list
        self.archive_database = self.table_config.to_snowflake_operator.archive_database
        self.database = self.table_config.target_database
        self.schema = self.table_config.target_schema
        self.table = self.table_config.target_table
        self.table_hist = self.table_config.table.strip('[').strip(']')

        if not self.archive_database:
            raise Exception(f"table name {table_name} not found in archive database")

        try:
            delete_log_table_config = get_table_config(
                table_name=f'{self.database}.{self.schema}.{self.table_hist}_delete_log'
            )
            delete_log_table_name = delete_log_table_config.target_table
            delete_log_column_name_list = delete_log_table_config.column_list.column_name_list
        except TableNotFoundError:
            delete_log_table_name = None
            delete_log_column_name_list = []

        self.delete_log_table_name = delete_log_table_name
        self.delete_log_column_name_list = delete_log_column_name_list

    @property
    def datetime_deleted(self):
        if 'datetime_deleted' in self.delete_log_column_name_list:
            return 'datetime_deleted'
        elif 'datetime_added' in self.delete_log_column_name_list:
            return 'datetime_added'
        else:
            return None

    @property
    def delete_log_select_list(self):
        if self.delete_log_table_name:
            nullable_delete_log_column_list = []
            for col in self.column_name_list:
                if col.lower() == self.delta_column.lower():
                    nullable_delete_log_column_list.append(self.datetime_deleted + ' AS ' + col)
                elif col in self.delete_log_column_name_list:
                    nullable_delete_log_column_list.append(col)
                else:
                    nullable_delete_log_column_list.append('NULL AS ' + col)
            return '        ' + ',\n        '.join(nullable_delete_log_column_list)
        else:
            return None

    @property
    def ddl_hist_table(self):
        unique_col_list = ', '.join(self.uniqueness_cols_str)
        archive_select_list = '        ' + ',\n        '.join(self.column_name_list)

        create_table = f"""
            CREATE OR REPLACE TRANSIENT TABLE {self.archive_database}.{self.schema}.{self.table_hist}__hist
            AS
            SELECT\n{self.select_list},
                is_deleted,
                {self.delta_column} AS effective_from_datetime,
                LEAD({self.delta_column}, 1, '9999-12-31')
                    OVER (PARTITION BY {unique_col_list}
                    ORDER BY {self.delta_column} ASC) AS effective_to_datetime,
                effective_to_datetime = '9999-12-31' AS is_current
            FROM (
                SELECT\n{archive_select_list},
                    FALSE AS is_deleted,
                    row_number() OVER (PARTITION BY {unique_col_list}, {self.delta_column}
                        ORDER BY NULL) AS row_dup_num
                FROM {self.archive_database}.{self.schema}.{self.table}
            """
        if self.delete_log_select_list:
            delete_select = f"""    UNION
                SELECT\n{self.delete_log_select_list},
                    TRUE AS is_deleted,
                    row_number() OVER (PARTITION BY {unique_col_list}, {self.datetime_deleted}
                        ORDER BY NULL) AS row_dup_num
                FROM {self.database}.{self.schema}.{self.delete_log_table_name}
                """
        else:
            delete_select = ''
        where_clause = """) AS d
            WHERE d.row_dup_num = 1;
            """
        sql = unindent_auto(create_table + delete_select + where_clause)
        return sql

    def create_hist_table(self, dry_run=False):
        """
        Creates a historical table with effective dates and an is_currrent flag.

        Args:
            dry_run: if True, will print out sql commands but not execute anything

        """

        if dry_run:
            print(self.ddl_hist_table)
        else:
            hook = self.table_config.to_snowflake_operator.snowflake_hook
            hook.execute_multiple(self.ddl_hist_table)
