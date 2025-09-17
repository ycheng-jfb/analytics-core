from edm.acquisition.configs import get_table_config
from include.utils.string import unindent_auto


class HistView:
    """
    This helper class provides a way to create historical views that track changes in our
    ecom tables using effective dates.

    Args:
        table_name

    Examples:
        >>> hist_view = HistView(
        >>>     table_name='lake.ultra_merchant.membership',
        >>> )
        >>> hist_view.create_hist_view(dry_run=True)

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
        self.schema = self.table_config.target_schema
        self.table = self.table_config.target_table
        self.table_hist = self.table_config.table.strip('[').strip(']')

        if not self.archive_database:
            raise Exception(f"table name {table_name} not found in archive database")

    @property
    def ddl_hist_view(self):
        unique_col_list = ', '.join(self.uniqueness_cols_str)
        create_view = f"""
            CREATE VIEW {self.archive_database}.{self.schema}.{self.table_hist}__hist
            AS
            SELECT\n{self.select_list},
                {self.delta_column} AS effective_from_datetime,
                LEAD({self.delta_column}, 1, '9999-12-31')
                    OVER (PARTITION BY {unique_col_list}
                    ORDER BY {self.delta_column} ASC) AS effective_to_datetime,
                effective_to_datetime = '9999-12-31' AS is_current
            FROM {self.archive_database}.{self.schema}.{self.table};
            """
        sql = unindent_auto(create_view)
        return sql

    def create_hist_view(self, dry_run=False):
        """
        Creates a historical view with effective dates and an is_currrent flag.

        Args:
            dry_run: if True, will print out sql commands but not execute anything

        """

        if dry_run:
            print(self.ddl_hist_view)
        else:
            hook = self.table_config.to_snowflake_operator.snowflake_hook
            hook.execute_multiple(self.ddl_hist_view)
