import logging
import warnings

from functools import cached_property
from snowflake.connector import ProgrammingError

from edm.acquisition.configs import get_table_config
from include.airflow.hooks.snowflake import SnowflakeHook
from include.config import snowflake_roles
from include.utils.string import unindent_auto


class MissingDeltaColException(Exception):
    pass


class ArchiveSourceTable:
    """
    Helper class to facilitate the merging of an archive source into lake_archive.

    Args:
        full_name: the full name of the source table, e.g. ``archive_blah_20200101.um.order_credit``

    """

    SCHEMA_MAP = {
        'ultracms': 'ultra_cms',
        'ultrarollup': 'ultra_rollup',
        'um': 'ultra_merchant',
        'uw': 'ultra_warehouse',
        'dbo': 'ultra_merchant',
    }

    def __init__(self, full_name):
        self.database, self.schema, self.table = full_name.split('.')
        self.full_name = f'{self.database}.{self.schema}."{self.table.upper()}"'

    def __repr__(self):
        return self.full_name

    @cached_property
    def snowflake_hook(self):
        return SnowflakeHook(role=snowflake_roles.etl_service_account, warehouse='DA_WH_ETL_LIGHT')

    @cached_property
    def source_columns(self):
        """
        List of column names in the source archive table.

        """
        query = f"""
        SELECT
            l.column_name
        FROM {self.database}.information_schema.columns l
        WHERE lower(l.table_name) = '{self.table}'
          AND lower(l.table_schema) = '{self.schema}'
        ORDER BY l.ordinal_position
        """
        rows = self.snowflake_hook.get_records(query)
        return [x[0].lower() for x in rows]

    @property
    def lake_columns(self):
        """
        List of column names in the current lake table.

        """
        return [x.name for x in self.lake_column_list]

    @cached_property
    def lake_config(self):
        """
        Current TableConfig class instance for the lake table that corresponds to this archive table.

        """
        return get_table_config(self.lake_table_name.lower().replace('"', ''))

    @cached_property
    def lake_column_list(self):
        """
        List of snowflake Column class instances for current lake table.

        """
        return self.lake_config.column_list

    @cached_property
    def watermark_column(self):
        """
        The first delta column in the current lake table config, if one exists, else ``None``.

        """
        try:
            return self.lake_column_list.delta_column_list[0].name
        except IndexError:
            return

    @property
    def delta_table_name(self):
        """
        This process will compare the archive source with the target table in lake_archive.

        Any records missing from lake_archive will be deposited in a delta table on lake_archive,
        named uniquely for the source database and lake archive table combination.

        This property provides that table name.

        """
        if 'staging_history' in self.database:
            suffix = 'delta__staging_history'
        elif 'staging' in self.database:
            suffix = 'delta__staging'
        elif 'um_archive' in self.database:
            suffix = 'delta__um_archive'
        else:
            print(self.database)
            raise Exception('invalid option')
        return f"lake_archive.{self.lake_config.target_schema}.{self.table}__{suffix}"

    @property
    def lake_archive_table_name(self):
        """
        Given the archive source table that defines this class instance, this property returns
        the corresponding lake_archive table.

        """
        tgt_database = 'lake_archive'
        if self.database == 'archive_dbp70_um_archive_20200127':
            tgt_schema = 'ultra_merchant'
            tgt_table = self.table.replace('_archive', '')
        elif self.database == 'archive_edw01_staging_20200127':
            if self.schema not in self.SCHEMA_MAP:
                print(f"skipping {self}")
                return
            tgt_schema = self.SCHEMA_MAP[self.schema]
            tgt_table = self.table
        elif self.database == 'archive_edw01_staging_history_20200127':
            if self.schema not in self.SCHEMA_MAP:
                print(f"skipping {self}")
                return
            tgt_schema = self.SCHEMA_MAP[self.schema]
            tgt_table = self.table.replace('_history', '')
        else:
            raise Exception('case not covered')
        return f'{tgt_database}.{tgt_schema}."{tgt_table.upper()}"'

    @property
    def lake_table_name(self):
        """
        Lake table name corresponding to this archive source table.

        """
        return (
            self.lake_archive_table_name.replace('lake_archive', 'lake')
            if self.lake_archive_table_name
            else None
        )

    @property
    def ddl_delta_table(self):
        """
        DDL to create the delta table.

        """
        cmd = f"""
            CREATE OR REPLACE TRANSIENT TABLE {self.delta_table_name}
            LIKE {self.lake_archive_table_name};
        """
        return unindent_auto(cmd)

    @property
    def dml_delta_insert(self):
        """
        DML to merge the delta from

        """
        return self._get_delta_insert_query(count_only=False)

    @property
    def dml_count(self):
        return self._get_delta_insert_query(count_only=True)

    def _get_delta_insert_query(self, count_only=True):
        uniqueness_list = [x.lower() for x in self.lake_column_list.uniqueness_cols_str]
        delta_col = self.lake_column_list.delta_column_list[0].name
        uniqueness_join = '\n            AND '.join(
            [f"s.{x} = t.{x}" for x in uniqueness_list + [delta_col]]
        )

        if count_only:
            select_list = "count(*)"
            insert_clause = ''
        else:
            select_list = ',\n        '.join(
                [x if x in self.source_columns else f'NULL as {x}' for x in self.lake_columns]
            )
            insert_list = ',\n        '.join([x for x in self.lake_columns])
            insert_clause = f"INSERT INTO {self.delta_table_name} ({insert_list})"
            if delta_col not in self.source_columns:
                raise MissingDeltaColException(
                    f"delta col is '{delta_col}' but does not exist in source"
                )

        cmd = f"""{insert_clause}
        SELECT
            {select_list}
        FROM {self.full_name} s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {self.lake_archive_table_name} t
            WHERE {uniqueness_join}
        )
        """
        return cmd

    @property
    def dml_lake_archive_append_query(self):
        insert_list = ', '.join([x for x in self.lake_columns])
        uniqueness_list = [x.lower() for x in self.lake_column_list.uniqueness_cols_str]
        delta_col = self.lake_column_list.delta_column_list[0].name
        uniqueness_list.append(delta_col)
        partition_clause = ', '.join([f"s.{x}" for x in uniqueness_list])
        insert_clause = f"INSERT INTO {self.lake_archive_table_name}"
        cmd = f"""{insert_clause}
        SELECT {insert_list}
        FROM (
            SELECT
                *, row_number() over ( PARTITION BY {partition_clause} ORDER BY {delta_col} DESC ) AS rn
            FROM {self.delta_table_name} s
        ) s
        WHERE s.rn = 1
        """
        return cmd

    def populate_delta_table(self, dry_run=False):
        """
        Load into the delta table the records from archive source that are not present in lake_archive
        table.

        Args:
            dry_run: if true, just print the command

        """
        if dry_run:
            print(self.ddl_delta_table)
            print(self.dml_delta_insert)
        else:
            print(f"running table '{self.delta_table_name}'")
            self.snowflake_hook.execute_multiple(
                ''.join(
                    (
                        self.ddl_delta_table,
                        self.dml_delta_insert,
                    )
                )
            )

    def append_to_lake_archive_table(self, dry_run=False):
        """
        Load into the delta table the records from archive source that are not present in lake_archive
        table.

        Args:
            dry_run: if true, just print the command

        """
        if dry_run:
            print(self.dml_lake_archive_append_query)
        else:
            print(f"running table '{self.delta_table_name}'")
            self.snowflake_hook.execute_multiple(self.dml_lake_archive_append_query)

    def get_counts(self):
        """
        Get basic stats for a given lake table family and return tuple.

        """
        query = f"""
        select
            '{self.full_name}' as table_name,
            (select count(*) from {self.full_name}) as source,
            (select count(*) from {self.lake_table_name}) as lake,
            (select count(*) from {self.lake_archive_table_name}) as lake_archive,
            (select count(*) from {self.delta_table_name}) as delta
        """
        rows = self.snowflake_hook.get_records(query)
        return rows[0]


logger = logging.getLogger()
logger.setLevel(logging.WARNING)

# run counts
bad_watermark = []
not_mapped = []
not_in_lake = []


stats_dict = {}


class ArchiveSourceMerge:
    """
    This helper class is used to orchestrate the merging into lake_archive of historical source archive
    databases.

    For example:

    - archive_dbp70_um_archive_20200127
    - archive_edm_db_lake_diyotta
    - archive_edm_db_landing_diyotta
    - archive_edw01_staging_20200127
    - archive_edw01_staging_history_20200127

    Most of the logic is controlled by the ArchveSourceTable class.  But some logic needs to be
    applied when looping through the tables on a database and merging them in, and that's what this
    class helps with.

    **Usage**

    Merging an archive source database into lake_archive is a two-step process.

    #. populate delta tables by running :meth:`~populate_all_delta_tables`.
    #. append the delta rows into lake archive with :meth:`append_to_all_lake_archive_tables`.

    As the populate method populates each delta table, it computes stats and stores in global variable
    :obj:`~.stats_dict`. After running populate, call :meth:`write_out_stats` write out the stats.
    Then you can load into excel and check sanity.

    Note:
        See docs for each method to understand caveats.  And use ``dry_run=True`` to examine what they
        do before running in production.

    Notes:
        -  source databases must be mapped in :meth:`ArchiveSourceTable.lake_archive_table_name`.  if
           adding a new source database, you must mapp it there, otherwise the tables will all be skipped.

        -  if the delta column is not present in the source database, the table will be skipped, because
           lake_archive is only useful for as-of queries, and for that you need a delta column.

        -  there is a dry-run method.  i recommend using it to verify what you will be doing

        -  you must only handle one source archive database at a time, processing it end-to-end.  By that
           I mean, for a given database populate all deltas then immediately append to lake_archive target.
           This is because when you do the next source database, you want all the deltas from the prior
           database to have been merged in when you run populate delta, so you don't duplicate the records.

    Examples:
        >>> table_name_list = [
        >>>     'archive_dbp70_um_archive_20200127.dbo.session_detail_archive',
        >>>     'archive_dbp70_um_archive_20200127.dbo.membership_promo_archive',
        >>> ]
        >>> asm = ArchiveSourceMerge(table_name_list=table_name_list)
        >>> asm.populate_all_delta_tables(dry_run=True)
        >>> asm.append_to_all_lake_archive_tables(dry_run=True)

    """

    STATS_HEADER_ROW = [
        'table_name',
        'source',
        'lake',
        'lake_archive',
        'delta',
    ]

    def __init__(self, table_name_list):
        self.table_name_list = table_name_list

    @classmethod
    def get_table_list(cls, database_name):
        """Returns all the names of all the tables in archive database."""
        query = f"""
        SELECT
            lower(l.table_catalog || '.' || l.table_schema || '.' || l.table_name)
        FROM {database_name.upper()}.information_schema.tables l
        where l.table_schema <> 'INFORMATION_SCHEMA'
        ORDER BY
            l.row_count
        """
        rows = SnowflakeHook(
            role=snowflake_roles.etl_service_account, warehouse='DA_WH_ETL_LIGHT'
        ).get_records(query)
        return [x[0] for x in rows]

    def populate_all_delta_tables(self, dry_run=False):
        """
        Takes each archive source table, create a delta table on lake_archive database,
        and populate it with records (defined by uniqueness + delta col).  It does not de-dupe --
        that is handled in :meth:`append_to_all_lake_archive_tables`.  But it does check for
        existence in target.

        Args:
            dry_run: just print out the commands but don't execute anything

        """
        self._run_all(step='populate_delta_table', dry_run=dry_run)

    def append_to_all_lake_archive_tables(self, dry_run=False):
        """
        After running :meth:`populate_all_delta_tables`, this method will append each delta table
        onto the target lake_archive table.

        When selecting from the delta tables, the generated query uses row_number to de-dupe with
        respect to uniqueness + delta col.  But it does not check for existence in the target.

        Important:
            You must run this only once because it does not do a merge -- it just blindly appends.

        Args:
            dry_run: just print out the commands but don't execute anything

        """
        self._run_all(step='append_to_lake_archive_table', dry_run=dry_run)

    def _run_all(self, step, dry_run=False):
        """
        Private method that will either populate the delta tables or append delta tables to the
        target lake_archive table.

        Args:
            step: either ``populate_delta_table`` or ``append_to_lake_archive_table``
            dry_run: just print out the commands but don't execute anything

        """

        if step not in ('populate_delta_table', 'append_to_lake_archive_table'):
            raise ValueError('invalid step provided')

        for table_name in self.table_name_list:
            archive_table = ArchiveSourceTable(table_name)

            if not archive_table.lake_table_name:
                # if no lake table, then the mapping between this source archive database and lake
                # is not defined in ArchiveSourceTable.  See the definition of the
                # ``lake_archive_table_name`` property
                warnings.warn(f"table {archive_table} not mapped in lake_archive_table_name")
                not_mapped.append(archive_table)
                continue

            try:
                if archive_table.watermark_column != 'datetime_modified':
                    warnings.warn(
                        f"table {archive_table.lake_table_name} watermark is {archive_table.watermark_column}"
                    )
                    bad_watermark.append(archive_table)
                    continue
            except ModuleNotFoundError:
                warnings.warn(f"config not found for table {archive_table.lake_table_name}")
                not_in_lake.append(archive_table)
                continue
            try:
                print(table_name)
                if step == 'populate_delta_table':
                    archive_table.populate_delta_table(dry_run=dry_run)
                    stats_dict[archive_table.full_name] = archive_table.get_counts()
                elif step == 'append_to_lake_archive_table':
                    try:
                        archive_table.append_to_lake_archive_table(dry_run=dry_run)
                    except ProgrammingError:
                        warnings.warn(f"delta table no exist: {archive_table.delta_table_name}")

            except MissingDeltaColException:
                warnings.warn(
                    f"{archive_table.delta_table_name} failed; delta column not in source"
                )

    @classmethod
    def write_out_stats(cls, filename):
        """
        After running :meth:`populate_all_delta_tables`, call this method to write out statistics
        computed on delta and lake_archive for sanity checks.

        Args:
            filename: where you want the file to be written

        """
        with open(filename, 'wt') as f:
            f.write('\t'.join([str(x) for x in cls.STATS_HEADER_ROW]))
            f.write('\n')
            for table_name, data in stats_dict.items():
                f.write('\t'.join([str(x) for x in data]))
                f.write('\n')
