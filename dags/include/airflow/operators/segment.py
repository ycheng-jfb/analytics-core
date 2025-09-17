import logging
import re
import time
import traceback

from include.airflow.hooks.snowflake import SnowflakeHook
from include.utils.snowpark import (
    create_table_with_schema,
    unquote_columns,
    wait_for_async_jobs,
    get_query_status,
    is_query_error,
    table_exists,
    struct_select,
    evolve_schema_if_necessary,
    copy_json_into_existing_table,
)

# from config.logging import configure_logging
from airflow.models import BaseOperator

from typing import Optional, List
from snowflake.snowpark import Session, DataFrame, AsyncJob, Window
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import (
    StructType,
    StructField,
    TimestampType,
    VariantType,
    StringType,
)


LOGGER = logging.getLogger("ingest_segment_json")


class SegmentLoadRawData(BaseOperator):
    def __init__(
        self,
        stage_path: str,
        grouping: str,
        table_prefix: str,
        s3_folders: list,
        raw_database: str,
        raw_schema: str,
        raw_suffix: str = "events",
        connection_dict: dict = {},
        **kwargs,
    ):
        self.stage_path = stage_path
        self.grouping = grouping
        self.table_prefix = table_prefix
        self.s3_folders = s3_folders
        self.raw_database = raw_database
        self.raw_schema = raw_schema
        self.raw_table = f"{self.table_prefix}_{raw_suffix}"
        self.connection_dict = connection_dict
        super().__init__(**kwargs)

    @property
    def snowpark_session(self):
        session = Session.builder.configs(
            SnowflakeHook(**self.connection_dict)._get_conn_params()
        ).create()
        return session

    def execute(self, context=None):
        session = self.snowpark_session
        LOGGER.info(f"Ingest segment JSON with session: {session}")

        # Prevent schema inference from creating tables with case-sensitive column names
        # Having case-sensitive column names can decrease the usability of raw tables
        session.sql("alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE").collect()

        LOGGER.info(f"Ingesting data for grouping: {self.grouping}")

        qualified_table_name = f"{self.raw_database}.{self.raw_schema}.{self.raw_table}"
        # Check if table creation is required
        if not table_exists(
            session, self.raw_database, self.raw_schema, self.raw_table
        ):
            # If raw table doesn't exist, create with table with only metadata columns
            # NOTE: this pattern may lead to and error for too many columns in schema evolution
            # If this happens, please talk with your Snowflake account team
            metadata_schema = StructType(
                [
                    StructField("META_CREATE_DATETIME", TimestampType()),
                    StructField("FILENAME", StringType()),
                ]
            )
            create_table_with_schema(session, qualified_table_name, metadata_schema)
            # Then enable schema evolution
            _ = session.sql(
                f"ALTER TABLE {qualified_table_name} SET ENABLE_SCHEMA_EVOLUTION = TRUE"
            ).collect()

        # Prepare and submit async copy job
        # TODO add additional metadata
        include_metadata = {
            "META_CREATE_DATETIME": "METADATA$START_SCAN_TIME",
            "FILENAME": "METADATA$FILENAME",
        }

        jobs = []
        for s3_folder in self.s3_folders:
            src_file_path = f"{self.stage_path}/{s3_folder}"
            async_job = copy_json_into_existing_table(
                session,
                qualified_table_name,
                src_file_path=src_file_path,
                include_metadata=include_metadata,
                block=False,
            )
            jobs.append((self.grouping, async_job))

        # Monitor async copy jobs
        failed_job_ids = wait_for_async_jobs(session, jobs)
        # Todo: clean this up
        if len(failed_job_ids) > 0:
            raise Exception("Ingest jobs failed")
        else:
            LOGGER.info(f"Ingest job was successful")


class SegmentRawToTargetTables(BaseOperator):
    def __init__(
        self,
        grouping: str,
        table_prefix: str,
        grouping_struct: StructType,
        raw_database: str,
        stage_database: str,
        target_database: str,
        raw_schema: str,
        stage_schema: str,
        target_schema: str,
        flatten_udtf_name: str,
        segment_shard_udf_name: str,
        country_udf_name: str,
        raw_suffix: str = "events",
        stage_suffix: str = "stage",
        connection_dict: dict = {},
        stage_status_check_wait: int = 5,
        explicit_event_list: list = None,
        explode_column_list: list = None,
        delete_raw: bool = False,
        drop_stage: bool = False,
        **kwargs,
    ):
        self.grouping = grouping
        self.table_prefix = table_prefix
        self.grouping_struct = grouping_struct
        self.raw_database = raw_database
        self.stage_database = stage_database
        self.target_database = target_database
        self.raw_schema = raw_schema
        self.raw_qualified_table = (
            f"{self.raw_database}.{self.raw_schema}.{self.table_prefix}_{raw_suffix}"
        )
        self.stage_schema = stage_schema
        self.stage_suffix = stage_suffix
        self.target_schema = target_schema
        self.flatten_udtf_name = flatten_udtf_name
        self.segment_shard_udf_name = segment_shard_udf_name
        self.country_udf_name = country_udf_name
        self.connection_dict = connection_dict
        self.stage_status_check_wait = stage_status_check_wait
        self.explicit_event_list = explicit_event_list
        self.explode_column_list = explode_column_list
        self.delete_raw = delete_raw
        self.drop_stage = drop_stage
        super().__init__(**kwargs)

    @property
    def snowpark_session(self):
        session = Session.builder.configs(
            SnowflakeHook(**self.connection_dict)._get_conn_params()
        ).create()
        return session

    def get_stage_qualified_table(self, event_type):
        return f"{self.stage_database}.{self.stage_schema}.{self.table_prefix}_{FlattenAndExplode.normalize_field_name(event_type)}_{self.stage_suffix}".lower()

    def get_target_qualified_table(self, event_type):
        return f"{self.target_database}.{self.target_schema}.{self.table_prefix}_{FlattenAndExplode.normalize_field_name(event_type)}".lower()

    def manage_merge_into_target_jobs(self, session, event_type_staging_jobs, grouping):
        running_jobs = {
            event_type: async_job for event_type, async_job in event_type_staging_jobs
        }
        failed_job_ids = []
        final_merge_jobs = []
        while running_jobs:
            for event_type, event_staging_job in list(running_jobs.items()):
                if event_staging_job.is_done():
                    LOGGER.info(
                        f"Staging job {event_type} is complete, query_id: {event_staging_job.query_id}"
                    )
                    del running_jobs[event_type]
                    status = get_query_status(session, event_staging_job.query_id)
                    if is_query_error(status):
                        LOGGER.error(
                            f"Staging job {event_type} failed with status: {status}, query_id: {event_staging_job.query_id}"
                        )
                        failed_job_ids.append(event_type)
                    else:
                        LOGGER.info(
                            f"Staging job {event_type} succeeded, query_id: {event_staging_job.query_id}"
                        )
                        insert_job = self.perform_async_merge_into_target(
                            session, event_type, grouping
                        )
                        final_merge_jobs.append((event_type, insert_job))
                        LOGGER.info(
                            f"Started insert job into lake table for {event_type}, query_id: {insert_job.query_id}"
                        )
                        # TODO when job is successful, also need to delete data from original staging table
                else:
                    LOGGER.info(
                        f"Staging job for {event_type} is still running, query_id: {event_staging_job.query_id}"
                    )
            time.sleep(self.stage_status_check_wait)

        LOGGER.info("All event staging jobs finished")

        return final_merge_jobs, failed_job_ids

    def perform_async_merge_into_target(
        self, session, event_type: str, grouping: str
    ) -> AsyncJob:
        LOGGER.info(
            f"Performing async insert into lake table for event_type: {event_type}, grouping: {grouping}"
        )
        source_table = self.get_stage_qualified_table(event_type)
        target_table = self.get_target_qualified_table(event_type)
        LOGGER.info(f"Using source_table: {source_table}, target_table: {target_table}")
        new_events_df = session.table(source_table)
        new_events_df = struct_select(
            new_events_df,
            self.grouping_struct,
            "_RESCUED_DATA",
            explode_column_list=self.explode_column_list,
            normalize_name_func=FlattenAndExplode.normalize_field_name,
        )

        if table_exists(
            session,
            self.target_database,
            self.target_schema,
            f'{self.table_prefix}_{event_type.replace(" ", "_")}',
        ):
            # Table already exists, so perform a merge
            LOGGER.info(f"Performing merge into target_table: {target_table}")
            evolve_schema_if_necessary(session, target_table, new_events_df)
            target_table_df = session.table(target_table)
            # TODO figure out proper logic for merge condition
            if "PROPERTIES_PRODUCTS_PRODUCT_ID" in new_events_df.schema.names:
                merge_conditions = (
                    new_events_df["MESSAGEID"] == target_table_df["MESSAGEID"]
                ) & (
                    new_events_df["PROPERTIES_PRODUCTS_PRODUCT_ID"]
                    == target_table_df["PROPERTIES_PRODUCTS_PRODUCT_ID"]
                )
            else:
                merge_conditions = (
                    new_events_df["MESSAGEID"] == target_table_df["MESSAGEID"]
                )

            cols_to_update = {
                col: new_events_df[col] for col in new_events_df.schema.names
            }
            metadata_col_to_update = {}  # "META_UPDATED_AT": F.current_timestamp()}
            updates = {**cols_to_update, **metadata_col_to_update}

            merge_logic = [
                F.when_matched().update(updates),
                F.when_not_matched().insert(updates),
            ]
            # noinspection PyTypeChecker
            return target_table_df.merge(
                new_events_df, merge_conditions, merge_logic, block=False
            )
        else:
            # Silver table is new, so create it
            LOGGER.info(f"Writing to new target_table: {target_table}")
            # noinspection PyTypeChecker
            return new_events_df.write.mode("overwrite").save_as_table(
                target_table, block=False
            )

    def execute(self, context=None):
        session = self.snowpark_session
        LOGGER.info(f"Load segment tables with session: {session}")

        LOGGER.info(
            f"Getting new event data from: {self.raw_qualified_table}, for grouping: {self.grouping}"
        )

        if self.explicit_event_list:
            events = [
                (x, "non_track") if x in ["page", "identify"] else (x, "track")
                for x in self.explicit_event_list
            ]
            LOGGER.info(f"Processing only explicitly provided events: {events}")
        else:
            track_types_df = (
                session.table(self.raw_qualified_table)
                .select("event")
                .filter("type = 'track'")
                .distinct()
            )
            track_types = [
                (row.as_dict()["EVENT"], "track") for row in track_types_df.collect()
            ]
            LOGGER.info(f"Obtained distinct track_types: {track_types}")
            non_track_types_df = (
                session.table(self.raw_qualified_table)
                .select("type")
                .filter("type != 'track'")
                .distinct()
            )
            non_track_types = [
                (row.as_dict()["TYPE"], "non_track")
                for row in non_track_types_df.collect()
            ]
            LOGGER.info(f"Obtained distinct non_track_types: {non_track_types}")
            events = track_types + non_track_types

        event_type_staging_jobs = []
        error_job_ids = []
        for event in events:
            try:
                event_type = event[0]
                # This first loop performs the fanout into many staging tables
                LOGGER.info(
                    f"Processing event type: {event_type}, for grouping: {self.grouping}"
                )
                # Select all new data for event type
                if event[1] == "track":
                    staged_event_df = session.table(self.raw_qualified_table).filter(
                        F.col("event") == event_type
                    )
                elif event[1] == "non_track":
                    staged_event_df = session.table(self.raw_qualified_table).filter(
                        F.col("type") == event_type
                    )

                # Perform dynamic flatten and explode using UDTF approach
                flatten_and_explode_udtf = F.table_function(self.flatten_udtf_name)
                staged_event_df = staged_event_df.with_column(
                    "CONSTRUCTED_OBJECT", F.object_construct("*")
                )
                staged_event_df = staged_event_df.select(
                    flatten_and_explode_udtf(F.col("CONSTRUCTED_OBJECT"))
                )

                # Dynamic pivot variant output into columns
                staged_event_df = staged_event_df.join_table_function(
                    "flatten", staged_event_df["OUTPUT"]
                ).drop(["SEQ", "PATH", "INDEX", "THIS"])
                staged_event_df = staged_event_df.pivot("key").min("value")
                staged_event_df = unquote_columns(staged_event_df)
                staged_event_df = staged_event_df.drop("OUTPUT")

                # Partition over messageId and filter out all records except most recent
                if "PROPERTIES_PRODUCTS_PRODUCT_ID" in staged_event_df.schema.names:
                    staged_event_df = (
                        staged_event_df.withColumn(
                            "rn",
                            F.row_number().over(
                                Window.partitionBy(
                                    ["MESSAGEID", "PROPERTIES_PRODUCTS_PRODUCT_ID"]
                                ).orderBy(
                                    *[
                                        F.desc(c)
                                        for c in ["RECEIVEDAT", "ORIGINALTIMESTAMP"]
                                    ]
                                )
                            ),
                        )
                        .filter(F.col("rn") == 1)
                        .drop("rn")
                    )
                else:
                    staged_event_df = (
                        staged_event_df.withColumn(
                            "rn",
                            F.row_number().over(
                                Window.partitionBy("MESSAGEID").orderBy(
                                    *[
                                        F.desc(c)
                                        for c in ["RECEIVEDAT", "ORIGINALTIMESTAMP"]
                                    ]
                                )
                            ),
                        )
                        .filter(F.col("rn") == 1)
                        .drop("rn")
                    )

                staged_event_df = staged_event_df.with_column(
                    "segment_shard",
                    F.call_function(self.segment_shard_udf_name, (F.col("FILENAME"))),
                )
                staged_event_df = staged_event_df.with_column(
                    "country",
                    F.call_function(self.country_udf_name, (F.col("segment_shard"))),
                )

                # Load flattened data into event-specific staging table
                event_staging_table = self.get_stage_qualified_table(event_type)
                LOGGER.info(
                    f"Writing data to semi-structured event staging table: {event_staging_table}"
                )
                event_staging_job = staged_event_df.write.mode(
                    "overwrite"
                ).save_as_table(event_staging_table, block=False)
                event_type_staging_jobs.append((event_type, event_staging_job))
            except Exception as e:
                error_job_ids.append(event[0])
                LOGGER.error(f"Error appeared when processing event: {event[0]}")
                traceback.print_exception(type(e), e, e.__traceback__)

        if event_type_staging_jobs:
            # Monitor staging jobs, starting inserts into target tables as they complete
            final_merge_jobs, failed_stage_job_ids = self.manage_merge_into_target_jobs(
                session, event_type_staging_jobs, self.grouping
            )

            # Wait for inserts into lake table to finish
            failed_target_job_ids = wait_for_async_jobs(session, final_merge_jobs)
        else:
            failed_stage_job_ids = []
            failed_target_job_ids = []

        # Cleanup stage tables for events that loaded successfully
        error_events = failed_stage_job_ids + failed_target_job_ids + error_job_ids
        for event in events:
            if event[0] in error_events:
                continue
            if self.drop_stage:
                stage_schema_and_table = self.get_stage_qualified_table(event[0])
                session.table(stage_schema_and_table).drop_table()
                LOGGER.info(f"Dropped stage table: {stage_schema_and_table}")
            if self.delete_raw:
                raw_table = session.table(self.raw_qualified_table)
                if event[1] == "track":
                    raw_table.delete(raw_table["event"] == event[0])
                elif event[1] == "non_track":
                    raw_table.delete(raw_table["type"] == event[0])
                LOGGER.info(
                    f"Delete '{event[0]}' from raw table: {self.raw_qualified_table}"
                )

        # Check for errors
        if failed_stage_job_ids or failed_target_job_ids or error_job_ids:
            # Todo: clean this up
            if failed_stage_job_ids:
                LOGGER.error(f"List of failed staging jobs: {failed_stage_job_ids}")
            if failed_target_job_ids:
                LOGGER.error(
                    f"List of failed merge-into-target jobs: {failed_target_job_ids}"
                )
            if error_job_ids:
                LOGGER.error(
                    f"Error appeared when processing these event/s: {error_job_ids}"
                )
            raise Exception(
                "One or more staging and/or merge-into-target jobs failed, or there was"
                " an error. Find list of events that had an issue above. Use event to "
                "find more detailed ERROR message in log."
            )


class FlattenAndExplode:
    def process(self, input_data: dict):
        rows = self.flatten_json_to_rows(input_data)
        for row in rows:
            yield (row,)

    def should_explode_list(self, key, explode_column_list: Optional[List[str]]):
        if explode_column_list is None:
            return False
        lookup_key = key.rstrip("_").upper()
        return any(lookup_key == col.upper() for col in explode_column_list)

    def flatten_json_to_rows(self, json_obj):
        explode_column_list = ["PROPERTIES_PRODUCTS"]

        def flatten(current, rows, key, value):
            clean_key = self.normalize_field_name(key)

            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    clean_sub_key = self.normalize_field_name(sub_key)
                    flatten(
                        current,
                        rows,
                        f"{clean_key}_{clean_sub_key}_" if key else clean_sub_key,
                        sub_value,
                    )
                if rows:
                    for row in rows:
                        row.update(current)
                else:
                    rows.append(current)

            elif isinstance(value, list) and self.should_explode_list(
                clean_key, explode_column_list
            ):
                for i, item in enumerate(value):
                    new_rows = []
                    new_current = {}
                    flatten(new_current, new_rows, f"{clean_key}", item)
                    rows = rows + new_rows

            else:
                current[clean_key.rstrip("_").upper()] = value

        rows = []
        current = {}
        flatten(current, rows, "", json_obj)

        return rows

    @staticmethod
    def normalize_field_name(input_key: str, delimiter="_"):
        new_key = input_key
        # Replace multiple whitespace with a single whitespace
        new_key = re.sub(r"\s+", delimiter, new_key)
        # Remove leading and trailing whitespace
        new_key = new_key.strip()
        # Convert any remaining whitespace to underscore
        new_key = new_key.replace(" ", delimiter)
        # Escape delimiter to allow for special regex characters to be used
        escaped_delimiter = "\\" + delimiter
        # Remove all non-alphanumeric characters except delimiter
        new_key = re.sub(rf"[^a-zA-Z0-9{escaped_delimiter}]", delimiter, new_key)
        # Replace multiple underscores with a single underscore
        new_key = re.sub(r"_+", delimiter, new_key)
        # Remove leading and trailing underscores (if any)
        new_key = new_key.strip(delimiter)
        # Convert to uppercase
        new_key = new_key.upper()
        return new_key
