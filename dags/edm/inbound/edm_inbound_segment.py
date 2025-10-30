import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.config import email_lists, owners, stages
from include.airflow.operators.segment import SegmentLoadRawData, SegmentRawToTargetTables

from task_configs.dag_config.segment.segment_config import segment_configs

default_args = {
    "start_date": pendulum.datetime(2024, 8, 9, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    'on_failure_callback': slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_segment",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
)

# Good overview of job
# 1. loads json into table, flattening out first child values into columns
# 2. collects distinct list of events from table
# 3. for each event, uses FlattenAndExplode udtf to flatten all data (nothing is filtered out, no use of schema object) and explodes lists defined in EXPLODE_COLUMN_LIST, then loads into staging table
# 4. use schema object to select desired columns from staging table to create target table df
# 5. confirm target table exists and create if not
# 6. check if new columns need to be added to target table based on columns in target table df and evolve schema if yes
# 7. merge taget table df into target table using messageId, and PROPERTIES_PRODUCTS_PRODUCT_ID if present

with dag:
    for cfg in segment_configs:
        load_raw = SegmentLoadRawData(
            task_id=f'segment_load_raw_{cfg.grouping}',
            stage_path=f'{stages.tsos_da_int_segment}/lake_202304',
            grouping=cfg.grouping,
            table_prefix=cfg.get_table_prefix(),
            s3_folders=cfg.s3_folders,
            raw_database=cfg.raw_database,
            raw_schema=cfg.raw_schema,
            raw_suffix=cfg.raw_suffix,
            connection_dict={
                'warehouse': 'DA_WH_ETL_HEAVY',
            },
        )
        raw_to_target = SegmentRawToTargetTables(
            task_id=f'segment_raw_to_targets_{cfg.grouping}',
            grouping=cfg.grouping,
            table_prefix=cfg.get_table_prefix(),
            grouping_struct=cfg.struct_definition,
            raw_database=cfg.raw_database,
            raw_schema=cfg.raw_schema,
            stage_database=cfg.stage_database,
            stage_schema=cfg.stage_schema,
            target_database=cfg.target_database,
            target_schema=cfg.target_schema,
            raw_suffix=cfg.raw_suffix,
            stage_suffix=cfg.stage_suffix,
            flatten_udtf_name='util.public.flatten_and_explode',
            segment_shard_udf_name='util.public.segment_shard_from_filename',
            country_udf_name='util.public.country_from_shard',
            explicit_event_list=cfg.explicit_event_list,
            explode_column_list=['PROPERTIES_PRODUCTS'],
            drop_stage=True,
            delete_raw=True,
            connection_dict={
                'warehouse': 'DA_WH_ETL_HEAVY',
            },
        )
        load_raw >> raw_to_target
