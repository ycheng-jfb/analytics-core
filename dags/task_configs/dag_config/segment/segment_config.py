import copy
from dataclasses import dataclass

from snowflake.snowpark.types import StructType, StringType, TimestampType

from task_configs.dag_config.segment.java_fk import java_fk_schema
from task_configs.dag_config.segment.java_fl import java_fl_schema
from task_configs.dag_config.segment.java_fl_ecom_mobile_app import java_fl_ecom_mobile_app_schema
from task_configs.dag_config.segment.java_jf import java_jf_schema
from task_configs.dag_config.segment.java_jf_ecom_mobile_app import java_jf_ecom_mobile_app_schema
from task_configs.dag_config.segment.java_sd import java_sd_schema
from task_configs.dag_config.segment.java_sx import java_sx_schema
from task_configs.dag_config.segment.javascript_fk import javascript_fk_schema
from task_configs.dag_config.segment.javascript_fl import javascript_fl_schema
from task_configs.dag_config.segment.javascript_jf import javascript_jf_schema
from task_configs.dag_config.segment.javascript_sd import javascript_sd_schema
from task_configs.dag_config.segment.javascript_sx import javascript_sx_schema
from task_configs.dag_config.segment.react_native_fl import react_native_fl_schema
from task_configs.dag_config.segment.react_native_fl_associate_app import (
    react_native_fl_associate_app_schema,
)
from task_configs.dag_config.segment.react_native_jf import react_native_jf_schema


@dataclass
class SegmentConfig:
    brand: str
    grouping: str
    struct_definition: StructType
    s3_folders: list
    raw_suffix: str = 'events'
    stage_suffix: str = 'stg'
    _default_fields = [
        ('META_CREATE_DATETIME', TimestampType()),
        ('segment_shard', StringType()),
        ('country', StringType()),
    ]
    explicit_event_list: list = None

    def __post_init__(self):
        self.struct_definition = copy.deepcopy(self.struct_definition)
        for x in self._default_fields:
            self.struct_definition.add(x[0], x[1])

        if self.brand == 'fl':
            self.raw_database = 'lake_stg'
            self.raw_schema = 'segment_fl'
            self.stage_database = 'lake_stg'
            self.stage_schema = 'segment_fl'
            self.target_database = 'lake_fl'
            self.target_schema = 'segment'
        elif self.brand == 'jf':
            self.raw_database = 'lake_stg'
            self.raw_schema = 'segment_jfb'
            self.stage_database = 'lake_stg'
            self.stage_schema = 'segment_jfb'
            self.target_database = 'lake_jfb'
            self.target_schema = 'segment'
        elif self.brand == 'sx':
            self.raw_database = 'lake_stg'
            self.raw_schema = 'segment_sxf'
            self.stage_database = 'lake_stg'
            self.stage_schema = 'segment_sxf'
            self.target_database = 'lake_sxf'
            self.target_schema = 'segment'

    def get_table_prefix(self):
        if '_fl_' in self.grouping:
            return self.grouping.replace('_fl_', '_')
        elif '_sx_' in self.grouping:
            return self.grouping.replace('_sx_', '_')
        elif self.grouping[-3:] == '_fl' or self.grouping[-3:] == '_sx':
            return self.grouping[:-3]
        else:
            return self.grouping


segment_configs = [
    SegmentConfig(
        brand='jf',
        grouping='java_jf_ecom_app',
        struct_definition=java_jf_ecom_mobile_app_schema,
        s3_folders=[
            'java_jf_ecom_app_prod',
        ],
    ),
    SegmentConfig(
        brand='fl',
        grouping='java_fl_ecom_mobile_app',
        struct_definition=java_fl_ecom_mobile_app_schema,
        s3_folders=[
            'java_fabletics_ecom_mobile_app_prod',
        ],
    ),
    SegmentConfig(
        brand='jf',
        grouping='java_fk',
        struct_definition=java_fk_schema,
        s3_folders=[
            'java_fabkids_production',
        ],
    ),
    SegmentConfig(
        brand='fl',
        grouping='java_fl',
        struct_definition=java_fl_schema,
        s3_folders=[
            'java_fabletics_ca_production',
            'java_fabletics_de_production',
            'java_fabletics_dk_production',
            'java_fabletics_es_production',
            'java_fabletics_fr_production',
            'java_fabletics_nl_production',
            'java_fabletics_se_production',
            'java_fabletics_uk_production',
            'java_fabletics_production',
        ],
    ),
    SegmentConfig(
        brand='fl',
        grouping='java_fl_retail',
        struct_definition=java_fl_schema,
        s3_folders=[
            'java_fabletics_retail_us_production_retail',
        ],
    ),
    SegmentConfig(
        brand='jf',
        grouping='java_jf',
        struct_definition=java_jf_schema,
        s3_folders=[
            'java_justfab_ca_production',
            'java_justfab_de_production',
            'java_justfab_dk_production',
            'java_justfab_es_production',
            'java_justfab_fr_production',
            'java_justfab_nl_production',
            'java_justfab_uk_production',
            'java_justfab_us_production',
        ],
    ),
    SegmentConfig(
        brand='jf',
        grouping='java_sd',
        struct_definition=java_sd_schema,
        s3_folders=[
            'java_shoedazzle_production',
        ],
    ),
    SegmentConfig(
        brand='sx',
        grouping='java_sx',
        struct_definition=java_sx_schema,
        s3_folders=[
            'java_sxf_de_production',
            'java_sxf_es_production',
            'java_sxf_eu_production',
            'java_sxf_fr_production',
            'java_sxf_uk_production',
            'java_sxf_us_production',
        ],
    ),
    SegmentConfig(
        brand='jf',
        grouping='javascript_fk',
        struct_definition=javascript_fk_schema,
        s3_folders=[
            'javascript_fabkids_production',
        ],
    ),
    SegmentConfig(
        brand='fl',
        grouping='javascript_fl',
        struct_definition=javascript_fl_schema,
        s3_folders=[
            'javascript_fabletics_ca_production',
            'javascript_fabletics_de_production',
            'javascript_fabletics_dk_production',
            'javascript_fabletics_es_production',
            'javascript_fabletics_fr_production',
            'javascript_fabletics_nl_production',
            'javascript_fabletics_se_production',
            'javascript_fabletics_uk_production',
            'javascript_fabletics_us_production',
        ],
    ),
    SegmentConfig(
        brand='jf',
        grouping='javascript_jf',
        struct_definition=javascript_jf_schema,
        s3_folders=[
            'javascript_justfab_ca_production',
            'javascript_justfab_de_production',
            'javascript_justfab_dk_production',
            'javascript_justfab_es_production',
            'javascript_justfab_fr_production',
            'javascript_justfab_nl_production',
            'javascript_justfab_production',
            'javascript_justfab_se_production',
            'javascript_justfab_uk_production',
        ],
    ),
    SegmentConfig(
        brand='jf',
        grouping='javascript_sd',
        struct_definition=javascript_sd_schema,
        s3_folders=[
            'javascript_shoedazzle_production',
        ],
    ),
    SegmentConfig(
        brand='sx',
        grouping='javascript_sx',
        struct_definition=javascript_sx_schema,
        s3_folders=[
            'javascript_sxf_de_production',
            'javascript_sxf_es_production',
            'javascript_sxf_eu_production',
            'javascript_sxf_fr_production',
            'javascript_sxf_uk_production',
            'javascript_sxf_us_production',
        ],
    ),
    SegmentConfig(
        brand='fl',
        grouping='react_native_fl',
        struct_definition=react_native_fl_schema,
        s3_folders=[
            'react_native_fabletics_production',
        ],
    ),
    SegmentConfig(
        brand='jf',
        grouping='react_native_jf',
        struct_definition=react_native_jf_schema,
        s3_folders=[
            'react_native_justfab_production',
        ],
    ),
    SegmentConfig(
        brand='fl',
        grouping='react_native_fl_associate_app',
        struct_definition=react_native_fl_associate_app_schema,
        s3_folders=[
            'react_native_fabletics_associate_app_prod',
        ],
    ),
    SegmentConfig(
        brand='fl',
        grouping='react_native_fl_associate_app_beta',
        struct_definition=react_native_fl_associate_app_schema,
        s3_folders=[
            'react_native_fabletics_associate_app_beta_prod',
        ],
    ),
]
