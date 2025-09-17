from dataclasses import dataclass, field
from typing import List, Optional, Union

from include.utils.snowflake import Column, CopyConfigCsv


class GDPRLakeTargetConfig:
    TARGET_DATABASE = 'lake_{brand}'
    TARGET_SCHEMA = 'gdpr'
    TARGET_TABLE = 'request_job'
    FULL_TABLE_NAME = f'{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}'
    COLUMN_LIST = [
        Column('customer_id', 'INT', uniqueness=True),
        Column('request_job_id', 'INT', uniqueness=True),
        Column('request_id', 'INT'),
        Column('system_id', 'INT'),
        Column('datetime_added', 'TIMESTAMP_NTZ(3)'),
        Column('statuscode', 'INT'),
        Column('snowflake_updated', 'INT'),
        Column('pushed_to_mssql', 'INT'),
    ]
    COPY_CONFIG = CopyConfigCsv(field_delimiter='\t')


@dataclass
class MaskConfig:
    """dataclass to maintain the mask value for a column"""

    col: str
    mask_expr: str
    quote: Optional[bool] = field(default=True)

    def __str__(self) -> str:
        if self.quote:
            return f"{self.col} = '{self.mask_expr}'"
        return f"{self.col} = {self.mask_expr}"


@dataclass
class PIIConfig:
    """dataclass to maintain mask configs for a table"""

    table: str
    mask_config: Union[MaskConfig, List[MaskConfig]]
    key_on: Optional[str] = field(default='customer_id')
    ref_tbl: Optional[str] = field(default=None)
    ref_col: Optional[str] = field(default=None)
    use_meta_original_customer_id: bool = True

    def __post_init__(self):
        if not isinstance(self.mask_config, list):
            self.mask_config = [self.mask_config]
        assert not (
            bool(self.ref_tbl) ^ bool(self.ref_col)
        ), 'ref_tbl and ref_col have to be both defined (or) both undefined'


pii_config: List[PIIConfig] = [
    PIIConfig(
        table='lake_consolidated.ultra_merchant_history.payment_transaction_psp',
        use_meta_original_customer_id=False,
        mask_config=MaskConfig(col='ip', mask_expr='0.0.0.0'),
        ref_tbl='lake_consolidated.ultra_merchant.psp',
        ref_col='psp_id',
        key_on='psp_id',
    ),
    PIIConfig(
        table='"LAKE_CONSOLIDATED"."ULTRA_MERCHANT_HISTORY"."ORDER"',
        use_meta_original_customer_id=False,
        mask_config=MaskConfig(col='ip', mask_expr='0.0.0.0'),
    ),
    PIIConfig(
        table='lake_consolidated.ultra_merchant_history.payment_transaction_creditcard',
        use_meta_original_customer_id=False,
        mask_config=MaskConfig(col='ip', mask_expr='0.0.0.0'),
        key_on='creditcard_id',
        ref_tbl='lake_consolidated.ultra_merchant.creditcard',
        ref_col='creditcard_id',
    ),
    PIIConfig(
        table='lake_consolidated.ultra_merchant_history.customer',
        use_meta_original_customer_id=False,
        mask_config=[
            MaskConfig(col='firstname', mask_expr='[removed]'),
            MaskConfig(col='lastname', mask_expr='[removed]'),
            MaskConfig(col='name', mask_expr='[removed]'),
            MaskConfig(col='company', mask_expr='[removed]'),
            MaskConfig(col='username', mask_expr='[removed]'),
            MaskConfig(col='email', mask_expr='[removed]'),
        ],
    ),
]
