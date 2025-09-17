from include.utils.acquisition.table_config import TableConfig
from include.utils.snowflake import Column


class CustomTableConfig(TableConfig):
    def __init__(
        self,
        target_table_name: str = None,
        remove_nones: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_table_name = target_table_name or self.table
        self.remove_nones = remove_nones


table_list = ["credit_recon", "token_recon"]

credit_recon_column_list = CustomTableConfig(
    database='reporting_base',
    table='credit_recon',
    schema='shared',
    column_list=[
        Column("record_type", "varchar"),
        Column("operating_unit", "varchar"),
        Column("org_id", "varchar"),
        Column("store_credit_id", "varchar"),
        Column("order_id", "varchar"),
        Column("refund_id", "varchar"),
        Column("trx_date", "varchar"),
        Column("trx_number", "varchar"),
        Column("amount_original", "varchar"),
        Column("amount_remaining", "varchar"),
        Column("fixed_vs_variable", "varchar"),
        Column("credit_reason", "varchar"),
        Column("party_name", "varchar"),
        Column("store_id", "varchar"),
        Column("order_amount", "varchar"),
        Column("vat_amount", "varchar"),
        Column("vat_rate", "varchar"),
        Column("currency_code", "varchar"),
        Column("calc_amount_remaining", "varchar"),
        Column("vat_prepaid_flag", "varchar"),
        Column("receivable_account", "varchar"),
        Column("customer_trx_id", "varchar"),
        Column("exchange_rate", "varchar"),
        Column("trx_type", "varchar"),
        Column("record_count", "varchar"),
        Column("min_trx_date", "varchar"),
        Column("max_trx_date", "varchar"),
        Column("sum_amount_original", "varchar"),
        Column("sum_amount_remaining", "varchar"),
        Column("sum_order_amount", "varchar"),
        Column("sum_vat_amount", "varchar"),
        Column("extract_datetime", "varchar"),
    ],
)

token_recon_column_list = CustomTableConfig(
    database='reporting_base',
    table='token_recon',
    schema='shared',
    column_list=[
        Column("record_type", "varchar"),
        Column("operating_unit", "varchar"),
        Column("org_id", "varchar"),
        Column("store_credit_id", "varchar"),
        Column("order_id", "varchar"),
        Column("refund_id", "varchar"),
        Column("token_id", "varchar"),
        Column("trx_date", "varchar"),
        Column("trx_number", "varchar"),
        Column("amount_original", "varchar"),
        Column("amount_remaining", "varchar"),
        Column("party_name", "varchar"),
        Column("store_id", "varchar"),
        Column("order_amount", "varchar"),
        Column("vat_amount", "varchar"),
        Column("vat_rate", "varchar"),
        Column("currency_code", "varchar"),
        Column("calc_amount_remaining", "varchar"),
        Column("vat_prepaid_flag", "varchar"),
        Column("receivable_account", "varchar"),
        Column("customer_trx_id", "varchar"),
        Column("exchange_rate", "varchar"),
        Column("trx_type", "varchar"),
        Column("record_count", "varchar"),
        Column("min_trx_date", "varchar"),
        Column("max_trx_date", "varchar"),
        Column("sum_amount_orig", "varchar"),
        Column("sum_amount_remain", "varchar"),
        Column("extract_datetime", "varchar"),
    ],
)
