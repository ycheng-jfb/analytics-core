from dataclasses import dataclass


@dataclass
class Config:
    att_conn_id: str
    file_pattern: str
    customer_tier: str


config_list = [
    Config(
        att_conn_id="attentive_fl",
        file_pattern="attn_flw_vip*",
        customer_tier="vip",
    ),
    Config(
        att_conn_id="attentive_fl",
        file_pattern="attn_flw_all_leads*",
        customer_tier="lead",
    ),
    Config(
        att_conn_id="attentive_flm",
        file_pattern="attn_flm_vip*",
        customer_tier="vip",
    ),
    Config(
        att_conn_id="attentive_flm",
        file_pattern="attn_flm_all_leads*",
        customer_tier="lead",
    ),
    Config(
        att_conn_id="attentive_yty",
        file_pattern="attn_yty_vip*",
        customer_tier="vip",
    ),
    Config(
        att_conn_id="attentive_yty",
        file_pattern="attn_yty_all_leads*",
        customer_tier="lead",
    ),
]
