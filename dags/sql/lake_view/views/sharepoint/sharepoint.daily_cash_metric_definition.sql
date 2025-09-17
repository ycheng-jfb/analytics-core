create or replace view lake_view.sharepoint.daily_cash_metric_definition (
    SEQUENCE,
    METRIC_GROUP,
    METRIC,
    DEFINITION,
    IS_AVAILABLE_IN_SSRS,
    IS_AVAILABLE_IN_TABLEAU,
    META_ROW_HASH,
    META_CREATE_DATETIME,
    META_UPDATE_DATETIME
) as
select
    SEQUENCE,
    METRIC_GROUP,
    METRIC,
    DEFINITION,
    IS_AVAILABLE_IN_SSRS,
    IS_AVAILABLE_IN_TABLEAU,
    HASH(
        SEQUENCE,
        METRIC_GROUP,
        METRIC,
        DEFINITION,
        IS_AVAILABLE_IN_SSRS,
        IS_AVAILABLE_IN_TABLEAU
    ) as META_ROW_HASH,
    _fivetran_synced::timestamp as META_CREATE_DATETIME,
    _fivetran_synced::timestamp as META_UPDATE_DATETIME
from lake_fivetran.fpa_nonconfidential_sharepoint_v1.daily_cash_documentation_daily_cash;
