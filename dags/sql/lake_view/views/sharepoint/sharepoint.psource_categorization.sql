create or replace view lake_view.sharepoint.psource_categorization (
    BRAND,
    GENDER,
    PSOURCE,
    L2_CATEGORIZATION,
    L1_CATEGORIZATION,
    IDEAL_GROUPING,
    META_ROW_HASH,
    META_CREATE_DATETIME,
    META_UPDATE_DATETIME
) as
select
    BRAND,
    GENDER,
    P_SOURCE,
    L_2_CATEGORIZATION,
    L_1_CATEGORIZATION,
    IDEAL_GROUPING,
    hash(
        BRAND,
        GENDER,
        P_SOURCE,
        L_2_CATEGORIZATION,
        L_1_CATEGORIZATION,
        IDEAL_GROUPING
    ) as meta_row_hash,
    _fivetran_synced::timestamp as meta_create_datetime,
    _fivetran_synced::timestamp as meta_update_datetime
from
    lake_fivetran.central_inbound_sharepoint_v1.psource_categorization_fabletics
union all
select
    BRAND,
    GENDER,
    P_SOURCE,
    L_2_CATEGORIZATION,
    L_1_CATEGORIZATION,
    IDEAL_GROUPING,
    hash(
        BRAND,
        GENDER,
        P_SOURCE,
        L_2_CATEGORIZATION,
        L_1_CATEGORIZATION,
        IDEAL_GROUPING
    ) as meta_row_hash,
    _fivetran_synced::timestamp as meta_create_datetime,
    _fivetran_synced::timestamp as meta_update_datetime
from
    lake_fivetran.central_inbound_sharepoint_v1.psource_categorization_just_fab
union all
select
    BRAND,
    GENDER,
    P_SOURCE,
    L_2_CATEGORIZATION,
    L_1_CATEGORIZATION,
    IDEAL_GROUPING,
    hash(
        BRAND,
        GENDER,
        P_SOURCE,
        L_2_CATEGORIZATION,
        L_1_CATEGORIZATION,
        IDEAL_GROUPING
    ) as meta_row_hash,
    _fivetran_synced::timestamp as meta_create_datetime,
    _fivetran_synced::timestamp as meta_update_datetime
from
    lake_fivetran.central_inbound_sharepoint_v1.psource_categorization_savage_x
    union all
select
    BRAND,
    GENDER,
    P_SOURCE,
    L_2_CATEGORIZATION,
    L_1_CATEGORIZATION,
    IDEAL_GROUPING,
    hash(
        BRAND,
        GENDER,
        P_SOURCE,
        L_2_CATEGORIZATION,
        L_1_CATEGORIZATION,
        IDEAL_GROUPING
    ) as meta_row_hash,
    _fivetran_synced::timestamp as meta_create_datetime,
    _fivetran_synced::timestamp as meta_update_datetime
from
    lake_fivetran.central_inbound_sharepoint_v1.psource_categorization_shoe_dazzle
    union all
select
    BRAND,
    GENDER,
    P_SOURCE,
    L_2_CATEGORIZATION,
    L_1_CATEGORIZATION,
    IDEAL_GROUPING,
    hash(
        BRAND,
        GENDER,
        P_SOURCE,
        L_2_CATEGORIZATION,
        L_1_CATEGORIZATION,
        IDEAL_GROUPING
    ) as meta_row_hash,
    _fivetran_synced::timestamp as meta_create_datetime,
    _fivetran_synced::timestamp as meta_update_datetime
from
lake_fivetran.central_inbound_sharepoint_v1.psource_categorization_fab_kids;
