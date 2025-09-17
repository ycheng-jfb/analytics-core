atlan_get_assets = {
    "searchType": "BASIC",
    "typeName": "AtlanAsset",
    "excludeDeletedEntities": True,
    "includeClassificationAttributes": True,
    "includeSubClassifications": True,
    "includeSubTypes": True,
    "limit": 10000,
    "offset": 0,
    "entityFilters": {
        "condition": "AND",
        "criterion": [
            {"attributeName": "updatedSource", "attributeValue": "lineage", "operator": "neq"},
            {
                "condition": "OR",
                "criterion": [
                    {
                        "attributeName": "qualifiedName",
                        "attributeValue": "snowflake/techstyle/edw/stg",
                        "operator": "startsWith",
                    },
                    {
                        "attributeName": "qualifiedName",
                        "attributeValue": "snowflake/techstyle/edw/data_model",
                        "operator": "startsWith",
                    },
                ],
            },
            {
                "condition": "OR",
                "criterion": [
                    {"attributeName": "typeName", "attributeValue": "AtlanTable", "operator": "eq"},
                    {"attributeName": "typeName", "attributeValue": "AtlanView", "operator": "eq"},
                ],
            },
        ],
    },
}

atlan_get_specific_asset_v2 = {
    "dsl": {
        "from": 0,
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {"match": {"__state": "ACTIVE"}},
                    {"match": {"__typeName.keyword": "Column"}},
                    {"match": {"qualifiedName": "--replace_me--"}},
                ]
            }
        },
    },
    "attributes": ["guid"],
}
