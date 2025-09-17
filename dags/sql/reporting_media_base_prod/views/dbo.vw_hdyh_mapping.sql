
create or replace view REPORTING_MEDIA_BASE_PROD.DBO.VW_HDYH_MAPPING(
	HDYH_VALUE,
	REALLOCATE_BRANDSITE,
	REALLOCATE_GATEWAY,
	CHANNEL,
	SUBCHANNEL
) as
SELECT
    hdyh_value,
    reallocate_brandsite,
    reallocate_gateway,
    channel,
    subchannel
FROM
    (SELECT
        ROW_NUMBER() OVER (PARTITION BY HDYH_VALUE ORDER BY META_CREATE_DATETIME DESC) AS RN,
        lower(HDYH_VALUE) as hdyh_value,
        lower(REALLOCATE_BRANDSITE) as reallocate_brandsite,
        lower(REALLOCATE_GATEWAY) as reallocate_gateway,
        channel,
        subchannel
    FROM lake_view.sharepoint.med_hdyh_legacy_mapping) hdyh
    WHERE hdyh.RN = 1
UNION
SELECT
    hdyh_value,
    reallocate_brandsite,
    reallocate_gateway,
    channel,
    subchannel
FROM
    (SELECT
        ROW_NUMBER() OVER (PARTITION BY HDYH ORDER BY ip.META_CREATE_DATETIME DESC) AS RN,
        lower(HDYH) as hdyh_value,
        'influencers' as reallocate_brandsite,
        'retain' as reallocate_gateway,
        'influencers' as channel,
        'ambassador' as subchannel
    FROM LAKE_VIEW.SHAREPOINT.MED_SELECTOR_INFLUENCER_PAID_SPEND_MAPPINGS ip
    LEFT JOIN lake_view.sharepoint.med_hdyh_legacy_mapping hd on lower(hd.HDYH_VALUE) = lower(ip.HDYH)
    WHERE ip.HDYH is not null
        and hd.HDYH_VALUE is null
        ) infl
    WHERE infl.RN = 1;
