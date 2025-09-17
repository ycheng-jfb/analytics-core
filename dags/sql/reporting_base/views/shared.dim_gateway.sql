
CREATE OR REPLACE VIEW shared.dim_gateway AS
SELECT
    dm_gateway_id,
    gateway_name,
    gateway_type,
    gateway_sub_type,
    gateway_code,
    gateway_status,
    gateway_statuscode,
    redirect_dm_gateway_id,
    store_id,
    store_brand_abbr,
    store_brand_name,
    store_country,
    store_region,
    offer,
    gender_featured,
    landing_location,
    device,
    ftv_or_rtv,
    influencer_name,
    product_category,
    experience_description,
    freeform,
    tracking_type,
    effective_start_datetime_final as effective_start_datetime,
    lead(dateadd(ms,-1, effective_start_datetime_final), 1, '9999-12-31') OVER (PARTITION BY dm_gateway_id ORDER BY effective_start_datetime_final)::timestamp_ntz(3) AS effective_end_datetime,
    IFF(rn=1, TRUE, FALSE) AS meta_is_current
FROM (
         SELECT dm.dm_gateway_id,
                dm.gateway_name,
                dm.gateway_type,
                dm.gateway_sub_type,
                dm.gateway_code,
                dm.gateway_status,
                dm.gateway_statuscode,
                dm.redirect_dm_gateway_id,
                dm.store_id,
                g.store_brand_abbr,
                dm.store_brand_name,
                dm.store_country,
                dm.store_region,
                g.offer,
                g.gender_featured,
                g.landing_location,
                g.device,
                g.ftv_or_rtv,
                g.influencer_name,
                g.product_category,
                g.experience_description,
                g.freeform,
                dm.tracking_type,
                CASE
                    WHEN g.effective_start_datetime BETWEEN dm.effective_start_datetime and dm.effective_end_datetime
                        THEN g.effective_start_datetime
                    ELSE dm.effective_start_datetime
                    END AS effective_start_datetime_final,
                row_number() over (partition by dm.dm_gateway_id ORDER BY effective_start_datetime_final DESC) as rn
         FROM history.dm_gateway dm
                  LEFT JOIN shared.gateway_naming_convention g
                            ON dm.dm_gateway_id = g.dm_gateway_id
                                AND (
                                       g.effective_start_datetime BETWEEN dm.effective_start_datetime and dm.effective_end_datetime
                                       OR
                                       dm.effective_start_datetime BETWEEN g.effective_start_datetime and g.effective_end_datetime
                                   )
     );
