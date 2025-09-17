create or replace view LAKE_VIEW.TIKTOK.AD_LINK(
	AD_ID,
	ADVERTISER_ID,
	VIDEO_ID,
	POSTER_URL,
	URL
) as
with ad as (
select ad_id,advertiser_id,video_id
from lake_fivetran.med_tiktok_v1.ad_history
QUALIFY row_number() OVER ( PARTITION BY ad_id ORDER BY updated_at desc) = 1
), video as (
select video_id,video_cover_url,preview_url, row_number() OVER ( PARTITION BY video_id ORDER BY updated_at desc) as rn
from lake_fivetran.med_tiktok_v1.video_history
)
select a.*,video_cover_url as poster_url,preview_url as url
from ad a
left join video v on a.video_id = v.video_id and v.rn = 1
;
