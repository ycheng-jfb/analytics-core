create or replace function util.public.segment_shard_from_filename(s string)
returns string
language python
runtime_version=3.10
handler = 'segment_shard_from_filename'
as $$
def segment_shard_from_filename(s):
    filename_parts = s.split('/')
    return filename_parts[1]
$$;
