create or replace function util.public.country_from_shard(s string)
returns string
language python
runtime_version=3.10
handler = 'country_from_shard'
as $$
def country_from_shard(s):
    shard_parts = s.split('_')
    if shard_parts[1] in ['shoedazzle', 'fabkids']:
        return 'us'
    elif shard_parts[0] in ['react','node']:
        return ''
    elif shard_parts[2] == 'ecom':
        return ''
    elif shard_parts[2] in ['production', 'prod']:
        return 'us'
    else:
        return shard_parts[2]
$$;
