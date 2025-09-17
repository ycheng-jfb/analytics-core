create or replace function util.public.flatten_and_explode(input object)
returns table(output variant)
language python
runtime_version=3.10
handler='FlattenAndExplode'
as $$
import re
from typing import Optional, List

#Copy and paste FlattenAndExplode class here when you need to deploy udtf. The normalize_field_name static function
# in FlattenAndExplode is also used in Segment operators, hence why it needs to live there.
$$;
