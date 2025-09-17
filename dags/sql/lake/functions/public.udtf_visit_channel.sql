USE DATABASE media_staging;

CREATE OR REPLACE FUNCTION lake.public.udtf_visit_channel(channel VARCHAR, media_source_hash VARCHAR)
    RETURNS TABLE(media_source_hash VARCHAR)
LANGUAGE JAVASCRIPT
STRICT
IMMUTABLE
AS '
{
    initialize: function() {
        this.media_source_hash = null;
    },

    processRow: function (row, rowWriter, context) {
        if (this.media_source_hash == null && row.CHANNEL != "Brand Site") {
            this.media_source_hash = row.MEDIA_SOURCE_HASH;
        }
        rowWriter.writeRow({MEDIA_SOURCE_HASH:  this.media_source_hash});
    },

    finalize: function (rowWriter, context) {/*...*/},
}
';
