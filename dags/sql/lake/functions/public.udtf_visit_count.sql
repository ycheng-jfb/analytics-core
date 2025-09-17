USE DATABASE media_staging;
USE WAREHOUSE media_wh;

-- DROP FUNCTION facebook.udtf_visit_count(TIMESTAMP, VARCHAR);
    -- intended use of udtf_visit_count is to increment when there is no session in the preceding 5 minutes
    -- also returns additional information, including the sequence of the session within the visit
CREATE OR REPLACE FUNCTION lake.public.udtf_visit_count(event_datetime TIMESTAMP, attribution_event_key VARCHAR)
    RETURNS TABLE (
        is_first BOOLEAN,
        visit_count DOUBLE,
        visit_seq DOUBLE,
        start_key DOUBLE,
        visit_start_datetime TIMESTAMP
    )
    LANGUAGE JAVASCRIPT
    RETURNS NULL ON NULL INPUT
    VOLATILE
AS
'
{
   initialize: function() {
       this.validStart = Date.parse("0000-00-00");
       this.thisDate = Date.parse("0000-00-00");
       this.visitCount = 0;
       this.visitSeq = 0;
       this.minsDiff = 1000;
       this.startKey = 0;
   },

   processRow: function (row, rowWriter, context) {
       this.thisDate = row.EVENT_DATETIME;
       this.minsDiff = (this.thisDate - this.validStart) / (1000 * 60);

       if (this.minsDiff < 5) {
           this.visitSeq += 1;
           rowWriter.writeRow({IS_FIRST:  false, VISIT_COUNT: this.visitCount, VISIT_SEQ: this.visitSeq, START_KEY: this.startKey, VISIT_START_DATETIME: this.validStart});
       }
       else {
           this.validStart = this.thisDate;
           this.startKey = row.ATTRIBUTION_EVENT_KEY
           this.visitCount = this.visitCount + 1;
           this.visitSeq = 1;
           rowWriter.writeRow({IS_FIRST:  true, VISIT_COUNT: this.visitCount, VISIT_SEQ: this.visitSeq, START_KEY: this.startKey, VISIT_START_DATETIME: this.validStart});
       }
   },

   finalize: function (rowWriter, context) {/*...*/},
}
';

-- DROP FUNCTION FACEBOOK.udtf_visit_count(TIMESTAMP, VARCHAR(25), VARCHAR(25));
CREATE OR REPLACE FUNCTION public.udtf_visit_count(event_datetime TIMESTAMP, channel VARCHAR(25), subchannel VARCHAR(25))
    RETURNS TABLE (
        is_first BOOLEAN,
        visit_count DOUBLE,
        channel VARCHAR(25),
        subchannel VARCHAR(25)
    )
    LANGUAGE JAVASCRIPT
    STRICT
    IMMUTABLE
AS
'
{
   initialize: function (argumentInfo, context) {
       validStart = Date.parse("0000-00-00");
       thisDate = Date.parse("0000-00-00");
       visitCount = 0;
       retChannel = null;
       retSubchannel = null;
       minsDiff = 1000;
   },

   processRow: function (row, rowWriter, context) {
       thisDate = row.EVENT_DATETIME;
       retChannel = (row.CHANNEL == "Brand Site") ? null : row.CHANNEL;
       retSubchannel = (row.SUBCHANNEL == "Brand Site") ? null : row.SUBCHANNEL;
       minsDiff = (thisDate - validStart) / (1000 * 60);

       if (minsDiff < 30) {
          rowWriter.writeRow({IS_FIRST:  false, VISIT_COUNT: visitCount, CHANNEL: retChannel, SUBCHANNEL: retSubchannel});
       }
       else {
           validStart = thisDate;
           visitCount += 1;
           rowWriter.writeRow({IS_FIRST:  true, VISIT_COUNT: visitCount, CHANNEL: retChannel, SUBCHANNEL: retSubchannel})
       }
   },

   finalize: function (rowWriter, context) {/*...*/},
}
';
