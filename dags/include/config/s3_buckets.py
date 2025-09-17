tsos_da_int_inbound = "tsos-da-int-inbound"
"""prod engineering integrations"""

tsos_da_int_outbound = "tsos-da-int-outbound"
"""prod export from Snowflake, for non-vendor related needs"""

tsos_da_slm_outbound = "slm-global-incoming-fabletics"
"""prod export from Snowflake, for SLM vendor"""

tsos_da_int_vendor = "tsos-da-int-vendor"
"""when we need to grant access for external vendors, this is the bucket"""

tsos_da_int_sortinghat = "tsos-da-int-sortinghat"
"""for Sorting Hat file replication and ingestion"""

tsos_da_int_backup_archive = "tsos-da-int-backup-archive"
"""for storing backup data or for archiving data"""

tsos_da_int_inbound_dev = "tsos-da-int-inbound-dev"
"""adhoc, temp, or testing ingestion into Snowflake"""

tsos_da_int_outbound_dev = "tsos-da-int-outbound-dev"
"""adhoc, temp, or testing export from Snowflake"""
