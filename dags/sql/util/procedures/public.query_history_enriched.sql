USE DATABASE util;
SET query_history_watermark = (SELECT IFNULL(MAX(end_time), CURRENT_DATE() - 90)
                               FROM util.public.query_history_enriched);

CREATE OR REPLACE TEMPORARY TABLE _query_history AS
    (SELECT *
     FROM util.public.query_history
     WHERE end_time IS NOT NULL
       AND end_time >= $query_history_watermark);

CREATE OR REPLACE TEMPORARY TABLE _dates_base AS
    (SELECT date_day AS date
     FROM (WITH rawdata AS (WITH p AS (SELECT 0 AS generated_number
                                       UNION ALL
                                       SELECT 1),
                                 unioned AS (SELECT p0.generated_number * POWER(2, 0)
                                                        +
                                                    p1.generated_number * POWER(2, 1)
                                                        +
                                                    p2.generated_number * POWER(2, 2)
                                                        +
                                                    p3.generated_number * POWER(2, 3)
                                                        +
                                                    p4.generated_number * POWER(2, 4)
                                                        +
                                                    p5.generated_number * POWER(2, 5)
                                                        +
                                                    p6.generated_number * POWER(2, 6)
                                                        +
                                                    p7.generated_number * POWER(2, 7)
                                                        +
                                                    p8.generated_number * POWER(2, 8)
                                                        +
                                                    p9.generated_number * POWER(2, 9)
                                                        +
                                                    p10.generated_number * POWER(2, 10)
                                                        +
                                                    p11.generated_number * POWER(2, 11)
                                                        +
                                                    p12.generated_number * POWER(2, 12)
                                                        + 1
                                                        AS generated_number
                                             FROM p AS p0
                                                      CROSS JOIN
                                                  p AS p1
                                                      CROSS JOIN
                                                  p AS p2
                                                      CROSS JOIN
                                                  p AS p3
                                                      CROSS JOIN
                                                  p AS p4
                                                      CROSS JOIN
                                                  p AS p5
                                                      CROSS JOIN
                                                  p AS p6
                                                      CROSS JOIN
                                                  p AS p7
                                                      CROSS JOIN
                                                  p AS p8
                                                      CROSS JOIN
                                                  p AS p9
                                                      CROSS JOIN
                                                  p AS p10
                                                      CROSS JOIN
                                                  p AS p11
                                                      CROSS JOIN
                                                  p AS p12)
                            SELECT *
                            FROM unioned
                            WHERE generated_number <= 10000
                            ORDER BY generated_number),

                all_periods AS (SELECT (
                                           DATEADD(
                                               DAY,
                                               ROW_NUMBER() OVER (ORDER BY 1) - 1,
                                               '2018-01-01'
                                           )
                                           ) AS date_day
                                FROM rawdata),

                filtered AS (SELECT *
                             FROM all_periods
                             WHERE date_day <= DATEADD(DAY, 1, CURRENT_DATE))

           SELECT *
           FROM filtered));

CREATE OR REPLACE TEMPORARY TABLE _rate_sheet_daily_base AS (SELECT date,
                                                                    usage_type,
                                                                    currency,
                                                                    effective_rate,
                                                                    service_type
                                                             FROM snowflake.organization_usage.rate_sheet_daily
                                                             WHERE account_locator = CURRENT_ACCOUNT());
CREATE OR REPLACE TEMPORARY TABLE _remaining_balance_daily_without_contract_view AS
    (SELECT date,
            organization_name,
            currency,
            free_usage_balance,
            capacity_balance,
            on_demand_consumption_balance,
            rollover_balance
     FROM snowflake.organization_usage.remaining_balance_daily

     QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY contract_number DESC) = 1);

CREATE OR REPLACE TEMPORARY TABLE _stop_thresholds AS
    (SELECT MIN(date) AS start_date
     FROM _rate_sheet_daily_base

     UNION ALL

     SELECT MIN(date) AS start_date
     FROM _remaining_balance_daily_without_contract_view);

CREATE OR REPLACE TEMPORARY TABLE _date_range AS
    (SELECT MAX(start_date) AS start_date,
            CURRENT_DATE    AS end_date
     FROM _stop_thresholds);

CREATE OR REPLACE TEMPORARY TABLE _remaining_balance_daily AS
    (SELECT date,
            free_usage_balance + capacity_balance + on_demand_consumption_balance +
            rollover_balance      AS remaining_balance,
            remaining_balance < 0 AS is_account_in_overage
     FROM _remaining_balance_daily_without_contract_view);

CREATE OR REPLACE TEMPORARY TABLE _latest_remaining_balance_daily AS
    (SELECT date,
            remaining_balance,
            is_account_in_overage
     FROM _remaining_balance_daily
     QUALIFY ROW_NUMBER() OVER (ORDER BY date DESC) = 1);

CREATE OR REPLACE TEMPORARY TABLE _rate_sheet_daily AS
    (SELECT r.*
     FROM _rate_sheet_daily_base r
              INNER JOIN _date_range d
                         ON r.date BETWEEN d.start_date AND d.end_date);

CREATE OR REPLACE TEMPORARY TABLE _rates_date_range_w_usage_types AS
    (SELECT d.start_date,
            d.end_date,
            usage_types.usage_type
     FROM _date_range d
              CROSS JOIN (SELECT DISTINCT usage_type FROM _rate_sheet_daily) AS usage_types);

CREATE OR REPLACE TEMPORARY TABLE _base AS
    (SELECT db.date,
            dr.usage_type
     FROM _dates_base AS db
              INNER JOIN _rates_date_range_w_usage_types AS dr
                         ON db.date BETWEEN dr.start_date AND dr.end_date);

CREATE OR REPLACE TEMPORARY TABLE _rates_w_overage AS
    (SELECT b.date,
            b.usage_type,
            COALESCE(
                rs.service_type,
                LAG(rs.service_type) IGNORE NULLS OVER (PARTITION BY b.usage_type ORDER BY b.date),
                LEAD(rs.service_type) IGNORE NULLS OVER (PARTITION BY b.usage_type ORDER BY b.date)
            )                                                                 AS service_type,
            COALESCE(
                rs.effective_rate,
                LAG(rs.effective_rate) IGNORE NULLS OVER (PARTITION BY b.usage_type ORDER BY b.date),
                LEAD(rs.effective_rate) IGNORE NULLS OVER (PARTITION BY b.usage_type ORDER BY b.date)
            )                                                                 AS effective_rate,
            COALESCE(
                rs.currency,
                LAG(rs.currency) IGNORE NULLS OVER (PARTITION BY b.usage_type ORDER BY b.date),
                LEAD(rs.currency) IGNORE NULLS OVER (PARTITION BY b.usage_type ORDER BY b.date)
            )                                                                 AS currency,
            b.usage_type LIKE 'overage-%'                                     AS is_overage_rate,
            REPLACE(b.usage_type, 'overage-', '')                             AS associated_usage_type,
            COALESCE(r.is_account_in_overage, l.is_account_in_overage, FALSE) AS _is_account_in_overage,
            CASE
                WHEN _is_account_in_overage AND is_overage_rate THEN 1
                WHEN NOT _is_account_in_overage AND NOT is_overage_rate THEN 1
                ELSE 0
                END                                                           AS rate_priority

     FROM _base b
              LEFT JOIN _latest_remaining_balance_daily AS l ON l.date IS NOT NULL
              LEFT JOIN _remaining_balance_daily AS r
                        ON b.date = r.date
              LEFT JOIN _rate_sheet_daily AS rs
                        ON b.date = rs.date
                            AND b.usage_type = rs.usage_type);

CREATE OR REPLACE TEMPORARY TABLE _rates AS
    (SELECT date,
            usage_type,
            associated_usage_type,
            service_type,
            effective_rate,
            currency,
            is_overage_rate
     FROM _rates_w_overage
     QUALIFY
         ROW_NUMBER() OVER (PARTITION BY date, service_type, associated_usage_type ORDER BY rate_priority DESC) = 1);

CREATE OR REPLACE TEMPORARY TABLE _daily_rates AS
    (SELECT date,
            associated_usage_type                                                                       AS usage_type,
            service_type,
            effective_rate,
            currency,
            is_overage_rate,
            ROW_NUMBER() OVER (PARTITION BY service_type, associated_usage_type ORDER BY date DESC) =
            1                                                                                           AS is_latest_rate
     FROM _rates
     ORDER BY date);

CREATE OR REPLACE TEMPORARY TABLE _stop_threshold AS
    (SELECT MAX(end_time) AS latest_ts
     FROM snowflake.account_usage.warehouse_metering_history);

CREATE OR REPLACE TEMPORARY TABLE _filtered_queries AS
    (SELECT query_id,
            query_text                 AS original_query_text,
            credits_used_cloud_services,
            warehouse_id,
            warehouse_size IS NOT NULL AS ran_on_warehouse,
            TIMEADD(
                'millisecond',
                queued_overload_time + compilation_time
                    + queued_provisioning_time + queued_repair_time
                    + list_external_files_time,
                start_time
            )                          AS execution_start_time,
            start_time,
            end_time,
            query_acceleration_bytes_scanned
     FROM util.public.query_history
     WHERE end_time <= (SELECT latest_ts FROM _stop_threshold)
       AND end_time < DATE_TRUNC(DAY, getdate())
       AND end_time >= $query_history_watermark);

CREATE OR REPLACE TEMPORARY TABLE _hours_list AS
    (SELECT DATEADD(
                'hour',
                '-' || ROW_NUMBER() OVER (ORDER BY SEQ4() ASC),
                DATEADD('day', '+1', CURRENT_DATE::TIMESTAMP_TZ)
            )                                 AS hour_start,
            DATEADD('hour', '+1', hour_start) AS hour_end
     FROM TABLE (GENERATOR(ROWCOUNT => (24 * 730))));

CREATE OR REPLACE TEMPORARY TABLE _query_hours AS
    (SELECT h.hour_start,
            h.hour_end,
            queries.*
     FROM _hours_list h
              INNER JOIN _filtered_queries AS queries
                         ON h.hour_start >= DATE_TRUNC('hour', queries.execution_start_time)
                             AND h.hour_start < queries.end_time
                             AND queries.ran_on_warehouse);

CREATE OR REPLACE TEMPORARY TABLE _query_seconds_per_hour AS
    (SELECT *,
            DATEDIFF('millisecond', GREATEST(execution_start_time, hour_start),
                     LEAST(end_time, hour_end))                                                    AS num_milliseconds_query_ran,
            SUM(num_milliseconds_query_ran)
                OVER (PARTITION BY warehouse_id, hour_start)                                       AS total_query_milliseconds_in_hour,
            DIV0(num_milliseconds_query_ran, total_query_milliseconds_in_hour)                     AS fraction_of_total_query_time_in_hour,
            SUM(query_acceleration_bytes_scanned)
                OVER (PARTITION BY warehouse_id, hour_start)                                       AS total_query_acceleration_bytes_scanned_in_hour,
            DIV0(query_acceleration_bytes_scanned,
                 total_query_acceleration_bytes_scanned_in_hour)                                   AS fraction_of_total_query_acceleration_bytes_scanned_in_hour,
            hour_start                                                                             AS hour
     FROM _query_hours);

CREATE OR REPLACE TEMPORARY TABLE _credits_billed_hourly AS
    (SELECT start_time                                                                    AS hour,
            entity_id                                                                     AS warehouse_id,
            SUM(IFF(service_type = 'WAREHOUSE_METERING', credits_used_compute, 0))        AS credits_used_compute,
            SUM(IFF(service_type = 'WAREHOUSE_METERING', credits_used_cloud_services,
                    0))                                                                   AS credits_used_cloud_services,
            SUM(IFF(service_type = 'QUERY_ACCELERATION', credits_used_compute, 0))        AS credits_used_query_acceleration
     FROM snowflake.account_usage.metering_history
     WHERE TRUE
       AND service_type IN ('QUERY_ACCELERATION', 'WAREHOUSE_METERING')
     GROUP BY 1, 2);

CREATE OR REPLACE TEMPORARY TABLE _query_cost AS
    (SELECT qs.*,
            cb.credits_used_compute * dr.effective_rate                   AS actual_warehouse_cost,
            cb.credits_used_compute *
            qs.fraction_of_total_query_time_in_hour *
            dr.effective_rate                                             AS allocated_compute_cost_in_hour,
            cb.credits_used_compute *
            qs.fraction_of_total_query_time_in_hour                       AS allocated_compute_credits_in_hour,
            cb.credits_used_query_acceleration *
            qs.fraction_of_total_query_acceleration_bytes_scanned_in_hour AS allocated_query_acceleration_credits_in_hour,
            allocated_query_acceleration_credits_in_hour *
            dr.effective_rate                                             AS allocated_query_acceleration_cost_in_hour
     FROM _query_seconds_per_hour qs
              INNER JOIN _credits_billed_hourly cb
                         ON qs.warehouse_id = cb.warehouse_id
                             AND qs.hour = cb.hour
              INNER JOIN _daily_rates dr
                         ON DATE(qs.start_time) = dr.date
                             AND dr.service_type = 'WAREHOUSE_METERING'
                             AND dr.usage_type = 'compute');

CREATE OR REPLACE TEMPORARY TABLE _cost_per_query AS
    (SELECT query_id,
            ANY_VALUE(start_time)                             AS start_time,
            ANY_VALUE(end_time)                               AS end_time,
            ANY_VALUE(execution_start_time)                   AS execution_start_time,
            SUM(allocated_compute_cost_in_hour)               AS compute_cost,
            SUM(allocated_compute_credits_in_hour)            AS compute_credits,
            SUM(allocated_query_acceleration_cost_in_hour)    AS query_acceleration_cost,
            SUM(allocated_query_acceleration_credits_in_hour) AS query_acceleration_credits,
            ANY_VALUE(credits_used_cloud_services)            AS credits_used_cloud_services,
            ANY_VALUE(ran_on_warehouse)                       AS ran_on_warehouse
     FROM _query_cost
     GROUP BY 1);

CREATE OR REPLACE TEMPORARY TABLE _credits_billed_daily AS
    (SELECT DATE(hour)                                                                        AS date,
            SUM(credits_used_compute)                                                         AS daily_credits_used_compute,
            SUM(credits_used_cloud_services)                                                  AS daily_credits_used_cloud_services,
            GREATEST(daily_credits_used_cloud_services - daily_credits_used_compute * 0.1,
                     0)                                                                       AS daily_billable_cloud_services
     FROM _credits_billed_hourly
     GROUP BY 1);

CREATE OR REPLACE TEMPORARY TABLE _all_queries AS
    (SELECT query_id,
            start_time,
            end_time,
            execution_start_time,
            compute_cost,
            compute_credits,
            query_acceleration_cost,
            query_acceleration_credits,
            credits_used_cloud_services,
            ran_on_warehouse
     FROM _cost_per_query

     UNION ALL

     SELECT query_id,
            start_time,
            end_time,
            execution_start_time,
            0 AS compute_cost,
            0 AS compute_credits,
            0 AS query_acceleration_cost,
            0 AS query_acceleration_credits,
            credits_used_cloud_services,
            ran_on_warehouse
     FROM _filtered_queries
     WHERE NOT ran_on_warehouse);

CREATE OR REPLACE TEMPORARY TABLE _cost_per_query_stg AS
    (SELECT aq.query_id,
            aq.start_time,
            aq.end_time,
            aq.execution_start_time,
            aq.compute_cost,
            aq.compute_credits,
            aq.query_acceleration_cost,
            aq.query_acceleration_credits,
            -- For the most recent day, which is not yet complete, this calculation won't be perfect.
            -- So, we don't look at any queries from the most recent day t, just t-1 and before.
            (DIV0(aq.credits_used_cloud_services, cbd.daily_credits_used_cloud_services) *
             cbd.daily_billable_cloud_services) *
            COALESCE(dr.effective_rate, cr.effective_rate)                     AS cloud_services_cost,
            DIV0(aq.credits_used_cloud_services, cbd.daily_credits_used_cloud_services) *
            cbd.daily_billable_cloud_services                                  AS cloud_services_credits,
            aq.compute_cost + aq.query_acceleration_cost + cloud_services_cost AS query_cost,
            aq.compute_credits +
            aq.query_acceleration_credits +
            cloud_services_credits                                             AS query_credits,
            aq.ran_on_warehouse,
            COALESCE(dr.currency, cr.currency)             AS currency
     FROM _all_queries aq
              INNER JOIN _credits_billed_daily cbd
                         ON DATE(aq.start_time) = cbd.date
              LEFT JOIN _daily_rates dr
                        ON DATE(aq.start_time) = dr.date
                            AND dr.service_type = 'CLOUD_SERVICES'
                            AND dr.usage_type = 'cloud services'
         -- current rates
              INNER JOIN _daily_rates AS cr
                         ON cr.is_latest_rate
                             AND cr.service_type = 'CLOUD_SERVICES'
                             AND cr.usage_type = 'cloud services'
     ORDER BY aq.start_time ASC);


CREATE OR REPLACE TEMPORARY TABLE _query_attribution_history AS
    (SELECT *
     FROM snowflake.account_usage.query_attribution_history
     WHERE end_time < DATE_TRUNC(DAY, getdate())
       AND end_time >= $query_history_watermark);

CREATE OR REPLACE TEMPORARY TABLE _final AS
SELECT qh.query_id
     , cpq.compute_credits            AS credits_attributed_compute
     , cpq.compute_cost               AS compute_cost
     , cpq.query_acceleration_credits AS credits_used_query_acceleration
     , cpq.query_acceleration_cost    AS query_acceleration_cost
     , cpq.cloud_services_credits     AS credits_used_cloud_services
     , cpq.cloud_services_cost        AS cloud_services_cost
     , cpq.query_cost                 AS total_cost
     , qh.query_text
     , qh.database_id
     , qh.database_name
     , qh.schema_id
     , qh.schema_name
     , qh.query_type
     , qh.session_id
     , qh.user_name
     , qh.role_name
     , qh.warehouse_id
     , qh.warehouse_name
     , qh.warehouse_size
     , qh.warehouse_type
     , qh.cluster_number
     , qh.query_tag
     , qh.execution_status
     , qh.error_code
     , qh.error_message
     , qh.start_time
     , qh.end_time
     , qh.warehouse_size IS NOT NULL  AS ran_on_warehouse
     , qh.total_elapsed_time          AS total_elapsed_time_ms
     , qh.compilation_time            AS compilation_time_ms
     , qh.queued_provisioning_time    AS queued_provisioning_time_ms
     , qh.queued_repair_time          AS queued_repair_time_ms
     , qh.queued_overload_time        AS queued_overload_time_ms
     , qh.transaction_blocked_time    AS transaction_blocked_time_ms
     , qh.list_external_files_time    AS list_external_files_time_ms
     , qh.execution_time              AS execution_time_ms
     , qh.bytes_scanned
     , qh.percentage_scanned_from_cache
     , qh.bytes_written
     , qh.bytes_written_to_result
     , qh.bytes_read_from_result
     , qh.rows_produced
     , qh.rows_inserted
     , qh.rows_updated
     , qh.rows_deleted
     , qh.rows_unloaded
     , qh.bytes_deleted
     , qh.partitions_scanned
     , qh.partitions_total
     , qh.bytes_spilled_to_local_storage
     , qh.bytes_spilled_to_remote_storage
     , qh.bytes_sent_over_the_network
     , qh.outbound_data_transfer_cloud
     , qh.outbound_data_transfer_region
     , qh.outbound_data_transfer_bytes
     , qh.inbound_data_transfer_cloud
     , qh.inbound_data_transfer_region
     , qh.inbound_data_transfer_bytes
     , qh.release_version
     , qh.external_function_total_invocations
     , qh.external_function_total_sent_rows
     , qh.external_function_total_received_rows
     , qh.external_function_total_sent_bytes
     , qh.external_function_total_received_bytes
     , qh.query_load_percent
     , qh.is_client_generated_statement
     , qh.query_acceleration_bytes_scanned
     , qh.query_acceleration_partitions_scanned
     , qh.query_acceleration_upper_limit_scale_factor
     , qah.parent_query_id
     , qah.root_query_id
FROM _query_history qh
         LEFT JOIN _cost_per_query_stg cpq ON qh.query_id = cpq.query_id
         LEFT JOIN _query_attribution_history AS qah ON qah.query_id = qh.query_id
ORDER BY TO_DATE(qh.start_time);


DELETE FROM util.public.query_history_enriched WHERE to_date(end_time) < current_date()-90;

MERGE INTO util.public.query_history_enriched AS t
    USING _final AS s
    ON t.query_id = s.query_id
    WHEN NOT MATCHED THEN
        INSERT (
                query_id, credits_attributed_compute, compute_cost, credits_used_query_acceleration,
                query_acceleration_cost, credits_used_cloud_services, cloud_services_cost, total_cost, query_text,
                database_id, database_name, schema_id, schema_name, query_type, session_id, user_name, role_name,
                warehouse_id, warehouse_name, warehouse_size, warehouse_type, cluster_number,
                query_tag, dag_id, task_id, execution_status, error_code, error_message, start_time, end_time, ran_on_warehouse,
                total_elapsed_time_ms, compilation_time_ms, queued_provisioning_time_ms, queued_repair_time_ms,
                queued_overload_time_ms, transaction_blocked_time_ms, list_external_files_time_ms, execution_time_ms,
                bytes_scanned, percentage_scanned_from_cache, bytes_written, bytes_written_to_result,
                bytes_read_from_result, rows_produced, rows_inserted, rows_updated, rows_deleted, rows_unloaded,
                bytes_deleted, partitions_scanned, partitions_total, bytes_spilled_to_local_storage,
                bytes_spilled_to_remote_storage, bytes_sent_over_the_network, outbound_data_transfer_cloud,
                outbound_data_transfer_region, outbound_data_transfer_bytes, inbound_data_transfer_cloud,
                inbound_data_transfer_region, inbound_data_transfer_bytes, release_version,
                external_function_total_invocations, external_function_total_sent_rows,
                external_function_total_received_rows, external_function_total_sent_bytes,
                external_function_total_received_bytes, query_load_percent, is_client_generated_statement,
                query_acceleration_bytes_scanned, query_acceleration_partitions_scanned,
                query_acceleration_upper_limit_scale_factor, parent_query_id, root_query_id
            )
            VALUES ( s.query_id
                   , s.credits_attributed_compute
                   , s.compute_cost
                   , s.credits_used_query_acceleration
                   , s.query_acceleration_cost
                   , s.credits_used_cloud_services
                   , s.cloud_services_cost
                   , s.total_cost
                   , s.query_text
                   , s.database_id
                   , s.database_name
                   , s.schema_id
                   , s.schema_name
                   , s.query_type
                   , s.session_id
                   , s.user_name
                   , s.role_name
                   , s.warehouse_id
                   , s.warehouse_name
                   , s.warehouse_size
                   , s.warehouse_type
                   , s.cluster_number
                   , s.query_tag
                   ,SPLIT_PART(s.query_tag, ',', 1)
                   ,SPLIT_PART(s.query_tag, ',', 2)
                   , s.execution_status
                   , s.error_code
                   , s.error_message
                   , s.start_time
                   , s.end_time
                   , s.ran_on_warehouse
                   , s.total_elapsed_time_ms
                   , s.compilation_time_ms
                   , s.queued_provisioning_time_ms
                   , s.queued_repair_time_ms
                   , s.queued_overload_time_ms
                   , s.transaction_blocked_time_ms
                   , s.list_external_files_time_ms
                   , s.execution_time_ms
                   , s.bytes_scanned
                   , s.percentage_scanned_from_cache
                   , s.bytes_written
                   , s.bytes_written_to_result
                   , s.bytes_read_from_result
                   , s.rows_produced
                   , s.rows_inserted
                   , s.rows_updated
                   , s.rows_deleted
                   , s.rows_unloaded
                   , s.bytes_deleted
                   , s.partitions_scanned
                   , s.partitions_total
                   , s.bytes_spilled_to_local_storage
                   , s.bytes_spilled_to_remote_storage
                   , s.bytes_sent_over_the_network
                   , s.outbound_data_transfer_cloud
                   , s.outbound_data_transfer_region
                   , s.outbound_data_transfer_bytes
                   , s.inbound_data_transfer_cloud
                   , s.inbound_data_transfer_region
                   , s.inbound_data_transfer_bytes
                   , s.release_version
                   , s.external_function_total_invocations
                   , s.external_function_total_sent_rows
                   , s.external_function_total_received_rows
                   , s.external_function_total_sent_bytes
                   , s.external_function_total_received_bytes
                   , s.query_load_percent
                   , s.is_client_generated_statement
                   , s.query_acceleration_bytes_scanned
                   , s.query_acceleration_partitions_scanned
                   , s.query_acceleration_upper_limit_scale_factor
                   , s.parent_query_id
                   , s.root_query_id);

-- ALTER TABLE util.public.query_history_enriched RECLUSTER WHERE end_time >= $query_history_watermark


-- Table reclustering is not available hence recluster data by table swapping.
-- CREATE TABLE util.public.query_history_enriched_new
-- CLUSTER BY (end_time) AS
-- SELECT * FROM util.public.query_history_enriched;
-- DROP TABLE util.public.query_history_enriched;
-- ALTER TABLE util.public.query_history_enriched_new RENAME TO util.public.query_history_enriched;
