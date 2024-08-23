

CREATE TABLE attributes (
    cid UUID,
    attribute_name TEXT,
    attribute_value TEXT,
    last_seen TIMESTAMPTZ
);

SELECT create_hypertable('attributes', 'last_seen', chunk_time_interval => INTERVAL '2 hours');

SELECT add_compress_policy('attributes', INTERVAL '1 day');
SELECT add_retention_policy('attributes', INTERVAL '1 day');

CREATE MATERIALIZED VIEW attributes_hourly AS
SELECT
    cid,
    attribute_name,
    attribute_value,
    max(last_seen) AS last_seen
FROM
    attributes
GROUP BY
    cid,
    attribute_name,
    attribute_value,
    time_bucket('1 hour', last_seen);

SELECT add_continuous_aggregate_policy('attributes_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '4 hours',
    schedule_interval => INTERVAL '1 hour');