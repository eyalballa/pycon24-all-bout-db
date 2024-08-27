

CREATE TABLE attributes (
    cid UUID,
    asset_id UUID,
    attribute_name TEXT,
    attribute_value TEXT,
    last_seen TIMESTAMPTZ
);

SELECT create_hypertable('attributes', 'last_seen', chunk_time_interval => INTERVAL '3 hours');

SELECT add_compress_policy('attributes', INTERVAL '2 days');
SELECT add_retention_policy('attributes', INTERVAL '3 days');

CREATE MATERIALIZED VIEW attributes_hourly AS
SELECT
    cid,
    asset_id,
    attribute_name,
    attribute_value,
    max(last_seen) AS last_seen
FROM
    attributes
GROUP BY
    cid,
    asset_id,
    attribute_name,
    attribute_value,
    time_bucket('1 day', last_seen);

SELECT add_continuous_aggregate_policy('attributes_daily',
    start_offset => INTERVAL '2 day',
    end_offset => INTERVAL '1 days',
    schedule_interval => INTERVAL '1 hour');