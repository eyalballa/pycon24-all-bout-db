CREATE TABLE default.aggregated_appliance_attributes_local
(
    `customer_id` String,
    `attribute_name` String,
    `attribute_value` String,
    `last_seen` SimpleAggregateFunction(max, DateTime),
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(last_seen)
ORDER BY (customer_id, attribute_name, attribute_value)
TTL toDate(last_seen) + toIntervalDay(3) TO VOLUME 'main', last_seen + toIntervalMonth(6)
SETTINGS storage_policy = 's3_tiered', index_granularity = 8192