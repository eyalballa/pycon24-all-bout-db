
CREATE TABLE attributes_local
(
    `customer_id` String,
    `asset_id` String,
    `attribute_name` String,
    `attribute_value` String,
    `last_seen` SimpleAggregateFunction(max, DateTime),
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(last_seen)
ORDER BY (customer_id, attribute_name, attribute_value)
TTL toDate(last_seen) + toIntervalDay(3) TO VOLUME 's3_main', last_seen + toIntervalMonth(6)
SETTINGS storage_policy = 's3_tiered', index_granularity = 8192


create table if not exists attributes ON CLUSTER '{cluster}'
AS default.attributes_local
ENGINE = Distributed('{cluster}', default, attributes_local, murmurHash3_64(asset_id));

