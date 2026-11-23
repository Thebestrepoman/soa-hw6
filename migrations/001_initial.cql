CREATE KEYSPACE IF NOT EXISTS warehouse WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
CREATE TABLE IF NOT EXISTS warehouse.schema_migrations (version text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product_zone (
    product_id text, zone_id text, available_quantity int, reserved_quantity int, manual_reserved_quantity int,
    last_event_at bigint, last_event_id text,
    PRIMARY KEY ((product_id), zone_id)
);
CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product (
    product_id text PRIMARY KEY, total_available int, total_reserved int,
    last_event_at bigint, last_event_id text
);
CREATE TABLE IF NOT EXISTS warehouse.inventory_by_zone (
    zone_id text, product_id text, available_quantity int, reserved_quantity int, manual_reserved_quantity int,
    last_event_at bigint, last_event_id text,
    PRIMARY KEY ((zone_id), product_id)
);
CREATE TABLE IF NOT EXISTS warehouse.zones (
    zone_id text PRIMARY KEY, capacity int, occupied int,
    last_event_at bigint
);
CREATE TABLE IF NOT EXISTS warehouse.orders (
    order_id text PRIMARY KEY, status text, items_json text,
    last_event_at bigint, last_event_id text
);
CREATE TABLE IF NOT EXISTS warehouse.processed_events (
    event_id text PRIMARY KEY, payload_hash text, plan_json text,
    source_partition int, source_offset bigint, outcome text, status text
);
CREATE TABLE IF NOT EXISTS warehouse.event_history (
    event_day date, event_id text, event_type text, event_at text,
    source_partition int, source_offset bigint, outcome text, payload_json text,
    PRIMARY KEY ((event_day), event_id)
);
CREATE TABLE IF NOT EXISTS warehouse.runtime_state (
    name text PRIMARY KEY, write_timestamp bigint
);
