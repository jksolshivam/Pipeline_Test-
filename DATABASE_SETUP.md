# ClickHouse Database Setup & Schema

This document outlines the required database structure for the In-App Events pipeline.

## 1. Configuration Tables (Internal)

These tables are used by both the **Producer** (for validation and routing) and the **Consumer** (to know where to write data).

### `tbl_apps`
Stores secrets for each application package.
```sql
CREATE TABLE IF NOT EXISTS tbl_apps (
    app_package_id String,
    app_package_secret String -- MUST be a 64-character hex string (32 bytes)
) ENGINE = ReplacingMergeTree()
ORDER BY app_package_id;
```

### `tbl_event_routes`
Maps API paths and Kafka topics to ClickHouse destinations.
```sql
CREATE TABLE IF NOT EXISTS tbl_event_routes (
    er_api_path String,           -- e.g., "/v1/events"
    er_kafka_topic String,        -- e.g., "app-events-topic"
    ch_db_name String,           -- e.g., "events_db"
    ch_table_name String,        -- e.g., "app_events"
    er_required_fields Array(String) -- Used by Producer for validation
) ENGINE = ReplacingMergeTree()
ORDER BY er_api_path;
```

---

## 2. Fallback Tables (Default Database)

The Consumer is configured to write errors and unmapped events to these tables in the `default` database.

### `unregistered_routes`
Captures events sent to an API path not found in `tbl_event_routes`.
```sql
CREATE TABLE IF NOT EXISTS default.unregistered_routes (
    api_path String,
    headers String,
    raw_body String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY created_at;
```

### `unlisted_package_ids`
Captures events from `x-package-id` headers that don't exist in `tbl_apps`.
```sql
CREATE TABLE IF NOT EXISTS default.unlisted_package_ids (
    package_id String,
    api_path String, -- Included for debugging
    headers String,
    raw_body String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY created_at;
```

---

## 3. Sample Event Table

A typical table for storing actual telemetry data.

```sql
CREATE DATABASE IF NOT EXISTS events_db;

CREATE TABLE IF NOT EXISTS events_db.app_events (
    event_timestamp DateTime64(3, 'UTC') DEFAULT now64(),
    package_id String,
    event_name String,
    payload String,
    device_id String
) ENGINE = MergeTree()
ORDER BY (package_id, event_timestamp);
```

---

## 4. Required Seeds

To test the system, ensure you have at least one record in the configuration tables:

```sql
-- Valid App Package
INSERT INTO tbl_apps (app_package_id, app_package_secret) VALUES 
('com.example.myapp', '7061636b6167655f7365637265745f3132333435363738393031323334353637');

-- Valid Route
INSERT INTO tbl_event_routes (er_api_path, er_kafka_topic, ch_db_name, ch_table_name, er_required_fields) VALUES 
('/v1/events', 'app-events-topic', 'events_db', 'app_events', ['event_name', 'device_id']);
```
