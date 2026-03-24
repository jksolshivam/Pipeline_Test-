# Document 2: Technical Documentation

## 1. Repository Structure

```text
in-app-events-v2/
|-- consumer/
|   |-- src/
|   |   |-- consumer.js
|   |   `-- secrets.js
|   |-- Dockerfile
|   |-- package.json
|   `-- README.md
|-- producer/
|   |-- src/
|   |   |-- app.js
|   |   |-- controllers/
|   |   |   `-- eventController.js
|   |   |-- middlewares/
|   |   |   `-- isRequestValid.js
|   |   |-- routes/
|   |   |   `-- eventRoutes.js
|   |   `-- utils/
|   |       |-- index.js
|   |       `-- secrets.js
|   |-- Dockerfile
|   |-- package.json
|   |-- test-event.js
|   `-- README.md
|-- Doc/
|-- docker-compose.yml
`-- README.md
```

## 2. High-Level Architecture

The system is made of two backend services:

### Producer Service

- Runtime: Node.js
- Framework: Fastify
- Role: receives HTTP requests and publishes valid events to Kafka

### Consumer Service

- Runtime: Node.js
- Framework style: standalone worker process
- Role: subscribes to Kafka topics, buffers records, and inserts them into ClickHouse

### Data Flow

1. A client sends a request to the producer at `POST /event/:route`.
2. The producer validates request headers and decrypts the payload when needed.
3. The producer looks up route metadata from ClickHouse-backed configuration.
4. The producer pushes the event into the mapped Kafka topic.
5. The consumer subscribes to matching Kafka topics.
6. The consumer batches events by destination table.
7. The consumer inserts batches into ClickHouse using `JSONEachRow`.

## 3. Producer Internals

Main entry point: `producer/src/app.js`

### Responsibilities

- starts the Fastify server on port `3004`
- loads secrets from AWS Secrets Manager
- creates a ClickHouse client
- reads app package secrets from `tbl_apps`
- reads route definitions from `tbl_event_routes`
- creates a Kafka producer using IAM-based SASL authentication
- registers routes and request parsers
- encrypts outbound responses when a package secret is present

### Important modules

- `producer/src/routes/eventRoutes.js`
  - registers `/`, `/health`, and `/event/:route`
- `producer/src/controllers/eventController.js`
  - handles route resolution and event enqueueing
- `producer/src/middlewares/isRequestValid.js`
  - checks required headers
  - validates request timestamp freshness
  - decrypts payload data if a package secret exists
- `producer/src/utils/index.js`
  - AES encryption/decryption helpers
  - required field validation
  - Kafka message buffering and flush logic
- `producer/src/utils/secrets.js`
  - AWS Secrets Manager loader

### Producer route behavior

- `GET /`
  - returns online status
- `GET /health`
  - used for health checks
- `POST /event/:route`
  - validates request
  - resolves route mapping
  - publishes to Kafka topic
  - writes unmatched traffic to special Kafka topics

### Special producer topics

If the route or package is not recognized, the producer writes to:

- `event-unregistered-route`
- `event-unlisted-package-id`

This is useful for audit and fallback analysis.

## 4. Consumer Internals

Main entry point: `consumer/src/consumer.js`

### Responsibilities

- loads secrets from AWS Secrets Manager
- creates a ClickHouse client
- loads route-to-table configuration from ClickHouse
- creates a Kafka consumer using IAM-based SASL authentication
- subscribes to topics matching `/^event-.*$/`
- buffers rows by target ClickHouse table
- flushes batches based on row count or timer interval
- retries failed flushes by restoring rows to the in-memory buffer

### Consumer route mapping

The consumer reads `tbl_event_routes` from ClickHouse and extends that set with built-in fallback routes:

- `event-unregistered-route -> default.unregistered_routes`
- `event-unlisted-package-id -> default.unlisted_package_ids`

### Batch processing behavior

- records are processed in Kafka `eachBatch`
- payloads are parsed from JSON
- rows are grouped by `database.table`
- flush occurs when:
  - buffered row count reaches `CLICKHOUSE_INSERT_BATCH_SIZE`
  - or timer reaches `CLICKHOUSE_INSERT_INTERVAL_MS`

## 5. Kafka Integration

Both services use:

- `kafkajs`
- `kafkajs-snappy`
- AWS MSK IAM SASL signer

### Producer Kafka usage

- creates a producer with:
  - `allowAutoTopicCreation: false`
  - `idempotent: true`
  - retry support
- buffers messages in memory before sending
- compresses messages with Snappy

### Consumer Kafka usage

- creates a consumer using a configured `groupId`
- subscribes to event topics by regex
- processes messages in batches
- sends heartbeats while iterating messages

## 6. ClickHouse Integration

Both services use `@clickhouse/client`.

### Producer side

ClickHouse is used as a dynamic configuration source:

- `tbl_apps`
  - stores package IDs and package secrets
- `tbl_event_routes`
  - stores API route names
  - mapped Kafka topics
  - destination database and table
  - required payload fields

### Consumer side

ClickHouse is the final destination for event data:

- inserts are done with `JSONEachRow`
- tables are chosen based on the route/topic mapping
- failed inserts are re-buffered for another flush attempt

## 7. Environment and Secret Requirements

The repo expects runtime configuration to come from AWS Secrets Manager under the secret name:

- `in-app-events-credentials`

### Producer-required values

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_CONFIG_DATABASE` optional, defaults to `default`
- `CONFIG_REFRESH_MS` optional
- `KAFKA_BROKERS`
- `KAFKA_CLIENT_ID`

### Consumer-required values

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_CONFIG_DATABASE` optional, defaults to `default`
- `CLICKHOUSE_INSERT_BATCH_SIZE` optional
- `CLICKHOUSE_INSERT_INTERVAL_MS` optional
- `KAFKA_BROKERS`
- `KAFKA_CLIENT_ID`
- `KAFKA_GROUP_ID`
- `AWS_REGION` optional in code, defaults to `us-east-1`

## 8. Docker and Deployment Notes

### Root docker-compose

The root `docker-compose.yml` defines two services:

- `producer`
- `consumer`

It builds each from its own directory and expects environment values or an `env_file` to be supplied externally.

### Producer container

- base image: `node:20-alpine`
- installs `dumb-init`
- exposes port `3004`
- includes a health check against `/health`

### Consumer container

- base image: `node:20-alpine`
- installs only production dependencies
- starts with `npm start`

## 9. Dependencies

### Producer dependencies

- `fastify`
- `kafkajs`
- `kafkajs-snappy`
- `@clickhouse/client`
- `@aws-sdk/client-secrets-manager`
- `aws-msk-iam-sasl-signer-js`

### Consumer dependencies

- `kafkajs`
- `kafkajs-snappy`
- `@clickhouse/client`
- `@aws-sdk/client-secrets-manager`
- `aws-msk-iam-sasl-signer-js`

## 10. Frontend and Grafana Status

### Frontend

No frontend application, static site, or UI asset pipeline is present in this repository.

### Grafana

No Grafana provisioning files, dashboards, or datasource definitions are present in the repository. If Grafana is part of the overall platform, it is managed outside this codebase and would typically read from ClickHouse or another monitoring source.

## 11. Operational Characteristics

### Strengths

- decoupled ingestion and storage
- dynamic route and secret loading
- batched writes for better ClickHouse throughput
- Docker-ready services
- support for managed Kafka authentication

### Current assumptions and constraints

- route configuration must already exist in ClickHouse tables
- secrets must already exist in AWS Secrets Manager
- the consumer uses in-memory buffering, so unflushed data can be lost on abrupt termination
- there is no built-in dashboarding layer in this repo

## 12. Summary

This project is a backend ingestion system built around Fastify, Kafka, ClickHouse, and AWS Secrets Manager. The producer receives and validates events, while the consumer performs buffered ingestion into analytics storage. The codebase is intentionally service-oriented and does not include a frontend or direct Grafana implementation.
