# Document 3: Detailed Steps and Walkthrough

## 1. Goal Of This Walkthrough

This document explains how to understand, run, and mentally trace the project from request entry to database insertion.

## 2. Before You Start

Make sure the following external systems are available:

- AWS Secrets Manager
- Kafka or AWS MSK cluster
- ClickHouse server
- ClickHouse tables used for configuration and event storage

The application assumes a secret named `in-app-events-credentials` already exists.

## 3. Project Walkthrough By Folder

### Root folder

- `README.md`
  - short architecture summary
- `docker-compose.yml`
  - starts producer and consumer containers
- `Doc/`
  - project documentation

### `producer/`

This folder contains the HTTP entrypoint of the pipeline.

- `src/app.js`
  - bootstraps Fastify, ClickHouse config loading, Kafka producer connection, and route registration
- `src/routes/eventRoutes.js`
  - defines the public endpoints
- `src/controllers/eventController.js`
  - applies route logic and Kafka enqueueing
- `src/middlewares/isRequestValid.js`
  - validates headers and decrypts payloads
- `src/utils/index.js`
  - common helpers for encryption, validation, and producer-side buffering
- `test-event.js`
  - local script for sending an encrypted test event

### `consumer/`

This folder contains the Kafka-to-ClickHouse worker.

- `src/consumer.js`
  - startup sequence, topic subscription, buffering, and flush logic
- `src/secrets.js`
  - AWS secret loading helper

## 4. End-To-End Runtime Flow

### Step 1: Producer starts

When `producer/src/app.js` runs, it:

1. sets timezone to UTC
2. creates the Fastify server
3. loads secrets from AWS Secrets Manager
4. creates a ClickHouse client
5. fetches package secret mappings from `tbl_apps`
6. fetches route mappings from `tbl_event_routes`
7. builds a Kafka producer using AWS IAM authentication
8. registers routes and parsers
9. starts listening on port `3004`

### Step 2: Consumer starts

When `consumer/src/consumer.js` runs, it:

1. loads secrets from AWS Secrets Manager
2. creates a ClickHouse client
3. fetches route mappings from `tbl_event_routes`
4. adds fallback routes for unmatched cases
5. creates a Kafka consumer
6. connects to Kafka
7. subscribes to topics matching `event-*`
8. starts buffering and flushing to ClickHouse

## 5. HTTP Request Walkthrough

The main producer endpoint is:

```http
POST /event/:route
```

Example:

```text
POST /event/v1/events
```

### Required headers

- `x-package-id`
- `x-ts`

### Header purpose

- `x-package-id`
  - identifies the sending app package
- `x-ts`
  - provides the request timestamp and is checked against a 5-minute timeout window

### What happens inside `isRequestValid`

1. the middleware checks if `x-package-id` exists
2. it checks if `x-ts` exists
3. it verifies the request timestamp is not stale
4. it looks up the app secret using the package ID
5. if a package secret exists, it tries to decrypt the request body
6. the parsed object is attached to `request.payloadJson`

If any required header is missing, the API returns `400`.

## 6. Route Resolution Walkthrough

After validation, `handleEventRoute` in `producer/src/controllers/eventController.js` runs.

It performs these checks:

1. reads the route name from `request.params.route`
2. loads the route definition from `configManager`
3. if the route does not exist:
   sends an event to Kafka topic `event-unregistered-route`
4. if the package secret is missing:
   sends an event to Kafka topic `event-unlisted-package-id`
5. validates required fields using the route metadata
6. pushes the valid event to the route's Kafka topic
7. returns HTTP `202`

This means the API is designed to accept processing asynchronously rather than waiting for a database insert.

## 7. Producer Buffering Walkthrough

Inside `producer/src/utils/index.js`, outgoing Kafka messages are buffered before they are sent.

### Why buffering is used

- reduces the number of Kafka produce operations
- improves throughput under frequent event traffic
- allows small bursts to be grouped efficiently

### Buffer behavior

- batch size threshold: `1000`
- linger time: `100 ms`

An event is stored in an in-memory topic buffer. It is flushed when:

- the buffer reaches 1000 messages
- or the linger timer expires

Messages are compressed using Snappy before sending.

## 8. Consumer Processing Walkthrough

The consumer subscribes to Kafka topics matching:

```text
/^event-.*$/
```

For each Kafka batch:

1. it verifies ClickHouse is initialized
2. it finds the matching route for the topic
3. it parses each message from JSON
4. it groups rows under `database.table`
5. it increments the total in-memory row count
6. it flushes immediately if the configured batch size is reached

### Flush behavior

`flushBuffer()` writes data using ClickHouse `insert()` with `JSONEachRow`.

If an insert fails:

- rows are added back into the in-memory buffer
- row count is restored
- the next flush attempt will retry them

## 9. Configuration Walkthrough

This project depends on configuration stored outside the code.

### AWS secret source

Both services load a secret named:

```text
in-app-events-credentials
```

### ClickHouse configuration tables

The producer and consumer rely on:

- `tbl_apps`
  - contains app package IDs and shared secrets
- `tbl_event_routes`
  - contains API route names, Kafka topics, ClickHouse database names, table names, and required fields

### Why this is useful

- new routes can be introduced without code changes
- app credentials can be rotated centrally
- producer and consumer stay aligned through shared database-backed config

## 10. Local Run Steps

### Option 1: Docker Compose

From the project root:

```bash
docker-compose up --build
```

This builds:

- the producer container
- the consumer container

You still need to provide the runtime environment or secret access expected by the containers.

### Option 2: Run services individually

Producer:

```bash
cd producer
npm install
npm start
```

Consumer:

```bash
cd consumer
npm install
npm start
```

## 11. Test Event Walkthrough

The file `producer/test-event.js` demonstrates how a client can send an encrypted request.

It:

1. defines a package secret and package ID
2. encrypts a JSON payload using AES-256-CBC
3. sets required headers
4. sends the request to `localhost:3004/event/v1/events`

This is useful for understanding the expected request format.

## 12. How To Explain This Project In Simple Words

If you need to present this project during a demo or viva, you can describe it like this:

"This project is a backend event pipeline. A producer API receives in-app events from clients, validates and routes them, and publishes them to Kafka. A separate consumer service reads those Kafka messages in batches and stores them in ClickHouse for analytics. Configuration and secrets are loaded dynamically from ClickHouse and AWS Secrets Manager, which makes the system easier to scale and maintain."

## 13. Notes On Missing Pieces In This Repository

These are important to mention while presenting the project:

- there is no frontend application in this repo
- there is no direct Grafana dashboard configuration in this repo
- Kafka, ClickHouse, and AWS Secrets Manager are expected to exist externally
- table creation scripts are not included in the files currently present

## 14. Suggested Demo Sequence

If you want to demonstrate the project step by step:

1. explain the architecture diagram
2. show `producer/src/app.js` and the `/event/:route` endpoint
3. show `producer/src/middlewares/isRequestValid.js`
4. show `producer/src/utils/index.js` for batching and encryption
5. show `consumer/src/consumer.js` for Kafka subscription and ClickHouse insertion
6. run `producer/test-event.js` or send a sample request
7. explain how the consumer flushes data into ClickHouse
8. mention how Grafana can be layered on top of ClickHouse externally for dashboards

## 15. Summary

The project works as a configurable ingestion backbone for in-app events. The producer accepts and validates events, Kafka decouples traffic, and the consumer performs efficient database writes. Once you understand the producer startup, request validation, route lookup, Kafka publish, consumer batch processing, and ClickHouse flush flow, the entire repository becomes straightforward to explain and maintain.
