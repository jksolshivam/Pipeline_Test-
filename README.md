# Event Ingestion Pipeline

Production-ready Node.js event ingestion pipeline orchestrating high-throughput data loading into ClickHouse via Kafka.

## Architecture

Client -> Producer API -> Kafka (MSK) -> Consumer Worker -> ClickHouse

### Components

1. **Producer Service**: A Fastify-based Node.js API that accepts `POST /event/:eventType` requests, validates payloads, appends metadata, and publishes messages asynchronously to Kafka. All configuration is stored in `src/app.js`.
2. **Consumer Service**: A Kafka worker that subscribes to incoming data topics using `kafkajs`, processes events iteratively (`eachBatch`), buffers them in memory, and writes robustly to ClickHouse using `@clickhouse/client` optimized `JSONEachRow` batched inserts. All configuration is stored in `src/consumer.js`.

Both services are configured purely via Environment Variables, making them perfect for running locally or inside an AWS VPC.

## Directory Structure

- `producer/`: HTTP API using Fastify. Logic housed in `src/app.js`.
- `consumer/`: Kafka worker pulling batches into ClickHouse. Logic housed in `src/consumer.js`.

## Local Testing

You can spin the services up using Docker:

```bash
docker-compose up --build
```

## Environment Configuration

Both applications depend on environment variables for Configuration. Example configuration:

**Kafka Configuration**
```env
KAFKA_BROKERS=broker-1:9092,broker-2:9092
KAFKA_CLIENT_ID=event-pipeline
```

**ClickHouse Configuration (Consumer Only)**
```env
CLICKHOUSE_HOST=http://my-clickhouse-host:8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=secure_pass
CLICKHOUSE_INSERT_BATCH_SIZE=1000
```

### Publishing an Event

Send a POST request to the producer API:

```bash
curl -X POST http://localhost:3000/event/user_signup \
  -H "Content-Type: application/json" \
  -d '{"userId": 1234, "platform": "android"}'
```
