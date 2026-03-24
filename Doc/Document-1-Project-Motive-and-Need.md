# Document 1: Project Motive and Need

## Project Overview

This project is an in-app event ingestion pipeline built to receive application events over HTTP, move them reliably through Kafka, and persist them into ClickHouse for analytics and downstream reporting. The repository is organized as two Node.js services:

- `producer`: accepts inbound event requests and publishes them to Kafka.
- `consumer`: reads Kafka events in batches and inserts them into ClickHouse.

The overall flow is:

`Client App -> Producer API -> Kafka -> Consumer Worker -> ClickHouse`

## Why This Project Is Needed

Modern applications generate a large number of user and system events such as app opens, signups, purchases, screen visits, and error logs. If these events are written directly into an analytics database from the client or from a single backend endpoint, several problems appear quickly:

- traffic spikes can overload the database
- validation logic becomes hard to manage
- routing different event types to different tables becomes messy
- failures in one component can cause end-to-end data loss
- the system becomes difficult to scale independently

This project addresses those issues by separating event ingestion into specialized stages.

## Core Problem It Solves

The pipeline solves the need for a reliable, scalable, and configurable way to collect in-app events and land them in an analytics store.

It specifically helps with:

- accepting events from different applications through a single API pattern
- validating headers and payload structure before ingestion
- buffering and batching data to reduce write pressure on storage
- decoupling event production from event consumption through Kafka
- dynamically mapping API routes to Kafka topics and ClickHouse tables
- supporting secure secret-based communication using AWS Secrets Manager

## Business Value

This design is useful when a team needs near-real-time event data for:

- product analytics
- customer behavior tracking
- performance monitoring
- experimentation and feature analysis
- compliance or audit trails
- dashboarding and BI workflows

By using Kafka and ClickHouse, the system supports both high event throughput and fast analytical querying.

## Why The Architecture Matters

The project is intentionally split into a producer and a consumer because each side has a different responsibility:

- the producer focuses on request handling, validation, encryption/decryption, route lookup, and publishing
- the consumer focuses on throughput, buffering, retry-friendly batch handling, and database insertion

This separation gives the team:

- better fault isolation
- easier horizontal scaling
- simpler maintenance
- cleaner operational ownership

## Key Components and Their Purpose

### 1. Node.js Producer Server

The producer is a Fastify-based API service. It exposes routes such as `/event/:route`, validates headers like `x-package-id` and `x-ts`, decrypts payloads when a package secret is available, checks required fields, and publishes the event to the Kafka topic associated with the requested route.

### 2. Kafka Integration

Kafka acts as the durable transport layer between ingestion and storage. This removes direct coupling between the API layer and ClickHouse, allowing the system to absorb bursts of traffic and process events asynchronously.

### 3. Node.js Consumer Worker

The consumer subscribes to Kafka topics that match the event naming convention, groups records into batches, and flushes them to ClickHouse in `JSONEachRow` format. This improves insert efficiency and reduces database overhead.

### 4. ClickHouse Configuration and Storage

ClickHouse is used in two ways:

- as the analytics/event storage destination
- as a source of dynamic configuration through tables such as `tbl_apps` and `tbl_event_routes`

This lets the system update routing and app-secret mappings without hardcoding them in source files.

### 5. AWS Secrets Manager

The services load runtime secrets such as Kafka brokers, client IDs, ClickHouse credentials, and consumer group information from AWS Secrets Manager. This keeps sensitive values out of source control.

### 6. Grafana Integration Status

There is no direct Grafana configuration, dashboard JSON, or Grafana API integration checked into this repository. In practical usage, Grafana would usually sit outside this repo and connect to ClickHouse or another metrics source to visualize pipeline health and event trends.

### 7. Frontend Assets Status

This repository does not contain a frontend application, static assets, or browser-side UI code. It is a backend/event-processing project intended to serve mobile apps, SDKs, or other backend clients.

## When This Project Is A Good Fit

This project is a good fit when:

- many applications send event data continuously
- the team needs reliable event delivery without tight database coupling
- event routes and schemas change over time
- analytics data must be queryable quickly
- operations require containerized services and cloud-managed secrets

## Summary

The motive behind this project is to build a dependable event ingestion backbone for in-app analytics. Its need comes from the challenge of handling high-throughput event traffic safely, validating and routing events dynamically, and storing them efficiently for reporting and analysis. The repo is backend-focused and centered on Node.js services, Kafka, ClickHouse, and AWS-managed configuration.
