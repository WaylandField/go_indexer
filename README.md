### Introduction

This service is a Go-based crypto indexer built to consume blockchain contract logs and produce queryable, operationally  transparent data. It is organized around Domain-Driven Design (DDD) and split into two streaming pipelines to separate concerns and scale independently: an Ordering stream (RPC → Kafka) that ensures deterministic event ordering and a Compute stream (Kafka → MySQL + ClickHouse + HTTP API) that persists data and serves queries.

### What the service does

  - Ingests on-chain contract logs from an RPC source.
  - Orders and streams those logs through Kafka for reliable, decoupled processing.
  - Persists structured state into MySQL (blocks, transactions, receipts, balances, state) and high-volume logs into ClickHouse.
  - Exposes a compact HTTP API for health checks, observability, and data access.

### Design highlights

  - DDD layering and boundaries: Domain entities and rules live under internal/domain, application orchestration in internal/application, infrastructure adapters in internal/infrastructure, and transport in internal/interfaces. Domain logic stays clean and independent of persistence or transport concerns.
  - Two-stage streaming architecture: The Ordering stream focuses on sequencing and durability, while the Compute stream focuses on storage and queryability. This clean split makes scaling and debugging easier.
  - Storage specialization: MySQL is used for relational state and ClickHouse for log analytics, matching storage engines to data access patterns.
  - Operational visibility built-in: The HTTP API provides /healthz, /readyz, /metrics, /version, and /state for monitoring, plus /logs, /blocks, /filters, and (when available) /transactions for data queries.
  - Reindexing support: A /reindex endpoint allows controlled backfills by resetting the last processed block to a specific height.
  - Local dev parity: A Hardhat-based local chain with deployment and log emission scripts supports realistic development and testing.