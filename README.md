# bcindex

bcindex is a production-oriented blockchain indexing service. It reliably pulls on-chain data from RPC (including contract logs, blocks, transactions, receipts, etc.), decouples “ingestion/ordering” from “compute/persistence/query” via Kafka, builds a scalable and replayable data pipeline with strong operational visibility, and exposes a lightweight HTTP API for querying and operations.

### Architecture Overview

This project uses a dual-pipeline design (Ordering Streaming + Compute Streaming) that splits deterministic ordering from compute/storage into two stages that can be scaled and replayed independently:

```
RPC node
  │  Fetch blocks/transactions/receipts/logs (confirmations, reorgs, etc.)
  ▼
Ordering Stream (cmd/ordering)
  │  Normalize messages + enforce event order + publish to Kafka (TopicPrefix-<chain_id>)
  ▼
Kafka
  ▼
Compute Stream (cmd/computing)
  │  Consume messages in batches / persist / commit offsets
  ├─ MySQL      : structured state (blocks/transactions/receipts/balances/progress)
  ├─ ClickHouse : high-throughput logs (logs)
  ▼
HTTP API (query + operations)
```

The key value of this split is that the Ordering layer focuses on turning an unreliable chain source into a deterministic event stream, while the Compute layer focuses on turning that stream into queryable storage. Kafka decouples the two, making horizontal scaling, fault isolation, and offline replay (reindex/backfill) straightforward.

### Key Capabilities

- Multi-chain support: topics are segmented by `chain_id` (default `bcindex-logs-<chain_id>`), so one deployment can index multiple chains.
- Replayable indexing: reset the processed watermark to backfill history, useful for schema changes, bug fixes, or historical sync.
- Storage specialization: MySQL holds structured, strongly constrained state; ClickHouse handles high-ingest/high-query log analytics.
- Ops-first: built-in endpoints like `/healthz`, `/readyz`, `/metrics`, `/state`, `/version` for liveness, dependency readiness, metrics, and config/state snapshots.

### Consistency and Reorg Handling

Blockchain data is inherently eventually consistent. An indexer must handle confirmations and chain reorganizations (reorgs):

- The Ordering stage controls visibility using confirmations plus polling, reducing rollback cost from reorgs.
- A `reorg` message type can be broadcast downstream so the Compute stage can rewind/recompute from a given height (the project defines a `reorg` message type).
- The Compute stage persists in batches and commits Kafka offsets, using a processing watermark (last processed block) for recovery and replay.

### HTTP API (Compute)

- `GET /healthz`: liveness probe.
- `GET /readyz`: readiness probe (DB reachable + RPC reachable).
- `GET /logs`: query logs by `chain_id/address/tx_hash/from_block/to_block/limit` (stored in ClickHouse).
- `GET /transactions`: query transactions with filters (stored in MySQL).
- `GET /balances`: query balance snapshots by address (stored in MySQL).
- `GET /blocks`: query indexed block range and watermark.
- `GET /filters`: return active contract/topic filter configuration.
- `GET /state`: return processing watermark and runtime config snapshot.
- `GET /metrics`: Prometheus text-format metrics.
- `GET /version`: build metadata (version/commit/buildTime).
- `POST /reindex?from_block=<N>&chain_id=<id>`: rewind the processing watermark to `N-1` to trigger backfill; `from_block=0` clears progress.

### Code Organization (DDD)

- `internal/domain`: entities, value objects, domain rules (no dependency on infrastructure/transport).
- `internal/application`: use-case orchestration, DTOs, batching, and mapping.
- `internal/infrastructure`: adapters for MySQL/ClickHouse/Kafka/RPC, etc.
- `internal/interfaces`: external interfaces such as the HTTP API.
- `cmd/ordering`, `cmd/computing`: two independently runnable binaries.

### Quick Start (Local)

1) Start dependencies: Kafka, MySQL, ClickHouse (optionally Redis and an OTEL Collector).

2) Configure environment variables (at minimum `RPC_URL`):

```bash
export RPC_URL="http://127.0.0.1:8545"
export CHAIN_IDS="31337"
```

3) Run both pipelines:

```bash
make ordering
make compute
```

4) Generate logs locally with Hardhat (optional):

```bash
yarn chain
yarn deploy
yarn emit
```

5) Verify endpoints:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl "http://127.0.0.1:8080/logs?limit=50"
```

### Configuration (Environment Variables)

- `RPC_URL`: required. RPC endpoint.
- `CHAIN_IDS`: required for Compute (comma-separated). Used to subscribe to Kafka topics per `chain_id`, e.g. `31337` or `1,56,137`.
- `DB_DSN`: Compute/MySQL DSN. Default: `root:@tcp(127.0.0.1:3306)/bcindex?parseTime=true&multiStatements=true`.
- `STATE_DB_DSN`: Ordering state DB DSN (defaults to `DB_DSN` if unset).
- `CLICKHOUSE_DSN`: default `clickhouse://127.0.0.1:9000?database=bcindex`.
- `HTTP_ADDR`: HTTP listen address. Default `:8080`.
- `KAFKA_BROKERS`: default `localhost:9092` (comma-separated).
- `KAFKA_TOPIC_PREFIX`: default `bcindex-logs`.
- `KAFKA_GROUP_ID`: Compute consumer group. Default `bcindex-compute`.
- `START_BLOCK`, `CONFIRMATIONS`, `BATCH_SIZE`, `POLL_INTERVAL`: start height, confirmations, batch size, and poll/flush interval.
- `CONTRACT_ADDRESS`, `TOPIC0`: contract/event filters (optional).
