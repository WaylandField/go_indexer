# bcindex

bcindex 是一个面向生产环境的区块链索引服务：从 RPC 可靠拉取链上数据（含合约日志、区块、交易、回执等），通过 Kafka 将“采集/排序”与“计算/落库/查询”解耦，实现可扩展、可回溯、可观测的数据管道，并对外提供轻量 HTTP API 供检索与运维可视化。

### 架构总览

该项目采用“Ordering Streaming + Compute Streaming”的双流水线设计，将确定性排序与计算存储拆分为两个可独立扩容/回放的阶段：

```
RPC 节点
  │  拉取区块/交易/Receipt/Logs（处理确认数、重组等）
  ▼
Ordering Stream (cmd/ordering)
  │  规范化消息 + 事件顺序 + 写入 Kafka（TopicPrefix-<chain_id>）
  ▼
Kafka
  ▼
Compute Stream (cmd/computing)
  │  消费消息批处理/落库/提交 offset
  ├─ MySQL      : 结构化状态（blocks/transactions/receipts/balances/处理进度）
  ├─ ClickHouse : 高吞吐日志（logs）
  ▼
HTTP API (查询 + 运维观测)
```

这种拆分的核心价值在于：Ordering 层专注“从不可靠链源到确定性事件流”的问题；Compute 层专注“从事件流到可查询存储”的问题。两者通过 Kafka 解耦，便于水平扩展、故障隔离与离线回放（reindex/backfill）。

### 关键能力

- 多链支持：按 chain_id 切分 Topic（默认 `bcindex-logs-<chain_id>`），同一套服务可索引多条链。
- 可回放：通过重置处理进度触发回补，适用于新增字段、修复 bug、补历史区间等场景。
- 存储分层：MySQL 负责强结构化、强约束的状态；ClickHouse 承担高写入/高查询的日志分析负载。
- 面向运维：内置 `/healthz`、`/readyz`、`/metrics`、`/state`、`/version` 等端点，覆盖探活、依赖检查、指标与配置快照。

### 一致性与链重组（Reorg）处理思路

区块链数据天然存在“最终一致性”，索引系统需要处理确认数（confirmations）与重组（reorg）：

- Ordering 阶段以“确认数 + 轮询”控制可见性，降低重组概率带来的回滚成本。
- 通过向下游广播 `reorg` 类型消息，Compute 阶段可以按链高度回滚/重算（项目内已定义 `reorg` 消息类型）。
- Compute 阶段按批次落库并提交 Kafka offset，配合处理进度水位（last processed block）实现可恢复、可回放。

### HTTP API（Compute）

- `GET /healthz`：存活探测。
- `GET /readyz`：就绪探测（DB 可用 + RPC 可达）。
- `GET /logs`：按 `chain_id/address/tx_hash/from_block/to_block/limit` 查询日志（存储于 ClickHouse）。
- `GET /transactions`：按过滤条件查询交易（存储于 MySQL）。
- `GET /balances`：按地址查询余额快照（存储于 MySQL）。
- `GET /blocks`：查询已索引区块范围与水位。
- `GET /filters`：返回当前合约/Topic 过滤配置。
- `GET /state`：返回处理水位与运行配置快照。
- `GET /metrics`：Prometheus 文本格式指标。
- `GET /version`：构建信息（version/commit/buildTime）。
- `POST /reindex?from_block=<N>&chain_id=<id>`：将处理进度回退到 `N-1` 触发回补；`from_block=0` 表示清空进度。

### 代码组织（DDD）

- `internal/domain`：实体、值对象、领域规则（不依赖基础设施/传输层）。
- `internal/application`：用例编排、DTO、批处理与映射。
- `internal/infrastructure`：MySQL/ClickHouse/Kafka/RPC 等适配器。
- `internal/interfaces`：HTTP API 等对外接口。
- `cmd/ordering`、`cmd/computing`：两个可独立运行的二进制入口。

### 快速开始（本地）

1) 启动依赖：Kafka、MySQL、ClickHouse（以及可选 Redis、OTEL Collector）。

2) 配置环境变量（至少需要 `RPC_URL`）：

```bash
export RPC_URL="http://127.0.0.1:8545"
export CHAIN_IDS="31337"
```

3) 运行两条流水线：

```bash
make ordering
make compute
```

4) 使用 Hardhat 产生本地日志（可选）：

```bash
yarn chain
yarn deploy
yarn emit
```

5) 验证接口：

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl "http://127.0.0.1:8080/logs?limit=50"
```

### 配置说明（环境变量）

- `RPC_URL`：必填，链 RPC 入口。
- `CHAIN_IDS`：Compute 必填（逗号分隔），用于订阅对应 `chain_id` 的 Kafka Topic，例如 `31337` 或 `1,56,137`。
- `DB_DSN`：Compute/MySQL DSN，默认 `root:@tcp(127.0.0.1:3306)/bcindex?parseTime=true&multiStatements=true`。
- `STATE_DB_DSN`：Ordering 状态库 DSN（未设置时复用 `DB_DSN`）。
- `CLICKHOUSE_DSN`：默认 `clickhouse://127.0.0.1:9000?database=bcindex`。
- `HTTP_ADDR`：HTTP 监听地址，默认 `:8080`。
- `KAFKA_BROKERS`：默认 `localhost:9092`（逗号分隔）。
- `KAFKA_TOPIC_PREFIX`：默认 `bcindex-logs`。
- `KAFKA_GROUP_ID`：Compute 消费组，默认 `bcindex-compute`。
- `START_BLOCK`、`CONFIRMATIONS`、`BATCH_SIZE`、`POLL_INTERVAL`：索引起点、确认数、批大小与轮询/flush 间隔。
- `CONTRACT_ADDRESS`、`TOPIC0`：合约/事件过滤（按需配置）。
