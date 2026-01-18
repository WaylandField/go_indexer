curl http://127.0.0.1:8080/healthz

curl http://127.0.0.1:8080/readyz

curl "http://127.0.0.1:8080/logs?address=0xabc...&from_block=0&to_block=500&limit=50"

curl "http://127.0.0.1:8080/transactions?tx_hash=0xdeadbeef...&limit=10"

curl http://127.0.0.1:8080/state

curl http://127.0.0.1:8080/blocks

curl http://127.0.0.1:8080/metrics

curl -X POST "http://127.0.0.1:8080/reindex?from_block=0"

curl "http://127.0.0.1:8080//balances"