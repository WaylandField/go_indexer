docker run -d \
  --name clickhouse-server \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=password \
  -p 8123:8123 \
  -p 9000:9000 \
  -p 9009:9009 \
  clickhouse/clickhouse-server