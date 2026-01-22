package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	RPCURL            string
	DBDSN             string
	StateDBDSN        string
	ClickhouseDSN     string
	HTTPAddr          string
	RedisAddr         string
	OtelEndpoint      string
	ContractAddress   string
	Topic0            string
	StartBlock        uint64
	Confirmations     uint64
	BatchSize         uint64
	LogFetchChunkSize uint64
	LogFetchWorkers   int
	PollInterval      time.Duration
	KafkaBrokers      []string
	KafkaTopicPrefix  string
	KafkaGroupID      string
	ChainIDs          []uint64
}

type EnvSource interface {
	Lookup(key string) (string, bool)
}

type EnvMap map[string]string

func (e EnvMap) Lookup(key string) (string, bool) {
	value, ok := e[key]
	return value, ok
}

func FromEnviron() EnvSource {
	env := make(EnvMap)
	for _, entry := range os.Environ() {
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			continue
		}
		env[parts[0]] = parts[1]
	}
	return env
}

func Load(source EnvSource) (Config, error) {
	if source == nil {
		return Config{}, errors.New("env source is required")
	}

	rpcURL, ok := source.Lookup("RPC_URL")
	if !ok || rpcURL == "" {
		return Config{}, errors.New("RPC_URL is required")
	}

	startBlock, err := parseUintEnv(source, "START_BLOCK", 0)
	if err != nil {
		return Config{}, err
	}
	confirmations, err := parseUintEnv(source, "CONFIRMATIONS", 0)
	if err != nil {
		return Config{}, err
	}
	batchSize, err := parseUintEnv(source, "BATCH_SIZE", 1000)
	if err != nil {
		return Config{}, err
	}
	logFetchChunkSize, err := parseUintEnv(source, "LOG_FETCH_CHUNK_SIZE", 0)
	if err != nil {
		return Config{}, err
	}
	logFetchWorkersRaw, err := parseUintEnv(source, "LOG_FETCH_WORKERS", 0)
	if err != nil {
		return Config{}, err
	}
	logFetchWorkers := int(logFetchWorkersRaw)

	pollInterval := 5 * time.Second
	if raw, ok := source.Lookup("POLL_INTERVAL"); ok && raw != "" {
		duration, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid POLL_INTERVAL: %w", err)
		}
		pollInterval = duration
	}

	dbDSN, ok := source.Lookup("DB_DSN")
	if !ok || strings.TrimSpace(dbDSN) == "" {
		dbDSN = "root:@tcp(127.0.0.1:3306)/bcindex?parseTime=true&multiStatements=true"
	}

	stateDBDSN := dbDSN
	if raw, ok := source.Lookup("STATE_DB_DSN"); ok && strings.TrimSpace(raw) != "" {
		stateDBDSN = raw
	}

	clickhouseDSN, ok := source.Lookup("CLICKHOUSE_DSN")
	if !ok || strings.TrimSpace(clickhouseDSN) == "" {
		clickhouseDSN = "clickhouse://127.0.0.1:9000?database=bcindex"
	}

	httpAddr := ":8080"
	if raw, ok := source.Lookup("HTTP_ADDR"); ok && raw != "" {
		httpAddr = raw
	}

	redisAddr := "127.0.0.1:6379"
	if raw, ok := source.Lookup("REDIS_ADDR"); ok {
		redisAddr = strings.TrimSpace(raw)
	}

	otelEndpoint, _ := source.Lookup("OTEL_EXPORTER_OTLP_ENDPOINT")
	otelEndpoint = strings.TrimSpace(otelEndpoint)

	contractAddress, _ := source.Lookup("CONTRACT_ADDRESS")
	topic0, _ := source.Lookup("TOPIC0")

	kafkaBrokers, err := parseList(source, "KAFKA_BROKERS", "localhost:9092")
	if err != nil {
		return Config{}, err
	}
	kafkaTopicPrefix, ok := source.Lookup("KAFKA_TOPIC_PREFIX")
	if !ok || kafkaTopicPrefix == "" {
		kafkaTopicPrefix = "bcindex-logs"
	}
	kafkaGroupID, ok := source.Lookup("KAFKA_GROUP_ID")
	if !ok || kafkaGroupID == "" {
		kafkaGroupID = "bcindex-compute"
	}
	chainIDs, err := parseUintList(source, "CHAIN_IDS")
	if err != nil {
		return Config{}, err
	}

	return Config{
		RPCURL:            rpcURL,
		DBDSN:             dbDSN,
		StateDBDSN:        stateDBDSN,
		ClickhouseDSN:     clickhouseDSN,
		HTTPAddr:          httpAddr,
		RedisAddr:         redisAddr,
		OtelEndpoint:      otelEndpoint,
		ContractAddress:   contractAddress,
		Topic0:            topic0,
		StartBlock:        startBlock,
		Confirmations:     confirmations,
		BatchSize:         batchSize,
		LogFetchChunkSize: logFetchChunkSize,
		LogFetchWorkers:   logFetchWorkers,
		PollInterval:      pollInterval,
		KafkaBrokers:      kafkaBrokers,
		KafkaTopicPrefix:  kafkaTopicPrefix,
		KafkaGroupID:      kafkaGroupID,
		ChainIDs:          chainIDs,
	}, nil
}

func parseUintEnv(source EnvSource, key string, defaultValue uint64) (uint64, error) {
	raw, ok := source.Lookup(key)
	if !ok || raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return value, nil
}

func parseList(source EnvSource, key string, defaultValue string) ([]string, error) {
	raw, ok := source.Lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		raw = defaultValue
	}
	items := strings.Split(raw, ",")
	var values []string
	for _, item := range items {
		value := strings.TrimSpace(item)
		if value == "" {
			continue
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%s is required", key)
	}
	return values, nil
}

func parseUintList(source EnvSource, key string) ([]uint64, error) {
	raw, ok := source.Lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	items := strings.Split(raw, ",")
	values := make([]uint64, 0, len(items))
	for _, item := range items {
		value := strings.TrimSpace(item)
		if value == "" {
			continue
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", key, err)
		}
		values = append(values, parsed)
	}
	return values, nil
}
