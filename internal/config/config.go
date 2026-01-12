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
	RPCURL          string
	DBPath          string
	HTTPAddr        string
	ContractAddress string
	Topic0          string
	StartBlock      uint64
	Confirmations   uint64
	BatchSize       uint64
	PollInterval    time.Duration
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

	pollInterval := 5 * time.Second
	if raw, ok := source.Lookup("POLL_INTERVAL"); ok && raw != "" {
		duration, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid POLL_INTERVAL: %w", err)
		}
		pollInterval = duration
	}

	dbPath, ok := source.Lookup("DB_PATH")
	if !ok || dbPath == "" {
		dbPath = "indexer.db"
	}

	httpAddr := ":8080"
	if raw, ok := source.Lookup("HTTP_ADDR"); ok && raw != "" {
		httpAddr = raw
	}

	contractAddress, _ := source.Lookup("CONTRACT_ADDRESS")
	topic0, _ := source.Lookup("TOPIC0")

	return Config{
		RPCURL:          rpcURL,
		DBPath:          dbPath,
		HTTPAddr:        httpAddr,
		ContractAddress: contractAddress,
		Topic0:          topic0,
		StartBlock:      startBlock,
		Confirmations:   confirmations,
		BatchSize:       batchSize,
		PollInterval:    pollInterval,
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
