package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bcindex/internal/application"
	"bcindex/internal/config"
	"bcindex/internal/infrastructure/ethrpc"
	"bcindex/internal/infrastructure/kafka"
	"bcindex/internal/infrastructure/logging"
	"bcindex/internal/infrastructure/mysql"
	"bcindex/internal/infrastructure/telemetry"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		slog.Error("config error", "err", err)
		os.Exit(1)
	}

	logFile := cfg.LogFile
	if logFile == "" {
		logFile = "logs/ordering.log"
	}
	if _, err := logging.Init(logging.Config{
		Level:      cfg.LogLevel,
		File:       logFile,
		MaxSizeMB:  cfg.LogMaxSizeMB,
		MaxBackups: cfg.LogMaxBackups,
	}); err != nil {
		slog.Error("logger init error", "err", err)
	}

	stateRepo, err := mysql.NewRepository(cfg.StateDBDSN)
	if err != nil {
		slog.Error("state db error", "err", err)
		os.Exit(1)
	}

	rpcClient, err := ethrpc.NewClient(ethrpc.Config{
		URL:     cfg.RPCURL,
		Address: cfg.ContractAddress,
		Topic0:  cfg.Topic0,
	})
	if err != nil {
		slog.Error("rpc error", "err", err)
		os.Exit(1)
	}

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:     cfg.KafkaBrokers,
		TopicPrefix: cfg.KafkaTopicPrefix,
	})
	if err != nil {
		slog.Error("kafka error", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	shutdownTracing, err := telemetry.InitTracer(context.Background(), "bcindex-ordering", cfg.OtelEndpoint)
	if err != nil {
		slog.Warn("tracing init error", "err", err)
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdownTracing(ctx); err != nil {
				slog.Warn("tracing shutdown error", "err", err)
			}
		}()
	}

	indexer, err := application.NewIndexer(rpcClient, producer, stateRepo, stateRepo, orderingObserver{}, application.IndexerConfig{
		StartBlock:        cfg.StartBlock,
		Confirmations:     cfg.Confirmations,
		PollInterval:      cfg.PollInterval,
		BatchSize:         cfg.BatchSize,
		LogFetchChunkSize: cfg.LogFetchChunkSize,
		LogFetchWorkers:   cfg.LogFetchWorkers,
	})
	if err != nil {
		slog.Error("indexer error", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.Info("ordering streaming started",
		"rpc", cfg.RPCURL,
		"start", cfg.StartBlock,
		"confirmations", cfg.Confirmations,
		"batch", cfg.BatchSize,
	)
	if err := indexer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		slog.Error("ordering stopped", "err", err)
	}
}

type orderingObserver struct{}

func (orderingObserver) OnLatestBlock(block uint64) {}

func (orderingObserver) OnBatchProcessed(fromBlock, toBlock uint64, logCount int) {
	slog.Info("ordering batch",
		"from", fromBlock,
		"to", toBlock,
		"logs", logCount,
	)
}
