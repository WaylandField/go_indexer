package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bcindex/internal/application"
	"bcindex/internal/config"
	"bcindex/internal/infrastructure/ethrpc"
	"bcindex/internal/infrastructure/kafka"
	"bcindex/internal/infrastructure/mysql"
	"bcindex/internal/infrastructure/telemetry"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	stateRepo, err := mysql.NewRepository(cfg.StateDBDSN)
	if err != nil {
		log.Fatalf("state db error: %v", err)
	}

	rpcClient, err := ethrpc.NewClient(ethrpc.Config{
		URL:     cfg.RPCURL,
		Address: cfg.ContractAddress,
		Topic0:  cfg.Topic0,
	})
	if err != nil {
		log.Fatalf("rpc error: %v", err)
	}

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:     cfg.KafkaBrokers,
		TopicPrefix: cfg.KafkaTopicPrefix,
	})
	if err != nil {
		log.Fatalf("kafka error: %v", err)
	}
	defer producer.Close()

	shutdownTracing, err := telemetry.InitTracer(context.Background(), "bcindex-ordering", cfg.OtelEndpoint)
	if err != nil {
		log.Printf("tracing init error: %v", err)
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdownTracing(ctx); err != nil {
				log.Printf("tracing shutdown error: %v", err)
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
		log.Fatalf("indexer error: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("ordering streaming started: rpc=%s start=%d confirmations=%d batch=%d", cfg.RPCURL, cfg.StartBlock, cfg.Confirmations, cfg.BatchSize)
	if err := indexer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Printf("ordering stopped: %v", err)
	}
}

type orderingObserver struct{}

func (orderingObserver) OnLatestBlock(block uint64) {}

func (orderingObserver) OnBatchProcessed(fromBlock, toBlock uint64, logCount int) {
	log.Printf("ordering batch from=%d to=%d logs=%d", fromBlock, toBlock, logCount)
}
