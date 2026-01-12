package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bcindex/internal/application"
	"bcindex/internal/config"
	"bcindex/internal/infrastructure/ethrpc"
	"bcindex/internal/infrastructure/sqlite"
	"bcindex/internal/interfaces/httpapi"
)

var (
	version   = "dev"
	commit    = "none"
	buildTime = "unknown"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	repo, err := sqlite.NewRepository(cfg.DBPath)
	if err != nil {
		log.Fatalf("db error: %v", err)
	}

	rpcClient, err := ethrpc.NewClient(ethrpc.Config{
		URL:     cfg.RPCURL,
		Address: cfg.ContractAddress,
		Topic0:  cfg.Topic0,
	})
	if err != nil {
		log.Fatalf("rpc error: %v", err)
	}

	metrics := httpapi.NewMetrics()
	if last, ok, err := repo.LastProcessedBlock(context.Background()); err == nil && ok {
		metrics.SetLastProcessed(last)
	}

	httpServer, err := httpapi.NewServer(cfg, repo, rpcClient, metrics, httpapi.BuildInfo{
		Version:   version,
		Commit:    commit,
		BuildTime: buildTime,
	})
	if err != nil {
		log.Fatalf("http server error: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	indexerObserver := httpServer.MetricsObserver()
	indexer, err := application.NewIndexer(rpcClient, repo, repo, indexerObserver, application.IndexerConfig{
		StartBlock:    cfg.StartBlock,
		Confirmations: cfg.Confirmations,
		PollInterval:  cfg.PollInterval,
		BatchSize:     cfg.BatchSize,
	})
	if err != nil {
		log.Fatalf("indexer error: %v", err)
	}

	go func() {
		log.Printf("http server listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(ctx, cfg.HTTPAddr); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("http server error: %v", err)
			cancel()
		}
	}()

	log.Printf("starting indexer: rpc=%s start=%d confirmations=%d batch=%d", cfg.RPCURL, cfg.StartBlock, cfg.Confirmations, cfg.BatchSize)
	if err := indexer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Printf("indexer stopped: %v", err)
	}
}
