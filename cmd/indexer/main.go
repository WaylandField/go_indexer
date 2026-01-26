package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"bcindex/internal/application"
	"bcindex/internal/config"
	"bcindex/internal/infrastructure/clickhouse"
	"bcindex/internal/infrastructure/ethrpc"
	"bcindex/internal/infrastructure/logging"
	"bcindex/internal/infrastructure/mysql"
	"bcindex/internal/infrastructure/storage"
	"bcindex/internal/infrastructure/telemetry"
	"bcindex/internal/interfaces/httpapi"
	"bcindex/internal/streaming"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	version   = "dev"
	commit    = "none"
	buildTime = "unknown"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		slog.Error("config error", "err", err)
		os.Exit(1)
	}

	logFile := cfg.LogFile
	if logFile == "" {
		logFile = "logs/indexer.log"
	}
	if _, err := logging.Init(logging.Config{
		Level:      cfg.LogLevel,
		File:       logFile,
		MaxSizeMB:  cfg.LogMaxSizeMB,
		MaxBackups: cfg.LogMaxBackups,
	}); err != nil {
		slog.Error("logger init error", "err", err)
	}

	mysqlRepo, err := mysql.NewRepository(cfg.DBDSN)
	if err != nil {
		slog.Error("db error", "err", err)
		os.Exit(1)
	}

	logRepo, err := clickhouse.NewRepository(cfg.ClickhouseDSN)
	if err != nil {
		slog.Error("clickhouse error", "err", err)
		os.Exit(1)
	}

	combinedRepo, err := storage.NewRepository(mysqlRepo, logRepo)
	if err != nil {
		slog.Error("storage error", "err", err)
		os.Exit(1)
	}

	var (
		repo  application.ComputeBalanceRepository = combinedRepo
		store httpapi.LogStore                     = combinedRepo
	)

	shutdownTracing, err := telemetry.InitTracer(context.Background(), "bcindex-compute", cfg.OtelEndpoint)
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

	rpcClient, err := ethrpc.NewClient(ethrpc.Config{
		URL:     cfg.RPCURL,
		Address: cfg.ContractAddress,
		Topic0:  cfg.Topic0,
	})
	if err != nil {
		slog.Error("rpc error", "err", err)
		os.Exit(1)
	}

	metrics := httpapi.NewMetrics()
	if len(cfg.ChainIDs) > 0 {
		var maxProcessed uint64
		for _, chainID := range cfg.ChainIDs {
			if last, ok, err := store.LastProcessedBlock(context.Background(), chainID); err == nil && ok {
				if last > maxProcessed {
					maxProcessed = last
				}
			}
		}
		if maxProcessed > 0 {
			metrics.SetLastProcessed(maxProcessed)
		}
	}

	httpServer, err := httpapi.NewServer(cfg, store, rpcClient, metrics, httpapi.BuildInfo{
		Version:   version,
		Commit:    commit,
		BuildTime: buildTime,
	})
	if err != nil {
		slog.Error("http server error", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		slog.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(ctx, cfg.HTTPAddr); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("http server error", "err", err)
			cancel()
		}
	}()

	if len(cfg.ChainIDs) == 0 {
		slog.Error("CHAIN_IDS is required for compute streaming")
		os.Exit(1)
	}

	var wg sync.WaitGroup
	readers := make([]*kafka.Reader, 0, len(cfg.ChainIDs))
	for _, chainID := range cfg.ChainIDs {
		topic := fmt.Sprintf("%s-%d", cfg.KafkaTopicPrefix, chainID)
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  cfg.KafkaBrokers,
			GroupID:  cfg.KafkaGroupID,
			Topic:    topic,
			MinBytes: 1,
			MaxBytes: 10e6,
		})
		readers = append(readers, reader)

		wg.Add(1)
		go func(chain uint64, r *kafka.Reader) {
			defer wg.Done()
			consumeStream(ctx, r, repo, metrics, chain, cfg)
		}(chainID, reader)
	}

	slog.Info("compute streaming started", "topics", len(cfg.ChainIDs), "group", cfg.KafkaGroupID)
	<-ctx.Done()
	for _, reader := range readers {
		_ = reader.Close()
	}
	wg.Wait()
}

func consumeStream(ctx context.Context, reader *kafka.Reader, repo application.ComputeBalanceRepository, metrics *httpapi.Metrics, chainID uint64, cfg config.Config) {
	tracer := otel.Tracer("bcindex/compute")
	batch := application.NewBatch()

	// Use a flush interval to ensure low-latency indexing even with low traffic
	flushInterval := 500 * time.Millisecond
	if cfg.PollInterval > 0 {
		flushInterval = cfg.PollInterval
	}

	for {
		// Use a timeout to allow periodic flushing
		fetchCtx, cancel := context.WithTimeout(ctx, flushInterval)
		message, err := reader.FetchMessage(fetchCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Timeout reached, flush any pending messages
				if batch.Len() > 0 {
					if err := batch.Flush(ctx, repo, reader); err != nil {
						slog.Error("batch flush error (timeout)", "err", err)
						// TODO: Add retry logic or backoff
					}
				}
				continue
			}
			if errors.Is(err, context.Canceled) {
				return
			}
			metrics.IncKafkaFetchErr()
			slog.Error("kafka fetch error", "err", err)
			time.Sleep(100 * time.Millisecond) // Avoid tight loop on error
			continue
		}

		decoded, err := streaming.Decode(message.Value)
		if err != nil {
			slog.Warn("message decode error", "err", err)
			metrics.IncKafkaDecodeErr()
			_ = reader.CommitMessages(ctx, message)
			continue
		}
		if decoded.ChainID != chainID {
			slog.Warn("unexpected chain id on topic", "chain_id", decoded.ChainID)
		}

		messageCtx := telemetry.ExtractKafkaHeaders(ctx, message.Headers)
		if !trace.SpanContextFromContext(messageCtx).IsValid() && decoded.TraceID != "" {
			if ctxWithTrace, ok := telemetry.ContextWithTraceID(messageCtx, decoded.TraceID); ok {
				messageCtx = ctxWithTrace
			}
		}
		messageCtx, span := tracer.Start(messageCtx, "compute.process_message", trace.WithSpanKind(trace.SpanKindConsumer))
		span.SetAttributes(
			attribute.String("message.type", string(decoded.Type)),
			attribute.Int64("chain.id", int64(decoded.ChainID)),
		)
		if decoded.BlockNumber != 0 {
			span.SetAttributes(attribute.Int64("block.number", int64(decoded.BlockNumber)))
		}
		if decoded.TxHash != "" {
			span.SetAttributes(attribute.String("tx.hash", decoded.TxHash))
		}

		// Handle Reorgs immediately (bypass batch)
		if decoded.Type == streaming.MessageTypeReorg {
			// Flush pending batch first to ensure order
			if batch.Len() > 0 {
				if err := batch.Flush(messageCtx, repo, reader); err != nil {
					slog.Error("pre-reorg flush error", "err", err)
					span.RecordError(err)
					span.End()
					continue // Retry reorg later? Or we might be stuck. For now, retry loop implicitly.
				}
			}

			if err := application.ApplyMessage(messageCtx, repo, decoded); err != nil {
				slog.Error("reorg apply error", "err", err)
				metrics.IncKafkaApplyErr()
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			} else {
				if err := reader.CommitMessages(ctx, message); err != nil {
					slog.Error("reorg commit error", "err", err)
				}
				// Reset metrics for reorg
				updateMetrics(metrics, decoded)
			}
			span.End()
			continue
		}

		// Add to Batch
		batch.Add(decoded, message)

		// Apply Balance Updates (Sequential)
		// Note: We apply balances immediately, but offsets are committed in batch.
		// If we crash, we might re-apply balances. Operations must be idempotent.
		// AddBalanceDelta is NOT idempotent (it adds).
		// TODO: To make it idempotent, we need to track applied log indices or use a versioned state.
		// For now, we accept the risk of double-counting on crash (rare) or need a better balance applier.
		if err := application.ApplyBalanceForMessage(messageCtx, repo, decoded, cfg); err != nil {
			slog.Warn("balance update error", "err", err)
		}
		span.End()

		updateMetrics(metrics, decoded)

		// Flush if batch full
		if batch.Len() >= int(cfg.BatchSize) {
			if err := batch.Flush(ctx, repo, reader); err != nil {
				slog.Error("batch flush error (size)", "err", err)
				// Retry or backoff logic needed here
			}
		}
	}
}

func updateMetrics(metrics *httpapi.Metrics, msg streaming.Message) {
	if metrics == nil {
		return
	}
	switch msg.Type {
	case streaming.MessageTypeLog:
		metrics.OnBatchProcessed(msg.BlockNumber, msg.BlockNumber, 1)
	case streaming.MessageTypeBlock:
		metrics.SetLastProcessed(msg.BlockNumber)
	case streaming.MessageTypeReorg:
		if msg.FromBlock == 0 {
			metrics.SetLastProcessed(0)
		} else {
			metrics.SetLastProcessed(msg.FromBlock - 1)
		}
	}
}
