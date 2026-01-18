package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"bcindex/internal/application"
	"bcindex/internal/config"
	"bcindex/internal/infrastructure/ethrpc"
	"bcindex/internal/infrastructure/mysql"
	"bcindex/internal/interfaces/httpapi"
	"bcindex/internal/streaming"

	"github.com/segmentio/kafka-go"
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

	repo, err := mysql.NewRepository(cfg.DBDSN)
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
	if len(cfg.ChainIDs) > 0 {
		var maxProcessed uint64
		for _, chainID := range cfg.ChainIDs {
			if last, ok, err := repo.LastProcessedBlock(context.Background(), chainID); err == nil && ok {
				if last > maxProcessed {
					maxProcessed = last
				}
			}
		}
		if maxProcessed > 0 {
			metrics.SetLastProcessed(maxProcessed)
		}
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

	go func() {
		log.Printf("http server listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(ctx, cfg.HTTPAddr); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("http server error: %v", err)
			cancel()
		}
	}()

	if len(cfg.ChainIDs) == 0 {
		log.Fatalf("CHAIN_IDS is required for compute streaming")
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

	log.Printf("compute streaming started: topics=%d group=%s", len(cfg.ChainIDs), cfg.KafkaGroupID)
	<-ctx.Done()
	for _, reader := range readers {
		_ = reader.Close()
	}
	wg.Wait()
}

func consumeStream(ctx context.Context, reader *kafka.Reader, repo application.ComputeBalanceRepository, metrics *httpapi.Metrics, chainID uint64, cfg config.Config) {
	var (
		messageCount uint64
		logCount     uint64
		blockCount   uint64
		reorgCount   uint64
		lastType     streaming.MessageType
		lastBlock    uint64
		lastTx       string
	)

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			metrics.IncKafkaFetchErr()
			log.Printf("kafka fetch error: %v", err)
			continue
		}

		decoded, err := streaming.Decode(message.Value)
		if err != nil {
			log.Printf("message decode error: %v", err)
			metrics.IncKafkaDecodeErr()
			_ = reader.CommitMessages(ctx, message)
			continue
		}
		if decoded.ChainID != chainID {
			log.Printf("unexpected chain_id %d on topic", decoded.ChainID)
		}

		if err := application.ApplyMessage(ctx, repo, decoded); err != nil {
			log.Printf("apply message error: %v", err)
			metrics.IncKafkaApplyErr()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if err := application.ApplyBalanceForMessage(ctx, repo, decoded, cfg); err != nil {
			log.Printf("balance update error: %v", err)
		}

		messageCount++
		lastType = decoded.Type
		switch decoded.Type {
		case streaming.MessageTypeLog:
			logCount++
			lastBlock = decoded.BlockNumber
			lastTx = decoded.TxHash
		case streaming.MessageTypeBlock:
			blockCount++
			lastBlock = decoded.BlockNumber
		case streaming.MessageTypeReorg:
			reorgCount++
			lastBlock = decoded.FromBlock
		}

		if messageCount%100 == 0 {
			log.Printf("compute stream stats chain=%d messages=%d logs=%d blocks=%d reorgs=%d last_type=%s last_block=%d last_tx=%s", chainID, messageCount, logCount, blockCount, reorgCount, lastType, lastBlock, lastTx)
		}

		metrics.ObserveKafkaMessage(message.Topic, message.Partition, message.Offset, len(message.Value), message.Time)
		updateMetrics(metrics, decoded)
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("kafka commit error: %v", err)
			metrics.IncKafkaCommitErr()
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
