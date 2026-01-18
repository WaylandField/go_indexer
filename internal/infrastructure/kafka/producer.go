package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"bcindex/internal/domain"
	"bcindex/internal/streaming"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	prefix string
}

type ProducerConfig struct {
	Brokers     []string
	TopicPrefix string
}

func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are required")
	}
	if strings.TrimSpace(cfg.TopicPrefix) == "" {
		cfg.TopicPrefix = "bcindex-logs"
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 500 * time.Millisecond,
	}
	return &Producer{writer: writer, prefix: cfg.TopicPrefix}, nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) PublishLogs(ctx context.Context, logs []domain.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	messages := make([]kafka.Message, 0, len(logs))
	for _, log := range logs {
		payload, err := streaming.Encode(streaming.Message{
			Type:        streaming.MessageTypeLog,
			ChainID:     log.ChainID,
			BlockNumber: log.BlockNumber,
			BlockHash:   log.BlockHash,
			TxHash:      log.TxHash,
			LogIndex:    log.LogIndex,
			Address:     log.Address,
			Data:        log.Data,
			Topics:      log.Topics,
			Removed:     log.Removed,
		})
		if err != nil {
			return err
		}
		messages = append(messages, kafka.Message{
			Topic: p.topicForChain(log.ChainID),
			Key:   []byte(log.Address),
			Value: payload,
		})
	}
	return p.writer.WriteMessages(ctx, messages...)
}

func (p *Producer) PublishBlocks(ctx context.Context, blocks []domain.BlockRecord) error {
	if len(blocks) == 0 {
		return nil
	}
	messages := make([]kafka.Message, 0, len(blocks))
	for _, block := range blocks {
		payload, err := streaming.Encode(streaming.Message{
			Type:        streaming.MessageTypeBlock,
			ChainID:     block.ChainID,
			BlockNumber: block.BlockNumber,
			BlockHash:   block.BlockHash,
		})
		if err != nil {
			return err
		}
		messages = append(messages, kafka.Message{
			Topic: p.topicForChain(block.ChainID),
			Key:   []byte(fmt.Sprintf("block:%d", block.BlockNumber)),
			Value: payload,
		})
	}
	return p.writer.WriteMessages(ctx, messages...)
}

func (p *Producer) PublishReorg(ctx context.Context, chainID uint64, fromBlock uint64, reason string) error {
	payload, err := streaming.Encode(streaming.Message{
		Type:      streaming.MessageTypeReorg,
		ChainID:   chainID,
		FromBlock: fromBlock,
		Reason:    reason,
	})
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: p.topicForChain(chainID),
		Key:   []byte("reorg"),
		Value: payload,
	})
}

func (p *Producer) topicForChain(chainID uint64) string {
	return fmt.Sprintf("%s-%d", p.prefix, chainID)
}
