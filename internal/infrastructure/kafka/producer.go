package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"bcindex/internal/domain"
	"bcindex/internal/infrastructure/telemetry"
	"bcindex/internal/streaming"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	tracer := otel.Tracer("bcindex/kafka")
	messages := make([]kafka.Message, 0, len(logs))
	spans := make([]trace.Span, 0, len(logs))
	for _, log := range logs {
		traceID, traceIDHex, ok := telemetry.NewTraceID()
		if !ok {
			traceIDHex = ""
		}
		traceCtx := ctx
		if ok {
			if spanCtx, ok := telemetry.NewSpanContext(traceID); ok {
				traceCtx = trace.ContextWithSpanContext(ctx, spanCtx)
			}
		}
		traceCtx, span := tracer.Start(traceCtx, "ordering.publish_log", trace.WithSpanKind(trace.SpanKindProducer))
		span.SetAttributes(
			attribute.Int64("chain.id", int64(log.ChainID)),
			attribute.Int64("block.number", int64(log.BlockNumber)),
			attribute.Int64("log.index", int64(log.LogIndex)),
			attribute.String("tx.hash", log.TxHash),
			attribute.String("address", log.Address),
		)

		payload, err := streaming.Encode(streaming.Message{
			Type:        streaming.MessageTypeLog,
			ChainID:     log.ChainID,
			TraceID:     traceIDHex,
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
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return err
		}
		headers := make([]kafka.Header, 0, 2)
		telemetry.InjectKafkaHeaders(traceCtx, &headers)
		messages = append(messages, kafka.Message{
			Topic:   p.topicForChain(log.ChainID),
			Key:     []byte(log.Address),
			Value:   payload,
			Headers: headers,
		})
		spans = append(spans, span)
	}
	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		for _, span := range spans {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}
	for _, span := range spans {
		span.End()
	}
	return err
}

func (p *Producer) PublishBlocks(ctx context.Context, blocks []domain.BlockRecord) error {
	if len(blocks) == 0 {
		return nil
	}
	tracer := otel.Tracer("bcindex/kafka")
	messages := make([]kafka.Message, 0, len(blocks))
	spans := make([]trace.Span, 0, len(blocks))
	for _, block := range blocks {
		traceID, traceIDHex, ok := telemetry.NewTraceID()
		if !ok {
			traceIDHex = ""
		}
		traceCtx := ctx
		if ok {
			if spanCtx, ok := telemetry.NewSpanContext(traceID); ok {
				traceCtx = trace.ContextWithSpanContext(ctx, spanCtx)
			}
		}
		traceCtx, span := tracer.Start(traceCtx, "ordering.publish_block", trace.WithSpanKind(trace.SpanKindProducer))
		span.SetAttributes(
			attribute.Int64("chain.id", int64(block.ChainID)),
			attribute.Int64("block.number", int64(block.BlockNumber)),
			attribute.String("block.hash", block.BlockHash),
		)

		payload, err := streaming.Encode(streaming.Message{
			Type:          streaming.MessageTypeBlock,
			ChainID:       block.ChainID,
			TraceID:       traceIDHex,
			BlockNumber:   block.BlockNumber,
			BlockHash:     block.BlockHash,
			ParentHash:    block.ParentHash,
			Timestamp:     block.Timestamp,
			BlockGasLimit: block.GasLimit,
			BlockGasUsed:  block.GasUsed,
			TxCount:       block.TxCount,
		})
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return err
		}
		headers := make([]kafka.Header, 0, 2)
		telemetry.InjectKafkaHeaders(traceCtx, &headers)
		messages = append(messages, kafka.Message{
			Topic:   p.topicForChain(block.ChainID),
			Key:     []byte(fmt.Sprintf("block:%d", block.BlockNumber)),
			Value:   payload,
			Headers: headers,
		})
		spans = append(spans, span)
	}
	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		for _, span := range spans {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}
	for _, span := range spans {
		span.End()
	}
	return err
}

func (p *Producer) PublishTransactions(ctx context.Context, transactions []domain.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}
	tracer := otel.Tracer("bcindex/kafka")
	messages := make([]kafka.Message, 0, len(transactions))
	spans := make([]trace.Span, 0, len(transactions))
	for _, tx := range transactions {
		traceID, traceIDHex, ok := telemetry.NewTraceID()
		if !ok {
			traceIDHex = ""
		}
		traceCtx := ctx
		if ok {
			if spanCtx, ok := telemetry.NewSpanContext(traceID); ok {
				traceCtx = trace.ContextWithSpanContext(ctx, spanCtx)
			}
		}
		traceCtx, span := tracer.Start(traceCtx, "ordering.publish_tx", trace.WithSpanKind(trace.SpanKindProducer))
		span.SetAttributes(
			attribute.Int64("chain.id", int64(tx.ChainID)),
			attribute.String("tx.hash", tx.TxHash),
			attribute.Int64("block.number", int64(tx.BlockNumber)),
		)

		payload, err := streaming.Encode(streaming.Message{
			Type:        streaming.MessageTypeTransaction,
			ChainID:     tx.ChainID,
			TraceID:     traceIDHex,
			BlockNumber: tx.BlockNumber,
			BlockHash:   tx.BlockHash,
			TxHash:      tx.TxHash,
			TxIndex:     tx.TxIndex,
			From:        tx.From,
			To:          tx.To,
			Value:       tx.Value,
			Nonce:       tx.Nonce,
			Gas:         tx.Gas,
			GasPrice:    tx.GasPrice,
			Input:       tx.Input,
			TxType:      tx.TxType,
		})
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return err
		}
		headers := make([]kafka.Header, 0, 2)
		telemetry.InjectKafkaHeaders(traceCtx, &headers)
		messages = append(messages, kafka.Message{
			Topic:   p.topicForChain(tx.ChainID),
			Key:     []byte(tx.TxHash),
			Value:   payload,
			Headers: headers,
		})
		spans = append(spans, span)
	}
	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		for _, span := range spans {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}
	for _, span := range spans {
		span.End()
	}
	return err
}

func (p *Producer) PublishReceipts(ctx context.Context, receipts []domain.Receipt) error {
	if len(receipts) == 0 {
		return nil
	}
	tracer := otel.Tracer("bcindex/kafka")
	messages := make([]kafka.Message, 0, len(receipts))
	spans := make([]trace.Span, 0, len(receipts))
	for _, receipt := range receipts {
		traceID, traceIDHex, ok := telemetry.NewTraceID()
		if !ok {
			traceIDHex = ""
		}
		traceCtx := ctx
		if ok {
			if spanCtx, ok := telemetry.NewSpanContext(traceID); ok {
				traceCtx = trace.ContextWithSpanContext(ctx, spanCtx)
			}
		}
		traceCtx, span := tracer.Start(traceCtx, "ordering.publish_receipt", trace.WithSpanKind(trace.SpanKindProducer))
		span.SetAttributes(
			attribute.Int64("chain.id", int64(receipt.ChainID)),
			attribute.String("tx.hash", receipt.TxHash),
			attribute.Int64("block.number", int64(receipt.BlockNumber)),
		)

		payload, err := streaming.Encode(streaming.Message{
			Type:              streaming.MessageTypeReceipt,
			ChainID:           receipt.ChainID,
			TraceID:           traceIDHex,
			BlockNumber:       receipt.BlockNumber,
			BlockHash:         receipt.BlockHash,
			TxHash:            receipt.TxHash,
			TxIndex:           receipt.TxIndex,
			Status:            receipt.Status,
			CumulativeGasUsed: receipt.CumulativeGasUsed,
			ReceiptGasUsed:    receipt.GasUsed,
			ContractAddress:   receipt.ContractAddress,
			LogsBloom:         receipt.LogsBloom,
			EffectiveGasPrice: receipt.EffectiveGasPrice,
		})
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return err
		}
		headers := make([]kafka.Header, 0, 2)
		telemetry.InjectKafkaHeaders(traceCtx, &headers)
		messages = append(messages, kafka.Message{
			Topic:   p.topicForChain(receipt.ChainID),
			Key:     []byte(receipt.TxHash),
			Value:   payload,
			Headers: headers,
		})
		spans = append(spans, span)
	}
	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		for _, span := range spans {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}
	for _, span := range spans {
		span.End()
	}
	return err
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
