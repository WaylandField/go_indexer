package application

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"bcindex/internal/domain"
	"bcindex/internal/streaming"

	"github.com/segmentio/kafka-go"
)

type Batch struct {
	logs         []domain.LogEntry
	blocks       []domain.BlockRecord
	transactions []domain.Transaction
	receipts     []domain.Receipt
	messages     []kafka.Message
	maxBlockNum  map[uint64]uint64
	minOffset    map[int]int64
	maxOffset    map[int]int64
}

func NewBatch() *Batch {
	return &Batch{
		maxBlockNum: make(map[uint64]uint64),
		minOffset:   make(map[int]int64),
		maxOffset:   make(map[int]int64),
	}
}

func (b *Batch) Add(msg streaming.Message, kafkaMsg kafka.Message) {
	switch msg.Type {
	case streaming.MessageTypeLog:
		b.logs = append(b.logs, MapToLogEntry(msg))
	case streaming.MessageTypeBlock:
		b.blocks = append(b.blocks, MapToBlockRecord(msg))
	case streaming.MessageTypeTransaction:
		b.transactions = append(b.transactions, MapToTransaction(msg))
	case streaming.MessageTypeReceipt:
		b.receipts = append(b.receipts, MapToReceipt(msg))
	}

	b.messages = append(b.messages, kafkaMsg)

	// Track progress
	if msg.BlockNumber > b.maxBlockNum[msg.ChainID] {
		b.maxBlockNum[msg.ChainID] = msg.BlockNumber
	}

	// Track offsets range for logging/debugging
	partition := kafkaMsg.Partition
	offset := kafkaMsg.Offset
	if min, ok := b.minOffset[partition]; !ok || offset < min {
		b.minOffset[partition] = offset
	}
	if max, ok := b.maxOffset[partition]; !ok || offset > max {
		b.maxOffset[partition] = offset
	}
}

func (b *Batch) Len() int {
	return len(b.messages)
}

type Committer interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

func (b *Batch) Flush(ctx context.Context, repo ComputeRepository, committer Committer) error {
	if b.Len() == 0 {
		return nil
	}

	start := time.Now()

	// 1. Bulk Insert
	if len(b.logs) > 0 {
		if err := repo.StoreLogs(ctx, b.logs); err != nil {
			return fmt.Errorf("failed to store logs: %w", err)
		}
	}
	if len(b.blocks) > 0 {
		if err := repo.StoreBlocks(ctx, b.blocks); err != nil {
			return fmt.Errorf("failed to store blocks: %w", err)
		}
	}
	if len(b.transactions) > 0 {
		if err := repo.StoreTransactions(ctx, b.transactions); err != nil {
			return fmt.Errorf("failed to store transactions: %w", err)
		}
	}
	if len(b.receipts) > 0 {
		if err := repo.StoreReceipts(ctx, b.receipts); err != nil {
			return fmt.Errorf("failed to store receipts: %w", err)
		}
	}

	// 2. Update State (Last Processed Block)
	for chainID, blockNum := range b.maxBlockNum {
		if err := repo.SetLastProcessedBlock(ctx, chainID, blockNum); err != nil {
			return fmt.Errorf("failed to update state for chain %d: %w", chainID, err)
		}
	}

	// 3. Commit Offsets
	if err := committer.CommitMessages(ctx, b.messages...); err != nil {
		return fmt.Errorf("failed to commit kafka messages: %w", err)
	}

	slog.Info("flushed batch",
		"count", b.Len(),
		"logs", len(b.logs),
		"blocks", len(b.blocks),
		"txs", len(b.transactions),
		"duration", time.Since(start),
	)

	b.Reset()
	return nil
}

func (b *Batch) Reset() {
	b.logs = b.logs[:0]
	b.blocks = b.blocks[:0]
	b.transactions = b.transactions[:0]
	b.receipts = b.receipts[:0]
	b.messages = b.messages[:0]
	clear(b.maxBlockNum)
	clear(b.minOffset)
	clear(b.maxOffset)
}
