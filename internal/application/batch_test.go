package application

import (
	"context"
	"testing"

	"bcindex/internal/domain"
	"bcindex/internal/streaming"

	"github.com/segmentio/kafka-go"
)

type mockRepo struct {
	logs         []domain.LogEntry
	blocks       []domain.BlockRecord
	transactions []domain.Transaction
	receipts     []domain.Receipt
	lastBlock    map[uint64]uint64
}

func (m *mockRepo) StoreLogs(ctx context.Context, logs []domain.LogEntry) error {
	m.logs = append(m.logs, logs...)
	return nil
}
func (m *mockRepo) StoreBlocks(ctx context.Context, blocks []domain.BlockRecord) error {
	m.blocks = append(m.blocks, blocks...)
	return nil
}
func (m *mockRepo) StoreTransactions(ctx context.Context, transactions []domain.Transaction) error {
	m.transactions = append(m.transactions, transactions...)
	return nil
}
func (m *mockRepo) StoreReceipts(ctx context.Context, receipts []domain.Receipt) error {
	m.receipts = append(m.receipts, receipts...)
	return nil
}
func (m *mockRepo) SetLastProcessedBlock(ctx context.Context, chainID uint64, block uint64) error {
	if m.lastBlock == nil {
		m.lastBlock = make(map[uint64]uint64)
	}
	m.lastBlock[chainID] = block
	return nil
}

// Stubs for interface compliance
func (m *mockRepo) DeleteLogsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	return nil
}
func (m *mockRepo) DeleteBlocksFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	return nil
}
func (m *mockRepo) DeleteTransactionsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	return nil
}
func (m *mockRepo) DeleteReceiptsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	return nil
}
func (m *mockRepo) ClearLastProcessedBlock(ctx context.Context, chainID uint64) error { return nil }

type mockCommitter struct {
	committed []kafka.Message
}

func (m *mockCommitter) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.committed = append(m.committed, msgs...)
	return nil
}

func TestBatch_AddAndFlush(t *testing.T) {
	batch := NewBatch()
	repo := &mockRepo{}
	committer := &mockCommitter{}
	ctx := context.Background()

	// Add Log
	batch.Add(streaming.Message{
		Type:        streaming.MessageTypeLog,
		ChainID:     1,
		BlockNumber: 100,
		TxHash:      "0x1",
		Address:     "0xAddr",
	}, kafka.Message{Offset: 1})

	// Add Block
	batch.Add(streaming.Message{
		Type:        streaming.MessageTypeBlock,
		ChainID:     1,
		BlockNumber: 101,
		BlockHash:   "0xBlock",
	}, kafka.Message{Offset: 2})

	if batch.Len() != 2 {
		t.Errorf("expected batch len 2, got %d", batch.Len())
	}

	// Flush
	if err := batch.Flush(ctx, repo, committer); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify Repo
	if len(repo.logs) != 1 {
		t.Errorf("expected 1 log, got %d", len(repo.logs))
	}
	if len(repo.blocks) != 1 {
		t.Errorf("expected 1 block, got %d", len(repo.blocks))
	}
	if repo.lastBlock[1] != 101 {
		t.Errorf("expected last processed block 101, got %d", repo.lastBlock[1])
	}

	// Verify Committer
	if len(committer.committed) != 2 {
		t.Errorf("expected 2 committed messages, got %d", len(committer.committed))
	}

	// Verify Reset
	if batch.Len() != 0 {
		t.Errorf("expected batch len 0 after reset, got %d", batch.Len())
	}
}
