package application

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"bcindex/internal/domain"
)

type LogSource interface {
	LatestBlockNumber(ctx context.Context) (uint64, error)
	FetchLogs(ctx context.Context, fromBlock, toBlock uint64) ([]domain.LogEntry, error)
	ChainID(ctx context.Context) (uint64, error)
	BlockHash(ctx context.Context, blockNumber uint64) (string, bool, error)
}

type BlockRepository interface {
	StoreBlocks(ctx context.Context, blocks []domain.BlockRecord) error
	GetBlockHash(ctx context.Context, chainID uint64, blockNumber uint64) (string, bool, error)
	DeleteBlocksFrom(ctx context.Context, chainID uint64, fromBlock uint64) error
}

type StateRepository interface {
	LastProcessedBlock(ctx context.Context, chainID uint64) (uint64, bool, error)
	SetLastProcessedBlock(ctx context.Context, chainID uint64, block uint64) error
	ClearLastProcessedBlock(ctx context.Context, chainID uint64) error
}

type StreamWriter interface {
	PublishLogs(ctx context.Context, logs []domain.LogEntry) error
	PublishBlocks(ctx context.Context, blocks []domain.BlockRecord) error
	PublishReorg(ctx context.Context, chainID uint64, fromBlock uint64, reason string) error
}

type IndexerObserver interface {
	OnLatestBlock(block uint64)
	OnBatchProcessed(fromBlock, toBlock uint64, logCount int)
}

type IndexerConfig struct {
	StartBlock    uint64
	Confirmations uint64
	PollInterval  time.Duration
	BatchSize     uint64
}

type Indexer struct {
	source   LogSource
	writer   StreamWriter
	blocks   BlockRepository
	state    StateRepository
	observer IndexerObserver
	cfg      IndexerConfig
}

var ErrBlockUnavailable = errors.New("block unavailable")

func NewIndexer(source LogSource, writer StreamWriter, blocks BlockRepository, state StateRepository, observer IndexerObserver, cfg IndexerConfig) (*Indexer, error) {
	if source == nil || writer == nil || blocks == nil || state == nil {
		return nil, errors.New("indexer dependencies must not be nil")
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	return &Indexer{source: source, writer: writer, blocks: blocks, state: state, observer: observer, cfg: cfg}, nil
}

func (i *Indexer) Run(ctx context.Context) error {
	chainID, err := i.source.ChainID(ctx)
	if err != nil {
		return err
	}

	current := i.cfg.StartBlock
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := i.reconcileReorg(ctx, chainID); err != nil {
			if errors.Is(err, ErrBlockUnavailable) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(i.cfg.PollInterval):
					continue
				}
			}
			return err
		}
		if last, ok, err := i.state.LastProcessedBlock(ctx, chainID); err != nil {
			return err
		} else if ok {
			current = last + 1
		} else {
			current = i.cfg.StartBlock
		}

		latest, err := i.source.LatestBlockNumber(ctx)
		if err != nil {
			return err
		}
		if i.observer != nil {
			i.observer.OnLatestBlock(latest)
		}
		if latest < i.cfg.Confirmations {
			latest = 0
		} else {
			latest -= i.cfg.Confirmations
		}

		if current > latest {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(i.cfg.PollInterval):
				continue
			}
		}

		toBlock := current + i.cfg.BatchSize - 1
		if toBlock > latest {
			toBlock = latest
		}

		logs, err := i.source.FetchLogs(ctx, current, toBlock)
		if err != nil {
			return err
		}
		sort.Slice(logs, func(a, b int) bool {
			if logs[a].BlockNumber == logs[b].BlockNumber {
				return logs[a].LogIndex < logs[b].LogIndex
			}
			return logs[a].BlockNumber < logs[b].BlockNumber
		})
		blocks, err := i.fetchBlocks(ctx, chainID, current, toBlock)
		if err != nil {
			if errors.Is(err, ErrBlockUnavailable) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(i.cfg.PollInterval):
					continue
				}
			}
			return err
		}
		if err := i.writer.PublishBlocks(ctx, blocks); err != nil {
			return err
		}
		if err := i.writer.PublishLogs(ctx, logs); err != nil {
			return err
		}
		if err := i.blocks.StoreBlocks(ctx, blocks); err != nil {
			return err
		}
		if err := i.state.SetLastProcessedBlock(ctx, chainID, toBlock); err != nil {
			return err
		}
		if i.observer != nil {
			i.observer.OnBatchProcessed(current, toBlock, len(logs))
		}

		current = toBlock + 1
	}
}

func (i *Indexer) fetchBlocks(ctx context.Context, chainID, fromBlock, toBlock uint64) ([]domain.BlockRecord, error) {
	if fromBlock > toBlock {
		return nil, nil
	}
	blocks := make([]domain.BlockRecord, 0, int(toBlock-fromBlock)+1)
	for block := fromBlock; block <= toBlock; block++ {
		hash, ok, err := i.source.BlockHash(ctx, block)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrBlockUnavailable
		}
		blocks = append(blocks, domain.BlockRecord{
			ChainID:     chainID,
			BlockNumber: block,
			BlockHash:   hash,
		})
	}
	return blocks, nil
}

func (i *Indexer) reconcileReorg(ctx context.Context, chainID uint64) error {
	last, ok, err := i.state.LastProcessedBlock(ctx, chainID)
	if err != nil || !ok {
		return err
	}
	storedHash, ok, err := i.blocks.GetBlockHash(ctx, chainID, last)
	if err != nil || !ok {
		return err
	}
	currentHash, ok, err := i.source.BlockHash(ctx, last)
	if err != nil {
		return err
	}
	if !ok {
		return ErrBlockUnavailable
	}
	if strings.EqualFold(currentHash, storedHash) {
		return nil
	}

	var rewind *uint64
	for block := last; ; {
		if block == 0 {
			break
		}
		block--
		storedHash, ok, err := i.blocks.GetBlockHash(ctx, chainID, block)
		if err != nil {
			return err
		}
		if !ok {
			if block == 0 {
				break
			}
			continue
		}
		currentHash, ok, err := i.source.BlockHash(ctx, block)
		if err != nil {
			return err
		}
		if !ok {
			return ErrBlockUnavailable
		}
		if strings.EqualFold(currentHash, storedHash) {
			rewind = &block
			break
		}
		if block == 0 {
			break
		}
	}

	if rewind == nil {
		if err := i.blocks.DeleteBlocksFrom(ctx, chainID, 0); err != nil {
			return err
		}
		if err := i.writer.PublishReorg(ctx, chainID, 0, "reorg"); err != nil {
			return err
		}
		return i.state.ClearLastProcessedBlock(ctx, chainID)
	}

	from := *rewind + 1
	if err := i.blocks.DeleteBlocksFrom(ctx, chainID, from); err != nil {
		return err
	}
	if err := i.writer.PublishReorg(ctx, chainID, from, "reorg"); err != nil {
		return err
	}
	return i.state.SetLastProcessedBlock(ctx, chainID, *rewind)
}
