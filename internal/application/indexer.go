package application

import (
	"context"
	"errors"
	"time"

	"bcindex/internal/domain"
)

type LogSource interface {
	LatestBlockNumber(ctx context.Context) (uint64, error)
	FetchLogs(ctx context.Context, fromBlock, toBlock uint64) ([]domain.LogEntry, error)
}

type LogRepository interface {
	StoreLogs(ctx context.Context, logs []domain.LogEntry) error
}

type StateRepository interface {
	LastProcessedBlock(ctx context.Context) (uint64, bool, error)
	SetLastProcessedBlock(ctx context.Context, block uint64) error
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
	logs     LogRepository
	state    StateRepository
	observer IndexerObserver
	cfg      IndexerConfig
}

func NewIndexer(source LogSource, logs LogRepository, state StateRepository, observer IndexerObserver, cfg IndexerConfig) (*Indexer, error) {
	if source == nil || logs == nil || state == nil {
		return nil, errors.New("indexer dependencies must not be nil")
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	return &Indexer{source: source, logs: logs, state: state, observer: observer, cfg: cfg}, nil
}

func (i *Indexer) Run(ctx context.Context) error {
	startBlock := i.cfg.StartBlock
	if last, ok, err := i.state.LastProcessedBlock(ctx); err != nil {
		return err
	} else if ok {
		startBlock = last + 1
	}

	current := startBlock
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
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
		if err := i.logs.StoreLogs(ctx, logs); err != nil {
			return err
		}
		if err := i.state.SetLastProcessedBlock(ctx, toBlock); err != nil {
			return err
		}
		if i.observer != nil {
			i.observer.OnBatchProcessed(current, toBlock, len(logs))
		}

		current = toBlock + 1
	}
}
