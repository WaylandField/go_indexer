package application

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
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
	StartBlock        uint64
	Confirmations     uint64
	PollInterval      time.Duration
	BatchSize         uint64
	LogFetchChunkSize uint64
	LogFetchWorkers   int
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

const fastEmptyFetchThreshold = 200 * time.Millisecond

func NewIndexer(source LogSource, writer StreamWriter, blocks BlockRepository, state StateRepository, observer IndexerObserver, cfg IndexerConfig) (*Indexer, error) {
	if source == nil || writer == nil || blocks == nil || state == nil {
		return nil, errors.New("indexer dependencies must not be nil")
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}
	if cfg.LogFetchChunkSize == 0 {
		if cfg.BatchSize < 100 {
			cfg.LogFetchChunkSize = cfg.BatchSize
		} else {
			cfg.LogFetchChunkSize = 100
		}
	}
	if cfg.LogFetchChunkSize > cfg.BatchSize {
		cfg.LogFetchChunkSize = cfg.BatchSize
	}
	if cfg.LogFetchWorkers <= 0 {
		cfg.LogFetchWorkers = 5
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
	batchSize := i.cfg.BatchSize
	maxBatchSize := i.cfg.BatchSize
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

		toBlock := current + batchSize - 1
		if toBlock > latest {
			toBlock = latest
		}

		fetchStart := time.Now()
		logs, err := i.fetchLogs(ctx, current, toBlock)
		if err != nil {
			if isResponseTooLarge(err) {
				if batchSize <= 1 {
					return err
				}
				batchSize /= 2
				if batchSize == 0 {
					batchSize = 1
				}
				continue
			}
			return err
		}
		if len(logs) == 0 && time.Since(fetchStart) < fastEmptyFetchThreshold && batchSize < maxBatchSize {
			next := batchSize * 2
			if next > maxBatchSize {
				next = maxBatchSize
			}
			batchSize = next
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

func isResponseTooLarge(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "response body too large") ||
		strings.Contains(lower, "response too large") ||
		strings.Contains(lower, "body too large") ||
		strings.Contains(lower, "request entity too large") ||
		strings.Contains(lower, "exceeds the limit")
}

func (i *Indexer) fetchLogs(ctx context.Context, fromBlock, toBlock uint64) ([]domain.LogEntry, error) {
	if fromBlock > toBlock {
		return nil, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	totalBlocks := toBlock - fromBlock + 1
	if i.cfg.LogFetchWorkers <= 1 || i.cfg.LogFetchChunkSize >= totalBlocks {
		return i.source.FetchLogs(ctx, fromBlock, toBlock)
	}

	type logRange struct {
		from  uint64
		to    uint64
		index int
	}
	type logResult struct {
		index int
		logs  []domain.LogEntry
		err   error
	}

	ranges := make([]logRange, 0, int((totalBlocks+i.cfg.LogFetchChunkSize-1)/i.cfg.LogFetchChunkSize))
	start := fromBlock
	for start <= toBlock {
		end := start + i.cfg.LogFetchChunkSize - 1
		if end > toBlock {
			end = toBlock
		}
		ranges = append(ranges, logRange{from: start, to: end, index: len(ranges)})
		if end == toBlock {
			break
		}
		start = end + 1
	}

	workerCount := i.cfg.LogFetchWorkers
	if workerCount > len(ranges) {
		workerCount = len(ranges)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan logRange)
	results := make(chan logResult, len(ranges))

	var wg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if ctx.Err() != nil {
					return
				}
				logs, err := i.source.FetchLogs(ctx, job.from, job.to)
				if err != nil {
					cancel()
				}
				results <- logResult{index: job.index, logs: logs, err: err}
				if err != nil {
					return
				}
			}
		}()
	}

	jobsSent := 0
	for _, job := range ranges {
		if ctx.Err() != nil {
			break
		}
		jobs <- job
		jobsSent++
	}
	close(jobs)

	parts := make([][]domain.LogEntry, len(ranges))
	var firstErr error
	for i := 0; i < jobsSent; i++ {
		result := <-results
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
		if result.err == nil {
			parts[result.index] = result.logs
		}
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	totalLogs := 0
	for _, logs := range parts {
		totalLogs += len(logs)
	}
	merged := make([]domain.LogEntry, 0, totalLogs)
	for _, logs := range parts {
		merged = append(merged, logs...)
	}
	return merged, nil
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
