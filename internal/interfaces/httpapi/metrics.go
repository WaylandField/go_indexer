package httpapi

import (
	"sync"
	"time"
)

type Metrics struct {
	mu              sync.RWMutex
	startTime       time.Time
	latestBlock     uint64
	lastProcessed   uint64
	lastBatchFrom   uint64
	lastBatchTo     uint64
	lastBatchCount  int
	totalLogsStored uint64
}

func NewMetrics() *Metrics {
	return &Metrics{startTime: time.Now()}
}

func (m *Metrics) OnLatestBlock(block uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latestBlock = block
}

func (m *Metrics) OnBatchProcessed(fromBlock, toBlock uint64, logCount int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastProcessed = toBlock
	m.lastBatchFrom = fromBlock
	m.lastBatchTo = toBlock
	m.lastBatchCount = logCount
	m.totalLogsStored += uint64(logCount)
}

func (m *Metrics) SetLastProcessed(block uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastProcessed = block
}

type Snapshot struct {
	StartTime       time.Time
	LatestBlock     uint64
	LastProcessed   uint64
	LastBatchFrom   uint64
	LastBatchTo     uint64
	LastBatchCount  int
	TotalLogsStored uint64
}

func (m *Metrics) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Snapshot{
		StartTime:       m.startTime,
		LatestBlock:     m.latestBlock,
		LastProcessed:   m.lastProcessed,
		LastBatchFrom:   m.lastBatchFrom,
		LastBatchTo:     m.lastBatchTo,
		LastBatchCount:  m.lastBatchCount,
		TotalLogsStored: m.totalLogsStored,
	}
}
