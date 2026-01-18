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
	kafkaMessages   uint64
	kafkaDecodeErrs uint64
	kafkaApplyErrs  uint64
	kafkaCommitErrs uint64
	kafkaFetchErrs  uint64
	kafkaLastTopic  string
	kafkaLastPart   int
	kafkaLastOffset int64
	kafkaLastBytes  int
	kafkaLastTime   time.Time
	kafkaLastLag    time.Duration
	kafkaMaxLag     time.Duration
	kafkaTopicCount map[string]uint64
	kafkaPartCount  map[string]map[int]uint64
	kafkaPartMaxLag map[string]map[int]time.Duration
}

func NewMetrics() *Metrics {
	return &Metrics{
		startTime:       time.Now(),
		kafkaTopicCount: make(map[string]uint64),
		kafkaPartCount:  make(map[string]map[int]uint64),
		kafkaPartMaxLag: make(map[string]map[int]time.Duration),
	}
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

func (m *Metrics) IncKafkaMessage() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kafkaMessages++
}

func (m *Metrics) IncKafkaDecodeErr() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kafkaDecodeErrs++
}

func (m *Metrics) IncKafkaApplyErr() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kafkaApplyErrs++
}

func (m *Metrics) IncKafkaCommitErr() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kafkaCommitErrs++
}

func (m *Metrics) IncKafkaFetchErr() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kafkaFetchErrs++
}

func (m *Metrics) ObserveKafkaMessage(topic string, partition int, offset int64, bytes int, ts time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kafkaMessages++
	m.kafkaLastTopic = topic
	m.kafkaLastPart = partition
	m.kafkaLastOffset = offset
	m.kafkaLastBytes = bytes
	m.kafkaLastTime = ts
	if !ts.IsZero() {
		lag := time.Since(ts)
		m.kafkaLastLag = lag
		if lag > m.kafkaMaxLag {
			m.kafkaMaxLag = lag
		}
	}
	if topic != "" {
		m.kafkaTopicCount[topic]++
		if _, ok := m.kafkaPartCount[topic]; !ok {
			m.kafkaPartCount[topic] = make(map[int]uint64)
		}
		if _, ok := m.kafkaPartMaxLag[topic]; !ok {
			m.kafkaPartMaxLag[topic] = make(map[int]time.Duration)
		}
		m.kafkaPartCount[topic][partition]++
		if m.kafkaLastLag > m.kafkaPartMaxLag[topic][partition] {
			m.kafkaPartMaxLag[topic][partition] = m.kafkaLastLag
		}
	}
}

type Snapshot struct {
	StartTime       time.Time
	LatestBlock     uint64
	LastProcessed   uint64
	LastBatchFrom   uint64
	LastBatchTo     uint64
	LastBatchCount  int
	TotalLogsStored uint64
	KafkaMessages   uint64
	KafkaDecodeErrs uint64
	KafkaApplyErrs  uint64
	KafkaCommitErrs uint64
	KafkaFetchErrs  uint64
	KafkaLastTopic  string
	KafkaLastPart   int
	KafkaLastOffset int64
	KafkaLastBytes  int
	KafkaLastTime   time.Time
	KafkaLastLag    time.Duration
	KafkaMaxLag     time.Duration
	KafkaTopicCount map[string]uint64
	KafkaPartCount  map[string]map[int]uint64
	KafkaPartMaxLag map[string]map[int]time.Duration
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
		KafkaMessages:   m.kafkaMessages,
		KafkaDecodeErrs: m.kafkaDecodeErrs,
		KafkaApplyErrs:  m.kafkaApplyErrs,
		KafkaCommitErrs: m.kafkaCommitErrs,
		KafkaFetchErrs:  m.kafkaFetchErrs,
		KafkaLastTopic:  m.kafkaLastTopic,
		KafkaLastPart:   m.kafkaLastPart,
		KafkaLastOffset: m.kafkaLastOffset,
		KafkaLastBytes:  m.kafkaLastBytes,
		KafkaLastTime:   m.kafkaLastTime,
		KafkaLastLag:    m.kafkaLastLag,
		KafkaMaxLag:     m.kafkaMaxLag,
		KafkaTopicCount: copyKafkaCounts(m.kafkaTopicCount),
		KafkaPartCount:  copyKafkaPartitionCounts(m.kafkaPartCount),
		KafkaPartMaxLag: copyKafkaPartitionLags(m.kafkaPartMaxLag),
	}
}

func copyKafkaCounts(source map[string]uint64) map[string]uint64 {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[string]uint64, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func copyKafkaPartitionCounts(source map[string]map[int]uint64) map[string]map[int]uint64 {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[string]map[int]uint64, len(source))
	for topic, partitions := range source {
		partClone := make(map[int]uint64, len(partitions))
		for partition, count := range partitions {
			partClone[partition] = count
		}
		clone[topic] = partClone
	}
	return clone
}

func copyKafkaPartitionLags(source map[string]map[int]time.Duration) map[string]map[int]time.Duration {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[string]map[int]time.Duration, len(source))
	for topic, partitions := range source {
		partClone := make(map[int]time.Duration, len(partitions))
		for partition, lag := range partitions {
			partClone[partition] = lag
		}
		clone[topic] = partClone
	}
	return clone
}
