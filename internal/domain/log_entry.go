package domain

// LogEntry represents a contract log returned by the chain.
type LogEntry struct {
	ChainID     uint64
	BlockNumber uint64
	BlockHash   string
	TxHash      string
	LogIndex    uint64
	Address     string
	Data        string
	Topics      []string
	Removed     bool
}
