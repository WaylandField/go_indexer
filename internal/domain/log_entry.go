package domain

// LogEntry represents a contract log returned by the chain.
type LogEntry struct {
	BlockNumber uint64
	TxHash      string
	LogIndex    uint64
	Address     string
	Data        string
	Topics      []string
	Removed     bool
}
