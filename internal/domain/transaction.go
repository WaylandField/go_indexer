package domain

// TransactionSummary represents a transaction inferred from indexed logs.
type TransactionSummary struct {
	TxHash      string
	BlockNumber uint64
	LogCount    int
}
