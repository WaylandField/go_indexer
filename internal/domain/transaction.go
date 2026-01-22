package domain

// TransactionSummary represents a transaction inferred from indexed logs.
type TransactionSummary struct {
	ChainID     uint64
	TxHash      string
	BlockNumber uint64
	LogCount    int
}

// Transaction represents a chain transaction record.
type Transaction struct {
	ChainID     uint64
	TxHash      string
	BlockNumber uint64
	BlockHash   string
	TxIndex     uint64
	From        string
	To          string
	Value       string
	Nonce       uint64
	Gas         uint64
	GasPrice    string
	Input       string
	TxType      uint64
}
