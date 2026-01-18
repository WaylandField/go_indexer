package application

type LogQueryFilter struct {
	ChainID   *uint64
	Address   string
	TxHash    string
	FromBlock *uint64
	ToBlock   *uint64
	Limit     int
}

type TransactionQueryFilter struct {
	ChainID   *uint64
	Address   string
	TxHash    string
	FromBlock *uint64
	ToBlock   *uint64
	Limit     int
}

type BalanceQueryFilter struct {
	ChainID *uint64
	Address string
	Limit   int
}
