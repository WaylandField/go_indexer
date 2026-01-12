package application

type LogQueryFilter struct {
	Address   string
	TxHash    string
	FromBlock *uint64
	ToBlock   *uint64
	Limit     int
}

type TransactionQueryFilter struct {
	Address   string
	TxHash    string
	FromBlock *uint64
	ToBlock   *uint64
	Limit     int
}
