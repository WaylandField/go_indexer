package domain

// Receipt represents a transaction receipt from the chain.
type Receipt struct {
	ChainID           uint64
	TxHash            string
	BlockNumber       uint64
	BlockHash         string
	TxIndex           uint64
	Status            uint64
	CumulativeGasUsed uint64
	GasUsed           uint64
	ContractAddress   string
	LogsBloom         string
	EffectiveGasPrice string
}
