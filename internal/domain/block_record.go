package domain

// BlockRecord stores the canonical hash for a block number.
type BlockRecord struct {
	ChainID     uint64
	BlockNumber uint64
	BlockHash   string
	ParentHash  string
	Timestamp   uint64
	GasLimit    uint64
	GasUsed     uint64
	TxCount     uint64
}
