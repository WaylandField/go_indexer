package domain

// BlockRecord stores the canonical hash for a block number.
type BlockRecord struct {
	ChainID     uint64
	BlockNumber uint64
	BlockHash   string
}
