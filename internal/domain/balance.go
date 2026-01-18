package domain

// Balance represents the indexed balance for an address.
type Balance struct {
	ChainID uint64
	Address string
	Balance string
}
