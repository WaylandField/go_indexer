package storage

import (
	"context"
	"errors"
	"math/big"

	"bcindex/internal/application"
	"bcindex/internal/domain"
	"bcindex/internal/infrastructure/clickhouse"
	"bcindex/internal/infrastructure/mysql"
)

type Repository struct {
	mysql *mysql.Repository
	logs  *clickhouse.LogRepository
}

func NewRepository(mysqlRepo *mysql.Repository, logRepo *clickhouse.LogRepository) (*Repository, error) {
	if mysqlRepo == nil {
		return nil, errors.New("mysql repository is required")
	}
	if logRepo == nil {
		return nil, errors.New("clickhouse log repository is required")
	}
	return &Repository{mysql: mysqlRepo, logs: logRepo}, nil
}

func (r *Repository) StoreLogs(ctx context.Context, logs []domain.LogEntry) error {
	return r.logs.StoreLogs(ctx, logs)
}

func (r *Repository) DeleteLogsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	return r.logs.DeleteLogsFrom(ctx, chainID, fromBlock)
}

func (r *Repository) QueryLogs(ctx context.Context, filter application.LogQueryFilter) ([]domain.LogEntry, error) {
	return r.logs.QueryLogs(ctx, filter)
}

func (r *Repository) QueryLogsAfter(ctx context.Context, chainID uint64, afterBlock uint64, afterLogIndex uint64, limit int) ([]domain.LogEntry, error) {
	return r.logs.QueryLogsAfter(ctx, chainID, afterBlock, afterLogIndex, limit)
}

func (r *Repository) StoreBlocks(ctx context.Context, blocks []domain.BlockRecord) error {
	return r.mysql.StoreBlocks(ctx, blocks)
}

func (r *Repository) StoreTransactions(ctx context.Context, transactions []domain.Transaction) error {
	return r.mysql.StoreTransactions(ctx, transactions)
}

func (r *Repository) StoreReceipts(ctx context.Context, receipts []domain.Receipt) error {
	return r.mysql.StoreReceipts(ctx, receipts)
}

func (r *Repository) DeleteBlocksFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	return r.mysql.DeleteBlocksFrom(ctx, chainID, fromBlock)
}

func (r *Repository) DeleteTransactionsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	return r.mysql.DeleteTransactionsFrom(ctx, chainID, fromBlock)
}

func (r *Repository) DeleteReceiptsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	return r.mysql.DeleteReceiptsFrom(ctx, chainID, fromBlock)
}

func (r *Repository) QueryTransactions(ctx context.Context, filter application.TransactionQueryFilter) ([]domain.Transaction, error) {
	return r.mysql.QueryTransactions(ctx, filter)
}

func (r *Repository) QueryBalances(ctx context.Context, filter application.BalanceQueryFilter) ([]domain.Balance, error) {
	return r.mysql.QueryBalances(ctx, filter)
}

func (r *Repository) BlockRange(ctx context.Context, chainID *uint64) (uint64, uint64, bool, error) {
	return r.mysql.BlockRange(ctx, chainID)
}

func (r *Repository) LastProcessedBlock(ctx context.Context, chainID uint64) (uint64, bool, error) {
	return r.mysql.LastProcessedBlock(ctx, chainID)
}

func (r *Repository) SetLastProcessedBlock(ctx context.Context, chainID uint64, block uint64) error {
	return r.mysql.SetLastProcessedBlock(ctx, chainID, block)
}

func (r *Repository) ClearLastProcessedBlock(ctx context.Context, chainID uint64) error {
	return r.mysql.ClearLastProcessedBlock(ctx, chainID)
}

func (r *Repository) AddBalanceDelta(ctx context.Context, chainID uint64, address string, delta *big.Int) error {
	return r.mysql.AddBalanceDelta(ctx, chainID, address, delta)
}

func (r *Repository) ResetBalances(ctx context.Context, chainID uint64) error {
	return r.mysql.ResetBalances(ctx, chainID)
}

func (r *Repository) Ping(ctx context.Context) error {
	if err := r.mysql.Ping(ctx); err != nil {
		return err
	}
	return r.logs.Ping(ctx)
}
