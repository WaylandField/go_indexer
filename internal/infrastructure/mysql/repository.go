package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"bcindex/internal/application"
	"bcindex/internal/domain"

	_ "github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(dsn string) (*Repository, error) {
	if dsn == "" {
		return nil, errors.New("db dsn is required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	if err := createSchema(db); err != nil {
		return nil, err
	}
	return &Repository{db: db}, nil
}

func createSchema(db *sql.DB) error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS logs (
			id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			chain_id BIGINT UNSIGNED NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			block_hash VARCHAR(66) NOT NULL,
			tx_hash VARCHAR(66) NOT NULL,
			log_index BIGINT UNSIGNED NOT NULL,
			address VARCHAR(42) NOT NULL,
			data MEDIUMTEXT NOT NULL,
			topics MEDIUMTEXT NOT NULL,
			removed TINYINT(1) NOT NULL,
			PRIMARY KEY (id),
			UNIQUE KEY logs_unique (chain_id, block_hash, block_number, tx_hash, log_index),
			KEY logs_block_idx (chain_id, block_number),
			KEY logs_address_idx (chain_id, address)
		)`,
		`CREATE TABLE IF NOT EXISTS blocks (
			chain_id BIGINT UNSIGNED NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			block_hash VARCHAR(66) NOT NULL,
			parent_hash VARCHAR(66) NOT NULL DEFAULT '',
			timestamp BIGINT UNSIGNED NOT NULL DEFAULT 0,
			gas_limit BIGINT UNSIGNED NOT NULL DEFAULT 0,
			gas_used BIGINT UNSIGNED NOT NULL DEFAULT 0,
			tx_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
			PRIMARY KEY (chain_id, block_number)
		)`,
		`CREATE TABLE IF NOT EXISTS transactions (
			chain_id BIGINT UNSIGNED NOT NULL,
			tx_hash VARCHAR(66) NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			block_hash VARCHAR(66) NOT NULL,
			tx_index BIGINT UNSIGNED NOT NULL,
			from_addr VARCHAR(42) NOT NULL,
			to_addr VARCHAR(42) NULL,
			value DECIMAL(65,0) NOT NULL,
			nonce BIGINT UNSIGNED NOT NULL,
			gas BIGINT UNSIGNED NOT NULL,
			gas_price DECIMAL(65,0) NOT NULL,
			input MEDIUMTEXT NOT NULL,
			tx_type BIGINT UNSIGNED NOT NULL,
			PRIMARY KEY (chain_id, tx_hash),
			KEY tx_block_idx (chain_id, block_number),
			KEY tx_from_idx (chain_id, from_addr),
			KEY tx_to_idx (chain_id, to_addr)
		)`,
		`CREATE TABLE IF NOT EXISTS receipts (
			chain_id BIGINT UNSIGNED NOT NULL,
			tx_hash VARCHAR(66) NOT NULL,
			block_number BIGINT UNSIGNED NOT NULL,
			block_hash VARCHAR(66) NOT NULL,
			tx_index BIGINT UNSIGNED NOT NULL,
			status TINYINT UNSIGNED NOT NULL,
			cumulative_gas_used BIGINT UNSIGNED NOT NULL,
			gas_used BIGINT UNSIGNED NOT NULL,
			contract_address VARCHAR(42) NULL,
			logs_bloom VARCHAR(514) NOT NULL,
			effective_gas_price DECIMAL(65,0) NOT NULL,
			PRIMARY KEY (chain_id, tx_hash),
			KEY receipts_block_idx (chain_id, block_number)
		)`,
		`CREATE TABLE IF NOT EXISTS balances (
			chain_id BIGINT UNSIGNED NOT NULL,
			address VARCHAR(42) NOT NULL,
			balance DECIMAL(65,0) NOT NULL,
			PRIMARY KEY (chain_id, address)
		)`,
		`CREATE TABLE IF NOT EXISTS state (
			state_key VARCHAR(64) NOT NULL,
			state_value VARCHAR(64) NOT NULL,
			PRIMARY KEY (state_key)
		)`,
	}
	for _, stmt := range schema {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	if err := ensureColumn(db, "logs", "chain_id", "BIGINT UNSIGNED NOT NULL"); err != nil {
		return err
	}
	if err := ensureColumn(db, "logs", "block_hash", "VARCHAR(66) NOT NULL"); err != nil {
		return err
	}
	if err := ensureColumn(db, "blocks", "parent_hash", "VARCHAR(66) NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumn(db, "blocks", "timestamp", "BIGINT UNSIGNED NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "blocks", "gas_limit", "BIGINT UNSIGNED NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "blocks", "gas_used", "BIGINT UNSIGNED NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "blocks", "tx_count", "BIGINT UNSIGNED NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	return nil
}

func ensureColumn(db *sql.DB, table, column, definition string) error {
	var count int
	row := db.QueryRow(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
		table,
		column,
	)
	if err := row.Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition)
	_, err := db.Exec(stmt)
	return err
}

func (r *Repository) StoreLogs(ctx context.Context, logs []domain.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	ctx, span := startDBSpan(ctx, "mysql.StoreLogs", attribute.Int("log.count", len(logs)))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT IGNORE INTO logs (chain_id, block_number, block_hash, tx_hash, log_index, address, data, topics, removed)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer stmt.Close()

	for _, log := range logs {
		topics, err := json.Marshal(log.Topics)
		if err != nil {
			_ = tx.Rollback()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		removed := 0
		if log.Removed {
			removed = 1
		}
		if _, err := stmt.ExecContext(ctx, log.ChainID, log.BlockNumber, log.BlockHash, log.TxHash, log.LogIndex, log.Address, log.Data, string(topics), removed); err != nil {
			_ = tx.Rollback()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (r *Repository) StoreBlocks(ctx context.Context, blocks []domain.BlockRecord) error {
	if len(blocks) == 0 {
		return nil
	}
	ctx, span := startDBSpan(ctx, "mysql.StoreBlocks", attribute.Int("block.count", len(blocks)))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO blocks (chain_id, block_number, block_hash, parent_hash, timestamp, gas_limit, gas_used, tx_count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			block_hash = VALUES(block_hash),
			parent_hash = VALUES(parent_hash),
			timestamp = VALUES(timestamp),
			gas_limit = VALUES(gas_limit),
			gas_used = VALUES(gas_used),
			tx_count = VALUES(tx_count)`)
	if err != nil {
		_ = tx.Rollback()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer stmt.Close()

	for _, block := range blocks {
		if _, err := stmt.ExecContext(ctx, block.ChainID, block.BlockNumber, block.BlockHash, block.ParentHash, block.Timestamp, block.GasLimit, block.GasUsed, block.TxCount); err != nil {
			_ = tx.Rollback()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (r *Repository) GetBlockHash(ctx context.Context, chainID uint64, blockNumber uint64) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var hash string
	if err := r.db.QueryRowContext(ctx, `SELECT block_hash FROM blocks WHERE chain_id = ? AND block_number = ?`, chainID, blockNumber).Scan(&hash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	return hash, true, nil
}

func (r *Repository) DeleteBlocksFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	ctx, span := startDBSpan(ctx, "mysql.DeleteBlocksFrom",
		attribute.Int64("chain.id", int64(chainID)),
		attribute.Int64("from.block", int64(fromBlock)),
	)
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := r.db.ExecContext(ctx, `DELETE FROM blocks WHERE chain_id = ? AND block_number >= ?`, chainID, fromBlock)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (r *Repository) DeleteLogsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	ctx, span := startDBSpan(ctx, "mysql.DeleteLogsFrom",
		attribute.Int64("chain.id", int64(chainID)),
		attribute.Int64("from.block", int64(fromBlock)),
	)
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := r.db.ExecContext(ctx, `DELETE FROM logs WHERE chain_id = ? AND block_number >= ?`, chainID, fromBlock)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (r *Repository) StoreTransactions(ctx context.Context, transactions []domain.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}
	ctx, span := startDBSpan(ctx, "mysql.StoreTransactions", attribute.Int("tx.count", len(transactions)))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO transactions (chain_id, tx_hash, block_number, block_hash, tx_index, from_addr, to_addr, value, nonce, gas, gas_price, input, tx_type)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			block_number = VALUES(block_number),
			block_hash = VALUES(block_hash),
			tx_index = VALUES(tx_index),
			from_addr = VALUES(from_addr),
			to_addr = VALUES(to_addr),
			value = VALUES(value),
			nonce = VALUES(nonce),
			gas = VALUES(gas),
			gas_price = VALUES(gas_price),
			input = VALUES(input),
			tx_type = VALUES(tx_type)`)
	if err != nil {
		_ = tx.Rollback()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer stmt.Close()

	for _, entry := range transactions {
		var toAddr any
		if entry.To != "" {
			toAddr = strings.ToLower(entry.To)
		}
		value := entry.Value
		if value == "" {
			value = "0"
		}
		gasPrice := entry.GasPrice
		if gasPrice == "" {
			gasPrice = "0"
		}
		txHash := strings.ToLower(entry.TxHash)
		blockHash := strings.ToLower(entry.BlockHash)
		if _, err := stmt.ExecContext(ctx, entry.ChainID, txHash, entry.BlockNumber, blockHash, entry.TxIndex, strings.ToLower(entry.From), toAddr, value, entry.Nonce, entry.Gas, gasPrice, entry.Input, entry.TxType); err != nil {
			_ = tx.Rollback()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (r *Repository) StoreReceipts(ctx context.Context, receipts []domain.Receipt) error {
	if len(receipts) == 0 {
		return nil
	}
	ctx, span := startDBSpan(ctx, "mysql.StoreReceipts", attribute.Int("receipt.count", len(receipts)))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO receipts (chain_id, tx_hash, block_number, block_hash, tx_index, status, cumulative_gas_used, gas_used, contract_address, logs_bloom, effective_gas_price)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			block_number = VALUES(block_number),
			block_hash = VALUES(block_hash),
			tx_index = VALUES(tx_index),
			status = VALUES(status),
			cumulative_gas_used = VALUES(cumulative_gas_used),
			gas_used = VALUES(gas_used),
			contract_address = VALUES(contract_address),
			logs_bloom = VALUES(logs_bloom),
			effective_gas_price = VALUES(effective_gas_price)`)
	if err != nil {
		_ = tx.Rollback()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer stmt.Close()

	for _, receipt := range receipts {
		var contract any
		if receipt.ContractAddress != "" {
			contract = strings.ToLower(receipt.ContractAddress)
		}
		effectiveGasPrice := receipt.EffectiveGasPrice
		if effectiveGasPrice == "" {
			effectiveGasPrice = "0"
		}
		txHash := strings.ToLower(receipt.TxHash)
		blockHash := strings.ToLower(receipt.BlockHash)
		if _, err := stmt.ExecContext(ctx, receipt.ChainID, txHash, receipt.BlockNumber, blockHash, receipt.TxIndex, receipt.Status, receipt.CumulativeGasUsed, receipt.GasUsed, contract, receipt.LogsBloom, effectiveGasPrice); err != nil {
			_ = tx.Rollback()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (r *Repository) DeleteTransactionsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	ctx, span := startDBSpan(ctx, "mysql.DeleteTransactionsFrom",
		attribute.Int64("chain.id", int64(chainID)),
		attribute.Int64("from.block", int64(fromBlock)),
	)
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := r.db.ExecContext(ctx, `DELETE FROM transactions WHERE chain_id = ? AND block_number >= ?`, chainID, fromBlock)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (r *Repository) DeleteReceiptsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	ctx, span := startDBSpan(ctx, "mysql.DeleteReceiptsFrom",
		attribute.Int64("chain.id", int64(chainID)),
		attribute.Int64("from.block", int64(fromBlock)),
	)
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := r.db.ExecContext(ctx, `DELETE FROM receipts WHERE chain_id = ? AND block_number >= ?`, chainID, fromBlock)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (r *Repository) QueryLogs(ctx context.Context, filter application.LogQueryFilter) ([]domain.LogEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	clauses := make([]string, 0, 5)
	args := make([]any, 0, 6)

	if filter.Address != "" {
		clauses = append(clauses, "address = ?")
		args = append(args, strings.ToLower(filter.Address))
	}
	if filter.ChainID != nil {
		clauses = append(clauses, "chain_id = ?")
		args = append(args, *filter.ChainID)
	}
	if filter.TxHash != "" {
		clauses = append(clauses, "tx_hash = ?")
		args = append(args, filter.TxHash)
	}
	if filter.FromBlock != nil {
		clauses = append(clauses, "block_number >= ?")
		args = append(args, *filter.FromBlock)
	}
	if filter.ToBlock != nil {
		clauses = append(clauses, "block_number <= ?")
		args = append(args, *filter.ToBlock)
	}

	query := `SELECT chain_id, block_number, block_hash, tx_hash, log_index, address, data, topics, removed FROM logs`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY block_number ASC, log_index ASC LIMIT ?"

	limit := filter.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	args = append(args, limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []domain.LogEntry
	for rows.Next() {
		var log domain.LogEntry
		var topicsRaw string
		var removed int
		if err := rows.Scan(&log.ChainID, &log.BlockNumber, &log.BlockHash, &log.TxHash, &log.LogIndex, &log.Address, &log.Data, &topicsRaw, &removed); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(topicsRaw), &log.Topics); err != nil {
			return nil, err
		}
		log.Removed = removed != 0
		logs = append(logs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return logs, nil
}

func (r *Repository) QueryLogsAfter(ctx context.Context, chainID uint64, afterBlock uint64, afterLogIndex uint64, limit int) ([]domain.LogEntry, error) {
	ctx, span := startDBSpan(ctx, "mysql.QueryLogsAfter",
		attribute.Int64("chain.id", int64(chainID)),
		attribute.Int64("after.block", int64(afterBlock)),
		attribute.Int64("after.log_index", int64(afterLogIndex)),
	)
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	query := `SELECT chain_id, block_number, block_hash, tx_hash, log_index, address, data, topics, removed
		FROM logs
		WHERE chain_id = ?
		AND (block_number > ? OR (block_number = ? AND log_index > ?))
		ORDER BY block_number ASC, log_index ASC
		LIMIT ?`

	rows, err := r.db.QueryContext(ctx, query, chainID, afterBlock, afterBlock, afterLogIndex, limit)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer rows.Close()

	var logs []domain.LogEntry
	for rows.Next() {
		var log domain.LogEntry
		var topicsRaw string
		var removed int
		if err := rows.Scan(&log.ChainID, &log.BlockNumber, &log.BlockHash, &log.TxHash, &log.LogIndex, &log.Address, &log.Data, &topicsRaw, &removed); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		if err := json.Unmarshal([]byte(topicsRaw), &log.Topics); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		log.Removed = removed != 0
		logs = append(logs, log)
	}
	if err := rows.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return logs, nil
}

func (r *Repository) QueryTransactions(ctx context.Context, filter application.TransactionQueryFilter) ([]domain.Transaction, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	clauses := make([]string, 0, 5)
	args := make([]any, 0, 6)

	if filter.Address != "" {
		clauses = append(clauses, "(from_addr = ? OR to_addr = ?)")
		address := strings.ToLower(filter.Address)
		args = append(args, address, address)
	}
	if filter.ChainID != nil {
		clauses = append(clauses, "chain_id = ?")
		args = append(args, *filter.ChainID)
	}
	if filter.TxHash != "" {
		clauses = append(clauses, "tx_hash = ?")
		args = append(args, strings.ToLower(filter.TxHash))
	}
	if filter.FromBlock != nil {
		clauses = append(clauses, "block_number >= ?")
		args = append(args, *filter.FromBlock)
	}
	if filter.ToBlock != nil {
		clauses = append(clauses, "block_number <= ?")
		args = append(args, *filter.ToBlock)
	}

	query := `SELECT chain_id, tx_hash, block_number, block_hash, tx_index, from_addr, to_addr, value, nonce, gas, gas_price, input, tx_type FROM transactions`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY block_number ASC, tx_index ASC LIMIT ?"

	limit := filter.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	args = append(args, limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transactions []domain.Transaction
	for rows.Next() {
		var tx domain.Transaction
		var toAddr sql.NullString
		if err := rows.Scan(&tx.ChainID, &tx.TxHash, &tx.BlockNumber, &tx.BlockHash, &tx.TxIndex, &tx.From, &toAddr, &tx.Value, &tx.Nonce, &tx.Gas, &tx.GasPrice, &tx.Input, &tx.TxType); err != nil {
			return nil, err
		}
		if toAddr.Valid {
			tx.To = toAddr.String
		}
		transactions = append(transactions, tx)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return transactions, nil
}

func (r *Repository) QueryBalances(ctx context.Context, filter application.BalanceQueryFilter) ([]domain.Balance, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	clauses := make([]string, 0, 2)
	args := make([]any, 0, 3)

	if filter.Address != "" {
		clauses = append(clauses, "address = ?")
		args = append(args, strings.ToLower(filter.Address))
	}
	if filter.ChainID != nil {
		clauses = append(clauses, "chain_id = ?")
		args = append(args, *filter.ChainID)
	}

	query := `SELECT chain_id, address, balance FROM balances`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY chain_id ASC, address ASC LIMIT ?"

	limit := filter.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	args = append(args, limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var balances []domain.Balance
	for rows.Next() {
		var balance domain.Balance
		if err := rows.Scan(&balance.ChainID, &balance.Address, &balance.Balance); err != nil {
			return nil, err
		}
		balances = append(balances, balance)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return balances, nil
}

func (r *Repository) BlockRange(ctx context.Context, chainID *uint64) (uint64, uint64, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var min sql.NullInt64
	var max sql.NullInt64
	query := `SELECT MIN(block_number), MAX(block_number) FROM blocks`
	args := []any{}
	if chainID != nil {
		query += " WHERE chain_id = ?"
		args = append(args, *chainID)
	}
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&min, &max); err != nil {
		return 0, 0, false, err
	}
	if !min.Valid || !max.Valid {
		return 0, 0, false, nil
	}
	return uint64(min.Int64), uint64(max.Int64), true, nil
}

func (r *Repository) LastProcessedBlock(ctx context.Context, chainID uint64) (uint64, bool, error) {
	var value string
	key := stateKey(chainID)
	if err := r.db.QueryRowContext(ctx, `SELECT state_value FROM state WHERE state_key = ?`, key).Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	var block uint64
	if _, err := fmt.Sscanf(value, "%d", &block); err != nil {
		return 0, false, err
	}
	return block, true, nil
}

func (r *Repository) SetLastProcessedBlock(ctx context.Context, chainID uint64, block uint64) error {
	ctx, span := startDBSpan(ctx, "mysql.SetLastProcessedBlock",
		attribute.Int64("chain.id", int64(chainID)),
		attribute.Int64("block.number", int64(block)),
	)
	defer span.End()
	key := stateKey(chainID)
	_, err := r.db.ExecContext(ctx, `INSERT INTO state (state_key, state_value) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE state_value = VALUES(state_value)`, key, fmt.Sprintf("%d", block))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (r *Repository) ClearLastProcessedBlock(ctx context.Context, chainID uint64) error {
	ctx, span := startDBSpan(ctx, "mysql.ClearLastProcessedBlock",
		attribute.Int64("chain.id", int64(chainID)),
	)
	defer span.End()
	key := stateKey(chainID)
	_, err := r.db.ExecContext(ctx, `DELETE FROM state WHERE state_key = ?`, key)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (r *Repository) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return r.db.PingContext(ctx)
}

func stateKey(chainID uint64) string {
	if chainID == 0 {
		return "last_block"
	}
	return fmt.Sprintf("last_block:%d", chainID)
}

func (r *Repository) AddBalanceDelta(ctx context.Context, chainID uint64, address string, delta *big.Int) error {
	if delta == nil {
		return nil
	}
	ctx, span := startDBSpan(ctx, "mysql.AddBalanceDelta",
		attribute.Int64("chain.id", int64(chainID)),
		attribute.String("address", strings.ToLower(address)),
	)
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := r.db.ExecContext(ctx, `INSERT INTO balances (chain_id, address, balance)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance)`, chainID, strings.ToLower(address), delta.String())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (r *Repository) ResetBalances(ctx context.Context, chainID uint64) error {
	ctx, span := startDBSpan(ctx, "mysql.ResetBalances",
		attribute.Int64("chain.id", int64(chainID)),
	)
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := r.db.ExecContext(ctx, `DELETE FROM balances WHERE chain_id = ?`, chainID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func startDBSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs = append(attrs, attribute.String("db.system", "mysql"))
	return otel.Tracer("bcindex/mysql").Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
}
