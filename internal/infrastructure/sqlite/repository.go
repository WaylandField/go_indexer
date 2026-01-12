package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"bcindex/internal/application"
	"bcindex/internal/domain"

	_ "modernc.org/sqlite"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(dbPath string) (*Repository, error) {
	if dbPath == "" {
		return nil, errors.New("db path is required")
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
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
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			block_number INTEGER NOT NULL,
			tx_hash TEXT NOT NULL,
			log_index INTEGER NOT NULL,
			address TEXT NOT NULL,
			data TEXT NOT NULL,
			topics TEXT NOT NULL,
			removed INTEGER NOT NULL,
			UNIQUE(block_number, tx_hash, log_index)
		)`,
		`CREATE TABLE IF NOT EXISTS state (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}
	for _, stmt := range schema {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) StoreLogs(ctx context.Context, logs []domain.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO logs (block_number, tx_hash, log_index, address, data, topics, removed)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(block_number, tx_hash, log_index) DO NOTHING`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, log := range logs {
		topics, err := json.Marshal(log.Topics)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		removed := 0
		if log.Removed {
			removed = 1
		}
		if _, err := stmt.ExecContext(ctx, log.BlockNumber, log.TxHash, log.LogIndex, log.Address, log.Data, string(topics), removed); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (r *Repository) QueryLogs(ctx context.Context, filter application.LogQueryFilter) ([]domain.LogEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	clauses := make([]string, 0, 4)
	args := make([]any, 0, 5)

	if filter.Address != "" {
		clauses = append(clauses, "address = ?")
		args = append(args, strings.ToLower(filter.Address))
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

	query := `SELECT block_number, tx_hash, log_index, address, data, topics, removed FROM logs`
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
		if err := rows.Scan(&log.BlockNumber, &log.TxHash, &log.LogIndex, &log.Address, &log.Data, &topicsRaw, &removed); err != nil {
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

func (r *Repository) QueryTransactions(ctx context.Context, filter application.TransactionQueryFilter) ([]domain.TransactionSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	clauses := make([]string, 0, 4)
	args := make([]any, 0, 5)

	if filter.Address != "" {
		clauses = append(clauses, "address = ?")
		args = append(args, strings.ToLower(filter.Address))
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

	query := `SELECT tx_hash, MIN(block_number) as block_number, COUNT(*) as log_count FROM logs`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " GROUP BY tx_hash ORDER BY block_number ASC LIMIT ?"

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

	var transactions []domain.TransactionSummary
	for rows.Next() {
		var tx domain.TransactionSummary
		if err := rows.Scan(&tx.TxHash, &tx.BlockNumber, &tx.LogCount); err != nil {
			return nil, err
		}
		transactions = append(transactions, tx)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return transactions, nil
}

func (r *Repository) BlockRange(ctx context.Context) (uint64, uint64, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var min sql.NullInt64
	var max sql.NullInt64
	if err := r.db.QueryRowContext(ctx, `SELECT MIN(block_number), MAX(block_number) FROM logs`).Scan(&min, &max); err != nil {
		return 0, 0, false, err
	}
	if !min.Valid || !max.Valid {
		return 0, 0, false, nil
	}
	return uint64(min.Int64), uint64(max.Int64), true, nil
}

func (r *Repository) LastProcessedBlock(ctx context.Context) (uint64, bool, error) {
	var value string
	if err := r.db.QueryRowContext(ctx, `SELECT value FROM state WHERE key = 'last_block'`).Scan(&value); err != nil {
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

func (r *Repository) SetLastProcessedBlock(ctx context.Context, block uint64) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO state (key, value) VALUES ('last_block', ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, fmt.Sprintf("%d", block))
	return err
}

func (r *Repository) ClearLastProcessedBlock(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM state WHERE key = 'last_block'`)
	return err
}

func (r *Repository) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return r.db.PingContext(ctx)
}
