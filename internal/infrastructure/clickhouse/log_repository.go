package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"bcindex/internal/application"
	"bcindex/internal/domain"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type LogRepository struct {
	db   *sql.DB
	conn clickhouse.Conn
}

func NewRepository(dsn string) (*LogRepository, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("clickhouse dsn is required")
	}
	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, err
	}
	db := clickhouse.OpenDB(options)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	if err := createSchema(db); err != nil {
		return nil, err
	}
	return &LogRepository{db: db, conn: conn}, nil
}

func createSchema(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS logs (
		chain_id UInt64,
		block_number UInt64,
		block_hash String,
		tx_hash String,
		log_index UInt64,
		address String,
		data String,
		topics Array(String),
		removed UInt8
	) ENGINE = MergeTree
	PARTITION BY chain_id
	ORDER BY (chain_id, block_number, log_index)`)
	return err
}

func (r *LogRepository) StoreLogs(ctx context.Context, logs []domain.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	batch, err := r.conn.PrepareBatch(ctx, `INSERT INTO logs (chain_id, block_number, block_hash, tx_hash, log_index, address, data, topics, removed)`)
	if err != nil {
		return err
	}

	for _, log := range logs {
		removed := uint8(0)
		if log.Removed {
			removed = 1
		}
		if err := batch.Append(
			log.ChainID,
			log.BlockNumber,
			strings.ToLower(log.BlockHash),
			strings.ToLower(log.TxHash),
			log.LogIndex,
			strings.ToLower(log.Address),
			log.Data,
			log.Topics,
			removed,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (r *LogRepository) DeleteLogsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := r.db.ExecContext(ctx, `ALTER TABLE logs DELETE WHERE chain_id = ? AND block_number >= ?`, chainID, fromBlock)
	return err
}

func (r *LogRepository) QueryLogs(ctx context.Context, filter application.LogQueryFilter) ([]domain.LogEntry, error) {
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
		var removed uint8
		if err := rows.Scan(&log.ChainID, &log.BlockNumber, &log.BlockHash, &log.TxHash, &log.LogIndex, &log.Address, &log.Data, &log.Topics, &removed); err != nil {
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

func (r *LogRepository) QueryLogsAfter(ctx context.Context, chainID uint64, afterBlock uint64, afterLogIndex uint64, limit int) ([]domain.LogEntry, error) {
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
		return nil, err
	}
	defer rows.Close()

	var logs []domain.LogEntry
	for rows.Next() {
		var log domain.LogEntry
		var removed uint8
		if err := rows.Scan(&log.ChainID, &log.BlockNumber, &log.BlockHash, &log.TxHash, &log.LogIndex, &log.Address, &log.Data, &log.Topics, &removed); err != nil {
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

func (r *LogRepository) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return r.db.PingContext(ctx)
}
