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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
			chain_id INTEGER NOT NULL DEFAULT 0,
			block_number INTEGER NOT NULL,
			block_hash TEXT NOT NULL DEFAULT '',
			tx_hash TEXT NOT NULL,
			log_index INTEGER NOT NULL,
			address TEXT NOT NULL,
			data TEXT NOT NULL,
			topics TEXT NOT NULL,
			removed INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS blocks (
			chain_id INTEGER NOT NULL,
			block_number INTEGER NOT NULL,
			block_hash TEXT NOT NULL,
			PRIMARY KEY(chain_id, block_number)
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
	if err := ensureColumn(db, "logs", "chain_id", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(db, "logs", "block_hash", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS logs_unique ON logs (chain_id, block_hash, block_number, tx_hash, log_index)`); err != nil {
		return err
	}
	return nil
}

func ensureColumn(db *sql.DB, table, column, definition string) error {
	query := fmt.Sprintf("PRAGMA table_info(%s)", table)
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition)
	_, err = db.Exec(stmt)
	return err
}

func (r *Repository) StoreLogs(ctx context.Context, logs []domain.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	ctx, span := startDBSpan(ctx, "sqlite.StoreLogs", attribute.Int("log.count", len(logs)))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO logs (chain_id, block_number, block_hash, tx_hash, log_index, address, data, topics, removed)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(chain_id, block_hash, block_number, tx_hash, log_index) DO NOTHING`)
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
	ctx, span := startDBSpan(ctx, "sqlite.StoreBlocks", attribute.Int("block.count", len(blocks)))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO blocks (chain_id, block_number, block_hash)
		VALUES (?, ?, ?)
		ON CONFLICT(chain_id, block_number) DO UPDATE SET block_hash = excluded.block_hash`)
	if err != nil {
		_ = tx.Rollback()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer stmt.Close()

	for _, block := range blocks {
		if _, err := stmt.ExecContext(ctx, block.ChainID, block.BlockNumber, block.BlockHash); err != nil {
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
	ctx, span := startDBSpan(ctx, "sqlite.DeleteBlocksFrom",
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
	ctx, span := startDBSpan(ctx, "sqlite.DeleteLogsFrom",
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

func (r *Repository) QueryLogs(ctx context.Context, filter application.LogQueryFilter) ([]domain.LogEntry, error) {
	ctx, span := startDBSpan(ctx, "sqlite.QueryLogs")
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	clauses := make([]string, 0, 4)
	args := make([]any, 0, 5)

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

func (r *Repository) QueryTransactions(ctx context.Context, filter application.TransactionQueryFilter) ([]domain.TransactionSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	clauses := make([]string, 0, 4)
	args := make([]any, 0, 5)

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

	query := `SELECT chain_id, tx_hash, MIN(block_number) as block_number, COUNT(*) as log_count FROM logs`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " GROUP BY chain_id, tx_hash ORDER BY block_number ASC LIMIT ?"

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
		if err := rows.Scan(&tx.ChainID, &tx.TxHash, &tx.BlockNumber, &tx.LogCount); err != nil {
			return nil, err
		}
		transactions = append(transactions, tx)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return transactions, nil
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
	if err := r.db.QueryRowContext(ctx, `SELECT value FROM state WHERE key = ?`, key).Scan(&value); err != nil {
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
	ctx, span := startDBSpan(ctx, "sqlite.SetLastProcessedBlock",
		attribute.Int64("chain.id", int64(chainID)),
		attribute.Int64("block.number", int64(block)),
	)
	defer span.End()
	key := stateKey(chainID)
	_, err := r.db.ExecContext(ctx, `INSERT INTO state (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, fmt.Sprintf("%d", block))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (r *Repository) ClearLastProcessedBlock(ctx context.Context, chainID uint64) error {
	ctx, span := startDBSpan(ctx, "sqlite.ClearLastProcessedBlock",
		attribute.Int64("chain.id", int64(chainID)),
	)
	defer span.End()
	key := stateKey(chainID)
	_, err := r.db.ExecContext(ctx, `DELETE FROM state WHERE key = ?`, key)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func startDBSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs = append(attrs, attribute.String("db.system", "sqlite"))
	return otel.Tracer("bcindex/sqlite").Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
}

func stateKey(chainID uint64) string {
	if chainID == 0 {
		return "last_block"
	}
	return fmt.Sprintf("last_block:%d", chainID)
}

func (r *Repository) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return r.db.PingContext(ctx)
}
