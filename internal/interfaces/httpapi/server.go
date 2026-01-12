package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bcindex/internal/application"
	"bcindex/internal/config"
	"bcindex/internal/domain"
)

type LogStore interface {
	QueryLogs(ctx context.Context, filter application.LogQueryFilter) ([]domain.LogEntry, error)
	QueryTransactions(ctx context.Context, filter application.TransactionQueryFilter) ([]domain.TransactionSummary, error)
	LastProcessedBlock(ctx context.Context) (uint64, bool, error)
	SetLastProcessedBlock(ctx context.Context, block uint64) error
	ClearLastProcessedBlock(ctx context.Context) error
	BlockRange(ctx context.Context) (uint64, uint64, bool, error)
	Ping(ctx context.Context) error
}

type RPCStatus interface {
	LatestBlockNumber(ctx context.Context) (uint64, error)
}

type BuildInfo struct {
	Version   string
	Commit    string
	BuildTime string
}

type Server struct {
	cfg       config.Config
	store     LogStore
	rpc       RPCStatus
	metrics   *Metrics
	buildInfo BuildInfo
}

func NewServer(cfg config.Config, store LogStore, rpc RPCStatus, metrics *Metrics, buildInfo BuildInfo) (*Server, error) {
	if store == nil || rpc == nil {
		return nil, errors.New("http server dependencies must not be nil")
	}
	if metrics == nil {
		metrics = NewMetrics()
	}
	return &Server{cfg: cfg, store: store, rpc: rpc, metrics: metrics, buildInfo: buildInfo}, nil
}

func (s *Server) MetricsObserver() *Metrics {
	return s.metrics
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/logs", s.handleLogs)
	mux.HandleFunc("/transactions", s.handleTransactions)
	mux.HandleFunc("/state", s.handleState)
	mux.HandleFunc("/filters", s.handleFilters)
	mux.HandleFunc("/blocks", s.handleBlocks)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/version", s.handleVersion)
	mux.HandleFunc("/reindex", s.handleReindex)
	return mux
}

func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	server := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.store.Ping(ctx); err != nil {
		respondError(w, http.StatusServiceUnavailable, "db not ready")
		return
	}
	if _, err := s.rpc.LatestBlockNumber(ctx); err != nil {
		respondError(w, http.StatusServiceUnavailable, "rpc not ready")
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	filter, err := parseLogFilter(r)
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	logs, err := s.store.QueryLogs(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "query failed")
		return
	}
	respondJSON(w, http.StatusOK, logs)
}

func (s *Server) handleTransactions(w http.ResponseWriter, r *http.Request) {
	filter, err := parseTransactionFilter(r)
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	transactions, err := s.store.QueryTransactions(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "query failed")
		return
	}
	respondJSON(w, http.StatusOK, transactions)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	last, ok, err := s.store.LastProcessedBlock(r.Context())
	if err != nil {
		respondError(w, http.StatusInternalServerError, "state read failed")
		return
	}
	response := map[string]any{
		"last_processed_block": last,
		"has_state":            ok,
		"config": map[string]any{
			"rpc_url":          s.cfg.RPCURL,
			"db_path":          s.cfg.DBPath,
			"http_addr":        s.cfg.HTTPAddr,
			"contract_address": s.cfg.ContractAddress,
			"topic0":           s.cfg.Topic0,
			"start_block":      s.cfg.StartBlock,
			"confirmations":    s.cfg.Confirmations,
			"batch_size":       s.cfg.BatchSize,
			"poll_interval":    s.cfg.PollInterval.String(),
		},
	}
	respondJSON(w, http.StatusOK, response)
}

func (s *Server) handleFilters(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{
		"contract_address": s.cfg.ContractAddress,
		"topic0":           s.cfg.Topic0,
	})
}

func (s *Server) handleBlocks(w http.ResponseWriter, r *http.Request) {
	minBlock, maxBlock, ok, err := s.store.BlockRange(r.Context())
	if err != nil {
		respondError(w, http.StatusInternalServerError, "block range failed")
		return
	}
	last, _, err := s.store.LastProcessedBlock(r.Context())
	if err != nil {
		respondError(w, http.StatusInternalServerError, "state read failed")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"min_block":            minBlock,
		"max_block":            maxBlock,
		"has_logs":             ok,
		"last_processed_block": last,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	snap := s.metrics.Snapshot()

	uptime := time.Since(snap.StartTime).Seconds()
	lag := float64(0)
	if snap.LastProcessed > 0 && snap.LatestBlock >= snap.LastProcessed {
		lag = float64(snap.LatestBlock - snap.LastProcessed)
	}

	fmt.Fprintf(w, "bcindex_uptime_seconds %.0f\n", uptime)
	fmt.Fprintf(w, "bcindex_latest_block %d\n", snap.LatestBlock)
	fmt.Fprintf(w, "bcindex_last_processed_block %d\n", snap.LastProcessed)
	fmt.Fprintf(w, "bcindex_last_batch_from %d\n", snap.LastBatchFrom)
	fmt.Fprintf(w, "bcindex_last_batch_to %d\n", snap.LastBatchTo)
	fmt.Fprintf(w, "bcindex_last_batch_count %d\n", snap.LastBatchCount)
	fmt.Fprintf(w, "bcindex_logs_total %d\n", snap.TotalLogsStored)
	fmt.Fprintf(w, "bcindex_block_lag %.0f\n", lag)
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, s.buildInfo)
}

func (s *Server) handleReindex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	from, err := parseUintParam(r, "from_block")
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	if from == 0 {
		if err := s.store.ClearLastProcessedBlock(r.Context()); err != nil {
			respondError(w, http.StatusInternalServerError, "failed to reset state")
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{
			"status": "ok",
		})
		return
	}

	target := from - 1
	if err := s.store.SetLastProcessedBlock(r.Context(), target); err != nil {
		respondError(w, http.StatusInternalServerError, "failed to update state")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"status":               "ok",
		"last_processed_block": target,
	})
}

func parseLogFilter(r *http.Request) (application.LogQueryFilter, error) {
	limit, err := parseLimit(r)
	if err != nil {
		return application.LogQueryFilter{}, err
	}
	from, to, err := parseBlockRange(r)
	if err != nil {
		return application.LogQueryFilter{}, err
	}

	return application.LogQueryFilter{
		Address:   strings.ToLower(r.URL.Query().Get("address")),
		TxHash:    r.URL.Query().Get("tx_hash"),
		FromBlock: from,
		ToBlock:   to,
		Limit:     limit,
	}, nil
}

func parseTransactionFilter(r *http.Request) (application.TransactionQueryFilter, error) {
	limit, err := parseLimit(r)
	if err != nil {
		return application.TransactionQueryFilter{}, err
	}
	from, to, err := parseBlockRange(r)
	if err != nil {
		return application.TransactionQueryFilter{}, err
	}

	return application.TransactionQueryFilter{
		Address:   strings.ToLower(r.URL.Query().Get("address")),
		TxHash:    r.URL.Query().Get("tx_hash"),
		FromBlock: from,
		ToBlock:   to,
		Limit:     limit,
	}, nil
}

func parseLimit(r *http.Request) (int, error) {
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			return 0, errors.New("invalid limit")
		}
		return value, nil
	}
	return 100, nil
}

func parseBlockRange(r *http.Request) (*uint64, *uint64, error) {
	fromRaw := r.URL.Query().Get("from_block")
	toRaw := r.URL.Query().Get("to_block")

	var from *uint64
	var to *uint64

	if fromRaw != "" {
		value, err := strconv.ParseUint(fromRaw, 10, 64)
		if err != nil {
			return nil, nil, errors.New("invalid from_block")
		}
		from = &value
	}
	if toRaw != "" {
		value, err := strconv.ParseUint(toRaw, 10, 64)
		if err != nil {
			return nil, nil, errors.New("invalid to_block")
		}
		to = &value
	}
	return from, to, nil
}

func parseUintParam(r *http.Request, key string) (uint64, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			return 0, fmt.Errorf("%s is required", key)
		}
		valueAny, ok := payload[key]
		if !ok {
			return 0, fmt.Errorf("%s is required", key)
		}
		switch v := valueAny.(type) {
		case float64:
			return uint64(v), nil
		case string:
			return strconv.ParseUint(v, 10, 64)
		default:
			return 0, fmt.Errorf("invalid %s", key)
		}
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return value, nil
}

func respondJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}
