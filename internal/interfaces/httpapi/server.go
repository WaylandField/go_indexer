package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"bcindex/internal/application"
	"bcindex/internal/config"
	"bcindex/internal/domain"
)

type LogStore interface {
	QueryLogs(ctx context.Context, filter application.LogQueryFilter) ([]domain.LogEntry, error)
	QueryTransactions(ctx context.Context, filter application.TransactionQueryFilter) ([]domain.Transaction, error)
	QueryBalances(ctx context.Context, filter application.BalanceQueryFilter) ([]domain.Balance, error)
	LastProcessedBlock(ctx context.Context, chainID uint64) (uint64, bool, error)
	SetLastProcessedBlock(ctx context.Context, chainID uint64, block uint64) error
	ClearLastProcessedBlock(ctx context.Context, chainID uint64) error
	BlockRange(ctx context.Context, chainID *uint64) (uint64, uint64, bool, error)
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
	mux.HandleFunc("/balances", s.handleBalances)
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

func (s *Server) handleBalances(w http.ResponseWriter, r *http.Request) {
	filter, err := parseBalanceFilter(r)
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	balances, err := s.store.QueryBalances(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "query failed")
		return
	}
	respondJSON(w, http.StatusOK, balances)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	chainID, err := parseOptionalUint(r, "chain_id")
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	state := map[string]any{}
	if chainID != nil {
		last, ok, err := s.store.LastProcessedBlock(r.Context(), *chainID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "state read failed")
			return
		}
		state["chain_id"] = *chainID
		state["last_processed_block"] = last
		state["has_state"] = ok
	} else {
		state["chains"] = s.cfg.ChainIDs
		chainStates := make(map[string]any, len(s.cfg.ChainIDs))
		for _, id := range s.cfg.ChainIDs {
			last, ok, err := s.store.LastProcessedBlock(r.Context(), id)
			if err != nil {
				respondError(w, http.StatusInternalServerError, "state read failed")
				return
			}
			chainStates[fmt.Sprintf("%d", id)] = map[string]any{
				"last_processed_block": last,
				"has_state":            ok,
			}
		}
		state["per_chain"] = chainStates
	}
	response := map[string]any{
		"state": state,
		"config": map[string]any{
			"rpc_url":          s.cfg.RPCURL,
			"db_dsn":           s.cfg.DBDSN,
			"state_db_dsn":     s.cfg.StateDBDSN,
			"clickhouse_dsn":   s.cfg.ClickhouseDSN,
			"http_addr":        s.cfg.HTTPAddr,
			"redis_addr":       s.cfg.RedisAddr,
			"otel_endpoint":    s.cfg.OtelEndpoint,
			"contract_address": s.cfg.ContractAddress,
			"topic0":           s.cfg.Topic0,
			"start_block":      s.cfg.StartBlock,
			"confirmations":    s.cfg.Confirmations,
			"batch_size":       s.cfg.BatchSize,
			"poll_interval":    s.cfg.PollInterval.String(),
			"kafka_brokers":    s.cfg.KafkaBrokers,
			"kafka_topic":      s.cfg.KafkaTopicPrefix,
			"kafka_group_id":   s.cfg.KafkaGroupID,
			"chain_ids":        s.cfg.ChainIDs,
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
	chainID, err := parseOptionalUint(r, "chain_id")
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	minBlock, maxBlock, ok, err := s.store.BlockRange(r.Context(), chainID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "block range failed")
		return
	}
	var last uint64
	var okState bool
	if chainID != nil {
		last, okState, err = s.store.LastProcessedBlock(r.Context(), *chainID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "state read failed")
			return
		}
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"chain_id":             chainID,
		"min_block":            minBlock,
		"max_block":            maxBlock,
		"has_logs":             ok,
		"last_processed_block": last,
		"has_state":            okState,
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
	fmt.Fprintf(w, "bcindex_kafka_messages_total %d\n", snap.KafkaMessages)
	fmt.Fprintf(w, "bcindex_kafka_decode_errors_total %d\n", snap.KafkaDecodeErrs)
	fmt.Fprintf(w, "bcindex_kafka_apply_errors_total %d\n", snap.KafkaApplyErrs)
	fmt.Fprintf(w, "bcindex_kafka_commit_errors_total %d\n", snap.KafkaCommitErrs)
	fmt.Fprintf(w, "bcindex_kafka_fetch_errors_total %d\n", snap.KafkaFetchErrs)
	fmt.Fprintf(w, "bcindex_kafka_last_partition %d\n", snap.KafkaLastPart)
	fmt.Fprintf(w, "bcindex_kafka_last_offset %d\n", snap.KafkaLastOffset)
	fmt.Fprintf(w, "bcindex_kafka_last_bytes %d\n", snap.KafkaLastBytes)
	if !snap.KafkaLastTime.IsZero() {
		fmt.Fprintf(w, "bcindex_kafka_last_timestamp_seconds %d\n", snap.KafkaLastTime.Unix())
	}
	if snap.KafkaLastLag > 0 {
		fmt.Fprintf(w, "bcindex_kafka_last_lag_seconds %.0f\n", snap.KafkaLastLag.Seconds())
	}
	if snap.KafkaMaxLag > 0 {
		fmt.Fprintf(w, "bcindex_kafka_max_lag_seconds %.0f\n", snap.KafkaMaxLag.Seconds())
	}
	if snap.KafkaLastTopic != "" {
		fmt.Fprintf(w, "bcindex_kafka_last_topic{topic=\"%s\"} 1\n", snap.KafkaLastTopic)
	}
	if len(snap.KafkaTopicCount) > 0 {
		topics := make([]string, 0, len(snap.KafkaTopicCount))
		for topic := range snap.KafkaTopicCount {
			topics = append(topics, topic)
		}
		sort.Strings(topics)
		for _, topic := range topics {
			fmt.Fprintf(w, "bcindex_kafka_topic_messages_total{topic=\"%s\"} %d\n", topic, snap.KafkaTopicCount[topic])
		}
	}
	if len(snap.KafkaPartCount) > 0 {
		topics := make([]string, 0, len(snap.KafkaPartCount))
		for topic := range snap.KafkaPartCount {
			topics = append(topics, topic)
		}
		sort.Strings(topics)
		for _, topic := range topics {
			partitions := make([]int, 0, len(snap.KafkaPartCount[topic]))
			for partition := range snap.KafkaPartCount[topic] {
				partitions = append(partitions, partition)
			}
			sort.Ints(partitions)
			for _, partition := range partitions {
				fmt.Fprintf(w, "bcindex_kafka_partition_messages_total{topic=\"%s\",partition=\"%d\"} %d\n", topic, partition, snap.KafkaPartCount[topic][partition])
				if lag, ok := snap.KafkaPartMaxLag[topic][partition]; ok && lag > 0 {
					fmt.Fprintf(w, "bcindex_kafka_partition_max_lag_seconds{topic=\"%s\",partition=\"%d\"} %.0f\n", topic, partition, lag.Seconds())
				}
			}
		}
	}
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, s.buildInfo)
}

func (s *Server) handleReindex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	chainID, err := parseOptionalUint(r, "chain_id")
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	targetChain := uint64(0)
	if chainID != nil {
		targetChain = *chainID
	} else if len(s.cfg.ChainIDs) == 1 {
		targetChain = s.cfg.ChainIDs[0]
	} else {
		respondError(w, http.StatusBadRequest, "chain_id is required")
		return
	}

	from, err := parseUintParam(r, "from_block")
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	if from == 0 {
		if err := s.store.ClearLastProcessedBlock(r.Context(), targetChain); err != nil {
			respondError(w, http.StatusInternalServerError, "failed to reset state")
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{
			"status":   "ok",
			"chain_id": targetChain,
		})
		return
	}

	target := from - 1
	if err := s.store.SetLastProcessedBlock(r.Context(), targetChain, target); err != nil {
		respondError(w, http.StatusInternalServerError, "failed to update state")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"status":               "ok",
		"chain_id":             targetChain,
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
	chainID, err := parseOptionalUint(r, "chain_id")
	if err != nil {
		return application.LogQueryFilter{}, err
	}

	return application.LogQueryFilter{
		ChainID:   chainID,
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
	chainID, err := parseOptionalUint(r, "chain_id")
	if err != nil {
		return application.TransactionQueryFilter{}, err
	}

	return application.TransactionQueryFilter{
		ChainID:   chainID,
		Address:   strings.ToLower(r.URL.Query().Get("address")),
		TxHash:    r.URL.Query().Get("tx_hash"),
		FromBlock: from,
		ToBlock:   to,
		Limit:     limit,
	}, nil
}

func parseBalanceFilter(r *http.Request) (application.BalanceQueryFilter, error) {
	limit, err := parseLimit(r)
	if err != nil {
		return application.BalanceQueryFilter{}, err
	}
	chainID, err := parseOptionalUint(r, "chain_id")
	if err != nil {
		return application.BalanceQueryFilter{}, err
	}
	return application.BalanceQueryFilter{
		ChainID: chainID,
		Address: strings.ToLower(r.URL.Query().Get("address")),
		Limit:   limit,
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

func parseOptionalUint(r *http.Request, key string) (*uint64, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid %s", key)
	}
	return &value, nil
}

func respondJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}
