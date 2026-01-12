package ethrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"bcindex/internal/domain"
)

type Client struct {
	url        string
	httpClient *http.Client
	idCounter  uint64
	address    string
	topic0     string
}

type Config struct {
	URL     string
	Address string
	Topic0  string
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.URL == "" {
		return nil, errors.New("rpc url is required")
	}
	return &Client{
		url:        cfg.URL,
		httpClient: &http.Client{},
		address:    strings.ToLower(cfg.Address),
		topic0:     strings.ToLower(cfg.Topic0),
	}, nil
}

func (c *Client) LatestBlockNumber(ctx context.Context) (uint64, error) {
	var result string
	if err := c.call(ctx, "eth_blockNumber", []any{}, &result); err != nil {
		return 0, err
	}
	return parseHexUint(result)
}

func (c *Client) FetchLogs(ctx context.Context, fromBlock, toBlock uint64) ([]domain.LogEntry, error) {
	filter := map[string]any{
		"fromBlock": formatHexUint(fromBlock),
		"toBlock":   formatHexUint(toBlock),
	}
	if c.address != "" {
		filter["address"] = c.address
	}
	if c.topic0 != "" {
		filter["topics"] = []any{c.topic0}
	}

	var result []rpcLog
	if err := c.call(ctx, "eth_getLogs", []any{filter}, &result); err != nil {
		return nil, err
	}

	logs := make([]domain.LogEntry, 0, len(result))
	for _, log := range result {
		blockNumber, err := parseHexUint(log.BlockNumber)
		if err != nil {
			return nil, err
		}
		logIndex, err := parseHexUint(log.LogIndex)
		if err != nil {
			return nil, err
		}
		logs = append(logs, domain.LogEntry{
			BlockNumber: blockNumber,
			TxHash:      log.TxHash,
			LogIndex:    logIndex,
			Address:     strings.ToLower(log.Address),
			Data:        log.Data,
			Topics:      log.Topics,
			Removed:     log.Removed,
		})
	}

	return logs, nil
}

type rpcLog struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber string   `json:"blockNumber"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"`
	Removed     bool     `json:"removed"`
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (c *Client) call(ctx context.Context, method string, params []any, result any) error {
	id := atomic.AddUint64(&c.idCounter, 1)
	payload, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("rpc status %d", resp.StatusCode)
	}

	var decoded rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return err
	}
	if decoded.Error != nil {
		return fmt.Errorf("rpc error %d: %s", decoded.Error.Code, decoded.Error.Message)
	}
	if result == nil {
		return nil
	}
	if len(decoded.Result) == 0 {
		return errors.New("rpc result is empty")
	}
	return json.Unmarshal(decoded.Result, result)
}

func parseHexUint(value string) (uint64, error) {
	trimmed := strings.TrimPrefix(value, "0x")
	if trimmed == "" {
		return 0, errors.New("empty hex value")
	}
	return strconv.ParseUint(trimmed, 16, 64)
}

func formatHexUint(value uint64) string {
	return fmt.Sprintf("0x%x", value)
}
