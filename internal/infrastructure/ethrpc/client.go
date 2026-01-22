package ethrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"bcindex/internal/domain"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Client struct {
	url        string
	httpClient *http.Client
	idCounter  uint64
	address    string
	topic0     string
	chainMu    sync.Mutex
	chainID    *uint64
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
	if cfg.Address != "" {
		if err := validateHexString(cfg.Address, 42, "contract address"); err != nil {
			return nil, err
		}
	}
	if cfg.Topic0 != "" {
		if err := validateHexString(cfg.Topic0, 66, "topic0"); err != nil {
			return nil, err
		}
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

func (c *Client) ChainID(ctx context.Context) (uint64, error) {
	return c.fetchChainID(ctx)
}

func (c *Client) BlockHash(ctx context.Context, blockNumber uint64) (string, bool, error) {
	var result *struct {
		Hash string `json:"hash"`
	}
	if err := c.call(ctx, "eth_getBlockByNumber", []any{formatHexUint(blockNumber), false}, &result); err != nil {
		return "", false, err
	}
	if result == nil || result.Hash == "" {
		return "", false, nil
	}
	return strings.ToLower(result.Hash), true, nil
}

func (c *Client) BlockWithTransactions(ctx context.Context, blockNumber uint64) (domain.BlockRecord, []domain.Transaction, bool, error) {
	chainID, err := c.fetchChainID(ctx)
	if err != nil {
		return domain.BlockRecord{}, nil, false, err
	}

	var result *rpcBlock
	if err := c.call(ctx, "eth_getBlockByNumber", []any{formatHexUint(blockNumber), true}, &result); err != nil {
		return domain.BlockRecord{}, nil, false, err
	}
	if result == nil || result.Hash == "" {
		return domain.BlockRecord{}, nil, false, nil
	}

	timestamp, err := parseHexUint(result.Timestamp)
	if err != nil {
		return domain.BlockRecord{}, nil, false, err
	}
	gasLimit, err := parseHexUint(result.GasLimit)
	if err != nil {
		return domain.BlockRecord{}, nil, false, err
	}
	gasUsed, err := parseHexUint(result.GasUsed)
	if err != nil {
		return domain.BlockRecord{}, nil, false, err
	}

	transactions := make([]domain.Transaction, 0, len(result.Transactions))
	for _, tx := range result.Transactions {
		txIndex, err := parseHexUintOptional(tx.TransactionIndex)
		if err != nil {
			return domain.BlockRecord{}, nil, false, err
		}
		nonce, err := parseHexUintOptional(tx.Nonce)
		if err != nil {
			return domain.BlockRecord{}, nil, false, err
		}
		gas, err := parseHexUintOptional(tx.Gas)
		if err != nil {
			return domain.BlockRecord{}, nil, false, err
		}
		value, err := parseHexBigIntString(tx.Value)
		if err != nil {
			return domain.BlockRecord{}, nil, false, err
		}
		gasPrice, err := parseHexBigIntString(tx.GasPrice)
		if err != nil {
			return domain.BlockRecord{}, nil, false, err
		}
		txType, err := parseHexUintOptional(tx.Type)
		if err != nil {
			return domain.BlockRecord{}, nil, false, err
		}

		transactions = append(transactions, domain.Transaction{
			ChainID:     chainID,
			TxHash:      strings.ToLower(tx.Hash),
			BlockNumber: blockNumber,
			BlockHash:   strings.ToLower(result.Hash),
			TxIndex:     txIndex,
			From:        strings.ToLower(tx.From),
			To:          strings.ToLower(tx.To),
			Value:       value,
			Nonce:       nonce,
			Gas:         gas,
			GasPrice:    gasPrice,
			Input:       tx.Input,
			TxType:      txType,
		})
	}

	return domain.BlockRecord{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		BlockHash:   strings.ToLower(result.Hash),
		ParentHash:  strings.ToLower(result.ParentHash),
		Timestamp:   timestamp,
		GasLimit:    gasLimit,
		GasUsed:     gasUsed,
		TxCount:     uint64(len(result.Transactions)),
	}, transactions, true, nil
}

func (c *Client) TransactionReceipt(ctx context.Context, txHash string) (domain.Receipt, bool, error) {
	chainID, err := c.fetchChainID(ctx)
	if err != nil {
		return domain.Receipt{}, false, err
	}

	var result *rpcReceipt
	if err := c.call(ctx, "eth_getTransactionReceipt", []any{txHash}, &result); err != nil {
		return domain.Receipt{}, false, err
	}
	if result == nil || result.TransactionHash == "" {
		return domain.Receipt{}, false, nil
	}

	blockNumber, err := parseHexUintOptional(result.BlockNumber)
	if err != nil {
		return domain.Receipt{}, false, err
	}
	txIndex, err := parseHexUintOptional(result.TransactionIndex)
	if err != nil {
		return domain.Receipt{}, false, err
	}
	status, err := parseHexUintOptional(result.Status)
	if err != nil {
		return domain.Receipt{}, false, err
	}
	cumulativeGasUsed, err := parseHexUintOptional(result.CumulativeGasUsed)
	if err != nil {
		return domain.Receipt{}, false, err
	}
	gasUsed, err := parseHexUintOptional(result.GasUsed)
	if err != nil {
		return domain.Receipt{}, false, err
	}
	effectiveGasPrice, err := parseHexBigIntString(result.EffectiveGasPrice)
	if err != nil {
		return domain.Receipt{}, false, err
	}

	return domain.Receipt{
		ChainID:           chainID,
		TxHash:            strings.ToLower(result.TransactionHash),
		BlockNumber:       blockNumber,
		BlockHash:         strings.ToLower(result.BlockHash),
		TxIndex:           txIndex,
		Status:            status,
		CumulativeGasUsed: cumulativeGasUsed,
		GasUsed:           gasUsed,
		ContractAddress:   strings.ToLower(result.ContractAddress),
		LogsBloom:         result.LogsBloom,
		EffectiveGasPrice: effectiveGasPrice,
	}, true, nil
}

func (c *Client) FetchLogs(ctx context.Context, fromBlock, toBlock uint64) ([]domain.LogEntry, error) {
	chainID, err := c.fetchChainID(ctx)
	if err != nil {
		return nil, err
	}

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
			ChainID:     chainID,
			BlockNumber: blockNumber,
			BlockHash:   strings.ToLower(log.BlockHash),
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
	BlockHash   string   `json:"blockHash"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"`
	Removed     bool     `json:"removed"`
}

type rpcTransaction struct {
	Hash             string `json:"hash"`
	BlockHash        string `json:"blockHash"`
	BlockNumber      string `json:"blockNumber"`
	TransactionIndex string `json:"transactionIndex"`
	From             string `json:"from"`
	To               string `json:"to"`
	Value            string `json:"value"`
	Nonce            string `json:"nonce"`
	Gas              string `json:"gas"`
	GasPrice         string `json:"gasPrice"`
	Input            string `json:"input"`
	Type             string `json:"type"`
}

type rpcBlock struct {
	Hash         string           `json:"hash"`
	ParentHash   string           `json:"parentHash"`
	Number       string           `json:"number"`
	Timestamp    string           `json:"timestamp"`
	GasLimit     string           `json:"gasLimit"`
	GasUsed      string           `json:"gasUsed"`
	Transactions []rpcTransaction `json:"transactions"`
}

type rpcReceipt struct {
	TransactionHash   string `json:"transactionHash"`
	BlockHash         string `json:"blockHash"`
	BlockNumber       string `json:"blockNumber"`
	TransactionIndex  string `json:"transactionIndex"`
	Status            string `json:"status"`
	CumulativeGasUsed string `json:"cumulativeGasUsed"`
	GasUsed           string `json:"gasUsed"`
	ContractAddress   string `json:"contractAddress"`
	LogsBloom         string `json:"logsBloom"`
	EffectiveGasPrice string `json:"effectiveGasPrice"`
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
	tracer := otel.Tracer("bcindex/ethrpc")
	ctx, span := tracer.Start(ctx, "rpc."+method, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(
		attribute.String("rpc.method", method),
		attribute.String("rpc.url", c.url),
	)
	id := atomic.AddUint64(&c.idCounter, 1)
	payload, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("rpc status %d", resp.StatusCode)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return err
	}

	var decoded rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return err
	}
	if decoded.Error != nil {
		err := fmt.Errorf("rpc error %d: %s (method=%s)", decoded.Error.Code, decoded.Error.Message, method)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return err
	}
	if result == nil {
		span.End()
		return nil
	}
	if len(decoded.Result) == 0 {
		err := errors.New("rpc result is empty")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return err
	}
	if err := json.Unmarshal(decoded.Result, result); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return err
	}
	span.End()
	return nil
}

func (c *Client) fetchChainID(ctx context.Context) (uint64, error) {
	c.chainMu.Lock()
	defer c.chainMu.Unlock()
	if c.chainID != nil {
		return *c.chainID, nil
	}
	var result string
	if err := c.call(ctx, "eth_chainId", []any{}, &result); err != nil {
		return 0, err
	}
	value, err := parseHexUint(result)
	if err != nil {
		return 0, err
	}
	c.chainID = &value
	return value, nil
}

func parseHexUint(value string) (uint64, error) {
	trimmed := strings.TrimPrefix(value, "0x")
	if trimmed == "" {
		return 0, errors.New("empty hex value")
	}
	return strconv.ParseUint(trimmed, 16, 64)
}

func parseHexUintOptional(value string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	return parseHexUint(value)
}

func parseHexBigIntString(value string) (string, error) {
	trimmed := strings.TrimPrefix(value, "0x")
	if trimmed == "" {
		return "0", nil
	}
	n := new(big.Int)
	if _, ok := n.SetString(trimmed, 16); !ok {
		return "", errors.New("invalid hex big int")
	}
	return n.String(), nil
}

func formatHexUint(value uint64) string {
	return fmt.Sprintf("0x%x", value)
}

func validateHexString(value string, length int, label string) error {
	if !strings.HasPrefix(value, "0x") {
		return fmt.Errorf("%s must start with 0x", label)
	}
	if len(value) != length {
		return fmt.Errorf("%s must be %d characters", label, length)
	}
	for _, r := range value[2:] {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return fmt.Errorf("%s must be hex", label)
		}
	}
	return nil
}
