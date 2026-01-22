package streaming

import (
	"encoding/json"
	"errors"
)

type MessageType string

const (
	MessageTypeLog         MessageType = "log"
	MessageTypeBlock       MessageType = "block"
	MessageTypeTransaction MessageType = "transaction"
	MessageTypeReceipt     MessageType = "receipt"
	MessageTypeReorg       MessageType = "reorg"
)

type Message struct {
	Type              MessageType `json:"type"`
	ChainID           uint64      `json:"chain_id"`
	TraceID           string      `json:"trace_id,omitempty"`
	BlockNumber       uint64      `json:"block_number,omitempty"`
	BlockHash         string      `json:"block_hash,omitempty"`
	ParentHash        string      `json:"parent_hash,omitempty"`
	Timestamp         uint64      `json:"timestamp,omitempty"`
	BlockGasLimit     uint64      `json:"block_gas_limit,omitempty"`
	BlockGasUsed      uint64      `json:"block_gas_used,omitempty"`
	TxCount           uint64      `json:"tx_count,omitempty"`
	TxHash            string      `json:"tx_hash,omitempty"`
	LogIndex          uint64      `json:"log_index,omitempty"`
	Address           string      `json:"address,omitempty"`
	Data              string      `json:"data,omitempty"`
	Topics            []string    `json:"topics,omitempty"`
	Removed           bool        `json:"removed,omitempty"`
	TxIndex           uint64      `json:"tx_index,omitempty"`
	From              string      `json:"from,omitempty"`
	To                string      `json:"to,omitempty"`
	Value             string      `json:"value,omitempty"`
	Nonce             uint64      `json:"nonce,omitempty"`
	Gas               uint64      `json:"gas,omitempty"`
	GasPrice          string      `json:"gas_price,omitempty"`
	Input             string      `json:"input,omitempty"`
	TxType            uint64      `json:"tx_type,omitempty"`
	Status            uint64      `json:"status,omitempty"`
	CumulativeGasUsed uint64      `json:"cumulative_gas_used,omitempty"`
	ReceiptGasUsed    uint64      `json:"receipt_gas_used,omitempty"`
	ContractAddress   string      `json:"contract_address,omitempty"`
	LogsBloom         string      `json:"logs_bloom,omitempty"`
	EffectiveGasPrice string      `json:"effective_gas_price,omitempty"`
	FromBlock         uint64      `json:"from_block,omitempty"`
	Reason            string      `json:"reason,omitempty"`
}

func Encode(msg Message) ([]byte, error) {
	if msg.Type == "" {
		return nil, errors.New("message type is required")
	}
	if msg.ChainID == 0 {
		return nil, errors.New("chain_id is required")
	}
	return json.Marshal(msg)
}

func Decode(payload []byte) (Message, error) {
	var msg Message
	if err := json.Unmarshal(payload, &msg); err != nil {
		return Message{}, err
	}
	if msg.Type == "" {
		return Message{}, errors.New("message type is missing")
	}
	if msg.ChainID == 0 {
		return Message{}, errors.New("chain_id is missing")
	}
	return msg, nil
}
