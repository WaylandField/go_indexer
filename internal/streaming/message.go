package streaming

import (
	"encoding/json"
	"errors"
)

type MessageType string

const (
	MessageTypeLog   MessageType = "log"
	MessageTypeBlock MessageType = "block"
	MessageTypeReorg MessageType = "reorg"
)

type Message struct {
	Type        MessageType `json:"type"`
	ChainID     uint64      `json:"chain_id"`
	TraceID     string      `json:"trace_id,omitempty"`
	BlockNumber uint64      `json:"block_number,omitempty"`
	BlockHash   string      `json:"block_hash,omitempty"`
	TxHash      string      `json:"tx_hash,omitempty"`
	LogIndex    uint64      `json:"log_index,omitempty"`
	Address     string      `json:"address,omitempty"`
	Data        string      `json:"data,omitempty"`
	Topics      []string    `json:"topics,omitempty"`
	Removed     bool        `json:"removed,omitempty"`
	FromBlock   uint64      `json:"from_block,omitempty"`
	Reason      string      `json:"reason,omitempty"`
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
