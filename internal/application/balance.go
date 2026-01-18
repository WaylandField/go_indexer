package application

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"bcindex/internal/config"
	"bcindex/internal/domain"
	"bcindex/internal/streaming"
)

type BalanceRepository interface {
	AddBalanceDelta(ctx context.Context, chainID uint64, address string, delta *big.Int) error
	ResetBalances(ctx context.Context, chainID uint64) error
	QueryLogsAfter(ctx context.Context, chainID uint64, afterBlock uint64, afterLogIndex uint64, limit int) ([]domain.LogEntry, error)
}

func ApplyBalanceForMessage(ctx context.Context, repo BalanceRepository, msg streaming.Message, cfg config.Config) error {
	if repo == nil {
		return nil
	}
	if cfg.Topic0 == "" {
		return nil
	}

	switch msg.Type {
	case streaming.MessageTypeLog:
		entry := domain.LogEntry{
			ChainID:     msg.ChainID,
			BlockNumber: msg.BlockNumber,
			BlockHash:   msg.BlockHash,
			TxHash:      msg.TxHash,
			LogIndex:    msg.LogIndex,
			Address:     msg.Address,
			Data:        msg.Data,
			Topics:      msg.Topics,
			Removed:     msg.Removed,
		}
		return applyBalanceForLog(ctx, repo, entry, cfg.Topic0)
	case streaming.MessageTypeReorg:
		return rebuildBalances(ctx, repo, msg.ChainID, cfg.Topic0)
	default:
		return nil
	}
}

func applyBalanceForLog(ctx context.Context, repo BalanceRepository, log domain.LogEntry, topic0 string) error {
	if len(log.Topics) == 0 || !strings.EqualFold(log.Topics[0], topic0) {
		return nil
	}
	if len(log.Topics) < 2 {
		return errors.New("missing indexed address topic")
	}
	address, err := decodeTopicAddress(log.Topics[1])
	if err != nil {
		return err
	}
	amount, err := decodeUint256(log.Data)
	if err != nil {
		return err
	}
	if amount.Sign() == 0 {
		return nil
	}
	return repo.AddBalanceDelta(ctx, log.ChainID, address, amount)
}

func rebuildBalances(ctx context.Context, repo BalanceRepository, chainID uint64, topic0 string) error {
	if err := repo.ResetBalances(ctx, chainID); err != nil {
		return err
	}
	var (
		afterBlock    uint64
		afterLogIndex uint64
	)
	for {
		logs, err := repo.QueryLogsAfter(ctx, chainID, afterBlock, afterLogIndex, 1000)
		if err != nil {
			return err
		}
		if len(logs) == 0 {
			return nil
		}
		for _, log := range logs {
			if err := applyBalanceForLog(ctx, repo, log, topic0); err != nil {
				return err
			}
			afterBlock = log.BlockNumber
			afterLogIndex = log.LogIndex
		}
	}
}

func decodeTopicAddress(topic string) (string, error) {
	if !strings.HasPrefix(topic, "0x") || len(topic) != 66 {
		return "", fmt.Errorf("invalid topic address: %s", topic)
	}
	return "0x" + topic[26:], nil
}

func decodeUint256(data string) (*big.Int, error) {
	clean := strings.TrimPrefix(data, "0x")
	if len(clean) < 64 {
		return nil, fmt.Errorf("invalid data length: %d", len(clean))
	}
	value := new(big.Int)
	_, ok := value.SetString(clean[:64], 16)
	if !ok {
		return nil, errors.New("failed to parse uint256")
	}
	return value, nil
}
