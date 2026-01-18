package application

import (
	"context"
	"errors"
	"log"

	"bcindex/internal/domain"
	"bcindex/internal/streaming"
)

type ComputeRepository interface {
	StoreLogs(ctx context.Context, logs []domain.LogEntry) error
	StoreBlocks(ctx context.Context, blocks []domain.BlockRecord) error
	DeleteLogsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error
	DeleteBlocksFrom(ctx context.Context, chainID uint64, fromBlock uint64) error
	ClearLastProcessedBlock(ctx context.Context, chainID uint64) error
	SetLastProcessedBlock(ctx context.Context, chainID uint64, block uint64) error
}

type ComputeBalanceRepository interface {
	ComputeRepository
	BalanceRepository
}

func ApplyMessage(ctx context.Context, repo ComputeRepository, msg streaming.Message) error {

	log.Printf("consume %v on topic", msg)

	if repo == nil {
		return errors.New("compute repository is required")
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
		return repo.StoreLogs(ctx, []domain.LogEntry{entry})
	case streaming.MessageTypeBlock:
		block := domain.BlockRecord{
			ChainID:     msg.ChainID,
			BlockNumber: msg.BlockNumber,
			BlockHash:   msg.BlockHash,
		}
		if err := repo.StoreBlocks(ctx, []domain.BlockRecord{block}); err != nil {
			return err
		}
		return repo.SetLastProcessedBlock(ctx, msg.ChainID, msg.BlockNumber)
	case streaming.MessageTypeReorg:
		from := msg.FromBlock
		if err := repo.DeleteLogsFrom(ctx, msg.ChainID, from); err != nil {
			return err
		}
		if err := repo.DeleteBlocksFrom(ctx, msg.ChainID, from); err != nil {
			return err
		}
		if from == 0 {
			return repo.ClearLastProcessedBlock(ctx, msg.ChainID)
		}
		return repo.SetLastProcessedBlock(ctx, msg.ChainID, from-1)
	default:
		return errors.New("unknown message type")
	}
}
