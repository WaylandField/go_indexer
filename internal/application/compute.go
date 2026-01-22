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
	StoreTransactions(ctx context.Context, transactions []domain.Transaction) error
	StoreReceipts(ctx context.Context, receipts []domain.Receipt) error
	DeleteLogsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error
	DeleteBlocksFrom(ctx context.Context, chainID uint64, fromBlock uint64) error
	DeleteTransactionsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error
	DeleteReceiptsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error
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
			ParentHash:  msg.ParentHash,
			Timestamp:   msg.Timestamp,
			GasLimit:    msg.BlockGasLimit,
			GasUsed:     msg.BlockGasUsed,
			TxCount:     msg.TxCount,
		}
		if err := repo.StoreBlocks(ctx, []domain.BlockRecord{block}); err != nil {
			return err
		}
		return repo.SetLastProcessedBlock(ctx, msg.ChainID, msg.BlockNumber)
	case streaming.MessageTypeTransaction:
		tx := domain.Transaction{
			ChainID:     msg.ChainID,
			TxHash:      msg.TxHash,
			BlockNumber: msg.BlockNumber,
			BlockHash:   msg.BlockHash,
			TxIndex:     msg.TxIndex,
			From:        msg.From,
			To:          msg.To,
			Value:       msg.Value,
			Nonce:       msg.Nonce,
			Gas:         msg.Gas,
			GasPrice:    msg.GasPrice,
			Input:       msg.Input,
			TxType:      msg.TxType,
		}
		return repo.StoreTransactions(ctx, []domain.Transaction{tx})
	case streaming.MessageTypeReceipt:
		receipt := domain.Receipt{
			ChainID:           msg.ChainID,
			TxHash:            msg.TxHash,
			BlockNumber:       msg.BlockNumber,
			BlockHash:         msg.BlockHash,
			TxIndex:           msg.TxIndex,
			Status:            msg.Status,
			CumulativeGasUsed: msg.CumulativeGasUsed,
			GasUsed:           msg.ReceiptGasUsed,
			ContractAddress:   msg.ContractAddress,
			LogsBloom:         msg.LogsBloom,
			EffectiveGasPrice: msg.EffectiveGasPrice,
		}
		return repo.StoreReceipts(ctx, []domain.Receipt{receipt})
	case streaming.MessageTypeReorg:
		from := msg.FromBlock
		if err := repo.DeleteLogsFrom(ctx, msg.ChainID, from); err != nil {
			return err
		}
		if err := repo.DeleteBlocksFrom(ctx, msg.ChainID, from); err != nil {
			return err
		}
		if err := repo.DeleteTransactionsFrom(ctx, msg.ChainID, from); err != nil {
			return err
		}
		if err := repo.DeleteReceiptsFrom(ctx, msg.ChainID, from); err != nil {
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
