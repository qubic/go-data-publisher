package archiver

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	archiverproto "github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/transactions-producer/entities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	archiverClient archiverproto.ArchiveServiceClient
}

func NewClient(host string) (*Client, error) {
	archiverConn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("creating grpc connection: %v", err)
	}

	return &Client{archiverClient: archiverproto.NewArchiveServiceClient(archiverConn)}, nil
}

func (c *Client) GetTickTransactions(ctx context.Context, tick uint32) ([]entities.Tx, error) {
	resp, err := c.archiverClient.GetTickTransactionsV2(ctx, &archiverproto.GetTickTransactionsRequestV2{TickNumber: tick})
	if err != nil {
		return nil, fmt.Errorf("calling grpc method: %v", err)
	}

	entitiesTx, err := archiveTxsToEntitiesTx(resp.Transactions)
	if err != nil {
		return nil, fmt.Errorf("converting archive tx to entities tx: %v", err)
	}

	return entitiesTx, nil
}

func (c *Client) GetProcessedTickIntervalsPerEpoch(ctx context.Context) ([]entities.ProcessedTickIntervalsPerEpoch, error) {
	resp, err := c.archiverClient.GetStatus(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("calling grpc method: %v", err)
	}

	return archiveStatusToEntitiesProcessedTickIntervals(resp.ProcessedTickIntervalsPerEpoch), nil
}

func archiveTxsToEntitiesTx(archiveTxs []*archiverproto.TransactionData) ([]entities.Tx, error) {
	entitiesTx := make([]entities.Tx, 0, len(archiveTxs))

	for _, archiveTx := range archiveTxs {
		inputBytes, err := hex.DecodeString(archiveTx.Transaction.InputHex)
		if err != nil {
			return nil, fmt.Errorf("decoding input hex: %v", err)
		}
		sigBytes, err := hex.DecodeString(archiveTx.Transaction.SignatureHex)
		if err != nil {
			return nil, fmt.Errorf("decoding signature hex: %v", err)
		}

		entitiesTx = append(entitiesTx, entities.Tx{
			TxID:       archiveTx.Transaction.TxId,
			SourceID:   archiveTx.Transaction.SourceId,
			DestID:     archiveTx.Transaction.DestId,
			Amount:     archiveTx.Transaction.Amount,
			TickNumber: archiveTx.Transaction.TickNumber,
			InputType:  archiveTx.Transaction.InputType,
			InputSize:  archiveTx.Transaction.InputSize,
			Input:      base64.StdEncoding.EncodeToString(inputBytes),
			Signature:  base64.StdEncoding.EncodeToString(sigBytes),
			Timestamp:  archiveTx.Timestamp,
			MoneyFlew:  archiveTx.MoneyFlew,
		})
	}

	return entitiesTx, nil
}

func archiveStatusToEntitiesProcessedTickIntervals(ptipe []*archiverproto.ProcessedTickIntervalsPerEpoch) []entities.ProcessedTickIntervalsPerEpoch {
	startingTicksForEpochs := make([]entities.ProcessedTickIntervalsPerEpoch, 0, len(ptipe))
	for _, epochIntervals := range ptipe {
		intervals := archiveEpochIntervalsToEntitiesEpochIntervals(epochIntervals)
		startingTicksForEpochs = append(startingTicksForEpochs, intervals)
	}

	return startingTicksForEpochs
}

func archiveEpochIntervalsToEntitiesEpochIntervals(archiveEpochIntervals *archiverproto.ProcessedTickIntervalsPerEpoch) entities.ProcessedTickIntervalsPerEpoch {
	intervals := make([]entities.ProcessedTickInterval, 0, len(archiveEpochIntervals.Intervals))
	for _, interval := range archiveEpochIntervals.Intervals {
		intervals = append(intervals, entities.ProcessedTickInterval{
			InitialProcessedTick: interval.InitialProcessedTick,
			LastProcessedTick:    interval.LastProcessedTick,
		})
	}

	return entities.ProcessedTickIntervalsPerEpoch{Epoch: archiveEpochIntervals.Epoch, Intervals: intervals}
}
