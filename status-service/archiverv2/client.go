package archiverv2

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"

	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	api protobuf.ArchiveServiceClient
}

func NewClient(host string) (*Client, error) {
	archiverConn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("creating archiver api connection: %v", err)
	}
	cl := Client{
		api: protobuf.NewArchiveServiceClient(archiverConn),
	}
	return &cl, nil
}

func (c *Client) GetStatus(ctx context.Context) (*domain.Status, error) {
	s, err := c.api.GetStatus(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("calling GetStatus api: %w", err)
	}

	domainStatus, err := convertToDomainStatus(s)
	if err != nil {
		return nil, fmt.Errorf("converting archiver status to domain status: %w", err)
	}

	return domainStatus, nil
}

func (c *Client) GetTickData(ctx context.Context, tickNumber uint32) (*domain.TickData, error) {
	request := protobuf.GetTickDataRequest{
		TickNumber: tickNumber,
	}
	response, err := c.api.GetTickData(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf("calling GetTickData api: %w", err)
	}
	if response == nil {
		return nil, errors.New("nil tick data response")
	}
	if response.GetTickData() == nil {
		log.Printf("[INFO] Archiver tick [%d] is empty.", tickNumber)
	} else if response.GetTickData().GetTransactionIds() == nil { // it's ok to call this on nil
		log.Printf("[INFO] Archiver tick [%d] has no transactions.", tickNumber)
	}
	convertedTd, err := convertTickData(response.GetTickData())
	if err != nil {
		return nil, fmt.Errorf("converting tick data: %w", err)
	}
	return convertedTd, nil // can return nil, for example in case of empty tick
}

func convertTickData(td *protobuf.TickData) (*domain.TickData, error) {
	if td == nil {
		return nil, nil
	}
	signature, err := hex.DecodeString(td.SignatureHex)
	if err != nil {
		return nil, fmt.Errorf("decoding signature hex: %w", err)
	}
	return &domain.TickData{
		ComputorIndex:  td.GetComputorIndex(),
		Epoch:          td.GetEpoch(),
		TickNumber:     td.GetTickNumber(),
		Timestamp:      td.GetTimestamp(),
		VarStruct:      td.GetVarStruct(),
		TimeLock:       td.GetTimeLock(),
		TransactionIds: td.GetTransactionIds(),
		ContractFees:   td.GetContractFees(),
		Signature:      base64.StdEncoding.EncodeToString(signature),
	}, nil

}

func convertToDomainStatus(archiverStatus *protobuf.GetStatusResponse) (*domain.Status, error) {
	lastTick := archiverStatus.GetLastProcessedTick()

	var intervals []*domain.TickInterval
	epochs := archiverStatus.GetProcessedTickIntervalsPerEpoch()
	for _, epochIntervals := range epochs {
		for _, interval := range epochIntervals.Intervals {
			intervals = append(intervals, &domain.TickInterval{
				Epoch: epochIntervals.Epoch,
				From:  interval.InitialProcessedTick,
				To:    interval.LastProcessedTick,
			})
		}
	}

	initialTick, err := calculateInitialTickOfCurrentEpoch(epochs)
	if err != nil {
		return nil, err
	}

	status := domain.Status{
		Tick:          lastTick.GetTickNumber(),
		Epoch:         lastTick.GetEpoch(),
		InitialTick:   initialTick,
		TickIntervals: intervals,
	}
	return &status, nil
}

func calculateInitialTickOfCurrentEpoch(epochs []*protobuf.ProcessedTickIntervalsPerEpoch) (uint32, error) {
	numberOfEpochs := len(epochs)
	if numberOfEpochs > 0 {
		latestEpoch := epochs[numberOfEpochs-1]
		if len(latestEpoch.GetIntervals()) > 0 {
			return latestEpoch.Intervals[len(latestEpoch.GetIntervals())-1].InitialProcessedTick, nil
		}
	}
	return 0, errors.New("calculating initial tick")
}
