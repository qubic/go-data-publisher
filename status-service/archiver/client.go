package archiver

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/status-service/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
)

type Client struct {
	api protobuff.ArchiveServiceClient
}

func NewClient(host string) (*Client, error) {
	archiverConn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("creating archiver api connection: %v", err)
	}
	cl := Client{
		api: protobuff.NewArchiveServiceClient(archiverConn),
	}
	return &cl, nil
}

func (c *Client) GetStatus(ctx context.Context) (*domain.Status, error) {
	s, err := c.api.GetStatus(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "calling GetStatus api")
	}

	var intervals []*domain.TickInterval
	epochs := s.GetProcessedTickIntervalsPerEpoch()
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
		Tick:          s.GetLastProcessedTick().GetTickNumber(),
		Epoch:         s.GetLastProcessedTick().GetEpoch(),
		InitialTick:   initialTick,
		TickIntervals: intervals,
	}
	return &status, nil
}

func (c *Client) GetTickData(ctx context.Context, tickNumber uint32) (*protobuff.TickData, error) {
	request := protobuff.GetTickDataRequest{
		TickNumber: tickNumber,
	}
	response, err := c.api.GetTickData(ctx, &request)
	if err != nil {
		return nil, errors.Wrap(err, "calling GetTickData api")
	}
	if response == nil {
		return nil, errors.New("nil tick data response")
	}
	if response.GetTickData() == nil {
		log.Printf("[INFO] Archiver tick [%d] is empty.", tickNumber)
	} else if response.GetTickData().GetTransactionIds() == nil { // it's ok to call this on nil
		log.Printf("[INFO] Archiver tick [%d] has no transactions.", tickNumber)
	}
	return response.GetTickData(), nil // can return nil, for example in case of empty tick
}

func calculateInitialTickOfCurrentEpoch(epochs []*protobuff.ProcessedTickIntervalsPerEpoch) (uint32, error) {
	numberOfEpochs := len(epochs)
	if numberOfEpochs > 0 {
		latestEpoch := epochs[numberOfEpochs-1]
		if len(latestEpoch.GetIntervals()) > 0 {
			return latestEpoch.Intervals[0].InitialProcessedTick, nil
		}
	}
	return 0, errors.New("calculating initial tick")
}
