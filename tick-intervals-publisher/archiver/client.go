package archiver

import (
	"context"
	"fmt"

	archiverproto "github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/tick-intervals-publisher/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	api archiverproto.ArchiveServiceClient
}

func NewClient(host string) (*Client, error) {
	archiverConn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("creating archiver api connection: %v", err)
	}
	cl := Client{
		api: archiverproto.NewArchiveServiceClient(archiverConn),
	}
	return &cl, nil
}

func (c *Client) GetStatus(ctx context.Context) (*domain.Status, error) {
	s, err := c.api.GetStatus(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("calling GetStatus api: %w", err)
	}
	return convertArchiverStatus(s)
}

func convertArchiverStatus(statusResponse *archiverproto.GetStatusResponse) (*domain.Status, error) {
	var intervals []*domain.TickInterval
	for _, epochIntervals := range statusResponse.GetProcessedTickIntervalsPerEpoch() {
		for _, interval := range epochIntervals.Intervals {
			intervals = append(intervals, &domain.TickInterval{
				Epoch: epochIntervals.Epoch,
				From:  interval.InitialProcessedTick,
				To:    interval.LastProcessedTick,
			})
		}
	}
	status := domain.Status{
		LatestTick:    statusResponse.GetLastProcessedTick().GetTickNumber(),
		LatestEpoch:   statusResponse.GetLastProcessedTick().GetEpoch(),
		TickIntervals: intervals,
	}
	return &status, nil
}
