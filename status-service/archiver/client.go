package archiver

import (
	"context"
	"fmt"
	"github.com/qubic/go-archiver/protobuff"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	api protobuff.ArchiveServiceClient
}

type Status struct {
	LatestEpoch   uint32
	LatestTick    uint32
	TickIntervals []*TickInterval
}

type TickInterval struct {
	Epoch uint32
	From  uint32
	To    uint32
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

func (c *Client) GetStatus(ctx context.Context) (*Status, error) {
	s, err := c.api.GetStatus(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("calling archiver: %v", err)
	}

	var intervals []*TickInterval
	for _, epochIntervals := range s.GetProcessedTickIntervalsPerEpoch() {
		for _, interval := range epochIntervals.Intervals {
			intervals = append(intervals, &TickInterval{
				Epoch: epochIntervals.Epoch,
				From:  interval.InitialProcessedTick,
				To:    interval.LastProcessedTick,
			})
		}
	}
	status := Status{
		LatestTick:    s.GetLastProcessedTick().GetTickNumber(),
		LatestEpoch:   s.GetLastProcessedTick().GetEpoch(),
		TickIntervals: intervals,
	}
	return &status, nil

}
