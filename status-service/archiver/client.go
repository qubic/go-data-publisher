package archiver

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/protobuff"
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

func (c *Client) GetStatus(ctx context.Context) (*protobuff.GetStatusResponse, error) {
	s, err := c.api.GetStatus(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "calling GetStatus api")
	}

	return s, nil
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
