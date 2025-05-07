package archiver

import (
	"context"
	"encoding/base64"
	"encoding/hex"
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

type TickData struct {
	ComputorIndex     uint32   `json:"computorIndex"`
	Epoch             uint32   `json:"epoch"`
	TickNumber        uint32   `json:"tickNumber"`
	Timestamp         uint64   `json:"timestamp"`
	VarStruct         string   `json:"varStruct,omitempty"` // []byte -> base64
	TimeLock          string   `json:"timeLock,omitempty"`  // []byte -> base64
	TransactionHashes []string `json:"transactionHashes,omitempty"`
	ContractFees      []int64  `json:"contractFees,omitempty"`
	Signature         string   `json:"signature"` // hex -> base64
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
		return nil, errors.Wrap(err, "calling GetStatus api")
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

func (c *Client) GetTickData(ctx context.Context, tickNumber uint32) (*TickData, error) {
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
		log.Printf("[INFO] Tick [%d] is empty.", tickNumber)
		return nil, nil
	}
	tickData, err := convertTickData(response.GetTickData())
	if err != nil {
		return nil, errors.Wrap(err, "converting tick data")
	}
	return tickData, nil
}

func convertTickData(td *protobuff.TickData) (*TickData, error) {
	sigBytes, err := hex.DecodeString(td.SignatureHex)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding signature hex [%s]", td.SignatureHex)
	}
	return &TickData{
		ComputorIndex:     td.ComputorIndex,
		Epoch:             td.Epoch,
		TickNumber:        td.TickNumber,
		Timestamp:         td.Timestamp,
		VarStruct:         base64.StdEncoding.EncodeToString(td.VarStruct),
		TimeLock:          base64.StdEncoding.EncodeToString(td.TimeLock),
		TransactionHashes: td.TransactionIds,
		ContractFees:      td.ContractFees,
		Signature:         base64.StdEncoding.EncodeToString(sigBytes),
	}, nil
}
