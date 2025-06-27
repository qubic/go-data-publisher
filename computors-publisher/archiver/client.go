package archiver

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"github.com/pkg/errors"
	"github.com/qubic/computors-publisher/domain"
	"github.com/qubic/go-archiver/protobuff"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"slices"
)

type Client struct {
	api protobuff.ArchiveServiceClient
}

func NewClient(host string) (*Client, error) {
	archiverConnection, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Wrap(err, "creating archiver connection")
	}

	return &Client{api: protobuff.NewArchiveServiceClient(archiverConnection)}, nil
}

func (c *Client) GetStatus(ctx context.Context) (*domain.Status, error) {
	status, err := c.api.GetStatus(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "getting archiver status")
	}

	var epochs []uint32

	for epoch := range status.LastProcessedTicksPerEpoch {
		epochs = append(epochs, epoch)
	}

	slices.Sort(epochs) // make sure epochs are not out of order

	return &domain.Status{EpochList: epochs}, nil
}

func (c *Client) GetEpochComputors(ctx context.Context, epoch uint32) (*domain.EpochComputors, error) {

	request := protobuff.GetComputorsRequest{
		Epoch: epoch,
	}
	response, err := c.api.GetComputors(ctx, &request)
	if err != nil {
		return nil, errors.Wrap(err, "getting archiver computor list")
	}
	if response == nil {
		return nil, errors.New("nil epoch computor list response")
	}
	if response.GetComputors() == nil {
		return nil, errors.New("nil epoch computor list")
	}

	computorList, err := convertComputorList(response.GetComputors())
	if err != nil {
		return nil, errors.Wrap(err, "converting epoch computor list")
	}

	return computorList, nil

}

func convertComputorList(computors *protobuff.Computors) (*domain.EpochComputors, error) {
	sigBytes, err := hex.DecodeString(computors.SignatureHex)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding computor list signature [%s]", computors.SignatureHex)
	}

	return &domain.EpochComputors{
		Epoch:      computors.Epoch,
		Identities: computors.Identities,
		Signature:  base64.StdEncoding.EncodeToString(sigBytes),
	}, nil
}
