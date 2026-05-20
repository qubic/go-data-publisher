package archiver

import (
	"context"
	"encoding/hex"
	"testing"

	archiverproto "github.com/qubic/go-archiver-v2/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type mockArchiveServiceClient struct {
	mock.Mock
}

func (m *mockArchiveServiceClient) GetStatus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*archiverproto.GetStatusResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*archiverproto.GetStatusResponse), args.Error(1)
}

func (m *mockArchiveServiceClient) GetTickTransactionsV2(ctx context.Context, in *archiverproto.GetTickTransactionsRequestV2, opts ...grpc.CallOption) (*archiverproto.GetTickTransactionsResponseV2, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*archiverproto.GetTickTransactionsResponseV2), args.Error(1)
}

func (m *mockArchiveServiceClient) GetTickData(ctx context.Context, in *archiverproto.GetTickDataRequest, opts ...grpc.CallOption) (*archiverproto.GetTickDataResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*archiverproto.GetTickDataResponse), args.Error(1)
}

func (m *mockArchiveServiceClient) GetTickQuorumDataV2(ctx context.Context, in *archiverproto.GetQuorumTickDataRequest, opts ...grpc.CallOption) (*archiverproto.GetQuorumTickDataResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*archiverproto.GetQuorumTickDataResponse), args.Error(1)
}

func (m *mockArchiveServiceClient) GetComputors(ctx context.Context, in *archiverproto.GetComputorsRequest, opts ...grpc.CallOption) (*archiverproto.GetComputorsResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*archiverproto.GetComputorsResponse), args.Error(1)
}

func (m *mockArchiveServiceClient) GetTransactionV2(ctx context.Context, in *archiverproto.GetTransactionRequestV2, opts ...grpc.CallOption) (*archiverproto.GetTransactionResponseV2, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*archiverproto.GetTransactionResponseV2), args.Error(1)
}

func (m *mockArchiveServiceClient) GetHealth(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*archiverproto.GetHealthResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*archiverproto.GetHealthResponse), args.Error(1)
}

func TestGetTickTransactions_ReturnsOneTransaction(t *testing.T) {
	mockArchiver := new(mockArchiveServiceClient)
	client := &Client{archiverClient: mockArchiver}

	ctx := context.Background()
	tick := uint32(123)

	expectedArchiverTx := &archiverproto.TransactionData{
		Transaction: &archiverproto.Transaction{
			TxId:         "hash1",
			SourceId:     "source1",
			DestId:       "dest1",
			Amount:       100,
			TickNumber:   tick,
			InputType:    1,
			InputSize:    0,
			InputHex:     hex.EncodeToString([]byte("input")),
			SignatureHex: hex.EncodeToString([]byte("sig")),
		},
		Timestamp: 1000,
		MoneyFlew: true,
	}

	mockArchiver.On("GetTickTransactionsV2", ctx, &archiverproto.GetTickTransactionsRequestV2{TickNumber: tick}, mock.Anything).
		Return(&archiverproto.GetTickTransactionsResponseV2{
			Transactions: []*archiverproto.TransactionData{expectedArchiverTx},
		}, nil)

	txs, err := client.GetTickTransactions(ctx, tick)
	assert.NoError(t, err)
	assert.Len(t, txs, 1)

	assert.Equal(t, "hash1", txs[0].Hash)
	assert.Equal(t, "source1", txs[0].Source)
	assert.Equal(t, "dest1", txs[0].Destination)
	assert.Equal(t, int64(100), txs[0].Amount)
	assert.Equal(t, tick, txs[0].TickNumber)
	assert.Equal(t, uint32(1), txs[0].InputType)
	assert.Equal(t, uint32(0), txs[0].InputSize)
	assert.Equal(t, "aW5wdXQ=", txs[0].InputData)
	assert.Equal(t, "c2ln", txs[0].Signature)
	assert.Equal(t, uint64(1000), txs[0].Timestamp)
	assert.Equal(t, true, txs[0].MoneyFlew)

	mockArchiver.AssertExpectations(t)
}
