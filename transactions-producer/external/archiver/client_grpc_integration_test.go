package archiver

import (
	"context"
	"encoding/hex"
	"net"
	"strings"
	"testing"

	archiverproto "github.com/qubic/go-archiver-v2/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type testArchiveServer struct {
	archiverproto.UnimplementedArchiveServiceServer
	getTickTransactionsV2Func func(ctx context.Context, in *archiverproto.GetTickTransactionsRequestV2) (*archiverproto.GetTickTransactionsResponseV2, error)
}

func (s *testArchiveServer) GetTickTransactionsV2(ctx context.Context, in *archiverproto.GetTickTransactionsRequestV2) (*archiverproto.GetTickTransactionsResponseV2, error) {
	if s.getTickTransactionsV2Func != nil {
		return s.getTickTransactionsV2Func(ctx, in)
	}
	return nil, nil
}

func TestGetTickTransactions_MaxReceiveSize(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	s := grpc.NewServer()
	mockServer := &testArchiveServer{}
	archiverproto.RegisterArchiveServiceServer(s, mockServer)

	go func() {
		_ = s.Serve(lis)
	}()
	t.Cleanup(s.Stop)

	// 18 megabytes is enough for 4096 transactions with 2048 bytes payload
	client, err := NewClient(lis.Addr().String(), 18*1024*1024)
	require.NoError(t, err)

	expectedTick := uint32(123)

	// Define behavior
	mockServer.getTickTransactionsV2Func = func(ctx context.Context, in *archiverproto.GetTickTransactionsRequestV2) (*archiverproto.GetTickTransactionsResponseV2, error) {
		if in.TickNumber != expectedTick {
			return nil, nil
		}

		const transactionCount int = 4096
		const payloadSize int = 2048

		transactions := make([]*archiverproto.TransactionData, transactionCount)
		inputHex := hex.EncodeToString(make([]byte, payloadSize))
		signatureHex := hex.EncodeToString(make([]byte, 64))
		for i := 0; i < transactionCount; i++ {
			transactions[i] = &archiverproto.TransactionData{
				Transaction: &archiverproto.Transaction{
					TxId:         strings.Repeat("a", 60),
					SourceId:     strings.Repeat("B", 60),
					DestId:       strings.Repeat("C", 60),
					Amount:       1000,
					TickNumber:   in.TickNumber,
					InputType:    0,
					InputSize:    uint32(payloadSize),
					InputHex:     inputHex,
					SignatureHex: signatureHex,
				},
				Timestamp: 123456789,
				MoneyFlew: true,
			}
		}
		return &archiverproto.GetTickTransactionsResponseV2{
			Transactions: transactions,
		}, nil
	}

	ctx := context.Background()
	txs, err := client.GetTickTransactions(ctx, expectedTick)
	require.NoError(t, err)
	assert.Len(t, txs, 4096)

}
