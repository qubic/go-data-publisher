package archiver

import (
	"testing"

	archiverproto "github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConversion_convertToDomainTickData(t *testing.T) {
	archiverTickData := &archiverproto.TickData{
		ComputorIndex:  1,
		Epoch:          2,
		TickNumber:     3,
		Timestamp:      4,
		VarStruct:      []byte("var_struct"),
		TimeLock:       []byte("time_lock"),
		TransactionIds: []string{"tx-hash-1", "tx-hash-2"},
		ContractFees:   []int64{1, 2, 3},
		SignatureHex:   "7369676e61747572650a",
	}

	converted, err := convertTickData(archiverTickData)
	require.NoError(t, err)
	require.NotNil(t, converted)

	assert.Equal(t, &domain.TickData{
		ComputorIndex:  1,
		Epoch:          2,
		TickNumber:     3,
		Timestamp:      4,
		VarStruct:      []byte("var_struct"),
		TimeLock:       []byte("time_lock"),
		TransactionIds: []string{"tx-hash-1", "tx-hash-2"},
		ContractFees:   []int64{1, 2, 3},
		Signature:      "c2lnbmF0dXJlCg==",
	}, converted)
}

func TestConversion_convertToDomainStatus(t *testing.T) {

	archiverStatus := &archiverproto.GetStatusResponse{
		LastProcessedTick: &archiverproto.ProcessedTick{
			TickNumber: 12345,
			Epoch:      123,
		},
		ProcessedTickIntervalsPerEpoch: []*archiverproto.ProcessedTickIntervalsPerEpoch{
			{
				Epoch: 100,
				Intervals: []*archiverproto.ProcessedTickInterval{
					{
						InitialProcessedTick: 1,
						LastProcessedTick:    1000,
					},
				},
			},
			{
				Epoch: 123,
				Intervals: []*archiverproto.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000,
						LastProcessedTick:    123456,
					},
				},
			},
		},
	}

	expectedStatus := &domain.Status{
		Epoch:       123,
		Tick:        12345,
		InitialTick: 10000,
		TickIntervals: []*domain.TickInterval{
			{
				Epoch: 100,
				From:  1,
				To:    1000,
			},
			{
				Epoch: 123,
				From:  10000,
				To:    123456,
			},
		},
	}

	convertedStatus, err := convertToDomainStatus(archiverStatus)
	require.NoError(t, err)
	assert.Equal(t, expectedStatus, convertedStatus)
}
