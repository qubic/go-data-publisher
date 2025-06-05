package domain

import (
	"github.com/qubic/go-archiver/protobuff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDomain_ConvertArchiverStatus(t *testing.T) {

	archiverStatus := &protobuff.GetStatusResponse{
		LastProcessedTick: &protobuff.ProcessedTick{
			TickNumber: 12345,
			Epoch:      123,
		},
		ProcessedTickIntervalsPerEpoch: []*protobuff.ProcessedTickIntervalsPerEpoch{
			{
				Epoch: 100,
				Intervals: []*protobuff.ProcessedTickInterval{
					{
						InitialProcessedTick: 1,
						LastProcessedTick:    1000,
					},
				},
			},
			{
				Epoch: 123,
				Intervals: []*protobuff.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000,
						LastProcessedTick:    123456,
					},
				},
			},
		},
	}

	expectedStatus := &Status{
		Epoch:       123,
		Tick:        12345,
		InitialTick: 10000,
		TickIntervals: []*TickInterval{
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

	convertedStatus, err := ConvertFromArchiverStatus(archiverStatus)
	require.NoError(t, err)
	assert.Equal(t, expectedStatus, convertedStatus)

}
