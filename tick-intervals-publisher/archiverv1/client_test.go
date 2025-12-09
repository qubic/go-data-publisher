package archiverv1

import (
	"testing"

	archproto "github.com/qubic/go-archiver/protobuff"
	"github.com/stretchr/testify/require"
)

func TestArchiverClient_convertStatus(t *testing.T) {
	archiverIntervalsPerEpoch := []*archproto.ProcessedTickIntervalsPerEpoch{{
		Epoch: 1,
		Intervals: []*archproto.ProcessedTickInterval{
			{
				InitialProcessedTick: 1,
				LastProcessedTick:    99,
			},
			{
				InitialProcessedTick: 1000,
				LastProcessedTick:    2000,
			},
		},
	}, {
		Epoch: 2,
		Intervals: []*archproto.ProcessedTickInterval{
			{
				InitialProcessedTick: 3000,
				LastProcessedTick:    3999,
			},
		},
	}}
	response := archproto.GetStatusResponse{
		LastProcessedTick: &archproto.ProcessedTick{
			TickNumber: 3999,
			Epoch:      2,
		},
		ProcessedTickIntervalsPerEpoch: archiverIntervalsPerEpoch,
	}

	status, err := convertArchiverStatus(&response)
	require.NoError(t, err)
	require.NotNil(t, status)
	require.NotEmpty(t, status)

	require.Equal(t, 3999, int(status.LatestTick))
	require.Equal(t, 2, int(status.LatestEpoch))

	intervals := status.TickIntervals
	require.NotEmpty(t, intervals)
	require.Len(t, intervals, 3)

	require.Equal(t, 1, int(intervals[0].Epoch))
	require.Equal(t, 1, int(intervals[1].Epoch))
	require.Equal(t, 2, int(intervals[2].Epoch))

	require.Equal(t, 1, int(intervals[0].From))
	require.Equal(t, 1000, int(intervals[1].From))
	require.Equal(t, 3000, int(intervals[2].From))

	require.Equal(t, 99, int(intervals[0].To))
	require.Equal(t, 2000, int(intervals[1].To))
	require.Equal(t, 3999, int(intervals[2].To))
}
