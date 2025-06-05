package db

import (
	archproto "github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"os"
	"testing"
)

func TestStore_SetAndGetLastProcessedTick(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	var tick uint32 = 123
	err = store.SetLastProcessedTick(tick)
	assert.NoError(t, err)

	retrievedTick, err := store.GetLastProcessedTick()
	assert.NoError(t, err)
	assert.Equal(t, tick, retrievedTick)
}

func TestStore_GetLastProcessedTickNotSet(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	_, err = store.GetLastProcessedTick()
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestStore_UpdateLastProcessedTick(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	var initialTick uint32 = 456
	var newTick uint32 = 789

	err = store.SetLastProcessedTick(initialTick)
	assert.NoError(t, err)

	retrievedTick, err := store.GetLastProcessedTick()
	assert.NoError(t, err)
	assert.Equal(t, initialTick, retrievedTick)

	err = store.SetLastProcessedTick(newTick)
	assert.NoError(t, err)

	retrievedTick, err = store.GetLastProcessedTick()
	assert.NoError(t, err)
	assert.Equal(t, newTick, retrievedTick)
}

func TestPebbleStore_GetSkippedTicks(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	ticks, err := store.GetSkippedTicks()
	assert.NoError(t, err)
	assert.NotNil(t, ticks)
	assert.Empty(t, ticks)
}

func TestStore_AddSkippedTick(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	assert.NoError(t, store.AddSkippedTick(12345))
	assert.NoError(t, store.AddSkippedTick(123456))

	skippedTicks, err := store.GetSkippedTicks()
	assert.NoError(t, err)
	assert.Len(t, skippedTicks, 2)
	assert.Contains(t, skippedTicks, uint32(12345))
	assert.Contains(t, skippedTicks, uint32(123456))
	assert.NotContains(t, skippedTicks, uint32(666))
}

func TestStore_SetAndGetProcessingStatus(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	status := &domain.Status{
		Epoch:       43,
		Tick:        42,
		InitialTick: 41,
		TickIntervals: []*domain.TickInterval{
			{
				Epoch: 1,
				From:  100,
				To:    200,
			},
		},
	}

	err = store.SetSourceStatus(status)
	require.NoError(t, err)

	retrievedStatus, err := store.GetSourceStatus()
	require.NoError(t, err)
	assert.Equal(t, status, retrievedStatus)
}

func TestStore_GetProcessingStatus_GivenNone_thenError(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	_, err = store.GetSourceStatus()
	require.Error(t, err)
}

func TestStore_SetAndGetArchiverStatus(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	// source format from archiver
	archiverStatus := &archproto.GetStatusResponse{
		LastProcessedTick: &archproto.ProcessedTick{
			TickNumber: 12345,
			Epoch:      123,
		},
		ProcessedTickIntervalsPerEpoch: []*archproto.ProcessedTickIntervalsPerEpoch{
			{
				Epoch: 100,
				Intervals: []*archproto.ProcessedTickInterval{
					{
						InitialProcessedTick: 1,
						LastProcessedTick:    1000,
					},
				},
			},
			{
				Epoch: 123,
				Intervals: []*archproto.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000,
						LastProcessedTick:    123456,
					},
				},
			},
		},
	}

	// target format from status service (compatible)
	expectedStatus := &protobuf.GetArchiverStatusResponse{
		LastProcessedTick: &protobuf.ProcessedTick{
			TickNumber: 12345,
			Epoch:      123,
		},
		ProcessedTickIntervalsPerEpoch: []*protobuf.ProcessedTickIntervalsPerEpoch{
			{
				Epoch: 100,
				Intervals: []*protobuf.ProcessedTickInterval{
					{
						InitialProcessedTick: 1,
						LastProcessedTick:    1000,
					},
				},
			},
			{
				Epoch: 123,
				Intervals: []*protobuf.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000,
						LastProcessedTick:    123456,
					},
				},
			},
		},
	}

	err = store.SetArchiverStatus(archiverStatus)
	require.NoError(t, err)
	retrievedStatus, err := store.GetArchiverStatus()
	require.NoError(t, err)
	assert.True(t, proto.Equal(expectedStatus, retrievedStatus))
}
