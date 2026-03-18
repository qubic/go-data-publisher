package sync

import (
	"context"
	"errors"
	"testing"

	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/metrics"
	"github.com/stretchr/testify/require"
)

type FakeEventsSearchClient struct {
	count uint32
	err   error
}

func (f *FakeEventsSearchClient) GetEventsCountForTick(_ context.Context, _ uint32) (uint32, error) {
	return f.count, f.err
}

type FakeEventsRedisClient struct {
	status domain.RedisEventsLastIngestedTickStatus
	exists bool
	err    error
}

func (f *FakeEventsRedisClient) GetEventsLastIngestedTickStatus(_ context.Context) (domain.RedisEventsLastIngestedTickStatus, bool, error) {
	return f.status, f.exists, f.err
}

type FakeEventsDataStore struct {
	lastProcessedTick uint32
	getErr            error
	setErr            error
}

func (f *FakeEventsDataStore) GetEventsLastProcessedTick() (uint32, error) {
	return f.lastProcessedTick, f.getErr
}

func (f *FakeEventsDataStore) SetEventsLastProcessedTick(tick uint32) error {
	if f.setErr != nil {
		return f.setErr
	}
	f.lastProcessedTick = tick
	return nil
}

var eventsMetrics = metrics.NewMetrics("events_test")

func TestEventsProcessor_Sync_GivenMatchingCounts_ThenUpdateLastProcessedTick(t *testing.T) {

	fakeElastic := &FakeEventsSearchClient{
		count: 100,
		err:   nil,
	}
	fakeRedis := &FakeEventsRedisClient{
		status: domain.RedisEventsLastIngestedTickStatus{
			TickNumber: 32100000,
			EventCount: 100,
		},
		exists: true,
		err:    nil,
	}
	fakeStore := &FakeEventsDataStore{
		lastProcessedTick: 32000000,
		getErr:            nil,
		setErr:            nil,
	}

	eventsProcessor := NewEventsProcessor(fakeElastic, fakeRedis, fakeStore, 0, eventsMetrics)
	err := eventsProcessor.sync()
	require.NoError(t, err)
	require.Equal(t, uint32(32100000), fakeStore.lastProcessedTick)

}

func TestEventsProcessor_Sync_GivenMismatchingCounts_ThenError(t *testing.T) {
	fakeElastic := &FakeEventsSearchClient{
		count: 10,
		err:   nil,
	}
	fakeRedis := &FakeEventsRedisClient{
		status: domain.RedisEventsLastIngestedTickStatus{
			TickNumber: 32100000,
			EventCount: 100,
		},
		exists: true,
		err:    nil,
	}
	fakeStore := &FakeEventsDataStore{
		lastProcessedTick: 32000000,
		getErr:            nil,
		setErr:            nil,
	}

	eventsProcessor := NewEventsProcessor(fakeElastic, fakeRedis, fakeStore, 0, eventsMetrics)
	err := eventsProcessor.sync()
	require.Error(t, err)
	require.Equal(t, uint32(32000000), fakeStore.lastProcessedTick)
}

func TestEventsProcessor_Sync_GivenRedisKeyNotExists_ThenError(t *testing.T) {
	fakeElastic := &FakeEventsSearchClient{}
	fakeRedis := &FakeEventsRedisClient{
		exists: false,
	}
	fakeStore := &FakeEventsDataStore{
		lastProcessedTick: 32000000,
	}

	eventsProcessor := NewEventsProcessor(fakeElastic, fakeRedis, fakeStore, 0, eventsMetrics)
	err := eventsProcessor.sync()
	require.Error(t, err)
	require.Equal(t, uint32(32000000), fakeStore.lastProcessedTick)
}

func TestEventsProcessor_Sync_GivenAlreadyCaughtUp_ThenNoOp(t *testing.T) {
	fakeElastic := &FakeEventsSearchClient{}
	fakeRedis := &FakeEventsRedisClient{
		status: domain.RedisEventsLastIngestedTickStatus{
			TickNumber: 32000000,
			EventCount: 50,
		},
		exists: true,
	}
	fakeStore := &FakeEventsDataStore{
		lastProcessedTick: 32000000,
	}

	eventsProcessor := NewEventsProcessor(fakeElastic, fakeRedis, fakeStore, 0, eventsMetrics)
	err := eventsProcessor.sync()
	require.NoError(t, err)
	require.Equal(t, uint32(32000000), fakeStore.lastProcessedTick)
}

func TestEventsProcessor_Sync_GivenElasticError_ThenError(t *testing.T) {
	fakeElastic := &FakeEventsSearchClient{
		err: errors.New("es unavailable"),
	}
	fakeRedis := &FakeEventsRedisClient{
		status: domain.RedisEventsLastIngestedTickStatus{
			TickNumber: 32100000,
			EventCount: 100,
		},
		exists: true,
	}
	fakeStore := &FakeEventsDataStore{
		lastProcessedTick: 32000000,
	}

	eventsProcessor := NewEventsProcessor(fakeElastic, fakeRedis, fakeStore, 0, eventsMetrics)
	err := eventsProcessor.sync()
	require.Error(t, err)
	require.Equal(t, uint32(32000000), fakeStore.lastProcessedTick)
}

func TestEventsProcessor_Sync_GivenRedisError_ThenError(t *testing.T) {
	fakeElastic := &FakeEventsSearchClient{}
	fakeRedis := &FakeEventsRedisClient{
		err: errors.New("redis unavailable"),
	}
	fakeStore := &FakeEventsDataStore{
		lastProcessedTick: 32000000,
	}

	eventsProcessor := NewEventsProcessor(fakeElastic, fakeRedis, fakeStore, 0, eventsMetrics)
	err := eventsProcessor.sync()
	require.Error(t, err)
	require.Equal(t, uint32(32000000), fakeStore.lastProcessedTick)
}

func TestEventsProcessor_Sync_GivenDataStoreSaveError_ThenError(t *testing.T) {
	fakeElastic := &FakeEventsSearchClient{
		count: 100,
	}
	fakeRedis := &FakeEventsRedisClient{
		status: domain.RedisEventsLastIngestedTickStatus{
			TickNumber: 32100000,
			EventCount: 100,
		},
		exists: true,
	}
	fakeStore := &FakeEventsDataStore{
		lastProcessedTick: 32000000,
		setErr:            errors.New("disk full"),
	}

	eventsProcessor := NewEventsProcessor(fakeElastic, fakeRedis, fakeStore, 0, eventsMetrics)
	err := eventsProcessor.sync()
	require.Error(t, err)
	require.Equal(t, uint32(32000000), fakeStore.lastProcessedTick)
}
