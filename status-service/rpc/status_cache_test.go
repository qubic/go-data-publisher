package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type FakeStatusProvider struct {
	lastProcessedTick uint32
	sourceStatus      *domain.Status
}

func (f *FakeStatusProvider) GetLastProcessedTick() (tick uint32, err error) {
	return f.lastProcessedTick, nil
}

func (f *FakeStatusProvider) GetSkippedTicks() ([]uint32, error) {
	panic("implement me") // not used yet
}

func (f *FakeStatusProvider) GetSourceStatus() (*domain.Status, error) {
	return f.sourceStatus, nil
}

func (f *FakeStatusProvider) GetArchiverStatus() (*protobuf.GetArchiverStatusResponse, error) {
	archiverStatus := protobuf.GetArchiverStatusResponse{
		LastProcessedTick: &protobuf.ProcessedTick{
			TickNumber: 123456,
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
	return &archiverStatus, nil
}

type FakeElasticClient struct {
	tickIntervals []*domain.TickInterval
}

func (f *FakeElasticClient) GetTickIntervals(_ context.Context, _ uint32) ([]*domain.TickInterval, error) {
	return f.tickIntervals, nil
}

func TestStatusCache_GetTickIntervals(t *testing.T) {
	tickIntervalsCache := createTickIntervalsCache()
	defer tickIntervalsCache.Stop()

	statusProvider := &FakeStatusProvider{
		lastProcessedTick: 12345,
		sourceStatus: &domain.Status{
			Epoch:       123,
			Tick:        123456,
			InitialTick: 10000,
			TickIntervals: []*domain.TickInterval{
				{
					Epoch: 123,
					From:  10000,
					To:    123456,
				},
			},
		},
	}

	elasticClient := &FakeElasticClient{
		tickIntervals: []*domain.TickInterval{
			{
				Epoch: 121,
				From:  1,
				To:    1000,
			},
			{
				Epoch: 122,
				From:  2000,
				To:    3000,
			},
		},
	}

	statusCache := NewStatusCache(statusProvider, elasticClient, nil, tickIntervalsCache)
	status, err := statusCache.GetTickIntervals(context.Background())
	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Len(t, status.Intervals, 3)
	assert.Equal(t, 1, int(status.Intervals[0].FirstTick))
	assert.Equal(t, 1000, int(status.Intervals[0].LastTick))
	assert.Equal(t, 2000, int(status.Intervals[1].FirstTick))
	assert.Equal(t, 3000, int(status.Intervals[1].LastTick))
	assert.Equal(t, 10000, int(status.Intervals[2].FirstTick))
	assert.Equal(t, 12345, int(status.Intervals[2].LastTick))
}

func TestStatusCache_GetArchiverStatus(t *testing.T) {
	tickIntervalsCache := createTickIntervalsCache()
	defer tickIntervalsCache.Stop()

	archiverStatusCache := createArchiverStatusCache()
	defer archiverStatusCache.Stop()

	statusProvider := &FakeStatusProvider{
		lastProcessedTick: 12345,
		sourceStatus: &domain.Status{
			Epoch:       123,
			Tick:        123456,
			InitialTick: 10000,
			TickIntervals: []*domain.TickInterval{
				{
					Epoch: 123,
					From:  10000,
					To:    123456,
				},
			},
		},
	}

	elasticClient := &FakeElasticClient{
		tickIntervals: []*domain.TickInterval{
			{
				Epoch: 122,
				From:  1,
				To:    1000,
			},
		},
	}

	statusCache := NewStatusCache(statusProvider, elasticClient, archiverStatusCache, tickIntervalsCache)
	status, err := statusCache.GetArchiverStatusResponse()
	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Equal(t, 12345, int(status.LastProcessedTick.TickNumber))
	assert.Equal(t, 123, int(status.LastProcessedTick.Epoch))

	assert.Len(t, status.ProcessedTickIntervalsPerEpoch, 2)
	assert.Equal(t, 1, int(status.ProcessedTickIntervalsPerEpoch[0].Intervals[0].InitialProcessedTick))
	assert.Equal(t, 1000, int(status.ProcessedTickIntervalsPerEpoch[0].Intervals[0].LastProcessedTick))
	assert.Equal(t, 10000, int(status.ProcessedTickIntervalsPerEpoch[1].Intervals[0].InitialProcessedTick))
	assert.Equal(t, 12345, int(status.ProcessedTickIntervalsPerEpoch[1].Intervals[0].LastProcessedTick)) // update tick here

	assert.Len(t, status.LastProcessedTicksPerEpoch, 2)
	assert.Equal(t, 1000, int(status.LastProcessedTicksPerEpoch[122]))
	assert.Equal(t, 12345, int(status.LastProcessedTicksPerEpoch[123]))

	assert.Len(t, status.GetSkippedTicks(), 1) // first interval starts at 1 therefore no skipped interval before
	assert.Equal(t, 1001, int(status.SkippedTicks[0].StartTick))
	assert.Equal(t, 9999, int(status.SkippedTicks[0].EndTick))

	assert.Empty(t, status.EmptyTicksPerEpoch)

}

func TestStatusCache_GetTickIntervals_givenLastTickInFirstInterval_thenReturnOnlyFirstInterval(t *testing.T) {
	tickIntervalsCache := createTickIntervalsCache()
	defer tickIntervalsCache.Stop()

	statusProvider := &FakeStatusProvider{
		lastProcessedTick: 666,
		sourceStatus: &domain.Status{
			Epoch:       123,
			Tick:        123456,
			InitialTick: 10000,
			TickIntervals: []*domain.TickInterval{
				{
					Epoch: 123,
					From:  10000,
					To:    123456,
				},
			},
		},
	}

	elasticClient := &FakeElasticClient{
		tickIntervals: []*domain.TickInterval{
			{
				Epoch: 122,
				From:  1,
				To:    1000,
			},
		},
	}

	statusCache := NewStatusCache(statusProvider, elasticClient, nil, tickIntervalsCache)
	status, err := statusCache.GetTickIntervals(context.Background())
	require.NoError(t, err)
	require.NotNil(t, status)

	assert.Len(t, status.Intervals, 1)
	assert.Equal(t, 122, int(status.Intervals[0].Epoch))
	assert.Equal(t, 1, int(status.Intervals[0].FirstTick))
	assert.Equal(t, 666, int(status.Intervals[0].LastTick))
}

func TestStatusCache_GetArchiverStatus_givenLastTickInFirstInterval_thenReturnCorrectLatestTickEpoch(t *testing.T) {
	tickIntervalsCache := createTickIntervalsCache()
	defer tickIntervalsCache.Stop()

	archiverStatusCache := createArchiverStatusCache()
	defer archiverStatusCache.Stop()

	statusProvider := &FakeStatusProvider{
		lastProcessedTick: 666,
		sourceStatus: &domain.Status{
			Epoch:       123,
			Tick:        123456,
			InitialTick: 10000,
			TickIntervals: []*domain.TickInterval{
				{
					Epoch: 123,
					From:  10000,
					To:    123456,
				},
			},
		},
	}

	elasticClient := &FakeElasticClient{
		tickIntervals: []*domain.TickInterval{
			{
				Epoch: 122,
				From:  1,
				To:    1000,
			},
		},
	}

	statusCache := NewStatusCache(statusProvider, elasticClient, archiverStatusCache, tickIntervalsCache)
	status, err := statusCache.GetArchiverStatusResponse()
	require.NoError(t, err)
	assert.Equal(t, 666, int(status.LastProcessedTick.TickNumber))
	assert.Equal(t, 122, int(status.LastProcessedTick.Epoch))

	assert.Len(t, status.ProcessedTickIntervalsPerEpoch, 1) // remove last epoch
	assert.Equal(t, 1, int(status.ProcessedTickIntervalsPerEpoch[0].Intervals[0].InitialProcessedTick))
	assert.Equal(t, 666, int(status.ProcessedTickIntervalsPerEpoch[0].Intervals[0].LastProcessedTick)) // replace with current tick

}

func createTickIntervalsCache() *ttlcache.Cache[string, []*domain.TickInterval] {
	var tickIntervalsCache = ttlcache.New[string, []*domain.TickInterval](
		ttlcache.WithTTL[string, []*domain.TickInterval](time.Nanosecond),
		ttlcache.WithDisableTouchOnHit[string, []*domain.TickInterval](), // don't refresh ttl upon getting the item from cache
	)
	go tickIntervalsCache.Start()
	return tickIntervalsCache
}

func createArchiverStatusCache() *ttlcache.Cache[string, *protobuf.GetArchiverStatusResponse] {
	var archiverStatusCache = ttlcache.New[string, *protobuf.GetArchiverStatusResponse](
		ttlcache.WithTTL[string, *protobuf.GetArchiverStatusResponse](time.Nanosecond),
		ttlcache.WithDisableTouchOnHit[string, *protobuf.GetArchiverStatusResponse](), // don't refresh ttl upon getting the item from cache
	)
	go archiverStatusCache.Start()
	return archiverStatusCache
}
