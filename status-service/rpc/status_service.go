package rpc

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/protobuf"
)

type ElasticClient interface {
	GetTickIntervals(ctx context.Context, beforeEpoch uint32) ([]*domain.TickInterval, error)
}

type StatusProvider interface {
	GetLastProcessedTick() (tick uint32, err error)
	GetLastProcessedEpoch() (tick uint32, err error)
	GetSkippedTicks() ([]uint32, error)
	GetSourceStatus() (*domain.Status, error)
}

type StatusService struct {
	database            StatusProvider
	elastic             ElasticClient
	archiverStatusCache *ttlcache.Cache[string, *protobuf.GetArchiverStatusResponse]
	archiverStatusLock  sync.Mutex
	tickIntervalsCache  *ttlcache.Cache[string, []*domain.TickInterval]
	tickIntervalLock    sync.Mutex
}

func NewStatusService(database StatusProvider, elastic ElasticClient, archiverStatusCache *ttlcache.Cache[string, *protobuf.GetArchiverStatusResponse],
	tickIntervalsCache *ttlcache.Cache[string, []*domain.TickInterval]) *StatusService {

	return &StatusService{
		database:            database,
		elastic:             elastic,
		archiverStatusCache: archiverStatusCache,
		tickIntervalsCache:  tickIntervalsCache,
	}
}

func (s *StatusService) GetLastProcessedTick() (tick uint32, err error) {
	return s.database.GetLastProcessedTick()
}

func (s *StatusService) GetLastProcessedEpoch() (tick uint32, err error) {
	return s.database.GetLastProcessedEpoch()
}

func (s *StatusService) GetErroneousSkippedTicks() ([]uint32, error) {
	return s.database.GetSkippedTicks()
}

func (s *StatusService) GetSourceStatus() (*domain.Status, error) {
	return s.database.GetSourceStatus()
}

func (s *StatusService) GetTickIntervals(ctx context.Context) (*protobuf.GetTickIntervalsResponse, error) {

	// get archiver status (last epoch only)
	sourceStatus, err := s.database.GetSourceStatus() // load from db
	if err != nil {
		return nil, fmt.Errorf("getting source status: %w", err)
	}

	lastProcessedTick, err := s.database.GetLastProcessedTick() // could be lower than archiver status latest tick
	if err != nil {
		return nil, fmt.Errorf("getting last processed tick: %w", err)
	}

	// get intervals up to epoch - 1
	elasticIntervals, err := s.getElasticIntervals(ctx, sourceStatus.Epoch) // get from cache
	if err != nil {
		return nil, fmt.Errorf("getting elastic intervals: %w", err)
	}

	intervals := make([]*protobuf.TickInterval, 0, len(elasticIntervals)+len(sourceStatus.TickIntervals))
	for _, interval := range elasticIntervals {
		intervals = appendIfInRange(intervals, interval, lastProcessedTick)
	}
	for _, interval := range sourceStatus.TickIntervals {
		intervals = appendIfInRange(intervals, interval, lastProcessedTick)
	}

	return &protobuf.GetTickIntervalsResponse{
		Intervals: intervals,
	}, nil
}

func appendIfInRange(intervals []*protobuf.TickInterval, interval *domain.TickInterval, endTick uint32) []*protobuf.TickInterval {
	if endTick >= interval.From && endTick <= interval.To {
		return append(intervals, &protobuf.TickInterval{
			Epoch:     interval.Epoch,
			FirstTick: interval.From,
			LastTick:  endTick, // fix last interval
		})
	} else if endTick > interval.To {
		return append(intervals, &protobuf.TickInterval{
			Epoch:     interval.Epoch,
			FirstTick: interval.From,
			LastTick:  interval.To,
		})
	} else { // else do nothing because not in range (endTick < interval.To)
		return intervals
	}
}

func (s *StatusService) GetArchiverStatusResponse() (*protobuf.GetArchiverStatusResponse, error) {
	s.archiverStatusLock.Lock() // lock so that we do not get multiple threads inside the `if`
	defer s.archiverStatusLock.Unlock()

	item := s.archiverStatusCache.Get(archiverStatusKey)
	if item == nil {
		tickIntervals, err := s.GetTickIntervals(context.Background())
		if err != nil {
			return nil, fmt.Errorf("getting tick intervals: %w", err)
		}
		response, err := s.createArchiverStatusResponse(tickIntervals)
		if err != nil {
			return nil, fmt.Errorf("creating archiver status: %w", err)
		}
		s.archiverStatusCache.Set(archiverStatusKey, response, ttlcache.DefaultTTL)
		return response, nil
	} else {
		return item.Value(), nil
	}
}

func (s *StatusService) getElasticIntervals(ctx context.Context, beforeEpoch uint32) ([]*domain.TickInterval, error) {
	s.tickIntervalLock.Lock() // lock so that we do not get multiple threads inside the `if`
	defer s.tickIntervalLock.Unlock()

	item := s.tickIntervalsCache.Get(tickIntervalsKey)
	if item == nil {
		start := time.Now()
		elasticIntervals, err := s.elastic.GetTickIntervals(ctx, beforeEpoch)
		if err != nil {
			return nil, fmt.Errorf("getting elastic intervals: %w", err)
		}
		s.tickIntervalsCache.Set(tickIntervalsKey, elasticIntervals, ttlcache.DefaultTTL)
		log.Printf("Updated intervals cache. Took %d ms.", time.Since(start).Milliseconds())
		return elasticIntervals, nil
	} else {
		return item.Value(), nil
	}
}

func (s *StatusService) createArchiverStatusResponse(tickIntervals *protobuf.GetTickIntervalsResponse) (*protobuf.GetArchiverStatusResponse, error) {
	tick, err := s.database.GetLastProcessedTick()
	if err != nil {
		return nil, fmt.Errorf("getting last processed tick: %w", err)
	}

	// get epoch for current tick (needed for archiver data structure)
	epoch, err := findEpoch(tickIntervals, tick)
	if err != nil {
		return nil, fmt.Errorf("finding epoch: %w", err)
	}

	status := &protobuf.GetArchiverStatusResponse{
		LastProcessedTick: &protobuf.ProcessedTick{
			TickNumber: tick,
			Epoch:      epoch,
		},
		LastProcessedTicksPerEpoch:     calculateLastProcessedTicksPerEpoch(tickIntervals),
		SkippedTicks:                   calculateSkippedTicks(tickIntervals),
		ProcessedTickIntervalsPerEpoch: calculateTickIntervalsPerEpoch(tickIntervals, tick),
		EmptyTicksPerEpoch:             make(map[uint32]uint32),
	}

	return status, nil
}

func calculateTickIntervalsPerEpoch(tickIntervals *protobuf.GetTickIntervalsResponse, tick uint32) []*protobuf.ProcessedTickIntervalsPerEpoch {
	var tickIntervalsPerEpochList []*protobuf.ProcessedTickIntervalsPerEpoch
	tickIntervalsPerEpoch := &protobuf.ProcessedTickIntervalsPerEpoch{ // dummy
		Epoch: 0,
	}
	for index, interval := range tickIntervals.GetIntervals() {
		if interval.Epoch > tickIntervalsPerEpoch.Epoch { // create new epoch
			tickIntervalsPerEpoch = &protobuf.ProcessedTickIntervalsPerEpoch{
				Epoch:     interval.Epoch,
				Intervals: []*protobuf.ProcessedTickInterval{},
			}
			tickIntervalsPerEpochList = append(tickIntervalsPerEpochList, tickIntervalsPerEpoch)
		}
		tickInterval := &protobuf.ProcessedTickInterval{
			InitialProcessedTick: interval.FirstTick,
			LastProcessedTick:    interval.LastTick,
		}
		if index == len(tickIntervals.GetIntervals())-1 {
			tickInterval.LastProcessedTick = tick
		}
		tickIntervalsPerEpoch.Intervals = append(tickIntervalsPerEpoch.Intervals, tickInterval)
	}
	return tickIntervalsPerEpochList
}

func calculateLastProcessedTicksPerEpoch(tickIntervals *protobuf.GetTickIntervalsResponse) map[uint32]uint32 {
	lastProcessedTicks := make(map[uint32]uint32)
	for _, interval := range tickIntervals.GetIntervals() {
		lastProcessedTicks[interval.Epoch] = interval.LastTick // same epoch overrides (assumes sorted ascending)
	}
	return lastProcessedTicks
}

func calculateSkippedTicks(tickIntervals *protobuf.GetTickIntervalsResponse) []*protobuf.SkippedTicksInterval {
	var skippedTicksList = make([]*protobuf.SkippedTicksInterval, 0, len(tickIntervals.GetIntervals()))
	next := uint32(1)
	for _, interval := range tickIntervals.GetIntervals() {
		if interval.FirstTick > next {
			skippedTicksList = append(skippedTicksList, &protobuf.SkippedTicksInterval{
				StartTick: next,
				EndTick:   interval.FirstTick - 1,
			})
		}
		next = interval.LastTick + 1
	}
	return skippedTicksList
}

func findEpoch(response *protobuf.GetTickIntervalsResponse, tick uint32) (uint32, error) {
	for _, interval := range response.GetIntervals() {
		if tick >= interval.FirstTick && tick <= interval.LastTick {
			return interval.Epoch, nil
		}
	}
	return 0, fmt.Errorf("no epoch found for tick [%d]", tick)
}
