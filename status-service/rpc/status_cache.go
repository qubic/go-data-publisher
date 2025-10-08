package rpc

import (
	"fmt"
	"sync"

	"github.com/jellydator/ttlcache/v3"
	"github.com/pkg/errors"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/protobuf"
)

type StatusProvider interface {
	GetLastProcessedTick() (tick uint32, err error)
	GetSkippedTicks() ([]uint32, error)
	GetSourceStatus() (*domain.Status, error)
}

type StatusCache struct {
	database            StatusProvider
	archiverStatusCache *ttlcache.Cache[string, *protobuf.GetArchiverStatusResponse]
	archiverStatusLock  sync.Mutex
	tickIntervalsCache  *ttlcache.Cache[string, *protobuf.GetTickIntervalsResponse]
	tickIntervalLock    sync.Mutex
}

func NewStatusCache(statusProvider StatusProvider, archiverStatusCache *ttlcache.Cache[string, *protobuf.GetArchiverStatusResponse],
	tickIntervalsCache *ttlcache.Cache[string, *protobuf.GetTickIntervalsResponse]) *StatusCache {

	return &StatusCache{
		database:            statusProvider,
		archiverStatusCache: archiverStatusCache,
		tickIntervalsCache:  tickIntervalsCache,
	}
}

func (s *StatusCache) GetLastProcessedTick() (tick uint32, err error) {
	return s.database.GetLastProcessedTick()
}

func (s *StatusCache) GetSkippedTicks() ([]uint32, error) {
	return s.database.GetSkippedTicks()
}

func (s *StatusCache) GetSourceStatus() (*domain.Status, error) {
	return s.database.GetSourceStatus()
}

func (s *StatusCache) GetTickIntervals() (*protobuf.GetTickIntervalsResponse, error) {
	s.tickIntervalLock.Lock() // lock so that we do not get multiple threads inside the `if`
	defer s.tickIntervalLock.Unlock()

	item := s.tickIntervalsCache.Get(tickIntervalsKey)
	if item == nil {
		response, err := s.createTickIntervalResponse()
		if err != nil {
			return nil, errors.Wrap(err, "creating tick intervals")
		}
		s.tickIntervalsCache.Set(tickIntervalsKey, response, ttlcache.DefaultTTL)
		return response, nil
	} else {
		return item.Value(), nil
	}
}

func (s *StatusCache) GetArchiverStatus() (*protobuf.GetArchiverStatusResponse, error) {
	s.archiverStatusLock.Lock() // lock so that we do not get multiple threads inside the `if`
	defer s.archiverStatusLock.Unlock()

	item := s.archiverStatusCache.Get(archiverStatusKey)
	if item == nil {
		tickIntervals, err := s.GetTickIntervals()
		if err != nil {
			return nil, errors.Wrap(err, "getting tick intervals")
		}
		response, err := s.createArchiverStatusResponse(tickIntervals)
		if err != nil {
			return nil, errors.Wrap(err, "creating archiver status")
		}
		s.archiverStatusCache.Set(archiverStatusKey, response, ttlcache.DefaultTTL)
		return response, nil
	} else {
		return item.Value(), nil
	}
}

func (s *StatusCache) createArchiverStatusResponse(tickIntervals *protobuf.GetTickIntervalsResponse) (*protobuf.GetArchiverStatusResponse, error) {
	tick, err := s.database.GetLastProcessedTick()
	if err != nil {
		return nil, errors.Wrap(err, "getting last processed tick")
	}

	// get epoch for current tick (needed for archiver data structure)
	epoch, err := findEpoch(tickIntervals, tick)

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
		// override last tick for epoch 172. data is not in archiver but in backend only.
		if interval.Epoch == 172 {
			interval.LastTick = 30897146
		}

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
		if interval.Epoch == 172 {
			lastProcessedTicks[172] = 30897146
		} else {
			lastProcessedTicks[interval.Epoch] = interval.LastTick // same epoch overrides (assumes sorted ascending)
		}
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

func (s *StatusCache) createTickIntervalResponse() (*protobuf.GetTickIntervalsResponse, error) {
	sourceStatus, err := s.database.GetSourceStatus()
	if err != nil {
		return nil, errors.Wrap(err, "getting source status")
	}

	lastProcessedTick, err := s.database.GetLastProcessedTick()
	if err != nil {
		return nil, errors.Wrap(err, "getting last processed tick")
	}

	// we don't show the last interval as the to tick might not be up to date
	// we take the initial tick from the last interval as there might be multiple intervals in the last epoch
	intervalsCount := len(sourceStatus.TickIntervals)
	intervals := make([]*protobuf.TickInterval, 0, intervalsCount)
	for _, interval := range sourceStatus.TickIntervals {
		// override last tick for epoch 172. data is not in archiver but in backend only.
		if interval.Epoch == 172 {
			interval.To = 30897146
		}

		if lastProcessedTick >= interval.From && lastProcessedTick <= interval.To {
			intervals = append(intervals, &protobuf.TickInterval{
				Epoch:     interval.Epoch,
				FirstTick: interval.From,
				LastTick:  lastProcessedTick, // fix last interval
			})
		} else if lastProcessedTick > interval.To {
			intervals = append(intervals, &protobuf.TickInterval{
				Epoch:     interval.Epoch,
				FirstTick: interval.From,
				LastTick:  interval.To,
			})
		} // else skip
	}
	return &protobuf.GetTickIntervalsResponse{
		Intervals: intervals,
	}, nil
}

func findEpoch(response *protobuf.GetTickIntervalsResponse, tick uint32) (uint32, error) {
	for _, interval := range response.GetIntervals() {
		if tick >= interval.FirstTick && tick <= interval.LastTick {
			return interval.Epoch, nil
		}
	}
	return 0, fmt.Errorf("no epoch found for tick [%d]", tick)
}
