package rpc

import (
	"github.com/jellydator/ttlcache/v3"
	"github.com/pkg/errors"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/protobuf"
	"sync"
)

type StatusProvider interface {
	GetLastProcessedTick() (tick uint32, err error)
	GetSkippedTicks() ([]uint32, error)
	GetSourceStatus() (*domain.Status, error)
	GetArchiverStatus() (*protobuf.GetArchiverStatusResponse, error)
}

type StatusCache struct {
	statusProvider      StatusProvider
	archiverStatusCache *ttlcache.Cache[string, *protobuf.GetArchiverStatusResponse]
	archiverStatusLock  sync.Mutex
	tickIntervalsCache  *ttlcache.Cache[string, *protobuf.GetTickIntervalsResponse]
	tickIntervalLock    sync.Mutex
}

func NewStatusCache(statusProvider StatusProvider, archiverStatusCache *ttlcache.Cache[string, *protobuf.GetArchiverStatusResponse],
	tickIntervalsCache *ttlcache.Cache[string, *protobuf.GetTickIntervalsResponse]) *StatusCache {

	return &StatusCache{
		statusProvider:      statusProvider,
		archiverStatusCache: archiverStatusCache,
		tickIntervalsCache:  tickIntervalsCache,
	}
}

func (s *StatusCache) GetLastProcessedTick() (tick uint32, err error) {
	return s.statusProvider.GetLastProcessedTick()
}

func (s *StatusCache) GetSkippedTicks() ([]uint32, error) {
	return s.statusProvider.GetSkippedTicks()
}

func (s *StatusCache) GetSourceStatus() (*domain.Status, error) {
	return s.statusProvider.GetSourceStatus()
}

func (s *StatusCache) GetTickIntervals() (*protobuf.GetTickIntervalsResponse, error) {
	s.tickIntervalLock.Lock() // lock so that we do not get multiple threads inside the `if`
	defer s.tickIntervalLock.Unlock()

	item := s.tickIntervalsCache.Get(tickIntervalsKey)
	if item == nil {
		response, err := createTickIntervalResponse(s.statusProvider)
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
		response, err := createArchiverStatusResponse(s.statusProvider, tickIntervals)
		if err != nil {
			return nil, errors.Wrap(err, "creating archiver status")
		}
		s.archiverStatusCache.Set(archiverStatusKey, response, ttlcache.DefaultTTL)
		return response, nil
	} else {
		return item.Value(), nil
	}
}

func createArchiverStatusResponse(sp StatusProvider, tickIntervals *protobuf.GetTickIntervalsResponse) (*protobuf.GetArchiverStatusResponse, error) {
	tick, err := sp.GetLastProcessedTick()
	if err != nil {
		return nil, errors.Wrap(err, "getting last processed tick")
	}
	archiverStatus, err := sp.GetArchiverStatus()
	if err != nil {
		return nil, errors.Wrap(err, "getting archiver status")
	}

	// get epoch for current tick (needed for archiver data structure)
	epoch := findEpoch(archiverStatus.ProcessedTickIntervalsPerEpoch, tick)

	// remove lastProcessedTicksPerEpoch for later epochs
	archiverStatus.LastProcessedTicksPerEpoch = removeFutureLastProcessedTicks(epoch, tick, archiverStatus.GetLastProcessedTicksPerEpoch())

	// remove skipped ticks that are in the future
	archiverStatus.SkippedTicks = removeFutureSkippedTicks(tick, archiverStatus.GetSkippedTicks())

	// remove processed tick intervals that are in the future
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

	status := &protobuf.GetArchiverStatusResponse{
		LastProcessedTick: &protobuf.ProcessedTick{
			TickNumber: tick,
			Epoch:      epoch,
		},
		ProcessedTickIntervalsPerEpoch: tickIntervalsPerEpochList,
		LastProcessedTicksPerEpoch:     removeFutureLastProcessedTicks(epoch, tick, archiverStatus.GetLastProcessedTicksPerEpoch()),
		SkippedTicks:                   removeFutureSkippedTicks(tick, archiverStatus.GetSkippedTicks()),
		EmptyTicksPerEpoch:             removeFutureEmptyTicks(epoch, archiverStatus.GetEmptyTicksPerEpoch()),
	}

	return status, nil
}

func removeFutureEmptyTicks(epoch uint32, emptyTicks map[uint32]uint32) map[uint32]uint32 {
	for key := range emptyTicks {
		if key > epoch {
			delete(emptyTicks, key)
		}
	}
	return emptyTicks
}

func removeFutureLastProcessedTicks(epoch, tick uint32, lastProcessedTicks map[uint32]uint32) map[uint32]uint32 {
	for key := range lastProcessedTicks {
		if key > epoch {
			delete(lastProcessedTicks, key)
		} else if key == epoch {
			lastProcessedTicks[epoch] = tick
		}
	}
	return lastProcessedTicks
}

func removeFutureSkippedTicks(tick uint32, skippedTickIntervals []*protobuf.SkippedTicksInterval) []*protobuf.SkippedTicksInterval {
	for index, skipped := range skippedTickIntervals {
		if skipped.StartTick > tick {
			return skippedTickIntervals[:index]
		}
	}
	return skippedTickIntervals
}

func findEpoch(allEpochsIntervals []*protobuf.ProcessedTickIntervalsPerEpoch, tick uint32) uint32 {
	var epoch uint32
	for _, epochIntervals := range allEpochsIntervals {
		epoch = epochIntervals.Epoch
		for _, interval := range epochIntervals.Intervals {
			if tick >= interval.InitialProcessedTick && tick <= interval.LastProcessedTick {
				return epoch
			}
		}
	}
	return epoch
}

func createTickIntervalResponse(sp StatusProvider) (*protobuf.GetTickIntervalsResponse, error) {
	sourceStatus, err := sp.GetSourceStatus()
	if err != nil {
		return nil, errors.Wrap(err, "getting source status")
	}

	lastProcessedTick, err := sp.GetLastProcessedTick()
	if err != nil {
		return nil, errors.Wrap(err, "getting last processed tick")
	}

	// we don't show the last interval as the to tick might not be up to date
	// we take the initial tick from the last interval as there might be multiple intervals in the last epoch
	intervalsCount := len(sourceStatus.TickIntervals)
	intervals := make([]*protobuf.TickInterval, 0, intervalsCount)
	for _, interval := range sourceStatus.TickIntervals {
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
