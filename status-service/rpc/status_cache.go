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
		response, err := createArchiverStatusResponse(s.statusProvider)
		if err != nil {
			return nil, errors.Wrap(err, "creating archiver status")
		}
		s.archiverStatusCache.Set(archiverStatusKey, response, ttlcache.DefaultTTL)
		return response, nil
	} else {
		return item.Value(), nil
	}

}

func createArchiverStatusResponse(sp StatusProvider) (*protobuf.GetArchiverStatusResponse, error) {
	lastProcessedTick, err := sp.GetLastProcessedTick()
	if err != nil {
		return nil, errors.Wrap(err, "getting last processed tick")
	}
	archiverStatus, err := sp.GetArchiverStatus()
	if err != nil {
		return nil, errors.Wrap(err, "getting archiver status")
	}

	// replace last processed tick (keep intervals as is)
	epoch := findEpoch(archiverStatus.ProcessedTickIntervalsPerEpoch, lastProcessedTick)
	archiverStatus.LastProcessedTick.TickNumber = lastProcessedTick
	archiverStatus.LastProcessedTick.Epoch = epoch

	return archiverStatus, nil
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
