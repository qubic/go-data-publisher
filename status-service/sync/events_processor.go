package sync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/metrics"
)

var ErrEventsTickNotYetProcessable = errors.New("events tick not yet processable")

type EventsSearchClient interface {
	GetEventsCountForTick(ctx context.Context, tickNumber uint32) (int, error)
}

type EventsRedisClient interface {
	GetConsumedEventLogTick(ctx context.Context, tickNumber uint32) (consumedEventLogTick domain.RedisConsumedEventLogTick, exists bool, err error)
}

type EventsDataStore interface {
	SetEventsLastProcessedTick(tick uint32) error
	GetEventsLastProcessedTick() (tick uint32, err error)
	GetSourceStatus() (status *domain.Status, err error)
}

type EventsProcessor struct {
	searchClient EventsSearchClient
	redisClient  EventsRedisClient
	dataStore    EventsDataStore

	metrics           *metrics.Metrics
	elasticQueryDelay time.Duration
	errorsCount       uint
}

func NewEventsProcessor(searchClient EventsSearchClient, redisClient EventsRedisClient, dataStore EventsDataStore, elasticQueryDelay time.Duration, metrics *metrics.Metrics) *EventsProcessor {
	return &EventsProcessor{
		searchClient:      searchClient,
		redisClient:       redisClient,
		dataStore:         dataStore,
		elasticQueryDelay: elasticQueryDelay,
		metrics:           metrics,
	}
}

func (ep *EventsProcessor) Synchronize() {
	ticker := time.Tick(1 * time.Second)
	for range ticker {
		err := ep.sync()
		if err != nil {
			ep.incrementErrorCount()
			log.Printf("Events check failed: %v", err)
			continue
		}
		ep.resetErrorCount()
	}
}

func (ep *EventsProcessor) sync() error {

	ctx := context.Background()
	// TODO: Not sure if this is alright, as this returns the intervals for the current epoch only.
	status, err := ep.dataStore.GetSourceStatus()
	if err != nil {
		return fmt.Errorf("getting stored tick range status: %w", err)
	}

	time.Sleep(ep.elasticQueryDelay)

	tick, err := ep.dataStore.GetEventsLastProcessedTick()
	if err != nil {
		return fmt.Errorf("get events last processed tick: %w", err)
	}

	_, start, end, epoch, err := calculateNextTickRange(tick, status.TickIntervals)
	if err != nil {
		return fmt.Errorf("calculating tick range: %w", err)
	}
	end = min(status.Tick, end)

	if start <= end && start > 0 && end > 0 && epoch > 0 {
		for tick := start; tick <= end; tick++ {
			err := ep.processTickEvents(ctx, tick)
			if errors.Is(err, ErrEventsTickNotYetProcessable) {
				break // caught up, wait for next cycle
			}
			if err != nil {
				return fmt.Errorf("processing events for tick %d: %w", tick, err)
			}

			err = ep.dataStore.SetEventsLastProcessedTick(tick)
			if err != nil {
				return fmt.Errorf("storing events last processed tick [%d]: %w", tick, err)
			}
			ep.metrics.SetEventsLastProcessedTick(tick)
		}
	}
	return nil

}

func (ep *EventsProcessor) processTickEvents(ctx context.Context, tick uint32) error {

	consumedEventLogTick, exists, err := ep.redisClient.GetConsumedEventLogTick(ctx, tick)
	if err != nil {
		return fmt.Errorf("getting consumed event log status for tick %d from redis: %w", tick, err)
	}

	if !exists {
		nextConsumedEventLogTick, nextExists, err := ep.redisClient.GetConsumedEventLogTick(ctx, tick+1)
		if err != nil {
			return fmt.Errorf("getting consumed event log status for tick %d from redis: %w", tick, err)
		}
		if !nextExists || time.Since(nextConsumedEventLogTick.Timestamp) < time.Second {
			return fmt.Errorf("checking tick n+1 for tick %d: %w", tick, ErrEventsTickNotYetProcessable)
		}

		elasticCount, err := ep.searchClient.GetEventsCountForTick(ctx, tick)
		if err != nil {
			return fmt.Errorf("getting event count for tick %d from elastic: %w", tick, err)
		}

		if elasticCount != 0 {
			return fmt.Errorf("events log mismatch. expected empty tick, found %d events in elastic for tick %d", elasticCount, tick)
		}

		// Safe to assume empty tick is verified if next tick exists for at least 1 second and there are no events in elastic.
		return nil
	}

	elasticCount, err := ep.searchClient.GetEventsCountForTick(ctx, tick)
	if err != nil {
		return fmt.Errorf("getting event count for tick %d from elastic: %w", tick, err)
	}

	if consumedEventLogTick.Stored != elasticCount {
		return fmt.Errorf("events log mismatch. redis: %d, elastic: %d", consumedEventLogTick.Stored, elasticCount)
	}

	return nil
}

func (ep *EventsProcessor) incrementErrorCount() {
	ep.errorsCount++
	ep.metrics.SetEventsErrors(ep.errorsCount)
}

func (ep *EventsProcessor) resetErrorCount() {
	ep.errorsCount = 0
	ep.metrics.SetEventsErrors(ep.errorsCount)
}
