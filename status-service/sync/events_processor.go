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

type EventsSearchClient interface {
	GetEventsCountForTick(ctx context.Context, tickNumber uint32) (uint32, error)
}

type EventsRedisClient interface {
	GetEventsLastIngestedTickStatus(ctx context.Context) (consumedEventLogTick domain.RedisEventsLastIngestedTickStatus, exists bool, err error)
}

type EventsDataStore interface {
	SetEventsLastProcessedTick(tick uint32) error
	GetEventsLastProcessedTick() (tick uint32, err error)
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

	lastProcessedTick, err := ep.dataStore.GetEventsLastProcessedTick()
	if err != nil {
		return fmt.Errorf("getting events last processed tick: %w", err)
	}

	lastIngestedTickStatus, exists, err := ep.redisClient.GetEventsLastIngestedTickStatus(ctx)
	if err != nil {
		return fmt.Errorf("getting events last ingested tick status from redis: %w", err)
	}
	if !exists {
		return errors.New("events last ingested tick status redis hash does not exists")
	}

	ep.metrics.SetEventsRedisLastIngestedTick(lastIngestedTickStatus.TickNumber)

	if lastIngestedTickStatus.TickNumber == lastProcessedTick {
		return nil
	}

	time.Sleep(ep.elasticQueryDelay)

	elasticCount, err := ep.searchClient.GetEventsCountForTick(ctx, lastIngestedTickStatus.TickNumber)
	if err != nil {
		return fmt.Errorf("getting event count for tick %d from elastic: %w", lastIngestedTickStatus.TickNumber, err)
	}

	if lastIngestedTickStatus.EventCount != elasticCount {
		return fmt.Errorf("events log mismatch. redis: %d, elastic: %d", lastIngestedTickStatus.EventCount, elasticCount)
	}

	log.Printf("Validated event count for tick %d: %d", lastIngestedTickStatus.TickNumber, lastIngestedTickStatus.EventCount)

	err = ep.dataStore.SetEventsLastProcessedTick(lastIngestedTickStatus.TickNumber)
	if err != nil {
		return fmt.Errorf("saving events last processed tick: %w", err)
	}

	ep.metrics.SetEventsLastProcessedTick(lastIngestedTickStatus.TickNumber)

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
