package sync

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/metrics"
)

type LogSearchClient interface {
	GetLogCountForTick(ctx context.Context, tickNumber uint32) (uint32, error)
}

type LogRedisClient interface {
	GetLogLastIngestedTickStatus(ctx context.Context) (consumedEventLogTick domain.RedisLogsLastIngestedTickStatus, err error)
}

type LogDataStore interface {
	SetLogLastProcessedTick(tick uint32) error
	GetLogLastProcessedTick() (tick uint32, err error)
}

type LogProcessor struct {
	searchClient LogSearchClient
	redisClient  LogRedisClient
	dataStore    LogDataStore

	metrics           *metrics.Metrics
	elasticQueryDelay time.Duration
	errorsCount       uint
}

func NewLogProcessor(searchClient LogSearchClient, redisClient LogRedisClient, dataStore LogDataStore, elasticQueryDelay time.Duration, metrics *metrics.Metrics) *LogProcessor {
	return &LogProcessor{
		searchClient:      searchClient,
		redisClient:       redisClient,
		dataStore:         dataStore,
		elasticQueryDelay: elasticQueryDelay,
		metrics:           metrics,
	}
}

func (ep *LogProcessor) Synchronize() {
	ticker := time.Tick(1 * time.Second)
	for range ticker {
		err := ep.sync()
		if err != nil {
			ep.incrementErrorCount()
			log.Printf("Logs check failed: %v", err)
			continue
		}
		ep.resetErrorCount()
	}
}

func (ep *LogProcessor) sync() error {

	ctx := context.Background()

	lastProcessedTick, err := ep.dataStore.GetLogLastProcessedTick()
	if err != nil {
		return fmt.Errorf("getting last processed log tick: %w", err)
	}

	lastIngestedTickStatus, err := ep.redisClient.GetLogLastIngestedTickStatus(ctx)
	if err != nil {
		return fmt.Errorf("getting last ingested log tick from redis: %w", err)
	}

	ep.metrics.SetLogsRedisLastIngestedTick(lastIngestedTickStatus.TickNumber)

	if lastIngestedTickStatus.TickNumber == lastProcessedTick {
		return nil
	}

	time.Sleep(ep.elasticQueryDelay)

	elasticCount, err := ep.searchClient.GetLogCountForTick(ctx, lastIngestedTickStatus.TickNumber)
	if err != nil {
		return fmt.Errorf("getting logs count for tick %d from elastic: %w", lastIngestedTickStatus.TickNumber, err)
	}

	if lastIngestedTickStatus.LogCount != elasticCount {
		return fmt.Errorf("logs log mismatch. redis: %d, elastic: %d", lastIngestedTickStatus.LogCount, elasticCount)
	}

	log.Printf("Validated logs count for tick %d: %d", lastIngestedTickStatus.TickNumber, lastIngestedTickStatus.LogCount)

	err = ep.dataStore.SetLogLastProcessedTick(lastIngestedTickStatus.TickNumber)
	if err != nil {
		return fmt.Errorf("saving logs last processed tick: %w", err)
	}

	ep.metrics.SetLogsLastProcessedTick(lastIngestedTickStatus.TickNumber)

	return nil
}

func (ep *LogProcessor) incrementErrorCount() {
	ep.errorsCount++
	ep.metrics.SetLogsErrors(ep.errorsCount)
}

func (ep *LogProcessor) resetErrorCount() {
	ep.errorsCount = 0
	ep.metrics.SetLogsErrors(ep.errorsCount)
}
