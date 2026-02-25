package sync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/qubic/tick-data-publisher/domain"
	"github.com/qubic/tick-data-publisher/metrics"
	"github.com/twmb/franz-go/pkg/kerr"
	"golang.org/x/sync/errgroup"
)

type ArchiveClient interface {
	GetStatus(ctx context.Context) (*domain.Status, error)
	GetTickData(ctx context.Context, tickNumber uint32) (*domain.TickData, error)
}

type DataStore interface {
	SetLastProcessedTick(tick uint32) error
	GetLastProcessedTick() (tick uint32, err error)
}

type Producer interface {
	SendMessage(ctx context.Context, tickData *domain.TickData) error
}

type TickDataProcessor struct {
	archiveClient     ArchiveClient
	dataStore         DataStore
	producer          Producer
	numWorkers        int
	processingMetrics *metrics.ProcessingMetrics
}

func NewTickDataProcessor(db DataStore, client ArchiveClient, producer Producer,
	numWorkers int, m *metrics.ProcessingMetrics) *TickDataProcessor {

	tdp := TickDataProcessor{
		dataStore:         db,
		archiveClient:     client,
		producer:          producer,
		numWorkers:        numWorkers,
		processingMetrics: m,
	}
	log.Printf("[INFO] using up to [%d] workers", numWorkers)
	return &tdp
}

func (p *TickDataProcessor) StartProcessing() error {
	ticker := time.Tick(1 * time.Second)
	for range ticker {
		err := p.process()
		if err != nil {
			var kafkaErr *kerr.Error
			if errors.As(err, &kafkaErr) { // go 1.26 would support errors.AsType
				if !kafkaErr.Retriable {
					return fmt.Errorf("non-retriable kafka error: %w", err)
				}
			}
			// only exit, if non-retriable kafka error
			log.Printf("Error processing tick data: %v", err)
		}
	}
	return nil
}

func (p *TickDataProcessor) PublishCustomTicks(ticks []uint32) error {
	log.Printf("[INFO] publishing custom ticks")
	ctx := context.Background()
	for _, tick := range ticks {
		err := p.processTick(ctx, tick)
		if err != nil {
			return fmt.Errorf("processing tick [%d]: %w", tick, err)
		}
		log.Printf("Published tick [%d].", tick)
	}
	return nil
}

func (p *TickDataProcessor) process() error {
	ctx := context.Background()
	status, err := p.archiveClient.GetStatus(ctx)
	if err != nil {
		return fmt.Errorf("get archive status: %w", err)
	}
	p.processingMetrics.SetSourceTick(status.LatestEpoch, status.LatestTick)

	tick, err := p.dataStore.GetLastProcessedTick()
	if err != nil {
		return fmt.Errorf("get last processed tick: %w", err)
	}

	start, end, epoch, err := calculateNextTickRange(tick, status.TickIntervals)
	if err != nil {
		return fmt.Errorf("calculating tick range: %w", err)
	}
	end = min(status.LatestTick, end) // don't exceed lastest tick

	if start <= end && start > 0 && end > 0 && epoch > 0 {
		if start == end {
			log.Printf("Processing tick [%d] for epoch [%d].", end, epoch)
		} else {
			log.Printf("Processing ticks from [%d] to [%d] for epoch [%d].", start, end, epoch)
		}
		err = p.processTickRange(ctx, epoch, start, end)
		if err != nil {
			return fmt.Errorf("processing tick range: %w", err)
		}
	}
	return nil
}

func (p *TickDataProcessor) processTickRange(ctx context.Context, epoch, from, to uint32) error {
	var nextTicks []uint32
	for tick := from; tick <= to; tick++ {
		// process several ticks in parallel
		nextTicks = append(nextTicks, tick)
		if len(nextTicks) == p.numWorkers || tick == to {
			err := p.processTickRangeParallel(ctx, nextTicks)
			if err != nil {
				return fmt.Errorf("processing ticks [%d]: %w", nextTicks, err)
			}
			nextTicks = nil
			err = p.dataStore.SetLastProcessedTick(tick) // set after completed batch only
			if err != nil {
				return fmt.Errorf("storing last processed tick [%d]: %w", tick, err)
			}
			p.processingMetrics.SetProcessedTick(epoch, tick)
		}

	}
	return nil
}

func (p *TickDataProcessor) processTickRangeParallel(ctx context.Context, ticks []uint32) error {
	var errorGroup errgroup.Group
	for _, tick := range ticks {
		errorGroup.Go(func() error {
			return p.processTick(ctx, tick)
		})
	}
	return errorGroup.Wait()
}

func (p *TickDataProcessor) processTick(ctx context.Context, tick uint32) error {
	tickData, err := p.archiveClient.GetTickData(ctx, tick)
	if err != nil {
		return fmt.Errorf("get tick data: %w", err)
	}
	if !isEmpty(tickData) {
		err = p.producer.SendMessage(ctx, tickData)
		if err != nil {
			return fmt.Errorf("sending message: %w", err)
		}
	}
	p.processingMetrics.IncProcessedMessages()
	p.processingMetrics.IncProcessedTicks()
	return nil
}

func isEmpty(tickData *domain.TickData) bool {
	return tickData == nil ||
		(tickData == &domain.TickData{}) ||
		tickData.TickNumber == 0 ||
		tickData.Epoch == 0 ||
		tickData.Epoch == 65535 // 2^16-1
}

func calculateNextTickRange(lastProcessedTick uint32, intervals []*domain.TickInterval) (uint32, uint32, uint32, error) {
	if len(intervals) == 0 {
		return 0, 0, 0, errors.New("invalid argument: missing tick intervals")
	}

	for _, interval := range intervals {
		if interval.To > lastProcessedTick {
			// found correct interval
			startTick := max(interval.From, lastProcessedTick+1)
			return startTick, interval.To, interval.Epoch, nil
		}
	}

	// no delta found do not sync
	return 0, 0, 0, nil
}
