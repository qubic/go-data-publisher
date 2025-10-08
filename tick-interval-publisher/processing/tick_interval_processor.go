package processing

import (
	"context"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/qubic/tick-interval-publisher/domain"
	"github.com/qubic/tick-interval-publisher/metrics"
)

type ArchiveClient interface {
	GetStatus(ctx context.Context) (*domain.Status, error)
}

type DataStore interface {
	SetLastProcessedEpoch(epoch uint32) error
	GetLastProcessedEpoch() (uint32, error)
}

type Producer interface {
	SendMessage(ctx context.Context, interval *domain.TickInterval) error
}

type TickIntervalProcessor struct {
	archiveClient     ArchiveClient
	dataStore         DataStore
	producer          Producer
	processingMetrics *metrics.ProcessingMetrics
}

func NewTickIntervalProcessor(db DataStore, client ArchiveClient, producer Producer,
	m *metrics.ProcessingMetrics) *TickIntervalProcessor {

	tdp := TickIntervalProcessor{
		dataStore:         db,
		archiveClient:     client,
		producer:          producer,
		processingMetrics: m,
	}
	return &tdp
}

func (p *TickIntervalProcessor) StartProcessing() {
	ticker := time.Tick(1 * time.Minute)
	for range ticker {
		err := p.process()
		if err != nil {
			log.Printf("Error processing tick intervals: %v", err)
		}
	}
}

func (p *TickIntervalProcessor) PublishCustomEpochs(epochs []uint32) error {
	log.Printf("[INFO] publishing custom epochs: %v", epochs)
	ctx := context.Background()
	status, err := p.archiveClient.GetStatus(ctx)
	if err != nil {
		return fmt.Errorf("getting archive status: %w", err)
	}

	for _, interval := range status.TickIntervals {
		if slices.Contains(epochs, interval.Epoch) {
			err = p.sendTickInterval(ctx, interval)
			if err != nil {
				return fmt.Errorf("sending tick interval: %w", err)
			}
			log.Printf("[INFO] Published interval for epoch [%d].", interval.Epoch)
		}
	}
	return nil
}

func (p *TickIntervalProcessor) process() error {
	ctx := context.Background()
	status, err := p.archiveClient.GetStatus(ctx)
	if err != nil {
		return fmt.Errorf("getting archive status: %w", err)
	}
	p.processingMetrics.SetSourceTick(status.LatestEpoch, status.LatestTick)

	processedEpoch, err := p.dataStore.GetLastProcessedEpoch()
	if err != nil {
		return fmt.Errorf("get last processed epoch: %w", err)
	}

	intervals := getUnprocessedIntervals(processedEpoch, status.TickIntervals)
	if len(intervals) == 0 {
		return nil
	}

	err = verifyIntervalsAscending(intervals) // should be ordered
	if err != nil {
		return fmt.Errorf("verifying intervals ascending: %w", err)
	}

	for _, interval := range intervals {

		if interval.Epoch < status.LatestEpoch { // ignore current epoch

			err = p.sendTickInterval(ctx, interval)
			if err != nil {
				return fmt.Errorf("sending tick interval: %w", err)
			}

			if interval.Epoch > processedEpoch {
				err = p.dataStore.SetLastProcessedEpoch(interval.Epoch)
				if err != nil {
					return fmt.Errorf("setting last processed epoch to [%d]: %w", interval.Epoch, err)
				}
				processedEpoch = interval.Epoch
			}

		}

	}

	return nil

}

func (p *TickIntervalProcessor) sendTickInterval(ctx context.Context, interval *domain.TickInterval) error {
	log.Printf("[INFO] processing interval for epoch [%d] from tick [%d] to [%d].", interval.Epoch, interval.From, interval.To)
	err := p.producer.SendMessage(ctx, interval)
	if err != nil {
		return fmt.Errorf("sending tick interval: %w", err)
	}
	p.processingMetrics.IncProcessedMessages()
	p.processingMetrics.SetProcessedTick(interval.Epoch, interval.To)
	return nil
}

func getUnprocessedIntervals(processedEpoch uint32, intervals []*domain.TickInterval) []*domain.TickInterval {
	for i, interval := range intervals {
		if interval.Epoch > processedEpoch {
			return intervals[i:]
		}
	}
	return []*domain.TickInterval{}
}

func verifyIntervalsAscending(intervals []*domain.TickInterval) error {
	epoch := uint32(0)
	from := uint32(0)
	for _, interval := range intervals {
		if interval.Epoch < epoch || interval.From < from {
			return fmt.Errorf("invalid order: current interval [%d/%d/%d] smaller than previous [%d/%d/%d]",
				interval.Epoch, interval.From, interval.To, epoch, from, epoch)
		}
		epoch = interval.Epoch
		from = interval.From
	}
	return nil
}
