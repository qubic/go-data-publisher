package sync

import (
	"context"
	"github.com/pkg/errors"
	"github.com/qubic/tick-data-publisher/archiver"
	"github.com/qubic/tick-data-publisher/metrics"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

type ArchiveClient interface {
	GetStatus(ctx context.Context) (*archiver.Status, error)
	GetTickData(ctx context.Context, tickNumber uint32) (*archiver.TickData, error)
}

type DataStore interface {
	SetLastProcessedTick(tick uint32) error
	GetLastProcessedTick() (tick uint32, err error)
}

type Producer interface {
	SendMessage(ctx context.Context, tickData *archiver.TickData) error
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

func (p *TickDataProcessor) StartProcessing() {
	ticker := time.Tick(1 * time.Second)
	for range ticker {
		err := p.process()
		if err != nil {
			log.Printf("Error processing tick data: %v", err)
		} else {
			log.Printf("Synced tick data")
		}
	}
}

func (p *TickDataProcessor) process() error {
	ctx := context.Background()
	status, err := p.archiveClient.GetStatus(ctx)
	if err != nil {
		return errors.Wrap(err, "get archive status")
	}
	p.processingMetrics.SetSourceTick(status.LatestEpoch, status.LatestTick)

	tick, err := p.dataStore.GetLastProcessedTick()
	if err != nil {
		return errors.Wrap(err, "get last processed tick")
	}

	start, end, epoch, err := calculateNextTickRange(tick, status.TickIntervals)
	if err != nil {
		return errors.Wrap(err, "calculating tick range")
	}
	end = min(status.LatestTick, end) // don't exceed lastest tick

	if start <= end && start > 0 && end > 0 && epoch > 0 {
		log.Printf("Processing ticks from [%d] to [%d] for epoch [%d].", start, end, epoch)
		// if start == end then process one tick
		err = p.processTickRange(ctx, epoch, start, end)
		if err != nil {
			return errors.Wrap(err, "processing tick range")
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
				return errors.Wrapf(err, "processing ticks [%d]", nextTicks)
			}
			nextTicks = nil
			err = p.dataStore.SetLastProcessedTick(tick) // set after completed batch only
			if err != nil {
				return errors.Wrapf(err, "storing last processed tick [%d]", tick)
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
		return errors.Wrap(err, "get tick data")
	}
	if tickData != nil || (tickData == &archiver.TickData{}) { // empty tick
		err = p.producer.SendMessage(ctx, tickData)
		if err != nil {
			return errors.Wrap(err, "sending message")
		}
	}
	p.processingMetrics.IncProcessedMessages()
	p.processingMetrics.IncProcessedTicks()
	return nil
}

func calculateNextTickRange(lastProcessedTick uint32, intervals []*archiver.TickInterval) (uint32, uint32, uint32, error) {
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
