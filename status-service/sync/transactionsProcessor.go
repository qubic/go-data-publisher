package sync

import (
	"context"
	"github.com/pkg/errors"
	"github.com/qubic/status-service/archiver"
	"github.com/qubic/status-service/metrics"
	"log"
	"time"
)

type ArchiveClient interface {
	GetStatus(ctx context.Context) (*archiver.Status, error)
}

type DataStore interface {
	SetLastProcessedTick(tick uint32) error
	GetLastProcessedTick() (tick uint32, err error)
}

type TickProcessor struct {
	archiveClient     ArchiveClient
	dataStore         DataStore
	processingMetrics *metrics.Metrics
}

func NewTickProcessor(archiveClient ArchiveClient, dataStore DataStore, m *metrics.Metrics) *TickProcessor {
	processor := TickProcessor{
		archiveClient:     archiveClient,
		dataStore:         dataStore,
		processingMetrics: m,
	}
	return &processor
}

func (p *TickProcessor) Synchronize() {
	ticker := time.Tick(1 * time.Second)
	for range ticker {
		err := p.sync()
		if err != nil {
			log.Printf("[WARN] sync run failed: %v", err)
		}
	}
}

func (p *TickProcessor) sync() error {
	ctx := context.Background()
	status, err := p.archiveClient.GetStatus(ctx)
	if err != nil {
		return errors.Wrap(err, "get archive status")
	}
	p.processingMetrics.SetSourceTick(status.LatestEpoch, status.LatestTick)

	tick, err := p.dataStore.GetLastProcessedTick()
	log.Printf("last processed tick: %v", tick)
	if err != nil {
		return errors.Wrap(err, "get last processed tick")
	}

	start, end, epoch, err := calculateNextTickRange(tick, status.TickIntervals)
	if err != nil {
		return errors.Wrap(err, "calculating tick range")
	}

	if start > end || start == 0 || end == 0 || epoch == 0 {
		log.Printf("No ticks to process.")
	} else { // if start == end then process one tick
		log.Printf("Processing ticks from [%d] to [%d] for epoch [%d].", start, end, epoch)
		err = p.processTickRange(ctx, epoch, start, end+1)
		if err != nil {
			return errors.Wrap(err, "processing tick range")
		}
	}

	return nil
}

func (p *TickProcessor) processTickRange(ctx context.Context, epoch, from, toExcl uint32) error {
	for tick := from; tick < toExcl; tick++ {
		err := p.processTick(ctx, tick)
		if err != nil {
			return errors.Wrapf(err, "processing tick [%d]", tick)
		}
		err = p.dataStore.SetLastProcessedTick(tick)
		if err != nil {
			return errors.Wrapf(err, "storing last processed tick [%d]", tick)
		}
		p.processingMetrics.SetProcessedTransactionsTick(epoch, tick)
	}
	return nil
}

func (p *TickProcessor) processTick(ctx context.Context, tick uint32) error {

	// query archiver for transactions

	// query elastic for transactions

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
