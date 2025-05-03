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
	GetTickData(ctx context.Context, tickNumber uint32) ([]string, error)
}

type SearchClient interface {
	GetTransactionHashes(ctx context.Context, tickNumber uint32) ([]string, error)
}

type DataStore interface {
	SetLastProcessedTick(tick uint32) error
	GetLastProcessedTick() (tick uint32, err error)
}

type TickProcessor struct {
	archiveClient     ArchiveClient
	searchClient      SearchClient
	dataStore         DataStore
	processingMetrics *metrics.Metrics
	errorsCount       int64
}

func NewTickProcessor(archiveClient ArchiveClient, elasticClient SearchClient, dataStore DataStore, m *metrics.Metrics) *TickProcessor {
	processor := TickProcessor{
		archiveClient:     archiveClient,
		searchClient:      elasticClient,
		dataStore:         dataStore,
		processingMetrics: m,
	}
	return &processor
}

func (p *TickProcessor) Synchronize() {
	ticker := time.Tick(1 * time.Second)
	for range ticker {
		err := p.sync()
		if err == nil {
			p.resetErrorCount()
			log.Printf("[INFO] all ticks matched.")
		} else {
			p.incrementErrorCount()
			log.Printf("[WARN] sync run failed: %v", err)
			p.sleepWithBackoff() // backoff if sync run fails subsequently
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
	end = min(status.LatestTick, end) // don't exceed lastest tick

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
	archiverTransactions, err := p.archiveClient.GetTickData(ctx, tick)
	if err != nil {
		return errors.Wrap(err, "get archiver transactions")
	}

	// query elastic for transactions
	elasticTransactions, err := p.searchClient.GetTransactionHashes(ctx, tick)
	if err != nil {
		return errors.Wrap(err, "get elastic transactions")
	}

	difference := Difference(ToSet(archiverTransactions), ToSet(elasticTransactions))
	if len(difference) > 0 { // transactions do not match
		log.Printf("[WARN] Delta of transaction hashes for tick [%d]: %v", tick, difference)
		log.Printf("Transactions in archiver: %v", archiverTransactions)
		log.Printf("Transactions in elastic: %v", elasticTransactions)
		return errors.Errorf("transactions mismatch for tick [%d]. Delta: %v", tick, difference)
	}

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

func (p *TickProcessor) sleepWithBackoff() {
	if p.errorsCount < 100 {
		time.Sleep(time.Duration(p.errorsCount) * 100 * time.Millisecond)
	} else {
		time.Sleep(10 * time.Second)
	}
}

func (p *TickProcessor) incrementErrorCount() {
	p.errorsCount++
	p.processingMetrics.SetError(true)
}

func (p *TickProcessor) resetErrorCount() {
	p.errorsCount = 0
	p.processingMetrics.SetError(false)
}
