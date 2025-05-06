package sync

import (
	"context"
	"github.com/pkg/errors"
	"github.com/qubic/status-service/archiver"
	"github.com/qubic/status-service/metrics"
	"github.com/qubic/status-service/util"
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
	AddSkippedTick(tick uint32) error
}

type TickProcessor struct {
	archiveClient      ArchiveClient
	searchClient       SearchClient
	dataStore          DataStore
	processingMetrics  *metrics.Metrics
	skipErroneousTicks bool
	errorsCount        uint
}

func NewTickProcessor(archiveClient ArchiveClient, elasticClient SearchClient, dataStore DataStore, m *metrics.Metrics, skip bool) *TickProcessor {
	processor := TickProcessor{
		archiveClient:      archiveClient,
		searchClient:       elasticClient,
		dataStore:          dataStore,
		processingMetrics:  m,
		skipErroneousTicks: skip,
	}
	return &processor
}

func (p *TickProcessor) Synchronize() {
	ticker := time.Tick(1 * time.Second)
	for range ticker {
		err := p.sync()
		if err == nil {
			p.resetErrorCount()
		} else {
			p.incrementErrorCount()
			log.Printf("Check failed: %v", err)
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
	log.Printf("Last processed tick: [%d].", tick)
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
		err = p.processTickRange(ctx, epoch, start, end+1)
		if err != nil {
			return errors.Wrap(err, "processing tick range")
		}
	}

	return nil
}

func (p *TickProcessor) processTickRange(ctx context.Context, epoch, from, toExcl uint32) error {
	for tick := from; tick < toExcl; tick++ {
		match, err := p.processTick(ctx, tick)
		if err != nil {
			return errors.Wrapf(err, "processing tick [%d]", tick)
		}
		if !match {
			if p.skipErroneousTicks {
				log.Printf("[WARN] skipping tick [%d].", tick)
				err = p.dataStore.AddSkippedTick(tick)
				if err != nil {
					return errors.Wrap(err, "trying to store skipped tick")
				}
			} else {
				return errors.Errorf("Transaction mismatch in tick [%d]", tick)
			}
		}
		err = p.dataStore.SetLastProcessedTick(tick)
		if err != nil {
			return errors.Wrapf(err, "storing last processed tick [%d]", tick)
		}
		p.processingMetrics.SetProcessedTransactionsTick(epoch, tick)
	}
	return nil
}

// processTick returns 'false' if the tick comparison between archiver and elastic does not match
func (p *TickProcessor) processTick(ctx context.Context, tick uint32) (bool, error) {
	// query archiver for transactions
	archiverTransactions, err := p.archiveClient.GetTickData(ctx, tick)
	if err != nil {
		return false, errors.Wrap(err, "get archiver transactions")
	}

	// query elastic for transactions
	elasticTransactions, err := p.searchClient.GetTransactionHashes(ctx, tick)
	if err != nil {
		return false, errors.Wrap(err, "get elastic transactions")
	}

	difference := util.Difference(util.ToSet(archiverTransactions), util.ToSet(elasticTransactions))
	if len(difference) > 0 { // transactions do not match
		if p.skipErroneousTicks { // verbose log only if we skip ticks
			log.Printf("Transaction mismatch for tick [%d].", tick)
			log.Printf("Archiver: %v", archiverTransactions)
			log.Printf("Elastic: %v", elasticTransactions)
		} else {
			log.Printf("Transaction mismatch for tick [%d]. Count: archiver [%d], elastic [%d].", tick, len(archiverTransactions), len(elasticTransactions))
		}
		return false, nil // return mismatch
	}

	return true, nil
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

func (p *TickProcessor) incrementErrorCount() {
	p.errorsCount++
	p.processingMetrics.SetError(p.errorsCount)
}

func (p *TickProcessor) resetErrorCount() {
	p.errorsCount = 0
	p.processingMetrics.SetError(p.errorsCount)
}
