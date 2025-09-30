package sync

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	archproto "github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/elastic"
	"github.com/qubic/go-data-publisher/status-service/metrics"
	"github.com/qubic/go-data-publisher/status-service/util"
	"golang.org/x/sync/errgroup"
	"log"
	"slices"
	"time"
)

type ArchiveClient interface {
	GetStatus(ctx context.Context) (*archproto.GetStatusResponse, error)
	GetTickData(ctx context.Context, tickNumber uint32) (*archproto.TickData, error)
}

type SearchClient interface {
	GetTransactionHashes(ctx context.Context, tickNumber uint32) ([]string, error)
	GetTickData(_ context.Context, tickNumber uint32) (*elastic.TickData, error)
}

type DataStore interface {
	GetLastProcessedTick() (tick uint32, err error)
	SetLastProcessedTick(tick uint32) error
	SetSourceStatus(status *domain.Status) error
	AddSkippedTick(tick uint32) error
}

type TickProcessor struct {
	archiveClient      ArchiveClient
	searchClient       SearchClient
	dataStore          DataStore
	processingMetrics  *metrics.Metrics
	syncTransactions   bool
	syncTickData       bool
	skipErroneousTicks bool
	maxWorkers         int
	elasticQueryDelay  time.Duration // time to delay checking elastic
	errorsCount        uint
}

type Config struct {
	SyncTransactions  bool
	SyncTickData      bool
	SkipTicks         bool
	NumMaxWorkers     int
	ElasticQueryDelay time.Duration
}

func NewTickProcessor(archiveClient ArchiveClient, elasticClient SearchClient, dataStore DataStore, m *metrics.Metrics, config Config) *TickProcessor {
	processor := TickProcessor{
		archiveClient:      archiveClient,
		searchClient:       elasticClient,
		dataStore:          dataStore,
		processingMetrics:  m,
		syncTransactions:   config.SyncTransactions,
		syncTickData:       config.SyncTickData,
		skipErroneousTicks: config.SkipTicks,
		maxWorkers:         max(1, config.NumMaxWorkers), // one worker minimum
		elasticQueryDelay:  config.ElasticQueryDelay,
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
	archiverStatus, err := p.archiveClient.GetStatus(ctx)
	if err != nil {
		return errors.Wrap(err, "get archive status")
	}

	status, err := domain.ConvertFromArchiverStatus(archiverStatus)
	if err != nil {
		return errors.Wrap(err, "convert archive status")
	}

	err = p.dataStore.SetSourceStatus(status) // store for rpc clients
	if err != nil {
		return errors.Wrapf(err, "setting status: %v", status)
	}

	p.processingMetrics.SetSourceTick(status.Epoch, status.Tick)

	// wait a bit to allow latest tick to sync
	time.Sleep(p.elasticQueryDelay)

	tick, err := p.dataStore.GetLastProcessedTick()
	if err != nil {
		return errors.Wrap(err, "get last processed tick")
	}

	start, end, epoch, err := calculateNextTickRange(tick, status.TickIntervals)
	if err != nil {
		return errors.Wrap(err, "calculating tick range")
	}
	end = min(status.Tick, end) // don't exceed lastest tick

	if start <= end && start > 0 && end > 0 && epoch > 0 {
		if start == end {
			log.Printf("Processing tick [%d].", start)
		} else {
			log.Printf("Processing ticks from [%d] to [%d] for epoch [%d].", start, end, epoch)
		}
		// if start == end then process one tick
		err = p.processTickRange(ctx, epoch, start, end)
		if err != nil {
			return errors.Wrap(err, "processing tick range")
		}
	}

	return nil
}

func (p *TickProcessor) processTickRange(ctx context.Context, epoch, from, to uint32) error {

	// work more in parallel, if tick range is larger
	numWorkers := min((int(to-from)/10)+1, p.maxWorkers)
	nextTicks := make([]uint32, 0, numWorkers)

	for tick := from; tick <= to; tick++ {
		nextTicks = append(nextTicks, tick)

		// if we have an error then process only one tick
		if p.errorsCount > 0 || len(nextTicks) == numWorkers || tick == to {

			err := p.processTicks(ctx, nextTicks)
			if err != nil {
				return errors.Wrapf(err, "processing ticks %v", nextTicks)
			}
			nextTicks = nil // reset

			// update stats
			err = p.dataStore.SetLastProcessedTick(tick)
			if err != nil {
				return errors.Wrapf(err, "storing last processed tick [%d]", tick)
			}
			p.processingMetrics.SetProcessedTransactionsTick(epoch, tick)

		}
	}

	return nil
}

func (p *TickProcessor) processTicks(ctx context.Context, ticks []uint32) error {
	var errorGroup errgroup.Group
	for _, tick := range ticks {
		errorGroup.Go(func() error {
			// log.Printf("Processing tick [%d]", tick)
			return p.processTick(ctx, tick)
		})
	}
	return errorGroup.Wait()
}

// processTick errors in case of mismatch only, if skipping ticks is disabled
func (p *TickProcessor) processTick(ctx context.Context, tick uint32) error {
	// query archiver for transactions
	tickData, err := p.archiveClient.GetTickData(ctx, tick)
	if err != nil {
		return errors.Wrap(err, "get archiver transactions")
	}

	// we have some invalid empty ticks in epoch 154. We can safely ignore them.
	if tickData != nil && tickData.GetEpoch() == 65535 && tick > 22175000 && tick < 22187500 {
		log.Printf("Correcting invalid empty tick data for tick [%d] to allow further processing.", tick)
		tickData = nil
	}

	if p.syncTickData {
		match, err := p.verifyTickData(ctx, tick, tickData)
		if err != nil {
			return errors.Wrap(err, "verifying tick data")
		}
		if !match {
			return p.handleTickMismatch(tick)
		}
	}

	if p.syncTransactions {
		match, err := p.verifyTickTransactions(ctx, tick, tickData)
		if err != nil {
			return errors.Wrap(err, "verifying transactions")
		}
		if !match {
			return p.handleTickMismatch(tick)
		}
	}
	return nil
}

func (p *TickProcessor) handleTickMismatch(tick uint32) error {
	if p.skipErroneousTicks {
		log.Printf("[WARN] skipping tick [%d].", tick)
		err := p.dataStore.AddSkippedTick(tick)
		if err != nil {
			return errors.Wrap(err, "trying to store skipped tick")
		}
		return nil
	} else {
		return errors.New("tick data mismatch") // error, if we do not skip faulty ticks
	}
}

func (p *TickProcessor) verifyTickData(ctx context.Context, tick uint32, archiveTd *archproto.TickData) (bool, error) {
	elasticTd, err := p.searchClient.GetTickData(ctx, tick)
	if err != nil {
		return false, errors.Wrap(err, "get elastic tick data")
	}

	match := archiveTd == nil && elasticTd == nil
	if archiveTd != nil && elasticTd != nil {
		bytes, dErr := hex.DecodeString(archiveTd.GetSignatureHex())
		if dErr != nil {
			return false, errors.Wrap(err, "decoding signature hex")
		}
		match = elasticTd.ComputorIndex == archiveTd.ComputorIndex &&
			elasticTd.Epoch == archiveTd.Epoch &&
			elasticTd.TickNumber == archiveTd.TickNumber &&
			len(elasticTd.TransactionHashes) == len(archiveTd.TransactionIds) &&
			slices.Compare(elasticTd.TransactionHashes, archiveTd.TransactionIds) == 0 &&
			elasticTd.Signature == base64.StdEncoding.EncodeToString(bytes)
	}

	if !match {
		if elasticTd == nil { // typical case
			log.Printf("Tick data mismatch for tick [%d]. Missing in Elastic.", tick)
		} else if archiveTd == nil { // typical case
			log.Printf("Tick data mismatch for tick [%d]. Missing in Archiver.", tick)
		} else { // complicated data mismatch
			log.Printf("Tick data mismatch for tick [%d]. Elastic: [%v]. Archiver: [%v]",
				tick, elasticDebugString(elasticTd), archiveDebugString(archiveTd))
		}
	}

	return match, nil
}

func (p *TickProcessor) verifyTickTransactions(ctx context.Context, tick uint32, tickData *archproto.TickData) (bool, error) {
	// query elastic for transactions
	elasticTransactions, err := p.searchClient.GetTransactionHashes(ctx, tick)
	if err != nil {
		return false, errors.Wrap(err, "get elastic transactions")
	}

	archiverTransactions := tickData.GetTransactionIds() // tick data and transaction ids can be nil
	difference := util.Difference(util.ToSet(archiverTransactions), util.ToSet(elasticTransactions))
	if len(difference) > 0 { // transactions do not match
		if p.skipErroneousTicks { // verbose log only if we skip ticks
			log.Printf("Transaction mismatch for tick [%d].", tick)
			log.Printf("Archiver: %v", tickData)
			log.Printf("Elastic: %v", elasticTransactions)
		} else {
			log.Printf("Transaction mismatch for tick [%d]. Count: archiver [%d], elastic [%d].",
				tick, len(archiverTransactions), len(elasticTransactions))
		}
		return false, nil // return mismatch
	}

	return true, nil
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

func elasticDebugString(td *elastic.TickData) string {
	return fmt.Sprintf("tick %d, epoch %d, signature %s", td.TickNumber, td.Epoch, td.Signature)
}

func archiveDebugString(td *archproto.TickData) string {
	return fmt.Sprintf("tick %d, epoch %d, signature hex %s", td.TickNumber, td.Epoch, td.SignatureHex)
}

func (p *TickProcessor) incrementErrorCount() {
	p.errorsCount++
	p.processingMetrics.SetError(p.errorsCount)
}

func (p *TickProcessor) resetErrorCount() {
	p.errorsCount = 0
	p.processingMetrics.SetError(0)
}
