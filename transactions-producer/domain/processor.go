package domain

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/qubic/transactions-producer/entities"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Fetcher interface {
	GetProcessedTickIntervalsPerEpoch(ctx context.Context) ([]entities.ProcessedTickIntervalsPerEpoch, error)
	GetTickTransactions(ctx context.Context, tick uint32) ([]entities.Tx, error)
}

type Publisher interface {
	PublishTickTransactions(tickTransactions []entities.TickTransactions) error
}

type statusStore interface {
	GetLastProcessedTick() (uint32, error)
	SetLastProcessedTick(tick uint32) error
}

type Processor struct {
	fetcher      Fetcher
	fetchTimeout time.Duration
	publisher    Publisher
	statusStore  statusStore
	maxWorkers   int
	logger       *zap.SugaredLogger
	syncMetrics  *Metrics
}

func NewProcessor(
	fetcher Fetcher,
	fetchTimeout time.Duration,
	publisher Publisher,
	statusStore statusStore,
	maxWorkers int,
	logger *zap.SugaredLogger,
	metrics *Metrics,
) *Processor {
	return &Processor{
		fetcher:      fetcher,
		fetchTimeout: fetchTimeout,
		publisher:    publisher,
		statusStore:  statusStore,
		maxWorkers:   maxWorkers,
		logger:       logger,
		syncMetrics:  metrics,
	}
}

func (p *Processor) Start() error {
	ticker := time.Tick(1 * time.Second)
	for range ticker {
		err := p.process()
		if err != nil {
			p.logger.Errorw("error running processing cycle", "error", err)
		}
	}
	return nil
}

func (p *Processor) PublishSingleTicks(ticks []uint32) error {
	intervals, e := p.fetcher.GetProcessedTickIntervalsPerEpoch(context.Background())
	if e != nil {
		return fmt.Errorf("getting tick intervals: %v", e)
	}

	for _, tick := range ticks {
		epoch, err := getEpochForTick(tick, intervals)
		if err != nil {
			return fmt.Errorf("getting epoch for tick [%d]: %v", tick, err)
		}

		p.logger.Infow("Trying to publish transactions", "tick", tick)
		err = p.processTick(epoch, tick)
		if err != nil {
			return fmt.Errorf("processing tick [%d]: %v", tick, err)
		}
		p.logger.Infow("Published transactions", "tick", tick)
	}
	return nil
}

func (p *Processor) process() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.fetchTimeout)
	defer cancel()
	intervals, err := p.fetcher.GetProcessedTickIntervalsPerEpoch(ctx)
	if err != nil {
		return fmt.Errorf("getting tick intervals: %v", err)
	}
	p.setLatestSourceTickToMetrics(intervals)

	tick, err := p.statusStore.GetLastProcessedTick()
	if err != nil {
		return fmt.Errorf("get last processed tick: %v", err)
	}

	start, end, epoch, err := calculateTickRange(tick, intervals)
	if err != nil {
		return fmt.Errorf("calculating tick range: %v", err)
	}

	if start <= end && start > 0 && end > 0 && epoch > 0 {

		// if start == end then process one tick
		err = p.processTickRange(epoch, start, end)
		if err != nil {
			return fmt.Errorf("processing tick range: %v", err)
		}
	}
	return nil
}

func (p *Processor) processTickRange(epoch, from, to uint32) error {
	p.logger.Infow("Processing ticks", "epoch", epoch, "from", from, "to", to)
	var nextTicks []uint32
	for tick := from; tick <= to; tick++ {
		// process several ticks in parallel
		nextTicks = append(nextTicks, tick)
		if len(nextTicks) == p.maxWorkers || tick == to {
			err := p.processTickRangeParallel(epoch, nextTicks)
			if err != nil {
				return fmt.Errorf("processing ticks [%d]: %v", nextTicks, err)
			}

			err = p.statusStore.SetLastProcessedTick(tick) // set after completed batch only
			if err != nil {
				return fmt.Errorf("storing last processed tick [%d]: %v", tick, err)
			}

			batchSize := len(nextTicks)
			p.logger.Infow("Published tick transactions", "nr_ticks", batchSize, "epoch", epoch, "tick", tick)
			p.syncMetrics.IncProcessedTicks(batchSize)
			p.syncMetrics.IncProcessedMessages(batchSize)
			p.syncMetrics.SetProcessedTick(epoch, tick)
			nextTicks = nil
		}
	}
	return nil
}

func (p *Processor) processTickRangeParallel(epoch uint32, ticks []uint32) error {
	var errorGroup errgroup.Group
	for _, tick := range ticks {
		errorGroup.Go(func() error {
			return p.processTick(epoch, tick)
		})
	}
	return errorGroup.Wait()
}

func (p *Processor) processTick(epoch, tick uint32) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.fetchTimeout)
	defer cancel()
	transactions, err := p.fetcher.GetTickTransactions(ctx, tick)
	if err != nil {
		return fmt.Errorf("fetching transactions: %v", err)
	}
	if len(transactions) == 0 {
		p.logger.Infow("Skipping tick without transactions", "epoch", epoch, "tick", tick)
	} else {
		tickTransactions := []entities.TickTransactions{
			{
				Epoch:        epoch,
				TickNumber:   tick,
				Transactions: transactions,
			},
		}
		err = p.publisher.PublishTickTransactions(tickTransactions)
		if err != nil {
			return fmt.Errorf("inserting batch: %v", err)
		}
	}
	return nil
}

func calculateTickRange(lastProcessedTick uint32, epochsIntervals []entities.ProcessedTickIntervalsPerEpoch) (uint32, uint32, uint32, error) {
	if len(epochsIntervals) == 0 {
		return 0, 0, 0, errors.New("invalid argument: missing tick intervals")
	}
	for _, epochIntervals := range epochsIntervals {
		epoch := epochIntervals.Epoch
		for _, interval := range epochIntervals.Intervals {
			if interval.LastProcessedTick > lastProcessedTick {
				// found next interval
				startTick := max(interval.InitialProcessedTick, lastProcessedTick+1)
				return startTick, interval.LastProcessedTick, epoch, nil
			}
		}
	}
	// no delta found do not sync
	return 0, 0, 0, nil
}

func getEpochForTick(tick uint32, intervals []entities.ProcessedTickIntervalsPerEpoch) (uint32, error) {
	for _, epochIntervals := range intervals {
		for _, interval := range epochIntervals.Intervals {
			if interval.InitialProcessedTick <= tick && interval.LastProcessedTick >= tick {
				return epochIntervals.Epoch, nil
			}
		}
	}
	return 0, fmt.Errorf("found no epoch")
}

func (p *Processor) setLatestSourceTickToMetrics(epochs []entities.ProcessedTickIntervalsPerEpoch) {
	// last epoch and last interval contains latest source tick
	if len(epochs) > 0 { // check if there are epochs
		latestEpochIndex := len(epochs) - 1
		if len(epochs[latestEpochIndex].Intervals) > 0 { // check if there are intervals
			latestIntervalIndex := len(epochs[latestEpochIndex].Intervals) - 1
			p.syncMetrics.SetSourceTick(epochs[latestEpochIndex].Epoch, epochs[latestEpochIndex].Intervals[latestIntervalIndex].LastProcessedTick)
		}
	}
}
