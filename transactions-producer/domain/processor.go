package domain

import (
	"context"
	"errors"
	"fmt"
	"github.com/qubic/transactions-producer/entities"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
)

type Fetcher interface {
	GetProcessedTickIntervalsPerEpoch(ctx context.Context) ([]entities.ProcessedTickIntervalsPerEpoch, error)
	GetTickTransactions(ctx context.Context, tick uint32) ([]entities.Tx, error)
}

type Publisher interface {
	PublishTickTransactions(tickTransactions []entities.TickTransactions) error
}

type statusStore interface {
	GetLastProcessedTick(epoch uint32) (uint32, error)
	SetLastProcessedTick(epoch uint32, tick uint32) error
	GetLastProcessedTickForAllEpochs() (map[uint32]uint32, error)
}

type Processor struct {
	fetcher      Fetcher
	fetchTimeout time.Duration
	publisher    Publisher
	statusStore  statusStore
	batchSize    int
	logger       *zap.SugaredLogger
	syncMetrics  *Metrics
}

func NewProcessor(
	fetcher Fetcher,
	fetchTimeout time.Duration,
	publisher Publisher,
	statusStore statusStore,
	batchSize int,
	logger *zap.SugaredLogger,
	metrics *Metrics,
) *Processor {
	return &Processor{
		fetcher:      fetcher,
		fetchTimeout: fetchTimeout,
		publisher:    publisher,
		statusStore:  statusStore,
		batchSize:    batchSize,
		logger:       logger,
		syncMetrics:  metrics,
	}
}

func (p *Processor) Start(nrWorkers int) error {
	for {
		err := p.runCycle(nrWorkers)
		if err != nil {
			p.logger.Errorw("error running cycle", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}
	}
}

func (p *Processor) runCycle(nrWorkers int) error {
	epochs, err := p.fetcher.GetProcessedTickIntervalsPerEpoch(context.Background())
	if err != nil {
		return fmt.Errorf("getting status: %v", err)
	}

	startingTicksForEpochs, err := p.getStartingTicksForEpochs(epochs)
	if err != nil {
		return fmt.Errorf("getting starting ticks for epochs: %v", err)
	}

	var startedWorkers atomic.Int32

	p.setLatestSourceTickToMetrics(epochs)

	for _, epochIntervals := range epochs {
		startingTick, ok := startingTicksForEpochs[epochIntervals.Epoch]
		if !ok {
			return fmt.Errorf("starting tick not found for epoch %d", epochIntervals.Epoch)
		}
		if startingTick == 0 {
			continue
		}

		p.waitWorkerToFreeUp(nrWorkers, &startedWorkers)

		startedWorkers.Add(1)
		go func() {
			defer startedWorkers.Add(-1)

			err = p.processEpoch(startingTick, epochIntervals)
			if err != nil {
				p.logger.Errorw("error processing cycle for epoch", "epoch", epochIntervals.Epoch, "error", err)
			} else {
				p.logger.Infow("Finished processing cycle for epoch", "epoch", epochIntervals.Epoch)
			}
		}()
	}

	p.waitAllWorkersToFinish(&startedWorkers)

	return nil
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

func (p *Processor) waitAllWorkersToFinish(startedWorkers *atomic.Int32) {
	for {
		if startedWorkers.Load() == int32(0) {
			break
		}

		time.Sleep(time.Second)
		continue
	}
}

func (p *Processor) waitWorkerToFreeUp(nrWorkers int, startedWorkers *atomic.Int32) {
	for {
		if startedWorkers.Load() != int32(nrWorkers) {
			break
		}

		time.Sleep(time.Second)
		continue
	}
}

func (p *Processor) getStartingTicksForEpochs(epochsIntervals []entities.ProcessedTickIntervalsPerEpoch) (map[uint32]uint32, error) {
	startingTicks := make(map[uint32]uint32)

	for _, epochIntervals := range epochsIntervals {
		lastProcessedTick, err := p.statusStore.GetLastProcessedTick(epochIntervals.Epoch)
		if errors.Is(err, entities.ErrStoreEntityNotFound) {
			startingTicks[epochIntervals.Epoch] = epochIntervals.Intervals[0].InitialProcessedTick
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("getting last processed tick: %v", err)
		}

		if lastProcessedTick == epochIntervals.Intervals[len(epochIntervals.Intervals)-1].LastProcessedTick {
			startingTicks[epochIntervals.Epoch] = 0
			continue
		}

		startingTicks[epochIntervals.Epoch] = lastProcessedTick + 1
	}

	return startingTicks, nil
}

func (p *Processor) processEpoch(startTick uint32, epochTickIntervals entities.ProcessedTickIntervalsPerEpoch) error {
	p.logger.Infow("Starting epoch processor", "epoch", epochTickIntervals.Epoch, "startTick", startTick)

	lastTickFromIntervals := epochTickIntervals.Intervals[len(epochTickIntervals.Intervals)-1].LastProcessedTick

	for {
		lastProcessedTick, err := p.processBatch(startTick, epochTickIntervals)
		if err != nil {
			p.logger.Errorw("error processing batch", "epoch", epochTickIntervals.Epoch, "startTick", startTick, "error", fmt.Errorf("processing batch: %v", err))
			continue
		}

		// if lastProcessedTick is equal to the last tick from intervals, we are done
		if lastProcessedTick == lastTickFromIntervals {
			break
		}

		startTick = lastProcessedTick + 1
	}

	return nil
}

func (p *Processor) processBatch(startTick uint32, epochTickIntervals entities.ProcessedTickIntervalsPerEpoch) (uint32, error) {
	epoch := epochTickIntervals.Epoch

	tickTransactionsBatch, tick, err := p.gatherTickTransactionsBatch(epoch, startTick, epochTickIntervals)
	if err != nil {
		return 0, fmt.Errorf("gathering tick tx batch: %v", err)
	}

	batchSize := len(tickTransactionsBatch)
	p.logger.Infow("Publishing tick transactions", "nr_ticks", batchSize, "epoch", epoch, "tick", tick)
	err = p.publisher.PublishTickTransactions(tickTransactionsBatch)
	if err != nil {
		return 0, fmt.Errorf("inserting batch: %v", err)
	}
	p.syncMetrics.IncProcessedTicks(batchSize)
	p.syncMetrics.IncProcessedMessages(batchSize)
	p.syncMetrics.SetProcessedTick(epochTickIntervals.Epoch, tick)

	p.logger.Infow("Storing last processed tick", "epoch", epoch, "tick", tick)
	err = p.statusStore.SetLastProcessedTick(epoch, tick)
	if err != nil {
		return 0, fmt.Errorf("setting last processed tick: %v", err)
	}

	return tick, nil
}

func (p *Processor) gatherTickTransactionsBatch(epoch, startTick uint32, epochTickIntervals entities.ProcessedTickIntervalsPerEpoch) ([]entities.TickTransactions, uint32, error) {

	var tickTransactionsBatch []entities.TickTransactions

	var tick uint32

	for intervalIndex, interval := range epochTickIntervals.Intervals {
		for tick = interval.InitialProcessedTick; tick <= interval.LastProcessedTick; tick++ {
			if tick < startTick {
				continue
			}

			transactions, err := func() ([]entities.Tx, error) {
				ctx, cancel := context.WithTimeout(context.Background(), p.fetchTimeout)
				defer cancel()

				return p.fetcher.GetTickTransactions(ctx, tick)
			}()

			if err != nil {
				if !errors.Is(err, entities.ErrEmptyTick) {
					p.logger.Errorw("error processing tick; retrying...", "epoch", epoch, "tick", tick, "error", fmt.Errorf("getting transactions: %v", err))
					tick--
					continue
				}
				transactions = []entities.Tx{}
			}

			tickTransactions := entities.TickTransactions{
				TickNumber:   tick,
				Epoch:        epoch,
				Transactions: transactions,
			}
			tickTransactionsBatch = append(tickTransactionsBatch, tickTransactions)

			if len(tickTransactionsBatch) >= p.batchSize || (intervalIndex == len(epochTickIntervals.Intervals)-1) && tick == interval.LastProcessedTick {
				return tickTransactionsBatch, tick, nil
			}
		}
	}

	return tickTransactionsBatch, tick, nil
}
