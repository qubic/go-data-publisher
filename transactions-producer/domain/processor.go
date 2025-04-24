package domain

import (
	"context"
	"errors"
	"fmt"
	entities2 "github.com/qubic/transactions-producer/entities"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
)

type Fetcher interface {
	GetProcessedTickIntervalsPerEpoch(ctx context.Context) ([]entities2.ProcessedTickIntervalsPerEpoch, error)
	GetTickTransactions(ctx context.Context, tick uint32) ([]entities2.Tx, error)
}

type Publisher interface {
	PublishTickTransactions(ctx context.Context, tickTransactions []entities2.TickTransactions) error
}

type statusStore interface {
	GetLastProcessedTick(epoch uint32) (uint32, error)
	SetLastProcessedTick(epoch uint32, tick uint32) error
	GetLastProcessedTickForAllEpochs() (map[uint32]uint32, error)
}

type Processor struct {
	fetcher        Fetcher
	fetchTimeout   time.Duration
	publisher      Publisher
	publishTimeout time.Duration
	statusStore    statusStore
	batchSize      int
	logger         *zap.SugaredLogger
}

func NewProcessor(
	fetcher Fetcher,
	fetchTimeout time.Duration,
	publisher Publisher,
	publishTimeout time.Duration,
	statusStore statusStore,
	batchSize int,
	logger *zap.SugaredLogger,
) *Processor {
	return &Processor{
		fetcher:        fetcher,
		fetchTimeout:   fetchTimeout,
		publisher:      publisher,
		publishTimeout: publishTimeout,
		statusStore:    statusStore,
		batchSize:      batchSize,
		logger:         logger,
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

func (p *Processor) waitAllWorkersToFinish(startedWorkers *atomic.Int32) {
	for {
		if startedWorkers.Load() == int32(0) {
			break
		}

		time.Sleep(5 * time.Second)
		continue
	}
}

func (p *Processor) waitWorkerToFreeUp(nrWorkers int, startedWorkers *atomic.Int32) {
	for {
		if startedWorkers.Load() != int32(nrWorkers) {
			break
		}

		time.Sleep(5 * time.Second)
		continue
	}
}

func (p *Processor) getStartingTicksForEpochs(epochsIntervals []entities2.ProcessedTickIntervalsPerEpoch) (map[uint32]uint32, error) {
	startingTicks := make(map[uint32]uint32)

	for _, epochIntervals := range epochsIntervals {
		lastProcessedTick, err := p.statusStore.GetLastProcessedTick(epochIntervals.Epoch)
		if errors.Is(err, entities2.ErrStoreEntityNotFound) {
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

func (p *Processor) processEpoch(startTick uint32, epochTickIntervals entities2.ProcessedTickIntervalsPerEpoch) error {
	p.logger.Infow("Starting epoch processor", "epoch", epochTickIntervals.Epoch, "startTick", startTick)

	for {
		lastProcessedTick, err := p.processBatch(startTick, epochTickIntervals)
		if err != nil {
			p.logger.Errorw("error processing batch", "epoch", epochTickIntervals.Epoch, "startTick", startTick, "error", fmt.Errorf("processing batch: %v", err))
			continue
		}

		// if lastProcessedTick is equal to the last tick from intervals, we are done
		lastTickFromIntervals := epochTickIntervals.Intervals[len(epochTickIntervals.Intervals)-1].LastProcessedTick
		if lastProcessedTick == lastTickFromIntervals {
			break
		}

		startTick = lastProcessedTick + 1
	}

	return nil
}

func (p *Processor) processBatch(startTick uint32, epochTickIntervals entities2.ProcessedTickIntervalsPerEpoch) (uint32, error) {
	epoch := epochTickIntervals.Epoch

	tickTransactionsBatch, tick, err := p.gatherTickTransactionsBatch(epoch, startTick, epochTickIntervals)
	if err != nil {
		return 0, fmt.Errorf("gathering tick tx batch: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.publishTimeout)
	defer cancel()

	p.logger.Infow("Publishing tick transactions", "nr_transactions", len(tickTransactionsBatch), "epoch", epoch, "tick", tick)
	err = p.publisher.PublishTickTransactions(ctx, tickTransactionsBatch)
	if err != nil {
		return 0, fmt.Errorf("inserting batch: %v", err)
	}

	p.logger.Infow("Storing last processed tick", "epoch", epoch, "tick", tick)
	err = p.statusStore.SetLastProcessedTick(epoch, tick)
	if err != nil {
		return 0, fmt.Errorf("setting last processed tick: %v", err)
	}

	return tick, nil
}

func (p *Processor) gatherTickTransactionsBatch(epoch, startTick uint32, epochTickIntervals entities2.ProcessedTickIntervalsPerEpoch) ([]entities2.TickTransactions, uint32, error) {

	var tickTransactionsBatch []entities2.TickTransactions

	var tick uint32

	for intervalIndex, interval := range epochTickIntervals.Intervals {
		for tick = interval.InitialProcessedTick; tick <= interval.LastProcessedTick; tick++ {
			if tick < startTick {
				continue
			}

			transactions, err := func() ([]entities2.Tx, error) {
				ctx, cancel := context.WithTimeout(context.Background(), p.fetchTimeout)
				defer cancel()

				return p.fetcher.GetTickTransactions(ctx, tick)
			}()

			if err != nil {
				if !errors.Is(err, entities2.ErrEmptyTick) {
					p.logger.Errorw("error processing tick; retrying...", "epoch", epoch, "tick", tick, "error", fmt.Errorf("getting transactions: %v", err))
					tick--
					continue
				}
				transactions = []entities2.Tx{}
			}

			tickTransactions := entities2.TickTransactions{
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
