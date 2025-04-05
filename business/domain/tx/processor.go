package tx

import (
	"context"
	"errors"
	"fmt"
	"github.com/qubic/go-data-publisher/entities"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
)

type Fetcher interface {
	GetProcessedTickIntervalsPerEpoch(ctx context.Context) ([]entities.ProcessedTickIntervalsPerEpoch, error)
	GetTickTransactions(ctx context.Context, tick uint32) ([]entities.Tx, error)
}

type Publisher interface {
	PublishTransactions(ctx context.Context, txs []entities.Tx) error
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
		p.waitWorkerToFreeUp(nrWorkers, &startedWorkers)

		startingTick, ok := startingTicksForEpochs[epochIntervals.Epoch]
		if !ok {
			return fmt.Errorf("starting tick not found for epoch %d", epochIntervals.Epoch)
		}

		startedWorkers.Add(1)
		go func() {
			defer startedWorkers.Add(-1)

			err = p.processEpoch(startingTick, epochIntervals)
			if err != nil {
				p.logger.Errorw("error processing epoch", "epoch", epochIntervals.Epoch, "error", err)
			} else {
				p.logger.Infow("Finished processing epoch", "epoch", epochIntervals.Epoch)
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
			continue
		}

		startingTicks[epochIntervals.Epoch] = lastProcessedTick + 1
	}

	return startingTicks, nil
}

func (p *Processor) processEpoch(startTick uint32, epochTickIntervals entities.ProcessedTickIntervalsPerEpoch) error {
	p.logger.Infow("Starting epoch processor", "epoch", epochTickIntervals.Epoch, "startTick", startTick)

	for {
		lastProcessedTick, err := p.processBatch(startTick, epochTickIntervals)
		if err != nil {
			p.logger.Errorw("error processing batch", "epoch", epochTickIntervals.Epoch, "startTick", startTick, "error", fmt.Errorf("processing batch: %v", err))
			continue
		}

		startTick = lastProcessedTick + 1
	}
}

func (p *Processor) processBatch(startTick uint32, epochTickIntervals entities.ProcessedTickIntervalsPerEpoch) (uint32, error) {
	epoch := epochTickIntervals.Epoch

	batchTxToInsert, tick, err := p.gatherTxBatch(epoch, startTick, epochTickIntervals)
	if err != nil {
		return 0, fmt.Errorf("gathering tx batch: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.publishTimeout)
	defer cancel()

	err = p.publisher.PublishTransactions(ctx, batchTxToInsert)
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

func (p *Processor) gatherTxBatch(epoch, startTick uint32, epochTickIntervals entities.ProcessedTickIntervalsPerEpoch) ([]entities.Tx, uint32, error) {
	batchTxToInsert := make([]entities.Tx, 0, p.batchSize+1024)
	var tick uint32

	for _, interval := range epochTickIntervals.Intervals {
		for tick = interval.InitialProcessedTick; tick <= interval.LastProcessedTick; tick++ {
			if tick < startTick {
				continue
			}

			txs, err := func() ([]entities.Tx, error) {
				ctx, cancel := context.WithTimeout(context.Background(), p.fetchTimeout)
				defer cancel()

				return p.fetcher.GetTickTransactions(ctx, tick)
			}()
			if errors.Is(err, entities.ErrEmptyTick) {
				if tick == interval.LastProcessedTick {
					return batchTxToInsert, tick, nil
				}

				continue
			}

			if err != nil {
				p.logger.Errorw("error processing tick; retrying...", "epoch", epoch, "tick", tick, "error", fmt.Errorf("getting transactions: %v", err))
				tick--
				continue
			}

			batchTxToInsert = append(batchTxToInsert, txs...)
			if len(batchTxToInsert) >= p.batchSize || tick == interval.LastProcessedTick {
				return batchTxToInsert, tick, nil
			}
		}
	}

	return batchTxToInsert, tick, nil
}
