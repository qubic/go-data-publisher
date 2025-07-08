package sync

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/qubic/computors-publisher/db"
	"github.com/qubic/computors-publisher/domain"
	"github.com/qubic/computors-publisher/metrics"
	"log"
	"time"
)

type ArchiveClient interface {
	GetStatus(ctx context.Context) (*domain.Status, error)
	GetEpochComputors(ctx context.Context, epoch uint32) (*domain.EpochComputors, error)
}

type DataStore interface {
	SetLastProcessedEpoch(epoch uint32) error
	GetLastProcessedEpoch() (uint32, error)
	SetLastStoredComputorListSum(epoch uint32, sum []byte) error
	GetLastStoredComputorListSum(epoch uint32) ([]byte, error)
}

type Producer interface {
	SendMessage(ctx context.Context, computorList *domain.EpochComputors) error
}

type EpochComputorsProcessor struct {
	archiveClient     ArchiveClient
	dataStore         DataStore
	Producer          Producer
	processingMetrics *metrics.ProcessingMetrics
}

func NewEpochComputorsProcessor(client ArchiveClient, store DataStore, producer Producer, metrics *metrics.ProcessingMetrics) *EpochComputorsProcessor {
	return &EpochComputorsProcessor{
		archiveClient:     client,
		dataStore:         store,
		Producer:          producer,
		processingMetrics: metrics,
	}
}

func (p *EpochComputorsProcessor) StartProcessing() error {

	// TODO I think we should not return an error here but retry if something goes wrong.
	//      There is always the possibility that one of the services we call is down temporarily.

	// do one initial process(), so we do not wait until first tick
	err := p.process()
	if err != nil {
		return fmt.Errorf("processing epoch computors: %w", err)
	}

	ticker := time.Tick(time.Second)
	for range ticker {
		err := p.process()
		if err != nil {
			return fmt.Errorf("processing epoch computors: %w", err)
		}
	}
	return nil
}

func (p *EpochComputorsProcessor) process() error {

	status, err := p.archiveClient.GetStatus(context.Background())
	if err != nil {
		return fmt.Errorf("getting archive status: %w", err)
	}

	lastProcessedEpoch, err := p.dataStore.GetLastProcessedEpoch()
	if err != nil {
		return fmt.Errorf("getting last processed epoch: %w", err)
	}
	if lastProcessedEpoch == 0 {
		lastProcessedEpoch = status.EpochList[0]
	}

	currentEpoch := status.EpochList[len(status.EpochList)-1]
	if lastProcessedEpoch == currentEpoch {
		log.Printf("Epoch up to date.")
		return nil
	}
	if lastProcessedEpoch > currentEpoch {
		return fmt.Errorf("last processed epoch [%d] is larger than current epoch [%d]: %w", lastProcessedEpoch, currentEpoch, err)
	}

	var epochsToProcess []uint32

	if currentEpoch-lastProcessedEpoch > 1 { // Process multiple epochs
		startIndex := -1
		for i, epoch := range status.EpochList {
			if epoch == lastProcessedEpoch {
				startIndex = i
				break
			}
		}

		if startIndex == -1 {
			return fmt.Errorf("last processed epoch %d not found in epoch list", lastProcessedEpoch)
		}
		epochsToProcess = status.EpochList[startIndex:]

	} else {
		epochsToProcess = []uint32{currentEpoch}
	}

	for _, epoch := range epochsToProcess {
		err = p.processEpoch(epoch, *status)
		if err != nil {
			return fmt.Errorf("processing epoch %d: %w", epoch, err)
		}
	}
	return nil
}

func (p *EpochComputorsProcessor) processEpoch(epoch uint32, status domain.Status) error {

	fmt.Printf("Processing epoch [%d]\n", epoch)

	lastStoredComputorListSum, err := p.dataStore.GetLastStoredComputorListSum(epoch)
	if err != nil && errors.Is(err, db.ErrNotFound) {
		return fmt.Errorf("getting last stored computor list for epoch [%d]: %w", epoch, err)
	}

	epochComputorList, err := p.fetchArchiverComputorList(epoch)
	if err != nil {
		return fmt.Errorf("fetching archive computor list: %w", err)
	}
	if epochComputorList.Epoch != epoch {
		return fmt.Errorf("wrong epoch computor list returned by archiver. expected [%d] got [%d]", epoch, epochComputorList.Epoch)
	}

	currentSum, err := computeComputorListSum(*epochComputorList)
	if err != nil {
		return fmt.Errorf("computing current computor list list sum for epoch [%d]: %w", epoch, err)
	}

	if bytes.Equal(currentSum, lastStoredComputorListSum) {
		fmt.Printf("No new computor list for epoch [%d]\n", epoch)
		return nil
	}

	// TODO this is only true for the current epoch. We should not do this for old epochs. Not sure if the tick number
	//      should be part of the domain object as we do not really know the correct tick number.
	epochComputorList.TickNumber = status.LastProcessedTick.TickNumber

	err = p.Producer.SendMessage(context.Background(), epochComputorList)
	if err != nil {
		return fmt.Errorf("producing epoch computor list record for epoch [%d]: %w", epoch, err)
	}

	err = p.dataStore.SetLastStoredComputorListSum(epoch, currentSum)
	if err != nil {
		return fmt.Errorf("setting last stored computor list sum for epoch [%d]: %w", epoch, err)
	}

	err = p.dataStore.SetLastProcessedEpoch(epoch)
	if err != nil {
		return fmt.Errorf("failed to store last processed epoch [%d]: %w", epoch, err)
	}
	p.processingMetrics.SetProcessedEpoch(epoch)

	fmt.Printf("Processed epoch: %d\n", epoch)
	return nil
}

func (p *EpochComputorsProcessor) fetchArchiverComputorList(epoch uint32) (*domain.EpochComputors, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	epochComputorList, err := p.archiveClient.GetEpochComputors(ctx, epoch)
	if err != nil {
		return nil, fmt.Errorf("getting archive computor list for epoch [%d]: %w", epoch, err)
	}

	return epochComputorList, nil

}

func computeComputorListSum(computors domain.EpochComputors) ([]byte, error) {

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// TODO I don't think we should add the tick number 0 to the hash. Either use a different object for the
	//      message or create the hash from the relevant parts explicitly. See events consumer for an example
	//      for hash generation.
	computors.TickNumber = 0 // we do not want to take the tick number into consideration

	err := enc.Encode(computors)
	if err != nil {
		return nil, fmt.Errorf("encoding computor list: %w", err)
	}

	s := md5.New() // TODO shouldn't we take K12 as it is used everywhere else, too?
	s.Write(buf.Bytes())
	sum := s.Sum(nil)
	return sum, nil
}
