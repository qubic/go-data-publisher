package sync

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/qubic/computors-publisher/domain"
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
}

type Producer interface {
	SendMessage(ctx context.Context, computorList *domain.EpochComputors) error
}

type EpochComputorsProcessor struct {
	archiveClient ArchiveClient
	dataStore     DataStore
	Producer      Producer
}

func NewEpochComputorsProcessor(client ArchiveClient, store DataStore, producer Producer) *EpochComputorsProcessor {
	return &EpochComputorsProcessor{
		archiveClient: client,
		dataStore:     store,
		Producer:      producer,
	}
}

func (p *EpochComputorsProcessor) StartProcessing() error {

	// do one initial process(), so we do not wait until first tick
	err := p.process()
	if err != nil {
		return errors.Wrap(err, "processing epoch computors")
	}

	ticker := time.Tick(1 * time.Hour * 24)
	for range ticker {
		err := p.process()
		if err != nil {
			return errors.Wrap(err, "processing epoch computors")
		}
	}
	return nil
}

func (p *EpochComputorsProcessor) process() error {

	status, err := p.archiveClient.GetStatus(context.Background())
	if err != nil {
		return errors.Wrap(err, "getting archive status")
	}
	lastProcessedEpoch, err := p.dataStore.GetLastProcessedEpoch()
	if err != nil {
		return errors.Wrap(err, "getting last processed epoch")
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
		return errors.Errorf("last processes epoch [%d] is larger than current netowrk epoch [%d]", lastProcessedEpoch, currentEpoch)
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
			return errors.Errorf("last processed epoch %d not found in epoch list", lastProcessedEpoch)
		}
		epochsToProcess = status.EpochList[startIndex:]

	} else {
		epochsToProcess = []uint32{currentEpoch}
	}

	for _, epoch := range epochsToProcess {
		err = p.processEpoch(epoch)
		if err != nil {
			return errors.Wrapf(err, "processing epoch %d", epoch)
		}
	}
	return nil
}

func (p *EpochComputorsProcessor) processEpoch(epoch uint32) error {

	fmt.Printf("Processing epoch [%d]\n", epoch)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	epochComputorList, err := p.archiveClient.GetEpochComputors(ctx, epoch)
	if err != nil {
		return errors.Wrapf(err, "getting archive computor list for epoch [%d]", epoch)
	}
	if epochComputorList.Epoch != epoch {
		return errors.Errorf("wrong epoch coputor list returned by archier. expected [%d] got [%d]", epoch, epochComputorList.Epoch)
	}

	fmt.Printf("Processed epoch: %d\n", epoch)

	err = p.Producer.SendMessage(ctx, epochComputorList)
	if err != nil {
		return errors.Wrapf(err, "producing epoch computor list record for epoch [%d]", epoch)
	}

	err = p.dataStore.SetLastProcessedEpoch(epoch)
	if err != nil {
		return errors.Wrapf(err, "failed to store last processed epoch [%d]", epoch)
	}

	return nil
}
