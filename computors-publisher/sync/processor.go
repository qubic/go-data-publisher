package sync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/qubic/computors-publisher/db"
	"github.com/qubic/computors-publisher/domain"
	"github.com/qubic/computors-publisher/metrics"
	"github.com/qubic/go-qubic/common"
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

func (p *EpochComputorsProcessor) StartProcessing() {
	// do one initial processing, so we do not wait until first tick
	p.process()
	log.Println("Initial processing completed. Starting loop...")
	ticker := time.Tick(time.Second * 10)
	for range ticker {
		p.process()
	}
}

func (p *EpochComputorsProcessor) process() {
	err := p.processEpochs()
	if err != nil {
		log.Printf("Error processing epoch computors: %v", err)
	}
}

func (p *EpochComputorsProcessor) processEpochs() error {

	status, err := p.archiveClient.GetStatus(context.Background())
	if err != nil {
		return fmt.Errorf("getting archive status: %w", err)
	}
	archiverEpoch := status.LastProcessedTick.Epoch
	archiverTick := status.LastProcessedTick.TickNumber
	p.processingMetrics.SetSourceTick(archiverEpoch, archiverTick)

	lastProcessedEpoch, err := p.dataStore.GetLastProcessedEpoch()
	if err != nil {
		return fmt.Errorf("getting last processed epoch: %w", err)
	}
	if lastProcessedEpoch == 0 {
		lastProcessedEpoch = status.EpochList[0]
	}

	if lastProcessedEpoch > archiverEpoch {
		return fmt.Errorf("last processed epoch [%d] is larger than current epoch [%d]: %w", lastProcessedEpoch, archiverEpoch, err)
	}

	epochsToProcess, err := p.findEpochsToPublish(status, archiverEpoch, lastProcessedEpoch)
	if err != nil {
		return fmt.Errorf("finding epochs to publish: %w", err)
	}

	for _, epoch := range epochsToProcess {
		err = p.processEpoch(epoch, status)
		if err != nil {
			return fmt.Errorf("processing epoch %d: %w", epoch, err)
		}
	}
	p.processingMetrics.SetProcessedTick(archiverEpoch, archiverTick)
	return nil
}

func (p *EpochComputorsProcessor) processEpoch(epoch uint32, status *domain.Status) error {

	lastStoredChecksum, err := p.dataStore.GetLastStoredComputorListSum(epoch)
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		return fmt.Errorf("getting last stored computor list for epoch [%d]: %w", epoch, err)
	}

	epochComputorList, err := p.fetchArchiverComputorList(epoch)
	if err != nil {
		return fmt.Errorf("fetching archive computor list: %w", err)
	}
	if epochComputorList.Epoch != epoch {
		return fmt.Errorf("wrong epoch computor list returned by archiver. expected [%d] got [%d]", epoch, epochComputorList.Epoch)
	}

	checksum, err := computeComputorsChecksum(*epochComputorList)
	if err != nil {
		return fmt.Errorf("computing computors checksum for epoch [%d]: %w", epoch, err)
	}

	if bytes.Equal(checksum, lastStoredChecksum) {
		return nil // same list already published
	}
	log.Printf("New computors list checksum [%s] for epoch [%d]. Previous: [%s]",
		hex.EncodeToString(checksum), epoch,
		hex.EncodeToString(lastStoredChecksum))

	if epochComputorList.TickNumber == 0 { // archiver might return '0' for initial list it collected in one epoch
		epochComputorList.TickNumber, err = calculateTickNumber(status, epoch, len(lastStoredChecksum) == 0)
		if err != nil {
			return fmt.Errorf("calculating tick number: %w", err)
		}
	} else {
		log.Printf("[WARN] tick number already set. Remove old tick number calculation code.")
	}

	log.Printf("Publish new list for epoch [%d], tick [%d], signature [%s].",
		epochComputorList.Epoch, epochComputorList.TickNumber, epochComputorList.Signature)
	err = p.Producer.SendMessage(context.Background(), epochComputorList)
	if err != nil {
		return fmt.Errorf("producing epoch computor list record for epoch [%d]: %w", epoch, err)
	}
	p.processingMetrics.SetProcessedTick(epochComputorList.Epoch, epochComputorList.TickNumber)
	p.processingMetrics.IncProcessedMessages()

	err = p.dataStore.SetLastStoredComputorListSum(epoch, checksum)
	if err != nil {
		return fmt.Errorf("setting last stored computor list sum for epoch [%d]: %w", epoch, err)
	}

	err = p.dataStore.SetLastProcessedEpoch(epoch)
	if err != nil {
		return fmt.Errorf("failed to store last processed epoch [%d]: %w", epoch, err)
	}

	log.Printf("Successfully published computors list for epoch [%d].", epoch)
	return nil
}

func (p *EpochComputorsProcessor) findEpochsToPublish(status *domain.Status, currentEpoch, lastProcessedEpoch uint32) ([]uint32, error) {
	if currentEpoch-lastProcessedEpoch > 1 { // Process multiple epochs
		startIndex := -1
		for i, epoch := range status.EpochList {
			if epoch == lastProcessedEpoch {
				startIndex = i
				break
			}
		}
		if startIndex == -1 {
			return nil, fmt.Errorf("last processed epoch %d not found in epoch list", lastProcessedEpoch)
		}
		return status.EpochList[startIndex:], nil
	} else {
		return []uint32{currentEpoch}, nil
	}
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

func calculateTickNumber(status *domain.Status, epoch uint32, isInitialListOfEpoch bool) (uint32, error) {
	if isInitialListOfEpoch {
		// initial list of epoch - return initial tick
		tickIntervals, ok := status.TickIntervals[epoch]
		if !ok || len(tickIntervals) == 0 {
			return 0, fmt.Errorf("calculating initial tick of epoch [%d]", epoch)
		}
		return tickIntervals[0].FirstTick, nil
	} else {
		log.Printf("Computors list changed within epoch.")
		if epoch != status.LastProcessedTick.Epoch {
			// setting tick number for changes in old epochs is not supported
			return 0, fmt.Errorf("unexpected list change in epoch [%d]", epoch)
		}
		// list changed within epoch. Return current tick.
		return status.LastProcessedTick.TickNumber, nil
	}
}

func computeComputorsChecksum(computors domain.EpochComputors) ([]byte, error) {
	var buff bytes.Buffer

	// epoch
	err := binary.Write(&buff, binary.LittleEndian, computors.Epoch)
	if err != nil {
		return nil, fmt.Errorf("writing epoch to buffer: %w", err)
	}

	// computors
	for _, identity := range computors.Identities {
		_, err = buff.Write([]byte(identity))
		if err != nil {
			return nil, fmt.Errorf("writing identity [%s] to buffer: %w", identity, err)
		}
	}

	// signature
	_, err = buff.Write([]byte(computors.Signature))
	if err != nil {
		return nil, fmt.Errorf("writing signature [%s] to buffer: %w", computors.Signature, err)
	}

	// k12 hash
	hash, err := common.K12Hash(buff.Bytes())
	if err != nil {
		return nil, fmt.Errorf("generating hash: %w", err)
	}
	return hash[:], nil
}
