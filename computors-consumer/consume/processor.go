package consume

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/qubic/computors-consumer/domain"
	"github.com/qubic/computors-consumer/elastic"
	"github.com/qubic/computors-consumer/metrics"
	"github.com/qubic/go-qubic/common"
)

type KafkaClient interface {
	PollMessages(ctx context.Context) ([]*domain.EpochComputors, error)
	Commit(ctx context.Context) error
	AllowRebalance()
}

type ElasticClient interface {
	BulkIndex(ctx context.Context, data []*elastic.EsDocument) error
	FindLatestComputorsListForEpoch(ctx context.Context, epoch uint32) (*elastic.ComputorsList, error)
}

type EpochProcessor struct {
	kafkaClient   KafkaClient
	elasticClient ElasticClient
	metrics       *metrics.Metrics
	lastTick      uint32
}

func NewEpochProcessor(client KafkaClient, elasticClient ElasticClient, metrics *metrics.Metrics) *EpochProcessor {
	return &EpochProcessor{
		kafkaClient:   client,
		elasticClient: elasticClient,
		metrics:       metrics,
	}
}

func (p *EpochProcessor) Consume() error {
	ticker := time.Tick(time.Second)
	for range ticker {
		count, err := p.consumeBatch(context.Background())
		if err != nil {
			log.Printf("Error consuming batch: %v", err)
			return fmt.Errorf("consuming batch: %w", err)
		} else {
			p.metrics.IncProcessedMessages(count)
			log.Printf("Consumed [%d] message(s).", count)
		}
	}
	return nil
}

func (p *EpochProcessor) consumeBatch(ctx context.Context) (int, error) {
	defer p.kafkaClient.AllowRebalance() // because of kgo.BlockRebalanceOnPoll()
	messages, err := p.kafkaClient.PollMessages(ctx)
	if err != nil {
		return -1, fmt.Errorf("polling kafka messages: %w", err)
	}

	// duplicate check only works if there is enough time between bulk insert and querying
	filteredMessages, err := p.filterDuplicateComputorLists(ctx, messages)
	if err != nil {
		return -1, fmt.Errorf("filter duplicate computor lists: %w", err)
	}

	if len(filteredMessages) > 0 {
		err = p.sendToElastic(ctx, filteredMessages)
		if err != nil {
			return -1, fmt.Errorf("sending epoch computor batch to elastic: %w", err)
		}
	} else {
		log.Printf("No messages to send to elastic.")
	}

	err = p.kafkaClient.Commit(ctx) // because of kgo.DisableAutoCommit()
	if err != nil {
		return -1, fmt.Errorf("commiting kafka batch: %w", err)
	}
	return len(messages), nil
}

func (p *EpochProcessor) filterDuplicateComputorLists(ctx context.Context, messages []*domain.EpochComputors) ([]*domain.EpochComputors, error) {
	filteredList := make([]*domain.EpochComputors, 0, len(messages))
	for _, computorsList := range messages {
		latestList, latestErr := p.elasticClient.FindLatestComputorsListForEpoch(ctx, computorsList.Epoch)
		if latestErr != nil {
			return nil, fmt.Errorf("checking latest computors list for epoch %d: %w", computorsList.Epoch, latestErr)
		}
		if latestList == nil || latestList.Signature != computorsList.Signature {
			log.Printf("Ingest computors list for epoch [%d] and tick [%d].",
				computorsList.Epoch, computorsList.TickNumber)
			filteredList = append(filteredList, computorsList)
		} else {
			log.Printf("Ignore duplicate computors list. Epoch [%d], tick [%d], signature [%s].",
				computorsList.Epoch, computorsList.TickNumber, computorsList.Signature)
		}
	}
	return filteredList, nil
}

func (p *EpochProcessor) sendToElastic(ctx context.Context, epochComputorsList []*domain.EpochComputors) error {
	var documents []*elastic.EsDocument
	for _, epochComputors := range epochComputorsList {
		document, err := convertToDocument(epochComputors)
		if err != nil {
			return fmt.Errorf("converting computor list to elastic document: %w", err)
		}
		documents = append(documents, document)

		if p.lastTick < epochComputors.TickNumber {
			p.lastTick = epochComputors.TickNumber
			p.metrics.SetProcessedTick(epochComputors.Epoch, epochComputors.TickNumber)
		}
	}
	err := p.elasticClient.BulkIndex(ctx, documents)
	if err != nil {
		return fmt.Errorf("bulk indexing elastic documents: %w", err)
	}
	return nil
}

func convertToDocument(computors *domain.EpochComputors) (*elastic.EsDocument, error) {
	val, err := json.Marshal(computors)
	if err != nil {
		return nil, fmt.Errorf("marshalling computors list %+v: %w", computors, err)
	}

	id, err := calculateUniqueId(computors) // to avoid storing duplicate data
	if err != nil {
		return nil, fmt.Errorf("creating unique id: %w", err)
	}
	document := &elastic.EsDocument{
		Id:      id,
		Payload: val,
	}
	return document, err
}

func calculateUniqueId(event *domain.EpochComputors) (string, error) {
	var buff bytes.Buffer
	err := binary.Write(&buff, binary.LittleEndian, event.Epoch)
	if err != nil {
		return "", fmt.Errorf("writing epoch to buffer: %w", err)
	}
	err = binary.Write(&buff, binary.LittleEndian, event.TickNumber)
	if err != nil {
		return "", fmt.Errorf("writing tick to buffer: %w", err)
	}
	for _, identity := range event.Identities {
		_, err = buff.Write([]byte(identity))
		if err != nil {
			return "", fmt.Errorf("writing identity [%s] to buffer: %w", identity, err)
		}
	}

	_, err = buff.Write([]byte(event.Signature))
	if err != nil {
		return "", fmt.Errorf("writing signature [%s] to buffer: %w", event.Signature, err)
	}

	hash, err := common.K12Hash(buff.Bytes())
	if err != nil {
		return "", fmt.Errorf("failed to hash event: %w", err)
	}
	return hex.EncodeToString(hash[:]), err
}
