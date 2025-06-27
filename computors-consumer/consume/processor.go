package consume

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/qubic/computors-consumer/domain"
	"github.com/qubic/computors-consumer/elastic"
	"log"
	"strconv"
	"time"
)

type KafkaClient interface {
	PollMessages(ctx context.Context) ([]*domain.EpochComputors, error)
	Commit(ctx context.Context) error
	AllowRebalance()
}

type ElasticClient interface {
	BulkIndex(ctx context.Context, data []*elastic.EsDocument) error
}

type EpochProcessor struct {
	kafkaClient   KafkaClient
	elasticClient ElasticClient
}

func NewEpochProcessor(client KafkaClient, elasticClient ElasticClient) *EpochProcessor {
	return &EpochProcessor{
		kafkaClient:   client,
		elasticClient: elasticClient,
	}
}

func (p *EpochProcessor) Consume() error {
	// do one initial consume(), so we do not wait until first tick
	err := p.consume()
	if err != nil {
		return errors.Wrap(err, "consuming batch")
	}

	ticker := time.Tick(time.Minute * 30)
	for range ticker {
		err := p.consume()
		if err != nil {
			return errors.Wrap(err, "consuming batch")
		}
	}
	return nil
}

func (p *EpochProcessor) consume() error {
	count, err := p.consumeBatch(context.Background())
	if err != nil {
		log.Printf("Error consuming batch: %v", err)
		return err
	} else {
		log.Printf("Consumed [%d] epochs.\n", count)
	}
	return nil
}

func (p *EpochProcessor) consumeBatch(ctx context.Context) (int, error) {
	defer p.kafkaClient.AllowRebalance()
	epochComputorList, err := p.kafkaClient.PollMessages(ctx)
	if err != nil {
		return -1, errors.Wrap(err, "polling kafka messages")
	}

	err = p.sendToElastic(ctx, epochComputorList)
	if err != nil {
		return -1, errors.Wrap(err, "sending epoch computor batch to elastic")
	}

	err = p.kafkaClient.Commit(ctx)
	if err != nil {
		return -1, errors.Wrap(err, "commiting kafka batch")
	}
	return len(epochComputorList), nil
}

func (p *EpochProcessor) sendToElastic(ctx context.Context, epochComputorsList []*domain.EpochComputors) error {
	var documents []*elastic.EsDocument
	for _, epochComputors := range epochComputorsList {
		document, err := convertToDocument(epochComputors)
		if err != nil {
			return errors.Wrap(err, "converting computor list to elastic document")
		}
		documents = append(documents, document)
	}
	err := p.elasticClient.BulkIndex(ctx, documents)
	if err != nil {
		return errors.Wrap(err, "bulk indexing elastic documents")
	}
	return nil
}

func convertToDocument(computors *domain.EpochComputors) (*elastic.EsDocument, error) {
	val, err := json.Marshal(computors)
	if err != nil {
		return nil, errors.Wrapf(err, "unmarshalling computors list %+v", computors)
	}
	document := &elastic.EsDocument{
		Id:      strconv.Itoa(int(computors.Epoch)),
		Payload: val,
	}
	return document, err
}
