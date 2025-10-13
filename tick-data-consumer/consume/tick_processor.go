package consume

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/qubic/tick-data-consumer/domain"
	"github.com/qubic/tick-data-consumer/elastic"
	"github.com/qubic/tick-data-consumer/metrics"
)

type KafkaClient interface {
	PollMessages(ctx context.Context) ([]*domain.TickData, error)
	Commit(ctx context.Context) error
	AllowRebalance()
}

type ElasticClient interface {
	BulkIndex(ctx context.Context, data []*elastic.EsDocument) error
}

type TickProcessor struct {
	kafkaClient    KafkaClient
	elasticClient  ElasticClient
	consumeMetrics *metrics.Metrics
}

func NewTickProcessor(kafkaClient KafkaClient, elasticClient ElasticClient, metrics *metrics.Metrics) *TickProcessor {
	return &TickProcessor{
		kafkaClient:    kafkaClient,
		elasticClient:  elasticClient,
		consumeMetrics: metrics,
	}
}

func (p *TickProcessor) Consume() error {
	for {
		count, err := p.consumeBatch(context.Background())
		if err != nil {
			// if there is an error consuming we abort. We need to fix the error before trying again.
			log.Printf("Error consuming batch: %v", err)
			return errors.Wrap(err, "consuming batch")
		} else {
			p.consumeMetrics.AddProcessedTicks(count)
			log.Printf("Processed [%d] ticks.", count)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (p *TickProcessor) consumeBatch(ctx context.Context) (int, error) {
	// get messages
	defer p.kafkaClient.AllowRebalance()
	tickDataList, err := p.kafkaClient.PollMessages(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "poll messages")
	}

	// send to elastic
	err = p.sendToElastic(ctx, tickDataList)
	if err != nil {
		return 0, err
	}

	// commit
	err = p.kafkaClient.Commit(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "committing batch")
	}
	return len(tickDataList), nil
}

func (p *TickProcessor) sendToElastic(ctx context.Context, tickDataList []*domain.TickData) error {
	var documents []*elastic.EsDocument
	for _, tickData := range tickDataList {
		if tickData == nil || tickData.Epoch == 0 || tickData.Epoch == 65535 || tickData.TickNumber == 0 {
			return errors.New("tick data is empty")
		}
		document, err := convertToDocument(tickData)
		if err != nil {
			return err
		}
		documents = append(documents, document)
	}
	err := p.elasticClient.BulkIndex(ctx, documents)
	if err != nil {
		return errors.Wrap(err, "elastic indexing")
	}
	return nil
}

func convertToDocument(tickData *domain.TickData) (*elastic.EsDocument, error) {
	val, err := json.Marshal(tickData)
	if err != nil {
		return nil, errors.Wrapf(err, "unmarshalling tick data %+v", tickData)
	}
	document := &elastic.EsDocument{
		Id:      strconv.Itoa(int(tickData.TickNumber)), // we use the tick number as unique id
		Payload: val,
	}
	return document, nil
}
