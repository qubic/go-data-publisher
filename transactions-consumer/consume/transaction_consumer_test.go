package consume

import (
	"context"
	"errors"
	"github.com/qubic/transactions-consumer/extern"
	"github.com/qubic/transactions-consumer/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"testing"
)

var m = metrics.NewMetrics("foo")
var elastic = &FakeElasticClient{}

type FakeKafkaClient struct {
	partitionErr error
	value        []byte
}

func (fkc *FakeKafkaClient) PollRecords(_ context.Context, _ int) kgo.Fetches {
	return createFetches(fkc.partitionErr, fkc.value)
}

func (fkc *FakeKafkaClient) CommitUncommittedOffsets(_ context.Context) error {
	return nil
}

func (fkc *FakeKafkaClient) AllowRebalance() {}

type FakeElasticClient struct {
	LatestBatch []extern.EsDocument
}

func (c *FakeElasticClient) BulkIndex(_ context.Context, data []extern.EsDocument) error {
	log.Printf("Bulk index [%d] documents.", len(data))
	c.LatestBatch = data
	return nil
}

func TestTransactionConsumer_ConsumeBatch(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		value: []byte(`{"epoch":123,"tickNumber":456,"transactions":[{"hash":"transaction-hash","source":"source-identity","destination":"destination-identity","amount":1,"tickNumber":2,"inputType":3,"inputSize":4,"inputData":"input-data","signature":"signature","timestamp":5,"moneyFlew":true}]}`),
	}
	transactionConsumer := &TransactionConsumer{
		kafkaClient:     kafkaClient,
		elasticClient:   elastic,
		consumerMetrics: m,
		currentTick:     0,
		currentEpoch:    0,
	}

	count, err := transactionConsumer.consumeBatch()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, uint32(123), transactionConsumer.currentEpoch)
	assert.Equal(t, uint32(456), transactionConsumer.currentTick)
	assert.Equal(t, "transaction-hash", elastic.LatestBatch[0].Id)
	assert.Equal(t, []byte(`{"hash":"transaction-hash","source":"source-identity","destination":"destination-identity","amount":1,"tickNumber":2,"inputType":3,"inputSize":4,"inputData":"input-data","signature":"signature","timestamp":5,"moneyFlew":true}`), elastic.LatestBatch[0].Payload)
}

func TestTransactionConsumer_GivenFetchError_ThenError(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		partitionErr: errors.New("partition-error"),
		value:        []byte("foo"),
	}
	transactionConsumer := &TransactionConsumer{
		kafkaClient:     kafkaClient,
		elasticClient:   &FakeElasticClient{},
		consumerMetrics: m,
		currentTick:     0,
		currentEpoch:    0,
	}

	_, err := transactionConsumer.consumeBatch()
	assert.ErrorContains(t, err, "fetching records")
}

func TestTransactionConsumer_GivenInvalidJson_ThenError(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		value: []byte(`{"hash":"transaction-hash"}`),
	}
	transactionConsumer := &TransactionConsumer{
		kafkaClient:     kafkaClient,
		elasticClient:   &FakeElasticClient{},
		consumerMetrics: m,
		currentTick:     0,
		currentEpoch:    0,
	}

	_, err := transactionConsumer.consumeBatch()
	assert.ErrorContains(t, err, "unmarshalling")
}

func createFetches(err error, value []byte) kgo.Fetches {
	return kgo.Fetches{
		{
			Topics: []kgo.FetchTopic{
				{
					Partitions: []kgo.FetchPartition{
						{
							Records: []*kgo.Record{
								{
									Value: value,
								},
							},
							Err: err,
						},
					},
				},
			},
		},
	}
}
