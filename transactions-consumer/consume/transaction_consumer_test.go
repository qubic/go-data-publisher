package consume

import (
	"context"
	"errors"
	"log"
	"testing"

	"github.com/qubic/transactions-consumer/extern"
	"github.com/qubic/transactions-consumer/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

var m = metrics.NewMetrics("foo")

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
	BatchesByIndex map[string][]extern.EsDocument
}

func (c *FakeElasticClient) BulkIndex(_ context.Context, data []extern.EsDocument, indexName string) error {
	log.Printf("Bulk index [%d] documents.", len(data))
	if c.BatchesByIndex == nil {
		c.BatchesByIndex = make(map[string][]extern.EsDocument)
	}
	c.BatchesByIndex[indexName] = data
	return nil
}

func TestTransactionConsumer_ConsumeBatch(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		value: []byte(`{"epoch":123,"tickNumber":456,"transactions":[{"hash":"transaction-hash","source":"source-identity","destination":"destination-identity","amount":1,"tickNumber":2,"inputType":3,"inputSize":4,"inputData":"input-data","signature":"signature","timestamp":5,"moneyFlew":true}]}`),
	}
	localElastic := &FakeElasticClient{}
	transactionConsumer := &TransactionConsumer{
		kafkaClient:        kafkaClient,
		elasticClient:      localElastic,
		consumerMetrics:    m,
		permanentIndexName: "default",
		currentTick:        0,
		currentEpoch:       0,
	}

	count, err := transactionConsumer.consumeBatch()
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, uint32(123), transactionConsumer.currentEpoch)
	assert.Equal(t, uint32(456), transactionConsumer.currentTick)

	docs := localElastic.BatchesByIndex["default"]
	require.NotNil(t, docs)
	assert.Equal(t, "transaction-hash", docs[0].Id)
	assert.Equal(t, []byte(`{"hash":"transaction-hash","source":"source-identity","destination":"destination-identity","amount":1,"tickNumber":2,"inputType":3,"inputSize":4,"inputData":"input-data","signature":"signature","timestamp":5,"moneyFlew":true}`), docs[0].Payload)
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
	require.ErrorContains(t, err, "fetching records")
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
	require.ErrorContains(t, err, "unmarshalling")
}

func TestTransactionConsumer_EphemeralAndPermanentIndexedSeparately(t *testing.T) {
	const zeroAddress = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB"
	kafkaClient := &FakeKafkaClient{
		value: []byte(`{"epoch":1,"tickNumber":1,"transactions":[` +
			`{"hash":"ephemeral-tx","source":"src","destination":"` + zeroAddress + `","amount":0,"tickNumber":1,"inputType":6,"inputSize":0,"inputData":"","signature":"","timestamp":0,"moneyFlew":false},` +
			`{"hash":"permanent-tx-1","source":"src","destination":"other-dest-1","amount":0,"tickNumber":1,"inputType":6,"inputSize":0,"inputData":"","signature":"","timestamp":0,"moneyFlew":false},` +
			`{"hash":"permanent-tx-2","source":"src","destination":"other-dest-2","amount":0,"tickNumber":1,"inputType":0,"inputSize":0,"inputData":"","signature":"","timestamp":0,"moneyFlew":false}` +
			`]}`),
	}
	localElastic := &FakeElasticClient{}
	consumer := &TransactionConsumer{
		kafkaClient:         kafkaClient,
		elasticClient:       localElastic,
		consumerMetrics:     m,
		permanentIndexName:  "permanent-index",
		ephemeralIndexName:  "ephemeral-index",
		ephemeralInputTypes: []uint32{6},
	}

	count, err := consumer.consumeBatch()
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	ephemeralDocs := localElastic.BatchesByIndex["ephemeral-index"]
	permanentDocs := localElastic.BatchesByIndex["permanent-index"]

	assert.Len(t, ephemeralDocs, 1)
	assert.Equal(t, "ephemeral-tx", ephemeralDocs[0].Id)

	assert.Len(t, permanentDocs, 2)
	assert.Equal(t, "permanent-tx-1", permanentDocs[0].Id)
	assert.Equal(t, "permanent-tx-2", permanentDocs[1].Id)
}

func TestTransactionConsumer_IsEphemeral(t *testing.T) {
	const zeroAddress = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB"
	const otherAddress = "FOO"

	tests := []struct {
		name                string
		ephemeralInputTypes []uint32
		inputType           uint32
		dest                string
		want                bool
	}{
		{
			name:                "all conditions met - is ephemeral",
			ephemeralInputTypes: []uint32{1, 2, 3},
			inputType:           2,
			dest:                zeroAddress,
			want:                true,
		},
		{
			name:                "empty ephemeral input types - not ephemeral",
			ephemeralInputTypes: []uint32{},
			inputType:           2,
			dest:                zeroAddress,
			want:                false,
		},
		{
			name:                "nil ephemeral input types - not ephemeral",
			ephemeralInputTypes: nil,
			inputType:           2,
			dest:                zeroAddress,
			want:                false,
		},
		{
			name:                "input type not in list - not ephemeral",
			ephemeralInputTypes: []uint32{1, 3},
			inputType:           2,
			dest:                zeroAddress,
			want:                false,
		},
		{
			name:                "dest is not zero address - not ephemeral",
			ephemeralInputTypes: []uint32{1, 2, 3},
			inputType:           2,
			dest:                otherAddress,
			want:                false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &TransactionConsumer{ephemeralInputTypes: tt.ephemeralInputTypes}
			got := c.isEphemeral(tt.inputType, tt.dest)
			assert.Equal(t, tt.want, got)
		})
	}
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
