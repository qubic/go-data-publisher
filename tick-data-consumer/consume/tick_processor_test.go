package consume

import (
	"context"
	"errors"
	"testing"

	"github.com/qubic/tick-data-consumer/domain"
	"github.com/qubic/tick-data-consumer/elastic"
	"github.com/qubic/tick-data-consumer/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type FakeKafkaClient struct {
	tickDataList        []*domain.TickData
	commitCount         int
	allowRebalanceCount int
}

func (f *FakeKafkaClient) PollMessages(_ context.Context) ([]*domain.TickData, error) {
	return f.tickDataList, nil
}

func (f *FakeKafkaClient) Commit(_ context.Context) error {
	f.commitCount++
	return nil
}

func (f *FakeKafkaClient) AllowRebalance() {
	f.allowRebalanceCount++
}

type FakeElasticClient struct {
	err            error
	bulkIndexCount int
}

func (f *FakeElasticClient) BulkIndex(_ context.Context, data []*elastic.EsDocument) error {
	for _, d := range data {
		if len(d.Id) == 0 {
			return errors.New("empty id")
		}
	}
	if f.err != nil {
		return f.err
	} else {
		f.bulkIndexCount += len(data)
		return nil
	}
}

var m = metrics.NewMetrics("test")

func TestTickProcessor_consumeBatch_thenFetchSendCommit(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		tickDataList: []*domain.TickData{
			{Epoch: 1, TickNumber: 1}, {Epoch: 1, TickNumber: 2},
		},
	}
	elasticClient := &FakeElasticClient{}
	processor := NewTickProcessor(kafkaClient, elasticClient, m)

	count, err := processor.consumeBatch(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Equal(t, 1, kafkaClient.commitCount)
	assert.Equal(t, 1, kafkaClient.allowRebalanceCount)
	assert.Equal(t, 2, elasticClient.bulkIndexCount)

	count, err = processor.consumeBatch(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Equal(t, 2, kafkaClient.commitCount)
	assert.Equal(t, 2, kafkaClient.allowRebalanceCount)
	assert.Equal(t, 4, elasticClient.bulkIndexCount)
}

func TestTickProcessor_consumeBatch_givenEmptyTick_thenError(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		tickDataList: []*domain.TickData{
			{Epoch: 65535, TickNumber: 0},
		},
	}
	elasticClient := &FakeElasticClient{}
	processor := NewTickProcessor(kafkaClient, elasticClient, m)

	_, err := processor.consumeBatch(context.Background())
	require.Error(t, err)
}

func TestTickProcessor_consumeBatch_givenErrorSending_thenErrorAndNoCommit(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		tickDataList: []*domain.TickData{
			{},
		},
	}
	elasticClient := &FakeElasticClient{
		err: errors.New("error"),
	}
	processor := NewTickProcessor(kafkaClient, elasticClient, m)

	_, err := processor.consumeBatch(context.Background())
	assert.Error(t, err)
	assert.Equal(t, 0, kafkaClient.commitCount)
	assert.Equal(t, 1, kafkaClient.allowRebalanceCount) // do not lock partitioning logic
	assert.Equal(t, 0, elasticClient.bulkIndexCount)
}

func TestTickProcessor_convertToDocument(t *testing.T) {
	tickData := &domain.TickData{
		ComputorIndex: 1,
		Epoch:         2,
		TickNumber:    3,
		Timestamp:     4,
		Signature:     "sig",
	}
	document, err := convertToDocument(tickData)
	assert.NoError(t, err)
	assert.NotNil(t, document)
	assert.Equal(t, "3", document.Id)
	assert.Equal(t, `{"computorIndex":1,"epoch":2,"tickNumber":3,"timestamp":4,"signature":"sig"}`, string(document.Payload))
}
