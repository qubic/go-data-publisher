package consume

import (
	"context"
	"errors"
	"testing"

	"github.com/qubic/tick-intervals-consumer/domain"
	"github.com/qubic/tick-intervals-consumer/elastic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type FakeKafkaClient struct {
	tickIntervals       []*domain.TickInterval
	commitCount         int
	allowRebalanceCount int
}

func (f *FakeKafkaClient) PollMessages(_ context.Context) ([]*domain.TickInterval, error) {
	return f.tickIntervals, nil
}

func (f *FakeKafkaClient) Commit(_ context.Context) error {
	f.commitCount++
	return nil
}

func (f *FakeKafkaClient) AllowRebalance() {
	f.allowRebalanceCount++
}

type FakeElasticClient struct {
	err                 error
	bulkIndexCount      int
	sentDocuments       []*elastic.EsDocument
	overlappingInterval *elastic.Interval
}

func (f *FakeElasticClient) FindOverlappingInterval(_ context.Context, _, _, _ uint32) (*elastic.Interval, error) {
	return f.overlappingInterval, nil
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
		f.sentDocuments = append(f.sentDocuments, data...)
		f.bulkIndexCount += len(data)
		return nil
	}
}

func TestProcessor_Consume_GivenError_ThenReturnError(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		tickIntervals: []*domain.TickInterval{
			{
				Epoch: 42,
				From:  123,
				To:    456,
			},
		},
	}
	esClient := &FakeElasticClient{
		err: errors.New("test error"),
	}
	processor := NewProcessor(kafkaClient, esClient)
	err := processor.Consume()
	require.Error(t, err)
	require.Equal(t, 0, kafkaClient.commitCount)
	require.Equal(t, 1, kafkaClient.allowRebalanceCount) // do not lock partitioning logic
	require.Equal(t, 0, esClient.bulkIndexCount)
}

func TestProcessor_consumeBatch(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		tickIntervals: []*domain.TickInterval{
			{
				Epoch: 42,
				From:  123,
				To:    456,
			},
			{
				Epoch: 42,
				From:  567,
				To:    789,
			},
		},
	}
	esClient := &FakeElasticClient{}
	processor := NewProcessor(kafkaClient, esClient)
	count, err := processor.consumeBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, 2, esClient.bulkIndexCount)
	require.Equal(t, 1, kafkaClient.commitCount)
	require.Equal(t, 1, kafkaClient.allowRebalanceCount)
}

func TestProcessor_consumeBatch_GivenInvalidInterval_ThenErrorAndDoNotCommit(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		tickIntervals: []*domain.TickInterval{
			{
				Epoch: 0,
				From:  0,
				To:    0,
			},
		},
	}
	esClient := &FakeElasticClient{}
	processor := NewProcessor(kafkaClient, esClient)
	count, err := processor.consumeBatch(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, count)
	require.Equal(t, 0, kafkaClient.commitCount)
	require.Equal(t, 1, kafkaClient.allowRebalanceCount) // do not lock partitioning logic
	require.Equal(t, 0, esClient.bulkIndexCount)
}

func TestTickProcessor_convertToDocument(t *testing.T) {
	interval := &domain.TickInterval{
		Epoch: 42,
		From:  123,
		To:    456,
	}
	document, err := convertToDocument(interval)
	assert.NoError(t, err)
	assert.NotNil(t, document)
	assert.Equal(t, "42-123", document.Id)
	assert.JSONEq(t, `{ "epoch":42, "from":123, "to":456 }`, string(document.Payload))
}

func TestTickProcessor_consumeBatch_GivenEmpty_ThenDoNothing(t *testing.T) {
	kafkaClient := &FakeKafkaClient{}
	esClient := &FakeElasticClient{}
	processor := NewProcessor(kafkaClient, esClient)
	count, err := processor.consumeBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, count)
	require.Equal(t, 1, kafkaClient.commitCount)
	require.Equal(t, 1, kafkaClient.allowRebalanceCount) // do not lock partitioning logic
	require.Equal(t, 0, esClient.bulkIndexCount)

}

func TestTickProcessor_consumeBatch_GivenOverlappingInterval_ThenFilter(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		tickIntervals: []*domain.TickInterval{
			{
				Epoch: 1,
				From:  2,
				To:    3,
			},
		},
	}
	esClient := &FakeElasticClient{
		overlappingInterval: &elastic.Interval{
			Epoch: 1,
			From:  2,
			To:    4,
		},
	}
	processor := NewProcessor(kafkaClient, esClient)
	count, err := processor.consumeBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, 1, kafkaClient.commitCount)
	require.Equal(t, 1, kafkaClient.allowRebalanceCount) // do not lock partitioning logic
	require.Equal(t, 0, esClient.bulkIndexCount)
}

func TestTickProcessor_consumeBatch_GivenSmallerOverlappingInterval_ThenReplace(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		tickIntervals: []*domain.TickInterval{
			{
				Epoch: 1,
				From:  2,
				To:    10,
			},
		},
	}
	esClient := &FakeElasticClient{
		overlappingInterval: &elastic.Interval{
			Epoch: 1,
			From:  2,
			To:    8,
		},
	}
	processor := NewProcessor(kafkaClient, esClient)
	count, err := processor.consumeBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, 1, kafkaClient.commitCount)
	require.Equal(t, 1, kafkaClient.allowRebalanceCount) // do not lock partitioning logic
	require.Equal(t, 1, esClient.bulkIndexCount)
	assert.JSONEq(t, `{ "epoch":1, "from":2, "to":10 }`, string(esClient.sentDocuments[0].Payload))
}

func TestTickProcessor_consumeBatch_GivenInvalidOverlappingInterval_ThenError(t *testing.T) {
	kafkaClient := &FakeKafkaClient{
		tickIntervals: []*domain.TickInterval{
			{
				Epoch: 1,
				From:  2,
				To:    10,
			},
		},
	}
	esClient := &FakeElasticClient{
		overlappingInterval: &elastic.Interval{
			Epoch: 1,
			From:  3,
			To:    8,
		},
	}
	processor := NewProcessor(kafkaClient, esClient)
	count, err := processor.consumeBatch(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "conflicts")
	require.Equal(t, 0, count)
}
