package consume

import (
	"context"
	"encoding/json"
	"github.com/qubic/computors-consumer/domain"
	"github.com/qubic/computors-consumer/elastic"
	"github.com/qubic/computors-consumer/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var m = metrics.NewMetrics("test")

type FakeKafkaClient struct {
	computorsList       []*domain.EpochComputors
	err                 error
	commitCount         int
	allowRebalanceCount int
}

func (f *FakeKafkaClient) PollMessages(_ context.Context) ([]*domain.EpochComputors, error) {
	return f.computorsList, f.err
}

func (f *FakeKafkaClient) Commit(_ context.Context) error {
	f.commitCount++
	return nil
}

func (f *FakeKafkaClient) AllowRebalance() {
	f.allowRebalanceCount++
}

type FakeElasticClient struct {
	lastDocuments  []*elastic.EsDocument
	duplicate      *elastic.ComputorsList
	err            error
	bulkIndexCount int
}

func (f *FakeElasticClient) FindLatestComputorsListForEpoch(ctx context.Context, epoch uint32) (*elastic.ComputorsList, error) {
	return f.duplicate, f.err
}

func (f *FakeElasticClient) BulkIndex(_ context.Context, documents []*elastic.EsDocument) error {
	f.lastDocuments = documents
	f.bulkIndexCount++
	return f.err
}

func TestProcessor_ConsumeBatch(t *testing.T) {
	computorsList := []*domain.EpochComputors{
		{Epoch: 1, TickNumber: 100, Identities: []string{"A", "B", "C"}, Signature: "signature-1"},
		{Epoch: 2, TickNumber: 200, Identities: []string{"A", "B", "D"}, Signature: "signature-2"},
	}

	kafkaClient := &FakeKafkaClient{
		computorsList: computorsList,
	}
	elasticClient := &FakeElasticClient{}
	processor := NewEpochProcessor(kafkaClient, elasticClient, m)
	count, err := processor.consumeBatch(context.Background())

	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, 1, kafkaClient.allowRebalanceCount)
	require.Equal(t, 1, kafkaClient.commitCount)
	require.Len(t, kafkaClient.computorsList, 2)
	require.Equal(t, 1, elasticClient.bulkIndexCount)
	require.Len(t, elasticClient.lastDocuments, 2)

	doc1, err := convertToDocument(computorsList[0])
	require.NoError(t, err)
	assert.Equal(t, doc1, elasticClient.lastDocuments[0])

	doc2, err := convertToDocument(computorsList[1])
	require.NoError(t, err)
	assert.Equal(t, doc2, elasticClient.lastDocuments[1])

}

func TestProcessor_ConsumeBatch_GivenDuplicate_IgnoreDuplicate(t *testing.T) {
	computorsList := []*domain.EpochComputors{
		{Epoch: 1, TickNumber: 100, Identities: []string{"A", "B", "C"}, Signature: "signature-1"},
		{Epoch: 2, TickNumber: 200, Identities: []string{"A", "B", "D"}, Signature: "signature-2"},
	}

	kafkaClient := &FakeKafkaClient{
		computorsList: computorsList,
	}
	elasticClient := &FakeElasticClient{
		duplicate: &elastic.ComputorsList{
			Epoch:      2,
			TickNumber: 200,
			Signature:  "signature-2",
		},
	}
	processor := NewEpochProcessor(kafkaClient, elasticClient, m)

	count, err := processor.consumeBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, 1, elasticClient.bulkIndexCount)
	require.Len(t, elasticClient.lastDocuments, 1)

	doc1, err := convertToDocument(computorsList[0])
	require.NoError(t, err)
	assert.Equal(t, doc1, elasticClient.lastDocuments[0])
}

func TestProcessor_CalculateId(t *testing.T) {
	content, err := os.ReadFile("example-computors-list.json")
	var computors *domain.EpochComputors
	err = json.Unmarshal(content, &computors)
	require.NoError(t, err)

	id, err := calculateUniqueId(computors)
	require.NoError(t, err)
	require.Equal(t, "0311802cd338e16653c563c94934de9673bc42561c9b5ee6d5784293cc68de50", id)
}

func TestProcessor_ConvertToDocument(t *testing.T) {
	content, err := os.ReadFile("example-computors-list.json")
	var computors *domain.EpochComputors
	err = json.Unmarshal(content, &computors)
	require.NoError(t, err)

	marshalled, err := json.Marshal(computors) // using content directly does not work for comparison
	require.NoError(t, err)

	document, err := convertToDocument(computors)
	require.NoError(t, err)
	require.Equal(t, "0311802cd338e16653c563c94934de9673bc42561c9b5ee6d5784293cc68de50", document.Id)
	require.Equal(t, marshalled, document.Payload)
}
