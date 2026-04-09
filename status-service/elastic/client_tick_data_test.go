package elastic

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(r *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func newTestClient(t *testing.T, statusCode int, body string) *Client {
	t.Helper()
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: statusCode,
				Header: http.Header{
					"Content-Type":      []string{"application/json"},
					"X-Elastic-Product": []string{"Elasticsearch"},
				},
				Body: io.NopCloser(strings.NewReader(body)),
			}, nil
		}),
	})
	require.NoError(t, err)
	return NewClient(esClient, "transactions", "tick-data", "intervals")
}

const hitResponse = `{
	"hits": {
		"hits": [{
			"_id": "12345",
			"_source": {
				"epoch": 158,
				"tickNumber": 12345,
				"computorIndex": 7,
				"timestamp": 1700000000,
				"timeLock": "abc123",
				"transactionHashes": ["hash1", "hash2"],
				"signature": "sig123"
			}
		}]
	}
}`

const minimalHitResponse = `{
	"hits": {
		"hits": [{
			"_id": "12345",
			"_source": {
				"epoch": 158,
				"tickNumber": 12345,
				"signature": "sig123"
			}
		}]
	}
}`

const emptyHitsResponse = `{"hits": {"hits": []}}`

const errorResponse = `{
	"error": {
		"type": "index_not_found_exception",
		"reason": "no such index [tick-data]"
	}
}`

func TestGetTickData_returnsFull(t *testing.T) {
	client := newTestClient(t, http.StatusOK, hitResponse)
	result, err := client.GetTickData(context.Background(), 12345)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint32(12345), result.TickNumber)
	assert.Equal(t, uint32(158), result.Epoch)
	assert.Equal(t, uint32(7), result.ComputorIndex)
	assert.Equal(t, uint64(1700000000), result.Timestamp)
	assert.Equal(t, "abc123", result.TimeLock)
	assert.Equal(t, []string{"hash1", "hash2"}, result.TransactionHashes)
	assert.Equal(t, "sig123", result.Signature)
}

func TestGetMinimalTickData_returnsOnlyRequestedFields(t *testing.T) {
	client := newTestClient(t, http.StatusOK, minimalHitResponse)
	result, err := client.GetMinimalTickData(context.Background(), 12345)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint32(12345), result.TickNumber)
	assert.Equal(t, uint32(158), result.Epoch)
	assert.Equal(t, "sig123", result.Signature)
	assert.Zero(t, result.ComputorIndex)
	assert.Zero(t, result.Timestamp)
	assert.Empty(t, result.TimeLock)
	assert.Empty(t, result.TransactionHashes)
}

func TestGetTickData_noHits_returnsNil(t *testing.T) {
	client := newTestClient(t, http.StatusOK, emptyHitsResponse)
	result, err := client.GetTickData(context.Background(), 99999)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestGetMinimalTickData_noHits_returnsNil(t *testing.T) {
	client := newTestClient(t, http.StatusOK, emptyHitsResponse)
	result, err := client.GetMinimalTickData(context.Background(), 99999)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestGetTickData_elasticError_returnsError(t *testing.T) {
	client := newTestClient(t, http.StatusNotFound, errorResponse)
	result, err := client.GetTickData(context.Background(), 12345)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "index_not_found_exception")
}
