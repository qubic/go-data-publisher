package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"log"
	"runtime"
	"strings"
	"time"
)

type EsDocument struct {
	Id      string
	Payload []byte
}

type Client struct {
	esClient  *elasticsearch.Client
	indexName string
}

func NewClient(esClient *elasticsearch.Client, indexName string) *Client {
	return &Client{
		esClient:  esClient,
		indexName: indexName,
	}
}

type searchResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []computorsHit `json:"hits"`
	} `json:"hits"`
}

type computorsHit struct {
	Source ComputorsList `json:"_source"`
}

type ComputorsList struct {
	Epoch      uint32 `json:"epoch"`
	TickNumber uint32 `json:"tickNumber"`
	Signature  string `json:"signature"`
}

func (c *Client) FindLatestComputorsListForEpoch(ctx context.Context, epoch uint32) (*ComputorsList, error) {

	query, err := createFindLatestComputorsListInEpoch(epoch)
	if err != nil {
		return nil, fmt.Errorf("creating query: %w", err)
	}

	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithSource("epoch", "tickNumber", "signature"),
		c.esClient.Search.WithIndex(c.indexName),
		c.esClient.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		return nil, fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("got error response from elastic: %s", res.String())
	}
	var result searchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	if result.Hits.Total.Value > 0 {
		return &result.Hits.Hits[0].Source, nil
	} else {
		return nil, nil
	}
}

func createFindLatestComputorsListInEpoch(epoch uint32) (string, error) {
	query := `{ "size": 1, "query": { "term": { "epoch": %d } }, "sort": [ { "tickNumber": {  "order": "desc" } } ] }`
	query = fmt.Sprintf(query, epoch)
	return query, nil
}

func (c *Client) BulkIndex(ctx context.Context, data []*EsDocument) error {
	start := time.Now().UnixMilli()
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      c.indexName,              // The default index name
		Client:     c.esClient,               // The Elasticsearch client
		NumWorkers: min(runtime.NumCPU(), 8), // 8 parallel connections are enough
	})
	if err != nil {
		return fmt.Errorf("creating bulk indexer: %w", err)
	}

	for _, d := range data {
		item := esutil.BulkIndexerItem{
			Action:       "index",
			DocumentID:   d.Id,
			RequireAlias: true,
			Body:         bytes.NewReader(d.Payload),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				msg := "Error indexing document"
				if err != nil {
					log.Printf("%s [%s]: %s: [%s]", msg, d.Id, string(d.Payload), err)
				} else {
					log.Printf("%s [%s]: %s: [%s: %s]", msg, d.Id, string(d.Payload), res.Error.Type, res.Error.Reason)
				}
			},
		}
		err = bi.Add(ctx, item)
	}

	err = bi.Close(ctx)
	if err != nil {
		return fmt.Errorf("closing bulk indexer: %w", err)
	}

	biStats := bi.Stats()
	end := time.Now().UnixMilli()
	if biStats.NumFailed > 0 {
		return fmt.Errorf("%d errors indexing [%d] documents",
			biStats.NumFailed,
			biStats.NumFlushed,
		)
	} else {
		log.Printf("Indexed %d documents (%d bytes, %d requests) in %dms.",
			biStats.NumFlushed,
			biStats.FlushedBytes,
			biStats.NumRequests,
			end-start,
		)
	}
	return nil
}
