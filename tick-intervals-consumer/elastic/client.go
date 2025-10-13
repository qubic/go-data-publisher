package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/pkg/errors"
)

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

type EsDocument struct {
	Id      string
	Payload []byte
}

type searchResponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []IntervalHit `json:"hits"`
	} `json:"hits"`
}

type IntervalHit struct {
	Source Interval `json:"_source"`
}

type Interval struct {
	Epoch uint32 `json:"epoch"`
	From  uint32 `json:"from"`
	To    uint32 `json:"to"`
}

func (c *Client) FindOverlappingInterval(ctx context.Context, epoch, from, to uint32) (*Interval, error) {
	// query for intervals the start tick falls into (expect 0 or 1 results)
	// (theoretically we could also have intervals that overlap and start before the start tick but that
	// should not happen and would be a severe data inconsistency)
	query := fmt.Sprintf(
		`{ "query": { "bool": { "must": [ { "term": { "epoch": %d } }, { "range": { "from": { "gte": %d, "lte": %d } } } ] } } }`, epoch, from, to)

	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithIndex(c.indexName),
		c.esClient.Search.WithBody(strings.NewReader(query)),
		c.esClient.Search.WithTrackTotalHits(2),
	)
	if err != nil {
		return nil, fmt.Errorf("performing search: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %v", err)
		}
	}(res.Body)

	if res.IsError() {
		return nil, fmt.Errorf("got error response from elastic: %s", res.String())
	}
	var result searchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	if result.Hits.Total.Value == 1 {
		return &result.Hits.Hits[0].Source, nil
	} else if result.Hits.Total.Value > 1 {
		// if there are more than one interval for the same start tick we have a data inconsistency
		return nil, fmt.Errorf("illegal state: found [%d] intervals for start tick [%d] and epoch [%d]",
			result.Hits.Total.Value, from, epoch)
	} else {
		return nil, nil
	}
}

func (c *Client) BulkIndex(ctx context.Context, data []*EsDocument) error {
	start := time.Now().UnixMilli()
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      c.indexName,              // The default index name
		Client:     c.esClient,               // The Elasticsearch client
		NumWorkers: min(runtime.NumCPU(), 8), // 8 parallel connections are enough
	})
	if err != nil {
		return errors.Wrap(err, "Error creating bulk indexer")
	}

	for _, d := range data {
		item := esutil.BulkIndexerItem{
			Action:       "index", // creates or replaces
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
		return errors.Wrap(err, "Error closing bulk indexer")
	}

	biStats := bi.Stats()
	end := time.Now().UnixMilli()
	if biStats.NumFailed > 0 {
		return errors.Errorf("%d errors indexing [%d] documents",
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
