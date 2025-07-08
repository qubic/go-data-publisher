package elastic

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"log"
	"runtime"
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
