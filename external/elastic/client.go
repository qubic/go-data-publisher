package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/qubic/go-data-publisher/entities"
	"net/http"
	"time"
)

type Client struct {
	port     string
	index    string
	esClient *elasticsearch.Client
}

func NewClient(address, index string, timeout time.Duration) (*Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{address},
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: timeout,
		},
	}

	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating elasticsearch client: %v", err)
	}

	return &Client{
		index:    index,
		esClient: esClient,
	}, nil
}

func (es *Client) PublishTransactions(ctx context.Context, txs []entities.Tx) error {
	var buf bytes.Buffer

	for _, tx := range txs {
		// Metadata line for each document
		meta := []byte(fmt.Sprintf(`{ "index": { "_index": "%s", "_id": "%s" } }%s`, es.index, tx.TxID, "\n"))
		buf.Write(meta)

		// Serialize the transaction to JSON
		data, err := json.Marshal(tx)
		if err != nil {
			return fmt.Errorf("error serializing transaction: %w", err)
		}
		buf.Write(data)
		buf.Write([]byte("\n")) // Add a newline between documents
	}

	// Send the bulk request
	res, err := es.esClient.Bulk(bytes.NewReader(buf.Bytes()), es.esClient.Bulk.WithRefresh("true"))
	if err != nil {
		return fmt.Errorf("bulk request failed: %w", err)
	}
	defer res.Body.Close()

	// Check response for errors
	if res.IsError() {
		return fmt.Errorf("bulk request error: %s", res.String())
	}

	return nil
}
