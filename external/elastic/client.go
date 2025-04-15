package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/qubic/go-data-publisher/entities"
	"log"
	"net/http"
	"time"
)

type Client struct {
	username       string
	password       string
	port           string
	index          string
	esClient       *elasticsearch.Client
	publishRetries int
}

type ClientOption func(*Client)

func WithPushRetries(nr int) ClientOption {
	return func(p *Client) {
		p.publishRetries = nr
	}
}

func WithBasicAuth(username, password string) ClientOption {
	return func(p *Client) {
		p.username = username
		p.password = password
	}
}

func NewClient(address, index string, timeout time.Duration, opts ...ClientOption) (*Client, error) {
	client := Client{}
	for _, opt := range opts {
		opt(&client)
	}

	client.index = index
	cfg := elasticsearch.Config{
		Addresses: []string{address},
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: timeout,
		},
	}
	if client.username != "" && client.password != "" {
		cfg.Username = client.username
		cfg.Password = client.password
	}

	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating elasticsearch client: %v", err)
	}

	client.esClient = esClient

	return &client, nil
}

func (es *Client) PublishTransactions(ctx context.Context, txs []entities.Tx) error {
	if len(txs) == 0 {
		return nil
	}

	var lastErr error
	for i := 0; i < es.publishRetries; i++ {
		err := es.publish(ctx, txs)
		if err != nil {
			log.Printf("Error inserting batch. Retrying... %s", err.Error())
			lastErr = err
			continue
		} else {
			lastErr = nil
			break
		}

	}

	if lastErr != nil {
		return fmt.Errorf("inserting tx batch with retry: %v", lastErr)
	}

	return nil
}

func (es *Client) publish(ctx context.Context, txs []entities.Tx) error {
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
