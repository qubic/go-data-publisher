package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/pkg/errors"
	"io"
	"log"
	"strings"
)

const queryTickTransactions = `{ "query": { "term": { "tickNumber": %d } } }`

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

type elasticResponse struct {
	Took int
	Hits struct {
		Total struct {
			Value int
		}
		Hits []struct {
			ID string `json:"_id"`
		}
	}
}

func (c *Client) GetTransactionHashes(ctx context.Context, tickNumber uint32) ([]string, error) {
	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithSize(1024),
		c.esClient.Search.WithSource("false"), // ID (== hash) is enough for us
		c.esClient.Search.WithIndex(c.indexName),
		c.esClient.Search.WithBody(strings.NewReader(fmt.Sprintf(queryTickTransactions, tickNumber))),
	)
	if err != nil || res.IsError() {
		return nil, errors.Wrap(err, "calling elastic")
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("[ERROR] closing body: %v", err)
		}
	}(res.Body)

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return nil, errors.Wrap(err, "decoding error information")
		}
		return nil, errors.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
	}
	if res.HasWarnings() {
		log.Printf("[WARN] elastic returned warnings: %v", res.Warnings())
	}

	var response elasticResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, errors.Wrap(err, "decoding response information")
	}
	if response.Took > 100 {
		log.Printf("[INFO] elastic response for tick [%d]: %d hits (%dms)", tickNumber, response.Hits.Total.Value, response.Took)
	}
	//goland:noinspection ALL
	hashes := []string{} // initialize empty because it's possible to have no hits
	for _, hit := range response.Hits.Hits {
		hashes = append(hashes, hit.ID)
	}

	return hashes, nil
}
