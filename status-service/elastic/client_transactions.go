package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
)

func (c *Client) GetTransactionHashes(ctx context.Context, tickNumber uint32) ([]string, error) {
	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithSize(1024),
		c.esClient.Search.WithSource("false"), // ID (== hash) is enough for us
		c.esClient.Search.WithIndex(c.transactionsIndex),
		c.esClient.Search.WithBody(strings.NewReader(fmt.Sprintf(tickNumberQuery, tickNumber))),
	)
	if err != nil {
		return nil, fmt.Errorf("calling elastic: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			log.Printf("Error closing body: %v", err)
		}
	}(res.Body)

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return nil, fmt.Errorf("decoding error information: %w", err)
		}
		return nil, fmt.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
	}
	if res.HasWarnings() {
		log.Printf("[WARN] elastic returned warnings: %v", res.Warnings())
	}

	var response elasticHits
	if err = json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response information: %w", err)
	}
	if response.Took > 100 {
		log.Printf("[WARN] slow elastic response for tick [%d]: %d hits (%dms)", tickNumber, len(response.Hits.Hits), response.Took)
	}

	if len(response.Hits.Hits) == 0 {
		log.Printf("[INFO] Elastic tick [%d]: no transactions.", tickNumber)
		return []string{}, nil
	}

	var hashes []string
	for _, hit := range response.Hits.Hits {
		hashes = append(hashes, hit.ID)
	}
	return hashes, nil
}
