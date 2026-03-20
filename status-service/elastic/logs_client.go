package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
)

const logCountQuery = `{ "query": { "term": { "tickNumber": %d } } }`

type LogsClient struct {
	esClient       *elasticsearch.Client
	logEventsIndex string
}

func NewLogsClient(esClient *elasticsearch.Client, logsIndex string) *LogsClient {
	return &LogsClient{
		esClient:       esClient,
		logEventsIndex: logsIndex,
	}
}

func (c *LogsClient) GetLogCountForTick(ctx context.Context, tickNumber uint32) (uint32, error) {

	res, err := c.esClient.Count(
		c.esClient.Count.WithContext(ctx),
		c.esClient.Count.WithIndex(c.logEventsIndex),
		c.esClient.Count.WithBody(strings.NewReader(fmt.Sprintf(logCountQuery, tickNumber))),
	)
	if err != nil {
		return 0, fmt.Errorf("calling elastic: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error closing body: %v", err)
		}
	}(res.Body)

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return 0, fmt.Errorf("decoding error information: %w", err)
		}
		errObj, ok := e["error"].(map[string]interface{})
		if !ok {
			return 0, fmt.Errorf("unexpected error response: %s", res.Status())
		}
		return 0, fmt.Errorf("[%s] %s: %s", res.Status(), errObj["type"], errObj["reason"])
	}
	if res.HasWarnings() {
		log.Printf("[WARN] elastic returned warnings: %v", res.Warnings())
	}

	var response elasticCount
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return 0, fmt.Errorf("decoding response information: %w", err)
	}

	return response.Count, nil
}

type elasticCount struct {
	Count uint32 `json:"count"`
}
