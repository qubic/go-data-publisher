package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/qubic/go-data-publisher/status-service/domain"
)

func (c *Client) GetTickIntervals(ctx context.Context, beforeEpoch uint32) ([]*domain.TickInterval, error) {

	query := `{ "query" : { "bool": { "must" : [ { "range": { "epoch": { "lt": %d } } } ] } },
                "sort": [ { "epoch": {  "order": "asc" } }, { "to": {  "order": "asc" } } ] }`

	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithSize(1000),
		c.esClient.Search.WithTrackTotalHits(false),
		c.esClient.Search.WithIndex(c.intervalsIndex),
		c.esClient.Search.WithBody(strings.NewReader(fmt.Sprintf(query, beforeEpoch))),
		c.esClient.Search.WithPretty(),
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
		log.Printf("[WARN] slow elastic response for tick intervals: %d hits (%dms)", len(response.Hits.Hits), response.Took)
	}

	if len(response.Hits.Hits) == 0 {
		log.Printf("[ERROR] Elastic: no tick intervals found.")
		return nil, fmt.Errorf("no tick intervals found")
	}

	var intervals []*domain.TickInterval
	for _, hit := range response.Hits.Hits {
		interval := domain.TickInterval{}
		err = json.Unmarshal(hit.Source, &interval)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling tick interval to json: %w", err)
		}
		intervals = append(intervals, &interval)
	}
	return intervals, nil

}
