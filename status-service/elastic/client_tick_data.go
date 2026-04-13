package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type TickData struct {
	Epoch             uint32   `json:"epoch"`
	TickNumber        uint32   `json:"tickNumber"`
	ComputorIndex     uint32   `json:"computorIndex"`
	Timestamp         uint64   `json:"timestamp"`
	TimeLock          string   `json:"timeLock"`
	TransactionHashes []string `json:"transactionHashes"`
	ContractFees      []int64  `json:"contractFees"`
	Signature         string   `json:"signature"`
}

func (c *Client) GetTickData(ctx context.Context, tickNumber uint32) (*TickData, error) {
	return queryTickData(c.fullTickDataCall, ctx, tickNumber)
}

func (c *Client) GetMinimalTickData(ctx context.Context, tickNumber uint32) (*TickData, error) {
	return queryTickData(c.minimalTickDataCall, ctx, tickNumber)
}

func (c *Client) minimalTickDataCall(ctx context.Context, tickNumber uint32) (*esapi.Response, error) {
	return c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithIndex(c.tickDataIndex),
		c.esClient.Search.WithBody(strings.NewReader(fmt.Sprintf(tickNumberQuery, tickNumber))),
		c.esClient.Search.WithSource("epoch", "tickNumber", "signature"),
	)
}

func (c *Client) fullTickDataCall(ctx context.Context, tickNumber uint32) (*esapi.Response, error) {
	return c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithIndex(c.tickDataIndex),
		c.esClient.Search.WithBody(strings.NewReader(fmt.Sprintf(tickNumberQuery, tickNumber))),
	)
}

func queryTickData(call func(c context.Context, tn uint32) (*esapi.Response, error), ctx context.Context, tickNumber uint32) (*TickData, error) {
	res, err := call(ctx, tickNumber)
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
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
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

	if len(response.Hits.Hits) == 0 {
		log.Printf("[INFO] Elastic tick [%d]: no tick data.", tickNumber)
		return nil, nil
	}

	tickData := TickData{}
	if err = json.Unmarshal(response.Hits.Hits[0].Source, &tickData); err != nil {
		return nil, fmt.Errorf("unmarshalling hit.source: %w", err)
	}

	return &tickData, nil
}
