package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

const tickNumberQuery = `{ "query": { "term": { "tickNumber": %d } } }`

type Client struct {
	esClient          *elasticsearch.Client
	transactionsIndex string
	tickDataIndex     string
}

func NewClient(esClient *elasticsearch.Client, transactionsIndex, tickDataIndex string) *Client {
	return &Client{
		esClient:          esClient,
		transactionsIndex: transactionsIndex,
		tickDataIndex:     tickDataIndex,
	}
}

type TickData struct {
	Epoch      uint32 `json:"epoch"`
	TickNumber uint32 `json:"tickNumber"`
	Signature  string `json:"signature"`
}

type elasticHits struct {
	Took int
	Hits struct {
		Total struct {
			Value int
		}
		Hits []struct {
			ID     string          `json:"_id"`
			Source json.RawMessage `json:"_source"`
		}
	}
}

type elasticDocument struct {
	Index  string          `json:"_index"`
	Id     string          `json:"_id"`
	Found  bool            `json:"found"`
	Source json.RawMessage `json:"_source"`
}

func (c *Client) GetTickData(ctx context.Context, tickNumber uint32) (*TickData, error) {

	res, err := c.esClient.Get(
		c.tickDataIndex,
		strconv.Itoa(int(tickNumber)),
		c.esClient.Get.WithContext(ctx),
		c.esClient.Get.WithSource("epoch", "tickNumber", "signature"))
	if err != nil {
		return nil, errors.Wrap(err, "calling elastic")
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("[ERROR] closing body: %v", err)
		}
	}(res.Body)

	// return if there is no tick data (alternative would be to ignore status code and check found property)
	if res.StatusCode == http.StatusNotFound {
		log.Printf("[INFO] Elastic tick [%d]: no tick data.", tickNumber)
		return nil, nil
	}

	if res.IsError() { // there could be some other status error
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return nil, errors.Wrap(err, "decoding error information")
		}
		return nil, errors.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
	}
	if res.HasWarnings() {
		log.Printf("[WARN] elastic returned warnings: %v", res.Warnings())
	}

	var response elasticDocument
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, errors.Wrap(err, "decoding response information")
	}
	tickData := TickData{}
	if err := json.Unmarshal(response.Source, &tickData); err != nil {
		return nil, errors.Wrap(err, "unmarshalling hit.source")
	}

	return &tickData, nil
}

func (c *Client) GetTransactionHashes(ctx context.Context, tickNumber uint32) ([]string, error) {
	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithSize(1024),
		c.esClient.Search.WithSource("false"), // ID (== hash) is enough for us
		c.esClient.Search.WithIndex(c.transactionsIndex),
		c.esClient.Search.WithBody(strings.NewReader(fmt.Sprintf(tickNumberQuery, tickNumber))),
	)
	if err != nil {
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

	var response elasticHits
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, errors.Wrap(err, "decoding response information")
	}
	if response.Took > 100 {
		log.Printf("[INFO] elastic response for tick [%d]: %d hits (%dms)", tickNumber, response.Hits.Total.Value, response.Took)
	}

	if response.Hits.Total.Value == 0 || len(response.Hits.Hits) == 0 {
		log.Printf("[INFO] Elastic tick [%d]: no transactions.", tickNumber)
		return []string{}, nil
	}

	var hashes []string
	for _, hit := range response.Hits.Hits {
		hashes = append(hashes, hit.ID)
	}
	return hashes, nil
}
