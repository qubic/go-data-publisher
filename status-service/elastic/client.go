package elastic

import (
	"encoding/json"

	"github.com/elastic/go-elasticsearch/v8"
)

const tickNumberQuery = `{ "query": { "term": { "tickNumber": %d } } }`

type Client struct {
	esClient          *elasticsearch.Client
	transactionsIndex string
	tickDataIndex     string
	intervalsIndex    string
}

func NewClient(esClient *elasticsearch.Client, transactionsIndex, tickDataIndex, intervalsIndex string) *Client {
	return &Client{
		esClient:          esClient,
		transactionsIndex: transactionsIndex,
		tickDataIndex:     tickDataIndex,
		intervalsIndex:    intervalsIndex,
	}
}

type elasticHits struct {
	Took int
	Hits struct {
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
