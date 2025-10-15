//go:build !ci
// +build !ci

package elastic

import (
	"context"
	"crypto/tls"
	"flag"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/joho/godotenv"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	elasticClient *Client
)

func TestElasticClient_getTickIntervals(t *testing.T) {
	intervals, err := elasticClient.GetTickIntervals(context.Background(), 180)
	require.NoError(t, err)
	require.Greater(t, len(intervals), 50)
	require.Less(t, int(intervals[len(intervals)-1].Epoch), 180) // 180 exclusive
	require.Greater(t, intervals[1].From, intervals[0].From)     // sorted
	require.Greater(t, intervals[10].Epoch, intervals[0].Epoch)  // sorted
}

func TestElasticClient_GetTickIntervals_GiveBeforeSecondStoredEpoch_ThenReturnFirst(t *testing.T) {
	intervals, err := elasticClient.GetTickIntervals(context.Background(), 105)
	require.NoError(t, err)
	require.Len(t, intervals, 1)
	require.Equal(t, &domain.TickInterval{
		Epoch: 104,
		From:  13360000,
		To:    13461047,
	}, intervals[0])
}

func TestElasticClient_getTransactionHashes(t *testing.T) {
	hashes, err := elasticClient.GetTransactionHashes(context.Background(), 24889941)
	assert.NoError(t, err)
	log.Println(hashes)
	assert.Len(t, hashes, 10)
}

func TestElasticClient_getTransactionHashes_givenEmptyTick(t *testing.T) {
	hashes, err := elasticClient.GetTransactionHashes(context.Background(), 24800000)
	assert.NoError(t, err)
	log.Println(hashes)
	assert.NotNilf(t, hashes, "expected hashes to not be nil")
	assert.Len(t, hashes, 0)
}

func TestElasticClient_getTransactionHashes_givenTickWithoutTransactions(t *testing.T) {
	hashes, err := elasticClient.GetTransactionHashes(context.Background(), 24800003)
	assert.NoError(t, err)
	log.Println(hashes)
	assert.NotNilf(t, hashes, "expected hashes to not be nil")
	assert.Len(t, hashes, 0)
}

func TestElasticClient_getTickData(t *testing.T) {
	tickData, err := elasticClient.GetTickData(nil, 24333026)
	require.NoError(t, err)
	require.NotNil(t, tickData)
	log.Printf("Tick data: %+v", tickData)
	assert.Equal(t, 24333026, int(tickData.TickNumber))
	assert.Equal(t, 158, int(tickData.Epoch))
	assert.NotZero(t, tickData.Timestamp)
	assert.NotEmpty(t, tickData.TransactionHashes)
	assert.NotZero(t, tickData.ComputorIndex)
	assert.NotEmpty(t, tickData.TimeLock)
	assert.Empty(t, tickData.ContractFees) // nil if not present
	assert.NotEmpty(t, tickData.Signature)
}

func TestElasticClient_getMinimalTickData(t *testing.T) {
	tickData, err := elasticClient.GetMinimalTickData(nil, 24333026)
	require.NoError(t, err)
	require.NotNil(t, tickData)
	log.Printf("Tick data: %+v", tickData)
	assert.Equal(t, 24333026, int(tickData.TickNumber))
	assert.Equal(t, 158, int(tickData.Epoch))
	assert.NotEmpty(t, tickData.Signature)
	assert.Zero(t, tickData.Timestamp)
	assert.Zero(t, tickData.ComputorIndex)
	assert.Empty(t, tickData.TransactionHashes)
	assert.Empty(t, tickData.TimeLock)
	assert.Empty(t, tickData.ContractFees)
}

func TestElasticClient_getTickData_givenUnknownTickNumber_thenReturnNil(t *testing.T) {
	tickData, err := elasticClient.GetTickData(nil, 1234567890)
	assert.NoError(t, err)
	assert.Nil(t, tickData)
}

func TestMain(m *testing.M) {
	setup()
	// Parse args and run
	flag.Parse()
	exitCode := m.Run()
	// Exit
	os.Exit(exitCode)
}

func setup() {
	const envPrefix = "QUBIC_STATUS_SERVICE"
	err := godotenv.Load("../.env.local")
	if err != nil {
		log.Printf("[WARN] no env file found")
	}
	var cfg struct {
		Elastic struct {
			Addresses          []string `conf:"default:https://localhost:9200"`
			Username           string   `conf:"default:qubic-query"`
			Password           string   `conf:"optional"`
			TransactionsIndex  string   `conf:"default:qubic-transactions-alias"`
			TickDataIndex      string   `conf:"default:qubic-tick-data-alias"`
			TIckIntervalsIndex string   `conf:"default:qubic-tick-intervals-alias"`
			Certificate        string   `conf:"default:../certs/elastic-dev/http_ca.crt"`
		}
	}
	err = conf.Parse(os.Args[1:], envPrefix, &cfg)
	if err != nil {
		log.Fatalf("error getting config: %v", err)
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: cfg.Elastic.Addresses,
		Username:  cfg.Elastic.Username,
		Password:  cfg.Elastic.Password,
		Transport: &http.Transport{
			ResponseHeaderTimeout: time.Second,
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		},
	})
	if err != nil {
		log.Fatalf("error creating elastic client: %v", err)
	}
	elasticClient = NewClient(esClient, cfg.Elastic.TransactionsIndex, cfg.Elastic.TickDataIndex, cfg.Elastic.TIckIntervalsIndex)
}
