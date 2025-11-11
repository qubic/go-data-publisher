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
	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
)

var (
	elasticClient *Client
)

func TestElasticClient_getLatestComputorsList(t *testing.T) {
	computors, err := elasticClient.FindLatestComputorsListForEpoch(context.Background(), 167)
	require.NoError(t, err)
	log.Println(computors)
	require.NotNil(t, computors)
}

func TestElasticClient_getLatestComputorsList_givenNoData_thenReturnNil(t *testing.T) {
	computors, err := elasticClient.FindLatestComputorsListForEpoch(context.Background(), 42)
	require.NoError(t, err)
	log.Println(computors)
	require.Nil(t, computors)
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
	const envPrefix = "QUBIC_COMPUTORS_CONSUMER"
	err := godotenv.Load("../.env.local")
	if err != nil {
		log.Printf("[WARN] no env file found")
	}
	var cfg struct {
		Elastic struct {
			Addresses   []string `conf:"default:https://localhost:9200"`
			Username    string   `conf:"default:qubic-ingestion"`
			Password    string   `conf:"optional,mask"`
			IndexName   string   `conf:"default:qubic-computors-alias"`
			Certificate string   `conf:"default:http_ca.crt"`
		}
	}
	err = conf.Parse(os.Args[1:], envPrefix, &cfg)
	if err != nil {
		log.Fatalf("error getting config: %v", err)
	}
	if cfg.Elastic.Password == "" {
		log.Printf("WARNING: no password configured")
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: cfg.Elastic.Addresses,
		Username:  cfg.Elastic.Username,
		Password:  cfg.Elastic.Password,
		Logger: &elastictransport.TextLogger{
			Output:             os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		},
		Transport: &http.Transport{
			ResponseHeaderTimeout: time.Second,
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		},
	})
	if err != nil {
		log.Fatalf("error creating elastic client: %v", err)
	}
	elasticClient = NewClient(esClient, cfg.Elastic.IndexName)
}
