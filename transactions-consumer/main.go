package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/transactions-consumer/consume"
	"github.com/qubic/transactions-consumer/extern"
	"github.com/qubic/transactions-consumer/metrics"
	"github.com/qubic/transactions-consumer/status"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

const envPrefix = "QUBIC_TRANSACTIONS_CONSUMER"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	log.SetOutput(os.Stdout) // default is stderr

	var cfg struct {
		Elastic struct {
			Addresses   []string `conf:"default:https://localhost:9200"`
			Username    string   `conf:"default:qubic-ingestion"`
			Password    string   `conf:"optional,mask"`
			IndexName   string   `conf:"default:qubic-transactions-alias"`
			Certificate string   `conf:"default:http_ca.crt"`
			MaxRetries  int      `conf:"default:15"`
			Stub        bool     `conf:"optional"` // only for testing
		}
		Broker struct {
			BootstrapServers []string `conf:"default:localhost:9092"`
			MetricsPort      int      `conf:"default:9999"`
			MetricsNamespace string   `conf:"default:qubic_kafka"`
			ConsumeTopic     string   `conf:"default:qubic-transactions"`
			ConsumerGroup    string   `conf:"default:qubic-elastic"`
		}
		Sync struct {
			Enabled bool `conf:"default:true"` // only for testing
		}
	}

	// load config
	if err := conf.Parse(os.Args[1:], envPrefix, &cfg); err != nil {
		switch {
		case errors.Is(err, conf.ErrHelpWanted):
			usage, err := conf.Usage(envPrefix, &cfg)
			if err != nil {
				return errors.Wrap(err, "generating config usage")
			}
			fmt.Println(usage)
			return nil
		case errors.Is(err, conf.ErrVersionWanted):
			version, err := conf.VersionString(envPrefix, &cfg)
			if err != nil {
				return errors.Wrap(err, "generating config version")
			}
			fmt.Println(version)
			return nil
		}
		return errors.Wrap(err, "parsing config")
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return errors.Wrap(err, "generating config for output")
	}
	log.Printf("main: Config :\n%v\n", out)

	m := kprom.NewMetrics(cfg.Broker.MetricsNamespace,
		kprom.Registerer(prometheus.DefaultRegisterer),
		kprom.Gatherer(prometheus.DefaultGatherer))
	kcl, err := kgo.NewClient(
		kgo.WithHooks(m),
		kgo.SeedBrokers(cfg.Broker.BootstrapServers...),
		kgo.ConsumeTopics(cfg.Broker.ConsumeTopic),
		kgo.ConsumerGroup(cfg.Broker.ConsumerGroup),
		kgo.BlockRebalanceOnPoll(),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelInfo, nil)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer kcl.Close()

	cert, err := os.ReadFile(cfg.Elastic.Certificate)
	if err != nil {
		log.Printf("[WARN] main: could not read elastic certificate: %v", err)
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:     cfg.Elastic.Addresses,
		Username:      cfg.Elastic.Username,
		Password:      cfg.Elastic.Password,
		CACert:        cert,
		RetryOnStatus: []int{502, 503, 504, 429},
		MaxRetries:    cfg.Elastic.MaxRetries,
		RetryBackoff:  calculateBackoff(),
	})
	var elasticClient consume.ElasticDocumentClient
	if cfg.Elastic.Stub {
		log.Printf("[WARN] main: Using stub ES client.")
		elasticClient = &ElasticClientStub{}
	} else {
		elasticClient = extern.NewElasticClient(esClient, cfg.Elastic.IndexName)
	}
	processingMetrics := metrics.NewMetrics(cfg.Broker.MetricsNamespace)
	consumer := consume.NewTransactionConsumer(kcl, elasticClient, processingMetrics)
	procError := make(chan error, 1)
	if cfg.Sync.Enabled {
		go func() {
			procError <- consumer.Consume()
		}()
	} else {
		log.Println("[WARN] main: Message consuming disabled")
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// status and metrics endpoint
	serverError := make(chan error, 1)
	go func() {
		log.Printf("main: Starting health and metrics endpoint on port [%d].", cfg.Broker.MetricsPort)
		http.HandleFunc("/health", status.Health)
		http.Handle("/metrics", promhttp.Handler())
		serverError <- http.ListenAndServe(fmt.Sprintf(":%d", cfg.Broker.MetricsPort), nil)
	}()

	log.Println("main: Service started.")

	for {
		select {
		case <-shutdown:
			log.Println("main: Received shutdown signal, shutting down...")
			return nil
		case err := <-procError:
			return fmt.Errorf("[ERROR] processing error: %v", err)
		case err := <-serverError:
			return fmt.Errorf("[ERROR] starting server: %v", err)
		}
	}
}

// calculateBackoff needs retry number because of multi threading
func calculateBackoff() func(i int) time.Duration {
	return func(i int) time.Duration {
		var d time.Duration
		if i < 10 {
			d = time.Second*time.Duration(i) + randomMillis()
		} else {
			d = time.Second*30 + randomMillis()
		}
		log.Printf("[WARN] elasticsearch client retry [%d] in %v.", i, d)
		return d
	}
}

func randomMillis() time.Duration {
	return time.Duration(rand.Intn(1000)) * time.Millisecond
}

type ElasticClientStub struct {
}

func (c *ElasticClientStub) BulkIndex(_ context.Context, _ []extern.EsDocument) error {
	return nil
}
