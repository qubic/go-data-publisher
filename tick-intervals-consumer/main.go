package main

import (
	"context"
	"errors"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/tick-intervals-consumer/consume"
	"github.com/qubic/tick-intervals-consumer/elastic"
	"github.com/qubic/tick-intervals-consumer/kafka"
	"github.com/qubic/tick-intervals-consumer/metrics"
	"github.com/qubic/tick-intervals-consumer/status"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

const envPrefix = "QUBIC_TICK_INTERVALS_CONSUMER"

func main() {
	log.SetOutput(os.Stdout)
	log.Println("Starting tick-intervals-consumer")
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {

	var cfg struct {
		Elastic struct {
			Addresses   []string `conf:"default:https://localhost:9200"`
			Username    string   `conf:"default:qubic-ingestion"`
			Password    string   `conf:"optional"`
			IndexName   string   `conf:"default:qubic-tick-intervals-alias"`
			Certificate string   `conf:"default:http_ca.crt"`
			MaxRetries  int      `conf:"default:25"`
			Stub        bool     `conf:"optional"` // only for testing
		}
		Broker struct {
			BootstrapServers []string `conf:"default:localhost:9092"`
			ConsumeTopic     string   `conf:"default:qubic-tick-intervals"`
			ConsumerGroup    string   `conf:"default:qubic-elastic"`
		}
		Sync struct {
			MetricsPort      int    `conf:"default:9999"`
			MetricsNamespace string `conf:"default:qubic_kafka"`
			Enabled          bool   `conf:"default:true"` // only for testing
		}
	}

	// read config
	if err := conf.Parse(os.Args[1:], envPrefix, &cfg); err != nil {
		switch {
		case errors.Is(err, conf.ErrHelpWanted):
			usage, err := conf.Usage(envPrefix, &cfg)
			if err != nil {
				return fmt.Errorf("generating config usage: %w", err)
			}
			fmt.Println(usage)
			return nil
		}
		return fmt.Errorf("parsing config: %w", err)
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return fmt.Errorf("generating config for output: %w", err)
	}
	log.Printf("main: Config :\n%v\n", out)

	m := kprom.NewMetrics(cfg.Sync.MetricsNamespace,
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

	var elasticClient consume.ElasticClient
	if cfg.Elastic.Stub {
		log.Printf("[WARN] main: stubbing elastic client") // only for testing kafka consumer
		elasticClient = &ElasticStubClient{}
	} else {
		esClient, err := elasticsearch.NewClient(elasticsearch.Config{
			Addresses:     cfg.Elastic.Addresses,
			Username:      cfg.Elastic.Username,
			Password:      cfg.Elastic.Password,
			CACert:        cert,
			RetryOnStatus: []int{502, 503, 504, 429},
			MaxRetries:    cfg.Elastic.MaxRetries,
			RetryBackoff:  calculateBackoff(),
		})
		if err != nil {
			return fmt.Errorf("creating elastic client: %w", err)
		}
		elasticClient = elastic.NewClient(esClient, cfg.Elastic.IndexName)
	}

	consumeMetrics := metrics.NewMetrics(cfg.Sync.MetricsNamespace)
	consumer := kafka.NewClient(kcl, consumeMetrics)
	processor := consume.NewProcessor(consumer, elasticClient)

	procError := make(chan error, 1)
	if cfg.Sync.Enabled {
		go func() {
			procError <- processor.Consume()
		}()
	} else {
		log.Println("[WARN] main: Message consuming disabled") // only for testing startup
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// status and metrics endpoint
	serverError := make(chan error, 1)
	go func() {
		log.Printf("main: Starting health and metrics endpoint on port [%d].", cfg.Sync.MetricsPort)
		http.HandleFunc("/health", status.Health)
		http.Handle("/metrics", promhttp.Handler())
		serverError <- http.ListenAndServe(fmt.Sprintf(":%d", cfg.Sync.MetricsPort), nil)
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

	return nil
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

type ElasticStubClient struct{}

func (e ElasticStubClient) FindOverlappingInterval(_ context.Context, _, _, _ uint32) (*elastic.Interval, error) {
	log.Println("[WARN] main: elastic client stubbed! Returning no data.")
	return nil, nil
}

func (e ElasticStubClient) BulkIndex(_ context.Context, data []*elastic.EsDocument) error {
	log.Printf("[WARN] main: elastic client stubbed! Skipping [%d] documents.", len(data))
	return nil
}
