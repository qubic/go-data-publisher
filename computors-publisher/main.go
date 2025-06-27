package main

import (
	"fmt"
	"github.com/ardanlabs/conf/v3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/computors-publisher/api"
	"github.com/qubic/computors-publisher/archiver"
	"github.com/qubic/computors-publisher/db"
	"github.com/qubic/computors-publisher/kafka"
	"github.com/qubic/computors-publisher/sync"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

const envPrefix = "QUBIC_COMPUTORS_PUBLISHER"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	log.SetOutput(os.Stdout)

	var cfg struct {
		Client struct {
			ArchiverGrpcHost string `conf:"default:localhost:8010"`
		}
		Broker struct {
			BootstrapServers []string `conf:"default:localhost:9092"`
			ProduceTopic     string   `conf:"default:qubic-computors"`
		}
		Sync struct {
			InternalStoreFolder string `conf:"default:store"`
			ServerPort          int    `conf:"default:8000"`
			MetricsPort         int    `conf:"default:9999"`
			MetricsNamespace    string `conf:"default:qubic-kafka"`
		}
	}

	help, err := conf.Parse(envPrefix, &cfg)
	if err != nil {
		if errors.Is(err, conf.ErrHelpWanted) {
			fmt.Println(help)
			return nil
		}
		return fmt.Errorf("parsing config: %w", err)
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return errors.Wrap(err, "generating config for output")
	}
	log.Printf("main: Config :\n%v\n", out)

	m := kprom.NewMetrics(cfg.Sync.MetricsNamespace,
		kprom.Registerer(prometheus.DefaultRegisterer),
		kprom.Gatherer(prometheus.DefaultGatherer))
	kcl, err := kgo.NewClient(
		kgo.WithHooks(m),
		kgo.SeedBrokers(cfg.Broker.BootstrapServers...),
		kgo.DefaultProduceTopic(cfg.Broker.ProduceTopic),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer kcl.Close()

	store, err := db.NewPebbleStore(cfg.Sync.InternalStoreFolder)
	if err != nil {
		return errors.Wrap(err, "creating store")
	}

	_, err = store.GetLastProcessedEpoch()
	if err != nil {
		if !errors.Is(err, db.ErrNotFound) {
			return errors.Wrap(err, "getting last processed epoch")
		}

		log.Println("[INFO]: Initializing database with last processed epoch as 0.")
		err := store.SetLastProcessedEpoch(0)
		if err != nil {
			return errors.Wrap(err, "initializing database value")
		}

	}
	archiverClient, err := archiver.NewClient(cfg.Client.ArchiverGrpcHost)
	if err != nil {
		return errors.Wrap(err, "creating archiver client")
	}
	kafkaProducer := kafka.NewEpochComputorsProducer(kcl)

	procErr := make(chan error, 1)
	processor := sync.NewEpochComputorsProcessor(archiverClient, store, kafkaProducer)
	go func() {
		procErr <- processor.StartProcessing()
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// status and metrics endpoint
	apiError := make(chan error, 1)
	go func() {
		mux := http.NewServeMux()
		server := api.NewHandler()
		mux.HandleFunc("/health", server.GetHealth)
		log.Printf("main: Starting server on port [%d].", cfg.Sync.ServerPort)
		apiError <- http.ListenAndServe(fmt.Sprintf(":%d", cfg.Sync.ServerPort), mux)
	}()

	metricsError := make(chan error, 1)
	go func() {
		log.Printf("main: Starting metrics server on port [%d].", cfg.Sync.MetricsPort)
		http.Handle("/metrics", promhttp.Handler())
		metricsError <- http.ListenAndServe(fmt.Sprintf(":%d", cfg.Sync.MetricsPort), nil)
	}()

	log.Println("main: Service started.")

	for {
		select {
		case <-shutdown:
			log.Println("main: Received shutdown signal, shutting down...")
			return nil
		case err := <-procErr:
			if err != nil {
				return fmt.Errorf("[ERROR] processing: %v", err)
			} else {
				log.Printf("main: Finished proessing.")
				return nil
			}
		case err := <-metricsError:
			return fmt.Errorf("[ERROR] starting metrics server: %v", err)
		case err := <-apiError:
			return fmt.Errorf("[ERROR] starting api server: %v", err)
		}
	}
	return nil
}
