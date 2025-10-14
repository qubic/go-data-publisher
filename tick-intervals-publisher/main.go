package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/ardanlabs/conf"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/tick-intervals-publisher/api"
	"github.com/qubic/tick-intervals-publisher/archiver"
	"github.com/qubic/tick-intervals-publisher/db"
	"github.com/qubic/tick-intervals-publisher/kafka"
	"github.com/qubic/tick-intervals-publisher/metrics"
	"github.com/qubic/tick-intervals-publisher/processing"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

const envPrefix = "QUBIC_TICK_INTERVALS_PUBLISHER"

func main() {
	log.SetOutput(os.Stdout)
	log.Println("Starting tick-intervals-publisher")
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {

	var cfg struct {
		Client struct {
			ArchiverGrpcHost string `conf:"default:localhost:8010"`
		}
		Broker struct {
			BootstrapServers []string `conf:"default:localhost:9092"`
			ProduceTopic     string   `conf:"default:qubic-tick-intervals"`
		}
		Sync struct {
			InternalStoreFolder string   `conf:"default:store"`
			MetricsPort         int      `conf:"default:9999"`
			MetricsNamespace    string   `conf:"default:qubic-kafka"`
			PublishCustomEpochs []uint32 `conf:"optional"`
			StartEpoch          uint32   `conf:"optional"`     // overrides last processed tick
			Enabled             bool     `conf:"default:true"` // only for testing
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
		kgo.DefaultProduceTopic(cfg.Broker.ProduceTopic),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelInfo, nil)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer kcl.Close()

	store, err := db.NewPebbleStore(cfg.Sync.InternalStoreFolder)
	if err != nil {
		return fmt.Errorf("creating pebble store: %w", err)
	}

	_, err = store.GetLastProcessedEpoch()
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			log.Println("[INFO]: Initializing database with last processed epoch 0.")
			err = store.SetLastProcessedEpoch(0)
			if err != nil {
				return fmt.Errorf("initializing last processed epoch: %w", err)
			}
		} else {
			return fmt.Errorf("getting last processed epoch: %w", err)
		}
	}
	if cfg.Sync.StartEpoch > 0 {
		log.Printf("[INFO]: Overriding last processed epoch with [%d]", cfg.Sync.StartEpoch-1)
		err = store.SetLastProcessedEpoch(cfg.Sync.StartEpoch - 1)
		if err != nil {
			return fmt.Errorf("setting last processed epoch: %w", err)
		}
	}

	cl, err := archiver.NewClient(cfg.Client.ArchiverGrpcHost)
	if err != nil {
		return fmt.Errorf("creating archiver client: %w", err)
	}

	producer := kafka.NewTickIntervalProducer(kcl)
	procMetrics := metrics.NewProcessingMetrics(cfg.Sync.MetricsNamespace)
	processor := processing.NewTickIntervalProcessor(store, cl, producer, procMetrics)

	procErr := make(chan error, 1)
	if !cfg.Sync.Enabled {
		log.Println("[WARN] main: producing messages disabled")
	} else if len(cfg.Sync.PublishCustomEpochs) > 0 {
		go func() { procErr <- processor.PublishCustomEpochs(cfg.Sync.PublishCustomEpochs) }()
	} else {
		go processor.StartProcessing()
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// health and metrics endpoint
	apiError := make(chan error, 1)
	go func() {
		log.Printf("main: Starting http server on port [%d].", cfg.Sync.MetricsPort)
		http.HandleFunc("/health", api.Health)
		http.Handle("/metrics", promhttp.Handler())
		apiError <- http.ListenAndServe(fmt.Sprintf(":%d", cfg.Sync.MetricsPort), nil)
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
		case err := <-apiError:
			return fmt.Errorf("[ERROR] starting http server: %v", err)
		}
	}

}
