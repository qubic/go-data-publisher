package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/ardanlabs/conf"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/tick-data-publisher/api"
	"github.com/qubic/tick-data-publisher/archiver"
	"github.com/qubic/tick-data-publisher/db"
	"github.com/qubic/tick-data-publisher/kafka"
	"github.com/qubic/tick-data-publisher/metrics"
	"github.com/qubic/tick-data-publisher/sync"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

const envPrefix = "QUBIC_TICK_DATA_PUBLISHER"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	log.SetOutput(os.Stdout) // default is stderr

	var cfg struct {
		Client struct {
			ArchiverGrpcHost string `conf:"default:localhost:8010"`
		}
		Broker struct {
			BootstrapServers []string `conf:"default:localhost:9092"`
			ProduceTopic     string   `conf:"default:qubic-tick-data"`
		}
		Sync struct {
			InternalStoreFolder string   `conf:"default:store"`
			ServerPort          int      `conf:"default:8000"`
			MetricsPort         int      `conf:"default:9999"`
			MetricsNamespace    string   `conf:"default:qubic_kafka"`
			NumWorkers          int      `conf:"default:16"` // maximum number of workers for parallel processing
			PublishCustomTicks  []uint32 `conf:"optional"`
			StartTick           uint32   `conf:"optional"`     // overrides last processed tick
			Enabled             bool     `conf:"default:true"` // only for testing
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
		return errors.Wrap(err, "creating db")
	}

	lastProcessedTick, err := store.GetLastProcessedTick()
	if cfg.Sync.StartTick > 0 || errors.Is(err, db.ErrNotFound) {
		log.Printf("Setting last processed tick to [%d]", cfg.Sync.StartTick)
		setErr := store.SetLastProcessedTick(cfg.Sync.StartTick)
		if setErr != nil {
			return errors.Wrap(err, "setting last processed tick")
		}
	} else if err != nil {
		return errors.Wrap(err, "getting last processed tick")
	} else {
		log.Printf("Resuming from tick: [%d].", lastProcessedTick)
	}

	cl, err := archiver.NewClient(cfg.Client.ArchiverGrpcHost)
	if err != nil {
		return errors.Wrap(err, "creating archiver client")
	}

	producer := kafka.NewTickDataProducer(kcl)
	procMetrics := metrics.NewProcessingMetrics(cfg.Sync.MetricsNamespace)
	procErr := make(chan error, 1)
	processor := sync.NewTickDataProcessor(store, cl, producer, cfg.Sync.NumWorkers, procMetrics)
	if !cfg.Sync.Enabled {
		log.Println("[WARN] main: Message consuming disabled")
	} else if len(cfg.Sync.PublishCustomTicks) > 0 {
		go func() { procErr <- processor.PublishCustomTicks(cfg.Sync.PublishCustomTicks) }()
	} else {
		go processor.StartProcessing()
	}
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
			return fmt.Errorf("[ERROR] starting server: %v", err)
		case err := <-apiError:
			return fmt.Errorf("[ERROR] starting server: %v", err)
		}
	}
}
