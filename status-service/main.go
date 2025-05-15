package main

import (
	"fmt"
	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/status-service/archiver"
	"github.com/qubic/status-service/db"
	"github.com/qubic/status-service/elastic"
	"github.com/qubic/status-service/metrics"
	"github.com/qubic/status-service/rpc"
	"github.com/qubic/status-service/sync"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

const envPrefix = "QUBIC_STATUS_SERVICE"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	log.SetOutput(os.Stdout) // default is stderr

	var cfg struct {
		Archiver struct {
			Host string `conf:"default:localhost:8010"`
		}
		Server struct {
			HttpHost        string `conf:"default:0.0.0.0:8000"`
			GrpcHost        string `conf:"default:0.0.0.0:8001"`
			MetricsHttpHost string `conf:"default:0.0.0.0:9999"`
		}
		Elastic struct {
			Addresses        []string `conf:"default:https://localhost:9200"`
			Username         string   `conf:"default:qubic-query"`
			Password         string   `conf:"optional"`
			TransactionIndex string   `conf:"default:qubic-transactions-alias"`
			TickDataIndex    string   `conf:"default:qubic-tick-data-alias"`
			CertificatePath  string   `conf:"default:http_ca.crt"`
		}
		Sync struct {
			MetricsNamespace    string `conf:"default:qubic-status-service"`
			InternalStoreFolder string `conf:"default:store"`
			NumMaxWorkers       int    `conf:"optional"`
			SkipTicks           bool   `conf:"default:false"`
			StartTick           uint32 `conf:"optional"`
			Transactions        bool   `conf:"default:true"`
			TickData            bool   `conf:"default:true"`
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

	store, err := db.NewPebbleStore(cfg.Sync.InternalStoreFolder)
	if err != nil {
		return errors.Wrap(err, "creating db")
	}

	lastProcessedTick, err := store.GetLastProcessedTick()
	if cfg.Sync.StartTick > 0 || errors.Is(err, db.ErrNotFound) {
		setErr := store.SetLastProcessedTick(cfg.Sync.StartTick)
		if setErr != nil {
			return errors.Wrap(err, "setting last processed tick")
		}
	} else if err != nil {
		return errors.Wrap(err, "getting last processed tick")
	} else {
		log.Printf("Resuming from last processed tick: [%d].", lastProcessedTick)
	}

	cert, err := os.ReadFile(cfg.Elastic.CertificatePath)
	if err != nil {
		log.Printf("[WARN] main: could not read elastic certificate: %v", err)
	}
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:     cfg.Elastic.Addresses,
		Username:      cfg.Elastic.Username,
		Password:      cfg.Elastic.Password,
		CACert:        cert,
		RetryOnStatus: []int{502, 503, 504, 429},
	})
	elasticClient := elastic.NewClient(esClient, cfg.Elastic.TransactionIndex, cfg.Elastic.TickDataIndex)

	cl, err := archiver.NewClient(cfg.Archiver.Host)
	if err != nil {
		return errors.Wrap(err, "creating archiver client")
	}

	m := metrics.NewMetrics(cfg.Sync.MetricsNamespace)
	processor := sync.NewTickProcessor(cl, elasticClient, store, m, sync.Config{
		SyncTransactions: cfg.Sync.Transactions,
		SyncTickData:     cfg.Sync.TickData,
		SkipTicks:        cfg.Sync.SkipTicks,
		NumMaxWorkers:    cfg.Sync.NumMaxWorkers,
	})
	if cfg.Sync.Transactions || cfg.Sync.TickData {
		go processor.Synchronize()
	} else {
		log.Println("[WARN] main: sync disabled")
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// status and metrics endpoint
	serverError := make(chan error, 1)
	server := rpc.NewStatusServiceServer(cfg.Server.GrpcHost, cfg.Server.HttpHost, store)
	err = server.Start(serverError)
	if err != nil {
		return fmt.Errorf("starting server: %w", err)
	}

	metricsServerError := make(chan error, 1)
	go func() {
		log.Printf("main: Starting metrics server on addr [%d].", cfg.Server.MetricsHttpHost)
		http.Handle("/metrics", promhttp.Handler())
		metricsServerError <- http.ListenAndServe(cfg.Server.MetricsHttpHost, nil)
	}()

	log.Println("main: Service started.")

	for {
		select {
		case <-shutdown:
			log.Println("main: Received shutdown signal, shutting down...")
			return nil
		case err := <-metricsServerError:
			return errors.Wrapf(err, "[ERROR] starting metrics endpoint.")
		case err := <-serverError:
			return errors.Wrapf(err, "[ERROR] starting server endpoint(s).")
		}
	}
}
