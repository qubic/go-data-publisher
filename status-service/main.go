package main

import (
	"fmt"
	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/jellydator/ttlcache/v3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/go-data-publisher/status-service/archiver"
	"github.com/qubic/go-data-publisher/status-service/db"
	"github.com/qubic/go-data-publisher/status-service/elastic"
	"github.com/qubic/go-data-publisher/status-service/metrics"
	"github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/qubic/go-data-publisher/status-service/rpc"
	"github.com/qubic/go-data-publisher/status-service/sync"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
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
			Addresses        []string      `conf:"default:https://localhost:9200"`
			Username         string        `conf:"default:qubic-query"`
			Password         string        `conf:"optional"`
			TransactionIndex string        `conf:"default:qubic-transactions-alias"`
			TickDataIndex    string        `conf:"default:qubic-tick-data-alias"`
			CertificatePath  string        `conf:"default:http_ca.crt"`
			Delay            time.Duration `conf:"default:800ms"`
		}
		Sync struct {
			MetricsNamespace    string `conf:"default:qubic-status-service"`
			InternalStoreFolder string `conf:"default:store"`
			NumMaxWorkers       int    `conf:"optional"`
			SkipTicks           bool   `conf:"default:false"`
			StartTick           uint32 `conf:"optional"`
			Transactions        bool   `conf:"default:true"`
			TickData            bool   `conf:"default:true"`
			VerifyFullTickData  bool   `conf:"default:false"`
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
	defer store.Close()

	// initialize last processed tick, if necessary
	startTick, err := initializeLastProcessedTick(cfg.Sync.StartTick, store)
	if err != nil {
		return errors.Wrap(err, "initializing last processed tick")
	}
	log.Printf("Resuming from tick: [%d].", startTick)

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
		SyncTransactions:   cfg.Sync.Transactions,
		SyncTickData:       cfg.Sync.TickData,
		SkipTicks:          cfg.Sync.SkipTicks,
		NumMaxWorkers:      cfg.Sync.NumMaxWorkers,
		ElasticQueryDelay:  cfg.Elastic.Delay,
		VerifyFullTickData: cfg.Sync.VerifyFullTickData,
	})
	if cfg.Sync.Transactions || cfg.Sync.TickData {
		go processor.Synchronize()
		log.Println("main: starting to process")
	} else {
		log.Println("[WARN] main: sync disabled")
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	var archiverStatusCache = ttlcache.New[string, *protobuf.GetArchiverStatusResponse](
		ttlcache.WithTTL[string, *protobuf.GetArchiverStatusResponse](time.Second),
		ttlcache.WithDisableTouchOnHit[string, *protobuf.GetArchiverStatusResponse](), // don't refresh ttl upon getting the item from cache
	)
	go archiverStatusCache.Start()
	defer archiverStatusCache.Stop()

	var tickIntervalsCache = ttlcache.New[string, *protobuf.GetTickIntervalsResponse](
		ttlcache.WithTTL[string, *protobuf.GetTickIntervalsResponse](time.Second),
		ttlcache.WithDisableTouchOnHit[string, *protobuf.GetTickIntervalsResponse](), // don't refresh ttl upon getting the item from cache
	)
	go tickIntervalsCache.Start()
	defer tickIntervalsCache.Stop()

	statusCache := rpc.NewStatusCache(store, archiverStatusCache, tickIntervalsCache)
	server := rpc.NewStatusServiceServer(cfg.Server.GrpcHost, cfg.Server.HttpHost, statusCache)
	serverError := make(chan error, 1)
	err = server.Start(serverError)
	if err != nil {
		return fmt.Errorf("starting server: %w", err)
	}
	log.Println("main: started web server")

	// metrics endpoint
	metricsServerError := make(chan error, 1)
	go func() {
		log.Printf("main: Starting metrics server on addr [%s].", cfg.Server.MetricsHttpHost)
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

func initializeLastProcessedTick(startTick uint32, store *db.PebbleStore) (uint32, error) {
	lastProcessedTick, err := store.GetLastProcessedTick()
	if startTick > 0 || errors.Is(err, db.ErrNotFound) {
		return startTick, store.SetLastProcessedTick(startTick)
	} else if err != nil {
		return 0, errors.Wrap(err, "getting last processed tick")
	} else {
		return lastProcessedTick, nil
	}
}
