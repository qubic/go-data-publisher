package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ardanlabs/conf"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/jellydator/ttlcache/v3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/go-data-publisher/status-service/archiverv1"
	"github.com/qubic/go-data-publisher/status-service/archiverv2"
	"github.com/qubic/go-data-publisher/status-service/db"
	"github.com/qubic/go-data-publisher/status-service/domain"
	"github.com/qubic/go-data-publisher/status-service/elastic"
	"github.com/qubic/go-data-publisher/status-service/metrics"
	"github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/qubic/go-data-publisher/status-service/redis"
	"github.com/qubic/go-data-publisher/status-service/rpc"
	"github.com/qubic/go-data-publisher/status-service/sync"
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
			Host   string `conf:"default:localhost:8010"`
			Legacy bool   `conf:"default:false"`
		}
		Server struct {
			HttpHost        string `conf:"default:0.0.0.0:8000"`
			GrpcHost        string `conf:"default:0.0.0.0:8001"`
			MetricsHttpHost string `conf:"default:0.0.0.0:9999"`
		}
		Elastic struct {
			Addresses          []string      `conf:"default:https://localhost:9200"`
			Username           string        `conf:"default:qubic-query"`
			Password           string        `conf:"optional,mask"`
			TransactionIndex   string        `conf:"default:qubic-transactions-alias"`
			TickDataIndex      string        `conf:"default:qubic-tick-data-alias"`
			TickIntervalsIndex string        `conf:"default:qubic-tick-intervals-alias"`
			CertificatePath    string        `conf:"default:http_ca.crt"`
			Delay              time.Duration `conf:"default:800ms"`
		}
		EventsElastic struct {
			Addresses       []string      `conf:"default:https://localhost:9200"`
			Username        string        `conf:"default:qubic-query"`
			Password        string        `conf:"optional,mask"`
			EventsIndex     string        `conf:"default:qubic-event-logs-read"`
			CertificatePath string        `conf:"default:http_ca.crt"`
			Delay           time.Duration `conf:"default:800ms"`
		}
		EventsRedis struct {
			MasterName        string   `conf:"default:elastic-redis"`
			SentinelAddresses []string `conf:"default:localhost:26379"`
			SentinelPassword  string   `conf:"optional,mask"`
			Password          string   `conf:"optional,mask"`
			DB                int      `conf:"default:0"`
			KeyName           string   `conf:"default:tick:highest"`
		}
		Sync struct {
			MetricsNamespace       string        `conf:"default:qubic_status_service"`
			InternalStoreFolder    string        `conf:"default:store"`
			NumMaxWorkers          int           `conf:"optional"`
			SkipTicks              bool          `conf:"default:false"`
			StartTick              uint32        `conf:"optional"`
			Transactions           bool          `conf:"default:true"`
			TickData               bool          `conf:"default:true"`
			VerifyFullTickData     bool          `conf:"default:false"`
			IntervalsCacheDuration time.Duration `conf:"default:1m"`
			Events                 bool          `conf:"default:true"`
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
	defer func(store *db.PebbleStore) {
		err := store.Close()
		if err != nil {
			log.Printf("[ERROR] closing db: %v", err)
		}
	}(store)

	// initialize last processed tick, if necessary
	startTick, err := initializeLastProcessedTick(cfg.Sync.StartTick, store)
	if err != nil {
		return errors.Wrap(err, "initializing last processed tick")
	}
	log.Printf("Resuming from tick: [%d].", startTick)

	eventsStartTick, err := initializeEventsLastProcessedTick(store)
	if err != nil {
		return fmt.Errorf("initializing events last processed tick: %w", err)
	}
	log.Printf("Resuming events from tick: [%d].", eventsStartTick)

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
	if err != nil {
		return fmt.Errorf("creating elastic client: %w", err)
	}
	elasticClient := elastic.NewClient(esClient, cfg.Elastic.TransactionIndex, cfg.Elastic.TickDataIndex, cfg.Elastic.TickIntervalsIndex)

	eventsCert, err := os.ReadFile(cfg.EventsElastic.CertificatePath)
	if err != nil {
		log.Printf("[WARN] main: could not read events elastic certificate: %v", err)
	}
	eventsEsClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:     cfg.EventsElastic.Addresses,
		Username:      cfg.EventsElastic.Username,
		Password:      cfg.EventsElastic.Password,
		CACert:        eventsCert,
		RetryOnStatus: []int{502, 503, 504, 429},
	})
	if err != nil {
		return fmt.Errorf("creating events elastic client: %w", err)
	}
	eventsElasticClient := elastic.NewEventsClient(eventsEsClient, cfg.EventsElastic.EventsIndex)
	eventsRedisClient := redis.NewEventsClient(redis.EventsRedisClientCfg{
		MasterName:        cfg.EventsRedis.MasterName,
		SentinelAddresses: cfg.EventsRedis.SentinelAddresses,
		SentinelPassword:  cfg.EventsRedis.SentinelPassword,
		Password:          cfg.EventsRedis.Password,
		Db:                cfg.EventsRedis.DB,
		KeyName:           cfg.EventsRedis.KeyName,
	})
	defer eventsRedisClient.Close()

	var cl sync.ArchiveClient
	if cfg.Archiver.Legacy {
		log.Printf("[WARN] legacy archiver client")
		cl, err = archiverv1.NewClient(cfg.Archiver.Host)
	} else {
		cl, err = archiverv2.NewClient(cfg.Archiver.Host)
	}
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

	eventsProcessor := sync.NewEventsProcessor(eventsElasticClient, eventsRedisClient, store, cfg.EventsElastic.Delay, m)
	if cfg.Sync.Events {
		go eventsProcessor.Synchronize()
		log.Println("main: starting to process events")
	} else {
		log.Println("[WARN] main: events sync disabled")
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	var archiverStatusCache = ttlcache.New[string, *protobuf.GetArchiverStatusResponse](
		ttlcache.WithTTL[string, *protobuf.GetArchiverStatusResponse](time.Second),
		ttlcache.WithDisableTouchOnHit[string, *protobuf.GetArchiverStatusResponse](), // don't refresh ttl upon getting the item from cache
	)
	go archiverStatusCache.Start()
	defer archiverStatusCache.Stop()

	var tickIntervalsCache = ttlcache.New[string, []*domain.TickInterval](
		ttlcache.WithTTL[string, []*domain.TickInterval](cfg.Sync.IntervalsCacheDuration),
		ttlcache.WithDisableTouchOnHit[string, []*domain.TickInterval](), // don't refresh ttl upon getting the item from cache
	)
	go tickIntervalsCache.Start()
	defer tickIntervalsCache.Stop()

	statusCache := rpc.NewStatusService(store, elasticClient, archiverStatusCache, tickIntervalsCache)
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
	}
	return lastProcessedTick, nil
}

func initializeEventsLastProcessedTick(store *db.PebbleStore) (uint32, error) {
	lastProcessedTick, err := store.GetEventsLastProcessedTick()
	if err != nil {
		if !errors.Is(err, db.ErrNotFound) {
			return 0, fmt.Errorf("failed to read events last processed tick: %w", err)
		}
		err := store.SetEventsLastProcessedTick(0)
		if err != nil {
			return 0, fmt.Errorf("setting initial events last processed tick: %w", err)
		}
	}
	return lastProcessedTick, nil
}
