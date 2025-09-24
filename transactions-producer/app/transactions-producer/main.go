package main

import (
	"encoding/json"
	"fmt"
	"github.com/ardanlabs/conf"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/transactions-producer/domain"
	"github.com/qubic/transactions-producer/entities"
	"github.com/qubic/transactions-producer/external/archiver"
	"github.com/qubic/transactions-producer/external/kafka"
	"github.com/qubic/transactions-producer/infrastructure/store/pebbledb"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const prefix = "QUBIC_TRANSACTIONS_PUBLISHER"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	config := zap.NewProductionConfig()
	// this is just for sugar, to display a readable date instead of an epoch time
	config.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.DateTime)

	logger, err := config.Build()
	if err != nil {
		return fmt.Errorf("creating logger: %v", err)
	}
	defer logger.Sync()
	sLogger := logger.Sugar()

	var cfg struct {
		InternalStoreFolder            string        `conf:"default:store"`
		ArchiverGrpcHost               string        `conf:"default:127.0.0.1:6001"`
		ArchiverReadTimeout            time.Duration `conf:"default:30s"`
		NrWorkers                      int           `conf:"default:20"`
		PublishCustomTicks             []uint32      `conf:"optional"`
		OverrideLastProcessedTick      bool          `conf:"default:false"`
		OverrideLastProcessedTickValue uint32        `conf:"default:0"`
		Kafka                          struct {
			BootstrapServers []string `conf:"default:localhost:9092"`
			TxTopic          string   `conf:"default:qubic-transactions-local"`
		}
		MetricsNamespace string `conf:"default:qubic-kafka"`
		MetricsPort      int    `conf:"default:9999"`
		Sync             struct {
			RangeStart   uint32 `conf:"optional"`
			RangeEnd     uint32 `conf:"optional"`
			RangeEpoch   uint32 `conf:"optional"`
			VoidTxStatus bool   `conf:"false"`
		}
	}

	if err := conf.Parse(os.Args[1:], prefix, &cfg); err != nil {
		switch err {
		case conf.ErrHelpWanted:
			usage, err := conf.Usage(prefix, &cfg)
			if err != nil {
				return fmt.Errorf("generating config usage: %v", err)
			}
			fmt.Println(usage)
			return nil
		case conf.ErrVersionWanted:
			version, err := conf.VersionString(prefix, &cfg)
			if err != nil {
				return fmt.Errorf("generating config version: %v", err)
			}
			fmt.Println(version)
			return nil
		}
		return fmt.Errorf("parsing config: %v", err)
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return fmt.Errorf("generating config for output: %v", err)
	}
	log.Printf("main: Config :\n%v\n", out)

	procStore, err := pebbledb.NewProcessorStore(cfg.InternalStoreFolder)
	if err != nil {
		return fmt.Errorf("creating processor store: %v", err)
	}

	lpt, err := procStore.GetLastProcessedTick()
	if cfg.OverrideLastProcessedTick {
		log.Printf("main: overriding last processed tick with [%d].", cfg.OverrideLastProcessedTickValue)
		if err := procStore.SetLastProcessedTick(cfg.OverrideLastProcessedTickValue); err != nil {
			return fmt.Errorf("setting last processed tick: %v", err)
		}
	} else if err == entities.ErrStoreEntityNotFound {
		log.Println("main: initializing last processed tick.")
		err = procStore.SetLastProcessedTick(0)
		if err != nil {
			return fmt.Errorf("setting last processed tick: %v", err)
		}
	} else {
		log.Printf("main: resuming from  last processed tick [%d].", lpt)
	}

	kafkaMetrics := kprom.NewMetrics(cfg.MetricsNamespace,
		kprom.Registerer(prometheus.DefaultRegisterer),
		kprom.Gatherer(prometheus.DefaultGatherer))
	kcl, err := kgo.NewClient(
		kgo.WithHooks(kafkaMetrics),
		// The default should eventually be removed after implementing publishing for multiple types of data.
		kgo.DefaultProduceTopic(cfg.Kafka.TxTopic),
		kgo.SeedBrokers(cfg.Kafka.BootstrapServers...),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelInfo, nil)),
	)
	if err != nil {
		return errors.Wrap(err, "creating kafka client")
	}

	kafkaClient := kafka.NewClient(kcl)

	archiverClient, err := archiver.NewClient(cfg.ArchiverGrpcHost, cfg.Sync.VoidTxStatus)
	if err != nil {
		return fmt.Errorf("creating archiver client: %v", err)
	}

	metrics := domain.NewMetrics(cfg.MetricsNamespace)
	proc := domain.NewProcessor(archiverClient, cfg.ArchiverReadTimeout, kafkaClient, procStore, cfg.NrWorkers, sLogger, metrics)
	if err != nil {
		return fmt.Errorf("creating processor: %v", err)
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	procErrors := make(chan error, 1)
	if cfg.Sync.RangeStart > 0 && cfg.Sync.RangeEnd > 0 && cfg.Sync.RangeEpoch > 0 {
		log.Printf("main: Processing range from [%d] to [%d] in epoch [%d].", cfg.Sync.RangeStart, cfg.Sync.RangeEnd, cfg.Sync.RangeEpoch)
		go func() {
			procErrors <- proc.ProcessTickRange(cfg.Sync.RangeEpoch, cfg.Sync.RangeStart, cfg.Sync.RangeEnd)
		}()

	} else if len(cfg.PublishCustomTicks) > 0 {
		log.Printf("main: publishing custom ticks: %v", cfg.PublishCustomTicks)
		go func() {
			procErrors <- proc.PublishSingleTicks(cfg.PublishCustomTicks)
		}()
	} else {
		go func() {
			procErrors <- proc.Start()
		}()
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("main: starting status and metrics endpoint on port [%d]", cfg.MetricsPort)
		http.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
			lastProcessedTick, err := procStore.GetLastProcessedTick()
			if err != nil {
				http.Error(w, fmt.Sprintf("getting last processed tick: %v", err), http.StatusInternalServerError)
				return
			}
			response := map[string]uint32{
				"lastProcessedTick": lastProcessedTick,
			}
			data, err := json.Marshal(response)
			if err != nil {
				http.Error(w, fmt.Sprintf("marshalling response: %v", err), http.StatusInternalServerError)
			}
			w.Header().Set("Content-Type", "application/json")
			_, err = w.Write(data)
			if err != nil {
				http.Error(w, fmt.Sprintf("writing response: %v", err), http.StatusInternalServerError)
				return
			}
		})
		http.Handle("/metrics", promhttp.Handler())
		serverErr <- http.ListenAndServe(fmt.Sprintf(":%d", cfg.MetricsPort), nil)
	}()

	for {
		select {
		case <-shutdown:
			return errors.New("shutting down")
		case err := <-procErrors:
			if err != nil {
				return fmt.Errorf("processing error: %v", err)
			} else {
				log.Print("main: processing finished without error")
				return nil
			}
		case err := <-serverErr:
			return fmt.Errorf("server error: %v", err)
		}
	}
}
