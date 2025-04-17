package main

import (
	"encoding/json"
	"fmt"
	"github.com/ardanlabs/conf"
	"github.com/pkg/errors"
	"github.com/qubic/go-data-publisher/business/domain/tx"
	"github.com/qubic/go-data-publisher/external/archiver"
	"github.com/qubic/go-data-publisher/external/kafka"
	"github.com/qubic/go-data-publisher/infrastructure/store/pebbledb"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const prefix = "QUBIC_GO_DATA_PUBLISHER"

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
		InternalStoreFolder                 string        `conf:"default:store"`
		ArchiverGrpcHost                    string        `conf:"default:127.0.0.1:6001"`
		ServerListenAddr                    string        `conf:"default:0.0.0.0:8000"`
		ArchiverReadTimeout                 time.Duration `conf:"default:20s"`
		PublishWriteTimeout                 time.Duration `conf:"default:5m"`
		BatchSize                           int           `conf:"default:100"`
		NrWorkers                           int           `conf:"default:20"`
		OverrideLastProcessedTick           bool          `conf:"default:false"`
		OverrideLastProcessedTickEpochValue uint32        `conf:"default:155"`
		OverrideLastProcessedTickValue      uint32        `conf:"default:22669394"`
		Kafka                               struct {
			BootstrapServers []string `conf:"default:localhost:9092"`

			// Assuming we would want to publish more than transactions,
			// we should either have multiple topics, or a single one, with logic to differentiate between record types.
			TxTopic string `conf:"default:qubic-kafka-tx"`
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

	if cfg.OverrideLastProcessedTick {
		if err := procStore.SetLastProcessedTick(cfg.OverrideLastProcessedTickEpochValue, cfg.OverrideLastProcessedTickValue); err != nil {
			return fmt.Errorf("setting last processed tick: %v", err)
		}
	}

	kcl, err := kgo.NewClient(
		// The default should eventually be removed after implementing publishing for multiple types of data.
		kgo.DefaultProduceTopic(cfg.Kafka.TxTopic),
		kgo.SeedBrokers(cfg.Kafka.BootstrapServers...),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
	if err != nil {
		return errors.Wrap(err, "creating kafka client")
	}
	kafkaClient := kafka.NewClient(kcl)

	archiverClient, err := archiver.NewClient(cfg.ArchiverGrpcHost)
	if err != nil {
		return fmt.Errorf("creating archiver client: %v", err)
	}

	proc := tx.NewProcessor(archiverClient, cfg.ArchiverReadTimeout, kafkaClient, cfg.PublishWriteTimeout, procStore, cfg.BatchSize, sLogger)
	if err != nil {
		return fmt.Errorf("creating processor: %v", err)
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	procErrors := make(chan error, 1)
	go func() {
		procErrors <- proc.Start(cfg.NrWorkers)
	}()

	http.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		epochsLastProcessedTick, err := procStore.GetLastProcessedTickForAllEpochs()
		if err != nil {
			http.Error(w, fmt.Sprintf("getting last processed tick for all epochs: %v", err), http.StatusInternalServerError)
			return
		}
		response := map[string]map[uint32]uint32{
			"lastProcessedTicks": epochsLastProcessedTick,
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

	serverErr := make(chan error, 1)

	go func() {
		serverErr <- http.ListenAndServe(cfg.ServerListenAddr, nil)
	}()

	for {
		select {
		case <-shutdown:
			return errors.New("shutting down")
		case err := <-procErrors:
			return fmt.Errorf("processing error: %v", err)
		case err := <-serverErr:
			return fmt.Errorf("server error: %v", err)
		}
	}
}
