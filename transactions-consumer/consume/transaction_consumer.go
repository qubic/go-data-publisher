package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/qubic/transactions-consumer/extern"
	"github.com/qubic/transactions-consumer/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitUncommittedOffsets(ctx context.Context) error
	AllowRebalance()
}

type ElasticDocumentClient interface {
	BulkIndex(ctx context.Context, data []extern.EsDocument, indexName string) error
}

type ConsumerConfig struct {
	MaxPollRecords      int
	PermanentIndexName  string
	EphemeralIndexName  string
	EphemeralInputTypes []uint32
}

type TransactionConsumer struct {
	kafkaClient         KafkaClient
	elasticClient       ElasticDocumentClient
	maxPollRecords      int
	permanentIndexName  string
	ephemeralIndexName  string
	ephemeralInputTypes []uint32
	consumerMetrics     *metrics.Metrics
	currentTick         uint32
	currentEpoch        uint32
}

type TickTransactions struct {
	Epoch        uint32        `json:"epoch"`
	TickNumber   uint32        `json:"tickNumber"`
	Transactions []Transaction `json:"transactions"`
}

type Transaction struct {
	Hash      string `json:"hash"`
	Source    string `json:"source"`
	Dest      string `json:"destination"`
	Amount    int64  `json:"amount"`
	Tick      uint32 `json:"tickNumber"`
	InputType uint32 `json:"inputType"`
	InputSize uint32 `json:"inputSize"`
	InputData string `json:"inputData"`
	Signature string `json:"signature"`
	Timestamp uint64 `json:"timestamp"`
	MoneyFlew bool   `json:"moneyFlew"`
}

func NewTransactionConsumer(client KafkaClient, elasticClient ElasticDocumentClient, m *metrics.Metrics, config *ConsumerConfig) *TransactionConsumer {
	return &TransactionConsumer{
		kafkaClient:         client,
		consumerMetrics:     m,
		elasticClient:       elasticClient,
		permanentIndexName:  config.PermanentIndexName,
		ephemeralIndexName:  config.EphemeralIndexName,
		ephemeralInputTypes: config.EphemeralInputTypes,
		maxPollRecords:      config.MaxPollRecords,
	}
}

func (c *TransactionConsumer) Consume(ctx context.Context) error {
	ticker := time.Tick(100 * time.Millisecond) // drops ticks if consuming is too slow
	for range ticker {
		select {
		case <-ctx.Done():
			log.Println("Shutdown signal received, stopping consumer.")
			return nil

		default:
			count, err := c.consumeBatch(ctx)
			if err != nil {
				return fmt.Errorf("consuming batch: %w", err)
			}
			if count > 0 {
				log.Printf("Processed [%d] transactions. Latest tick: [%d].", count, c.currentTick)
			}
		}
	}
	return nil
}

func (c *TransactionConsumer) consumeBatch(ctx context.Context) (int, error) {
	defer c.kafkaClient.AllowRebalance()                        // because of the configured kgo.BlockRebalanceOnPoll() option
	fetches := c.kafkaClient.PollRecords(ctx, c.maxPollRecords) // batch process max x messages in one run
	if errs := fetches.Errors(); len(errs) > 0 {
		// Only non-retryable errors are returned.
		// Errors are typically per partition.
		for _, err := range errs {
			log.Printf("Error: %v", err)
		}
		return -1, errors.New("fetching records")
	}

	var permanentDocuments []extern.EsDocument
	var ephemeralDocuments []extern.EsDocument
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()

		var tickTransactions TickTransactions
		err := unmarshalTickTransactions(record, &tickTransactions)
		if err != nil {
			return -1, errors.Wrapf(err, "unmarshalling record value %s", string(record.Value))
		}

		for _, transaction := range tickTransactions.Transactions {
			val, err := json.Marshal(transaction)
			if err != nil {
				return -1, errors.Wrapf(err, "unmarshalling transaction %+v", transaction)
			}
			document := extern.EsDocument{Id: transaction.Hash, Payload: val}

			if c.isEphemeral(transaction.InputType, transaction.Dest) {
				ephemeralDocuments = append(ephemeralDocuments, document)
			} else {
				permanentDocuments = append(permanentDocuments, document)
			}

		}

		// on the initial sync metrics will be wrong because the publisher publishes multiple epochs in parallel, but we
		// only track the latest epoch here
		if tickTransactions.TickNumber > c.currentTick {
			c.currentTick = tickTransactions.TickNumber
			c.currentEpoch = tickTransactions.Epoch
		}
		c.consumerMetrics.IncProcessedTicks()
		c.consumerMetrics.IncProcessedMessages()
	}

	if len(ephemeralDocuments) != 0 {
		err := c.elasticClient.BulkIndex(ctx, ephemeralDocuments, c.ephemeralIndexName)
		if err != nil {
			return -1, errors.Wrapf(err, "bulk indexing [%d] documents (eph).", len(ephemeralDocuments))
		}
	}

	if len(permanentDocuments) != 0 {
		err := c.elasticClient.BulkIndex(ctx, permanentDocuments, c.permanentIndexName)
		if err != nil {
			return -1, errors.Wrapf(err, "bulk indexing [%d] documents.", len(permanentDocuments))
		}
	}

	c.consumerMetrics.SetProcessedTick(c.currentEpoch, c.currentTick)

	err := c.kafkaClient.CommitUncommittedOffsets(ctx)
	if err != nil {
		return -1, errors.Wrap(err, "committing offsets")
	}
	return len(permanentDocuments) + len(ephemeralDocuments), nil
}

func (c *TransactionConsumer) isEphemeral(inputType uint32, dest string) bool {
	const zeroAddress = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB"
	return len(c.ephemeralInputTypes) > 0 && slices.Contains(c.ephemeralInputTypes, inputType) && dest == zeroAddress
}

func unmarshalTickTransactions(record *kgo.Record, tickTransactions *TickTransactions) error {
	err := json.Unmarshal(record.Value, &tickTransactions)
	if err == nil && (tickTransactions.TickNumber == 0 || tickTransactions.Epoch == 0) {
		err = errors.Errorf("Missing tick and/or epoch information. %+v", tickTransactions)
	}
	return err
}
