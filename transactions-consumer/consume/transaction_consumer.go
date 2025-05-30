package consume

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/qubic/transactions-consumer/extern"
	"github.com/qubic/transactions-consumer/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"time"
)

type KafkaClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitUncommittedOffsets(ctx context.Context) error
	AllowRebalance()
}

type ElasticDocumentClient interface {
	BulkIndex(ctx context.Context, data []extern.EsDocument) error
}

type TransactionConsumer struct {
	kafkaClient     KafkaClient
	elasticClient   ElasticDocumentClient
	consumerMetrics *metrics.Metrics
	currentTick     uint32
	currentEpoch    uint32
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

func NewTransactionConsumer(client KafkaClient, elasticClient ElasticDocumentClient, m *metrics.Metrics) *TransactionConsumer {
	return &TransactionConsumer{
		kafkaClient:     client,
		consumerMetrics: m,
		elasticClient:   elasticClient,
	}
}

func (c *TransactionConsumer) Consume() error {
	for {
		count, err := c.consumeBatch()
		if err == nil {
			log.Printf("Processed [%d] transactions. Latest tick: [%d]", count, c.currentTick)
		} else {
			// if there is an error consuming we abort. We need to fix the error before trying again.
			log.Printf("Error consuming batch: %v", err) // exits
			return errors.Wrap(err, "consuming batch")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *TransactionConsumer) consumeBatch() (int, error) {
	ctx := context.Background()
	defer c.kafkaClient.AllowRebalance()            // because of the configured kgo.BlockRebalanceOnPoll() option
	fetches := c.kafkaClient.PollRecords(ctx, 1000) // batch process max x messages in one run
	if errs := fetches.Errors(); len(errs) > 0 {
		// Only non-retryable errors are returned.
		// Errors are typically per partition.
		for _, err := range errs {
			log.Printf("Error: %v", err)
		}
		return -1, errors.New("fetching records")
	}

	var documents []extern.EsDocument
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
			documents = append(documents, extern.EsDocument{
				Id:      transaction.Hash,
				Payload: val,
			})
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

	err := c.elasticClient.BulkIndex(ctx, documents)
	if err != nil {
		return -1, errors.Wrapf(err, "bulk indexing [%d] documents.", len(documents))
	}
	c.consumerMetrics.SetProcessedTick(c.currentEpoch, c.currentTick)

	err = c.kafkaClient.CommitUncommittedOffsets(ctx)
	if err != nil {
		return -1, errors.Wrap(err, "committing offsets")
	}
	return len(documents), nil
}

func unmarshalTickTransactions(record *kgo.Record, tickTransactions *TickTransactions) error {
	err := json.Unmarshal(record.Value, &tickTransactions)
	if err == nil && (tickTransactions.TickNumber == 0 || tickTransactions.Epoch == 0) {
		err = errors.Errorf("Missing tick and/or epoch information. %+v", tickTransactions)
	}
	return err
}
