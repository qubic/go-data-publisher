package kafka

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/qubic/tick-data-consumer/domain"
	"github.com/qubic/tick-data-consumer/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
)

type Client struct {
	kcl                *kgo.Client
	consumeMetrics     *metrics.Metrics
	lastProcessedEpoch uint32
	lastProcessedTick  uint32
}

func NewClient(kafkaClient *kgo.Client, metrics *metrics.Metrics) *Client {
	return &Client{
		kcl:            kafkaClient,
		consumeMetrics: metrics,
	}
}

func (c *Client) PollMessages(ctx context.Context) ([]*domain.TickData, error) {
	fetches := c.kcl.PollRecords(ctx, 1000) // batch process max x messages in one run
	if errs := fetches.Errors(); len(errs) > 0 {
		// Only non-retryable errors are returned.
		// Errors are typically per partition.
		for _, err := range errs {
			log.Printf("Error: %v", err)
		}
		return nil, errors.New("fetching records")
	}

	var messages []*domain.TickData
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		tickData, err := unmarshalTickData(record)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshalling record %s", string(record.Value))
		}
		messages = append(messages, tickData)
		c.consumeMetrics.IncProcessedMessages()
		if tickData.TickNumber > c.lastProcessedTick {
			c.lastProcessedTick = tickData.TickNumber
			c.lastProcessedEpoch = tickData.Epoch
		}
	}

	return messages, nil
}

// AllowRebalance needs to be called after polling in case option BlockRebalanceOnPoll is set
func (c *Client) AllowRebalance() {
	c.kcl.AllowRebalance() // because of the kgo.BlockRebalanceOnPoll() option
}

func (c *Client) Commit(ctx context.Context) error {
	err := c.kcl.CommitUncommittedOffsets(ctx)
	if err != nil {
		return errors.Wrap(err, "committing offsets")
	}
	c.consumeMetrics.SetProcessedTick(c.lastProcessedEpoch, c.lastProcessedTick)
	return nil
}

func unmarshalTickData(record *kgo.Record) (*domain.TickData, error) {
	var tickData domain.TickData
	err := json.Unmarshal(record.Value, &tickData)
	if err == nil && (tickData.ComputorIndex == 0 || tickData.TickNumber == 0 || tickData.Epoch == 0) {
		err = errors.Errorf("Tick data with missing information: %+v", tickData)
	}
	return &tickData, nil
}
